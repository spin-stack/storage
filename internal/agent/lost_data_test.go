package agent_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// The catalog's two watermarks, read at attach.
//
// `image.ErrNotPublished` and a short replay are the object store and the local disk each
// answering "there is nothing here", and neither of them can say whether there was
// supposed to be. The Control Plane can, and now does: published_sequence and
// durable_sequence ride on the desired state, and this file is what each of them decides.
//
// The e2e lane proves the seam — that a real Control Plane puts them on the wire and a
// real guest gets an I/O error. These are the cases that lane cannot separate, plus the
// ones where the answer must be "carry on": a floor that refused a volume it should have
// served would take the fleet down more thoroughly than the bug it replaces.

// lostDataVolume is a desired state with the two catalog watermarks set. Everything else
// matches desiredVolume, whose zeros are the "the catalog has never heard a report about
// this volume" case that must keep booting.
func lostDataVolume(t *testing.T, epoch, published, durable int64) *storagev1.DesiredVolume {
	t.Helper()
	v := desiredVolume(t, epoch)
	v.PublishedSequence = published
	v.DurableSequence = durable
	return v
}

// publishOneSession writes n blocks through the device, stops the manager so the image
// reaches the store, and returns the sequence that image carries.
//
// The sequence is read from the Agent's own report rather than counted, because that is
// the number the Control Plane would have been told and therefore the number the next
// attach is checked against. Counting writes here would make the fixture agree with itself
// and with nothing else.
func publishOneSession(t *testing.T, m *agent.VolumeManager, v *storagev1.DesiredVolume, n int) int64 {
	t.Helper()
	writePattern(t, serveVolume(t, m, v), 0, n, 0xC1)

	vols, err := m.Volumes(t.Context())
	if err != nil {
		t.Fatalf("Volumes: %v", err)
	}
	var seq int64
	for _, s := range vols {
		if s.VolumeID == v.GetVolumeId() {
			seq = s.LocalSequence
		}
	}
	if seq == 0 {
		t.Fatalf("volume %s reported sequence 0 after %d writes; there is nothing for a later session to be short of",
			v.GetVolumeId(), n)
	}
	//nolint:usetesting // t.Context is cancelled before cleanups run, and Close must be able to publish
	if err := m.Close(context.Background()); err != nil {
		t.Fatalf("publishing the session: %v", err)
	}
	return seq
}

// TestAVolumeThatHasPublishedRefusesWhenItsImageIsGone is the rule the e2e lane cannot
// isolate: there, deleting the manifest also leaves a WAL that may or may not still hold
// the records, so both floors are in play. Here the local WAL is deliberately whole — the
// durable floor is satisfied by replay alone — and the *only* fact that has changed is
// that the catalog says this volume published an image and the bucket has none.
//
// It has to refuse anyway. The volume's own segments answering today's reads is not the
// question; the question is that an object which is supposed to exist does not, and a
// volume that boots as if it were new republishes create-only over the missing key — at
// which point whatever else went missing with it is gone for good.
func TestAVolumeThatHasPublishedRefusesWhenItsImageIsGone(t *testing.T) {
	t.Parallel()
	d, store := sim.NewDisk(), sim.NewObjectStore()
	v := desiredVolume(t, 1)

	seq := publishOneSession(t, resumeSession(t, d, store), v, 32)

	// The accident: one key, and nothing else touched. InjectPermanentDelete is what makes
	// it the accident rather than a delete marker the Agent could see through — a lifecycle
	// expiry on a versioned bucket removes the bytes.
	u, err := ids.Parse(v.GetVolumeId())
	if err != nil {
		t.Fatal(err)
	}
	store.InjectPermanentDelete()
	//nolint:usetesting // the store is shared across sessions; see resumeSession
	if err := store.Delete(context.Background(), image.ManifestKey([16]byte(u))); err != nil {
		t.Fatalf("deleting the manifest: %v", err)
	}

	second := resumeSession(t, d, store)
	//nolint:usetesting // see above
	defer func() { _ = second.Close(context.Background()) }()

	// The catalog is unchanged and still correct — this is what makes the failure so quiet.
	told := lostDataVolume(t, 1, seq, seq)
	told.VolumeId = v.GetVolumeId()
	if err := second.Apply(t.Context(), []*storagev1.DesiredVolume{told}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	dev, ok := second.Device(told.GetVolumeId())
	if !ok {
		t.Fatalf("no device for volume %s", told.GetVolumeId())
	}

	// What a guest gets. Not an assertion on a flag: this is the read path, and a gate that
	// set baseFailed and answered the read from the replayed segments would satisfy every
	// assertion about the gate itself.
	_, err = dev.ReadAt(make([]byte, resumeBlock), 0)
	switch {
	case err == nil:
		t.Fatalf("the volume served a read although the catalog says it published at sequence %d and the bucket holds no image: "+
			"a guest cannot tell these bytes from the ones it lost", seq)
	case !errors.Is(err, wal.ErrBaseUnavailable):
		t.Fatalf("reading the volume failed with %v, not %v: the refusal has to reach the guest as an I/O error",
			err, wal.ErrBaseUnavailable)
	}
	if !errors.Is(err, agent.ErrImageMissing) {
		t.Fatalf("the read's error does not carry %v, so nothing tells an operator which of the two floors refused: %v",
			agent.ErrImageMissing, err)
	}
}

// TestARefusedVolumeIsNotRepublishedOverItsOwnMissingImage is the second half, and the
// half that makes the first one worth doing: a volume that refuses reads but still
// publishes at teardown writes its blank-or-partial session over the manifest key that
// went missing, create-only, and the loss becomes permanent.
func TestARefusedVolumeIsNotRepublishedOverItsOwnMissingImage(t *testing.T) {
	t.Parallel()
	d, store := sim.NewDisk(), sim.NewObjectStore()
	v := desiredVolume(t, 1)

	seq := publishOneSession(t, resumeSession(t, d, store), v, 32)

	u, err := ids.Parse(v.GetVolumeId())
	if err != nil {
		t.Fatal(err)
	}
	key := image.ManifestKey([16]byte(u))
	store.InjectPermanentDelete()
	//nolint:usetesting // see above
	if err := store.Delete(context.Background(), key); err != nil {
		t.Fatalf("deleting the manifest: %v", err)
	}

	second := resumeSession(t, d, store)
	told := lostDataVolume(t, 1, seq, seq)
	told.VolumeId = v.GetVolumeId()
	if err := second.Apply(t.Context(), []*storagev1.DesiredVolume{told}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	//nolint:usetesting // see above
	err = second.Close(context.Background())
	if !errors.Is(err, agent.ErrNoReadView) {
		t.Fatalf("the teardown of a refused volume returned %v; it has to be %v, or the publish went ahead",
			err, agent.ErrNoReadView)
	}
	//nolint:usetesting // see above
	if _, err := store.Get(context.Background(), key); err == nil {
		t.Fatalf("%s is back: the volume that could not find its image published one, which is the moment the tenant's data stops being recoverable", key)
	}
}

// TestAVolumeThatComesBackShortOfTheACKedSequenceRefuses is face 2 at unit speed: the
// image is intact and behind, and the local WAL that held the difference is gone.
//
// The store here takes everything, so nothing failed and nothing is retryable — which is
// exactly the shape of the reproduced incident. A host rebooted onto an empty instance
// store finds a perfectly good image, serves it, and the tenant reads what it wrote two
// sessions ago.
func TestAVolumeThatComesBackShortOfTheACKedSequenceRefuses(t *testing.T) {
	t.Parallel()
	d, store := sim.NewDisk(), sim.NewObjectStore()
	v := desiredVolume(t, 1)

	seq := publishOneSession(t, resumeSession(t, d, store), v, 32)

	// The instance store goes away with the host: a *different* disk, which is what a
	// reboot onto ephemeral local storage actually produces. Deleting files out of the old
	// one would model the same thing less honestly, because it would leave the directory.
	second := resumeSession(t, sim.NewDisk(), store)
	//nolint:usetesting // see above
	defer func() { _ = second.Close(context.Background()) }()

	// The catalog remembers ACKs the image does not carry. It cannot forget them either —
	// UpdateWatermarks takes GREATEST — so this is the permanent state of that row.
	told := lostDataVolume(t, 1, seq, seq+11)
	told.VolumeId = v.GetVolumeId()
	if err := second.Apply(t.Context(), []*storagev1.DesiredVolume{told}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	dev, ok := second.Device(told.GetVolumeId())
	if !ok {
		t.Fatalf("no device for volume %s", told.GetVolumeId())
	}

	_, err := dev.ReadAt(make([]byte, resumeBlock), 0)
	switch {
	case err == nil:
		t.Fatalf("the volume served a read at sequence %d after ACKing %d: the guest gets its old bytes and no error",
			seq, seq+11)
	case !errors.Is(err, agent.ErrDurabilityLost):
		t.Fatalf("reading the rolled-back volume failed with %v, which does not carry %v", err, agent.ErrDurabilityLost)
	}
}

// TestTheCatalogFloorsAdmitEveryVolumeThatIsNotShort is the other side, and it is the one
// worth more than the two above: a floor that refuses volumes it should serve is a worse
// outage than the bug it replaces, and every case here was reachable in normal operation
// the day this landed.
//
// The table is over the *desired state*, because that is the only thing that differs — the
// bucket and the disk are built the same way in each case, by publishing one session.
func TestTheCatalogFloorsAdmitEveryVolumeThatIsNotShort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// told builds the desired state from the sequence the session published.
		told func(seq int64) (published, durable int64)
	}{{
		// The catalog has never had a report about this volume: nothing has been placed
		// on a host long enough to say anything. Both floors are zero and admit everything.
		name: "a catalog that knows nothing",
		told: func(int64) (int64, int64) { return 0, 0 },
	}, {
		// The steady state. Both numbers are exactly what the image carries.
		name: "the catalog agrees with the image",
		told: func(seq int64) (int64, int64) { return seq, seq },
	}, {
		// The *common* case, and the reason the comparison is `<` and not `!=`. A report
		// reaches the catalog after the fdatasync it describes, and published_sequence
		// only reaches it on the session after the publish — so a healthy attach is
		// routinely ahead of both numbers.
		name: "the catalog is behind, which every report is",
		told: func(seq int64) (int64, int64) { return seq / 2, seq - 1 },
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d, store := sim.NewDisk(), sim.NewObjectStore()
			v := desiredVolume(t, 1)
			seq := publishOneSession(t, resumeSession(t, d, store), v, 32)

			second := resumeSession(t, d, store)
			//nolint:usetesting // see above
			defer func() { _ = second.Close(context.Background()) }()

			published, durable := tc.told(seq)
			told := lostDataVolume(t, 1, published, durable)
			told.VolumeId = v.GetVolumeId()
			if err := second.Apply(t.Context(), []*storagev1.DesiredVolume{told}); err != nil {
				t.Fatalf("Apply: %v", err)
			}
			dev, ok := second.Device(told.GetVolumeId())
			if !ok {
				t.Fatalf("no device for volume %s", told.GetVolumeId())
			}
			// Read *what the first session wrote*, not any block: a refusal and an empty
			// view are told apart by the bytes, and only the bytes.
			readBack(t, dev, []int64{0}, 0xC1, fmt.Sprintf(
				"the catalog said published=%d durable=%d against a session that reached sequence %d, "+
					"and the volume that holds every byte of it was refused",
				published, durable, seq))
		})
	}
}
