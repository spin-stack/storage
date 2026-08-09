package agent_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/simio/objectstore"
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

	// And the fleet is told, which is the half the guest's I/O error cannot supply. The
	// refusal above is correct and, on its own, entirely invisible outside this process:
	// the volume keeps reporting the watermarks its replay recovered, so the catalog and
	// `-fleet-status` read exactly as they did while it was healthy, and the operator's
	// only signal is one ERROR line in this host's log at attach time.
	//
	// Asserted on what the next report will carry, not on the Volume's field: the report
	// is the one thing anything outside this host ever sees.
	got := reportFor(t, second, told.GetVolumeId())
	if got.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_IMAGE_MISSING {
		t.Fatalf("the report says refusal=%s; a volume whose image is gone has to say which of the two floors refused it", got.Refusal)
	}
	if !strings.Contains(got.RefusalDetail, "object store holds no image") {
		t.Fatalf("refusal detail = %q, and it is what an operator reads before touching the bucket", got.RefusalDetail)
	}
}

// reportFor is one volume's entry in what the Agent will report next. It fails when
// there is none: a volume that stops being reported is the silence every assertion in
// this file is ultimately about.
func reportFor(t *testing.T, m *agent.VolumeManager, volumeID string) agent.VolumeStatus {
	t.Helper()
	vols, err := m.Volumes(t.Context())
	if err != nil {
		t.Fatalf("Volumes: %v", err)
	}
	for _, v := range vols {
		if v.VolumeID == volumeID {
			return v
		}
	}
	t.Fatalf("volume %s appears in no report at all: %+v", volumeID, vols)
	return agent.VolumeStatus{}
}

// refusalSession is resumeSession plus the listener factory, because the assertion below
// is about the socket rather than about the bytes. It is a second constructor and not a
// return value added to resumeSession: a dozen callers want the manager alone, and the
// socket is the concern of exactly this file.
func refusalSession(t *testing.T, d *sim.Disk, store objectstore.Store) (*agent.VolumeManager, *listenerFactory) {
	t.Helper()
	f := newListenerFactory()
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir:   holdDataDir,
		SocketDir: refusalSocketDir,
		Budget:    testBudget(),
	}, agent.VolumeManagerDeps{
		Clock:   sim.NewClock(time.Unix(1_700_000_000, 0).UTC()),
		Disk:    d,
		Listen:  f.listen,
		Mapper:  unusedMapper{},
		EventFD: unusedEventFD,
		Store:   store,
		Rand:    rand.Reader,
	})
	if err != nil {
		t.Fatalf("NewVolumeManager: %v", err)
	}
	return m, f
}

// refusalSocketDir is where refusalSession's manager would put its sockets. It matches
// resumeSession's so that the two fixtures cannot drift into naming a volume's socket
// differently.
const refusalSocketDir = "/run/spin"

// TestARefusedVolumeTakesItsSocketDownAndStaysOnTheReport is the difference between a
// tenant seeing a disk that is absent and a tenant seeing a disk that exists and cannot
// be read.
//
// Refusing used to stop only the *reads*. The listener was already bound by `start`
// before `fetchBase` ran, the supervisor kept re-opening it, and so a guest attached a
// perfectly ordinary 256 MiB /dev/vda and took a hard I/O error on every sector. That is
// the least actionable signal this system can emit: a VM's own boot logic can act on a
// disk that is not there, and it cannot act on one that answers EIO — it retries, mounts
// degraded, or hangs in initrd with nothing naming the volume.
//
// The two halves are asserted together on purpose, because the obvious way to close the
// first breaks the second. Cancelling the volume's context stops the serve loop, and the
// report is built from the same `m.volumes` entry — if the cancellation also dropped the
// runtime, the volume would vanish from the wire, and an absence there is exactly what a
// volume nobody ever placed on this host looks like (see VolumeManager.Volumes).
func TestARefusedVolumeTakesItsSocketDownAndStaysOnTheReport(t *testing.T) {
	t.Parallel()
	d, store := sim.NewDisk(), sim.NewObjectStore()
	v := desiredVolume(t, 1)

	seq := publishOneSession(t, resumeSession(t, d, store), v, 32)

	u, err := ids.Parse(v.GetVolumeId())
	if err != nil {
		t.Fatal(err)
	}
	store.InjectPermanentDelete()
	//nolint:usetesting // the store is shared across sessions; see resumeSession
	if err := store.Delete(context.Background(), image.ManifestKey([16]byte(u))); err != nil {
		t.Fatalf("deleting the manifest: %v", err)
	}

	second, f := refusalSession(t, d, store)
	//nolint:usetesting // see above
	defer func() { _ = second.Close(context.Background()) }()

	told := lostDataVolume(t, 1, seq, seq)
	told.VolumeId = v.GetVolumeId()
	if err := second.Apply(t.Context(), []*storagev1.DesiredVolume{told}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// The socket was bound — `start` opens the listener before the base is fetched, so
	// there is a real listener to take away. A fixture where nothing ever listened would
	// make the assertion below vacuous.
	socket := path.Join(refusalSocketDir, told.GetVolumeId()+".sock")
	ln := f.listenerFor(socket)
	if ln == nil {
		t.Fatalf("nothing bound %s; the volume never started, so this test proves nothing", socket)
	}

	// Waited for on the listener's own close, not on a duration: the refusal happens on
	// the base-fetch goroutine and the supervisor observes it on another, so there is no
	// point in this test's goroutine at which it has already happened.
	//
	// The deadline is this test's own and shorter than the suite's, unlike
	// TestServeIsSupervised, which leans on t.Context(). The difference is worth the two
	// lines: leaning on t.Context() means the *only* failure this can produce is the
	// suite's timeout panic — a stack, with the sentence below never printed — and the
	// sentence is the whole diagnosis. Nothing here reads a wall clock (INV-01);
	// context.WithTimeout takes the duration and the runtime does the waiting.
	wait, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	select {
	case <-ln.closed:
	case <-wait.Done():
		t.Fatalf("%s is still bound although the catalog says volume %s published at sequence %d "+
			"and the object store holds no image for it: a guest attaches a device it can only take I/O errors from",
			socket, told.GetVolumeId(), seq)
	}

	// And it does not come back. The supervisor re-listens on every session error by
	// design, so "closed once" and "not being served" are different claims — a supervisor
	// that re-opened here would put the socket back a microsecond later, for ever.
	//
	// Deterministic rather than a settle window: the supervisor closes the listener and
	// then reads ctx.Err(), which context guarantees is non-nil once Done has fired, so
	// by the time the close above is observable the re-listen branch is already unreachable.
	if n := countPaths(f, socket); n != 1 {
		t.Fatalf("%s was opened %d times: the supervisor put the refused volume's socket back", socket, n)
	}

	// The half a cancellation is most likely to break. The volume has no serve loop left,
	// and it must still be on the wire — with its reason — or the fleet cannot tell it
	// from a volume that was never placed here.
	got := reportFor(t, second, told.GetVolumeId())
	if got.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_IMAGE_MISSING {
		t.Fatalf("the report says refusal=%s after the socket was taken down; taking the device away "+
			"must not take the volume off the report", got.Refusal)
	}
	if !strings.Contains(got.RefusalDetail, "object store holds no image") {
		t.Fatalf("refusal detail = %q, and it is the sentence that survives the runtime being stopped", got.RefusalDetail)
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

	// Two refusals, two enum values, and this is where that choice earns its keep: the
	// operator's next step differs completely — an object that should be there and is not
	// versus one that is there and is behind — and a single "REFUSED" token, or a free
	// string nobody controls, would put both incidents in the same column.
	got := reportFor(t, second, told.GetVolumeId())
	if got.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_DURABILITY_LOST {
		t.Fatalf("the report says refusal=%s; a volume that came back short has to be told apart from one whose image is missing", got.Refusal)
	}
	if !strings.Contains(got.RefusalDetail, "ACKed to a guest as durable") {
		t.Fatalf("refusal detail = %q, and it is the sentence that names the two sequences", got.RefusalDetail)
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
