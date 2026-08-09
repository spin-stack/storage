package agent_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// The regression, at the seam where it was reachable: two individually correct fixes —
// every placement grants a fresh epoch, and a volume that replays below the sequence a
// guest was told was durable refuses to serve — together bricked a volume whose data was
// intact on the local disk.
//
// A session is ACKed and not published; the operator does the documented thing and
// attaches the volume again; the Agent opens a WAL directory named for the *new* epoch,
// which is empty, and the floor refuses it. Another attach is another epoch, and moving
// the directory by hand is refused because the segment header carries its own epoch. The
// bytes are there and nothing an operator can run reaches them.
//
// The e2e lane drives this through real binaries and a real guest. What that lane cannot
// separate is the *catalog's* half — the durable sequence arrives on the desired state and
// deciding it here is what makes each case its own assertion.

// unpublishedSession writes n blocks through the device, ACKs them with a guest's fsync,
// and then gives the host up without publishing — the state a SIGKILLed Agent leaves and
// the state a detach that lands in the seconds before the next poll leaves.
//
// The teardown is the operator's own abandon (a cancelled context, `agent.Close`'s second
// signal), because that is the one exit that leaves no image *and* releases the data
// directory, which a later session in this process has to take. A kill leaves exactly the
// same bytes on the device; what it does not leave is a lock the test can reclaim.
//
// It returns the sequence the Agent would have reported to the Control Plane — the promise
// the next attach has to keep — and the offsets a later session must read back.
func unpublishedSession(t *testing.T, m *agent.VolumeManager, store *sim.ObjectStore, v *storagev1.DesiredVolume, n int) (int64, []int64) {
	t.Helper()
	dev := serveVolume(t, m, v)
	offsets := writePattern(t, dev, 0, n, 0xC1)
	// The guest's fsync. Everything below turns on this having been ACKed: without it
	// there is no promise to keep and no defect in refusing.
	if err := dev.Flush(t.Context()); err != nil {
		t.Fatalf("flushing the session: %v", err)
	}

	vols, err := m.Volumes(t.Context())
	if err != nil {
		t.Fatalf("Volumes: %v", err)
	}
	var seq int64
	for _, s := range vols {
		if s.VolumeID == v.GetVolumeId() {
			seq = s.DurableSequence
		}
	}
	if seq == 0 {
		t.Fatalf("volume %s ACKed nothing after %d writes and a flush; there is no promise for the next attach to keep",
			v.GetVolumeId(), n)
	}

	// A store that will not take the image, and an operator who stops waiting. Both are
	// needed: Close publishes successfully against a store that answers, and it is the
	// refusal *plus* the cancelled context that reproduce "this session is nowhere but
	// this host's device" while still releasing the data directory.
	store.InjectThrottle(1 << 20)
	abandoned, cancel := context.WithCancel(t.Context())
	cancel()
	if err := m.Close(abandoned); !errors.Is(err, agent.ErrPublishAbandoned) {
		t.Fatalf("abandoning the session ended with %v, not %v: the setup needs a session that is nowhere but this device",
			err, agent.ErrPublishAbandoned)
	}
	store.InjectThrottle(0)
	return seq, offsets
}

// epochDirs is what this host still holds on the device for one volume, by epoch. It
// stats the disk rather than asking the Agent: "the data directory is growing" is a claim
// about a device, and no counter the code keeps can make it true or false.
func epochDirs(t *testing.T, d *sim.Disk, volumeID string, epochs ...uint64) map[uint64]int {
	t.Helper()
	u, err := ids.Parse(volumeID)
	if err != nil {
		t.Fatal(err)
	}
	out := map[uint64]int{}
	for _, e := range epochs {
		names, err := d.List(wal.SegmentDir(holdDataDir+"/wal", [16]byte(u), e) + "/")
		if err != nil {
			t.Fatalf("listing epoch %d of %s: %v", e, volumeID, err)
		}
		out[e] = len(names)
	}
	return out
}

// TestAVolumeAttachedAtAFreshEpochServesTheSessionItsPredecessorNeverPublished is the
// regression itself.
//
// The assertion is the read, through the device a guest would be served from. A fix that
// moved the files but not the read view, or that satisfied the floor without recovering
// the records, would leave this returning zeros — which is the failure the floor exists to
// prevent, arrived at from the other side.
func TestAVolumeAttachedAtAFreshEpochServesTheSessionItsPredecessorNeverPublished(t *testing.T) {
	t.Parallel()
	d, store := sim.NewDisk(), sim.NewObjectStore()

	first := desiredVolume(t, 1)
	acked, offsets := unpublishedSession(t, resumeSession(t, d, store), store, first, 32)

	// The attach: a fresh epoch, no image in the bucket, and a catalog that remembers
	// exactly what this fleet promised.
	regranted := lostDataVolume(t, 2, 0, acked)
	regranted.VolumeId = first.GetVolumeId()

	second := resumeSession(t, d, store)
	//nolint:usetesting // the store is shared across sessions; see resumeSession
	dev := serveVolume(t, second, regranted)
	readBack(t, dev, offsets, 0xC1,
		fmt.Sprintf("the volume was ACKed up to sequence %d under epoch 1 and re-attached at epoch 2; "+
			"those records are on this host's disk and nothing else has them", acked))

	// And the epoch it came from is gone. Nothing used to delete one, so an A->B->A cycle
	// left a sealed segment beside the live one and every attach added another — for ever,
	// charged against the guest budget the Agent measures from that filesystem.
	dirs := epochDirs(t, d, regranted.GetVolumeId(), 1, 2)
	if dirs[1] != 0 {
		t.Fatalf("epoch 1 still holds %d segment file(s) after being taken up by epoch 2", dirs[1])
	}
	if dirs[2] == 0 {
		t.Fatalf("epoch 2 holds no segment file: the records were read back out of thin air")
	}
}

// TestAnAttachAtAFreshEpochLeavesTheRecordsTheObjectStoreAlreadyHolds is the other half of
// the same rule, and the one a "move the whole directory" implementation gets wrong.
//
// A session that *was* published leaves its segments on disk — release deliberately
// deletes nothing, so a restart can republish. Attaching at a fresh epoch must not lay
// those records back over the image: sequences are per-volume, so a record at or below the
// image's sequence is either the very record the image was built from or a sequence a
// later host reissued, and in the second case theirs is the one in the image.
//
// The observable is the same read, and what makes it an assertion is the *second* pattern:
// the volume is written again after the image is published, so a carry that laid the old
// records back on top would answer with 0xC1 where 0xD2 is the truth.
func TestAnAttachAtAFreshEpochLeavesTheRecordsTheObjectStoreAlreadyHolds(t *testing.T) {
	t.Parallel()
	d, store := sim.NewDisk(), sim.NewObjectStore()

	v := desiredVolume(t, 1)
	published := publishOneSession(t, resumeSession(t, d, store), v, 32)

	// Session 2 at a fresh epoch overwrites the same range and publishes in its turn.
	second := lostDataVolume(t, 2, published, published)
	second.VolumeId = v.GetVolumeId()
	m2 := resumeSession(t, d, store)
	dev2 := serveVolume(t, m2, second)
	offsets := writePattern(t, dev2, 0, 32, 0xD2)
	//nolint:usetesting // t.Context is cancelled before cleanups run and Close must publish
	if err := m2.Close(t.Context()); err != nil {
		t.Fatalf("publishing the second session: %v", err)
	}

	// Session 3, another fresh epoch. Both earlier epochs are still on this device.
	third := lostDataVolume(t, 3, published, published)
	third.VolumeId = v.GetVolumeId()
	dev3 := serveVolume(t, resumeSession(t, d, store), third)
	readBack(t, dev3, offsets, 0xD2,
		"epoch 1's records are still on this host's disk and the object store holds what replaced them; "+
			"answering with the older ones is a superseded write served with no error anywhere")

	dirs := epochDirs(t, d, v.GetVolumeId(), 1, 2, 3)
	if dirs[1] != 0 || dirs[2] != 0 {
		t.Fatalf("the drained epochs still hold %d and %d segment file(s); every attach would add another",
			dirs[1], dirs[2])
	}
}
