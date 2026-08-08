package agent_test

import (
	"bytes"
	"crypto/rand"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/blockdev"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// A clone that has stopped once is the shape no test in this repository had: the two DST
// clone scenarios and `integration/e2e`'s clone lane all read a clone that has never
// stopped, and a clone only publishes an image of its own when it stops. So the second
// session — the one where the clone has both a parent snapshot *and* an image — had
// never run anywhere, and it did not work: fetchBase loaded the image, tried to slide the
// parent underneath it, and cow refused, because image.Load returns an unlayered map.
// Every read of the restarted clone then failed with ErrBaseUnavailable. A clone worked
// exactly once (CHUNK-ADDRESSING-SPEC §2).
//
// The assertion is the bytes a guest reads back through the device, split three ways on
// purpose, because a single offset cannot tell the failure modes apart:
//
//   - a range only the parent ever wrote, which the clone inherited and never touched —
//     this is the one that goes to zeros if the inheritance is dropped instead of
//     flattened;
//   - a range the clone wrote itself, which proves the second session found its own image
//     rather than merely re-reading the parent's snapshot;
//   - a range the parent wrote and the clone overwrote, which proves the layering order
//     survived the flattening — an image that resolved the base *over* the layer reads
//     the parent's byte here and passes the other two.
func TestACloneThatStoppedOnceStartsAgainAndReadsBothHalves(t *testing.T) {
	const (
		parentOnly = int64(0)                 // written by the parent, untouched by the clone
		shared     = int64(4 * testBlockSize) // written by the parent, overwritten by the clone
		cloneOnly  = int64(8 * testBlockSize) // written only by the clone
		parentByte = byte(0xA1)
		cloneByte  = byte(0xC2)
	)

	store := sim.NewObjectStore()

	// The parent writes, snapshots, and stops. Its snapshot is what the clone descends
	// from, and it is the only place the parent's bytes exist for the clone's first boot.
	parentDisk := sim.NewDisk()
	parent := cloneSession(t, "/var/lib/spin-parent", parentDisk, store)
	p := desiredVolume(t, 1)
	// The descriptor a provisioner would have written: the clone's Agent reads it to find
	// whether the parent descends from anything itself (agent.parentChain).
	writeDescriptor(t, store, p.GetVolumeId(), lineageLink{})
	pdev := serveClone(t, parent, p)
	writeBlock(t, pdev, parentOnly, parentByte)
	writeBlock(t, pdev, shared, parentByte)
	snapID := ids.New().String()
	if _, err := parent.Snapshot(t.Context(), p.GetVolumeId(), snapID); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := parent.Close(t.Context()); err != nil {
		t.Fatalf("closing the parent's Agent: %v", err)
	}

	c := desiredVolume(t, 1)
	c.ParentSnapshotId, c.ParentVolumeId = snapID, p.GetVolumeId()

	// First session: the clone has no image of its own, so the parent's snapshot *is* its
	// base. This is the only session anything covered, and it has always worked.
	cloneDisk := sim.NewDisk()
	first := cloneSession(t, "/var/lib/spin-clone", cloneDisk, store)
	cdev := serveClone(t, first, c)
	readBlock(t, cdev, parentOnly, parentByte, "the clone's first session inherits its parent's bytes")
	writeBlock(t, cdev, shared, cloneByte)
	writeBlock(t, cdev, cloneOnly, cloneByte)
	// Stopping is what publishes the clone's own image, and publishing is what makes the
	// second session a different problem from the first.
	if err := first.Close(t.Context()); err != nil {
		t.Fatalf("closing the clone's first Agent: %v", err)
	}

	// Second session: same disk, same bucket, same desired state — a restart, which is
	// the seam. Nothing here is new machinery; the only thing that changed between the
	// two sessions is that the clone now has an image.
	second := cloneSession(t, "/var/lib/spin-clone", cloneDisk, store)
	defer func() { _ = second.Close(t.Context()) }()
	again := serveClone(t, second, c)
	readBlock(t, again, parentOnly, parentByte, "a restarted clone still reads the ranges only its parent ever wrote")
	readBlock(t, again, cloneOnly, cloneByte, "a restarted clone reads what it wrote in its previous session")
	readBlock(t, again, shared, cloneByte, "a restarted clone reads its own overwrite, not the parent's older byte")
}

// cloneSession is one incarnation of an Agent over a caller-owned disk and bucket, so a
// restart is a new manager against an unchanged device and an unchanged object store —
// which is what a restart is. A manager that built either for itself would hide the seam
// under test.
func cloneSession(t *testing.T, dataDir string, disk *sim.Disk, store objectstore.Store) *agent.VolumeManager {
	t.Helper()
	f := newListenerFactory()
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: dataDir, SocketDir: "/run/spin", Budget: testBudget(),
	}, agent.VolumeManagerDeps{
		Clock:   sim.NewClock(time.Unix(1_700_000_000, 0).UTC()),
		Disk:    disk,
		Listen:  f.listen,
		Mapper:  unusedMapper{},
		EventFD: unusedEventFD,
		Store:   store,
		Rand:    rand.Reader,
	})
	if err != nil {
		t.Fatalf("NewVolumeManager(%s): %v", dataDir, err)
	}
	return m
}

// serveClone starts v and returns its device with the read view resolved. The first read
// is what waits for the base — a volume is served before its image is loaded, by design —
// so a base that failed surfaces here, as an error on a read, rather than as a timing
// question in a later assertion.
func serveClone(t *testing.T, m *agent.VolumeManager, v *storagev1.DesiredVolume) *blockdev.Device {
	t.Helper()
	if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{v}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	dev, ok := m.Device(v.GetVolumeId())
	if !ok {
		t.Fatalf("no device for volume %s", v.GetVolumeId())
	}
	if _, err := dev.ReadAt(make([]byte, testBlockSize), 0); err != nil {
		t.Fatalf("waiting for the read view of %s: %v", v.GetVolumeId(), err)
	}
	return dev
}

func writeBlock(t *testing.T, dev *blockdev.Device, off int64, b byte) {
	t.Helper()
	if _, err := dev.WriteAt(bytes.Repeat([]byte{b}, testBlockSize), off); err != nil {
		t.Fatalf("WriteAt %d: %v", off, err)
	}
}

func readBlock(t *testing.T, dev *blockdev.Device, off int64, want byte, why string) {
	t.Helper()
	got := make([]byte, testBlockSize)
	if _, err := dev.ReadAt(got, off); err != nil {
		t.Fatalf("ReadAt %d: %v — %s", off, err, why)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte{want}, testBlockSize)) {
		t.Fatalf("offset %d reads %#x, want %#x repeated — %s", off, got[:8], want, why)
	}
}
