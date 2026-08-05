package agent_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/blockdev"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// What a *second* session does with the first session's local WAL.
//
// A restarted Agent re-attaches at the same epoch (ADR-0024), so it opens the same
// directory — <data-dir>/wal/<volume-id>/<epoch> — with the previous session's segments
// still in it. Nothing in this repository asked what happens next, and two of the three
// possible answers are wrong: ignoring those segments loses everything a session wrote
// after its last publish, and keeping all of them makes a host's data directory grow by a
// session on every restart.
//
// These two tests are a pair and neither is worth anything alone. The first says the
// records a published image already holds are given back to the device; the second says
// the records no image holds are kept, and read back. A version that reclaimed nothing
// passes the second; a version that reclaimed everything passes the first.

// resumeSession is one incarnation of an Agent: a manager over a disk and an object store
// the caller owns, so the next one can be built over the same two. That is the whole
// fixture — a restart is a new process against an unchanged device and an unchanged
// bucket, and a manager that built either for itself would hide exactly the seam under
// test.
func resumeSession(t *testing.T, d *sim.Disk, store objectstore.Store) *agent.VolumeManager {
	t.Helper()
	f := newListenerFactory()
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir:   holdDataDir,
		SocketDir: "/run/spin",
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
	return m
}

// resumeBlock is the size of every write these tests make. 4 KiB against the 128 KiB
// segments testBudget produces (a 1 MiB share, an eighth of it per segment) seals a
// segment about every thirty records — so a session of half a megabyte leaves several
// *sealed* segments, which is the premise of the whole file: reclaim never unlinks the
// newest, so a fixture with one segment cannot tell a working reclaim from none.
const resumeBlock = 4096

// serveVolume starts v and returns its device with the read view already resolved. The
// read is what waits for the base — the volume is served before its image is loaded, by
// design — so doing it here keeps every later assertion about the disk rather than about
// a goroutine's timing.
func serveVolume(t *testing.T, m *agent.VolumeManager, v *storagev1.DesiredVolume) *blockdev.Device {
	t.Helper()
	if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{v}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	dev, ok := m.Device(v.GetVolumeId())
	if !ok {
		t.Fatalf("no device for volume %s", v.GetVolumeId())
	}
	if _, err := dev.ReadAt(make([]byte, resumeBlock), 0); err != nil {
		t.Fatalf("waiting for the read view of %s: %v", v.GetVolumeId(), err)
	}
	return dev
}

// writePattern fills n blocks starting at off with b, through the device a guest would be
// served from. It returns the offsets, so a later session can be asked for exactly them.
func writePattern(t *testing.T, dev *blockdev.Device, off int64, n int, b byte) []int64 {
	t.Helper()
	payload := bytes.Repeat([]byte{b}, resumeBlock)
	offsets := make([]int64, 0, n)
	for i := range n {
		at := off + int64(i)*resumeBlock
		if _, err := dev.WriteAt(payload, at); err != nil {
			t.Fatalf("WriteAt %d: %v", at, err)
		}
		offsets = append(offsets, at)
	}
	return offsets
}

// readBack fails unless every offset still holds b. This is the assertion the file exists
// for: not "a segment survived" but "the bytes a guest was told were safe come back",
// which is the only statement a reclaim can be judged against.
func readBack(t *testing.T, dev *blockdev.Device, offsets []int64, b byte, why string) {
	t.Helper()
	want := bytes.Repeat([]byte{b}, resumeBlock)
	got := make([]byte, resumeBlock)
	for _, at := range offsets {
		if _, err := dev.ReadAt(got, at); err != nil {
			t.Fatalf("reading offset %d after a restart: %v — %s", at, err, why)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("offset %d reads %#x where %#x was written and ACKed — %s",
				at, got[:8], want[:8], why)
		}
	}
}

// walBytes reports the segment files of one volume's WAL directory and what they occupy.
// It stats the disk rather than asking the log: "the data directory is growing" is a
// statement about a device, and no counter the code maintains can make it true or false.
func walBytes(t *testing.T, d *sim.Disk, v *storagev1.DesiredVolume) (names []string, total int64) {
	t.Helper()
	u, err := ids.Parse(v.GetVolumeId())
	if err != nil {
		t.Fatal(err)
	}
	dir := wal.SegmentDir(holdDataDir+"/wal", [16]byte(u), uint64(v.GetEpoch()))
	names, err = d.List(dir + "/")
	if err != nil {
		t.Fatalf("listing %s: %v", dir, err)
	}
	for _, name := range names {
		f, err := d.Open(name)
		if err != nil {
			t.Fatalf("opening %s: %v", name, err)
		}
		size, err := f.Size()
		_ = f.Close()
		if err != nil {
			t.Fatalf("sizing %s: %v", name, err)
		}
		total += size
	}
	return names, total
}

// TestASecondSessionGivesBackTheWALItsImageAlreadyHolds.
//
// A volume stops, its image is published, and the Agent restarts onto the same data
// directory at the same epoch. Every record in those segments is inside the image the
// second session then loads, so keeping them buys nothing — and nothing was giving them
// back. A host that started and stopped one volume ten times held ten sessions of WAL,
// and Limits.MaxLocalBytes (the volume's share of the device) is cleared by nothing at
// all, so the end state is a guest whose WRITEs are refused with backpressure for good,
// on a volume whose every byte is safe in the bucket.
func TestASecondSessionGivesBackTheWALItsImageAlreadyHolds(t *testing.T) {
	t.Parallel()
	d, store := sim.NewDisk(), sim.NewObjectStore()
	v := desiredVolume(t, 1)

	first := resumeSession(t, d, store)
	offsets := writePattern(t, serveVolume(t, first, v), 0, 128, 0xA1)

	before, beforeBytes := walBytes(t, d, v)
	if len(before) < 2 {
		t.Fatalf("the session left %d segment(s); reclaim never unlinks the newest, so this fixture could not tell a working reclaim from none",
			len(before))
	}

	// Stopping is what publishes (ADR-0026), and releasing deliberately deletes nothing:
	// the records stay exactly as they were so a publish that had failed could be retried
	// by the next incarnation (SHUTDOWN-PUBLISH-SPEC §6). Pinned here because the
	// assertion below would also be satisfied by a release that reclaimed.
	if err := first.Close(context.Background()); err != nil { //nolint:usetesting // t.Context is cancelled before cleanups run, and a cancelled context is how an operator abandons a publish
		t.Fatalf("stopping the first session: %v", err)
	}
	if held, _ := walBytes(t, d, v); len(held) != len(before) {
		t.Fatalf("releasing the volume left %d of %d segments: an abandoned publish would have nothing to retry from",
			len(held), len(before))
	}

	second := resumeSession(t, d, store)
	t.Cleanup(func() { _ = second.Close(context.Background()) }) //nolint:usetesting // see above
	dev := serveVolume(t, second, v)

	after, afterBytes := walBytes(t, d, v)
	if len(after) != 1 || afterBytes >= beforeBytes {
		t.Fatalf("a second session kept %d segment(s) holding %d bytes, out of the %d segments and %d bytes its own image already covers: "+
			"the data directory grows by a session on every restart, and nothing ever clears MaxLocalBytes",
			len(after), afterBytes, len(before), beforeBytes)
	}
	t.Logf("%d segments (%d bytes) reduced to %d (%d bytes)", len(before), beforeBytes, len(after), afterBytes)

	// And the guest is unaffected, which is the half that makes the reclaim legal rather
	// than merely tidy.
	readBack(t, dev, offsets, 0xA1, "the second session reclaimed segments its image did not cover")
}

// abandonSession stops m the way an operator does when the store will not take the
// session: one attempt, then a second signal. Everything it held stays on disk, which is
// what the next incarnation resumes from (SHUTDOWN-PUBLISH-SPEC §6).
func abandonSession(t *testing.T, m *agent.VolumeManager) {
	t.Helper()
	ctx, abandon := context.WithCancel(context.Background()) //nolint:usetesting // see above
	abandon()
	if err := m.Close(ctx); !errors.Is(err, agent.ErrPublishAbandoned) {
		t.Fatalf("Close returned %v; this fixture needs a session that was never published", err)
	}
}

// TestASessionThatNeverPublishedKeepsItsWAL is the counterweight, and without it the test
// above is satisfied by deleting the WAL outright.
//
// Four sessions, because a wrong reclaim is invisible to the session that performs it:
// resume replays every record into the read view *before* the base arrives, so a session
// that unlinked segments it should have kept still answers every read correctly out of
// memory. Only the incarnation after it finds them gone. That is not a contrivance —
// re-attaching happens at the same epoch (ADR-0024), so a host whose store is down for
// two restarts is exactly this.
//
//  1. writes 0xB1 and publishes;
//  2. writes 0xB2 and cannot publish — the store refuses every PUT and the operator
//     abandons the teardown — so those records exist on this host and nowhere else;
//  3. resumes over both, publishes nothing either. This is where a reclaim that ignored
//     the published point would destroy step 2;
//  4. resumes and must read back *both* patterns: 0xB1 from the image, 0xB2 from the
//     segments every reclaim was required to leave alone.
//
// The two ranges do not overlap, on purpose: overlapping ones are how a test that proves
// nothing passes, because either session's writes would satisfy the other's assertion.
func TestASessionThatNeverPublishedKeepsItsWAL(t *testing.T) {
	t.Parallel()
	d := sim.NewDisk()
	store := &failingStore{Store: sim.NewObjectStore()}
	v := desiredVolume(t, 1)

	first := resumeSession(t, d, store)
	published := writePattern(t, serveVolume(t, first, v), 0, 128, 0xB1)
	if err := first.Close(context.Background()); err != nil { //nolint:usetesting // see above
		t.Fatalf("stopping the first session: %v", err)
	}

	store.refuseEverything()
	second := resumeSession(t, d, store)
	// Enough to seal segments of its own, so "it keeps its WAL" is not satisfied by the
	// one thing reclaim never does anyway — unlink the newest file. A second session that
	// fitted inside the segment it adopted would survive a reclaim that removed
	// everything it was allowed to touch.
	unpublished := writePattern(t, serveVolume(t, second, v), 512<<10, 64, 0xB2)
	abandonSession(t, second)

	third := resumeSession(t, d, store)
	serveVolume(t, third, v)
	abandonSession(t, third)

	store.takeEverything()
	fourth := resumeSession(t, d, store)
	t.Cleanup(func() { _ = fourth.Close(context.Background()) }) //nolint:usetesting // see above
	dev := serveVolume(t, fourth, v)

	readBack(t, dev, published, 0xB1, "the image the first session published")
	readBack(t, dev, unpublished, 0xB2,
		"the second session's writes are in no image, so reclaiming their segments destroys the only copy")
}
