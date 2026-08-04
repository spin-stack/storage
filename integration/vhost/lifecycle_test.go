//go:build integration

package vhost_test

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/simio/real"
	"github.com/spin-stack/storage/internal/testinfra"
	"github.com/spin-stack/storage/internal/vhost/hostio"
	"github.com/spin-stack/storage/internal/wal"
)

// ADR-0018's definition of done, and DEV-0007's: one volume, one host, a real QEMU guest
// running **write → FLUSH → verified object → checkpoint → truncate**, and then reading
// the same range back.
//
// Every piece of that chain has had tests for weeks. What none of them could say is
// whether the pieces hold *together* under a real guest — and the answer, until DEV-0018
// was fixed two hours ago, was that a real guest could not get past its first block
// request. This is the test that says the chain is closed.
//
// It runs the **Agent's own runtime**, not a hand-assembled lane: `agent.VolumeManager`
// binds the socket, owns the WAL, resumes it, fetches the read view from the object
// store and schedules the checkpoint. Assembling those by hand here would be asserting
// that a test can wire them up, which is not the question.

// TestAGuestSurvivesCheckpointAndTruncation boots the same volume twice.
//
//  1. The guest writes a pattern with a buffered write and calls fsync. The FLUSH is
//     answered out of §14.4, so the record is in a verified object.
//  2. With the guest gone, the Agent publishes a checkpoint and truncates the local WAL
//     to it — the §21.1 order, and the point at which the *only* remaining copy of those
//     bytes is in the object store.
//  3. The guest boots again and reads the range back. It gets its own bytes, out of a
//     read view rebuilt from S3, because there is nowhere else left for them to come
//     from.
//
// Step 3 is the one that could not have been faked. If truncation had removed data the
// restart could not recover, this reads zeros — which is exactly what increment 5 was
// written to prevent and what `DurableRangeChecker` watches for in simulation. Here it is
// a real kernel reading a real device.
//
// Proven against a planted bug, and the first plant was invalid, which is worth
// recording: pointing *both* Agents at a different bucket left them consistent and the
// test passed. Deleting the bucket between the two boots is the real one, and the guest
// then reports `read-back mismatch at 1048576` — the bytes are gone from the only place
// left holding them.
//
// Rewritten for ADR-0026. It used to publish a checkpoint and truncate; there is no
// checkpoint now, and what a volume leaves behind is its image, published when it stops.
// The local WAL is removed outright instead, which is a stronger statement of the same
// thing: the second boot has nothing but the image to read from.
func TestAGuestSurvivesAStopAndComesBackFromItsImage(t *testing.T) {
	kernel, initramfs := testinfra.GuestImages(t)
	_, _ = testinfra.QEMUPaths(t) // skip early if QEMU is missing, before anything is built

	ctx := t.Context()
	dir := laneDir(t)
	volumeID := ids.New().String()

	m, sock := startAgent(t, ctx, dir, volumeID)

	// (1) The guest writes and fsyncs.
	if _, out := testinfra.RunLinuxGuest(t, ctx, sock, kernel, initramfs); !strings.Contains(out, "GUESTINIT-PASS") {
		t.Fatalf("the first boot did not report a pass:\n%s", testinfra.VerdictLines(out))
	}

	before := segmentFiles(t, dir, volumeID)
	if len(before) == 0 {
		t.Fatal("the guest's write left no WAL segments, so there is nothing for the image to have to replace")
	}

	// (2) Stopping the Agent is what publishes (ADR-0026). Nothing before this left the
	// host: the guest's fsync ACKed on fdatasync alone.
	if err := m.Close(); err != nil {
		t.Fatalf("stopping the Agent: %v", err)
	}
	if len(imageObjects(t, dir)) == 0 {
		t.Fatal("stopping the Agent published no image: the guest's data exists only in a WAL nobody will read")
	}

	// (3) Take the local WAL away, so only the image can answer. This is what the
	// checkpoint-and-truncate step used to do the long way round, and it is the whole
	// point of the test: a second boot that read the segments would prove nothing about
	// what left the host.
	if err := os.RemoveAll(filepath.Join(dir, "wal")); err != nil {
		t.Fatalf("removing the local WAL: %v", err)
	}
	if len(segmentFiles(t, dir, volumeID)) != 0 {
		t.Fatal("the local WAL is still there; the next boot could answer from it")
	}
	t.Logf("%d segments removed; the image is the only copy left", len(before))

	// (4) A second Agent, a second boot, and the same range read back.
	m2, sock2 := startAgent(t, ctx, dir, volumeID)
	defer func() { _ = m2.Close() }()

	_, out := testinfra.RunLinuxGuest(t, ctx, sock2, kernel, initramfs, "spin.mode=verify")
	switch {
	case strings.Contains(out, "GUESTINIT-FAIL"):
		t.Fatalf("after a checkpoint and a truncation the guest could not read its own data:\n%s",
			testinfra.VerdictLines(out))
	case !strings.Contains(out, "GUESTINIT-PASS"):
		t.Fatalf("the second boot reported no verdict:\n%s", out)
	}
}

// startAgent runs the real per-volume runtime against dir and returns the manager and
// the socket it bound. The wiring is production's: hostio for the socket and the kernel
// objects, real.Disk and a filesystem object store.
func startAgent(t *testing.T, ctx context.Context, dir, volumeID string) (*agent.VolumeManager, string) {
	t.Helper()
	d, err := real.NewDisk(dir)
	if err != nil {
		t.Fatalf("real.NewDisk: %v", err)
	}
	store, err := real.NewObjectStore(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("real.NewObjectStore: %v", err)
	}
	lm := lease.NewManager(real.NewClock(), leaseTTL)
	lm.Grant()

	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		// "." because the Disk is rooted at dir: DataDir is a path inside the Disk's
		// namespace, and passing dir here is DEV-0017.
		DataDir:   ".",
		SocketDir: dir,
		// Segments small enough to seal, because reclaim only unlinks sealed ones — a
		// truncation that unlinks nothing would make step 3 vacuous.
		Limits: wal.Limits{SegmentBytes: 8 << 10},
	}, agent.VolumeManagerDeps{
		Clock:   real.NewClock(),
		Disk:    d,
		Listen:  hostio.Listen,
		Mapper:  hostio.NewMapper(),
		EventFD: hostio.NewEventFD,
		Store:   store,
	})
	if err != nil {
		t.Fatalf("NewVolumeManager: %v", err)
	}
	if err := m.Apply(ctx, []*storagev1.DesiredVolume{{
		VolumeId: volumeID, SizeBytes: deviceSize, BlockSize: 512, Epoch: 1,
		State: storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}); err != nil {
		t.Fatalf("starting the volume: %v", err)
	}
	sock := filepath.Join(dir, volumeID+".sock")
	waitForSocket(t, sock)
	return m, sock
}

// waitForSocket blocks until the Agent has bound its listener. Apply returns once the
// listener is open, so this is a guard against the ordering changing rather than a poll
// anyone expects to spin.
func waitForSocket(t *testing.T, sock string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for {
		if _, err := os.Stat(sock); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("the Agent never bound %s", sock)
		default:
		}
	}
}

// segmentFiles lists the volume's WAL segments on the real filesystem — the thing a
// truncation removes, and the only evidence that it removed anything.
func segmentFiles(t *testing.T, dir, volumeID string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "wal", volumeID, "1", "*"))
	if err != nil {
		t.Fatal(err)
	}
	return matches
}

// imageObjects lists what the volume published, which is the artefact ADR-0026's whole
// contract produces. Asserted on the bucket rather than on a call: a publish that
// returned nil and wrote nothing satisfies any assertion on its error.
func imageObjects(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	root := filepath.Join(dir, "bucket", "image")
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			out = append(out, p)
		}
		return nil
	})
	return out
}
