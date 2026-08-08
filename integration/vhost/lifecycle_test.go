//go:build integration

package vhost_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/real"
	"github.com/spin-stack/storage/internal/testinfra"
	"github.com/spin-stack/storage/internal/vhost/hostio"
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
	if _, out := testinfra.RunLinuxGuest(t, sock, kernel, initramfs); !strings.Contains(out, "GUESTINIT-PASS") {
		t.Fatalf("the first boot did not report a pass:\n%s", testinfra.VerdictLines(out))
	}

	before := segmentFiles(t, dir, volumeID)
	if len(before) == 0 {
		t.Fatal("the guest's write left no WAL segments, so there is nothing for the image to have to replace")
	}

	// (2) Stopping the Agent is what publishes (ADR-0026). Nothing before this left the
	// host: the guest's fsync ACKed on fdatasync alone.
	if err := m.Close(t.Context()); err != nil {
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
	defer func() { _ = m2.Close(t.Context()) }()

	_, out := testinfra.RunLinuxGuest(t, sock2, kernel, initramfs, "spin.mode=verify")
	switch {
	case strings.Contains(out, "GUESTINIT-FAIL"):
		t.Fatalf("after a checkpoint and a truncation the guest could not read its own data:\n%s",
			testinfra.VerdictLines(out))
	case !strings.Contains(out, "GUESTINIT-PASS"):
		t.Fatalf("the second boot reported no verdict:\n%s", out)
	}
}

// TestASecondSessionGivesBackTheWALItsImageAlreadyHolds is the test above with its third
// step **removed**, and that is the whole point of having both.
//
// That one deletes the local WAL by hand so only the image can answer. This one leaves it
// exactly where the first session left it, which is what a restarted Agent actually finds:
// re-attaching happens at the *same* epoch (ADR-0024), so the second session opens
// <data-dir>/wal/<volume-id>/<epoch> with the previous session's segments still in it.
// Nothing in this repository had ever asked what happens to them.
//
// What happened was nothing: they were replayed on top of an image that already held
// every one of their records, and kept. Kept for the life of the host, because a session
// only ever appends and the next one adopts what it finds — so a volume started and
// stopped ten times held ten sessions of WAL, all of it redundant, against a
// Limits.MaxLocalBytes that no session clears. The visible end of that is a guest whose
// WRITEs are refused with backpressure for good, on a volume whose every byte is safe in
// the bucket.
//
// The assertions are bytes on a real filesystem before and after, and a real kernel
// reading its own pattern back afterwards. Neither is enough alone: an Agent that deleted
// the whole directory passes the first, and one that reclaimed nothing passes the second.
func TestASecondSessionGivesBackTheWALItsImageAlreadyHolds(t *testing.T) {
	kernel, initramfs := testinfra.GuestImages(t)
	_, _ = testinfra.QEMUPaths(t) // skip early if QEMU is missing, before anything is built

	ctx := t.Context()
	dir := laneDir(t)
	volumeID := ids.New().String()

	// A 128 KiB share, so Budget makes the segments 16 KiB. guestinit's write mode puts
	// eight 4 KiB blocks on the device — about 33 KiB of records — which fills two
	// segments and opens a third. That matters because reclaim never unlinks the newest
	// segment: on the lane's ordinary 1 MiB share this guest's whole session fits in one
	// file, and a test over one file cannot tell a working reclaim from none.
	budget := agent.Budget{DeviceBytes: 1 << 20, ReserveBytes: 64 << 10, GuestBytes: 128 << 10, MaxVolumes: 1}

	m, sock := startAgentBudgeted(t, ctx, dir, volumeID, budget)

	// (1) A real guest writes and fsyncs. Nothing has left the host yet: the ACK is a
	// local fdatasync (§14.8, INV-18).
	if _, out := testinfra.RunLinuxGuest(t, sock, kernel, initramfs); !strings.Contains(out, "GUESTINIT-PASS") {
		t.Fatalf("the first boot did not report a pass:\n%s", testinfra.VerdictLines(out))
	}

	before := segmentFiles(t, dir, volumeID)
	beforeBytes := totalBytes(t, before)
	if len(before) < 2 {
		t.Fatalf("the guest's session left %d segment file(s) in %s: reclaim never unlinks the newest, "+
			"so this fixture could not tell a working reclaim from none", len(before), filepath.Join(dir, "wal"))
	}

	// (2) Stopping publishes the image, and releasing the volume deliberately deletes
	// nothing — the records stay so a publish that had failed could be retried by the next
	// incarnation (SHUTDOWN-PUBLISH-SPEC §6). Both halves are asserted, because the
	// assertion after the restart would also be satisfied by a release that reclaimed.
	if err := m.Close(t.Context()); err != nil {
		t.Fatalf("stopping the Agent: %v", err)
	}
	if len(imageObjects(t, dir)) == 0 {
		t.Fatal("stopping the Agent published no image: there is nothing that could make the local WAL redundant")
	}
	if held := segmentFiles(t, dir, volumeID); len(held) != len(before) {
		t.Fatalf("releasing the volume left %d of %d segments: an abandoned publish would have nothing to retry from",
			len(held), len(before))
	}

	// (3) A second Agent on the same directory, and a real kernel reading the range back.
	// The read is what proves the reclaim cost the guest nothing, and it is also what
	// makes the measurement below well defined: a read cannot be answered until the base
	// is installed, and installing the base is what reclaims.
	m2, sock2 := startAgentBudgeted(t, ctx, dir, volumeID, budget)
	defer func() { _ = m2.Close(t.Context()) }()

	_, out := testinfra.RunLinuxGuest(t, sock2, kernel, initramfs, "spin.mode=verify")
	switch {
	case strings.Contains(out, "GUESTINIT-FAIL"):
		t.Fatalf("after the second session reclaimed the WAL its image covers, the guest could not read its own data:\n%s",
			testinfra.VerdictLines(out))
	case !strings.Contains(out, "GUESTINIT-PASS"):
		t.Fatalf("the second boot reported no verdict:\n%s", out)
	}

	after := segmentFiles(t, dir, volumeID)
	afterBytes := totalBytes(t, after)
	if len(after) != 1 || afterBytes >= beforeBytes {
		t.Fatalf("the second session kept %d segment file(s) holding %d bytes, out of the %d files and %d bytes "+
			"its own image already covers: this host's data directory grows by a session on every restart, "+
			"and nothing ever clears MaxLocalBytes",
			len(after), afterBytes, len(before), beforeBytes)
	}
	t.Logf("%d segments (%d bytes) reduced to %d (%d bytes), and the guest still reads its own pattern",
		len(before), beforeBytes, len(after), afterBytes)
}

// totalBytes is what those files occupy on the device. Stat and not a number the Agent
// reports: "the data directory is growing" is a statement about a filesystem, and no
// counter the code maintains can make it true or false.
func totalBytes(t *testing.T, names []string) int64 {
	t.Helper()
	var total int64
	for _, name := range names {
		fi, err := os.Stat(name)
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		total += fi.Size()
	}
	return total
}

// startAgent runs the real per-volume runtime against dir and returns the manager and
// the socket it bound. The wiring is production's: hostio for the socket and the kernel
// objects, real.Disk and a filesystem object store.
func startAgent(t *testing.T, ctx context.Context, dir, volumeID string) (*agent.VolumeManager, string) {
	t.Helper()
	return startAgentBudgeted(t, ctx, dir, volumeID, laneBudget())
}

// startAgentBudgeted is startAgent with the device budget chosen by the caller. Only the
// reclaim test needs one: the segment size is an eighth of the share, so a test that
// wants several *sealed* segments out of the 32 KiB this guest writes has to say so.
func startAgentBudgeted(t *testing.T, ctx context.Context, dir, volumeID string, budget agent.Budget) (*agent.VolumeManager, string) {
	t.Helper()
	store, err := real.NewObjectStore(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("real.NewObjectStore: %v", err)
	}
	return startAgentOn(t, ctx, dir, volumeID, store, budget)
}

// laneBudget is what most of this lane runs on. A budget rather than a raw wal.Limits:
// production has no other way to bound a log any more, so a lane that set the limits
// itself would be testing a wiring no Agent uses (ADR-0013 §1). The share is 1 MiB, which
// Budget turns into 128 KiB segments — large enough that the guest is never the one
// refused, and small enough to seal at all.
func laneBudget() agent.Budget {
	return agent.Budget{DeviceBytes: 8 << 20, ReserveBytes: 1 << 20, GuestBytes: 1 << 20, MaxVolumes: 1}
}

// startAgentOn is startAgent with the object store handed in, for the one test that has
// to watch the store from the outside: the snapshot scenario below suspends the upload at
// its first call and needs a store it can hold, not one this function builds and keeps.
// Everything else about the wiring is identical, deliberately — two copies of the Agent's
// production wiring would drift, and the one that drifted would be the one nobody ran.
func startAgentOn(t *testing.T, ctx context.Context, dir, volumeID string, store objectstore.Store, budget agent.Budget) (*agent.VolumeManager, string) {
	t.Helper()
	d, err := real.NewDisk(dir)
	if err != nil {
		t.Fatalf("real.NewDisk: %v", err)
	}
	lm := lease.NewManager(real.NewClock(), leaseTTL)
	lm.Grant()

	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		// "." because the Disk is rooted at dir: DataDir is a path inside the Disk's
		// namespace, and passing dir here is DEV-0017.
		DataDir:   ".",
		SocketDir: dir,
		Budget:    budget,
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
	if err := m.Apply(ctx, []*storagev1.DesiredVolume{desiredVolume(volumeID, "")}); err != nil {
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

// desiredVolume is what the Control Plane hands the Agent for this lane's volume. The
// snapshot request is a *field of the desired state*, not a method call, which is the
// whole of §19's trigger path on the Agent side: `control-plane -snapshot-volume` writes
// a catalog row, the row becomes pending_snapshot_id, and Apply is where the Agent finds
// it. Driving it through Apply rather than through VolumeManager.Snapshot is deliberate —
// Snapshot exists for lanes and production never calls it, so a lane that used it would
// be exercising a path the fleet does not have.
func desiredVolume(volumeID, snapshotID string) *storagev1.DesiredVolume {
	return &storagev1.DesiredVolume{
		VolumeId: volumeID, SizeBytes: deviceSize, BlockSize: 512, Epoch: 1,
		State:             storagev1.VolumeState_VOLUME_STATE_ACTIVE,
		PendingSnapshotId: snapshotID,
	}
}

// The two regions this lane's snapshot assertion is built on, both inside the 16 MiB
// device and neither at LBA 0, which anything that probes a block device may write to.
//
//   - guestRegion is where `integration/guestinit`'s hold mode writes: 4 KiB blocks at
//     2 MiB + k·64 KiB for k in 0..7, so 512 KiB from 2 MiB covers all of them. That
//     bound is a contract with the guest program, and the last assertion in the test is
//     what enforces it: if the guest's offsets ever move out of this window, the volume's
//     own image stops differing from the filler and the test fails saying so.
//   - decoyRegion is *not* written by anything. It exists only to make the frozen view
//     two chunks instead of one, because uploadChunks reads a chunk's bytes and only then
//     talks to the store — with a single chunk the whole copy is already in memory before
//     the first call, and there would be no moment to suspend the upload *at*.
const (
	decoyOffset = 512 << 10
	decoyLength = 64 << 10
	decoyByte   = 0xD1

	guestRegionOffset = 2 << 20
	guestRegionLength = 512 << 10
	fillerByte        = 0xF5
)

// TestASnapshotOfAWritingGuestIsOnePointAndNotASmear is §19's actual claim, and until
// this test nothing in the repository had ever put it in front of a guest.
//
// §19 says a snapshot is a **sequence number, not an event**: the capture is a pointer
// swap, the guest keeps writing microseconds later, and the copy that lands in the bucket
// is the volume *as it was at that number* — not whatever the WAL held when the upload
// finished. Every existing proof of that is structural. The e2e lane's "snapshot of a
// live volume" asserts that a manifest appears while the socket still exists; the unit
// tests assert that Freeze returns a map and a sequence. Both are satisfied by a snapshot
// that copies the live view, which is precisely the bug this exists to catch.
//
// Why the weaker assertions prove nothing, spelled out because they are the ones a reader
// will reach for first:
//
//   - "the snapshot object exists" — a smeared snapshot exists too. It is an object with
//     the wrong bytes in it, and a clone made from it is a machine that boots a state its
//     parent never had.
//   - "the sequence is non-zero" / "the sequence is below the volume's" — Sequence is a
//     number the *same function* writes into the manifest; nothing ties it to the bytes
//     next to it. Freeze could return the live map with a correct sequence and every
//     sequence assertion in the tree stays green.
//   - "the guest's data is in the snapshot" — that is completeness, the opposite half.
//     A snapshot that copies everything, including writes made after the freeze, passes it
//     perfectly.
//
// The only thing that separates a frozen point from a smear is a byte that **changed
// after the capture**, so the test has to manufacture one, and the shape of it is forced
// by two facts about what is available:
//
//  1. The guest writes one constant pattern (`integration/guestinit`), so the volume's
//     contents stop changing about 400 ms into a hold run — every later moment looks
//     identical, and a snapshot taken at any of them is indistinguishable from a smear.
//     So the changing byte has to be "filler the guest then overwrites", which means the
//     region must already be in the frozen view: uploadChunks evaluates the view's delta
//     once, up front, so a range that did not exist at the freeze is never uploaded at
//     all and could never carry a late write. Hence the volume boots from an image this
//     test publishes, with the guest's whole write region pre-filled.
//  2. Whether a write lands inside the freeze→upload window cannot be left to timing.
//     The store is therefore a double that **suspends the upload at its first call** and
//     holds it there while a real kernel boots and writes. That turns "the guest wrote
//     during the copy" from a race into an ordering the test enforces.
//
// So: the volume holds filler, a snapshot is asked for through the desired state, the
// upload is stopped at the store with the guest's region not yet read, a real Linux guest
// then boots and writes into that region — proven by heartbeats, each printed only after
// an fsync this backend answered — and the upload is let go. What the snapshot must
// contain is filler and nothing else.
//
// Proven able to fail. The plant is the natural error in wal.Log.Freeze — take the view
// without swapping a fresh layer over it, so the "frozen" map is the live one:
//
//	frozen := l.view
//	-	l.view = cow.NewIntervalMapOver(frozen)
//
// and the test goes red on the bytes:
//
//	the snapshot carries a byte the guest wrote after it was frozen: at volume offset
//	2097152 it holds 0x41, and the point it was frozen at held 0xf5
func TestASnapshotOfAWritingGuestIsOnePointAndNotASmear(t *testing.T) {
	kernel, initramfs := testinfra.GuestImages(t)
	_, _ = testinfra.QEMUPaths(t) // skip early if QEMU is missing, before anything is built

	ctx := t.Context()
	dir := laneDir(t)
	volumeID := ids.New().String()
	u, err := ids.Parse(volumeID)
	if err != nil {
		t.Fatal(err)
	}
	vol := [16]byte(u)

	bucket, err := real.NewObjectStore(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("real.NewObjectStore: %v", err)
	}
	// The Agent gets the gate; every assertion reads the bucket underneath it, so what
	// the test observes is the object store and not the double's bookkeeping.
	gate := &gatedStore{Store: bucket}

	// The state the snapshot has to show. It is published with image.Publish — the same
	// call a volume's own stop makes — rather than by writing JSON, so this is an image
	// the Agent can load for the ordinary reason: a previous session left it.
	base := cow.NewIntervalMap()
	base.Overwrite(decoyOffset, bytes.Repeat([]byte{decoyByte}, decoyLength))
	base.Overwrite(guestRegionOffset, bytes.Repeat([]byte{fillerByte}, guestRegionLength))
	if _, err := image.Publish(ctx, bucket, rand.Reader, nil, image.OwnLineage(vol), base, nil, 0, ""); err != nil {
		t.Fatalf("publishing the image this volume boots from: %v", err)
	}

	m, sock := startAgentOn(t, ctx, dir, volumeID, gate, laneBudget())

	// Arm the gate before asking, not after: the reconcile path starts the upload inside
	// Apply, and a gate armed afterwards would be armed behind it.
	held := gate.holdNextChunkHead()

	snapshotID := ids.New().String()
	if err := m.Apply(ctx, []*storagev1.DesiredVolume{desiredVolume(volumeID, snapshotID)}); err != nil {
		t.Fatalf("asking for snapshot %s through the desired state: %v", snapshotID, err)
	}

	// The upload is now suspended at the store, which is the fact the rest of the test
	// rests on: Freeze has already run, and the guest's region has *not* been read yet.
	// Asserting which key it stopped on is what keeps that true — if uploadChunks ever
	// reads every range before its first store call, this says so instead of quietly
	// leaving the test with no window to write in.
	if got, want := held.await(t, liveTimeout), chunkKey(vol, bytes.Repeat([]byte{decoyByte}, decoyLength)); got != want {
		t.Fatalf("the snapshot upload stopped at %s, want the first chunk %s: this test needs the guest's region to be unread at this point",
			got, want)
	}

	// Now a real kernel, on a device the Agent is serving, writing into the region the
	// suspended upload is about to read.
	g := testinfra.StartLinuxGuest(t, sock, kernel, initramfs, testinfra.GuestHold)
	g.WaitForLine(t, testinfra.GuestAlive, testinfra.GuestBootTimeout)
	// Heartbeat 12: the hold loop covers its eight blocks in its first eight iterations,
	// so by then every byte the guest ever writes has been written at least once — with
	// the snapshot's copy still stopped at the store. Each heartbeat is printed after
	// that iteration's fsync and never before it, so this is a FLUSH this backend
	// answered rather than time passing.
	g.WaitForLine(t, fmt.Sprintf("%s %d ", testinfra.GuestAlive, 12), liveTimeout)

	held.release()

	waitUntil(t, liveTimeout, "the snapshot manifest to appear in the bucket", func() bool {
		_, err := bucket.Head(ctx, image.SnapshotKey(vol, snapshotID))
		return err == nil
	})

	// Read it the way a clone reads its nearest ancestor: the same load agent.parentView
	// performs at the top of its walk. (The symbol named here was `agent.cloneView`,
	// which has not existed for some time; parentView now layers one of these per link.)
	view, man, err := image.LoadSnapshot(ctx, bucket, nil, image.OwnLineage(vol), snapshotID)
	if err != nil {
		t.Fatalf("reading snapshot %s back: %v", snapshotID, err)
	}
	got := make([]byte, guestRegionLength)
	view.Read(guestRegionOffset, got)
	if off, ok := firstByteNot(got, fillerByte); ok {
		t.Fatalf("the snapshot carries a byte the guest wrote after it was frozen: at volume offset %d it holds %#x, and the point it was frozen at (sequence %d) held %#x.\n"+
			"§19 promises the copy is the volume at that sequence, not what the WAL held when the upload finished:\n%s",
			guestRegionOffset+off, got[off], man.Sequence, byte(fillerByte), testinfra.VerdictLines(g.Console()))
	}
	// The decoy too, which catches the other way of being wrong: a snapshot that froze
	// something other than this volume's view reads as zeros here.
	decoy := make([]byte, decoyLength)
	view.Read(decoyOffset, decoy)
	if off, ok := firstByteNot(decoy, decoyByte); ok {
		t.Fatalf("the snapshot lost what the volume held before the freeze: at offset %d it holds %#x, want %#x",
			decoyOffset+off, decoy[off], byte(decoyByte))
	}

	// And the half that keeps the assertion above from being vacuous. A guest that wrote
	// nothing — a wrong socket, a hold mode that moved its offsets, a device that
	// swallowed every request — leaves a snapshot full of filler as well, and everything
	// so far would pass. The volume's *own* image, published when it stops, is where those
	// writes must be.
	out := g.Stop(t, liveTimeout)
	if strings.Contains(out, "GUESTINIT-FAIL") {
		t.Fatalf("the guest failed while the snapshot was being taken:\n%s", testinfra.VerdictLines(out))
	}
	if err := m.Close(ctx); err != nil {
		t.Fatalf("stopping the Agent: %v", err)
	}
	live, _, _, err := image.Load(ctx, bucket, nil, image.OwnLineage(vol), nil)
	if err != nil {
		t.Fatalf("reading the volume's own image back: %v", err)
	}
	written := make([]byte, guestRegionLength)
	live.Read(guestRegionOffset, written)
	off, ok := firstByteNot(written, fillerByte)
	if !ok {
		t.Fatalf("the volume's own image holds nothing but filler in [%d,%d): the guest wrote nothing there, so the snapshot being pure filler proved nothing:\n%s",
			guestRegionOffset, guestRegionOffset+guestRegionLength, testinfra.VerdictLines(out))
	}
	t.Logf("the guest's writes are in the volume's image from offset %d and in none of the snapshot, which was frozen at sequence %d",
		guestRegionOffset+off, man.Sequence)
}

// firstByteNot reports the first offset in b that is not want, so a failure names the
// byte rather than saying the two blobs differ.
func firstByteNot(b []byte, want byte) (uint64, bool) {
	for i, got := range b {
		if got != want {
			return uint64(i), true
		}
	}
	return 0, false
}

// chunkKey is where image.uploadChunks puts a chunk holding exactly these bytes. The key
// is the digest of the plaintext (content-addressed, see internal/image), so the test can
// name a chunk without asking the code under test where it went.
func chunkKey(vol [16]byte, plain []byte) string {
	sum := sha256.Sum256(plain)
	return image.ChunksPrefix(vol) + hex.EncodeToString(sum[:])
}

// gatedStore is an object store that can be told to stop the next chunk Head it is asked
// for and hold it until the test lets go.
//
// Head and not Put, because a chunk whose bytes are already in the bucket is skipped
// rather than re-uploaded — the frozen copy of an unchanged region makes no Put at all —
// and Head is the one call every chunk makes.
//
// A chunk Head and not any Head, which the first run of this test settled: loading a
// volume's base image *does* Head, once, on `manifest.json`, for the ETag its own publish
// will CAS against. A gate that stopped at the first Head of any kind stopped there, held
// the volume's read view instead of its snapshot, and said so — which is what the
// assertion on the held key is for. Chunk keys are reached by exactly one path, the
// upload, and a guest's fsync is no object-store traffic whatsoever (INV-18).
//
// A fault-injecting store already exists in simio/sim, and this is not it: sim's faults
// make a call *fail*, and what this needs is a call that has not returned yet. Suspending
// the upload is the only way to make "the guest wrote while the copy was being made" a
// fact rather than a race against a machine that is fast on the day it is written.
type gatedStore struct {
	objectstore.Store

	mu   sync.Mutex
	hold chan struct{} // non-nil while armed; closed to let the held call proceed
	key  string        // the key of the call being held, once there is one
}

// holdNextChunkHead arms the gate and returns the handle the test drives it with.
func (g *gatedStore) holdNextChunkHead() *heldCall {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.hold, g.key = make(chan struct{}), ""
	return &heldCall{store: g, hold: g.hold}
}

func (g *gatedStore) Head(ctx context.Context, key string) (objectstore.ObjectInfo, error) {
	g.mu.Lock()
	hold := g.hold
	if hold != nil && !strings.HasPrefix(key, "chunks/") {
		hold = nil
	}
	if hold != nil {
		// Disarmed by the first arrival: everything after it goes straight through, so
		// releasing the gate releases the whole upload and not one call of it.
		g.hold, g.key = nil, key
	}
	g.mu.Unlock()
	if hold != nil {
		<-hold
	}
	return g.Store.Head(ctx, key)
}

// heldCall is one armed gate.
type heldCall struct {
	store *gatedStore
	hold  chan struct{}
}

// await blocks until a Head has arrived and been stopped, and returns its key.
func (h *heldCall) await(t *testing.T, timeout time.Duration) string {
	t.Helper()
	var key string
	waitUntil(t, timeout, "the snapshot's copy to reach the object store", func() bool {
		h.store.mu.Lock()
		defer h.store.mu.Unlock()
		key = h.store.key
		return key != ""
	})
	return key
}

// release lets the held call — and therefore the rest of the upload — proceed.
func (h *heldCall) release() { close(h.hold) }
