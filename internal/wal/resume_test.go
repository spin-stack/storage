package wal_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// TestNewLogOverANonEmptyWALIsRefused: NewLog always starts the sequence space at
// the boundary it was given, so building one over a WAL that already holds records
// re-issues sequences 1..N under the same (volume, epoch). Under encryption that is
// GCM nonce reuse (§15.2 derives the nonce from volume/epoch/sequence, INV-15), and
// in the bucket it is two different objects claiming the same span (INV-21). It also
// serves an empty read view for data that is in the WAL and in S3.
//
// A constructor that cannot see the file's contents must not be the one that decides
// this: the first append is where the damage starts, so that is where it fails
// closed.
func TestNewLogOverANonEmptyWALIsRefused(t *testing.T) {
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	vol := [16]byte{11}
	l1 := wal.NewLog(d, "wal", clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	for i := range 3 {
		if _, err := l1.Write(uint64(i)*64, []byte("pre-crash"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := l1.Sync(); err != nil {
		t.Fatal(err)
	}
	sizeBefore := walSize(t, l1)

	// The agent restarts and re-attaches at the same epoch.
	l2 := wal.NewLog(d, "wal", clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	seq, err := l2.Write(4096, []byte("post-restart"), 0)
	if err == nil {
		t.Fatalf("a log rebuilt over a non-empty WAL wrote sequence %d, re-issuing the sequence space", seq)
	}

	// Nothing was appended: the rejected write must not leave a record behind.
	sizeAfter := walSize(t, l1)
	if sizeAfter != sizeBefore {
		t.Fatalf("refused write still appended %d bytes", sizeAfter-sizeBefore)
	}
}

// resumeWorld is a volume that wrote, uploaded part of its WAL, and then lost the
// process that owned it.
type resumeWorld struct {
	clk   *sim.Clock
	disk  *sim.Disk
	store *sim.ObjectStore
	vol   [16]byte
	log   *wal.Log
}

func newResumeWorld(t *testing.T, enc *wal.Encryption) *resumeWorld {
	t.Helper()
	w := &resumeWorld{
		clk:   sim.NewClock(time.Unix(1_700_000_000, 0).UTC()),
		disk:  sim.NewDisk(),
		store: sim.NewObjectStore(),
		vol:   [16]byte{21},
	}
	w.log = wal.NewLog(w.disk, "wal", w.clk, w.vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	w.log.EnableRemote(
		wal.NewBatcher(w.clk, w.vol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(w.store, 5),
		leaseOK{},
	)
	if enc != nil {
		w.log.EnableEncryption(enc)
	}
	return w
}

// TestResumeContinuesTheSequenceSpaceAndTheView: the restart the previous test
// refuses to fake. A resumed log must know exactly where the WAL ends — its first
// new sequence is last+1 — and must serve the data that is still in the file. A log
// that restarts at 1 re-uses (volume, epoch, sequence), which is both the duplicate
// object span of INV-21 and the reused GCM nonce of §15.2.
func TestResumeContinuesTheSequenceSpaceAndTheView(t *testing.T) {
	w := newResumeWorld(t, nil)
	payloads := []string{"first-record", "second-record", "third-record"}
	for i, p := range payloads {
		if _, err := w.log.Write(uint64(i)*64, []byte(p), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.log.Sync(); err != nil {
		t.Fatal(err)
	}

	resumed, err := wal.Resume(w.disk, "wal", w.clk, w.vol, 1, 0,
		wal.Limits{MaxUnflushedBytes: 1 << 20}, nil)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if got := resumed.Watermarks().Local; got != 3 {
		t.Fatalf("resumed local watermark = %d, want 3", got)
	}
	for i, p := range payloads {
		buf := make([]byte, len(p))
		resumed.Read(uint64(i)*64, buf)
		if string(buf) != p {
			t.Fatalf("resumed read at %d = %q, want %q", i*64, buf, p)
		}
	}
	seq, err := resumed.Write(4096, []byte("after-the-restart"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 4 {
		t.Fatalf("first sequence after the restart = %d, want 4 (last_local+1)", seq)
	}
}

// TestResumeRebuildsAnEncryptedView: the resumed view is plaintext for an encrypted
// volume — the ciphertext is what is on disk, never what a guest read returns
// (INV-15).
func TestResumeRebuildsAnEncryptedView(t *testing.T) {
	dek, err := crypto.GenerateDEK(&ramp{b: 21}, 3)
	if err != nil {
		t.Fatal(err)
	}
	w := newResumeWorld(t, &wal.Encryption{DEK: dek, VolumeID: [16]byte{21}})
	secret := []byte("guest-secret-bytes")
	if _, err := w.log.Write(0, secret, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := w.log.Discard(64, 32); err != nil {
		t.Fatal(err)
	}
	if err := w.log.Sync(); err != nil {
		t.Fatal(err)
	}

	enc := &wal.Encryption{DEK: dek, VolumeID: w.vol}
	resumed, err := wal.Resume(w.disk, "wal", w.clk, w.vol, 1, 0,
		wal.Limits{MaxUnflushedBytes: 1 << 20}, enc)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	buf := make([]byte, len(secret))
	resumed.Read(0, buf)
	if !bytes.Equal(buf, secret) {
		t.Fatalf("resumed read = %q, want the plaintext %q", buf, secret)
	}
	if resumed.Watermarks().Local != 2 {
		t.Fatalf("resumed local = %d, want 2", resumed.Watermarks().Local)
	}
}

// misfileWAL moves every segment of (fromVol, fromEpoch) into the directory of
// (toVol, toEpoch), keeping the file names. It is the restored-backup / reused-
// directory / path-bug case in one operation: the segments are exactly where this
// volume's WAL is expected to be, and only their headers say otherwise.
func misfileWAL(t *testing.T, d *sim.Disk, fromVol [16]byte, fromEpoch uint64, toVol [16]byte, toEpoch uint64) {
	t.Helper()
	names, err := wal.SegmentFiles(d, "wal", fromVol, fromEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) == 0 {
		t.Fatal("nothing to misfile: the source WAL has no segments")
	}
	to := wal.SegmentDir("wal", toVol, toEpoch)
	for _, name := range names {
		base := name[strings.LastIndex(name, "/")+1:]
		if err := d.Rename(name, to+"/"+base); err != nil {
			t.Fatal(err)
		}
	}
}

// TestResumeRefusesRecordsFromAnotherEpoch: the epoch is in the WAL's path, so a log
// resuming epoch N never reads epoch M's directory — that is the point of putting it
// there. What is still possible is a directory *filed* under this epoch whose
// segments belong to another one: a restored backup, a reused directory, a path bug.
// The segment header says what the file is, the path says where it was filed, and
// when they disagree the resume fails closed before a record is decoded.
func TestResumeRefusesRecordsFromAnotherEpoch(t *testing.T) {
	w := newResumeWorld(t, nil)
	if _, err := w.log.Write(0, []byte("epoch-1 record"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.log.Sync(); err != nil {
		t.Fatal(err)
	}
	misfileWAL(t, w.disk, w.vol, 1, w.vol, 9)

	if _, err := wal.Resume(w.disk, "wal", w.clk, w.vol, 9, 0,
		wal.Limits{MaxUnflushedBytes: 1 << 20}, nil); !errors.Is(err, wal.ErrForeignEpoch) {
		t.Fatalf("resuming epoch 9 over epoch 1's segments must fail closed, got %v", err)
	}
}

// TestResumeRefusesAnotherVolumesRecords: the same fail-closed check as the epoch
// one, on the identity that has no other guard. An encrypted volume's payloads are
// bound to their volume by the GCM AAD, but DISCARD and WRITE_ZEROES carry no payload
// and a plaintext volume carries no tag — nothing else would notice.
func TestResumeRefusesAnotherVolumesRecords(t *testing.T) {
	w := newResumeWorld(t, nil) // volume {21}
	if _, err := w.log.Write(0, []byte("volume-21's data"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.log.Sync(); err != nil {
		t.Fatal(err)
	}
	other := [16]byte{99}
	misfileWAL(t, w.disk, w.vol, 1, other, 1)

	if _, err := wal.Resume(w.disk, "wal", w.clk, other, 1, 0,
		wal.Limits{MaxUnflushedBytes: 1 << 20}, nil); !errors.Is(err, wal.ErrForeignVolume) {
		t.Fatalf("resuming volume %x over volume %x's segments must fail closed, got %v", other, w.vol, err)
	}
}

// TestResumeUploadsOnlyTheTailS3NeverGot: the records between the durable point and
// the end of the WAL exist only on this host. A resume that forgets them loses every
// write since the last successful upload; a resume that re-uploads the whole file
// re-issues spans S3 already has, which is the divergent-object case of INV-21. Only
// the tail moves.
func TestResumeUploadsOnlyTheTailS3NeverGot(t *testing.T) {
	ctx := t.Context()
	w := newResumeWorld(t, nil)
	if _, err := w.log.Write(0, []byte("durable-one"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := w.log.Write(64, []byte("durable-two"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.log.Flush(ctx); err != nil { // sequences 1..2 reach S3
		t.Fatal(err)
	}
	if _, err := w.log.Write(128, []byte("only-on-this-host"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.log.Sync(); err != nil {
		t.Fatal(err)
	}
	durable := w.log.Watermarks().Durable
	objectsBefore, _ := w.store.List(ctx, "wal/")

	resumed, err := wal.Resume(w.disk, "wal", w.clk, w.vol, 1, durable,
		wal.Limits{MaxUnflushedBytes: 1 << 20}, nil)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	resumed.EnableRemote(
		wal.NewBatcher(w.clk, w.vol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(w.store, 5),
		leaseOK{},
	)
	if err := resumed.Flush(ctx); err != nil {
		t.Fatalf("the resumed tail must be uploadable: %v", err)
	}

	prefix, err := recovery.DurablePrefix(ctx, w.store, w.vol, 1)
	if err != nil {
		t.Fatal(err)
	}
	if prefix != 3 {
		t.Fatalf("S3 reproduces up to %d after the resume, want 3", prefix)
	}
	objectsAfter, _ := w.store.List(ctx, "wal/")
	if len(objectsAfter) != len(objectsBefore)+1 {
		t.Fatalf("resume produced %d new objects, want exactly the one covering the tail",
			len(objectsAfter)-len(objectsBefore))
	}
}

// TestResumeStopsAtATornTail: a crash mid-append leaves a partial record. Replay
// yields the intact prefix (INV-05), so the resumed log continues after the last
// *whole* record — it must not adopt the torn one or skip past it.
func TestResumeStopsAtATornTail(t *testing.T) {
	w := newResumeWorld(t, nil)
	for i := range 3 {
		if _, err := w.log.Write(uint64(i)*64, []byte("record"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.log.Sync(); err != nil {
		t.Fatal(err)
	}
	// The third record is cut short in the newest (and only) segment, which is the
	// one place a torn tail is allowed.
	newest := w.log.SegmentNames()[len(w.log.SegmentNames())-1]
	size := walSize(t, w.log)
	w.disk.TornTail(newest, int(size)-10)
	w.disk.Crash() // the page cache is gone with the process

	resumed, err := wal.Resume(w.disk, "wal", w.clk, w.vol, 1, 0,
		wal.Limits{MaxUnflushedBytes: 1 << 20}, nil)
	if err != nil {
		t.Fatalf("a torn tail is the normal crash case, not an error: %v", err)
	}
	if got := resumed.Watermarks().Local; got != 2 {
		t.Fatalf("resumed local = %d, want 2 (the last intact record)", got)
	}
}

// TestRemoteGapBackpressureIsExplicit is §5.7/INV-04 for the backlog that actually
// matters: MaxUnflushedBytes is cleared by fdatasync, so on a `local` volume (or a
// remote one during an S3 outage where anything calls Sync) nothing bounds the bytes
// that only this host holds. The guest must get an explicit error rather than the
// host silently filling NVMe with writes no other machine has.
func TestRemoteGapBackpressureIsExplicit(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	l := wal.NewLog(d, "wal", clk, [16]byte{22}, 1, wal.Limits{
		MaxUnflushedBytes: 1 << 20, // roomy: this is not the limit under test
		MaxRemoteGapBytes: 400,     // ~2 records (104-byte header + payload)
	})
	l.SetDurabilityMode(wal.ModeLocal)

	var lastErr error
	for i := range 10 {
		if _, err := l.Write(uint64(i)*64, make([]byte, 64), 0); err != nil {
			lastErr = err
			break
		}
		if err := l.Sync(); err != nil { // fdatasync clears the *unflushed* accounting
			t.Fatal(err)
		}
		if err := l.Flush(ctx); err != nil { // a local-mode ACK: still nothing in S3
			t.Fatal(err)
		}
	}
	if !errors.Is(lastErr, wal.ErrBackpressure) {
		t.Fatalf("the un-remote-durable backlog grew unbounded; last write error: %v", lastErr)
	}
}

// TestWriteRejectsTheFUAFlag: the flag carries the FLUSH ACK contract, and Write
// implements none of it (§14.3.1, §14.8). Swallowing it silently is what makes a
// host-page-cache write look like a durable one.
func TestWriteRejectsTheFUAFlag(t *testing.T) {
	l, _, _ := newLog(t, wal.Limits{MaxUnflushedBytes: 1 << 20})
	if _, err := l.Write(0, []byte("fua"), format.FlagFUA); !errors.Is(err, wal.ErrFUAOnWrite) {
		t.Fatalf("want ErrFUAOnWrite, got %v", err)
	}
}
