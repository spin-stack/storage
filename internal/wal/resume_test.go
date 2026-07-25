package wal_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// reopen returns a second handle on the same WAL file, which is what an agent
// restart has: the file is still there, full of records, and the process that knew
// the watermarks is gone (§16 ATTACHING→ACTIVE validates the epoch, it does not
// bump it).
func reopen(t *testing.T, d *sim.Disk, name string) disk.File {
	t.Helper()
	f, err := d.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

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
	f, err := d.Create("wal/active.wal")
	if err != nil {
		t.Fatal(err)
	}
	vol := [16]byte{11}
	l1 := wal.NewLog(f, clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	for i := range 3 {
		if _, err := l1.Write(uint64(i)*64, []byte("pre-crash"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := l1.Sync(); err != nil {
		t.Fatal(err)
	}
	sizeBefore, _ := f.Size()

	// The agent restarts and re-attaches at the same epoch.
	l2 := wal.NewLog(reopen(t, d, "wal/active.wal"), clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	seq, err := l2.Write(4096, []byte("post-restart"), 0)
	if err == nil {
		t.Fatalf("a log rebuilt over a non-empty WAL wrote sequence %d, re-issuing the sequence space", seq)
	}

	// Nothing was appended: the rejected write must not leave a record behind.
	sizeAfter, _ := f.Size()
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
	f, err := w.disk.Create("wal/active.wal")
	if err != nil {
		t.Fatal(err)
	}
	w.log = wal.NewLog(f, w.clk, w.vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
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

	resumed, err := wal.Resume(reopen(t, w.disk, "wal/active.wal"), w.clk, w.vol, 1, 0,
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
	resumed, err := wal.Resume(reopen(t, w.disk, "wal/active.wal"), w.clk, w.vol, 1, 0,
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

// TestResumeRefusesRecordsFromAnotherEpoch: a WAL file replayed under the wrong
// identity is the restored-backup / reused-directory case. A record from another
// epoch must never be adopted as this epoch's — it would be applied to the guest's
// extents and counted in this epoch's sequence space.
func TestResumeRefusesRecordsFromAnotherEpoch(t *testing.T) {
	w := newResumeWorld(t, nil)
	if _, err := w.log.Write(0, []byte("epoch-1 record"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.log.Sync(); err != nil {
		t.Fatal(err)
	}

	if _, err := wal.Resume(reopen(t, w.disk, "wal/active.wal"), w.clk, w.vol, 9, 0,
		wal.Limits{MaxUnflushedBytes: 1 << 20}, nil); !errors.Is(err, wal.ErrForeignEpoch) {
		t.Fatalf("resuming epoch 9 over an epoch-1 WAL must fail closed, got %v", err)
	}
}

// TestResumeUploadsOnlyTheTailS3NeverGot: the records between the durable point and
// the end of the WAL exist only on this host. A resume that forgets them loses every
// write since the last successful upload; a resume that re-uploads the whole file
// re-issues spans S3 already has, which is the divergent-object case of INV-21. Only
// the tail moves.
func TestResumeUploadsOnlyTheTailS3NeverGot(t *testing.T) {
	ctx := context.Background()
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

	resumed, err := wal.Resume(reopen(t, w.disk, "wal/active.wal"), w.clk, w.vol, 1, durable,
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
	f, _ := w.disk.Open("wal/active.wal")
	size, _ := f.Size()
	w.disk.TornTail("wal/active.wal", int(size)-10) // the third record is cut short

	resumed, err := wal.Resume(reopen(t, w.disk, "wal/active.wal"), w.clk, w.vol, 1, 0,
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
	ctx := context.Background()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	f, _ := d.Create("wal/local.wal")
	l := wal.NewLog(f, clk, [16]byte{22}, 1, wal.Limits{
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
