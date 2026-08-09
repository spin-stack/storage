package wal_test

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/crypto"
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
		if err := resumed.Read(uint64(i)*64, buf); err != nil {
			t.Fatalf("read: %v", err)
		}
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
	if err := resumed.Read(0, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
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

// captureSlog redirects the default logger into a buffer for the length of the test and
// returns it. Resume logs through the same package every binary in this tree logs
// through, so what this buffer holds is what an operator would have read in the Agent's
// output — the only place a discarded record can be noticed from outside the process.
func captureSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var out bytes.Buffer
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(restore) })
	return &out
}

// tornWorld is a volume that wrote `whole` records, synced, then wrote one more that a
// crash cut short. It returns the intact byte length of the WAL, the newest segment's
// name and how many bytes the torn record left behind.
func tornWorld(t *testing.T, whole int, cut int64) (w *resumeWorld, newest string, intact, discarded int64) {
	t.Helper()
	w = newResumeWorld(t, nil)
	for i := range whole {
		if _, err := w.log.Write(uint64(i)*64, []byte("a record that was really durable"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.log.Sync(); err != nil {
		t.Fatal(err)
	}
	intact = walSize(t, w.log)

	if _, err := w.log.Write(4096, []byte("the record the crash cut in half"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.log.Sync(); err != nil {
		t.Fatal(err)
	}
	full := walSize(t, w.log)

	names := w.log.SegmentNames()
	newest = names[len(names)-1]
	w.disk.TornTail(newest, int(full-cut))
	w.disk.Crash() // the page cache went with the process
	return w, newest, intact, full - cut - intact
}

// TestResumeSaysWhatATornTailDiscarded.
//
// Dropping a torn tail is the design — a partial record was never durable, INV-05 says
// replay yields the intact prefix — but the *silence* around it is not. Reproduced on a
// real Agent: truncate the newest segment 50 bytes into its last record and restart, and
// the Agent replays the prefix, physically trims the file, serves the volume and prints
// six lines with no occurrence of torn, truncated, dropped or WARN. The only component
// that noticed a record had been thrown away was the tenant's guest, reporting a
// read-back mismatch.
//
// This asserts on the process's output, not on a counter inside the log: the operator
// reads stderr, and a field nobody prints is the same as no field.
func TestResumeSaysWhatATornTailDiscarded(t *testing.T) {
	logged := captureSlog(t)
	w, newest, intact, discarded := tornWorld(t, 2, 10)

	resumed, err := wal.Resume(w.disk, "wal", w.clk, w.vol, 1, 0,
		wal.Limits{MaxUnflushedBytes: 1 << 20}, nil)
	if err != nil {
		t.Fatalf("a torn tail is the normal crash case, not an error: %v", err)
	}
	if got := resumed.Watermarks().Local; got != 2 {
		t.Fatalf("resumed local = %d, want 2 — the fixture did not tear a record", got)
	}

	out := logged.String()
	if !strings.Contains(out, "level=WARN") {
		t.Fatalf("a resume that threw a record away logged no WARN.\nlogged:\n%s", out)
	}
	for _, want := range []string{
		"discarded_bytes=" + fmt.Sprint(discarded), // how much was thrown away
		"stopped_at_offset=" + fmt.Sprint(intact),  // where parsing stopped
		"segment=" + newest,                        // which file
		"volume_id=" + format.UUIDString(w.vol),    // which volume
		"epoch=1",                                  // which epoch
		"recovered_sequence=2",                     // how far the WAL actually got
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the resume line does not name %q.\nlogged:\n%s", want, out)
		}
	}
}

// TestACleanResumeSaysNothing: the WARN above is only worth having if it means
// something. A WAL that ended on a whole record lost nothing, so it must not print a
// line that says it did — an operator who sees this warning after every restart stops
// reading it, which is the same silence with more output.
func TestACleanResumeSaysNothing(t *testing.T) {
	logged := captureSlog(t)
	w := newResumeWorld(t, nil)
	for i := range 3 {
		if _, err := w.log.Write(uint64(i)*64, []byte("record"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.log.Sync(); err != nil {
		t.Fatal(err)
	}
	w.disk.Crash()

	if _, err := wal.Resume(w.disk, "wal", w.clk, w.vol, 1, 0,
		wal.Limits{MaxUnflushedBytes: 1 << 20}, nil); err != nil {
		t.Fatal(err)
	}
	if out := logged.String(); strings.Contains(out, "level=WARN") {
		t.Fatalf("a resume that lost nothing warned anyway.\nlogged:\n%s", out)
	}
}

// TestResumeReportsTheSequenceItRecovered.
//
// The Agent ACKed sequence 3 to its guest and told the Control Plane so;
// durable_sequence=3 is in Postgres right now. After the crash the local WAL holds 2.
// Nothing in the process compares those two numbers, because until this the resumed log
// had no way to say the second one: Watermarks().Local is max(the floor it was handed,
// what replay found), so a caller cannot tell "the WAL held 2" from "the WAL held
// nothing and the floor was 2".
//
// This is the accessor that comparison needs. The comparison itself belongs to the
// caller that knows the catalog's number.
func TestResumeReportsTheSequenceItRecovered(t *testing.T) {
	w, newest, intact, discarded := tornWorld(t, 2, 10)

	resumed, err := wal.Resume(w.disk, "wal", w.clk, w.vol, 1, 0,
		wal.Limits{MaxUnflushedBytes: 1 << 20}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rep := resumed.ResumeReport()
	if !rep.Resumed {
		t.Error("a log built by Resume does not report itself as resumed")
	}
	if rep.RecoveredSequence != 2 {
		t.Errorf("recovered sequence = %d, want 2 (the last whole record on the device)", rep.RecoveredSequence)
	}
	if rep.Records != 2 {
		t.Errorf("records replayed = %d, want 2", rep.Records)
	}
	if !rep.TornTail {
		t.Error("the report does not say the tail was cut")
	}
	if rep.TornSegment != newest {
		t.Errorf("torn segment = %q, want %q", rep.TornSegment, newest)
	}
	if rep.StoppedAtOffset != intact {
		t.Errorf("stopped-at offset = %d, want %d", rep.StoppedAtOffset, intact)
	}
	if rep.DiscardedBytes != discarded {
		t.Errorf("discarded bytes = %d, want %d", rep.DiscardedBytes, discarded)
	}

	// What the caller does with it: the catalog's durable_sequence is above what the
	// device still holds, and that is the condition a caller must refuse to serve on.
	const catalogDurable = 3
	if rep.RecoveredSequence >= catalogDurable {
		t.Fatalf("the fixture did not lose an acknowledged record: recovered %d >= catalog %d",
			rep.RecoveredSequence, catalogDurable)
	}

	// And the reason this cannot be read off Watermarks(). Resume the same torn WAL
	// with a floor of 9 — what a caller passes when the object store is known to
	// reproduce up to 9 — and the local watermark is 9 while the device still holds 2.
	// A report that echoed the watermark, or the floor, would say the volume is whole.
	w2, _, _, _ := tornWorld(t, 2, 10)
	high, err := wal.Resume(w2.disk, "wal", w2.clk, w2.vol, 1, 9,
		wal.Limits{MaxUnflushedBytes: 1 << 20}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := high.Watermarks().Local; got != 9 {
		t.Fatalf("local watermark = %d, want 9 — the fixture does not set up the confusion", got)
	}
	if got := high.ResumeReport().RecoveredSequence; got != 2 {
		t.Fatalf("recovered sequence = %d, want 2: the report is echoing the watermark, "+
			"which is exactly the number that cannot answer this question", got)
	}
}

// TestResumeSaysItRemovedASegmentStub: a crash between creating a segment file and its
// header reaching the device leaves a file too short to classify. Resume removes it,
// correctly — a header is synced before the segment is used, so it carried no records —
// and unlinking a file out of a volume's WAL directory without a word is the same
// silence as dropping the torn tail was.
func TestResumeSaysItRemovedASegmentStub(t *testing.T) {
	logged := captureSlog(t)
	w := newResumeWorld(t, nil)
	stub := wal.SegmentDir("wal", w.vol, 1) + "/000000000000000001.seg"
	f, err := w.disk.Create(stub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Append(make([]byte, 20)); err != nil { // short of a 64-byte header
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := wal.Resume(w.disk, "wal", w.clk, w.vol, 1, 0,
		wal.Limits{MaxUnflushedBytes: 1 << 20}, nil); err != nil {
		t.Fatalf("a header-less stub is the normal crash case, not an error: %v", err)
	}
	if gone, err := w.disk.Exists(stub); err != nil || gone {
		t.Fatalf("the stub is still there (exists=%v, err=%v) — the fixture did not exercise the removal", gone, err)
	}
	out := logged.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "discarded_bytes=20") ||
		!strings.Contains(out, "segment="+stub) {
		t.Fatalf("removing a segment out of the WAL directory said nothing useful.\nlogged:\n%s", out)
	}
}

// TestAFreshLogIsNotAResumedOne: a log that never replayed anything must not report a
// recovered sequence, or the caller's comparison against the catalog fires on every new
// volume.
func TestAFreshLogIsNotAResumedOne(t *testing.T) {
	l, _, _ := newLog(t, wal.Limits{MaxUnflushedBytes: 1 << 20})
	if rep := l.ResumeReport(); rep.Resumed || rep.RecoveredSequence != 0 || rep.TornTail {
		t.Fatalf("a fresh log reports %+v, want the zero report", rep)
	}
}

// TestAJunkTailNamesTheOffsetToTruncateTo.
//
// Bytes that are not a partial record — a stray append, a device that returned garbage —
// are not a torn tail: replay stops there, every record after them is unreachable, and
// resume fails. It must keep failing; shortening the WAL silently is the thing this
// blocker exists to stop. What was missing is the way out. The reconcile loop retried
// `wal/format: bad magic` every 5s forever and named no file, no offset and no repair,
// so the volume was wedged with no operator action available.
//
// The proof is not that the message contains a number: it is that truncating to the
// number the message names brings the volume back.
func TestAJunkTailNamesTheOffsetToTruncateTo(t *testing.T) {
	w := newResumeWorld(t, nil)
	for i := range 3 {
		if _, err := w.log.Write(uint64(i)*64, []byte("record"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.log.Sync(); err != nil {
		t.Fatal(err)
	}
	clean := walSize(t, w.log)
	names := w.log.SegmentNames()
	newest := names[len(names)-1]

	junk := appendToSegment(t, w.disk, newest, bytes.Repeat([]byte{0xA5}, 3000))

	_, err := wal.Resume(w.disk, "wal", w.clk, w.vol, 1, 0,
		wal.Limits{MaxUnflushedBytes: 1 << 20}, nil)
	if err == nil {
		t.Fatal("a WAL whose tail is unreadable resumed anyway, silently shortening the volume")
	}
	if !errors.Is(err, format.ErrBadMagic) {
		t.Fatalf("the failure changed shape: %v", err)
	}
	if !strings.Contains(err.Error(), newest) {
		t.Errorf("the error does not name the segment to repair: %v", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprint(clean)) {
		t.Errorf("the error does not name the offset to truncate to (%d): %v", clean, err)
	}
	if !strings.Contains(err.Error(), fmt.Sprint(junk)) {
		t.Errorf("the error does not say how many bytes truncating would discard (%d): %v", junk, err)
	}

	// An operator follows it: the repair uses the number the error printed, not the one
	// this test computed, so an error that names a plausible-looking wrong offset fails
	// here rather than passing on a substring match.
	truncateSegment(t, w.disk, newest, offsetFromRepair(t, err))
	resumed, err := wal.Resume(w.disk, "wal", w.clk, w.vol, 1, 0,
		wal.Limits{MaxUnflushedBytes: 1 << 20}, nil)
	if err != nil {
		t.Fatalf("truncating to the offset the error named did not repair the WAL: %v", err)
	}
	if got := resumed.Watermarks().Local; got != 3 {
		t.Fatalf("after the repair local = %d, want 3 (every whole record is back)", got)
	}
}

// offsetFromRepair reads the byte length the error tells an operator to truncate to.
var repairRE = regexp.MustCompile(`truncating it to (\d+) bytes`)

func offsetFromRepair(t *testing.T, err error) int64 {
	t.Helper()
	m := repairRE.FindStringSubmatch(err.Error())
	if m == nil {
		t.Fatalf("the error names no truncation length an operator could act on: %v", err)
	}
	n, perr := strconv.ParseInt(m[1], 10, 64)
	if perr != nil {
		t.Fatal(perr)
	}
	return n
}

// appendToSegment appends b to a segment file and makes it durable, returning len(b).
func appendToSegment(t *testing.T, d *sim.Disk, name string, b []byte) int {
	t.Helper()
	f, err := d.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck // test fixture
	if _, err := f.Append(b); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	return len(b)
}

// truncateSegment is the operator's repair: cut the file to the offset the error named.
func truncateSegment(t *testing.T, d *sim.Disk, name string, size int64) {
	t.Helper()
	f, err := d.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck // test fixture
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
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
