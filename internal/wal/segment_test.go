package wal_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// segVol is the volume every test in this file writes as.
var segVol = [16]byte{0x5e, 0x60}

// segLog builds a log whose segments seal at segBytes, so a boundary is a few records
// away rather than 32 MiB away.
func segLog(t *testing.T, d *sim.Disk, root string, segBytes int64) *wal.Log {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	return wal.NewLog(d, root, clk, segVol, 1, wal.Limits{MaxUnflushedBytes: 1 << 30, SegmentBytes: segBytes})
}

// TestReclaimUnlinksTheSegmentsBelowThePublishedPoint is the test that fails against
// the pre-segment WAL, and the reason the whole increment exists.
//
// `TruncateLocal` used to record `truncatedUpTo` and call `file.Truncate(0)` only when
// `upTo >= local` — only when the checkpoint had reached the very end of the log. On a
// volume that is being written to that never happens, so the reclaim path was a no-op
// and every defence against a full device sat downstream of it.
//
// Five sealed segments, the published point inside the third: exactly the first two go,
// the third is kept whole (it holds records above the published point, which exist on
// this host alone), and the device gets the bytes back.
func TestReclaimUnlinksTheSegmentsBelowThePublishedPoint(t *testing.T) {
	d := sim.NewDisk()
	d.SetDeviceBudget(1 << 20)
	l := segLog(t, d, "wal", 512)

	// Three ~130-byte records per segment, six segments' worth of writes; the seal is
	// explicit so the arithmetic does not depend on the record size.
	const segs, perSeg = 6, 3
	for g := range segs {
		for j := range perSeg {
			seq := g*perSeg + j
			if _, err := l.Write(uint64(seq)*4096, fmt.Appendf(nil, "record-%02d", seq), 0); err != nil {
				t.Fatalf("write %d: %v", seq, err)
			}
		}
		if g < segs-1 {
			if err := l.Seal(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if got := len(l.SegmentNames()); got != segs {
		t.Fatalf("staged %d segments, want %d", got, segs)
	}
	before := l.SegmentNames()
	usedBefore, err := d.Usage()
	if err != nil {
		t.Fatal(err)
	}

	// The checkpoint publishes a point inside the third segment (records 7..9).
	const published = 8
	if err := l.AdvanceDurable(segs * perSeg); err != nil {
		t.Fatal(err)
	}
	if err := l.AdvancePublished(published); err != nil {
		t.Fatal(err)
	}
	if err := l.TruncateLocal(published); err != nil {
		t.Fatalf("truncate to the published point: %v", err)
	}

	after := l.SegmentNames()
	if len(after) != segs-2 {
		t.Fatalf("truncation left %d segments, want %d (the first two are entirely below %d)",
			len(after), segs-2, published)
	}
	if after[0] != before[2] {
		t.Fatalf("the oldest surviving segment is %s, want %s (the one holding sequence %d)",
			after[0], before[2], published)
	}
	usedAfter, err := d.Usage()
	if err != nil {
		t.Fatal(err)
	}
	if usedAfter.UsedBytes >= usedBefore.UsedBytes {
		t.Fatalf("the device still holds %d bytes (was %d): the truncation reclaimed nothing",
			usedAfter.UsedBytes, usedBefore.UsedBytes)
	}
	if got := l.ReclaimedBytes(); got != usedBefore.UsedBytes-usedAfter.UsedBytes {
		t.Fatalf("the log reports %d bytes reclaimed, the device gave back %d",
			got, usedBefore.UsedBytes-usedAfter.UsedBytes)
	}

	// The segment holding the published point is intact: every record from its first
	// through the last write is still replayable, in order.
	recs, err := wal.ReplaySegments(d, "wal", segVol, 1)
	if err != nil {
		t.Fatalf("replay after the truncation: %v", err)
	}
	if recs[0].Sequence != 7 {
		t.Fatalf("the WAL now starts at sequence %d, want 7 (the third segment's first record)", recs[0].Sequence)
	}
	if last := recs[len(recs)-1].Sequence; last != segs*perSeg {
		t.Fatalf("the WAL ends at %d, want %d", last, segs*perSeg)
	}
	for i := 1; i < len(recs); i++ {
		if recs[i].Sequence != recs[i-1].Sequence+1 {
			t.Fatalf("hole after the truncation: %d follows %d", recs[i].Sequence, recs[i-1].Sequence)
		}
	}
}

// TestReclaimKeepsTheNewestSegmentEvenWhenItIsEntirelyBelowThePoint: the newest
// segment is the one that may still be appended to and the only one allowed to end in
// a torn record. Unlinking it would put a later crash's torn tail in a file that is no
// longer the newest, which the next replay would correctly call corruption.
func TestReclaimKeepsTheNewestSegmentEvenWhenItIsEntirelyBelowThePoint(t *testing.T) {
	d := sim.NewDisk()
	l := segLog(t, d, "wal", 512)
	for i := range 3 {
		if _, err := l.Write(uint64(i)*4096, []byte("record"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.AdvanceDurable(3); err != nil {
		t.Fatal(err)
	}
	if err := l.AdvancePublished(3); err != nil {
		t.Fatal(err)
	}
	if err := l.TruncateLocal(3); err != nil {
		t.Fatal(err)
	}
	if got := len(l.SegmentNames()); got != 1 {
		t.Fatalf("the newest segment was unlinked: %d segments left, want 1", got)
	}
}

// plantSegment writes a segment file directly, so a test can stage a directory a bug
// or a restore would produce and no correct writer ever would.
func plantSegment(t *testing.T, d *sim.Disk, root string, h format.SegmentHeader, body []byte, name string) {
	t.Helper()
	if name == "" {
		name = fmt.Sprintf("%018d.seg", h.FirstSequence)
	}
	full := wal.SegmentDir(root, segVol, 1) + "/" + name
	f, err := d.Create(full)
	if err != nil {
		t.Fatal(err)
	}
	hb, err := h.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Append(append(hb, body...)); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// segBody encodes count records starting at first, all WRITEs of the same shape.
func segBody(t *testing.T, first uint64, count int, vol [16]byte, epoch uint64) []byte {
	t.Helper()
	var recs []wal.Record
	for i := range count {
		seq := first + uint64(i)
		payload := fmt.Appendf(nil, "rec-%03d", seq)
		recs = append(recs, wal.Record{
			Type: format.RecordWrite, VolumeID: vol, Epoch: epoch, Sequence: seq,
			Offset: seq * 4096, Length: uint32(len(payload)), Payload: payload,
		})
	}
	b, err := wal.Serialize(recs)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestSegmentDirectoryFailureCases is the table the format review asks for: what a
// resume must accept, and what it must refuse rather than answer with fewer records.
//
// The dividing line is not "how bad does it look" but "could a correct writer have
// produced this". A crash mid-append can only ever tear the newest segment, because
// that is the only one open; anything else — a tear in a sealed file, a hole in the
// middle of the directory, a header naming another volume — means the returned records
// would differ from what was written, and INV-05 says that is an error, never a
// shorter answer.
func TestSegmentDirectoryFailureCases(t *testing.T) {
	const epoch = 1
	other := [16]byte{0xff, 0xee}

	tests := []struct {
		name string
		// plant lays out the directory. It runs on an empty disk.
		plant func(t *testing.T, d *sim.Disk)
		// wantErr is the sentinel a resume must fail with, or nil for a directory it
		// must accept.
		wantErr error
		// wantRecords is the number of records an accepted directory yields.
		wantRecords int
	}{
		{
			name: "a torn tail in the newest segment is the ordinary crash",
			plant: func(t *testing.T, d *sim.Disk) {
				body := segBody(t, 1, 3, segVol, epoch)
				plantSegment(t, d, "wal", format.SegmentHeader{VolumeID: segVol, Epoch: epoch, FirstSequence: 1},
					body[:len(body)-12], "") // the third record is cut short
			},
			wantRecords: 2,
		},
		{
			name: "a torn record in a segment that is not the newest is corruption",
			plant: func(t *testing.T, d *sim.Disk) {
				body := segBody(t, 1, 3, segVol, epoch)
				plantSegment(t, d, "wal", format.SegmentHeader{VolumeID: segVol, Epoch: epoch, FirstSequence: 1},
					body[:len(body)-12], "")
				plantSegment(t, d, "wal", format.SegmentHeader{VolumeID: segVol, Epoch: epoch, FirstSequence: 4},
					segBody(t, 4, 2, segVol, epoch), "")
			},
			wantErr: wal.ErrTornSegment,
		},
		{
			name: "a missing middle segment is a hole, not a short answer",
			plant: func(t *testing.T, d *sim.Disk) {
				plantSegment(t, d, "wal", format.SegmentHeader{VolumeID: segVol, Epoch: epoch, FirstSequence: 1},
					segBody(t, 1, 3, segVol, epoch), "")
				// sequences 4..6 would live here; the file is gone
				plantSegment(t, d, "wal", format.SegmentHeader{VolumeID: segVol, Epoch: epoch, FirstSequence: 7},
					segBody(t, 7, 3, segVol, epoch), "")
			},
			wantErr: wal.ErrSegmentGap,
		},
		{
			name: "a header-only segment carries nothing and is skipped",
			plant: func(t *testing.T, d *sim.Disk) {
				plantSegment(t, d, "wal", format.SegmentHeader{VolumeID: segVol, Epoch: epoch, FirstSequence: 1},
					segBody(t, 1, 3, segVol, epoch), "")
				plantSegment(t, d, "wal", format.SegmentHeader{VolumeID: segVol, Epoch: epoch, FirstSequence: 4}, nil, "")
			},
			wantRecords: 3,
		},
		{
			name: "a segment header naming another volume fails closed",
			plant: func(t *testing.T, d *sim.Disk) {
				plantSegment(t, d, "wal", format.SegmentHeader{VolumeID: other, Epoch: epoch, FirstSequence: 1},
					segBody(t, 1, 2, other, epoch), "")
			},
			wantErr: wal.ErrForeignVolume,
		},
		{
			name: "a segment header naming another epoch fails closed",
			plant: func(t *testing.T, d *sim.Disk) {
				plantSegment(t, d, "wal", format.SegmentHeader{VolumeID: segVol, Epoch: 9, FirstSequence: 1},
					segBody(t, 1, 2, segVol, 9), "")
			},
			wantErr: wal.ErrForeignEpoch,
		},
		{
			name: "a name that disagrees with the header it holds",
			plant: func(t *testing.T, d *sim.Disk) {
				plantSegment(t, d, "wal", format.SegmentHeader{VolumeID: segVol, Epoch: epoch, FirstSequence: 1},
					segBody(t, 1, 2, segVol, epoch), "000000000000000042.seg")
			},
			wantErr: wal.ErrCorruptSegment,
		},
		{
			name: "a first record that is not the one the header announces",
			plant: func(t *testing.T, d *sim.Disk) {
				plantSegment(t, d, "wal", format.SegmentHeader{VolumeID: segVol, Epoch: epoch, FirstSequence: 1},
					segBody(t, 5, 2, segVol, epoch), "")
			},
			wantErr: wal.ErrCorruptSegment,
		},
		{
			name: "a file in the WAL directory that is not a segment",
			plant: func(t *testing.T, d *sim.Disk) {
				plantSegment(t, d, "wal", format.SegmentHeader{VolumeID: segVol, Epoch: epoch, FirstSequence: 1},
					segBody(t, 1, 2, segVol, epoch), "")
				f, err := d.Create(wal.SegmentDir("wal", segVol, epoch) + "/notes.txt")
				if err != nil {
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
			},
			wantErr: wal.ErrCorruptSegment,
		},
		{
			name: "a truncated header on a segment that is not the newest",
			plant: func(t *testing.T, d *sim.Disk) {
				plantSegment(t, d, "wal", format.SegmentHeader{VolumeID: segVol, Epoch: epoch, FirstSequence: 1},
					segBody(t, 1, 2, segVol, epoch), "")
				plantSegment(t, d, "wal", format.SegmentHeader{VolumeID: segVol, Epoch: epoch, FirstSequence: 3},
					segBody(t, 3, 1, segVol, epoch), "")
				names, err := wal.SegmentFiles(d, "wal", segVol, epoch)
				if err != nil {
					t.Fatal(err)
				}
				d.TornTail(names[0], 20)
				d.Crash()
			},
			wantErr: wal.ErrCorruptSegment,
		},
		{
			name: "a bit flipped inside a sealed segment is caught by CRC",
			plant: func(t *testing.T, d *sim.Disk) {
				body := segBody(t, 1, 3, segVol, epoch)
				body[format.RecordHeaderSize+2] ^= 0x01 // inside the first record's payload
				plantSegment(t, d, "wal", format.SegmentHeader{VolumeID: segVol, Epoch: epoch, FirstSequence: 1}, body, "")
			},
			wantErr: format.ErrPayloadCRC,
		},
		{
			name: "a foreign record inside a segment whose header is this volume's",
			plant: func(t *testing.T, d *sim.Disk) {
				var body []byte
				body = append(body, segBody(t, 1, 1, segVol, epoch)...)
				body = append(body, segBody(t, 2, 1, other, epoch)...)
				plantSegment(t, d, "wal", format.SegmentHeader{VolumeID: segVol, Epoch: epoch, FirstSequence: 1}, body, "")
			},
			wantErr: wal.ErrForeignVolume,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := sim.NewDisk()
			tc.plant(t, d)
			clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
			resumed, err := wal.Resume(d, "wal", clk, segVol, epoch, 0, wal.Limits{MaxUnflushedBytes: 1 << 20}, nil)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("resume returned %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resume of a directory a crash can produce must succeed: %v", err)
			}
			recs, err := wal.ReplaySegments(d, "wal", segVol, epoch)
			if err != nil {
				t.Fatalf("replay: %v", err)
			}
			if len(recs) != tc.wantRecords {
				t.Fatalf("replay returned %d records, want %d", len(recs), tc.wantRecords)
			}
			if got := resumed.Watermarks().Local; got != uint64(tc.wantRecords) {
				t.Fatalf("resumed local watermark = %d, want %d", got, tc.wantRecords)
			}
		})
	}
}

// TestResumeCutsTheTornTailSoTheNextRecordIsReachable: a resumed log appends at the end
// of the newest segment. If the torn bytes were left in place, every record written
// after the restart would sit behind a hole replay stops at — reported as success, with
// the records after it silently gone. The tear is cut off instead.
func TestResumeCutsTheTornTailSoTheNextRecordIsReachable(t *testing.T) {
	d := sim.NewDisk()
	l := segLog(t, d, "wal", 4096)
	for i := range 3 {
		if _, err := l.Write(uint64(i)*4096, []byte("before-the-crash"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	names := l.SegmentNames()
	size := walSize(t, l)
	d.TornTail(names[len(names)-1], int(size)-11)
	d.Crash()

	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	resumed, err := wal.Resume(d, "wal", clk, segVol, 1, 0, wal.Limits{MaxUnflushedBytes: 1 << 20}, nil)
	if err != nil {
		t.Fatalf("a torn tail is the normal crash case: %v", err)
	}
	if got := resumed.Watermarks().Local; got != 2 {
		t.Fatalf("resumed local = %d, want 2 (the last intact record)", got)
	}
	if _, err := resumed.Write(1<<20, []byte("after-the-restart"), 0); err != nil {
		t.Fatal(err)
	}
	recs, err := wal.ReplaySegments(d, "wal", segVol, 1)
	if err != nil {
		t.Fatalf("replay after the restart: %v", err)
	}
	if len(recs) != 3 || recs[2].Sequence != 3 {
		t.Fatalf("replay returned %d records ending at %d; the record written after the restart "+
			"is behind the tear", len(recs), recs[len(recs)-1].Sequence)
	}
}

// TestARecordNeverStraddlesASegment: a record that does not fit in what is left seals
// the segment and starts the next one. It is what makes replaying one segment a
// self-contained operation and an unlink safe to reason about — a record split across
// two files would be half-destroyed by reclaiming the older one.
func TestARecordNeverStraddlesASegment(t *testing.T) {
	d := sim.NewDisk()
	const segBytes = 600 // a 64-byte header plus one 104+payload record, not two
	l := segLog(t, d, "wal", segBytes)
	for i := range 6 {
		if _, err := l.Write(uint64(i)*4096, make([]byte, 300), 0); err != nil {
			t.Fatal(err)
		}
	}
	names := l.SegmentNames()
	if len(names) < 3 {
		t.Fatalf("six 404-byte records into %d-byte segments produced %d segments", segBytes, len(names))
	}
	for _, name := range names {
		f, err := d.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		size, err := f.Size()
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		body := size - int64(format.SegmentHeaderSize)
		if body%(int64(format.RecordHeaderSize)+300) != 0 {
			t.Fatalf("segment %s holds %d body bytes, not a whole number of records", name, body)
		}
	}
	// And the whole run still replays, across every boundary.
	recs, err := wal.ReplaySegments(d, "wal", segVol, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 6 {
		t.Fatalf("replay across %d segments returned %d records, want 6", len(names), len(recs))
	}
}

// TestSegmentNamesAreSequenceOrder: the directory listing is the index, which only
// works while lexicographic order is numeric order.
func TestSegmentNamesAreSequenceOrder(t *testing.T) {
	d := sim.NewDisk()
	for _, first := range []uint64{1, 2, 10, 100, 999_999} {
		plantSegment(t, d, "wal", format.SegmentHeader{VolumeID: segVol, Epoch: 1, FirstSequence: first}, nil, "")
	}
	names, err := wal.SegmentFiles(d, "wal", segVol, 1)
	if err != nil {
		t.Fatal(err)
	}
	want := []uint64{1, 2, 10, 100, 999_999}
	for i, name := range names {
		base := name[strings.LastIndex(name, "/")+1:]
		if base != fmt.Sprintf("%018d.seg", want[i]) {
			t.Fatalf("listing position %d is %s, want the segment starting at %d", i, base, want[i])
		}
	}
}

// TestResumeDiscardsASegmentStub: a crash can land between Create returning (the name
// is durable — Disk.Create fsyncs the parent directory) and the header reaching the
// device. The file that leaves behind carries no records and cannot be classified, so
// a resume takes it back out of the directory rather than deciding what it is.
func TestResumeDiscardsASegmentStub(t *testing.T) {
	tests := []struct {
		name       string
		before     int // whole segments planted before the stub
		wantRecs   int
		wantLocal  uint64
		wantResume int // segments the resumed log holds
	}{
		{name: "the stub is the only segment", before: 0, wantRecs: 0, wantLocal: 0, wantResume: 0},
		{name: "the stub follows a whole segment", before: 1, wantRecs: 3, wantLocal: 3, wantResume: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := sim.NewDisk()
			next := uint64(1)
			for range tc.before {
				plantSegment(t, d, "wal", format.SegmentHeader{VolumeID: segVol, Epoch: 1, FirstSequence: next},
					segBody(t, next, 3, segVol, 1), "")
				next += 3
			}
			// The stub: a file with a name and nothing (usable) in it.
			stub := wal.SegmentDir("wal", segVol, 1) + "/" + fmt.Sprintf("%018d.seg", next)
			f, err := d.Create(stub)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Append([]byte{'W', 'S'}); err != nil { // the header never finished
				t.Fatal(err)
			}
			if err := f.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}

			clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
			resumed, err := wal.Resume(d, "wal", clk, segVol, 1, 0, wal.Limits{MaxUnflushedBytes: 1 << 20}, nil)
			if err != nil {
				t.Fatalf("a stub is a crash's leftovers, not corruption: %v", err)
			}
			if got := len(resumed.SegmentNames()); got != tc.wantResume {
				t.Fatalf("the resumed log holds %d segments, want %d", got, tc.wantResume)
			}
			if got := resumed.Watermarks().Local; got != tc.wantLocal {
				t.Fatalf("resumed local = %d, want %d", got, tc.wantLocal)
			}
			if exists, err := d.Exists(stub); err != nil || exists {
				t.Fatalf("the stub is still in the directory (exists=%t err=%v)", exists, err)
			}
			// And the log keeps working: the next record continues the sequence space.
			seq, err := resumed.Write(1<<20, []byte("after-the-stub"), 0)
			if err != nil {
				t.Fatal(err)
			}
			if seq != tc.wantLocal+1 {
				t.Fatalf("the record after the stub took sequence %d, want %d", seq, tc.wantLocal+1)
			}
			recs, err := wal.ReplaySegments(d, "wal", segVol, 1)
			if err != nil {
				t.Fatal(err)
			}
			if len(recs) != tc.wantRecs+1 {
				t.Fatalf("replay returned %d records, want %d", len(recs), tc.wantRecs+1)
			}
		})
	}
}

// TestARecordLargerThanASegmentGoesInAlone: a record that cannot fit in an empty
// segment must not send the log around a loop of sealing empty files. It goes in
// alone, oversize, and the record after it rotates.
func TestARecordLargerThanASegmentGoesInAlone(t *testing.T) {
	d := sim.NewDisk()
	l := segLog(t, d, "wal", 200) // smaller than a record header
	if _, err := l.Write(0, make([]byte, 500), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Write(4096, []byte("next"), 0); err != nil {
		t.Fatal(err)
	}
	if got := len(l.SegmentNames()); got != 2 {
		t.Fatalf("an oversize record produced %d segments, want 2 (it alone, then the next)", got)
	}
	recs, err := wal.ReplaySegments(d, "wal", segVol, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 || recs[0].Length != 500 {
		t.Fatalf("replay returned %+v, want the oversize record and its successor", recs)
	}
}

// TestCloseReleasesTheOpenSegmentWithoutSealingIt: Close is not a durability step and
// not a seal. A log that is closed and rebuilt goes through Resume, which reopens the
// newest segment for append — so the records written after the restart land in the
// same file, not in a new one.
func TestCloseReleasesTheOpenSegmentWithoutSealingIt(t *testing.T) {
	d := sim.NewDisk()
	l := segLog(t, d, "wal", 4096)
	if _, err := l.Write(0, []byte("before"), 0); err != nil {
		t.Fatal(err)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	before := l.SegmentNames()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("closing twice must be harmless: %v", err)
	}

	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	resumed, err := wal.Resume(d, "wal", clk, segVol, 1, 0, wal.Limits{MaxUnflushedBytes: 1 << 20}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resumed.Write(4096, []byte("after"), 0); err != nil {
		t.Fatal(err)
	}
	if got := resumed.SegmentNames(); len(got) != 1 || got[0] != before[0] {
		t.Fatalf("the resumed log wrote into %v, want the one segment it inherited (%v)", got, before)
	}
}

// TestAFullDeviceAtASegmentBoundaryLeavesNoStub: the device can fill on the create
// that starts the next segment rather than on a record append. ADR-0013 wants that
// state reached at a segment boundary, and what must not survive it is a file the
// directory names and replay cannot classify — so the failed create takes its own file
// back out. The log reports a full device, keeps the records it accepted, and replays
// clean.
func TestAFullDeviceAtASegmentBoundaryLeavesNoStub(t *testing.T) {
	const root = "wal-boundary-enospc"
	d := sim.NewDisk()
	l := segLog(t, d, root, 512)

	// One record in, then the device is capped at exactly what is already there: the
	// next record needs a new segment, and creating it cannot even write a header.
	if _, err := l.Write(0, []byte("accepted"), 0); err != nil {
		t.Fatal(err)
	}
	if err := l.Seal(); err != nil {
		t.Fatal(err)
	}
	used := walSize(t, l)
	d.InjectENOSPC(root, used)

	if _, err := l.Write(4096, []byte("refused"), 0); err == nil {
		t.Fatal("a device with no room accepted a write that needed a new segment")
	}
	if got := l.Degraded(); got != wal.DegradedOutOfSpace {
		t.Fatalf("the log reports %q after a create the device refused, want %q", got, wal.DegradedOutOfSpace)
	}
	names, err := wal.SegmentFiles(d, root, segVol, 1)
	if err != nil {
		t.Fatalf("the refused create left the directory unreadable: %v", err)
	}
	if len(names) != 1 {
		t.Fatalf("the directory holds %d segments (%v), want the one that was written", len(names), names)
	}
	recs, err := wal.ReplaySegments(d, root, segVol, 1)
	if err != nil {
		t.Fatalf("replay after a refused create: %v", err)
	}
	if len(recs) != 1 || recs[0].Sequence != 1 {
		t.Fatalf("replay returned %+v, want only the accepted record", recs)
	}

	// Space comes back and the log continues at the sequence the refused write did
	// not consume.
	d.ClearENOSPC(root)
	seq, err := l.Write(8192, []byte("accepted-again"), 0)
	if err != nil {
		t.Fatalf("write after space was reclaimed: %v", err)
	}
	if seq != 2 {
		t.Fatalf("the write after ENOSPC took sequence %d, want 2", seq)
	}
	if got := l.Degraded(); got != wal.DegradedNone {
		t.Fatalf("the log still reports %q after an append the device took", got)
	}
}
