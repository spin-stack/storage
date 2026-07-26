package wal_test

import (
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// A partial append is the ordinary way a disk fails: ENOSPC, a torn write at a device
// boundary, a truncated writeback. The Log rejected such a write to its caller and
// left the bytes on disk, which breaks the WAL in two different ways depending on how
// much landed:
//
//   - enough bytes to decode: replay resurrects a record the guest was told FAILED,
//     carrying a sequence the Log then re-assigns to a different record — two records
//     with the same sequence and different content;
//   - fewer: replay stops at the tear and reports success, silently dropping every
//     record written after it (in `local` mode those were ACKed on the fdatasync).
//
// The rule a WAL has to keep: after a rejected append the file contains exactly the
// records that were accepted.
func TestRejectedAppendLeavesNothingBehind(t *testing.T) {
	// A WRITE record is 104 bytes of header plus the payload.
	tests := []struct {
		name  string
		limit int // bytes the disk accepts before failing
	}{
		{"nothing lands", 0},
		{"part of the header", 40},
		{"the header but no payload", 104},
		{"header and part of the payload", 110},
		{"all but the last byte", 114},
		{"the whole record, reported as short", 130},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
			d := sim.NewDisk()
			vol := [16]byte{7}
			l := wal.NewLog(d, "wal", clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})

			if _, err := l.Write(0, []byte("accepted-01"), 0); err != nil {
				t.Fatal(err)
			}
			sizeAfterAccepted := walSize(t, l)

			// The fault is aimed at the whole WAL directory: which segment the next
			// record lands in is the log's business, not the test's.
			d.InjectShortAppend("wal", tc.limit)
			if _, err := l.Write(4096, []byte("rejected-02"), 0); err == nil {
				t.Fatal("a short append must be reported to the caller")
			}

			// Nothing of the rejected record may remain.
			if size := walSize(t, l); size != sizeAfterAccepted {
				t.Fatalf("the WAL is %d bytes after a rejected append, want %d (the bytes were left behind)",
					size, sizeAfterAccepted)
			}

			// A later write must be the next sequence, and replay must return exactly
			// the accepted records — no phantom, no duplicate sequence.
			seq, err := l.Write(8192, []byte("accepted-03"), 0)
			if err != nil {
				t.Fatal(err)
			}
			if seq != 2 {
				t.Fatalf("next accepted write got sequence %d, want 2", seq)
			}
			if err := l.Sync(); err != nil {
				t.Fatal(err)
			}

			recs, err := wal.ReplaySegments(d, "wal", vol, 1)
			if err != nil {
				t.Fatalf("replay of the repaired log: %v", err)
			}
			if len(recs) != 2 {
				t.Fatalf("replay returned %d records, want the 2 accepted ones", len(recs))
			}
			seen := map[uint64]uint64{}
			for _, r := range recs {
				if prev, dup := seen[r.Sequence]; dup {
					t.Fatalf("sequence %d appears twice (offsets %d and %d)", r.Sequence, prev, r.Offset)
				}
				seen[r.Sequence] = r.Offset
			}
			if seen[1] != 0 || seen[2] != 8192 {
				t.Fatalf("replayed records are not the accepted ones: %v", seen)
			}
		})
	}
}

// TestRejectedAppendDoesNotAdvanceTheWatermark: the guest was told the write failed,
// so nothing about it may be visible — including the local sequence a snapshot would
// capture.
func TestRejectedAppendDoesNotAdvanceTheWatermark(t *testing.T) {
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	l := wal.NewLog(d, "wal", clk, [16]byte{7}, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})

	if _, err := l.Write(0, []byte("accepted"), 0); err != nil {
		t.Fatal(err)
	}
	before := l.Watermarks()

	d.InjectShortAppend("wal", 60)
	if _, err := l.Write(4096, []byte("rejected"), 0); err == nil {
		t.Fatal("expected the short append to fail")
	}
	if got := l.Watermarks(); got.Local != before.Local {
		t.Fatalf("local watermark moved to %d on a rejected write (was %d)", got.Local, before.Local)
	}
}
