package wal_test

import (
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// TestWALSegmentReplayProperty is INV-05 carried across segment boundaries: for any
// interleaving of writes, seals, checkpoints and truncations, the WAL directory
// replays to exactly the records that were accepted and not reclaimed — in order, with
// no duplicate sequence, no hole, and nothing above the published point discarded.
//
// The byte-level property (TestWALReplayProperty: truncate at every byte, flip a bit,
// get the exact state or a detected error) is unchanged and still runs; this one adds
// what a directory of files can get wrong that one file could not — a record split
// across two segments, a segment unlinked with live records in it, a rotation that
// loses the record that triggered it, a torn tail in the wrong file.
//
// Segments seal at a few hundred bytes so an ordinary draw crosses several boundaries.
// SegmentBytes is not a correctness parameter (ADR-0013 wants it derived from the
// device share); what the code below depends on is only that it stays fixed for the
// life of a log.
func TestWALSegmentReplayProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		d := sim.NewDisk()
		clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
		vol := [16]byte{0x9a, 0x11}
		segBytes := int64(rapid.IntRange(200, 900).Draw(t, "segment_bytes"))
		limits := wal.Limits{MaxUnflushedBytes: 1 << 30, SegmentBytes: segBytes}
		l := wal.NewLog(d, "wal", clk, vol, 1, limits)

		// accepted is the model: every record the log told its caller it took, in the
		// order it took them.
		var accepted []wal.Record
		var published uint64

		steps := rapid.IntRange(1, 40).Draw(t, "steps")
		for range steps {
			switch rapid.SampledFrom([]string{"write", "clear", "seal", "checkpoint", "truncate"}).Draw(t, "op") {
			case "write":
				off := uint64(rapid.IntRange(0, 32).Draw(t, "offset")) * 512
				n := rapid.IntRange(0, 200).Draw(t, "payload_len")
				payload := rapid.SliceOfN(rapid.Byte(), n, n).Draw(t, "payload")
				seq, err := l.Write(off, payload, 0)
				if err != nil {
					t.Fatalf("write: %v", err)
				}
				accepted = append(accepted, wal.Record{
					Type: format.RecordWrite, VolumeID: vol, Epoch: 1, Sequence: seq,
					Offset: off, Length: uint32(n), Payload: payload,
				})
			case "clear":
				off := uint64(rapid.IntRange(0, 32).Draw(t, "clear_offset")) * 512
				length := uint32(rapid.IntRange(0, 4096).Draw(t, "extent"))
				typ := rapid.SampledFrom([]format.RecordType{format.RecordDiscard, format.RecordWriteZeroes}).
					Draw(t, "clear_type")
				var (
					seq uint64
					err error
				)
				if typ == format.RecordDiscard {
					seq, err = l.Discard(off, length)
				} else {
					seq, err = l.WriteZeroes(off, length)
				}
				if err != nil {
					t.Fatalf("clear: %v", err)
				}
				accepted = append(accepted, wal.Record{
					Type: typ, VolumeID: vol, Epoch: 1, Sequence: seq, Offset: off, Length: length,
				})
			case "seal":
				if err := l.Seal(); err != nil {
					t.Fatalf("seal: %v", err)
				}
			case "checkpoint":
				// A checkpoint publishes a point at or below what is durable. Nothing
				// here uploads, so durable is advanced to stand for "S3 can reproduce
				// it" — the ordering rules are what this property needs, and they are
				// the same either way.
				local := l.Watermarks().Local
				if local == 0 {
					continue
				}
				target := published + uint64(rapid.IntRange(0, 8).Draw(t, "publish_step"))
				if target > local {
					target = local
				}
				if err := l.AdvanceDurable(local); err != nil {
					t.Fatalf("advance durable to %d: %v", local, err)
				}
				if err := l.AdvancePublished(target); err != nil {
					t.Fatalf("advance published to %d (durable %d): %v", target, local, err)
				}
				published = target
			case "truncate":
				if err := l.TruncateLocal(published); err != nil {
					t.Fatalf("truncate to the published point %d: %v", published, err)
				}
			}
		}
		if err := l.Sync(); err != nil {
			t.Fatalf("sync: %v", err)
		}

		recs, err := wal.ReplaySegments(d, "wal", vol, 1)
		if err != nil {
			t.Fatalf("replay of a WAL no fault touched: %v", err)
		}

		// (1) What survives is a suffix of what was accepted: reclamation takes whole
		// segments off the front and never touches anything else.
		k := len(accepted) - len(recs)
		if k < 0 {
			t.Fatalf("replay returned %d records, more than the %d accepted", len(recs), len(accepted))
		}
		for i, got := range recs {
			want := accepted[k+i]
			if got.Sequence != want.Sequence || got.Type != want.Type ||
				got.Offset != want.Offset || got.Length != want.Length ||
				string(got.Payload) != string(want.Payload) {
				t.Fatalf("replayed record %d is %+v, want %+v", i, got, want)
			}
		}

		// (2) No duplicate sequence and no hole, across every segment boundary.
		seen := map[uint64]bool{}
		for i, r := range recs {
			if seen[r.Sequence] {
				t.Fatalf("sequence %d replayed twice", r.Sequence)
			}
			seen[r.Sequence] = true
			if i > 0 && r.Sequence != recs[i-1].Sequence+1 {
				t.Fatalf("hole in the replayed run: %d follows %d", r.Sequence, recs[i-1].Sequence)
			}
		}

		// (3) INV-13: everything discarded was at or below the published point. Above
		// it, this host holds the only copy.
		if k > 0 && accepted[k-1].Sequence > published {
			t.Fatalf("sequence %d was reclaimed with published at %d", accepted[k-1].Sequence, published)
		}

		// (4) A resumed log rebuilds the same view over what is left, and continues the
		// sequence space rather than reissuing it.
		resumed, err := wal.Resume(d, "wal", clk, vol, 1, 0, limits, nil)
		if err != nil {
			t.Fatalf("resume: %v", err)
		}
		if len(accepted) > 0 {
			if got, want := resumed.Watermarks().Local, accepted[len(accepted)-1].Sequence; got != want {
				t.Fatalf("resumed local watermark = %d, want %d", got, want)
			}
		}
		want := wal.ApplyAll(accepted[k:])
		for _, r := range recs {
			buf := make([]byte, max(int(r.Length), 1))
			resumed.Read(r.Offset, buf)
			model := make([]byte, len(buf))
			readModel(want, r.Offset, model)
			if string(buf) != string(model) {
				t.Fatalf("the resumed view at offset %d reads %x, the model says %x", r.Offset, buf, model)
			}
		}
	})
}

// readModel fills buf from a modelled State: a byte nothing wrote (or that a DISCARD
// cleared) reads as zero (§14.6).
func readModel(s *wal.State, off uint64, buf []byte) {
	for i := range buf {
		buf[i] = s.At(off + uint64(i))
	}
}
