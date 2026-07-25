package wal_test

import (
	"testing"

	"pgregory.net/rapid"

	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// genRecords draws a valid record sequence with monotonic sequence numbers.
func genRecords(t *rapid.T) []wal.Record {
	n := rapid.IntRange(0, 8).Draw(t, "n")
	recs := make([]wal.Record, 0, n)
	for i := range n {
		typ := rapid.SampledFrom([]format.RecordType{
			format.RecordWrite, format.RecordDiscard, format.RecordWriteZeroes,
		}).Draw(t, "type")
		r := wal.Record{
			Type:     typ,
			Epoch:    1,
			Sequence: uint64(i + 1),
			Offset:   uint64(rapid.IntRange(0, 4096).Draw(t, "offset")),
		}
		if typ == format.RecordWrite {
			plen := rapid.IntRange(0, 64).Draw(t, "plen")
			r.Payload = rapid.SliceOfN(rapid.Byte(), plen, plen).Draw(t, "payload")
			r.Length = uint32(plen)
		} else {
			r.Length = uint32(rapid.IntRange(0, 4096).Draw(t, "extent"))
		}
		recs = append(recs, r)
	}
	return recs
}

// TestWALReplayProperty is INV-05 / §25.2: for any record sequence, replay of the
// serialized form is total and safe — full replay reconstructs the state; any
// truncation yields the intact prefix cleanly; any single-bit corruption is caught
// (never applied silently).
func TestWALReplayProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		recs := genRecords(t)
		b, err := wal.Serialize(recs)
		if err != nil {
			t.Fatalf("serialize: %v", err)
		}

		// (1) Full replay reconstructs the exact logical state.
		got, err := wal.Replay(b)
		if err != nil {
			t.Fatalf("full replay error: %v", err)
		}
		if !wal.ApplyAll(got).Equal(wal.ApplyAll(recs)) {
			t.Fatal("full replay produced a different state")
		}

		// Cumulative encoded sizes → truncation oracle.
		sizes := make([]int, len(recs))
		cum := 0
		for i, r := range recs {
			e, _ := r.Encode()
			cum += len(e)
			sizes[i] = cum
		}
		fitting := func(k int) int {
			n := 0
			for i := range recs {
				if sizes[i] <= k {
					n++
				} else {
					break
				}
			}
			return n
		}

		// (2) Truncation at every byte: intact prefix, no error.
		for k := 0; k <= len(b); k++ {
			pref, err := wal.Replay(b[:k])
			if err != nil {
				t.Fatalf("truncation at %d returned error: %v", k, err)
			}
			wantN := fitting(k)
			if len(pref) != wantN {
				t.Fatalf("truncation at %d: got %d records, want %d", k, len(pref), wantN)
			}
			if !wal.ApplyAll(pref).Equal(wal.ApplyAll(recs[:wantN])) {
				t.Fatalf("truncation at %d: prefix state mismatch", k)
			}
		}

		// (3) Single-bit corruption is always detected, and the verified prefix
		// (records before the corrupted one) is byte-faithful.
		if len(b) > 0 {
			bit := rapid.IntRange(0, len(b)*8-1).Draw(t, "bit")
			corrupt := append([]byte(nil), b...)
			corrupt[bit/8] ^= 1 << (bit % 8)

			pref, err := wal.Replay(corrupt)
			if err == nil {
				t.Fatalf("single-bit flip at %d was not detected", bit)
			}
			if !wal.ApplyAll(pref).Equal(wal.ApplyAll(recs[:len(pref)])) {
				t.Fatalf("verified prefix corrupted before detection at bit %d", bit)
			}
		}
	})
}
