package wal_test

import (
	"testing"

	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

func TestSerializeReplayRoundTrip(t *testing.T) {
	recs := []wal.Record{
		{Type: format.RecordWrite, Epoch: 1, Sequence: 1, Offset: 0, Payload: []byte("aaaa")},
		{Type: format.RecordWrite, Epoch: 1, Sequence: 2, Offset: 512, Payload: []byte("bbbbbb")},
		{Type: format.RecordDiscard, Epoch: 1, Sequence: 3, Offset: 0, Length: 4},
		{Type: format.RecordWriteZeroes, Epoch: 1, Sequence: 4, Offset: 1024, Length: 512},
	}
	b, err := wal.Serialize(recs)
	if err != nil {
		t.Fatal(err)
	}
	got, err := wal.Replay(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(recs) {
		t.Fatalf("replayed %d records, want %d", len(got), len(recs))
	}
	if !wal.ApplyAll(got).Equal(wal.ApplyAll(recs)) {
		t.Fatal("replayed state differs from original")
	}
}

func TestReplayTornTailStopsClean(t *testing.T) {
	recs := []wal.Record{
		{Type: format.RecordWrite, Epoch: 1, Sequence: 1, Offset: 0, Payload: []byte("first")},
		{Type: format.RecordWrite, Epoch: 1, Sequence: 2, Offset: 8, Payload: []byte("second")},
	}
	b, _ := wal.Serialize(recs)
	// Chop the last record mid-payload: a crash during append.
	torn := b[:len(b)-3]
	got, err := wal.Replay(torn)
	if err != nil {
		t.Fatalf("torn tail should replay cleanly, got %v", err)
	}
	if len(got) != 1 || string(got[0].Payload) != "first" {
		t.Fatalf("torn tail should keep only the intact prefix, got %+v", got)
	}
}

func TestReplayDiscardReadsAsZero(t *testing.T) {
	recs := []wal.Record{
		{Type: format.RecordWrite, Epoch: 1, Sequence: 1, Offset: 0, Payload: []byte{1, 2, 3, 4}},
		{Type: format.RecordDiscard, Epoch: 1, Sequence: 2, Offset: 1, Length: 2},
	}
	st := wal.ApplyAll(recs)
	// After discarding offsets 1..2, only offsets 0 and 3 remain non-zero.
	want := wal.NewState()
	want.Apply(wal.Record{Type: format.RecordWrite, Offset: 0, Payload: []byte{1, 0, 0, 4}})
	if !st.Equal(want) {
		t.Fatal("discard should zero the discarded range")
	}
}
