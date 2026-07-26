package wal_test

import (
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

func TestSummaryReflectsDurableObjects(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	l := remoteLog(t, store)

	// Two flushes → two WAL objects → summary lists both, durable=last.
	_, _ = l.Write(0, []byte("first"), 0)
	if err := l.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	_, _ = l.Write(8, []byte("second"), 0)
	_, _ = l.Write(16, []byte("third"), 0)
	if err := l.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := l.WriteSummary(ctx); err != nil {
		t.Fatal(err)
	}

	s, err := wal.ReadSummary(ctx, store, [16]byte{2}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if s.DurableSequence != 3 {
		t.Fatalf("summary durable_sequence = %d, want 3", s.DurableSequence)
	}
	if len(s.Objects) != 2 {
		t.Fatalf("summary should list 2 objects, got %d", len(s.Objects))
	}
	// Objects cover a contiguous sequence prefix 1..3.
	if s.Objects[0].First != 1 || s.Objects[len(s.Objects)-1].Last != 3 {
		t.Fatalf("summary object range not contiguous to durable: %+v", s.Objects)
	}
	for _, o := range s.Objects {
		if !strings.HasPrefix(o.Key, "wal/") {
			t.Fatalf("unexpected object key %q", o.Key)
		}
	}
}

func TestSummaryOverwritesLatest(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	l := remoteLog(t, store)

	_, _ = l.Write(0, []byte("a"), 0)
	_ = l.Flush(ctx)
	_ = l.WriteSummary(ctx)

	_, _ = l.Write(8, []byte("b"), 0)
	_ = l.Flush(ctx)
	_ = l.WriteSummary(ctx)

	// Latest summary wins; exactly one summary object exists.
	objs, _ := store.List(ctx, "wal/")
	summaries := 0
	for _, o := range objs {
		if strings.HasSuffix(o.Key, "summary.json") {
			summaries++
		}
	}
	if summaries != 1 {
		t.Fatalf("expected exactly 1 summary object, got %d", summaries)
	}
	s, _ := wal.ReadSummary(ctx, store, [16]byte{2}, 1)
	if s.DurableSequence != 2 {
		t.Fatalf("latest summary should show durable=2, got %d", s.DurableSequence)
	}
}
