package gc_test

import (
	"slices"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/checkpoint"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/gc"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

const vol = "00000000-0000-7000-8000-000000000080"

// upload a WAL object covering [first,last] and return its key.
func putWAL(t *testing.T, store *sim.ObjectStore, v [16]byte, epoch, first, last uint64) string {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	b := wal.NewBatcher(clk, v, epoch, 0, wal.DefaultBatchConfig())
	for seq := first; seq <= last; seq++ {
		enc, _ := wal.Record{Sequence: seq, Epoch: epoch, Payload: []byte("x")}.Encode()
		b.Append(seq, enc, false)
	}
	b.Flush()
	key, err := wal.NewUploader(store, 3).Upload(t.Context(), b.Pending()[0])
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// TestGCMarksOrphansNotLive is INV-14: an orphan (unreferenced) WAL object is
// marked; live objects (referenced by a checkpoint) and structural objects are not.
func TestGCMarksOrphansNotLive(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	var v [16]byte
	v[6], v[8] = 0x70, 0x80

	// Structural metadata + a live WAL object referenced by a checkpoint.
	_ = descriptor.Write(ctx, store, descriptor.Descriptor{DEKKeyID: 1, VolumeID: vol, SizeBytes: 1, BlockSize: 65536, KEKID: "k", DEKWrapped: []byte{1}})
	live := putWAL(t, store, v, 1, 1, 2)
	// A real checkpoint carries the digest over its own contents (§21.1); the sweep
	// refuses an anchor that does not describe itself, so publish one that does.
	if err := checkpoint.Publish(ctx, store, checkpoint.Checkpoint{
		VolumeID: vol, Epoch: 1, DurableSequence: 2, Objects: []string{live},
		RootDigest: checkpoint.Digest(2, []string{live}),
	}); err != nil {
		t.Fatal(err)
	}
	// An orphan WAL object no checkpoint/manifest references.
	orphan := putWAL(t, store, v, 1, 9, 9)

	reachable, err := gc.Reachable(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	marks, err := gc.Collect(ctx, store, reachable)
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Contains(marks, orphan) {
		t.Fatalf("orphan %q should be marked; marks=%v", orphan, marks)
	}
	if slices.Contains(marks, live) {
		t.Fatal("a live (checkpoint-referenced) object must never be marked")
	}
	if slices.Contains(marks, descriptor.Key(vol)) {
		t.Fatal("a structural object (descriptor) must never be marked")
	}

	// The GC only computed marks; nothing was deleted — every object still exists.
	for _, key := range []string{live, orphan, descriptor.Key(vol)} {
		if _, err := store.Head(ctx, key); err != nil {
			t.Fatalf("GC must not delete anything, but %q is gone: %v", key, err)
		}
	}
}

// TestGCHasNoPermanentDeleteCapability: the marks are reversible — a marked object is
// still present (the lifecycle sweeps later, not the GC).
func TestGCMarksAreReversible(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	_, _ = store.Put(ctx, "wal/x/1/9-9-h.wal", []byte("orphan"), objectstore.PutOptions{})

	reachable, _ := gc.Reachable(ctx, store)
	marks, _ := gc.Collect(ctx, store, reachable)
	if len(marks) != 1 {
		t.Fatalf("expected 1 mark, got %v", marks)
	}
	// Still retrievable after marking (reversible).
	if _, err := store.Get(ctx, marks[0]); err != nil {
		t.Fatalf("a marked object must still be retrievable, got %v", err)
	}
}

// TestGCNeverMarksATermClaim: the Control Plane's term claims (ADR-0011) are the only
// record of which terms have been issued, and they have to outlive the database they
// were issued from. A sweep that collected them would restore the exact failure the
// claim exists to prevent — a restored database re-issuing a term a live leader still
// holds — with the added twist that it would look like a routine cost-control pass.
func TestGCNeverMarksATermClaim(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())

	claim := "control-plane/terms/00000000000000000042"
	if _, err := store.Put(ctx, claim, []byte(`{"term":42,"holder_id":"cp-a"}`), objectstore.PutOptions{IfNoneMatch: true}); err != nil {
		t.Fatal(err)
	}
	// Old enough that the grace period protects nothing.
	clk.Advance(72 * time.Hour)

	reachable, err := gc.Reachable(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if !reachable[claim] {
		t.Fatalf("the term claim %s is not a GC root", claim)
	}
	marked, err := gc.Mark(ctx, store, clk, reachable, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(marked, claim) {
		t.Fatalf("the sweep marked a term claim: %v", marked)
	}
}
