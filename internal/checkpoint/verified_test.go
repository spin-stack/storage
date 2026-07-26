package checkpoint_test

import (
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/checkpoint"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// checkpointWorld is a volume with a remote log, ready to flush.
type checkpointWorld struct {
	store *sim.ObjectStore
	vol   [16]byte
	log   *wal.Log
}

func newWorld(t *testing.T) *checkpointWorld {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := sim.NewObjectStore()
	vol := v7Vol()
	return &checkpointWorld{store: store, vol: vol, log: remoteLog(t, store, clk, vol)}
}

// A checkpoint is what authorises deleting the only other copy of the data: it
// advances published, and INV-13 lets local WAL be truncated up to published. It was
// built from the log's own durable watermark — a number the Agent set when its PUTs
// returned, not one anybody re-verified against S3. When S3's contiguous prefix falls
// behind that number (a lost object, a mis-scoped lifecycle rule, a corrupt upload),
// the checkpoint publishes the higher sequence and unlocks truncation over sequences
// whose only remaining copy was local. The digest cannot catch it: it hashes key
// strings and cannot express a hole.

// TestCheckpointDoesNotPublishBeyondWhatS3Holds is the property: published never
// exceeds the durable point S3 can prove.
func TestCheckpointDoesNotPublishBeyondWhatS3Holds(t *testing.T) {
	ctx := t.Context()
	w := newWorld(t)

	// Three flushed objects, so the log's durable watermark is 3.
	for i := range 3 {
		if _, err := w.log.Write(uint64(i)*4096, []byte("payload"), 0); err != nil {
			t.Fatal(err)
		}
		if err := w.log.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got := w.log.Watermarks().Durable; got != 3 {
		t.Fatalf("setup: log durable = %d, want 3", got)
	}

	// The middle object is lost. S3 can now only prove sequence 1.
	objs, err := w.store.List(ctx, "wal/")
	if err != nil || len(objs) != 3 {
		t.Fatalf("setup: %d objects err=%v", len(objs), err)
	}
	if err := w.store.Delete(ctx, objs[1].Key); err != nil {
		t.Fatal(err)
	}
	proven, err := recovery.DurablePrefix(ctx, w.store, w.vol, 1)
	if err != nil {
		t.Fatal(err)
	}
	if proven != 1 {
		t.Fatalf("setup: S3 proves %d, want 1", proven)
	}

	cp, err := checkpoint.NewCheckpointer(w.store).Create(ctx, w.log, w.vol, 1)
	if err != nil {
		// Refusing is an acceptable answer; publishing a lie is not.
		if w.log.Watermarks().Published != 0 {
			t.Fatalf("published advanced to %d on a failed checkpoint", w.log.Watermarks().Published)
		}
		return
	}
	if cp.DurableSequence > proven {
		t.Fatalf("checkpoint published sequence %d while S3 can prove only %d", cp.DurableSequence, proven)
	}
	if got := w.log.Watermarks().Published; got > proven {
		t.Fatalf("published advanced to %d, above what S3 holds (%d) — this authorises truncating local WAL over data that exists nowhere else", got, proven)
	}
}

// TestTruncateStillRefusedAfterAnUnverifiableCheckpoint: the consequence that matters.
// INV-13 must keep the local copy of anything S3 cannot prove.
func TestTruncateStillRefusedAfterAnUnverifiableCheckpoint(t *testing.T) {
	ctx := t.Context()
	w := newWorld(t)

	for i := range 3 {
		if _, err := w.log.Write(uint64(i)*4096, []byte("payload"), 0); err != nil {
			t.Fatal(err)
		}
		if err := w.log.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	objs, _ := w.store.List(ctx, "wal/")
	if err := w.store.Delete(ctx, objs[1].Key); err != nil {
		t.Fatal(err)
	}

	_, _ = checkpoint.NewCheckpointer(w.store).Create(ctx, w.log, w.vol, 1)

	// Truncating at the log's durable watermark would discard the only copy of
	// sequences 2 and 3.
	if err := w.log.TruncateLocal(3); !errors.Is(err, wal.ErrTruncateAboveDurable) {
		t.Fatalf("truncation at 3 must be refused after an unverifiable checkpoint, got %v", err)
	}
}

// TestCheckpointObjectsAreContiguous: the object list a rebuild replays cannot have a
// hole in it, and the root digest cannot express one — so the list itself must be
// checked when it is built.
func TestCheckpointObjectsAreContiguous(t *testing.T) {
	ctx := t.Context()
	w := newWorld(t)

	for i := range 3 {
		if _, err := w.log.Write(uint64(i)*4096, []byte("payload"), 0); err != nil {
			t.Fatal(err)
		}
		if err := w.log.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	objs, _ := w.store.List(ctx, "wal/")
	if err := w.store.Delete(ctx, objs[1].Key); err != nil {
		t.Fatal(err)
	}

	cp, err := checkpoint.NewCheckpointer(w.store).Create(ctx, w.log, w.vol, 1)
	if err != nil {
		return // refusing is fine
	}
	// Whatever it published must be replayable on its own.
	if _, _, err := recovery.Recover(ctx, w.store, nil, w.vol, cp.Epoch); err != nil {
		t.Fatalf("the published checkpoint's state is not recoverable: %v", err)
	}
	if cp.DurableSequence > 1 {
		t.Fatalf("checkpoint covers sequence %d across a hole", cp.DurableSequence)
	}
}

// TestCheckpointRefusesWhenS3HoldsMoreThanThisLogAcked: the other direction. If the
// prefix is longer than anything this writer ACKed, someone else is publishing into
// the epoch — putting this log's name on that data is how two writers become one
// corrupted volume.
func TestCheckpointRefusesWhenS3HoldsMoreThanThisLogAcked(t *testing.T) {
	ctx := t.Context()
	w := newWorld(t)

	if _, err := w.log.Write(0, []byte("payload"), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.log.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// A second writer continues the same epoch's sequence space — the split-brain
	// shape: two logs, one epoch, one prefix.
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	l2 := wal.NewLogAfter(d, "wal", clk, w.vol, 1, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l2.EnableRemote(wal.NewBatcher(clk, w.vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(w.store, 5), leaseOK{})
	if _, err := l2.Write(4096, []byte("someone else"), 0); err != nil {
		t.Fatal(err)
	}
	if err := l2.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// w.log still believes durable == 1 while the prefix now reaches 2.
	if _, err := checkpoint.NewCheckpointer(w.store).Create(ctx, w.log, w.vol, 1); err == nil {
		t.Fatal("a checkpoint must refuse to publish a prefix this log never ACKed")
	}
}
