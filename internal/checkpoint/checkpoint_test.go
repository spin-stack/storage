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

func v7Vol() [16]byte {
	var v [16]byte
	v[6], v[8] = 0x70, 0x80
	return v
}

func remoteLog(t *testing.T, store *sim.ObjectStore, clk *sim.Clock, vol [16]byte) *wal.Log {
	t.Helper()
	d := sim.NewDisk()
	l := wal.NewLog(d, "wal", clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), leaseOK{})
	return l
}

// TestCheckpointThenTruncate: a checkpoint advances published; only then can local
// WAL be truncated up to that point (§21.1). Recovery from S3 is unaffected.
func TestCheckpointThenTruncate(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := v7Vol()
	l := remoteLog(t, store, clk, vol)

	_, _ = l.Write(0, []byte("a"), 0)
	_, _ = l.Write(8, []byte("b"), 0)
	if err := l.Flush(ctx); err != nil { // durable = 2
		t.Fatal(err)
	}

	// Before a checkpoint, published is 0 → truncating above it is refused (INV-13).
	if err := l.TruncateLocal(2); !errors.Is(err, wal.ErrTruncateAboveDurable) {
		t.Fatalf("truncate before checkpoint: want ErrTruncateAboveDurable, got %v", err)
	}

	cp, err := checkpoint.NewCheckpointer(store).Create(ctx, l, vol, 1)
	if err != nil {
		t.Fatal(err)
	}
	if cp.DurableSequence != 2 || l.Watermarks().Published != 2 {
		t.Fatalf("checkpoint/published wrong: cp=%+v published=%d", cp, l.Watermarks().Published)
	}

	// Now truncation up to the published point is allowed.
	if err := l.TruncateLocal(2); err != nil {
		t.Fatalf("truncate after checkpoint: %v", err)
	}
	if l.TruncatedUpTo() != 2 {
		t.Fatalf("truncatedUpTo = %d, want 2", l.TruncatedUpTo())
	}

	// Recovery from S3 is untouched by local truncation.
	if durable, _ := recovery.DurablePrefix(ctx, store, vol, 1); durable != 2 {
		t.Fatalf("recovery after truncation = %d, want 2", durable)
	}
}

// TestNeverTruncateAboveDurable is INV-13: even after a checkpoint, truncating
// beyond the published point is refused.
func TestNeverTruncateAboveDurable(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := v7Vol()
	l := remoteLog(t, store, clk, vol)

	_, _ = l.Write(0, []byte("a"), 0)
	if err := l.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	_, _ = checkpoint.NewCheckpointer(store).Create(ctx, l, vol, 1) // published = 1

	// A later un-checkpointed write.
	_, _ = l.Write(8, []byte("b"), 0) // local = 2, published = 1
	if err := l.TruncateLocal(2); !errors.Is(err, wal.ErrTruncateAboveDurable) {
		t.Fatalf("truncate above published: want ErrTruncateAboveDurable, got %v", err)
	}
	if err := l.TruncateLocal(1); err != nil {
		t.Fatalf("truncate at published: %v", err)
	}
}

func TestReadMissingCheckpoint(t *testing.T) {
	if _, err := checkpoint.Read(t.Context(), sim.NewObjectStore(), "v", 1, 5); err == nil {
		t.Fatal("reading a missing checkpoint should error")
	}
}

func TestCheckpointImmutable(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	cp := checkpoint.Checkpoint{VolumeID: "v", Epoch: 1, DurableSequence: 5}
	if err := checkpoint.Publish(ctx, store, cp); err != nil {
		t.Fatal(err)
	}
	cp.RootDigest = "changed"
	if err := checkpoint.Publish(ctx, store, cp); err == nil {
		t.Fatal("a published checkpoint must be immutable")
	}
	got, err := checkpoint.Read(ctx, store, "v", 1, 5)
	if err != nil || got.DurableSequence != 5 {
		t.Fatalf("read: %+v err=%v", got, err)
	}
}

// leaseOK is the fence for tests that are not about fencing: remote durability
// requires a lease checker (DEV-0004), and these hold a valid one.
type leaseOK struct{}

func (leaseOK) Valid() bool { return true }
