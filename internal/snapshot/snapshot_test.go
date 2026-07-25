package snapshot_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/snapshot"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

func v7Vol() [16]byte {
	var v [16]byte
	v[6], v[8] = 0x70, 0x80
	return v
}

func remoteLog(t *testing.T, store *sim.ObjectStore, clk *sim.Clock, vol [16]byte) *wal.Log {
	t.Helper()
	d := sim.NewDisk()
	f, _ := d.Create("wal/active.wal")
	l := wal.NewLog(f, clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), leaseOK{})
	return l
}

// TestSnapshotIsPauseFreeAndCapturesSequence: the capture does not advance the clock
// (pause ≈ 0) and the manifest's target is the captured sequence.
func TestSnapshotIsPauseFreeAndCapturesSequence(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := v7Vol()
	l := remoteLog(t, store, clk, vol)

	_, _ = l.Write(0, []byte("a"), 0)
	_, _ = l.Write(8, []byte("b"), 0)

	m, pause, err := snapshot.NewSnapshotter(store, clk).Create(ctx, l, vol, 1, "snap-1", "")
	if err != nil {
		t.Fatal(err)
	}
	if pause != 0 {
		t.Fatalf("snapshot pause = %v, want ~0", pause)
	}
	if m.TargetSequence != 2 {
		t.Fatalf("target sequence = %d, want 2", m.TargetSequence)
	}
	if len(m.Objects) == 0 || m.RootDigest == "" {
		t.Fatalf("manifest incomplete: %+v", m)
	}
}

// TestSnapshotExcludesLaterWrites: writes after the capture are not in the snapshot.
func TestSnapshotExcludesLaterWrites(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := v7Vol()
	l := remoteLog(t, store, clk, vol)
	snap := snapshot.NewSnapshotter(store, clk)

	_, _ = l.Write(0, []byte("a"), 0)
	m, _, err := snap.Create(ctx, l, vol, 1, "snap-1", "")
	if err != nil {
		t.Fatal(err)
	}
	before := len(m.Objects)

	// More writes + a flush → a new object at a higher sequence.
	_, _ = l.Write(8, []byte("b"), 0)
	_, _ = l.Write(16, []byte("c"), 0)
	if err := l.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// The published manifest is unchanged (immutable) and still excludes the new writes.
	got, err := snapshot.Read(ctx, store, m.VolumeID, "snap-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.TargetSequence != 1 || len(got.Objects) != before {
		t.Fatalf("snapshot changed after later writes: %+v", got)
	}
}

func TestReadMissingManifest(t *testing.T) {
	if _, err := snapshot.Read(context.Background(), sim.NewObjectStore(), "vol", "nope"); err == nil {
		t.Fatal("reading a missing manifest should error")
	}
}

// TestSnapshotManifestIsImmutable is INV-16: republishing a manifest fails.
func TestSnapshotManifestIsImmutable(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	m := snapshot.Manifest{SnapshotID: "s1", VolumeID: format.UUIDString(v7Vol()), Epoch: 1, TargetSequence: 5}
	if err := snapshot.Publish(ctx, store, m); err != nil {
		t.Fatal(err)
	}
	m.TargetSequence = 99 // attempt to change it
	if err := snapshot.Publish(ctx, store, m); !errors.Is(err, objectstore.ErrPreconditionFailed) {
		t.Fatalf("a published manifest must be immutable, got %v", err)
	}
}

// leaseOK is the fence for tests that are not about fencing: remote durability
// requires a lease checker (DEV-0004), and these hold a valid one.
type leaseOK struct{}

func (leaseOK) Valid() bool { return true }
