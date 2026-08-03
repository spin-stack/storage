package controlplane_test

import (
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

const (
	parentVol  = "00000000-0000-7000-8000-000000000061"
	snapID     = "00000000-0000-7000-8000-000000000062"
	cloneVol   = "00000000-0000-7000-8000-000000000063"
	cloneHostA = "00000000-0000-7000-8000-0000000000d1"
	reqID      = "00000000-0000-7000-8000-0000000000e1"
)

// cpStore returns a catalog, the object store a clone writes its descriptor into, and
// a valid term.
func cpStore(t *testing.T) (metadata.Store, objectstore.Store, int64) {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	md := metasim.New(clk.Wall)
	term, _ := md.AcquireLeadership(t.Context(), "cp")
	return md, sim.NewObjectStore(), term
}

func TestCloneIsIndependentOfParent(t *testing.T) {
	ctx := t.Context()
	md, store, term := cpStore(t)
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: parentVol, SizeBytes: 1 << 30, BlockSize: 65536, Durability: lifecycle.DurabilityRemote,
		// Deliberately not 1. A parent at the first version would let an implementation
		// that hardcodes "the first version" pass this test — which one did, until the
		// assertion below was checked against a planted bug.
		State: lifecycle.VolumeActive, ChainDepth: 0, DEKWrapped: []byte{7}, KEKID: "kek", DEKKeyID: 42,
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := md.CreateSnapshot(ctx, term, metadata.Snapshot{
		SnapshotID: snapID, VolumeID: parentVol, Epoch: 1, TargetSequence: 10,
		RootDigest: "abc", State: lifecycle.SnapshotPublished, RequestID: reqID,
	}); err != nil {
		t.Fatal(err)
	}

	clone, err := controlplane.Clone(ctx, md, store, term, snapID, cloneVol, cloneHostA, nil)
	if err != nil {
		t.Fatal(err)
	}
	// The clone is a new active child inheriting the parent's shape, chain depth +1.
	if clone.VolumeID != cloneVol || clone.SizeBytes != 1<<30 || clone.ChainDepth != 1 || clone.CurrentEpoch != 1 {
		t.Fatalf("clone shape wrong: %+v", clone)
	}
	if string(clone.DEKWrapped) != string([]byte{7}) || clone.KEKID != "kek" {
		t.Fatal("clone must inherit the parent DEK to read the shared base")
	}
	// And the DEK's *version* with it. crypto.DevKMS binds the version as GCM
	// additional authenticated data, so a clone carrying the wrapped key without the
	// number that names it cannot unwrap at all — and the failure would surface on the
	// clone's first WRITE, a long way from the code that dropped it.
	parentRow, err := md.GetVolume(ctx, parentVol)
	if err != nil {
		t.Fatal(err)
	}
	if clone.DEKKeyID != parentRow.DEKKeyID {
		t.Fatalf("clone carries DEK version %d, parent %d", clone.DEKKeyID, parentRow.DEKKeyID)
	}
	// Parent is untouched.
	p, _ := md.GetVolume(ctx, parentVol)
	if p.ChainDepth != 0 {
		t.Fatalf("parent chain depth changed: %d", p.ChainDepth)
	}
}

// TestCloneWithStaleTermFails: a zombie CP cannot create the clone volume (§7).
func TestCloneWithStaleTermFails(t *testing.T) {
	ctx := t.Context()
	md, store, term := cpStore(t)
	_ = md.CreateVolume(ctx, term, metadata.Volume{DEKKeyID: 1, VolumeID: parentVol, SizeBytes: 1 << 30, BlockSize: 65536, State: lifecycle.VolumeActive, DEKWrapped: []byte{7}, KEKID: "kek"}, nil)
	_ = md.CreateSnapshot(ctx, term, metadata.Snapshot{SnapshotID: snapID, VolumeID: parentVol, Epoch: 1, TargetSequence: 10, RootDigest: "abc", State: lifecycle.SnapshotPublished, RequestID: reqID})

	stale := term
	if _, err := md.AcquireLeadership(ctx, "cp-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := controlplane.Clone(ctx, md, store, stale, snapID, cloneVol, cloneHostA, nil); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("want ErrStaleTerm, got %v", err)
	}
}

func TestCloneFromMissingSnapshotFails(t *testing.T) {
	ctx := t.Context()
	md, store, term := cpStore(t)
	if _, err := controlplane.Clone(ctx, md, store, term, "no-such-snap", cloneVol, cloneHostA, nil); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("clone from a missing snapshot: want ErrNotFound, got %v", err)
	}
}

func TestResizeGrowsOnly(t *testing.T) {
	ctx := t.Context()
	md, _, term := cpStore(t)
	_ = md.CreateVolume(ctx, term, metadata.Volume{DEKKeyID: 1, VolumeID: parentVol, SizeBytes: 100, BlockSize: 65536, State: lifecycle.VolumeActive, DEKWrapped: []byte{1}, KEKID: "k"}, nil)

	if err := md.ResizeVolume(ctx, term, parentVol, 200); err != nil {
		t.Fatalf("grow: %v", err)
	}
	if v, _ := md.GetVolume(ctx, parentVol); v.SizeBytes != 200 {
		t.Fatalf("size = %d, want 200", v.SizeBytes)
	}
	// Shrink is rejected (§3 non-goal).
	if err := md.ResizeVolume(ctx, term, parentVol, 50); !errors.Is(err, metadata.ErrShrinkNotAllowed) {
		t.Fatalf("shrink: want ErrShrinkNotAllowed, got %v", err)
	}
}

// TestAFailedCloneChargesNothing is ADR-0017's structural claim, kept as its regression
// guard: the destination is charged when the volume row naming it exists, and a clone
// that fails never writes one. There is no delta anybody has to remember to reverse.
func TestAFailedCloneChargesNothing(t *testing.T) {
	ctx := t.Context()
	md, store, term := cpStore(t)
	if err := md.UpsertHost(ctx, term, metadata.Host{
		HostID: cloneHostA, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
	}); err != nil {
		t.Fatal(err)
	}
	// A bound the clone cannot fit: the write is refused by CreateVolume's own
	// predicate, which is the authoritative check (§28.2).
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: parentVol, SizeBytes: 1 << 30, BlockSize: 65536, DEKKeyID: 1,
		State: lifecycle.VolumeActive, DEKWrapped: []byte{7}, KEKID: "kek",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := md.CreateSnapshot(ctx, term, metadata.Snapshot{
		SnapshotID: snapID, VolumeID: parentVol, Epoch: 1, TargetSequence: 1,
		RootDigest: "d", State: lifecycle.SnapshotPublished, RequestID: reqID,
	}); err != nil {
		t.Fatal(err)
	}
	_, err := controlplane.Clone(ctx, md, store, term, snapID, cloneVol, cloneHostA,
		&metadata.CapacityBound{HostID: cloneHostA, AddBytes: 1 << 30, Limit: 1})
	if !errors.Is(err, metadata.ErrCapacityExceeded) {
		t.Fatalf("want ErrCapacityExceeded, got %v", err)
	}
	if dst, _ := md.GetHost(ctx, cloneHostA); dst.NVMeCommittedBytes != 0 {
		t.Fatalf("a refused clone leaked %d committed bytes", dst.NVMeCommittedBytes)
	}
	if _, err := md.GetVolume(ctx, cloneVol); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("a refused clone must not create the volume: %v", err)
	}
}
