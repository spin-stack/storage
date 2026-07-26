package controlplane_test

import (
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/sim"
)

const (
	parentVol  = "00000000-0000-7000-8000-000000000061"
	snapID     = "00000000-0000-7000-8000-000000000062"
	cloneVol   = "00000000-0000-7000-8000-000000000063"
	cloneHostA = "00000000-0000-7000-8000-0000000000d1"
	reqID      = "00000000-0000-7000-8000-0000000000e1"
)

func cpStore(t *testing.T) (metadata.Store, int64) {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	md := metasim.New(clk.Wall)
	term, _ := md.AcquireLeadership(t.Context(), "cp")
	return md, term
}

func TestCloneIsIndependentOfParent(t *testing.T) {
	ctx := t.Context()
	md, term := cpStore(t)
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: parentVol, SizeBytes: 1 << 30, BlockSize: 65536, Durability: lifecycle.DurabilityRemote,
		State: lifecycle.VolumeActive, ChainDepth: 0, DEKWrapped: []byte{7}, KEKID: "kek",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := md.CreateSnapshot(ctx, term, metadata.Snapshot{
		SnapshotID: snapID, VolumeID: parentVol, Epoch: 1, TargetSequence: 10,
		RootDigest: "abc", State: lifecycle.SnapshotPublished, RequestID: reqID,
	}); err != nil {
		t.Fatal(err)
	}

	clone, err := controlplane.Clone(ctx, md, term, snapID, cloneVol, cloneHostA, nil)
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
	// Parent is untouched.
	p, _ := md.GetVolume(ctx, parentVol)
	if p.ChainDepth != 0 {
		t.Fatalf("parent chain depth changed: %d", p.ChainDepth)
	}
}

// TestCloneWithStaleTermFails: a zombie CP cannot create the clone volume (§7).
func TestCloneWithStaleTermFails(t *testing.T) {
	ctx := t.Context()
	md, term := cpStore(t)
	_ = md.CreateVolume(ctx, term, metadata.Volume{VolumeID: parentVol, SizeBytes: 1 << 30, BlockSize: 65536, State: lifecycle.VolumeActive, DEKWrapped: []byte{7}, KEKID: "kek"}, nil)
	_ = md.CreateSnapshot(ctx, term, metadata.Snapshot{SnapshotID: snapID, VolumeID: parentVol, Epoch: 1, TargetSequence: 10, RootDigest: "abc", State: lifecycle.SnapshotPublished, RequestID: reqID})

	stale := term
	if _, err := md.AcquireLeadership(ctx, "cp-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := controlplane.Clone(ctx, md, stale, snapID, cloneVol, cloneHostA, nil); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("want ErrStaleTerm, got %v", err)
	}
}

func TestCloneFromMissingSnapshotFails(t *testing.T) {
	ctx := t.Context()
	md, term := cpStore(t)
	if _, err := controlplane.Clone(ctx, md, term, "no-such-snap", cloneVol, cloneHostA, nil); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("clone from a missing snapshot: want ErrNotFound, got %v", err)
	}
}

func TestResizeGrowsOnly(t *testing.T) {
	ctx := t.Context()
	md, term := cpStore(t)
	_ = md.CreateVolume(ctx, term, metadata.Volume{VolumeID: parentVol, SizeBytes: 100, BlockSize: 65536, State: lifecycle.VolumeActive, DEKWrapped: []byte{1}, KEKID: "k"}, nil)

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

func TestSnapshotCatalogRoundTrip(t *testing.T) {
	ctx := t.Context()
	md, term := cpStore(t)
	_ = md.CreateVolume(ctx, term, metadata.Volume{VolumeID: parentVol, SizeBytes: 1, BlockSize: 65536, State: lifecycle.VolumeActive, DEKWrapped: []byte{1}, KEKID: "k"}, nil)
	snap := metadata.Snapshot{SnapshotID: snapID, VolumeID: parentVol, Epoch: 2, TargetSequence: 7, RootDigest: "d", State: lifecycle.SnapshotPublished, ManifestKey: "snapshots/x", RequestID: reqID}
	if err := md.CreateSnapshot(ctx, term, snap); err != nil {
		t.Fatal(err)
	}
	got, err := md.GetSnapshot(ctx, snapID)
	if err != nil {
		t.Fatal(err)
	}
	if got.TargetSequence != 7 || got.RootDigest != "d" || got.ManifestKey != "snapshots/x" {
		t.Fatalf("snapshot round-trip: %+v", got)
	}
	if _, err := md.GetSnapshot(ctx, "no-such-snap"); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("missing snapshot: %v", err)
	}
}
