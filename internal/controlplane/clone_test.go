package controlplane_test

import (
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/snapshot"
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

func TestSnapshotCatalogRoundTrip(t *testing.T) {
	ctx := t.Context()
	md, _, term := cpStore(t)
	_ = md.CreateVolume(ctx, term, metadata.Volume{DEKKeyID: 1, VolumeID: parentVol, SizeBytes: 1, BlockSize: 65536, State: lifecycle.VolumeActive, DEKWrapped: []byte{1}, KEKID: "k"}, nil)
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

// TestACloneIsRebuiltAsAClone is §22.5 for the chain: a clone whose catalog is gone must
// come back as a clone, not as an empty volume.
//
// The link is the difference between a volume that reads its parent's data and one that
// reads zeros, and rebuild-metadata reconstructs volumes from descriptors — so a
// descriptor that does not carry it turns a restore into the same data-loss-shaped bug a
// fresh clone used to have.
//
// It also pins the ordering: the parent snapshot belongs to a *different* volume, so the
// rebuild links clones in a second pass, after every snapshot row exists. Rebuilding with
// the clone's descriptor sorting first is exactly the case that fails the foreign key if
// that pass is removed.
func TestACloneIsRebuiltAsAClone(t *testing.T) {
	ctx := t.Context()
	md, store, term := cpStore(t)

	// The *clone's* id is minted first, so that ListVolumeIDs — which sorts, and v7 ids
	// sort by creation — hands the rebuild the clone before the parent. That is the
	// order that breaks a single-pass rebuild: the clone's row would reference a
	// snapshot row that does not exist yet, and the foreign key refuses it. Minting the
	// parent first makes the test pass for the wrong reason.
	cloneVol := ids.New().String()
	parentVol, snapID := ids.New().String(), ids.New().String()
	for _, d := range []descriptor.Descriptor{
		{VolumeID: parentVol, SizeBytes: 1 << 30, BlockSize: 4096,
			Durability: lifecycle.DurabilityRemote, CurrentEpoch: 1,
			KEKID: "k", DEKWrapped: []byte{1}, DEKKeyID: 1},
		{VolumeID: cloneVol, SizeBytes: 1 << 30, BlockSize: 4096,
			Durability: lifecycle.DurabilityRemote, CurrentEpoch: 1, ChainDepth: 1,
			KEKID: "k", DEKWrapped: []byte{1}, DEKKeyID: 1,
			ParentSnapshotID: snapID},
	} {
		if err := descriptor.Write(ctx, store, d); err != nil {
			t.Fatal(err)
		}
	}
	// The manifest is what makes the snapshot row reconstructible (§19): its presence
	// in S3 is the evidence a snapshot was published. It also has to be *written* here
	// rather than the row created directly, because that is the only input the rebuild
	// reads.
	man := snapshot.Manifest{
		SnapshotID: snapID, VolumeID: parentVol, Epoch: 1, TargetSequence: 1,
	}
	man.RootDigest = snapshot.Digest(man.TargetSequence, man.Objects)
	if err := snapshot.Publish(ctx, store, man); err != nil {
		t.Fatal(err)
	}

	if _, err := controlplane.RebuildMetadata(ctx, store, epoch.NewStore(store), md, term); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	got, err := md.GetVolume(ctx, cloneVol)
	if err != nil {
		t.Fatal(err)
	}
	if got.ParentSnapshotID != snapID {
		t.Fatalf("the rebuilt clone descends from %q, want %q — it would read zeros",
			got.ParentSnapshotID, snapID)
	}
}
