package controlplane_test

import (
	"context"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/snapshot"
)

// DEV-0009. §22.5 makes total loss of PostgreSQL survivable because the S3 layout is
// self-describing. Rebuilding only the volume rows does not deliver that: the
// snapshot catalog is what clones, chains, and restores are anchored to, and a
// rebuild that silently drops it leaves the fleet with volumes nobody can restore.

const (
	rebuiltVol  = "00000000-0000-7000-8000-00000000ab01"
	rebuiltSnap = "00000000-0000-7000-8000-00000000ab02"
	rebuiltReq  = "00000000-0000-7000-8000-00000000ab03"
)

func TestRebuildRestoresTheSnapshotCatalog(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	epochs := epoch.NewStore(store)

	// The self-describing layout: a descriptor, its epoch object, and a published
	// snapshot manifest.
	if err := descriptor.Write(ctx, store, descriptor.Descriptor{
		VolumeID: rebuiltVol, SizeBytes: 1 << 30, BlockSize: 65536,
		Durability: lifecycle.DurabilityRemote, CurrentEpoch: 3, KEKID: "kek-1",
		DEKWrapped: []byte{1, 2},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := epochs.Init(ctx, rebuiltVol, 3); err != nil {
		t.Fatal(err)
	}
	m := snapshot.Manifest{
		SnapshotID: rebuiltSnap, VolumeID: rebuiltVol, Epoch: 3, TargetSequence: 42,
		Objects: []string{"wal/x/3/1-42.wal"},
	}
	m.RootDigest = snapshot.Digest(m.TargetSequence, m.Objects)
	if err := snapshot.Publish(ctx, store, m); err != nil {
		t.Fatal(err)
	}

	md := metasim.New(clk.Wall)
	term, _ := md.AcquireLeadership(ctx, "cp")
	res, err := controlplane.RebuildMetadata(ctx, store, epochs, md, term)
	if err != nil {
		t.Fatal(err)
	}
	if res.Volumes != 1 {
		t.Fatalf("rebuilt %d volumes, want 1", res.Volumes)
	}
	if res.Snapshots != 1 {
		t.Fatalf("rebuilt %d snapshots, want 1 — the catalog is not optional (§22.5)", res.Snapshots)
	}

	got, err := md.GetSnapshot(ctx, rebuiltSnap)
	if err != nil {
		t.Fatalf("the snapshot is not in the catalog: %v", err)
	}
	if got.VolumeID != rebuiltVol || got.TargetSequence != 42 || got.Epoch != 3 {
		t.Fatalf("rebuilt snapshot = %+v", got)
	}
	if got.RootDigest != m.RootDigest {
		t.Fatalf("root digest = %q, want the manifest's %q", got.RootDigest, m.RootDigest)
	}
	if got.State != lifecycle.SnapshotPublished {
		t.Fatalf("a manifest in S3 is a published snapshot; state = %q", got.State)
	}
	if got.ManifestKey != snapshot.ManifestKey(rebuiltVol, rebuiltSnap) {
		t.Fatalf("manifest key = %q", got.ManifestKey)
	}
}

// TestRebuildIsIdempotent: rebuild-metadata is run by an operator under pressure,
// possibly twice. It must converge, not duplicate or fail.
func TestRebuildIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	epochs := epoch.NewStore(store)

	if err := descriptor.Write(ctx, store, descriptor.Descriptor{
		VolumeID: rebuiltVol, SizeBytes: 1, BlockSize: 65536,
		Durability: lifecycle.DurabilityLocal, CurrentEpoch: 1, KEKID: "k", DEKWrapped: []byte{1},
	}); err != nil {
		t.Fatal(err)
	}
	m := snapshot.Manifest{SnapshotID: rebuiltSnap, VolumeID: rebuiltVol, Epoch: 1, TargetSequence: 7}
	m.RootDigest = snapshot.Digest(m.TargetSequence, m.Objects)
	if err := snapshot.Publish(ctx, store, m); err != nil {
		t.Fatal(err)
	}

	md := metasim.New(clk.Wall)
	term, _ := md.AcquireLeadership(ctx, "cp")
	if _, err := controlplane.RebuildMetadata(ctx, store, epochs, md, term); err != nil {
		t.Fatal(err)
	}
	second, err := controlplane.RebuildMetadata(ctx, store, epochs, md, term)
	if err != nil {
		t.Fatalf("second rebuild: %v", err)
	}
	if second.Volumes != 0 || second.Snapshots != 0 {
		t.Fatalf("second rebuild re-created rows: %+v", second)
	}
}

// TestRebuildReportsWhatItCouldNotReconstruct: silence about missing state is what
// turns a partial rebuild into a surprise. Hosts and in-flight operations are not in
// S3 at all, and the caller has to know that.
func TestRebuildReportsWhatItCouldNotReconstruct(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	md := metasim.New(clk.Wall)
	term, _ := md.AcquireLeadership(ctx, "cp")

	res, err := controlplane.RebuildMetadata(ctx, store, epoch.NewStore(store), md, term)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.NotReconstructible) == 0 {
		t.Fatal("the result must name the state S3 cannot rebuild (hosts, leases, operations)")
	}
	var mentionsHosts bool
	for _, what := range res.NotReconstructible {
		if what == "hosts" {
			mentionsHosts = true
		}
	}
	if !mentionsHosts {
		t.Fatalf("NotReconstructible = %v, want hosts among them", res.NotReconstructible)
	}
	_ = metadata.Host{}
}
