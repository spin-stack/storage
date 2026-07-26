package controlplane_test

import (
	"context"
	"fmt"
	"sync"
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

// TestRebuildRefusesACorruptManifest: a manifest that no longer matches its own
// digest is corrupt (INV-16 says a published one never changes). Writing a catalog
// row for it would give that corruption authority over restores.
func TestRebuildRefusesACorruptManifest(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	epochs := epoch.NewStore(store)

	if err := descriptor.Write(ctx, store, descriptor.Descriptor{
		VolumeID: rebuiltVol, SizeBytes: 1, BlockSize: 65536,
		Durability: lifecycle.DurabilityRemote, CurrentEpoch: 1, KEKID: "k", DEKWrapped: []byte{1},
	}); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Publish(ctx, store, snapshot.Manifest{
		SnapshotID: rebuiltSnap, VolumeID: rebuiltVol, Epoch: 1, TargetSequence: 7,
		RootDigest: "not-the-digest",
	}); err != nil {
		t.Fatal(err)
	}

	md := metasim.New(clk.Wall)
	term, _ := md.AcquireLeadership(ctx, "cp")
	if _, err := controlplane.RebuildMetadata(ctx, store, epochs, md, term); err == nil {
		t.Fatal("a manifest that fails its own digest must not become a catalog row")
	}
	if _, err := md.GetSnapshot(ctx, rebuiltSnap); err == nil {
		t.Fatal("the corrupt snapshot was written to the catalog anyway")
	}
}

// TestRebuildAddsMissingSnapshotsToAnExistingVolume: the two halves fail
// independently — a PITR restore can bring volumes back while the catalog is still
// short, and the rebuild has to fill that in rather than skipping the volume.
func TestRebuildAddsMissingSnapshotsToAnExistingVolume(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	epochs := epoch.NewStore(store)
	md := metasim.New(clk.Wall)
	term, _ := md.AcquireLeadership(ctx, "cp")

	if err := descriptor.Write(ctx, store, descriptor.Descriptor{
		VolumeID: rebuiltVol, SizeBytes: 1, BlockSize: 65536,
		Durability: lifecycle.DurabilityRemote, CurrentEpoch: 1, KEKID: "k", DEKWrapped: []byte{1},
	}); err != nil {
		t.Fatal(err)
	}
	m := snapshot.Manifest{SnapshotID: rebuiltSnap, VolumeID: rebuiltVol, Epoch: 1, TargetSequence: 7}
	m.RootDigest = snapshot.Digest(m.TargetSequence, m.Objects)
	if err := snapshot.Publish(ctx, store, m); err != nil {
		t.Fatal(err)
	}
	// The volume row survived; the catalog did not.
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: rebuiltVol, SizeBytes: 1, BlockSize: 65536, State: lifecycle.VolumeDetached,
		DEKWrapped: []byte{1}, KEKID: "k",
	}); err != nil {
		t.Fatal(err)
	}

	res, err := controlplane.RebuildMetadata(ctx, store, epochs, md, term)
	if err != nil {
		t.Fatal(err)
	}
	if res.Volumes != 0 || res.Snapshots != 1 {
		t.Fatalf("rebuild = %+v, want 0 volumes and 1 snapshot", res)
	}
	if _, err := md.GetSnapshot(ctx, rebuiltSnap); err != nil {
		t.Fatalf("the snapshot was not added to the existing volume: %v", err)
	}
}

// Finding 2 (medium). Two operators run rebuild-metadata at the same time — the
// ordinary shape of an incident, where whoever is awake runs the runbook. Both see
// ErrNotFound for the same volume and both INSERT; the loser used to abort with a
// unique-violation after having written an arbitrary prefix of the catalog, and
// returned a RebuildResult whose Volumes count reads exactly like a completed
// rebuild. Wave 1 made CreateVolume/CreateSnapshot converge on conflict instead of
// aborting; this pins that from the rebuild's side, which is the only place the
// consequence is visible.
func TestTwoConcurrentRebuildsBothComplete(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	epochs := epoch.NewStore(store)

	// Enough volumes, each with a snapshot, that the two runs genuinely interleave.
	const volumes = 8
	type want struct{ epoch int64 }
	expected := map[string]want{}
	for i := range volumes {
		volID := fmt.Sprintf("00000000-0000-7000-8000-0000000%05d", i)
		snapID := fmt.Sprintf("00000000-0000-7000-8000-0000001%05d", i)
		ep := int64(i + 1)
		if err := descriptor.Write(ctx, store, descriptor.Descriptor{
			VolumeID: volID, SizeBytes: int64(i+1) << 20, BlockSize: 65536,
			Durability: lifecycle.DurabilityRemote, CurrentEpoch: ep, KEKID: "k", DEKWrapped: []byte{byte(i)},
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := epochs.Init(ctx, volID, uint64(ep)); err != nil {
			t.Fatal(err)
		}
		m := snapshot.Manifest{SnapshotID: snapID, VolumeID: volID, Epoch: uint64(ep), TargetSequence: uint64(i)}
		m.RootDigest = snapshot.Digest(m.TargetSequence, m.Objects)
		if err := snapshot.Publish(ctx, store, m); err != nil {
			t.Fatal(err)
		}
		expected[volID] = want{epoch: ep}
	}

	md := metasim.New(clk.Wall)
	term, err := md.AcquireLeadership(ctx, "cp")
	if err != nil {
		t.Fatal(err)
	}

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		errs  []error
		start = make(chan struct{})
		res   [2]controlplane.RebuildResult
	)
	for i := range res {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release both operators at once
			r, err := controlplane.RebuildMetadata(ctx, store, epochs, md, term)
			mu.Lock()
			defer mu.Unlock()
			res[i] = r
			if err != nil {
				errs = append(errs, err)
			}
		}()
	}
	close(start)
	wg.Wait()

	for _, err := range errs {
		t.Fatalf("a concurrent rebuild aborted part-way through the catalog: %v", err)
	}
	for volID, w := range expected {
		v, err := md.GetVolume(ctx, volID)
		if err != nil {
			t.Fatalf("%s never made it into the catalog: %v", volID, err)
		}
		if v.CurrentEpoch != w.epoch {
			t.Fatalf("%s is at epoch %d, S3 says %d", volID, v.CurrentEpoch, w.epoch)
		}
	}
	// Neither run may claim more than exists: a count larger than the bucket holds
	// reads as a rebuild that found more than it did.
	for i, r := range res {
		if r.Volumes > volumes || r.Snapshots > volumes {
			t.Fatalf("run %d reported %+v, but S3 holds %d volumes", i, r, volumes)
		}
	}
}
