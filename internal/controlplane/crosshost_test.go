package controlplane_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/materialize"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/placement"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/snapshot"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

const (
	destHost = "00000000-0000-7000-8000-0000000000d2"
	volSize  = int64(1) << 30
)

// clonePolicy is the §28.2 bound a cross-host clone reserves under — the same rule
// placement used to admit the destination in the first place, passed to the write so
// the ceiling still means something when two placements raced for it.
var clonePolicy = placement.Policy{MaxOversubscription: 2.0}

// crossHostWorld sets up a volume with durable data in S3, its published snapshot,
// and a metadata store with a source and a destination host.
func crossHostWorld(t *testing.T) (metadata.Store, int64, *sim.ObjectStore, snapshot.Manifest) {
	t.Helper()
	ctx := context.Background()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := sim.NewObjectStore()
	md := metasim.New(clk.Wall)
	term, _ := md.AcquireLeadership(ctx, "cp")

	var vol [16]byte
	vol[6], vol[8] = 0x70, 0x80
	vol[15] = 1
	volID := format.UUIDString(vol)

	for _, h := range []string{cloneHostA, destHost} {
		if err := md.UpsertHost(ctx, term, metadata.Host{
			HostID: h, State: lifecycle.HostActive, NVMeTotalBytes: 10 * volSize,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: volID, SizeBytes: volSize, BlockSize: 65536, Durability: lifecycle.DurabilityRemote,
		State: lifecycle.VolumeActive, PrimaryHostID: cloneHostA, DEKWrapped: []byte{7}, KEKID: "kek",
	}); err != nil {
		t.Fatal(err)
	}

	d := sim.NewDisk()
	f, err := d.Create("wal/active.wal")
	if err != nil {
		t.Fatal(err)
	}
	l := wal.NewLog(f, clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), leaseOK{})
	if _, err := l.Write(0, []byte("source-data"), 0); err != nil {
		t.Fatal(err)
	}
	m, _, err := snapshot.NewSnapshotter(store, clk).Create(ctx, l, vol, 1, snapID, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := md.CreateSnapshot(ctx, term, metadata.Snapshot{
		SnapshotID: snapID, VolumeID: volID, Epoch: 1, TargetSequence: int64(m.TargetSequence),
		RootDigest: m.RootDigest, SourceHostID: cloneHostA, State: lifecycle.SnapshotPublished, RequestID: reqID,
	}); err != nil {
		t.Fatal(err)
	}
	return md, term, store, m
}

// TestCloneCrossHostMaterializesOnDestination: the clone lands on a host that never
// had the data, rebuilt from S3 alone, with the destination's capacity committed.
func TestCloneCrossHostMaterializesOnDestination(t *testing.T) {
	ctx := context.Background()
	md, term, store, m := crossHostWorld(t)

	res, err := controlplane.CloneCrossHost(ctx, md, materialize.New(store, nil, nil),
		clonePolicy, term, m.SnapshotID, cloneVol, destHost)
	if err != nil {
		t.Fatalf("cross-host clone: %v", err)
	}
	if res.Volume.PrimaryHostID != destHost || res.Volume.ChainDepth != 1 {
		t.Fatalf("clone shape wrong: %+v", res.Volume)
	}
	buf := make([]byte, 11)
	res.View.Read(0, buf)
	if string(buf) != "source-data" {
		t.Fatalf("materialized state = %q, want source-data", buf)
	}
	if res.Progress.Objects == 0 || res.Progress.Bytes == 0 {
		t.Fatalf("progress not reported: %+v", res.Progress)
	}
	// Capacity is committed on the destination, not on the source.
	dst, _ := md.GetHost(ctx, destHost)
	if dst.NVMeCommittedBytes != volSize {
		t.Fatalf("destination committed = %d, want %d", dst.NVMeCommittedBytes, volSize)
	}
	if src, _ := md.GetHost(ctx, cloneHostA); src.NVMeCommittedBytes != 0 {
		t.Fatalf("source committed = %d, want 0", src.NVMeCommittedBytes)
	}
}

// TestCloneCrossHostReleasesCapacityOnFailure: a failed materialization must not
// leak a reservation, or the fleet slowly loses placeable capacity (§28.2).
func TestCloneCrossHostReleasesCapacityOnFailure(t *testing.T) {
	ctx := context.Background()
	md, term, store, m := crossHostWorld(t)

	// The snapshot references an object that is no longer there.
	if err := store.Delete(ctx, m.Objects[0]); err != nil {
		t.Fatal(err)
	}
	_, err := controlplane.CloneCrossHost(ctx, md, materialize.New(store, nil, nil),
		clonePolicy, term, m.SnapshotID, cloneVol, destHost)
	if !errors.Is(err, materialize.ErrMissingObject) {
		t.Fatalf("want ErrMissingObject, got %v", err)
	}
	if dst, _ := md.GetHost(ctx, destHost); dst.NVMeCommittedBytes != 0 {
		t.Fatalf("failed clone leaked %d committed bytes", dst.NVMeCommittedBytes)
	}
	if _, err := md.GetVolume(ctx, cloneVol); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("a failed cross-host clone must not create the volume: %v", err)
	}
}

// TestCloneCrossHostFromOrphanSnapshotFails: a snapshot whose volume is gone (a
// catalog left inconsistent by a partial rebuild) cannot be cloned.
func TestCloneCrossHostFromOrphanSnapshotFails(t *testing.T) {
	ctx := context.Background()
	md, term, store, _ := crossHostWorld(t)

	const orphanSnap = "00000000-0000-7000-8000-0000000000b9"
	if err := md.CreateSnapshot(ctx, term, metadata.Snapshot{
		SnapshotID: orphanSnap, VolumeID: "00000000-0000-7000-8000-0000000000ba",
		Epoch: 1, TargetSequence: 1, RootDigest: "d", State: lifecycle.SnapshotPublished,
		RequestID: "00000000-0000-7000-8000-0000000000bb",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := controlplane.CloneCrossHost(ctx, md, materialize.New(store, nil, nil),
		clonePolicy, term, orphanSnap, cloneVol, destHost); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if dst, _ := md.GetHost(ctx, destHost); dst.NVMeCommittedBytes != 0 {
		t.Fatalf("committed %d bytes for an orphan snapshot", dst.NVMeCommittedBytes)
	}
}

// TestCloneCrossHostToUnknownHostFails: the destination is read before anything is
// reserved, so no materialization work is started for a host that does not exist.
func TestCloneCrossHostToUnknownHostFails(t *testing.T) {
	ctx := context.Background()
	md, term, store, m := crossHostWorld(t)

	if _, err := controlplane.CloneCrossHost(ctx, md, materialize.New(store, nil, nil),
		clonePolicy, term, m.SnapshotID, cloneVol, "00000000-0000-7000-8000-00000000dead"); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if _, err := md.GetVolume(ctx, cloneVol); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("clone volume must not exist: %v", err)
	}
}

// TestCloneCrossHostFromMissingSnapshotFails: nothing is committed when the source
// does not even exist.
func TestCloneCrossHostFromMissingSnapshotFails(t *testing.T) {
	ctx := context.Background()
	md, term, store, _ := crossHostWorld(t)

	if _, err := controlplane.CloneCrossHost(ctx, md, materialize.New(store, nil, nil),
		clonePolicy, term, "no-such-snap", cloneVol, destHost); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if dst, _ := md.GetHost(ctx, destHost); dst.NVMeCommittedBytes != 0 {
		t.Fatalf("committed %d bytes for a clone that never started", dst.NVMeCommittedBytes)
	}
}

// leaseOK is the fence for tests that are not about fencing: remote durability
// requires a lease checker (DEV-0004), and these hold a valid one.
type leaseOK struct{}

func (leaseOK) Valid() bool { return true }
