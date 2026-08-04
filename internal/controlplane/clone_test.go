package controlplane_test

import (
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/placement"
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
		VolumeID: parentVol, SizeBytes: 1 << 30, BlockSize: 65536,
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

	addHost(t, md, term, cloneHostA, lifecycle.HostActive, 1<<40)
	clone, err := controlplane.Clone(ctx, md, store, placement.Policy{}, term, snapID, cloneVol)
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

	addHost(t, md, term, cloneHostA, lifecycle.HostActive, 1<<40)

	stale := term
	if _, err := md.AcquireLeadership(ctx, "cp-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := controlplane.Clone(ctx, md, store, placement.Policy{}, stale, snapID, cloneVol); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("want ErrStaleTerm, got %v", err)
	}
}

func TestCloneFromMissingSnapshotFails(t *testing.T) {
	ctx := t.Context()
	md, store, term := cpStore(t)
	if _, err := controlplane.Clone(ctx, md, store, placement.Policy{}, term, "no-such-snap", cloneVol); !errors.Is(err, metadata.ErrNotFound) {
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
//
// The refusal now comes from placement rather than from CreateVolume's predicate,
// because Clone computes its own bound from the host it chose — so a bound the clone
// cannot fit is a fleet with no room, and Choose says so first. The predicate itself is
// covered where it lives, in metadatatest's capacity case.
func TestAFailedCloneChargesNothing(t *testing.T) {
	ctx := t.Context()
	md, store, term := cpStore(t)
	// A host with no room for the clone: total is smaller than the volume.
	addHost(t, md, term, cloneHostA, lifecycle.HostActive, 1<<20)
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: parentVol, SizeBytes: 1 << 30, BlockSize: 65536, DEKKeyID: 1,
		State: lifecycle.VolumeActive, DEKWrapped: []byte{7}, KEKID: "kek",
	}, nil); err != nil {
		t.Fatal(err)
	}
	createSnapshot(t, md, term, cloneHostA)

	if _, err := controlplane.Clone(ctx, md, store, placement.Policy{}, term, snapID, cloneVol); !errors.Is(err, placement.ErrNoCapacity) {
		t.Fatalf("want ErrNoCapacity, got %v", err)
	}
	if dst, _ := md.GetHost(ctx, cloneHostA); dst.NVMeCommittedBytes != 0 {
		t.Fatalf("a refused clone leaked %d committed bytes", dst.NVMeCommittedBytes)
	}
	if _, err := md.GetVolume(ctx, cloneVol); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("a refused clone must not create the volume: %v", err)
	}
}

// §20's placement rule 1, and under ADR-0026 most of the boot-time story: a cross-host
// clone pays a full download from the object store, a same-host clone reads local NVMe.
// The host that took the snapshot is the one that still has the data, and it is a fact
// rather than a guess because that host stamped it when it published (increment 3b).
//
// The second case is the half that must not be assumed away. Same-host is a preference:
// the source can be cordoned, full or gone, and coupling scheduling to a host with no
// obligation to be up would turn a fast path into an outage.
func TestACloneStartsWhereTheDataAlreadyIs(t *testing.T) {
	const sourceHost, otherHost = cloneHostA, "00000000-0000-7000-8000-0000000000d2"
	tests := []struct {
		name       string
		sourceStat lifecycle.HostState
		sourceCap  int64
		want       string
	}{
		{"the source host holds the data", lifecycle.HostActive, 1 << 40, sourceHost},
		// Emptier than the source, so a policy that merely balanced would pick it in
		// both rows and this table would prove nothing.
		{"the source is cordoned", lifecycle.HostCordoned, 1 << 40, otherHost},
		{"the source is full", lifecycle.HostActive, 1 << 20, otherHost},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			md, store, term := cpStore(t)
			addHost(t, md, term, sourceHost, tc.sourceStat, tc.sourceCap)
			addHost(t, md, term, otherHost, lifecycle.HostActive, 1<<41)
			if err := md.CreateVolume(ctx, term, metadata.Volume{
				VolumeID: parentVol, SizeBytes: 1 << 30, BlockSize: 65536, DEKKeyID: 1,
				State: lifecycle.VolumeActive, DEKWrapped: []byte{7}, KEKID: "kek",
			}, nil); err != nil {
				t.Fatal(err)
			}
			createSnapshot(t, md, term, sourceHost)

			clone, err := controlplane.Clone(ctx, md, store, placement.Policy{}, term, snapID, cloneVol)
			if err != nil {
				t.Fatalf("Clone: %v", err)
			}
			if clone.PrimaryHostID != tc.want {
				t.Fatalf("clone placed on %s, want %s", clone.PrimaryHostID, tc.want)
			}
			// And the bytes are charged where it landed, not where it was asked for.
			h, _ := md.GetHost(ctx, tc.want)
			if h.NVMeCommittedBytes != 1<<30 {
				t.Fatalf("host %s committed %d bytes, want the clone's %d", tc.want, h.NVMeCommittedBytes, 1<<30)
			}
		})
	}
}

// A snapshot that is not PUBLISHED has nothing written for a clone to read: the objects
// are still being uploaded, or the upload failed. Cloning it would produce a volume that
// reads zeros for everything its parent wrote — DEV-0007's shape, reached through the
// catalog instead of through a missing field.
func TestCloneRefusesASnapshotThatWasNeverPublished(t *testing.T) {
	ctx := t.Context()
	md, store, term := cpStore(t)
	addHost(t, md, term, cloneHostA, lifecycle.HostActive, 1<<40)
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: parentVol, SizeBytes: 1 << 30, BlockSize: 65536, DEKKeyID: 1,
		State: lifecycle.VolumeActive, DEKWrapped: []byte{7}, KEKID: "kek",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := md.CreateSnapshot(ctx, term, metadata.Snapshot{
		SnapshotID: snapID, VolumeID: parentVol, Epoch: 1,
		State: lifecycle.SnapshotCreating, RequestID: reqID,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := controlplane.Clone(ctx, md, store, placement.Policy{}, term, snapID, cloneVol); err == nil {
		t.Fatal("a snapshot that was never published was accepted as a clone source")
	}
	if _, err := md.GetVolume(ctx, cloneVol); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("a refused clone must not create the volume: %v", err)
	}
}

func addHost(t *testing.T, md metadata.Store, term int64, id string, state lifecycle.HostState, total int64) {
	t.Helper()
	if err := md.UpsertHost(t.Context(), term, metadata.Host{
		HostID: id, State: state, NVMeTotalBytes: total,
	}); err != nil {
		t.Fatal(err)
	}
}

func createSnapshot(t *testing.T, md metadata.Store, term int64, sourceHost string) {
	t.Helper()
	if err := md.CreateSnapshot(t.Context(), term, metadata.Snapshot{
		SnapshotID: snapID, VolumeID: parentVol, Epoch: 1, TargetSequence: 10,
		RootDigest: "abc", SourceHostID: sourceHost,
		State: lifecycle.SnapshotPublished, RequestID: reqID,
	}); err != nil {
		t.Fatal(err)
	}
}
