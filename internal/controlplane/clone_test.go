package controlplane_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/obs"
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
	clone, err := controlplane.Clone(ctx, md, store, placement.Policy{}, nil, term, snapID, cloneVol)
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
	if _, err := controlplane.Clone(ctx, md, store, placement.Policy{}, nil, stale, snapID, cloneVol); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("want ErrStaleTerm, got %v", err)
	}
}

func TestCloneFromMissingSnapshotFails(t *testing.T) {
	ctx := t.Context()
	md, store, term := cpStore(t)
	if _, err := controlplane.Clone(ctx, md, store, placement.Policy{}, nil, term, "no-such-snap", cloneVol); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("clone from a missing snapshot: want ErrNotFound, got %v", err)
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

	if _, err := controlplane.Clone(ctx, md, store, placement.Policy{}, nil, term, snapID, cloneVol); !errors.Is(err, placement.ErrNoCapacity) {
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

			clone, err := controlplane.Clone(ctx, md, store, placement.Policy{}, nil, term, snapID, cloneVol)
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
	if _, err := controlplane.Clone(ctx, md, store, placement.Policy{}, nil, term, snapID, cloneVol); err == nil {
		t.Fatal("a snapshot that was never published was accepted as a clone source")
	}
	if _, err := md.GetVolume(ctx, cloneVol); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("a refused clone must not create the volume: %v", err)
	}
}

// TestALineageStopsGrowingAtTheCeiling drives the production path five times and then a
// sixth, which is the only way to see the ceiling as an operator does: a lineage that
// grows until it does not.
//
// Nothing here asserts on a field Clone set. The refusal is judged by what the fleet
// holds afterwards — no row for the volume that was refused, no descriptor under its key,
// not one byte charged to the host it would have landed on, and no volume anywhere in the
// catalog past the ceiling — because a gate that returns the right error and writes the
// row anyway satisfies every assertion on `err`.
//
// The per-step depth is read back from a collector rather than from the returned struct.
// `chain_depth` is a series the §26.2 catalog has declared since before anything recorded
// it, and "a name arrived" would pass on a producer wired to the wrong number, which is
// the failure this repository has actually shipped.
func TestALineageStopsGrowingAtTheCeiling(t *testing.T) {
	ctx := t.Context()
	md, store, term := cpStore(t)
	addHost(t, md, term, cloneHostA, lifecycle.HostActive, 1<<40)

	root := ids.New().String()
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: root, SizeBytes: 1 << 30, BlockSize: 65536, DEKKeyID: 1,
		State: lifecycle.VolumeActive, DEKWrapped: []byte{7}, KEKID: "kek",
	}, nil); err != nil {
		t.Fatal(err)
	}

	snap := publishSnapshotOf(t, md, term, root)
	deepest := root
	for depth := 1; depth <= controlplane.MaxChainDepth; depth++ {
		// A fresh collector per link: the samples differ only in their volume label, and
		// a single reader would leave the assertion at the mercy of which data point the
		// SDK returned last.
		prov, err := obs.NewTestProvider("control-plane")
		if err != nil {
			t.Fatal(err)
		}
		clone, err := controlplane.Clone(ctx, md, store, placement.Policy{}, prov.Recorder(), term, snap, ids.New().String())
		if err != nil {
			t.Fatalf("a clone at depth %d was refused below the ceiling of %d: %v", depth, controlplane.MaxChainDepth, err)
		}
		gauges, err := prov.GaugeValues(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got := gauges["chain_depth"]; got != float64(depth) {
			t.Fatalf("the collector decoded chain_depth=%v for the clone the Control Plane created at depth %d", got, depth)
		}
		snap, deepest = publishSnapshotOf(t, md, term, clone.VolumeID), clone.VolumeID
	}

	before, err := md.GetHost(ctx, cloneHostA)
	if err != nil {
		t.Fatal(err)
	}
	refused := ids.New().String()
	prov, err := obs.NewTestProvider("control-plane")
	if err != nil {
		t.Fatal(err)
	}
	// Errorf and not Fatalf, so that a build with no ceiling reports what it did rather
	// than only that it did not refuse: the four assertions below are the ones that say
	// a lineage grew past the limit, and they are the point of the test.
	switch _, cerr := controlplane.Clone(ctx, md, store, placement.Policy{}, prov.Recorder(), term, snap, refused); {
	case !errors.Is(cerr, controlplane.ErrChainTooDeep):
		t.Errorf("a clone of a volume at the ceiling: want ErrChainTooDeep, got %v", cerr)
	// The message is the operator's whole interface to this refusal: it has to name the
	// verb that gets them back under the ceiling and the volume to run it on.
	case !strings.Contains(cerr.Error(), "FLATTEN") || !strings.Contains(cerr.Error(), deepest):
		t.Errorf("the refusal tells an operator nothing to do: %v", cerr)
	}

	if _, err := md.GetVolume(ctx, refused); !errors.Is(err, metadata.ErrNotFound) {
		t.Errorf("the refused clone left a row behind: %v", err)
	}
	if _, err := store.Head(ctx, descriptor.Key(refused)); !errors.Is(err, objectstore.ErrNotFound) {
		t.Errorf("the refused clone left a descriptor in the bucket: %v", err)
	}
	after, err := md.GetHost(ctx, cloneHostA)
	if err != nil {
		t.Fatal(err)
	}
	if after.NVMeCommittedBytes != before.NVMeCommittedBytes {
		t.Errorf("the refused clone charged %d bytes to %s", after.NVMeCommittedBytes-before.NVMeCommittedBytes, cloneHostA)
	}
	if series, err := prov.CollectedMetrics(ctx); err != nil {
		t.Fatal(err)
	} else if series["chain_depth"] {
		t.Error("the refused clone reported a chain depth for a volume that does not exist")
	}

	// And the lineage really did reach the ceiling, so the loop above was not five clones
	// of the root: this is the assertion a missing refusal takes past MaxChainDepth.
	vols, err := md.ListVolumes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var deepestFound int32
	for _, v := range vols {
		deepestFound = max(deepestFound, v.ChainDepth)
	}
	if deepestFound != controlplane.MaxChainDepth {
		t.Fatalf("the catalog's deepest volume is at depth %d, the ceiling is %d", deepestFound, controlplane.MaxChainDepth)
	}
}

// publishSnapshotOf records a PUBLISHED snapshot of a volume — what an Agent's publish
// leaves behind, and the only shape Clone accepts as a source — and returns its id.
func publishSnapshotOf(t *testing.T, md metadata.Store, term int64, volumeID string) string {
	t.Helper()
	id := ids.New().String()
	if err := md.CreateSnapshot(t.Context(), term, metadata.Snapshot{
		SnapshotID: id, VolumeID: volumeID, Epoch: 1, TargetSequence: 10,
		RootDigest: "abc", SourceHostID: cloneHostA,
		State: lifecycle.SnapshotPublished, RequestID: ids.New().String(),
	}); err != nil {
		t.Fatal(err)
	}
	return id
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
