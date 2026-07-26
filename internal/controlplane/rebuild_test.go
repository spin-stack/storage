package controlplane_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// TestRebuildMetadataFromS3 is INV-20: with PostgreSQL wiped, rebuild-metadata
// reconstructs the volumes from the self-describing S3 layout.
func TestRebuildMetadataFromS3(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	epochs := epoch.NewStore(store)

	// Two volumes exist in S3 (descriptor + epoch object), nothing in PG.
	descs := []descriptor.Descriptor{
		{VolumeID: volID, SizeBytes: 1 << 30, BlockSize: 65536, Durability: lifecycle.DurabilityRemote, KEKID: "k1", DEKWrapped: []byte{1, 2}},
		{VolumeID: host1 /*any v7 id*/, SizeBytes: 2 << 30, BlockSize: 65536, Durability: "local", KEKID: "k2", DEKWrapped: []byte{3}},
	}
	epochsByVol := map[string]uint64{volID: 5, host1: 0}
	for _, d := range descs {
		if err := descriptor.Write(ctx, store, d); err != nil {
			t.Fatal(err)
		}
		if _, err := epochs.Init(ctx, d.VolumeID, epochsByVol[d.VolumeID]); err != nil {
			t.Fatal(err)
		}
	}

	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	md := metasim.New(clk.Wall)
	term, _ := md.AcquireLeadership(ctx, "cp")

	n, err := controlplane.RebuildMetadata(ctx, store, epochs, md, term)
	if err != nil {
		t.Fatal(err)
	}
	if n.Volumes != 2 {
		t.Fatalf("rebuilt %+v, want 2 volumes", n)
	}

	// The rebuilt volume takes its epoch from the authoritative epoch object.
	v, err := md.GetVolume(ctx, volID)
	if err != nil {
		t.Fatal(err)
	}
	if v.CurrentEpoch != 5 || v.SizeBytes != 1<<30 || v.KEKID != "k1" || v.State != lifecycle.VolumeDetached {
		t.Fatalf("rebuilt volume wrong: %+v", v)
	}

	// Rebuild is idempotent: a second pass adds nothing.
	if n2, _ := controlplane.RebuildMetadata(ctx, store, epochs, md, term); n2.Volumes != 0 {
		t.Fatalf("second rebuild should add nothing, added %+v", n2)
	}
}

func TestDescriptorRoundTrip(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	d := descriptor.Descriptor{VolumeID: volID, SizeBytes: 42, BlockSize: 65536, Durability: lifecycle.DurabilityRemote, CurrentEpoch: 3, KEKID: "k", DEKWrapped: []byte{9}}
	if err := descriptor.Write(ctx, store, d); err != nil {
		t.Fatal(err)
	}
	got, err := descriptor.Read(ctx, store, volID)
	if err != nil {
		t.Fatal(err)
	}
	if got.VolumeID != d.VolumeID || got.SizeBytes != d.SizeBytes || got.CurrentEpoch != d.CurrentEpoch ||
		got.KEKID != d.KEKID || string(got.DEKWrapped) != string(d.DEKWrapped) {
		t.Fatalf("descriptor round-trip: got %+v want %+v", got, d)
	}
	ids, _ := descriptor.ListVolumeIDs(ctx, store)
	if len(ids) != 1 || ids[0] != volID {
		t.Fatalf("list volume ids = %v", ids)
	}
	// Reading a missing descriptor errors.
	if _, err := descriptor.Read(ctx, store, "no-such-volume"); err == nil {
		t.Fatal("reading a missing descriptor should error")
	}
	// Listing an empty store yields nothing.
	if empty, _ := descriptor.ListVolumeIDs(ctx, sim.NewObjectStore()); len(empty) != 0 {
		t.Fatalf("empty store should list no volumes, got %v", empty)
	}
}

// Finding 1 (high). rebuild-metadata exists for exactly one situation: PostgreSQL
// lost or rewound, S3 intact. A PITR restore is the ordinary way to get there, and a
// PITR restore rewinds `volumes.current_epoch` while the epoch object in S3 — the
// fencing authority (§12.4) — still carries the epoch that was actually granted.
//
// The rebuild skipped every volume whose row was already present, so it reported
// success with Volumes:0 and an empty NotReconstructible for a row it left
// disagreeing with S3. Nothing else can repair it either: Promote refuses outright
// when `stored > pgEpoch` (§12.3 step 4), and BumpVolumeEpoch only ever grants
// pgEpoch+1, so the volume is unattachable forever — during the exact incident
// rebuild-metadata is the tool for.

const (
	staleVol  = "00000000-0000-7000-8000-00000000cc01"
	staleVol2 = "00000000-0000-7000-8000-00000000cc02"
	staleHost = "00000000-0000-7000-8000-00000000cc03"
)

// TestRebuildRepairsAVolumeRowLeftBehindByS3 drives the whole incident: the row says
// epoch 2, S3 says 5, a promotion is impossible, the operator runs the rebuild, and
// the promotion must work afterwards.
func TestRebuildRepairsAVolumeRowLeftBehindByS3(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	epochs := epoch.NewStore(store)

	// S3: the volume is at epoch 5 and says so in both places.
	if err := descriptor.Write(ctx, store, descriptor.Descriptor{
		VolumeID: staleVol, SizeBytes: 8 << 30, BlockSize: 65536,
		Durability: lifecycle.DurabilityRemote, CurrentEpoch: 5, KEKID: "kek-1",
		DEKWrapped: []byte{7, 7},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := epochs.Init(ctx, staleVol, 5); err != nil {
		t.Fatal(err)
	}

	// PostgreSQL: restored from a backup taken before epochs 3, 4 and 5 were granted.
	md := metasim.New(clk.Wall)
	term, err := md.AcquireLeadership(ctx, "cp")
	if err != nil {
		t.Fatal(err)
	}
	if err := md.UpsertHost(ctx, term, metadata.Host{
		HostID: staleHost, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
	}); err != nil {
		t.Fatal(err)
	}
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: staleVol, SizeBytes: 8 << 30, BlockSize: 65536,
		Durability: lifecycle.DurabilityRemote, CurrentEpoch: 2,
		State: lifecycle.VolumeActive, PrimaryHostID: staleHost,
		KEKID: "kek-1", DEKWrapped: []byte{7, 7},
	}); err != nil {
		t.Fatal(err)
	}

	// The symptom the operator sees before running anything.
	prom := controlplane.NewPromoter(md, epochs, clk, 30*time.Second, time.Second)
	if _, err := prom.Promote(ctx, term, staleVol, clk.Wall(), staleHost); !errors.Is(err, controlplane.ErrEpochConflict) {
		t.Fatalf("setup: promote before the rebuild = %v, want ErrEpochConflict", err)
	}

	res, err := controlplane.RebuildMetadata(ctx, store, epochs, md, term)
	if err != nil {
		t.Fatal(err)
	}

	v, err := md.GetVolume(ctx, staleVol)
	if err != nil {
		t.Fatal(err)
	}
	if v.CurrentEpoch != 5 && !slices.Contains(res.NotReconstructible, staleVol) {
		t.Fatalf("the row is still at epoch %d while S3 is at 5, and the rebuild reported %+v — "+
			"the volume is unattachable and nothing says so", v.CurrentEpoch, res)
	}
	// A count of rows *created* says nothing about a row that was repaired, and an
	// operator reading Volumes:0 would move on.
	if !slices.Contains(res.Repaired, staleVol) {
		t.Fatalf("the rebuild corrected the row but reported %+v", res)
	}
	// The row must not be *rewritten* wholesale: state and ownership are the §7
	// machine's, not S3's.
	if v.State != lifecycle.VolumeActive || v.PrimaryHostID != staleHost {
		t.Fatalf("the rebuild overwrote live ownership/state: %+v", v)
	}
	if _, err := prom.Promote(ctx, term, staleVol, clk.Wall(), staleHost); err != nil {
		t.Fatalf("promote after the rebuild: %v — the volume is still unattachable", err)
	}
}

// TestRebuildResumesAfterALeadershipChangeMidLoop: there is no transaction around
// the creation loop, so a rebuild can die having written an arbitrary prefix of the
// catalog. A leadership change is the ordinary way — the operator's CP loses the
// election halfway through — and every mutation is term-guarded, so the rest of the
// loop fails with ErrStaleTerm. The re-run under the new term has to finish the job
// rather than trip over what the first run already wrote.
func TestRebuildResumesAfterALeadershipChangeMidLoop(t *testing.T) {
	ctx := t.Context()
	inner := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	epochs := epoch.NewStore(inner)

	descs := []descriptor.Descriptor{
		{VolumeID: staleVol, SizeBytes: 1 << 30, BlockSize: 65536, Durability: lifecycle.DurabilityRemote, CurrentEpoch: 4, KEKID: "k1", DEKWrapped: []byte{1}},
		{VolumeID: staleVol2, SizeBytes: 2 << 30, BlockSize: 65536, Durability: lifecycle.DurabilityRemote, CurrentEpoch: 9, KEKID: "k2", DEKWrapped: []byte{2}},
	}
	for _, d := range descs {
		if err := descriptor.Write(ctx, inner, d); err != nil {
			t.Fatal(err)
		}
		if _, err := epochs.Init(ctx, d.VolumeID, uint64(d.CurrentEpoch)); err != nil {
			t.Fatal(err)
		}
	}

	md := metasim.New(clk.Wall)
	term, err := md.AcquireLeadership(ctx, "cp-1")
	if err != nil {
		t.Fatal(err)
	}

	// Another Control Plane wins the election while this rebuild is between volumes.
	store := &rebuildHookStore{ObjectStore: inner}
	store.onGet = func(key string) error {
		if key == descriptor.Key(staleVol2) {
			store.onGet = nil
			_, err := md.AcquireLeadership(ctx, "cp-2")
			return err
		}
		return nil
	}

	partial, err := controlplane.RebuildMetadata(ctx, store, epoch.NewStore(store), md, term)
	if !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("a rebuild that lost leadership mid-loop = %v (%+v), want ErrStaleTerm", err, partial)
	}

	// The operator re-runs it under the CP that now holds leadership.
	newTerm, err := md.GetLeader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := controlplane.RebuildMetadata(ctx, inner, epochs, md, newTerm.Term)
	if err != nil {
		t.Fatalf("the re-run must finish what the interrupted rebuild started: %v", err)
	}
	if res.Volumes != 1 {
		t.Fatalf("the re-run created %d volumes, want the one the first run never reached", res.Volumes)
	}
	for _, d := range descs {
		v, err := md.GetVolume(ctx, d.VolumeID)
		if err != nil {
			t.Fatalf("%s is missing after the re-run: %v", d.VolumeID, err)
		}
		if v.CurrentEpoch != d.CurrentEpoch {
			t.Fatalf("%s is at epoch %d, S3 says %d", d.VolumeID, v.CurrentEpoch, d.CurrentEpoch)
		}
	}
}

// rebuildHookStore lets a test change the world *between* two of the rebuild's own
// calls, which is where an interrupted rebuild lives.
type rebuildHookStore struct {
	*sim.ObjectStore
	onGet func(key string) error
}

func (h *rebuildHookStore) Get(ctx context.Context, key string) ([]byte, error) {
	if h.onGet != nil {
		if err := h.onGet(key); err != nil {
			return nil, err
		}
	}
	return h.ObjectStore.Get(ctx, key)
}

// TestRebuildNamesARowThatIsAheadOfS3: the other direction of the same disagreement.
// The row claims an epoch the object store never recorded — PostgreSQL bumped and
// the CAS never landed, or a descriptor was restored from an older backup. Rewinding
// a fencing token is not the rebuild's call (§12.4), so it must converge everything
// it can and say plainly that this one is not reconciled, rather than report a clean
// run over a volume nobody can promote.
func TestRebuildNamesARowThatIsAheadOfS3(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	epochs := epoch.NewStore(store)

	if err := descriptor.Write(ctx, store, descriptor.Descriptor{
		VolumeID: staleVol, SizeBytes: 1 << 30, BlockSize: 65536,
		Durability: lifecycle.DurabilityRemote, CurrentEpoch: 2, KEKID: "k", DEKWrapped: []byte{1},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := epochs.Init(ctx, staleVol, 2); err != nil {
		t.Fatal(err)
	}

	md := metasim.New(clk.Wall)
	term, err := md.AcquireLeadership(ctx, "cp")
	if err != nil {
		t.Fatal(err)
	}
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: staleVol, SizeBytes: 1 << 30, BlockSize: 65536,
		Durability: lifecycle.DurabilityRemote, CurrentEpoch: 7,
		State: lifecycle.VolumeDetached, KEKID: "k", DEKWrapped: []byte{1},
	}); err != nil {
		t.Fatal(err)
	}

	res, err := controlplane.RebuildMetadata(ctx, store, epochs, md, term)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(res.Conflicting, staleVol) {
		t.Fatalf("a row ahead of the epoch object was reported as a clean rebuild: %+v", res)
	}
	if slices.Contains(res.Repaired, staleVol) {
		t.Fatalf("the rebuild claimed to have repaired a row it cannot repair: %+v", res)
	}
	v, err := md.GetVolume(ctx, staleVol)
	if err != nil {
		t.Fatal(err)
	}
	if v.CurrentEpoch != 7 {
		t.Fatalf("the rebuild rewound the fencing token to %d", v.CurrentEpoch)
	}
}

// TestRebuildGrowsARowThatShrankUnderAPITR: the same failure in the size column. A
// restore that predates a resize leaves the row smaller than the volume actually is,
// and the guest addresses blocks past the end of it.
func TestRebuildGrowsARowThatShrankUnderAPITR(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	epochs := epoch.NewStore(store)

	if err := descriptor.Write(ctx, store, descriptor.Descriptor{
		VolumeID: staleVol, SizeBytes: 16 << 30, BlockSize: 65536,
		Durability: lifecycle.DurabilityRemote, CurrentEpoch: 1, KEKID: "k", DEKWrapped: []byte{1},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := epochs.Init(ctx, staleVol, 1); err != nil {
		t.Fatal(err)
	}

	md := metasim.New(clk.Wall)
	term, err := md.AcquireLeadership(ctx, "cp")
	if err != nil {
		t.Fatal(err)
	}
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: staleVol, SizeBytes: 4 << 30, BlockSize: 65536,
		Durability: lifecycle.DurabilityRemote, CurrentEpoch: 1,
		State: lifecycle.VolumeDetached, KEKID: "k", DEKWrapped: []byte{1},
	}); err != nil {
		t.Fatal(err)
	}

	res, err := controlplane.RebuildMetadata(ctx, store, epochs, md, term)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(res.Repaired, staleVol) {
		t.Fatalf("a row smaller than the volume was reported as a clean rebuild: %+v", res)
	}
	v, err := md.GetVolume(ctx, staleVol)
	if err != nil {
		t.Fatal(err)
	}
	if v.SizeBytes != 16<<30 {
		t.Fatalf("size = %d, want the descriptor's %d", v.SizeBytes, int64(16<<30))
	}
}
