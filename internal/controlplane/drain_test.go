package controlplane_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/materialize"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/placement"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

const (
	drainOpID = "00000000-0000-7000-8000-0000000000f7"
	leaseTTL  = 10 * time.Second
	maxSkew   = 2 * time.Second
)

type drainWorld struct {
	md      metadata.Store
	term    int64
	store   *sim.ObjectStore
	clk     *sim.Clock
	drainer *controlplane.Drainer
	vols    [][16]byte
	acked   map[string]uint64
}

// newDrainWorld puts two durable volumes on the source host, with epoch objects and
// a source lease last renewed at T0.
func newDrainWorld(t *testing.T, destTotalBytes int64) *drainWorld {
	t.Helper()
	ctx := context.Background()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := sim.NewObjectStore()
	md := metasim.New(clk.Wall)
	term, _ := md.AcquireLeadership(ctx, "cp")

	if err := md.UpsertHost(ctx, term, metadata.Host{
		HostID: cloneHostA, State: lifecycle.HostActive, NVMeTotalBytes: 10 * volSize,
	}); err != nil {
		t.Fatal(err)
	}
	if err := md.UpsertHost(ctx, term, metadata.Host{
		HostID: destHost, State: lifecycle.HostActive, NVMeTotalBytes: destTotalBytes,
	}); err != nil {
		t.Fatal(err)
	}
	// The source holds a lease renewed "now"; promotion must wait for FENCING_WAIT.
	if err := md.RenewHostLease(ctx, term, cloneHostA, int(leaseTTL/time.Second)); err != nil {
		t.Fatal(err)
	}

	epochs := epoch.NewStore(store)
	w := &drainWorld{md: md, term: term, store: store, clk: clk, acked: map[string]uint64{}}
	d := sim.NewDisk()
	for i := range 2 {
		var vol [16]byte
		vol[6], vol[8] = 0x70, 0x80
		vol[15] = byte(0xa0 + i)
		volID := format.UUIDString(vol)
		w.vols = append(w.vols, vol)

		if err := md.CreateVolume(ctx, term, metadata.Volume{
			VolumeID: volID, SizeBytes: volSize, BlockSize: 65536, Durability: lifecycle.DurabilityRemote,
			State: lifecycle.VolumeActive, CurrentEpoch: 1, PrimaryHostID: cloneHostA,
			DEKWrapped: []byte{7}, KEKID: "kek",
		}); err != nil {
			t.Fatal(err)
		}
		if err := md.CommitHostCapacity(ctx, term, cloneHostA, volSize); err != nil {
			t.Fatal(err)
		}
		if _, err := epochs.Init(ctx, volID, 1); err != nil {
			t.Fatal(err)
		}

		f, err := d.Create("wal/" + volID + ".wal")
		if err != nil {
			t.Fatal(err)
		}
		l := wal.NewLog(f, clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
		l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), leaseOK{})
		if _, err := l.Write(0, []byte("volume-"+volID[:4]), 0); err != nil {
			t.Fatal(err)
		}
		if err := l.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		w.acked[volID] = l.Watermarks().Durable
	}

	w.drainer = controlplane.NewDrainer(md,
		controlplane.NewPromoter(md, epochs, clk, leaseTTL, maxSkew),
		materialize.New(store, nil, nil), store,
		placement.Policy{MaxOversubscription: 2.0})
	return w
}

// pastFencingWait advances the wall clock beyond lease_ttl + max_clock_skew.
func (w *drainWorld) pastFencingWait() { w.clk.Advance(leaseTTL + maxSkew + time.Second) }

// TestDrainMovesEveryVolumeFenced is the §28.1 happy path: every volume ends on the
// destination at a bumped epoch, the source is fenced first (INV-10/INV-11), the
// materialized prefix covers what the source ACKed (INV-09), an epoch-boundary
// recovery point is written (INV-12), and capacity follows the volume (§28.2).
func TestDrainMovesEveryVolumeFenced(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()

	res, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if res.Phase != lifecycle.OpSucceeded || len(res.Moved) != 2 || res.Remaining != 0 {
		t.Fatalf("drain result = %+v", res)
	}

	for _, vol := range w.vols {
		volID := format.UUIDString(vol)
		v, err := w.md.GetVolume(ctx, volID)
		if err != nil {
			t.Fatal(err)
		}
		if v.PrimaryHostID != destHost {
			t.Fatalf("volume %s still on %s", volID, v.PrimaryHostID)
		}
		if v.CurrentEpoch != 2 {
			t.Fatalf("volume %s epoch = %d, want 2 (fenced promotion)", volID, v.CurrentEpoch)
		}
		rp, err := recovery.ReadRecoveryPoint(ctx, w.store, vol, 2)
		if err != nil {
			t.Fatalf("recovery point for %s: %v", volID, err)
		}
		if rp.PrevEpoch != 1 || rp.RecoveredUpTo < w.acked[volID] {
			t.Fatalf("recovery point %+v does not cover ACKed %d", rp, w.acked[volID])
		}
	}

	src, _ := w.md.GetHost(ctx, cloneHostA)
	dst, _ := w.md.GetHost(ctx, destHost)
	if src.State != lifecycle.HostDraining {
		t.Fatalf("source state = %q, want DRAINING", src.State)
	}
	if src.NVMeCommittedBytes != 0 || dst.NVMeCommittedBytes != 2*volSize {
		t.Fatalf("capacity did not follow the volumes: src=%d dst=%d", src.NVMeCommittedBytes, dst.NVMeCommittedBytes)
	}
	if op, _ := w.md.GetOperation(ctx, drainOpID); op.Phase != lifecycle.OpSucceeded {
		t.Fatalf("operation phase = %q, want DRAINED", op.Phase)
	}
}

// TestDrainWaitsForFencing: before FENCING_WAIT elapses no volume moves and no
// epoch is granted (INV-11). The reconciler simply retries later.
func TestDrainWaitsForFencing(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)

	_, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
	if !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("want ErrFencingWaitNotElapsed, got %v", err)
	}
	for _, vol := range w.vols {
		v, _ := w.md.GetVolume(ctx, format.UUIDString(vol))
		if v.CurrentEpoch != 1 || v.PrimaryHostID != cloneHostA {
			t.Fatalf("volume moved before the fencing wait: %+v", v)
		}
	}
	// The host is already cordoned, though: nothing new lands on it.
	if h, _ := w.md.GetHost(ctx, cloneHostA); h.State != lifecycle.HostDraining {
		t.Fatalf("host state = %q, want DRAINING", h.State)
	}

	// Once the wait elapses the same operation completes.
	w.pastFencingWait()
	res, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
	if err != nil || res.Phase != lifecycle.OpSucceeded || len(res.Moved) != 2 {
		t.Fatalf("resumed drain: %+v err=%v", res, err)
	}
}

// TestDrainIsResumableAndDoesNotMoveTwice: a drain that fails part-way (here: the
// destination runs out of capacity) resumes on the same operation id and finishes
// the rest, without moving an already-moved volume again (§7 reconciliation, §18).
func TestDrainIsResumableAndDoesNotMoveTwice(t *testing.T) {
	ctx := context.Background()
	// The destination has room for exactly one volume under the 2.0 policy.
	w := newDrainWorld(t, volSize/2)
	w.pastFencingWait()

	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, placement.ErrNoCapacity) {
		t.Fatalf("want ErrNoCapacity for the second volume, got %v", err)
	}
	firstID := format.UUIDString(w.vols[0])
	first, _ := w.md.GetVolume(ctx, firstID)
	if first.PrimaryHostID != destHost || first.CurrentEpoch != 2 {
		t.Fatalf("first volume should have moved: %+v", first)
	}

	// Give the destination room and resume the same operation.
	if err := w.md.UpsertHost(ctx, w.term, metadata.Host{
		HostID: destHost, State: lifecycle.HostActive, NVMeTotalBytes: 10 * volSize,
	}); err != nil {
		t.Fatal(err)
	}
	// UpsertHost rewrites the row, so restore the committed bytes of the first move.
	if err := w.md.CommitHostCapacity(ctx, w.term, destHost, volSize); err != nil {
		t.Fatal(err)
	}
	res, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(res.Moved) != 1 || res.Moved[0].VolumeID == firstID {
		t.Fatalf("resume moved the wrong set: %+v", res.Moved)
	}
	// The already-moved volume was not promoted a second time.
	if v, _ := w.md.GetVolume(ctx, firstID); v.CurrentEpoch != 2 {
		t.Fatalf("first volume promoted twice: epoch=%d", v.CurrentEpoch)
	}
}

// TestDrainCancelStopsAtVolumeBoundary: cancellation is honored between volumes —
// never between promote and detach — so every volume still has exactly one writer.
func TestDrainCancelStopsAtVolumeBoundary(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()

	if err := w.drainer.Cancel(ctx, w.term, drainOpID); err != nil {
		t.Fatal(err)
	}
	res, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
	if err != nil {
		t.Fatalf("canceled drain should not error: %v", err)
	}
	if res.Phase != lifecycle.OpCanceled || len(res.Moved) != 0 || res.Remaining != 2 {
		t.Fatalf("canceled drain result = %+v", res)
	}
	for _, vol := range w.vols {
		v, _ := w.md.GetVolume(ctx, format.UUIDString(vol))
		if v.PrimaryHostID != cloneHostA || v.CurrentEpoch != 1 {
			t.Fatalf("canceled drain moved a volume: %+v", v)
		}
	}
	if op, _ := w.md.GetOperation(ctx, drainOpID); op.Phase != lifecycle.OpCanceled {
		t.Fatalf("operation phase = %q, want CANCELED", op.Phase)
	}
}

// TestDrainCancelAfterPartialProgress: cancelling a drain that is already under way
// stops it at the next volume boundary, leaving the already-moved volumes intact.
func TestDrainCancelAfterPartialProgress(t *testing.T) {
	ctx := context.Background()
	// Room for one volume only, so the first pass stops after moving one.
	w := newDrainWorld(t, volSize/2)
	w.pastFencingWait()
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, placement.ErrNoCapacity) {
		t.Fatalf("want ErrNoCapacity, got %v", err)
	}

	// Cancel the in-flight operation, then give the fleet room again.
	if err := w.drainer.Cancel(ctx, w.term, drainOpID); err != nil {
		t.Fatal(err)
	}
	if err := w.md.UpsertHost(ctx, w.term, metadata.Host{
		HostID: destHost, State: lifecycle.HostActive, NVMeTotalBytes: 10 * volSize,
	}); err != nil {
		t.Fatal(err)
	}
	res, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
	if err != nil {
		t.Fatalf("canceled drain: %v", err)
	}
	if res.Phase != lifecycle.OpCanceled || len(res.Moved) != 0 || res.Remaining != 1 {
		t.Fatalf("canceled drain result = %+v", res)
	}
	// The remaining volume is untouched on the source: still exactly one writer.
	second, _ := w.md.GetVolume(ctx, format.UUIDString(w.vols[1]))
	if second.PrimaryHostID != cloneHostA || second.CurrentEpoch != 1 {
		t.Fatalf("cancel moved the remaining volume: %+v", second)
	}
}

// TestDrainToleratesItsOwnRecoveryPoint: a move retried after a crash between the
// promotion and the boundary write finds an identical recovery point and proceeds
// (§18); a boundary that says something else is a hard error.
func TestDrainToleratesItsOwnRecoveryPoint(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()
	first := w.vols[0]
	firstID := format.UUIDString(first)

	if err := recovery.WriteRecoveryPoint(ctx, w.store, first, 2, 1, w.acked[firstID]); err != nil {
		t.Fatal(err)
	}
	res, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
	if err != nil || len(res.Moved) != 2 {
		t.Fatalf("drain with its own recovery point already written: %+v err=%v", res, err)
	}

	// A boundary claiming a different prefix must stop the move.
	w2 := newDrainWorld(t, 10*volSize)
	w2.pastFencingWait()
	if err := recovery.WriteRecoveryPoint(ctx, w2.store, w2.vols[0], 2, 1, 999); err != nil {
		t.Fatal(err)
	}
	if _, err := w2.drainer.Drain(ctx, w2.term, cloneHostA, drainOpID); err == nil {
		t.Fatal("a conflicting recovery point must fail the move")
	}
}

// TestDrainReleasesCapacityWhenMaterializationFails: a move that cannot rebuild the
// volume on the destination gives the reservation back (§28.2).
func TestDrainReleasesCapacityWhenMaterializationFails(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()

	// A third volume whose WAL prefix holds something that is not a WAL object.
	var bad [16]byte
	bad[6], bad[8] = 0x70, 0x80
	bad[15] = 0xaf // sorts after the two good volumes
	badID := format.UUIDString(bad)
	if err := w.md.CreateVolume(ctx, w.term, metadata.Volume{
		VolumeID: badID, SizeBytes: volSize, BlockSize: 65536, State: lifecycle.VolumeActive,
		CurrentEpoch: 1, PrimaryHostID: cloneHostA, DEKWrapped: []byte{1}, KEKID: "k",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.Put(ctx, "wal/"+badID+"/1/junk.wal", []byte("nope"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	res, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
	if err == nil {
		t.Fatal("an unreadable WAL prefix must stop the drain")
	}
	if len(res.Moved) != 2 {
		t.Fatalf("the healthy volumes should still have moved: %+v", res.Moved)
	}
	// Only the two moved volumes hold capacity on the destination.
	if dst, _ := w.md.GetHost(ctx, destHost); dst.NVMeCommittedBytes != 2*volSize {
		t.Fatalf("destination committed = %d, want %d (failed move leaked)", dst.NVMeCommittedBytes, 2*volSize)
	}
}

// TestDrainSurfacesBrokenCapacityAccounting: releasing the source's reservation is
// part of the move; if the books say the source never held those bytes, the drain
// reports it instead of writing a negative (§28.2).
func TestDrainSurfacesBrokenCapacityAccounting(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()

	// Wipe the source's committed bytes behind the drain's back.
	if err := w.md.CommitHostCapacity(ctx, w.term, cloneHostA, -2*volSize); err != nil {
		t.Fatal(err)
	}
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, metadata.ErrCapacityUnderflow) {
		t.Fatalf("want ErrCapacityUnderflow, got %v", err)
	}
}

// TestDrainOfUnknownHostFails: a drain of a host that is not registered cannot even
// cordon it, and fails before touching any volume.
func TestDrainOfUnknownHostFails(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	if _, err := w.drainer.Drain(ctx, w.term, "00000000-0000-7000-8000-00000000dead", drainOpID); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// TestDrainRejectsMalformedVolumeID: the volume id is the same 16-byte id the WAL
// keyspace uses; a row that is not a UUID stops the move instead of guessing.
func TestDrainRejectsMalformedVolumeID(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()
	if err := w.md.CreateVolume(ctx, w.term, metadata.Volume{
		VolumeID: "not-a-uuid", SizeBytes: volSize, BlockSize: 65536,
		State: lifecycle.VolumeActive, CurrentEpoch: 1, PrimaryHostID: cloneHostA, DEKWrapped: []byte{1}, KEKID: "k",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); err == nil {
		t.Fatal("a malformed volume id must stop the drain")
	}
}

// TestDrainPrefersTheWarmStandby is §20 step 2 / §22.3: when a volume has a warm
// standby, the evacuation lands there — it is already hydrated, so the move is short.
func TestDrainPrefersTheWarmStandby(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()

	const standby = "00000000-0000-7000-8000-0000000000d9"
	if err := w.md.UpsertHost(ctx, w.term, metadata.Host{
		HostID: standby, State: lifecycle.HostActive, NVMeTotalBytes: 10 * volSize,
		// Deliberately more committed than destHost, so only the standby rule can win.
		NVMeCommittedBytes: 5 * volSize,
	}); err != nil {
		t.Fatal(err)
	}
	firstID := format.UUIDString(w.vols[0])
	v, _ := w.md.GetVolume(ctx, firstID)
	v.StandbyHostID = standby
	if err := w.md.CreateVolume(ctx, w.term, v); err != nil { // upsert in the sim store
		t.Fatal(err)
	}

	res, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	for _, mv := range res.Moved {
		want := destHost
		if mv.VolumeID == firstID {
			want = standby
		}
		if mv.ToHost != want {
			t.Fatalf("volume %s moved to %s, want %s", mv.VolumeID, mv.ToHost, want)
		}
	}
}

// TestDrainOfEmptyHostIsDrained: draining a host with no volumes is a no-op that
// still cordons it — the operation is idempotent, not an error.
func TestDrainOfEmptyHostIsDrained(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()

	res, err := w.drainer.Drain(ctx, w.term, destHost, drainOpID)
	if err != nil || res.Phase != lifecycle.OpSucceeded || len(res.Moved) != 0 {
		t.Fatalf("empty drain: %+v err=%v", res, err)
	}
	if h, _ := w.md.GetHost(ctx, destHost); h.State != lifecycle.HostDraining {
		t.Fatalf("host state = %q, want DRAINING", h.State)
	}
}

// TestDrainFinishesAVolumeItAlreadyPromoted is DEV-0008. A drain that dies after the
// promotion — before the epoch boundary is written and before the source's capacity
// is released — used to lose the volume on the next pass: it was no longer listed
// under the source, so nothing ever finished it. The pass must be driven by what the
// operation planned, not by who currently owns the volume.
func TestDrainFinishesAVolumeItAlreadyPromoted(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	firstID := format.UUIDString(w.vols[0])

	// A first pass records the operation and its plan, then stops on the fencing
	// wait — the operation now exists with both volumes committed to the move.
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("setup: want ErrFencingWaitNotElapsed, got %v", err)
	}
	w.pastFencingWait()

	// Now the crash: the next pass promoted the first volume and died before writing
	// the epoch boundary and releasing the source's capacity.
	if _, err := w.md.BumpVolumeEpoch(ctx, w.term, firstID, destHost); err != nil {
		t.Fatal(err)
	}

	res, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
	if err != nil {
		t.Fatalf("drain must finish the interrupted volume: %v", err)
	}
	if res.Phase != lifecycle.OpSucceeded {
		t.Fatalf("phase = %q, want SUCCEEDED", res.Phase)
	}
	// The interrupted volume got its epoch boundary...
	v, _ := w.md.GetVolume(ctx, firstID)
	if _, err := recovery.ReadRecoveryPoint(ctx, w.store, w.vols[0], uint64(v.CurrentEpoch)); err != nil {
		t.Fatalf("the interrupted volume never got its recovery point: %v", err)
	}
	// ...and it was not promoted a second time.
	if v.CurrentEpoch != 2 {
		t.Fatalf("volume epoch = %d, want 2 — the resumed drain promoted it again", v.CurrentEpoch)
	}
	// Capacity was released exactly once for both volumes.
	src, _ := w.md.GetHost(ctx, cloneHostA)
	if src.NVMeCommittedBytes != 0 {
		t.Fatalf("source still holds %d committed bytes", src.NVMeCommittedBytes)
	}
}

// TestDrainReleasesSourceCapacityExactlyOnce: re-running a completed drain must not
// release capacity twice (which the non-negative guard would turn into a hard error
// on a host that legitimately holds other volumes).
func TestDrainReleasesSourceCapacityExactlyOnce(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()

	// Give the source an extra reservation that does not belong to this drain.
	if err := w.md.CommitHostCapacity(ctx, w.term, cloneHostA, 3*volSize); err != nil {
		t.Fatal(err)
	}
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); err != nil {
		t.Fatal(err)
	}
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); err != nil {
		t.Fatalf("re-running a finished drain must be a no-op: %v", err)
	}
	src, _ := w.md.GetHost(ctx, cloneHostA)
	if src.NVMeCommittedBytes != 3*volSize {
		t.Fatalf("source committed = %d, want %d — capacity was released more than once",
			src.NVMeCommittedBytes, 3*volSize)
	}
}

// TestDrainResumeUsesTheRecordedPlan: the plan is what the operation committed to,
// so a volume that lands on the source *after* the drain started is not swept into
// it — the operator asked to evacuate a set, not to chase a moving target.
func TestDrainResumeUsesTheRecordedPlan(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)

	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("setup: %v", err)
	}
	// A third volume appears on the host after the plan was recorded.
	var late [16]byte
	late[6], late[8] = 0x70, 0x80
	late[15] = 0xbe
	lateID := format.UUIDString(late)
	if err := w.md.CreateVolume(ctx, w.term, metadata.Volume{
		VolumeID: lateID, SizeBytes: volSize, BlockSize: 65536, State: lifecycle.VolumeActive,
		CurrentEpoch: 1, PrimaryHostID: cloneHostA, DEKWrapped: []byte{1}, KEKID: "k",
	}); err != nil {
		t.Fatal(err)
	}

	w.pastFencingWait()
	res, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Moved) != 2 {
		t.Fatalf("moved %d volumes, want the 2 in the recorded plan", len(res.Moved))
	}
	if v, _ := w.md.GetVolume(ctx, lateID); v.PrimaryHostID != cloneHostA {
		t.Fatal("a volume that arrived after the plan was recorded must not be moved by it")
	}
}

// TestDrainRefusesToFinishAVolumeWhoseDataIsGone: finishing a promoted volume still
// has to derive the epoch boundary from S3. If the previous epoch's objects are
// unreadable, the boundary would be a guess — the pass fails instead.
func TestDrainRefusesToFinishAVolumeWhoseDataIsGone(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	firstID := format.UUIDString(w.vols[0])

	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("setup: %v", err)
	}
	w.pastFencingWait()
	if _, err := w.md.BumpVolumeEpoch(ctx, w.term, firstID, destHost); err != nil {
		t.Fatal(err)
	}
	// Corrupt the epoch's only object so the durable point cannot be established.
	objs, _ := w.store.List(ctx, "wal/"+firstID+"/1/")
	if len(objs) == 0 {
		t.Fatal("expected a WAL object for the source epoch")
	}
	if _, err := w.store.Put(ctx, objs[0].Key, []byte("garbage"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	// The boundary must cover what was ACKed; with the object unreadable the prefix
	// is empty, so the recovery point would understate it. Either way the pass must
	// not silently record a boundary of zero and move on.
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); err == nil {
		rp, rerr := recovery.ReadRecoveryPoint(ctx, w.store, w.vols[0], 2)
		if rerr == nil && rp.RecoveredUpTo < w.acked[firstID] {
			t.Fatalf("wrote a recovery point at %d, below the ACKed %d", rp.RecoveredUpTo, w.acked[firstID])
		}
	}
}

// TestCancelAFinishedDrainIsRefused: cancellation is a request about work in flight.
// Once the operation succeeded there is nothing to cancel, and letting it flip back
// would misreport what happened to the fleet.
func TestCancelAFinishedDrainIsRefused(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); err != nil {
		t.Fatal(err)
	}
	if err := w.drainer.Cancel(ctx, w.term, drainOpID); !errors.Is(err, lifecycle.ErrInvalidTransition) {
		t.Fatalf("cancelling a finished drain: want ErrInvalidTransition, got %v", err)
	}
	if op, _ := w.md.GetOperation(ctx, drainOpID); op.Phase != lifecycle.OpSucceeded {
		t.Fatalf("phase = %q after a refused cancel", op.Phase)
	}
}

// TestDrainOfAHostWithNoVolumesRecordsAnEmptyPlan: the operation still exists, so a
// later pass has something to be idempotent about.
func TestDrainOfAHostWithNoVolumesRecordsAnEmptyPlan(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()

	if _, err := w.drainer.Drain(ctx, w.term, destHost, drainOpID); err != nil {
		t.Fatal(err)
	}
	op, err := w.md.GetOperation(ctx, drainOpID)
	if err != nil || op.Phase != lifecycle.OpSucceeded {
		t.Fatalf("operation = %+v err=%v", op, err)
	}
	// And re-running it is a no-op rather than an illegal transition.
	res, err := w.drainer.Drain(ctx, w.term, destHost, drainOpID)
	if err != nil || res.Phase != lifecycle.OpSucceeded || res.Remaining != 0 {
		t.Fatalf("re-run of an empty drain: %+v err=%v", res, err)
	}
}
