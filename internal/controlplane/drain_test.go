package controlplane_test

import (
	"context"
	"errors"
	"strings"
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
	drainOpID  = "00000000-0000-7000-8000-0000000000f7"
	drainOpID2 = "00000000-0000-7000-8000-0000000000f8"
	drainHostC = "00000000-0000-7000-8000-0000000000f9"
	leaseTTL   = 10 * time.Second
	maxSkew    = 2 * time.Second
)

// drainPolicy is the §28.2 rule the drain world runs under. Test-side ledger writes
// go through it too (drainWorld.book), so a test can never set up a fleet state the
// production write would have refused.
var drainPolicy = placement.Policy{MaxOversubscription: 2.0}

// Faults the drain tests inject. They stand for "the process died here": the effect
// of everything before them landed, and nothing after them ran.
var (
	errProgressLost = errors.New("test: the progress write never landed")
	errBoundaryLost = errors.New("test: the epoch boundary write never landed")
)

// failBoundaryWrite kills the pass exactly where the promotion has landed and the
// epoch boundary has not.
func failBoundaryWrite(key string) error {
	if strings.HasSuffix(key, "recovery-point.json") {
		return errBoundaryLost
	}
	return nil
}

// hookedStore wraps a metadata.Store so a test can act at an exact point inside one
// Drain pass. beforeUpdate runs ahead of the progress write and may fail it;
// afterUpdate runs once it has landed. That is how a crash *between* two durable
// writes is reconstructed without forking a process. The progress write is the only
// hook the drain still needs: after ADR-0017 it is also the reservation, so every
// capacity effect passes through it.
type hookedStore struct {
	metadata.Store
	beforeUpdate func(op metadata.Operation) error
	afterUpdate  func(op metadata.Operation)
}

func (s *hookedStore) UpdateOperation(ctx context.Context, term int64, op metadata.Operation, bound *metadata.CapacityBound) error {
	if s.beforeUpdate != nil {
		if err := s.beforeUpdate(op); err != nil {
			return err
		}
	}
	err := s.Store.UpdateOperation(ctx, term, op, bound)
	if err == nil && s.afterUpdate != nil {
		s.afterUpdate(op)
	}
	return err
}

// putFaultStore fails a chosen object PUT. Only the Drainer's own store is wrapped,
// so the fault hits the epoch boundary the drain writes and nothing else.
type putFaultStore struct {
	objectstore.Store
	fail func(key string) error
}

func (s *putFaultStore) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	if s.fail != nil {
		if err := s.fail(key); err != nil {
			return objectstore.PutResult{}, err
		}
	}
	return s.Store.Put(ctx, key, data, opts)
}

type drainWorld struct {
	md      metadata.Store // what the Drainer talks to (hooks included)
	base    *metasim.Store // the same store without the hooks, for test-side setup
	hooks   *hookedStore   // where a test arms its faults
	faults  *putFaultStore // ...and its object-store faults
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
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := sim.NewObjectStore()
	base := metasim.New(clk.Wall)
	md := &hookedStore{Store: base}
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
	faults := &putFaultStore{Store: store}
	w := &drainWorld{
		md: md, base: base, hooks: md, faults: faults,
		term: term, store: store, clk: clk, acked: map[string]uint64{},
	}
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
		}, nil); err != nil {
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
		materialize.New(store, nil, nil), faults, drainPolicy)
	return w
}

// fill puts n extra volumes of volSize on hostID: capacity another operation placed
// there, which after ADR-0017 is the only way capacity exists at all. A test that
// wants a host to be N volumes full says so by putting N volumes on it.
func (w *drainWorld) fill(t *testing.T, hostID string, n int) {
	t.Helper()
	ctx := t.Context()
	for i := range n {
		var vol [16]byte
		vol[6], vol[8] = 0x70, 0x80
		vol[13], vol[14] = 0xf0, byte(len(hostID))
		vol[15] = byte(0xc0 + i)
		if err := w.base.CreateVolume(ctx, w.term, metadata.Volume{
			VolumeID: format.UUIDString(vol), SizeBytes: volSize, BlockSize: 65536,
			State: lifecycle.VolumeActive, CurrentEpoch: 1, PrimaryHostID: hostID,
			DEKWrapped: []byte{1}, KEKID: "k",
		}, nil); err != nil {
			t.Fatal(err)
		}
	}
}

// committed reports a host's committed NVMe bytes.
func (w *drainWorld) committed(t *testing.T, hostID string) int64 {
	t.Helper()
	h, err := w.base.GetHost(t.Context(), hostID)
	if err != nil {
		t.Fatalf("host %s: %v", hostID, err)
	}
	return h.NVMeCommittedBytes
}

// noBoundary asserts that no epoch-boundary object exists for (vol, epoch). The
// object is create-only and every later recovery reads it as the floor, so one
// written by the wrong party is not a mistake that can be corrected later.
func (w *drainWorld) noBoundary(t *testing.T, vol [16]byte, epoch uint64) {
	t.Helper()
	if rp, err := recovery.ReadRecoveryPoint(t.Context(), w.store, vol, epoch); err == nil {
		t.Fatalf("an epoch boundary was written for epoch %d: %+v", epoch, rp)
	}
}

// killAfterTheMove makes every progress write fail once the volume is already the
// destination's — the window between a completed move and the record of it, with
// nothing surviving to say the move happened. A one-shot fault would not do: the
// pass's own FAILED record would then persist the progress the crash lost.
func (w *drainWorld) killAfterTheMove(t *testing.T, volumeID string) {
	t.Helper()
	w.hooks.beforeUpdate = func(op metadata.Operation) error {
		if !strings.Contains(string(op.CurrentState), volumeID) {
			return nil
		}
		v, err := w.base.GetVolume(t.Context(), volumeID)
		if err != nil || v.PrimaryHostID != destHost {
			return nil
		}
		return errProgressLost
	}
}

// killAfterThePromotion fails the progress write that records the promotion. The
// epoch is already granted and the volume is already the destination's; nothing has
// said so yet. That is the window a ledger cannot survive — the source is still
// charged for a volume it no longer holds — and the one ADR-0017 removes.
func (w *drainWorld) killAfterThePromotion(t *testing.T) {
	t.Helper()
	w.hooks.beforeUpdate = func(op metadata.Operation) error {
		if strings.Contains(string(op.CurrentState), `"PROMOTED"`) {
			return errProgressLost
		}
		return nil
	}
}

// killAtTheProgressWrite fails the write that records a finished move.
func (w *drainWorld) killAtTheProgressWrite(t *testing.T) {
	t.Helper()
	w.hooks.beforeUpdate = func(op metadata.Operation) error {
		if strings.Contains(string(op.CurrentState), `"DONE"`) {
			return errProgressLost
		}
		return nil
	}
}

// clearFaults disarms every hook, so the next pass runs clean.
func (w *drainWorld) clearFaults() {
	w.hooks.beforeUpdate = nil
	w.faults.fail = nil
}

// addHost registers an extra ACTIVE host with room for ten volumes.
func (w *drainWorld) addHost(t *testing.T, hostID string) {
	t.Helper()
	if err := w.base.UpsertHost(t.Context(), w.term, metadata.Host{
		HostID: hostID, State: lifecycle.HostActive, NVMeTotalBytes: 10 * volSize,
	}); err != nil {
		t.Fatal(err)
	}
}

// pastFencingWait advances the wall clock beyond lease_ttl + max_clock_skew.
func (w *drainWorld) pastFencingWait() { w.clk.Advance(leaseTTL + maxSkew + time.Second) }

// reconcile drives Drain the way the reconciler does: a fencing wait is not a
// failure, it is the operation asking for time. Every other outcome — success,
// ErrNoCapacity, an injected fault — comes straight back, so a test that is about
// one of those still sees it on the pass that produced it.
//
// A drain of N volumes needs N dwells, not one. Each volume enters FENCING_WAIT when
// its own promotion starts, and ADR-0015 measures the wait from that observation, so
// the pass that fences volume k+1 is the pass that promotes volume k. That is the
// §28.1 cost ADR-0016 states in the same terms — at most one lease_ttl +
// max_clock_skew per volume moved — and it is what stage 2 of that ADR removes.
func (w *drainWorld) reconcile(t *testing.T, hostID, operationID string) (controlplane.DrainResult, error) {
	t.Helper()
	var (
		res controlplane.DrainResult
		err error
	)
	var moved []controlplane.Move
	for range 16 {
		res, err = w.drainer.Drain(t.Context(), w.term, hostID, operationID)
		moved = append(moved, res.Moved...)
		res.Moved = moved // what the *operation* moved, not just its last pass
		if !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
			return res, err
		}
		w.pastFencingWait()
	}
	t.Fatalf("the drain never settled: %+v err=%v", res, err)
	return res, err
}

// TestDrainMovesEveryVolumeFenced is the §28.1 happy path: every volume ends on the
// destination at a bumped epoch, the source is fenced first (INV-10/INV-11), the
// materialized prefix covers what the source ACKed (INV-09), an epoch-boundary
// recovery point is written (INV-12), and capacity follows the volume (§28.2).
func TestDrainMovesEveryVolumeFenced(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()

	res, err := w.reconcile(t, cloneHostA, drainOpID)
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
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)

	// A raw pass, not the reconcile loop: what is under test is the refusal itself.
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
	res, err := w.reconcile(t, cloneHostA, drainOpID)
	if err != nil || res.Phase != lifecycle.OpSucceeded || len(res.Moved) != 2 {
		t.Fatalf("resumed drain: %+v err=%v", res, err)
	}
}

// TestDrainIsResumableAndDoesNotMoveTwice: a drain that fails part-way (here: the
// destination runs out of capacity) resumes on the same operation id and finishes
// the rest, without moving an already-moved volume again (§7 reconciliation, §18).
func TestDrainIsResumableAndDoesNotMoveTwice(t *testing.T) {
	ctx := t.Context()
	// The destination has room for exactly one volume under the 2.0 policy.
	w := newDrainWorld(t, volSize/2)
	w.pastFencingWait()

	if _, err := w.reconcile(t, cloneHostA, drainOpID); !errors.Is(err, placement.ErrNoCapacity) {
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
	res, err := w.reconcile(t, cloneHostA, drainOpID)
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
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()

	if err := w.drainer.Cancel(ctx, w.term, drainOpID); err != nil {
		t.Fatal(err)
	}
	res, err := w.reconcile(t, cloneHostA, drainOpID)
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
	ctx := t.Context()
	// Room for one volume only, so the first pass stops after moving one.
	w := newDrainWorld(t, volSize/2)
	w.pastFencingWait()
	if _, err := w.reconcile(t, cloneHostA, drainOpID); !errors.Is(err, placement.ErrNoCapacity) {
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
	res, err := w.reconcile(t, cloneHostA, drainOpID)
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
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()
	first := w.vols[0]
	firstID := format.UUIDString(first)

	if err := recovery.WriteRecoveryPoint(ctx, w.store, first, 2, 1, w.acked[firstID]); err != nil {
		t.Fatal(err)
	}
	res, err := w.reconcile(t, cloneHostA, drainOpID)
	if err != nil || len(res.Moved) != 2 {
		t.Fatalf("drain with its own recovery point already written: %+v err=%v", res, err)
	}

	// A boundary claiming a different prefix must stop the move.
	w2 := newDrainWorld(t, 10*volSize)
	w2.pastFencingWait()
	if err := recovery.WriteRecoveryPoint(ctx, w2.store, w2.vols[0], 2, 1, 999); err != nil {
		t.Fatal(err)
	}
	if _, err := w2.reconcile(t, cloneHostA, drainOpID); err == nil {
		t.Fatal("a conflicting recovery point must fail the move")
	}
}

// TestDrainReleasesCapacityWhenMaterializationFails: a move that cannot rebuild the
// volume on the destination gives the reservation back (§28.2).
func TestDrainReleasesCapacityWhenMaterializationFails(t *testing.T) {
	ctx := t.Context()
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
	}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.Put(ctx, "wal/"+badID+"/1/junk.wal", []byte("nope"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	res, err := w.reconcile(t, cloneHostA, drainOpID)
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

// TestDrainOfUnknownHostFails: a drain of a host that is not registered cannot even
// cordon it, and fails before touching any volume.
func TestDrainOfUnknownHostFails(t *testing.T) {
	w := newDrainWorld(t, 10*volSize)
	if _, err := w.reconcile(t, "00000000-0000-7000-8000-00000000dead", drainOpID); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// TestDrainRejectsMalformedVolumeID: the volume id is the same 16-byte id the WAL
// keyspace uses; a row that is not a UUID stops the move instead of guessing.
func TestDrainRejectsMalformedVolumeID(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()
	if err := w.md.CreateVolume(ctx, w.term, metadata.Volume{
		VolumeID: "not-a-uuid", SizeBytes: volSize, BlockSize: 65536,
		State: lifecycle.VolumeActive, CurrentEpoch: 1, PrimaryHostID: cloneHostA, DEKWrapped: []byte{1}, KEKID: "k",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := w.reconcile(t, cloneHostA, drainOpID); err == nil {
		t.Fatal("a malformed volume id must stop the drain")
	}
}

// TestDrainPrefersTheWarmStandby is §20 step 2 / §22.3: when a volume has a warm
// standby, the evacuation lands there — it is already hydrated, so the move is short.
func TestDrainPrefersTheWarmStandby(t *testing.T) {
	ctx := t.Context()
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
	if err := w.md.CreateVolume(ctx, w.term, v, nil); err != nil { // upsert in the sim store
		t.Fatal(err)
	}

	res, err := w.reconcile(t, cloneHostA, drainOpID)
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
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()

	res, err := w.reconcile(t, destHost, drainOpID)
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
//
// The crash is now produced by failing this drain's own boundary write. The setup
// used to stand it in with an external BumpVolumeEpoch, which is indistinguishable
// from another actor promoting the volume — the case a drain must refuse to finish
// (TestDrainRefusesToFinishAVolumeAnotherActorPromoted), so it can no longer stand in
// for this one.
func TestDrainFinishesAVolumeItAlreadyPromoted(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	firstID := format.UUIDString(w.vols[0])

	// A first pass records the operation and its plan, then stops on the fencing
	// wait — the operation now exists with both volumes committed to the move.
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("setup: want ErrFencingWaitNotElapsed, got %v", err)
	}
	w.pastFencingWait()

	// Now the crash: the next pass promotes the first volume and dies before writing
	// the epoch boundary and releasing the source's capacity.
	w.faults.fail = failBoundaryWrite
	if _, err := w.reconcile(t, cloneHostA, drainOpID); !errors.Is(err, errBoundaryLost) {
		t.Fatalf("setup: want errBoundaryLost, got %v", err)
	}
	w.faults.fail = nil
	if v, _ := w.md.GetVolume(ctx, firstID); v.CurrentEpoch != 2 || v.PrimaryHostID != destHost {
		t.Fatalf("setup: the volume should be promoted with no boundary yet: %+v", v)
	}

	res, err := w.reconcile(t, cloneHostA, drainOpID)
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

// TestDrainLeavesTheSourceChargedForWhatItStillHolds: re-running a completed drain
// must not change the accounting of a host that legitimately holds other volumes.
//
// Under the ledger this was the exactly-once claim about a release; under ADR-0017
// there is no release to repeat — the source stops being charged the moment the
// volume stops being its primary — so what is left to check is that the drain moves
// only its own plan and that the fleet's arithmetic is right afterwards. That it is
// now a boring test is the point: the class of bug it guarded is gone, not untested.
func TestDrainLeavesTheSourceChargedForWhatItStillHolds(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)

	// The plan is captured on the first pass, while the fence is still running.
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("setup: %v", err)
	}
	// Three volumes land on the source afterwards: nothing this drain committed to.
	w.fill(t, cloneHostA, 3)
	w.pastFencingWait()

	if _, err := w.reconcile(t, cloneHostA, drainOpID); err != nil {
		t.Fatal(err)
	}
	if _, err := w.reconcile(t, cloneHostA, drainOpID); err != nil {
		t.Fatalf("re-running a finished drain must be a no-op: %v", err)
	}
	if got := w.committed(t, cloneHostA); got != 3*volSize {
		t.Fatalf("source committed = %d, want %d — the drain touched volumes it never planned", got, 3*volSize)
	}
	if got := w.committed(t, destHost); got != 2*volSize {
		t.Fatalf("destination committed = %d, want %d", got, 2*volSize)
	}
}

// TestDrainResumeUsesTheRecordedPlan: the plan is what the operation committed to,
// so a volume that lands on the source *after* the drain started is not swept into
// it — the operator asked to evacuate a set, not to chase a moving target.
func TestDrainResumeUsesTheRecordedPlan(t *testing.T) {
	ctx := t.Context()
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
	}, nil); err != nil {
		t.Fatal(err)
	}

	w.pastFencingWait()
	res, err := w.reconcile(t, cloneHostA, drainOpID)
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
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	firstID := format.UUIDString(w.vols[0])

	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("setup: %v", err)
	}
	w.pastFencingWait()
	// This drain's own promotion landed; the boundary write did not.
	w.faults.fail = failBoundaryWrite
	if _, err := w.reconcile(t, cloneHostA, drainOpID); !errors.Is(err, errBoundaryLost) {
		t.Fatalf("setup: want errBoundaryLost, got %v", err)
	}
	w.faults.fail = nil
	// Corrupt the epoch's only object so the durable point cannot be established.
	objs, _ := w.store.List(ctx, "wal/"+firstID+"/1/")
	if len(objs) == 0 {
		t.Fatal("expected a WAL object for the source epoch")
	}
	if _, err := w.store.Put(ctx, objs[0].Key, []byte("garbage"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	// The boundary is immutable and every later recovery treats it as the floor, so
	// recording one below what the volume already had durable loses that data
	// permanently. With the object unreadable the prefix is empty, so the pass must
	// refuse rather than write a zero.
	_, err := w.reconcile(t, cloneHostA, drainOpID)
	if !errors.Is(err, controlplane.ErrDurableRegression) {
		t.Fatalf("want ErrDurableRegression, got %v", err)
	}
	if rp, rerr := recovery.ReadRecoveryPoint(ctx, w.store, w.vols[0], 2); rerr == nil {
		t.Fatalf("an epoch boundary was written anyway: %+v", rp)
	}
}

// TestDrainRevokesTheSourceLeaseBeforeItPromotes: evacuating a *healthy* host. The
// drain refuses to promote a source whose lease is still live — correctly, that is
// INV-11 — but nothing takes the lease away, so on a host that is up and being
// renewed the wait is measured against an instant that keeps moving and the drain
// waits forever. Revoking is the Control Plane withdrawing its own record of the
// source as a writer; it belongs in the fencing step, before the wait.
//
// It must not shorten the wait by a single tick. The Agent counts its copy of the
// lease down on a monotonic clock (§12.2) and never learns the row is gone, so a
// promotion granted early would run against a writer that can still ACK — the
// failure INV-11 exists to prevent.
func TestDrainRevokesTheSourceLeaseBeforeItPromotes(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize) // the source's lease was renewed "now"

	// The source is healthy: this pass runs while its lease is live.
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("a live lease must hold the promotion back: %v", err)
	}
	if _, err := w.base.GetHostLease(ctx, cloneHostA); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("the drain waited on the source's lease but never revoked it: %v", err)
	}

	// Still inside last_renewal + lease_ttl + max_clock_skew: the revocation is not
	// an excuse to promote early.
	w.clk.Advance(leaseTTL)
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("revoking the lease shortened the fencing wait: %v", err)
	}
	for _, vol := range w.vols {
		if v, _ := w.md.GetVolume(ctx, format.UUIDString(vol)); v.CurrentEpoch != 1 {
			t.Fatalf("a volume was promoted inside the fencing wait: %+v", v)
		}
	}

	// Past the full wait the healthy host is evacuated — the instant it is measured
	// from survives the revocation.
	w.clk.Advance(maxSkew + time.Second)
	res, err := w.reconcile(t, cloneHostA, drainOpID)
	if err != nil {
		t.Fatalf("a healthy host must be evacuable: %v", err)
	}
	if res.Phase != lifecycle.OpSucceeded || len(res.Moved) != 2 {
		t.Fatalf("drain result = %+v", res)
	}
}

// TestDrainDoesNotRevokeTheDestinationsLease is the guard rail on the step above.
// The revocation happens while the source is still the volume's primary; a pass that
// resumes *after* its own promotion landed would, if it revoked by primary host id,
// take away the lease promotion had just granted to the destination — fencing the
// new writer with nobody to replace it.
//
// The crash is the one that leaves the drain in that state: the promotion landed and
// the progress write that records it did not.
func TestDrainDoesNotRevokeTheDestinationsLease(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()
	firstID := format.UUIDString(w.vols[0])

	w.hooks.beforeUpdate = func(op metadata.Operation) error {
		if strings.Contains(string(op.CurrentState), `"PROMOTED"`) {
			return errProgressLost
		}
		return nil
	}
	if _, err := w.reconcile(t, cloneHostA, drainOpID); !errors.Is(err, errProgressLost) {
		t.Fatalf("setup: want errProgressLost, got %v", err)
	}
	w.hooks.beforeUpdate = nil
	if v, _ := w.md.GetVolume(ctx, firstID); v.PrimaryHostID != destHost || v.CurrentEpoch != 2 {
		t.Fatalf("setup: the promotion should have landed: %+v", v)
	}

	if _, err := w.reconcile(t, cloneHostA, drainOpID); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if _, err := w.base.GetHostLease(ctx, destHost); err != nil {
		t.Fatalf("the resumed pass revoked the lease of the host it had just promoted: %v", err)
	}
}

// TestCancelAFinishedDrainIsRefused: cancellation is a request about work in flight.
// Once the operation succeeded there is nothing to cancel, and letting it flip back
// would misreport what happened to the fleet.
func TestCancelAFinishedDrainIsRefused(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()
	if _, err := w.reconcile(t, cloneHostA, drainOpID); err != nil {
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
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()

	if _, err := w.reconcile(t, destHost, drainOpID); err != nil {
		t.Fatal(err)
	}
	op, err := w.md.GetOperation(ctx, drainOpID)
	if err != nil || op.Phase != lifecycle.OpSucceeded {
		t.Fatalf("operation = %+v err=%v", op, err)
	}
	// And re-running it is a no-op rather than an illegal transition.
	res, err := w.reconcile(t, destHost, drainOpID)
	if err != nil || res.Phase != lifecycle.OpSucceeded || res.Remaining != 0 {
		t.Fatalf("re-run of an empty drain: %+v err=%v", res, err)
	}
}

// TestDrainRefusesAnEpochBoundaryBelowThePGWatermark: the other floor. PostgreSQL's
// durable watermark is lazy and informative (§5.8), so it can only *raise* the floor
// — but when it is present and higher than what the object store yields, the move is
// about to lose ACKed data and must stop.
func TestDrainRefusesAnEpochBoundaryBelowThePGWatermark(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	firstID := format.UUIDString(w.vols[0])

	// The Agent had reported a much higher durable sequence than S3 can show.
	if err := w.md.UpdateWatermarks(ctx, w.term, firstID, 500, 400, 0); err != nil {
		t.Fatal(err)
	}
	w.pastFencingWait()

	_, err := w.reconcile(t, cloneHostA, drainOpID)
	if !errors.Is(err, controlplane.ErrDurableRegression) {
		t.Fatalf("want ErrDurableRegression, got %v", err)
	}
	if rp, rerr := recovery.ReadRecoveryPoint(ctx, w.store, w.vols[0], 2); rerr == nil {
		t.Fatalf("a boundary below the reported durable point was written: %+v", rp)
	}
}

// TestDrainAcceptsAnEmptyEpoch: a volume that was moved and never written again has
// an epoch with no objects at all. That is not data loss — it is an empty epoch, and
// recording a boundary of zero for it is correct.
func TestDrainAcceptsAnEmptyEpoch(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()

	// A third volume on the source with no WAL objects at all.
	var empty [16]byte
	empty[6], empty[8] = 0x70, 0x80
	empty[15] = 0xE0
	emptyID := format.UUIDString(empty)
	if err := w.md.CreateVolume(ctx, w.term, metadata.Volume{
		VolumeID: emptyID, SizeBytes: volSize, BlockSize: 65536, State: lifecycle.VolumeActive,
		CurrentEpoch: 1, PrimaryHostID: cloneHostA, DEKWrapped: []byte{1}, KEKID: "k",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := epoch.NewStore(w.store).Init(ctx, emptyID, 1); err != nil {
		t.Fatal(err)
	}

	res, err := w.reconcile(t, cloneHostA, drainOpID)
	if err != nil {
		t.Fatalf("an empty epoch must move cleanly: %v", err)
	}
	if len(res.Moved) != 3 {
		t.Fatalf("moved %d volumes, want 3", len(res.Moved))
	}
	rp, err := recovery.ReadRecoveryPoint(ctx, w.store, empty, 2)
	if err != nil || rp.RecoveredUpTo != 0 {
		t.Fatalf("empty epoch boundary = %+v err=%v, want RecoveredUpTo 0", rp, err)
	}
}

// TestDrainAccountsForOneMoveWhenTheProgressWriteFails is the exactly-once claim
// DEV-0008 was closed on, at the boundary that used to break it: the move completed
// — the volume promoted, the epoch boundary written — and the write that records it
// as finished never landed. Under the ledger the resumed pass had to prove its own
// release had already happened, and could not: a stranger's change that nets to one
// volume size is indistinguishable from its own.
//
// Under ADR-0017 there is nothing to prove. The volume is the destination's, so the
// destination is charged and the source is not, and a resumed pass computes the same
// answer as the pass that crashed. **The test is kept, and it is meant to be
// uninteresting**: it is a regression guard on a class of bug that was removed
// rather than guarded, not weak coverage of one that is still there.
func TestDrainAccountsForOneMoveWhenTheProgressWriteFails(t *testing.T) {
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()
	firstID := format.UUIDString(w.vols[0])

	w.killAfterTheMove(t, firstID)
	if _, err := w.reconcile(t, cloneHostA, drainOpID); !errors.Is(err, errProgressLost) {
		t.Fatalf("setup: want errProgressLost, got %v", err)
	}
	if got := w.committed(t, cloneHostA); got != volSize {
		t.Fatalf("source committed = %d after one move, want %d", got, volSize)
	}
	if got := w.committed(t, destHost); got != volSize {
		t.Fatalf("destination committed = %d after one move, want %d", got, volSize)
	}

	w.hooks.beforeUpdate = nil
	res, err := w.reconcile(t, cloneHostA, drainOpID)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if res.Phase != lifecycle.OpSucceeded {
		t.Fatalf("phase = %q, want SUCCEEDED", res.Phase)
	}
	if got := w.committed(t, cloneHostA); got != 0 {
		t.Fatalf("source committed = %d, want 0", got)
	}
	if got := w.committed(t, destHost); got != 2*volSize {
		t.Fatalf("destination committed = %d, want %d", got, 2*volSize)
	}
}

// TestDrainCancelBetweenTheMoveAndTheProgressWrite: a cancellation that lands in the
// window between a completed move and the record of it must not make the resumed
// pass release that volume's capacity again.
func TestDrainCancelBetweenTheMoveAndTheProgressWrite(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()
	firstID := format.UUIDString(w.vols[0])

	var canceled bool
	w.hooks.beforeUpdate = func(op metadata.Operation) error {
		if canceled || !strings.Contains(string(op.CurrentState), firstID) {
			return nil
		}
		if w.committed(t, cloneHostA) != volSize {
			return nil
		}
		canceled = true
		return w.drainer.Cancel(ctx, w.term, drainOpID)
	}
	if _, err := w.reconcile(t, cloneHostA, drainOpID); err == nil {
		t.Fatal("the progress write must fail once the operation is CANCELING")
	}
	w.hooks.beforeUpdate = nil

	res, err := w.reconcile(t, cloneHostA, drainOpID)
	if err != nil {
		t.Fatalf("resume after cancel: %v", err)
	}
	if res.Phase != lifecycle.OpCanceled {
		t.Fatalf("phase = %q, want CANCELED", res.Phase)
	}
	// The first volume was released once; the second never left the source.
	if got := w.committed(t, cloneHostA); got != volSize {
		t.Fatalf("source committed = %d, want %d (exactly one release)", got, volSize)
	}
	second, _ := w.md.GetVolume(ctx, format.UUIDString(w.vols[1]))
	if second.PrimaryHostID != cloneHostA || second.CurrentEpoch != 1 {
		t.Fatalf("the canceled pass moved the remaining volume: %+v", second)
	}
}

// TestDrainRefusesToFinishAVolumeAnotherActorPromoted: a volume in this drain's plan
// is promoted elsewhere by a concurrent failover. The drain fenced nobody for it, so
// it can neither author its epoch boundary — the object is create-only, and one that
// understates the previous epoch collapses the live epoch's contiguous prefix to zero
// — nor release capacity for a move it did not make.
func TestDrainRefusesToFinishAVolumeAnotherActorPromoted(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	firstID := format.UUIDString(w.vols[0])

	// The plan is recorded with both volumes while both are still on the source.
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("setup: %v", err)
	}
	w.pastFencingWait()

	// A failover this drain knows nothing about promotes the first volume to a third
	// host, which is what charges it there (ADR-0017).
	w.addHost(t, drainHostC)
	if _, err := w.base.BumpVolumeEpoch(ctx, w.term, firstID, drainHostC, 1); err != nil {
		t.Fatal(err)
	}

	res, err := w.reconcile(t, cloneHostA, drainOpID)
	if err != nil {
		t.Fatalf("the drain must still evacuate the rest of the host: %v", err)
	}
	if res.Phase != lifecycle.OpSucceeded {
		t.Fatalf("phase = %q, want SUCCEEDED", res.Phase)
	}
	// No boundary for a promotion this operation did not perform...
	w.noBoundary(t, w.vols[0], 2)
	// ...and the accounting follows the volumes rather than this drain's opinion of
	// them: the foreign volume is charged to the host that actually took it, and the
	// destination is charged only for the one this drain moved.
	if got := w.committed(t, drainHostC); got != volSize {
		t.Fatalf("the host that took the volume is charged %d, want %d", got, volSize)
	}
	if got := w.committed(t, destHost); got != volSize {
		t.Fatalf("destination committed = %d, want %d", got, volSize)
	}
	if got := w.committed(t, cloneHostA); got != 0 {
		t.Fatalf("the evacuated source is charged %d, want 0", got)
	}
}

// TestASecondDrainOfTheSameHostIsRefused: an operator retry that reaches for a fresh
// operation id — a UI that regenerated it, a request replayed after a timeout, a
// reconciler that lost track of the first — must not start a second evacuation of a
// host that already has one. Two drains capture the same plan and promote the same
// volumes; each race one of them loses leaves it holding a destination reservation
// nobody will release, and placement under-uses that host forever (§28.2).
//
// The exclusion is over *live* work: once the owning operation is finished, the same
// host may be drained again.
func TestASecondDrainOfTheSameHostIsRefused(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)

	// The first operation records its plan and stops on the fencing wait.
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("setup: %v", err)
	}
	w.pastFencingWait()
	// The owning operation holds a reservation on its destination for the volume it
	// is fencing: that is what a plan entry is (ADR-0017). The refused request must
	// not change it either way.
	reserved := w.committed(t, destHost)

	_, err := w.reconcile(t, cloneHostA, drainOpID2)
	if !errors.Is(err, controlplane.ErrHostAlreadyDraining) {
		t.Fatalf("want ErrHostAlreadyDraining, got %v", err)
	}
	if !strings.Contains(err.Error(), drainOpID) {
		t.Fatalf("the refusal must name the operation that owns the host: %v", err)
	}
	// The refused request recorded nothing, reserved nothing and moved nothing.
	if _, gerr := w.md.GetOperation(ctx, drainOpID2); !errors.Is(gerr, metadata.ErrNotFound) {
		t.Fatalf("the refused drain recorded an operation: %v", gerr)
	}
	if got := w.committed(t, destHost); got != reserved {
		t.Fatalf("the refused drain changed the destination's committed bytes: %d, want %d", got, reserved)
	}
	for _, vol := range w.vols {
		if v, _ := w.md.GetVolume(ctx, format.UUIDString(vol)); v.PrimaryHostID != cloneHostA {
			t.Fatalf("the refused drain moved a volume: %+v", v)
		}
	}

	// The operation that owns the host still runs, and its own passes are never
	// refused by its own record.
	if _, err := w.reconcile(t, cloneHostA, drainOpID); err != nil {
		t.Fatalf("the owning drain: %v", err)
	}
	if _, err := w.reconcile(t, cloneHostA, drainOpID2); err != nil {
		t.Fatalf("a fresh drain once the first finished: %v", err)
	}
}

// TestTwoDrainsOfTheSameHostAccountForOneEvacuation is the accounting half of the
// same finding, kept because the refusal is only as good as what it prevents: a
// second operation id must leave the source's and the destination's committed bytes
// exactly as the owning drain left them, whether or not it ever runs.
//
// It used to run both drains to completion and check that the second released
// nothing it had not moved. That is now unreachable through Drain — the second is
// refused before it can capture a plan — so the setup states the refusal and the
// assertions stay: they are about the fleet's accounting, not about which of the two
// mechanisms enforces it.
func TestTwoDrainsOfTheSameHostAccountForOneEvacuation(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)

	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("setup: %v", err)
	}
	w.pastFencingWait()

	if _, err := w.reconcile(t, cloneHostA, drainOpID); err != nil {
		t.Fatalf("first drain: %v", err)
	}
	if got := w.committed(t, cloneHostA); got != 0 {
		t.Fatalf("after the first drain the source holds %d, want 0", got)
	}

	// A second operation id, now that the first is finished. It captures an empty
	// plan and must move — and therefore charge — nothing.
	if _, err := w.reconcile(t, cloneHostA, drainOpID2); err != nil {
		t.Fatalf("second drain: %v", err)
	}
	if got := w.committed(t, cloneHostA); got != 0 {
		t.Fatalf("source committed = %d, want 0 — the second drain moved volumes it never planned", got)
	}
	if got := w.committed(t, destHost); got != 2*volSize {
		t.Fatalf("destination committed = %d, want %d — the second drain charged it a second time", got, 2*volSize)
	}
}

// TestDrainResumeWhenTheVolumeWasPromotedTwice: the previous epoch of a boundary may
// never be a subtraction. Here the drain promoted the volume (epoch 2) and died
// before writing the boundary; an unrelated promotion then advanced it to epoch 3.
// Inferring prev = current-1 records a permanent boundary of zero for an epoch that
// holds ACKed data, which recovery then reads as "durable through nothing".
func TestDrainResumeWhenTheVolumeWasPromotedTwice(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()
	firstID := format.UUIDString(w.vols[0])

	w.faults.fail = failBoundaryWrite
	if _, err := w.reconcile(t, cloneHostA, drainOpID); !errors.Is(err, errBoundaryLost) {
		t.Fatalf("setup: want errBoundaryLost, got %v", err)
	}
	w.faults.fail = nil
	if v, _ := w.md.GetVolume(ctx, firstID); v.CurrentEpoch != 2 || v.PrimaryHostID != destHost {
		t.Fatalf("setup: the volume should be promoted with no boundary yet: %+v", v)
	}

	// A second promotion, by somebody else, advances the volume again.
	w.addHost(t, drainHostC)
	if _, err := w.base.BumpVolumeEpoch(ctx, w.term, firstID, drainHostC, 2); err != nil {
		t.Fatal(err)
	}

	_, err := w.reconcile(t, cloneHostA, drainOpID)
	if !errors.Is(err, controlplane.ErrEpochAdvanced) {
		t.Fatalf("want ErrEpochAdvanced, got %v", err)
	}
	// Nothing may be written for the epoch this drain did not grant.
	w.noBoundary(t, w.vols[0], 3)
}

// TestDrainRefusesAnOperationIdRecordedForAnotherHost: an operator retry with a
// copy-pasted id, a replayed request, or a UI keyed on the wrong entity must not
// make host B pay for host A's volumes — reservations decremented for volumes it
// never held, create-only boundary keys burned on the volumes' live epoch, and
// SUCCEEDED reported over fabricated moves.
func TestDrainRefusesAnOperationIdRecordedForAnotherHost(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)

	// The operation is recorded for the source host and stops on the fencing wait.
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("setup: %v", err)
	}
	w.pastFencingWait()
	reserved := w.committed(t, destHost) // the owning drain's own in-flight plan

	_, err := w.reconcile(t, destHost, drainOpID)
	if !errors.Is(err, controlplane.ErrOperationMismatch) {
		t.Fatalf("want ErrOperationMismatch, got %v", err)
	}
	if got := w.committed(t, destHost); got != reserved {
		t.Fatalf("host %s was billed %d bytes for another host's volumes (want %d)", destHost, got, reserved)
	}
	// The mismatch is diagnosed before anything is written, cordon included.
	if h, _ := w.md.GetHost(ctx, destHost); h.State != lifecycle.HostActive {
		t.Fatalf("the mismatched request moved %s to %s", destHost, h.State)
	}
	for _, vol := range w.vols {
		w.noBoundary(t, vol, 1) // the live epoch: a boundary here is unrecoverable
		w.noBoundary(t, vol, 2)
		if v, _ := w.md.GetVolume(ctx, format.UUIDString(vol)); v.PrimaryHostID != cloneHostA {
			t.Fatalf("a volume moved under the mismatched id: %+v", v)
		}
	}
}

// TestDuplicateDrainDoesNotReCordonARepairedHost: a replayed request for a finished
// drain must perform no host-state transition. A host an operator repaired and
// returned to ACTIVE would otherwise leave placement again, silently, while the call
// reports success. (The DEAD arm is by design: a dead host is still evacuated.)
func TestDuplicateDrainDoesNotReCordonARepairedHost(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()
	if _, err := w.reconcile(t, cloneHostA, drainOpID); err != nil {
		t.Fatal(err)
	}
	if err := w.base.SetHostState(ctx, w.term, cloneHostA, lifecycle.HostActive); err != nil {
		t.Fatal(err)
	}

	res, err := w.reconcile(t, cloneHostA, drainOpID)
	if err != nil || res.Phase != lifecycle.OpSucceeded || res.Remaining != 0 {
		t.Fatalf("duplicate of a finished drain: %+v err=%v", res, err)
	}
	if h, _ := w.md.GetHost(ctx, cloneHostA); h.State != lifecycle.HostActive {
		t.Fatalf("the duplicate request cordoned a repaired host: state = %q", h.State)
	}
}

// TestDrainAbortsWhenTheDestinationDiesBeforeThePromotion: the chosen destination
// dies during the (potentially long) materialization. Promoting anyway would set the
// volume's primary to a dead host, commit its capacity there and release the
// source's — a volume stranded with no writer.
func TestDrainAbortsWhenTheDestinationDiesBeforeThePromotion(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()

	// The destination dies the moment this drain has reserved it — which after
	// ADR-0017 is the progress write that names it, not a separate ledger write.
	var killed bool
	w.hooks.afterUpdate = func(op metadata.Operation) {
		if killed || !strings.Contains(string(op.CurrentState), destHost) {
			return
		}
		killed = true
		if err := w.base.SetHostState(ctx, w.term, destHost, lifecycle.HostDead); err != nil {
			t.Errorf("kill destination: %v", err)
		}
	}

	_, err := w.reconcile(t, cloneHostA, drainOpID)
	if !errors.Is(err, controlplane.ErrDestinationHostUnusable) {
		t.Fatalf("want ErrDestinationHostUnusable, got %v", err)
	}
	first, _ := w.md.GetVolume(ctx, format.UUIDString(w.vols[0]))
	if first.PrimaryHostID != cloneHostA || first.CurrentEpoch != 1 {
		t.Fatalf("the volume was moved onto a dead host: %+v", first)
	}
	if got := w.committed(t, destHost); got != 0 {
		t.Fatalf("destination committed = %d, want 0 — the aborted move leaked its reservation", got)
	}
	if got := w.committed(t, cloneHostA); got != 2*volSize {
		t.Fatalf("source committed = %d, want %d — capacity was released for a move that never happened", got, 2*volSize)
	}
}

// TestDrainWithAStaleTermMutatesNothing: a zombie Control Plane must learn it is a
// zombie before it cordons a host or reserves anything (§7).
func TestDrainWithAStaleTermMutatesNothing(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()

	if _, err := w.drainer.Drain(ctx, w.term-1, cloneHostA, drainOpID); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("want ErrStaleTerm, got %v", err)
	}
	if h, _ := w.md.GetHost(ctx, cloneHostA); h.State != lifecycle.HostActive {
		t.Fatalf("a stale-term drain cordoned the host: %q", h.State)
	}
	if _, err := w.md.GetOperation(ctx, drainOpID); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("a stale-term drain recorded an operation: %v", err)
	}
	if got := w.committed(t, destHost); got != 0 {
		t.Fatalf("a stale-term drain reserved %d bytes", got)
	}
	for _, vol := range w.vols {
		w.noBoundary(t, vol, 2)
	}
}

// TestDrainSurfacesAStaleTermInsteadOfMoving: leadership changes between the
// destination reservation and the promotion, so the move cannot proceed. The drain
// reports the stale term.
//
// Under the ledger this test also had to check that the drain surfaced a
// *reservation it could not release*: the reservation was a second write, and the
// stale term refused it too, leaving a phantom that made placement under-use a host
// that was actually empty. There is no second write any more (ADR-0017) — the
// reservation is the progress entry, and an operation whose progress cannot be
// updated has not reserved anything new — so the failure mode and the sentinel that
// named it are both gone.
func TestDrainSurfacesAStaleTermInsteadOfMoving(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()

	var lost bool
	w.hooks.afterUpdate = func(op metadata.Operation) {
		if lost || !strings.Contains(string(op.CurrentState), destHost) {
			return
		}
		lost = true
		if _, err := w.base.AcquireLeadership(ctx, "cp-2"); err != nil {
			t.Errorf("new leader: %v", err)
		}
	}

	_, err := w.reconcile(t, cloneHostA, drainOpID)
	if !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("want the stale term reported, got %v", err)
	}
	first, _ := w.md.GetVolume(ctx, format.UUIDString(w.vols[0]))
	if first.PrimaryHostID != cloneHostA || first.CurrentEpoch != 1 {
		t.Fatalf("a zombie Control Plane moved a volume: %+v", first)
	}
}

// TestDrainRefusesToPromoteASourceThatRenewedItsLease: a renewal that lands between
// two passes of a drain must not slip under the promotion. The source's lease is
// monotonic and per host: if it is valid past the promotion, the source keeps ACKing
// FLUSHes into the old epoch while the destination writes the new one, and
// everything ACKed above the recorded boundary is discarded at recovery.
func TestDrainRefusesToPromoteASourceThatRenewedItsLease(t *testing.T) {
	ctx := t.Context()
	// Room for exactly one volume, so the first pass stops after moving one.
	w := newDrainWorld(t, volSize/2)
	w.pastFencingWait()
	if _, err := w.reconcile(t, cloneHostA, drainOpID); !errors.Is(err, placement.ErrNoCapacity) {
		t.Fatalf("setup: want ErrNoCapacity, got %v", err)
	}

	// The source is alive after all: its heartbeat renews the lease mid-evacuation.
	if err := w.base.RenewHostLease(ctx, w.term, cloneHostA, int(leaseTTL/time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := w.base.UpsertHost(ctx, w.term, metadata.Host{
		HostID: destHost, State: lifecycle.HostActive, NVMeTotalBytes: 10 * volSize,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("want ErrFencingWaitNotElapsed, got %v", err)
	}
	second, _ := w.md.GetVolume(ctx, format.UUIDString(w.vols[1]))
	if second.PrimaryHostID != cloneHostA || second.CurrentEpoch != 1 {
		t.Fatalf("a volume was promoted away from a host holding a live lease: %+v", second)
	}
	w.noBoundary(t, w.vols[1], 2)
}

// TestDrainRefusesADestinationAnotherPlacementFilled is the §28.2 bound at the write
// that places the bytes. placement.Choose evaluates the bound correctly and is pure,
// so two operations that read the fleet before either placed anything — here a drain
// and a clone — choose the same destination and both proceed. Nothing between the
// drain's read and its reservation re-evaluates the rule, so the destination lands
// past MaxOversubscription × NVMeTotalBytes with neither caller having made a
// mistake, and the volumes that follow are placed against a number that already lies.
//
// After ADR-0017 the reservation is the progress write, so that is where the bound
// lives, and the race is staged there.
func TestDrainRefusesADestinationAnotherPlacementFilled(t *testing.T) {
	ctx := t.Context()
	// The destination holds one volume-worth of NVMe; under the 2.0 policy its
	// declared ceiling is 2*volSize.
	w := newDrainWorld(t, volSize)
	w.pastFencingWait()
	const limit = 2 * volSize

	// A clone admitted against the same fleet read takes the whole ceiling, landing
	// in the window between the drain's placement decision and its reservation.
	var raced bool
	w.hooks.beforeUpdate = func(op metadata.Operation) error {
		if raced || !strings.Contains(string(op.CurrentState), destHost) {
			return nil
		}
		raced = true
		w.fill(t, destHost, 2)
		return nil
	}

	_, err := w.reconcile(t, cloneHostA, drainOpID)
	if !errors.Is(err, metadata.ErrCapacityExceeded) {
		t.Fatalf("want ErrCapacityExceeded, got %v", err)
	}
	if got := w.committed(t, destHost); got != limit {
		t.Fatalf("destination committed = %d, want %d — the reservation went past the declared bound", got, limit)
	}
	// Nothing was moved onto a host the policy says cannot hold it.
	first, _ := w.md.GetVolume(ctx, format.UUIDString(w.vols[0]))
	if first.PrimaryHostID != cloneHostA || first.CurrentEpoch != 1 {
		t.Fatalf("a volume was promoted onto an over-committed destination: %+v", first)
	}
	w.noBoundary(t, w.vols[0], 2)
}

// TestDrainWritesTheBoundaryAsTheEpochHolder: the epoch boundary is the floor every
// later claim about the new epoch is measured against, and the object is create-only —
// once written it is the answer forever. So it may only be authored by the host the
// epoch was granted to (§12.5), which the drain normally is: Promote granted the epoch
// to its destination moments earlier.
//
// The case that is not: this drain's promotion lands, its boundary write does not, and
// before it is resumed an overtaking promotion moves the volume on. The resumed pass
// must not record a boundary for an epoch that now belongs to somebody else. The check
// is a pre-check by construction — the PUT is the last thing WriteAs does and the
// object is immutable, so a later read could refuse nothing — and reverting the drain
// to the anonymous recovery.WriteRecoveryPoint makes this test fail.
func TestDrainWritesTheBoundaryAsTheEpochHolder(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)
	firstID := format.UUIDString(w.vols[0])

	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("setup: %v", err)
	}
	w.pastFencingWait()
	w.faults.fail = failBoundaryWrite
	if _, err := w.reconcile(t, cloneHostA, drainOpID); !errors.Is(err, errBoundaryLost) {
		t.Fatalf("setup: want errBoundaryLost, got %v", err)
	}
	w.faults.fail = nil

	// Somebody else promotes the volume on before this drain is resumed.
	epochs := epoch.NewStore(w.store)
	r, etag, err := epochs.CurrentRecord(ctx, firstID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := epochs.Grant(ctx, firstID, etag, r.Epoch+1, drainHostC); err != nil {
		t.Fatal(err)
	}

	if _, err := w.reconcile(t, cloneHostA, drainOpID); err == nil {
		t.Fatal("the resumed drain recorded a boundary for an epoch it no longer holds")
	}
	if _, rerr := recovery.ReadRecoveryPoint(ctx, w.store, w.vols[0], r.Epoch); rerr == nil {
		t.Fatal("a boundary was written by a host that does not hold the epoch")
	}
}
