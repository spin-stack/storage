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
// Drain pass. beforeUpdate/beforeCommit run ahead of the call and may fail it;
// afterCommit runs once the call has landed. That is how a crash *between* two
// durable writes is reconstructed without forking a process.
type hookedStore struct {
	metadata.Store
	beforeUpdate func(op metadata.Operation) error
	beforeCommit func(hostID string, delta int64) error
	afterCommit  func(hostID string, delta int64)
}

func (s *hookedStore) UpdateOperation(ctx context.Context, term int64, op metadata.Operation) error {
	if s.beforeUpdate != nil {
		if err := s.beforeUpdate(op); err != nil {
			return err
		}
	}
	return s.Store.UpdateOperation(ctx, term, op)
}

func (s *hookedStore) CommitHostCapacity(ctx context.Context, term int64, hostID string, delta int64) error {
	if s.beforeCommit != nil {
		if err := s.beforeCommit(hostID, delta); err != nil {
			return err
		}
	}
	err := s.Store.CommitHostCapacity(ctx, term, hostID, delta)
	if err == nil && s.afterCommit != nil {
		s.afterCommit(hostID, delta)
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
	ctx := context.Background()
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
		materialize.New(store, nil, nil), faults,
		placement.Policy{MaxOversubscription: 2.0})
	return w
}

// committed reports a host's committed NVMe bytes.
func (w *drainWorld) committed(t *testing.T, hostID string) int64 {
	t.Helper()
	h, err := w.base.GetHost(context.Background(), hostID)
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
	if rp, err := recovery.ReadRecoveryPoint(context.Background(), w.store, vol, epoch); err == nil {
		t.Fatalf("an epoch boundary was written for epoch %d: %+v", epoch, rp)
	}
}

// killAfterTheRelease makes every progress write fail once the volume's bytes are
// already off the source — the window between a completed move and the record of it,
// with nothing surviving to say the move happened. A one-shot fault would not do:
// the pass's own FAILED record would then persist the progress the crash lost.
func (w *drainWorld) killAfterTheRelease(t *testing.T, volumeID string) {
	t.Helper()
	w.hooks.beforeUpdate = func(op metadata.Operation) error {
		if !strings.Contains(string(op.CurrentState), volumeID) || w.committed(t, cloneHostA) != volSize {
			return nil
		}
		return errProgressLost
	}
}

// addHost registers an extra ACTIVE host with room for ten volumes.
func (w *drainWorld) addHost(t *testing.T, hostID string) {
	t.Helper()
	if err := w.base.UpsertHost(context.Background(), w.term, metadata.Host{
		HostID: hostID, State: lifecycle.HostActive, NVMeTotalBytes: 10 * volSize,
	}); err != nil {
		t.Fatal(err)
	}
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
//
// The crash is now produced by failing this drain's own boundary write. The setup
// used to stand it in with an external BumpVolumeEpoch, which is indistinguishable
// from another actor promoting the volume — the case a drain must refuse to finish
// (TestDrainRefusesToFinishAVolumeAnotherActorPromoted), so it can no longer stand in
// for this one.
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

	// Now the crash: the next pass promotes the first volume and dies before writing
	// the epoch boundary and releasing the source's capacity.
	w.faults.fail = failBoundaryWrite
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, errBoundaryLost) {
		t.Fatalf("setup: want errBoundaryLost, got %v", err)
	}
	w.faults.fail = nil
	if v, _ := w.md.GetVolume(ctx, firstID); v.CurrentEpoch != 2 || v.PrimaryHostID != destHost {
		t.Fatalf("setup: the volume should be promoted with no boundary yet: %+v", v)
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
	// This drain's own promotion landed; the boundary write did not.
	w.faults.fail = failBoundaryWrite
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, errBoundaryLost) {
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
	_, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
	if !errors.Is(err, controlplane.ErrDurableRegression) {
		t.Fatalf("want ErrDurableRegression, got %v", err)
	}
	if rp, rerr := recovery.ReadRecoveryPoint(ctx, w.store, w.vols[0], 2); rerr == nil {
		t.Fatalf("an epoch boundary was written anyway: %+v", rp)
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

// TestDrainRefusesAnEpochBoundaryBelowThePGWatermark: the other floor. PostgreSQL's
// durable watermark is lazy and informative (§5.8), so it can only *raise* the floor
// — but when it is present and higher than what the object store yields, the move is
// about to lose ACKed data and must stop.
func TestDrainRefusesAnEpochBoundaryBelowThePGWatermark(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	firstID := format.UUIDString(w.vols[0])

	// The Agent had reported a much higher durable sequence than S3 can show.
	if err := w.md.UpdateWatermarks(ctx, w.term, firstID, 500, 400, 0); err != nil {
		t.Fatal(err)
	}
	w.pastFencingWait()

	_, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
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
	ctx := context.Background()
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
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.md.CommitHostCapacity(ctx, w.term, cloneHostA, volSize); err != nil {
		t.Fatal(err)
	}
	if _, err := epoch.NewStore(w.store).Init(ctx, emptyID, 1); err != nil {
		t.Fatal(err)
	}

	res, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
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

// TestDrainReleasesCapacityOnceWhenTheProgressWriteFails is the exactly-once claim
// DEV-0008 was closed on, at the boundary that breaks it: the move completed — the
// volume promoted, the epoch boundary written, the source's capacity released — and
// the write that records it as finished never landed. The resumed pass must not
// release those bytes a second time: without slack that is ErrCapacityUnderflow on
// every later pass (the host stays DRAINING and the remaining volumes are never
// evacuated), and with slack it silently eats another volume's reservation.
func TestDrainReleasesCapacityOnceWhenTheProgressWriteFails(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()
	firstID := format.UUIDString(w.vols[0])

	w.killAfterTheRelease(t, firstID)
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, errProgressLost) {
		t.Fatalf("setup: want errProgressLost, got %v", err)
	}
	if got := w.committed(t, cloneHostA); got != volSize {
		t.Fatalf("source committed = %d after one release, want %d", got, volSize)
	}

	w.hooks.beforeUpdate = nil
	res, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if res.Phase != lifecycle.OpSucceeded {
		t.Fatalf("phase = %q, want SUCCEEDED", res.Phase)
	}
	// Two volumes, two releases — never three.
	if got := w.committed(t, cloneHostA); got != 0 {
		t.Fatalf("source committed = %d, want 0 (each volume released exactly once)", got)
	}
	if got := w.committed(t, destHost); got != 2*volSize {
		t.Fatalf("destination committed = %d, want %d", got, 2*volSize)
	}
}

// TestDrainCancelBetweenTheMoveAndTheProgressWrite: a cancellation that lands in the
// window between a completed move and the record of it must not make the resumed
// pass release that volume's capacity again.
func TestDrainCancelBetweenTheMoveAndTheProgressWrite(t *testing.T) {
	ctx := context.Background()
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
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); err == nil {
		t.Fatal("the progress write must fail once the operation is CANCELING")
	}
	w.hooks.beforeUpdate = nil

	res, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
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
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	firstID := format.UUIDString(w.vols[0])

	// The plan is recorded with both volumes while both are still on the source.
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("setup: %v", err)
	}
	w.pastFencingWait()

	// A failover this drain knows nothing about promotes the first volume to a third
	// host and charges it there.
	w.addHost(t, drainHostC)
	if _, err := w.base.BumpVolumeEpoch(ctx, w.term, firstID, drainHostC); err != nil {
		t.Fatal(err)
	}
	if err := w.base.CommitHostCapacity(ctx, w.term, drainHostC, volSize); err != nil {
		t.Fatal(err)
	}

	res, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
	if err != nil {
		t.Fatalf("the drain must still evacuate the rest of the host: %v", err)
	}
	if res.Phase != lifecycle.OpSucceeded {
		t.Fatalf("phase = %q, want SUCCEEDED", res.Phase)
	}
	// No boundary for a promotion this operation did not perform...
	w.noBoundary(t, w.vols[0], 2)
	// ...and no release for it either: only the volume this drain moved.
	if got := w.committed(t, cloneHostA); got != volSize {
		t.Fatalf("source committed = %d, want %d (only this drain's own move released)", got, volSize)
	}
	if got := w.committed(t, destHost); got != volSize {
		t.Fatalf("destination committed = %d, want %d", got, volSize)
	}
}

// TestTwoDrainsOfTheSameHostReleaseCapacityOnce: two operation ids planning the same
// host while its volumes are still there. Whichever moves a volume owns its release;
// the other must not release it again — silently consuming another volume's
// reservation, or wedging the fleet's accounting with ErrCapacityUnderflow.
func TestTwoDrainsOfTheSameHostReleaseCapacityOnce(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	// Reservations on the source that belong to volumes no drain is moving.
	if err := w.base.CommitHostCapacity(ctx, w.term, cloneHostA, 3*volSize); err != nil {
		t.Fatal(err)
	}

	// Both operations capture the same plan before either can move anything.
	for _, id := range []string{drainOpID, drainOpID2} {
		if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, id); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
			t.Fatalf("setup %s: %v", id, err)
		}
	}
	w.pastFencingWait()

	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); err != nil {
		t.Fatalf("first drain: %v", err)
	}
	if got := w.committed(t, cloneHostA); got != 3*volSize {
		t.Fatalf("after the first drain the source holds %d, want %d", got, 3*volSize)
	}

	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID2); err != nil {
		t.Fatalf("second drain: %v", err)
	}
	if got := w.committed(t, cloneHostA); got != 3*volSize {
		t.Fatalf("source committed = %d, want %d — the second drain released capacity it never moved", got, 3*volSize)
	}
	if got := w.committed(t, destHost); got != 2*volSize {
		t.Fatalf("destination committed = %d, want %d — the second drain reserved a second time", got, 2*volSize)
	}
}

// TestDrainResumeWhenTheVolumeWasPromotedTwice: the previous epoch of a boundary may
// never be a subtraction. Here the drain promoted the volume (epoch 2) and died
// before writing the boundary; an unrelated promotion then advanced it to epoch 3.
// Inferring prev = current-1 records a permanent boundary of zero for an epoch that
// holds ACKed data, which recovery then reads as "durable through nothing".
func TestDrainResumeWhenTheVolumeWasPromotedTwice(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()
	firstID := format.UUIDString(w.vols[0])

	w.faults.fail = failBoundaryWrite
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, errBoundaryLost) {
		t.Fatalf("setup: want errBoundaryLost, got %v", err)
	}
	w.faults.fail = nil
	if v, _ := w.md.GetVolume(ctx, firstID); v.CurrentEpoch != 2 || v.PrimaryHostID != destHost {
		t.Fatalf("setup: the volume should be promoted with no boundary yet: %+v", v)
	}

	// A second promotion, by somebody else, advances the volume again.
	w.addHost(t, drainHostC)
	if _, err := w.base.BumpVolumeEpoch(ctx, w.term, firstID, drainHostC); err != nil {
		t.Fatal(err)
	}

	_, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
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
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)

	// The operation is recorded for the source host and stops on the fencing wait.
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("setup: %v", err)
	}
	w.pastFencingWait()

	_, err := w.drainer.Drain(ctx, w.term, destHost, drainOpID)
	if !errors.Is(err, controlplane.ErrOperationMismatch) {
		t.Fatalf("want ErrOperationMismatch, got %v", err)
	}
	if got := w.committed(t, destHost); got != 0 {
		t.Fatalf("host %s was billed %d bytes for another host's volumes", destHost, got)
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
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); err != nil {
		t.Fatal(err)
	}
	if err := w.base.SetHostState(ctx, w.term, cloneHostA, lifecycle.HostActive); err != nil {
		t.Fatal(err)
	}

	res, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
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
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()

	var killed bool
	w.hooks.afterCommit = func(hostID string, delta int64) {
		if killed || hostID != destHost || delta <= 0 {
			return
		}
		killed = true
		if err := w.base.SetHostState(ctx, w.term, destHost, lifecycle.HostDead); err != nil {
			t.Errorf("kill destination: %v", err)
		}
	}

	_, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
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
	ctx := context.Background()
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

// TestDrainSurfacesAReservationItCouldNotRelease: leadership changes between the
// destination reservation and the promotion. The move cannot proceed and the
// reservation cannot be released either — the stale term is refused by both writes —
// so the drain must say so. A phantom reservation nobody reconciles makes placement
// under-use, and eventually refuse, a host that is actually empty (§28.2).
func TestDrainSurfacesAReservationItCouldNotRelease(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()

	var lost bool
	w.hooks.afterCommit = func(hostID string, delta int64) {
		if lost || hostID != destHost || delta <= 0 {
			return
		}
		lost = true
		if _, err := w.base.AcquireLeadership(ctx, "cp-2"); err != nil {
			t.Errorf("new leader: %v", err)
		}
	}

	_, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
	if !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("want the stale term reported, got %v", err)
	}
	if !errors.Is(err, controlplane.ErrReservationNotReleased) {
		t.Fatalf("the leaked reservation must be surfaced, not swallowed: %v", err)
	}
	if !strings.Contains(err.Error(), destHost) {
		t.Fatalf("the error must name the host holding the reservation: %v", err)
	}
}

// TestDrainRefusesToPromoteASourceThatRenewedItsLease: a renewal that lands between
// two passes of a drain must not slip under the promotion. The source's lease is
// monotonic and per host: if it is valid past the promotion, the source keeps ACKing
// FLUSHes into the old epoch while the destination writes the new one, and
// everything ACKed above the recorded boundary is discarded at recovery.
func TestDrainRefusesToPromoteASourceThatRenewedItsLease(t *testing.T) {
	ctx := context.Background()
	// Room for exactly one volume, so the first pass stops after moving one.
	w := newDrainWorld(t, volSize/2)
	w.pastFencingWait()
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, placement.ErrNoCapacity) {
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
	// UpsertHost rewrites the row, so restore the committed bytes of the first move.
	if err := w.base.CommitHostCapacity(ctx, w.term, destHost, volSize); err != nil {
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

// TestDrainRefusesToGuessWhenTheCapacityLedgerMoved: the resumed pass proves whether
// its own release landed by comparing the host's committed bytes against what it
// recorded before attempting it. When a third party has moved the same ledger in the
// meantime that proof is gone, and the drain says so rather than guessing — releasing
// again would consume a reservation belonging to a volume nobody is moving.
func TestDrainRefusesToGuessWhenTheCapacityLedgerMoved(t *testing.T) {
	ctx := context.Background()
	w := newDrainWorld(t, 10*volSize)
	w.pastFencingWait()
	firstID := format.UUIDString(w.vols[0])

	// Kill the pass between the release and the record of it.
	w.killAfterTheRelease(t, firstID)
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, errProgressLost) {
		t.Fatalf("setup: want errProgressLost, got %v", err)
	}
	w.hooks.beforeUpdate = nil

	// Somebody else books three volumes onto the source before the drain resumes.
	if err := w.base.CommitHostCapacity(ctx, w.term, cloneHostA, 3*volSize); err != nil {
		t.Fatal(err)
	}
	if _, err := w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID); !errors.Is(err, controlplane.ErrCapacityLedgerMoved) {
		t.Fatalf("want ErrCapacityLedgerMoved, got %v", err)
	}
	// Whatever else is true, the drain did not release a second time.
	if got := w.committed(t, cloneHostA); got != 4*volSize {
		t.Fatalf("source committed = %d, want %d — the resumed pass released again", got, 4*volSize)
	}
}
