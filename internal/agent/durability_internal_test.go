package agent

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/checkpoint"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/ioclass"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/vhost"
	"github.com/spin-stack/storage/internal/wal"
)

// The durability scheduler decides *when*. These tests drive that decision directly
// rather than through the loop's timer: what is worth pinning is which trigger fires,
// what stops a checkpoint from being published at all, and which failures mean this host
// has lost the volume (ADR-0023).

type schedListener struct{ closed chan struct{} }

func (l *schedListener) Accept() (vhost.Conn, error) { <-l.closed; return nil, errors.New("closed") }
func (l *schedListener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}

type schedMapper struct{}

func (schedMapper) Map(*os.File, uint64, uint64) ([]byte, error) {
	return nil, errors.New("no front-end")
}
func (schedMapper) Unmap([]byte) error { return nil }

func schedEventFD(*os.File) (vhost.EventFD, error) { return nil, errors.New("no front-end") }

// schedRig is a manager with one running volume, in remote mode, with a lease and an
// io-class budget the test controls.
type schedRig struct {
	m     *VolumeManager
	v     *Volume
	vol   [16]byte
	clk   *sim.Clock
	store *sim.ObjectStore
	lease bool
}

func newSchedRig(t *testing.T, cfg VolumeManagerConfig) *schedRig {
	t.Helper()
	r := &schedRig{
		clk:   sim.NewClock(time.Unix(1_700_000_000, 0).UTC()),
		store: sim.NewObjectStore(),
		lease: true,
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "/var/lib/spin"
	}
	if cfg.SocketDir == "" {
		cfg.SocketDir = "/run/spin"
	}
	if cfg.HostID == "" {
		cfg.HostID = ids.New().String()
	}
	m, err := NewVolumeManager(cfg, VolumeManagerDeps{
		Clock:   r.clk,
		Disk:    sim.NewDisk(),
		Listen:  func(string) (vhost.Listener, error) { return &schedListener{closed: make(chan struct{})}, nil },
		Mapper:  schedMapper{},
		EventFD: schedEventFD,
		Store:   r.store,
		Lease:   func() bool { return r.lease },
	})
	if err != nil {
		t.Fatalf("NewVolumeManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	r.m = m

	id := ids.New()
	r.vol = [16]byte(id)
	if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{{
		VolumeId: id.String(), SizeBytes: 1 << 20, BlockSize: 512, Epoch: 1,
		Durability: storagev1.Durability_DURABILITY_REMOTE,
		State:      storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	m.mu.Lock()
	r.v = m.volumes[id.String()]
	m.mu.Unlock()
	if r.v == nil {
		t.Fatal("the volume did not start")
	}
	// Wait for the read view, the way a guest does: every volume with an object store
	// behind it builds one at start (a resumed volume's own objects, a clone's parent,
	// a promoted volume's predecessor), and the scheduler declines while it is pending
	// — a checkpoint taken then compares the store's real durable point against 0 and
	// reads as a second writer (ADR-0023). A real Agent satisfies this by itself,
	// because the guest reads.
	if _, err := r.v.dev.ReadAt(make([]byte, 512), 0); err != nil {
		t.Fatalf("the read that waits for the base: %v", err)
	}
	return r
}

// write puts n bytes of WAL on disk through the device a guest would use.
func (r *schedRig) write(t *testing.T, n int) {
	t.Helper()
	for written := 0; written < n; written += 4096 {
		if _, err := r.v.dev.WriteAt(make([]byte, 4096), int64(written%(1<<20))); err != nil {
			t.Fatalf("WriteAt: %v", err)
		}
	}
}

// TestTheByteTriggerFiresAtTheDesignsThreshold. §21.1: 256 MiB of WAL or two minutes.
// The threshold is measured against the WAL actually on disk, which is what truncation
// gives back — so the trigger resets itself and needs no separate accounting.
func TestTheByteTriggerFiresAtTheDesignsThreshold(t *testing.T) {
	r := newSchedRig(t, VolumeManagerConfig{CheckpointBytes: 64 << 10, CheckpointInterval: time.Hour})
	last := r.clk.Now()

	if due, _ := r.m.checkpointDue(r.v, last); due {
		t.Fatal("a checkpoint is due on an empty WAL")
	}

	r.write(t, 128<<10)
	due, why := r.m.checkpointDue(r.v, last)
	if !due {
		local, _ := r.v.log.LocalBytes()
		t.Fatalf("no checkpoint due with %d bytes of WAL against a %d-byte threshold", local, 64<<10)
	}
	if why != "bytes" {
		t.Errorf("trigger reported as %q, want \"bytes\"", why)
	}
}

// TestTheIntervalTriggerFiresOnAnIdleVolume. The byte trigger alone would let a volume
// that writes slowly hold its WAL for ever; §21.1 pairs it with two minutes.
func TestTheIntervalTriggerFiresOnAnIdleVolume(t *testing.T) {
	r := newSchedRig(t, VolumeManagerConfig{CheckpointBytes: 1 << 40, CheckpointInterval: 2 * time.Minute})
	last := r.clk.Now()
	r.clk.Advance(2 * time.Minute)

	due, why := r.m.checkpointDue(r.v, last)
	if !due {
		t.Fatal("no checkpoint due two minutes after the last one on an idle volume")
	}
	if why != "interval" {
		t.Errorf("trigger reported as %q, want \"interval\"", why)
	}
}

// TestNoCheckpointWithoutAValidLease is §12.2 stated where it bites: a SELF_FENCED Agent
// "deja de publicar checkpoints/manifests". The lease is checked before Create even
// though Create verifies the epoch, because the two fail differently and this is the
// cheap one.
func TestNoCheckpointWithoutAValidLease(t *testing.T) {
	r := newSchedRig(t, VolumeManagerConfig{})
	r.write(t, 8192)
	r.lease = false

	if ran, _ := r.m.checkpointOnce(t.Context(), r.v, r.vol, 1, "test"); ran {
		t.Fatal("a checkpoint was published with no valid lease (§12.2)")
	}
	keys, _ := r.store.List(t.Context(), "checkpoints/")
	if len(keys) != 0 {
		t.Errorf("%d checkpoint objects were published without a lease", len(keys))
	}
}

// TestBackgroundYieldsToTheGuest is INV-17 on this path. A checkpoint is a LIST and a PUT
// against the same store the guest's FLUSH uses; while a foreground op is in flight the
// scheduler must decline and try again, not queue behind it.
func TestBackgroundYieldsToTheGuest(t *testing.T) {
	r := newSchedRig(t, VolumeManagerConfig{})
	sched := ioclass.NewScheduler(4)
	r.m.deps.IOClass = sched
	r.write(t, 8192)

	sched.Begin(ioclass.Foreground) // the guest is mid-request
	if ran, _ := r.m.checkpointOnce(t.Context(), r.v, r.vol, 1, "test"); ran {
		t.Fatal("a checkpoint ran while a foreground op was in flight (INV-17)")
	}
	sched.End(ioclass.Foreground)

	if ran, err := r.m.checkpointOnce(t.Context(), r.v, r.vol, 1, "test"); !ran {
		t.Logf("checkpoint error: %v", err)
		t.Fatal("the checkpoint did not run once the guest was done")
	}
}

// TestACheckpointReclaimsTheLocalWAL is the whole increment in one assertion: without it
// published stays 0 for the life of the process and not one byte comes back.
func TestACheckpointReclaimsTheLocalWAL(t *testing.T) {
	r := newSchedRig(t, VolumeManagerConfig{Limits: wal.Limits{SegmentBytes: 8192}})
	r.write(t, 64<<10)
	if err := r.v.log.Flush(t.Context()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	before, err := r.v.log.LocalBytes()
	if err != nil {
		t.Fatal(err)
	}

	if ran, err := r.m.checkpointOnce(t.Context(), r.v, r.vol, 1, "test"); !ran {
		t.Logf("checkpoint error: %v", err)
		t.Fatal("the checkpoint did not run")
	}

	if w := r.v.log.Watermarks(); w.Published == 0 {
		t.Fatal("published is still 0 after a checkpoint")
	}
	after, err := r.v.log.LocalBytes()
	if err != nil {
		t.Fatal(err)
	}
	if after >= before {
		t.Errorf("local WAL went from %d to %d bytes: nothing was reclaimed", before, after)
	}
	if r.v.log.ReclaimedBytes() <= 0 {
		t.Error("no reclaimed bytes recorded")
	}
}

// TestTheObjectStoreCanFenceThisHost is ADR-0023. A checkpoint failure that proves a
// second writer is in this epoch is fencing, arriving by a door the design does not
// describe — and the volume must stop serving by the same path the Control Plane's
// refusal takes, epoch recorded so Apply does not restart it.
func TestTheObjectStoreCanFenceThisHost(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"S3 proves more than this log ever ACKed", fmt.Errorf("wrapped: %w", checkpoint.ErrDurablePointMismatch)},
		{"a different checkpoint exists at this sequence", fmt.Errorf("wrapped: %w", checkpoint.ErrCheckpointConflict)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newSchedRig(t, VolumeManagerConfig{})
			id := r.v.id

			r.m.handleCheckpointError(t.Context(), r.v, tc.err)

			if _, ok := r.m.Device(id); ok {
				t.Fatal("the volume is still being served after the object store proved a second writer")
			}
			r.m.mu.Lock()
			fenced := r.m.fencedEpoch[id]
			r.m.mu.Unlock()
			if fenced != 1 {
				t.Errorf("fenced epoch recorded as %d, want 1 — Apply would restart the volume", fenced)
			}
		})
	}
}

// TestATransientFailureIsRetried is the other half: an unreachable store is not a
// fencing witness, and a volume must not be torn down over one.
func TestATransientFailureIsRetried(t *testing.T) {
	r := newSchedRig(t, VolumeManagerConfig{})
	id := r.v.id

	r.m.handleCheckpointError(t.Context(), r.v, errors.New("the object store is unreachable"))

	if _, ok := r.m.Device(id); !ok {
		t.Fatal("the volume was torn down over a transient checkpoint failure")
	}
}

// TestCheckpointSaysWhyItDeclined. The scheduler declines for three reasons and they are
// not interchangeable: a pending base clears on its own, a lapsed lease is fencing, and a
// denied budget means the guest is busy. An explicit Checkpoint has to say which — the
// first version returned "did not run" and swallowed the cause, which is how the base
// interaction below stayed invisible until a DST run tripped over it.
func TestCheckpointSaysWhyItDeclined(t *testing.T) {
	r := newSchedRig(t, VolumeManagerConfig{})
	id := r.v.id

	r.lease = false
	err := r.m.Checkpoint(t.Context(), id)
	if err == nil {
		t.Fatal("an explicit checkpoint succeeded with no lease")
	}
	if !strings.Contains(err.Error(), "lease") {
		t.Errorf("the refusal does not name the lease: %v", err)
	}

	if err := r.m.Checkpoint(t.Context(), ids.New().String()); err == nil {
		t.Error("a checkpoint was accepted for a volume this host does not serve")
	}
}

// TestNoCheckpointWhileTheBaseIsPending is the interaction a DST run caught: a resumed
// log reports durable = 0 until its base arrives, so a checkpoint taken in that window
// raises ErrDurablePointMismatch — which ADR-0023 reads as "another writer is in this
// epoch" and acts on by fencing. A healthy host would fence itself out of its own volume
// on every restart.
func TestNoCheckpointWhileTheBaseIsPending(t *testing.T) {
	r := newSchedRig(t, VolumeManagerConfig{})

	pending, err := wal.ResumeAwaitingBase(sim.NewDisk(), "wal", r.clk, r.vol, 1, wal.Limits{}, nil)
	if err != nil {
		t.Fatalf("ResumeAwaitingBase: %v", err)
	}
	defer func() { _ = pending.Close() }()
	r.v.log = pending

	ran, cerr := r.m.checkpointOnce(t.Context(), r.v, r.vol, 1, "test")
	if ran {
		t.Fatal("a checkpoint ran while the volume was still recovering its read view")
	}
	if cerr != nil {
		t.Errorf("a pending base is a decline, not a failure: %v", cerr)
	}
}

// TestCheckpointsNeedAStoreAndAnIdentity. A local-only Agent has nothing to publish into,
// and a checkpoint with no host id is an unattributable publication into an epoch —
// which is precisely what §12.4's ownership check cannot verify. Both are refused before
// a scheduler is ever started, and the reason is logged once rather than every poll.
func TestCheckpointsNeedAStoreAndAnIdentity(t *testing.T) {
	tests := []struct {
		name string
		mut  func(*VolumeManager)
		want string
	}{
		{"no object store", func(m *VolumeManager) { m.deps.Store = nil }, "object store"},
		{"no host id", func(m *VolumeManager) { m.cfg.HostID = "" }, "host id"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newSchedRig(t, VolumeManagerConfig{})
			if err := r.m.checkpointsEnabled(); err != nil {
				t.Fatalf("a fully wired manager cannot checkpoint: %v", err)
			}
			tc.mut(r.m)
			err := r.m.checkpointsEnabled()
			if err == nil {
				t.Fatal("a scheduler was allowed with a missing dependency")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not name %q", err, tc.want)
			}
		})
	}
}

// localRig is schedRig's `local` twin: the §14.8 mode where the FLUSH ACKs on fdatasync
// and S3 catches up afterwards.
func newLocalRig(t *testing.T) *schedRig {
	t.Helper()
	r := &schedRig{
		clk:   sim.NewClock(time.Unix(1_700_000_000, 0).UTC()),
		store: sim.NewObjectStore(),
		lease: true,
	}
	m, err := NewVolumeManager(VolumeManagerConfig{
		DataDir: "/var/lib/spin", SocketDir: "/run/spin", HostID: ids.New().String(),
	}, VolumeManagerDeps{
		Clock:   r.clk,
		Disk:    sim.NewDisk(),
		Listen:  func(string) (vhost.Listener, error) { return &schedListener{closed: make(chan struct{})}, nil },
		Mapper:  schedMapper{},
		EventFD: schedEventFD,
		Store:   r.store,
		Lease:   func() bool { return r.lease },
	})
	if err != nil {
		t.Fatalf("NewVolumeManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	r.m = m

	id := ids.New()
	r.vol = [16]byte(id)
	if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{{
		VolumeId: id.String(), SizeBytes: 1 << 20, BlockSize: 512, Epoch: 1,
		Durability: storagev1.Durability_DURABILITY_LOCAL,
		State:      storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	v, ok := m.volumes[id.String()]
	if !ok {
		t.Fatal("the local-mode volume is not being served")
	}
	r.v = v
	return r
}

// writeAndFlush drives a guest's write and FLUSH through the device.
func (r *schedRig) writeAndFlush(t *testing.T) {
	t.Helper()
	if _, err := r.v.dev.WriteAt(bytes.Repeat([]byte{0x5A}, 4096), 0); err != nil {
		t.Fatalf("guest write: %v", err)
	}
	if err := r.v.dev.Flush(t.Context()); err != nil {
		t.Fatalf("guest FLUSH: %v", err)
	}
}

func (r *schedRig) objectCount(t *testing.T) int {
	t.Helper()
	objs, err := r.store.List(t.Context(), "wal/")
	if err != nil {
		t.Fatal(err)
	}
	return len(objs)
}

// §14.8 rule 3 says S3 is asynchronous in `local` mode. Until drainOnce existed, nothing
// performed the "asynchronous" half: durableStep returns before the upload block, which
// is the only place in the tree that drains batcher.Pending(). So a local volume put
// nothing in the object store, ever — durable stayed 0, no checkpoint could publish,
// TruncateLocal(published) reclaimed nothing, and the WAL grew for the life of the
// volume. That is the pre-increment-3 failure, reappearing for every local volume the
// moment the Agent started honouring the mode.
func TestALocalVolumeFlushesWithoutS3AndDrainsAfterwards(t *testing.T) {
	r := newLocalRig(t)

	// The ACK itself: no object, and that is the mode working rather than failing.
	r.writeAndFlush(t)
	if n := r.objectCount(t); n != 0 {
		t.Fatalf("a local FLUSH put %d object(s) in the store; §14.8 says it ACKs on fdatasync alone", n)
	}
	if got := r.v.log.Watermarks().Durable; got != 0 {
		t.Fatalf("durable = %d after a local FLUSH; the object store has not confirmed anything yet", got)
	}

	// And then the asynchronous half.
	r.m.drainOnce(t.Context(), r.v)
	if n := r.objectCount(t); n == 0 {
		t.Fatal("the drain put nothing in the object store: a local volume's records never leave the host")
	}
	if got := r.v.log.Watermarks().Durable; got == 0 {
		t.Fatal("the drain uploaded but never advanced durable, so no checkpoint can publish and no byte is ever reclaimed")
	}
}

// The B2 decision, pinned: uploading is not gated on the lease and advancing
// durable_sequence is.
//
// An object is create-only under a deterministic key and INV-21 hard-fails a divergent
// PUT, so writing one asserts nothing about who owns the volume — and a fenced host holds
// the only copy of these records, so refusing to upload them would turn a fencing event
// into data loss. durable_sequence is the claim: INV-03 orders it, INV-13 truncates
// against it, and a promoted successor reads it. §14.8 frees the FLUSH ACK from the lease
// in local mode; it does not free the watermark.
func TestAFencedLocalVolumeUploadsButDoesNotClaimDurability(t *testing.T) {
	r := newLocalRig(t)
	r.writeAndFlush(t)

	r.lease = false
	r.m.drainOnce(t.Context(), r.v)

	if n := r.objectCount(t); n == 0 {
		t.Fatal("a fenced host refused to upload; its records exist nowhere else, so this turns fencing into data loss")
	}
	if got := r.v.log.Watermarks().Durable; got != 0 {
		t.Fatalf("durable advanced to %d without a valid lease (§12.2, INV-06): a fenced host moved a watermark its successor trusts", got)
	}

	// And it resumes cleanly once the lease is back, rather than needing another write.
	r.lease = true
	r.m.drainOnce(t.Context(), r.v)
	if got := r.v.log.Watermarks().Durable; got == 0 {
		t.Fatal("durable never advanced after the lease came back; the volume is stuck until the next FLUSH")
	}
}
