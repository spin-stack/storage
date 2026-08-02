package agent

import (
	"errors"
	"fmt"
	"os"
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
		State: storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	m.mu.Lock()
	r.v = m.volumes[id.String()]
	m.mu.Unlock()
	if r.v == nil {
		t.Fatal("the volume did not start")
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

// TestNoCheckpointWithoutAValidLease is §12.6 stated where it bites: a SELF_FENCED Agent
// "deja de publicar checkpoints/manifests". The lease is checked before Create even
// though Create verifies the epoch, because the two fail differently and this is the
// cheap one.
func TestNoCheckpointWithoutAValidLease(t *testing.T) {
	r := newSchedRig(t, VolumeManagerConfig{})
	r.write(t, 8192)
	r.lease = false

	if r.m.checkpointOnce(t.Context(), r.v, r.vol, 1, "test") {
		t.Fatal("a checkpoint was published with no valid lease (§12.6)")
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
	if r.m.checkpointOnce(t.Context(), r.v, r.vol, 1, "test") {
		t.Fatal("a checkpoint ran while a foreground op was in flight (INV-17)")
	}
	sched.End(ioclass.Foreground)

	if !r.m.checkpointOnce(t.Context(), r.v, r.vol, 1, "test") {
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

	if !r.m.checkpointOnce(t.Context(), r.v, r.vol, 1, "test") {
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
