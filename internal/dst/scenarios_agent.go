package dst

import (
	"context"
	"errors"
	"fmt"
	"os"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/vhost"
)

// The Agent's seam, rather than the WAL's. Everything else in this harness drives
// wal.Log, controlplane and recovery directly, which is where the durability
// invariants live — but "this host stopped serving a volume it lost" is not a
// property of any of them. It belongs to agent.VolumeManager, and until this file
// existed nothing simulated proved it: DEV-0012 was closed with unit tests alone,
// which CLAUDE.md counts as a stop signal for fencing code.

func agentScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "fenced-volume-stops-serving", Run: scenarioFencedVolumeStopsServing},
	}
}

func agentCheckers() []Checker { return []Checker{NewFencedVolumeChecker()} }

// FencedVolumeChecker enforces the Agent's half of INV-10 (§16, §12.3): once the
// Control Plane has refused a volume's report, this host must not answer another
// request for it. The fleet's guarantee is one effective writer; a host that keeps
// serving reads out of a WAL it no longer owns hands a guest bytes that another
// writer has already moved past, and the guest has no way to tell.
type FencedVolumeChecker struct{ violation error }

// NewFencedVolumeChecker returns a fresh checker.
func NewFencedVolumeChecker() *FencedVolumeChecker { return &FencedVolumeChecker{} }

func (c *FencedVolumeChecker) Name() string { return "fenced-volume-not-served" }

func (c *FencedVolumeChecker) Observe(e Event) {
	if e.Kind == EventVolumeServe && e.ServedAfterFence && c.violation == nil {
		c.violation = fmt.Errorf("volume %s was served after being fenced at step %d (violates §16/INV-10)",
			e.Key, e.Step)
	}
}

func (c *FencedVolumeChecker) Check() error { return c.violation }

// simListener is the vhost socket a simulated Agent binds. There is no front-end in
// this world, so Accept blocks until Close — which is the whole contract Server.Serve
// relies on to be cancellable, and the only part of the socket a simulation can
// legitimately model (INV-01: no real sockets here).
type simListener struct{ closed chan struct{} }

func newSimListener() *simListener { return &simListener{closed: make(chan struct{})} }

func (l *simListener) Accept() (vhost.Conn, error) {
	<-l.closed
	return nil, errors.New("listener closed")
}

func (l *simListener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}

// simMapper and simEventFD stand where the kernel objects go. vhost.NewServer refuses
// a Config without them and no session ever reaches them, so they fail rather than
// pretend: a simulation that quietly mapped memory would be modelling something that
// does not exist here.
type simMapper struct{}

func (simMapper) Map(*os.File, uint64, uint64) ([]byte, error) {
	return nil, errors.New("no front-end in the simulation")
}
func (simMapper) Unmap([]byte) error { return nil }

func simEventFD(*os.File) (vhost.EventFD, error) {
	return nil, errors.New("no front-end in the simulation")
}

// scenarioFencedVolumeStopsServing drives the real agent.VolumeManager through the
// three moments that decide whether fencing means anything:
//
//  1. a volume the Control Plane lists is served, and a guest's WRITE lands in its WAL;
//  2. the Control Plane refuses its report — this host is not the writer any more — and
//     the runtime must be gone: no device, no socket, nothing to answer with;
//  3. the desired state has *not* caught up, and repeating it must not bring the volume
//     back. Only a higher epoch does, because a higher epoch is the fleet granting the
//     volume to this host again.
//
// Step 3 is the one worth simulating. The Control Plane refuses the *report* while
// GetDesiredState may keep listing the volume for seconds afterwards, so a manager with
// no memory of the fencing restarts it on the very next cycle — serving a volume it was
// just told it lost, with nothing in the trace to say so.
func scenarioFencedVolumeStopsServing(s *Sim) error {
	return fencedVolumeStopsServing(s, honestFencing)
}

const (
	honestFencing  = false
	fencingIgnored = true
)

func fencedVolumeStopsServing(s *Sim, ignoreFencing bool) error {
	ctx := context.Background()

	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir:   "/var/lib/spin",
		SocketDir: "/run/spin",
	}, agent.VolumeManagerDeps{
		Clock:   s.Clock,
		Disk:    s.Disk,
		Listen:  func(string) (vhost.Listener, error) { return newSimListener(), nil },
		Mapper:  simMapper{},
		EventFD: simEventFD,
	})
	if err != nil {
		return fmt.Errorf("building the volume manager: %w", err)
	}
	defer func() { _ = m.Close() }()

	// A deterministic id: same seed, same volume, same trace (INV-02).
	volumeID := ids.NewAt(simEpoch*1000, s.Rand).String()
	desired := func(epoch int64) []*storagev1.DesiredVolume {
		return []*storagev1.DesiredVolume{{
			VolumeId:  volumeID,
			SizeBytes: 1 << 20,
			BlockSize: 512,
			Epoch:     epoch,
			State:     storagev1.VolumeState_VOLUME_STATE_ACTIVE,
		}}
	}

	// (1) The volume is served and takes a guest's write.
	if err := m.Apply(ctx, desired(1)); err != nil {
		return fmt.Errorf("applying the desired state: %w", err)
	}
	dev, ok := m.Device(volumeID)
	if !ok {
		return errors.New("the volume the Control Plane listed is not being served")
	}
	if _, err := dev.WriteAt(make([]byte, 512), 0); err != nil {
		return fmt.Errorf("a guest write before fencing: %w", err)
	}
	s.Notef("volume %s served at epoch 1; one guest write landed", volumeID)

	// (2) The Control Plane refuses the report. This host is not the writer.
	if !ignoreFencing {
		if err := m.Fence(ctx, []string{volumeID}); err != nil {
			return fmt.Errorf("fencing: %w", err)
		}
	}

	// Whether anything still answers for this volume is exactly the invariant. It is
	// emitted rather than only returned, so the checker sees it on every seed and a
	// future scenario that serves a fenced volume some other way trips the same wire.
	_, stillServed := m.Device(volumeID)
	s.Emit(Event{Kind: EventVolumeServe, Key: volumeID, ServedAfterFence: stillServed})
	if stillServed {
		return fmt.Errorf("volume %s still has a device after being fenced (§16/INV-10)", volumeID)
	}
	if vols, err := m.Volumes(ctx); err != nil {
		return err
	} else if len(vols) != 0 {
		return fmt.Errorf("a fenced volume is still reported as served: %+v", vols)
	}

	// (3) The desired state has not caught up. Repeating it must change nothing.
	for range 3 {
		if err := m.Apply(ctx, desired(1)); err != nil {
			return fmt.Errorf("re-applying the stale desired state: %w", err)
		}
	}
	_, restarted := m.Device(volumeID)
	s.Emit(Event{Kind: EventVolumeServe, Key: volumeID, ServedAfterFence: restarted})
	if restarted {
		return fmt.Errorf("volume %s came back at the epoch it was fenced out of (§16/INV-10)", volumeID)
	}
	s.Notef("the stale desired state did not resurrect the fenced volume")

	// A higher epoch is the fleet granting it again, and it is the only thing that does.
	if err := m.Apply(ctx, desired(2)); err != nil {
		return fmt.Errorf("applying the re-granted volume: %w", err)
	}
	vols, err := m.Volumes(ctx)
	if err != nil {
		return err
	}
	if len(vols) != 1 || vols[0].Epoch != 2 {
		return fmt.Errorf("a volume re-granted at epoch 2 is not being served: %+v", vols)
	}
	s.Notef("epoch 2 re-granted the volume; it is served again from a fresh WAL root")
	return nil
}
