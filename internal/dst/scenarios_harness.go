package dst

import (
	"context"
	"errors"
	"fmt"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
)

// Harness-level scenarios: fault injection driven by the seed, resource exhaustion,
// and anything whose subject is the simulation itself rather than one subsystem.

func harnessScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "restored-control-plane", Run: scenarioRestoredControlPlane},
	}
}

// rewoundLeadership is a metadata store whose leadership row was restored from a
// backup: elections resume from an earlier term, so the same term is handed out
// twice. Nothing inside the database can tell — the term is derived from the row.
type rewoundLeadership struct {
	metadata.Store
	replay []int64
	n      int
}

func (s *rewoundLeadership) AcquireLeadership(ctx context.Context, holderID string) (int64, error) {
	if s.n < len(s.replay) {
		t := s.replay[s.n]
		s.n++
		return t, nil
	}
	return s.Store.AcquireLeadership(ctx, holderID)
}

// scenarioRestoredControlPlane is ADR-0011 under the incident it was written for: the
// Control Plane database is restored to a point before the running leader's term, so
// the next election offers a term that is still in use. Every §7 mutation is guarded
// by that number, so two processes would pass every guard at once — not a zombie and a
// leader, but two leaders, each reading the other's writes as its own resumed work.
//
// The scenario drives the real elector against the real simulated object store and
// asserts both halves: that the rewound database really does offer the live term back
// (otherwise the rest proves nothing), and that the elector refuses to return it,
// climbing past every term the bucket has ever witnessed.
func scenarioRestoredControlPlane(s *Sim) error {
	ctx := context.Background()
	md := metasim.New(s.Clock.Wall)
	elector := controlplane.NewElector(md, s.Store)

	first, err := elector.Acquire(ctx, "cp-a")
	if err != nil {
		return err
	}
	host := "00000000-0000-7000-8000-0000000000a1"
	if err := md.UpsertHost(ctx, first, metadata.Host{
		HostID: host, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
	}); err != nil {
		return err
	}
	s.Emit(Event{Kind: EventNote, Msg: fmt.Sprintf("leader cp-a at term %d", first)})

	// The restore. The row is back before cp-a's term while cp-a is still running.
	restored := &rewoundLeadership{Store: md, replay: []int64{first}}
	s.Emit(Event{Kind: EventFault, Msg: "control-plane database restored to an earlier term"})

	// The fault is real: read the database alone and it offers the live term back.
	offered, err := restored.AcquireLeadership(ctx, "cp-b")
	if err != nil {
		return err
	}
	if offered != first {
		return fmt.Errorf("the rewind did not reproduce: election offered %d, not the live %d", offered, first)
	}

	// The elector is what stops it: the claim for that term is already in the bucket.
	second, err := controlplane.NewElector(&rewoundLeadership{Store: md, replay: []int64{first}}, s.Store).
		Acquire(ctx, "cp-b")
	if err != nil {
		return err
	}
	if second <= first {
		return fmt.Errorf("a restored database re-issued term %d while cp-a still holds %d (violates §7)", second, first)
	}
	s.Emit(Event{Kind: EventNote, Msg: fmt.Sprintf("leader cp-b climbed to term %d", second)})

	// And the §7 guard is doing its job again: cp-a is now the zombie.
	err = md.UpsertHost(ctx, first, metadata.Host{
		HostID: host, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
	})
	if !errors.Is(err, metadata.ErrStaleTerm) {
		return fmt.Errorf("the superseded leader's mutation returned %v, want ErrStaleTerm", err)
	}
	return nil
}
