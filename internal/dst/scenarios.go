package dst

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spin-stack/storage/internal/simio/network"
)

// MandatoryScenario is one entry in the §25.1 must-be-green-on-every-PR set.
//
// **Every data-path arm is gone, and the harness is not.** The scenarios that crashed a
// write-ahead log around fdatasync, tore an append at every byte, filled a device,
// carried records across an epoch and raced two hosts to publish an image were the best
// things in this package, and each one went with the engine it drove — a scenario whose
// subject does not exist tests the scenario. What is left is the harness itself, the
// seeded world, the fault injectors and the checker substrate, because the commit
// protocol that replaces that engine needs exactly this with a new subject: the same
// crash-at-every-point shape, aimed at a HEAD compare-and-swap and an immutable commit
// manifest instead of at a WAL segment.
type MandatoryScenario struct {
	Name string
	Run  Scenario
}

// MandatoryScenarios is the set the `task dst` gate runs. It is assembled from the
// per-area lists (core here, then drain, recovery and harness) so two increments can
// add scenarios without both editing one literal.
func MandatoryScenarios() []MandatoryScenario {
	all := coreScenarios()
	all = append(all, harnessScenarios()...)
	return all
}

// coreScenarios are the §25.1 entries that predate the split by area.
func coreScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "clock-drift-beyond-skew", Run: scenarioClockDriftBeyondSkew},
		{Name: "network-partition", Run: scenarioNetworkPartition},
	}
}

// scenarioClockDriftBeyondSkew: inject wall skew far beyond max_clock_skew (2 s).
// Monotonic time — the basis of writer safety (§12.1) — must be unaffected; only
// wall time moves. The MonotonicClockChecker corroborates across the run.
func scenarioClockDriftBeyondSkew(s *Sim) error {
	before := s.Clock.Now()
	wallBefore := s.Clock.Wall()

	drift := time.Duration(3+s.Rand.Intn(30)) * time.Second // always > 2 s skew bound
	s.Clock.SetSkew(drift)
	s.Emit(Event{Kind: EventFault, Msg: fmt.Sprintf("inject wall skew=%s", drift)})

	if got := s.Clock.Now(); got != before {
		return fmt.Errorf("skew perturbed monotonic time: %d -> %d", before, got)
	}
	// Advance and confirm monotonic still moves normally.
	s.Tick(time.Second)
	if s.Clock.Now() <= before {
		return fmt.Errorf("monotonic did not advance after Tick")
	}
	// Wall reflects the injected skew.
	wallAfter := s.Clock.Wall()
	if wallAfter.Sub(wallBefore) < drift {
		return fmt.Errorf("wall did not reflect injected skew: delta=%s drift=%s", wallAfter.Sub(wallBefore), drift)
	}
	return nil
}

// scenarioNetworkPartition: a Control-Plane/Agent link is partitioned; sends fail
// while partitioned and resume after heal (the §12/§23 fencing precondition).
func scenarioNetworkPartition(s *Sim) error {
	ctx := context.Background()
	addr := "agent:1"
	l, err := s.Net.Listen(addr)
	if err != nil {
		return err
	}
	defer l.Close()

	accepted := make(chan network.Conn, 1)
	accErr := make(chan error, 1)
	go func() {
		c, err := l.Accept(ctx)
		if err != nil {
			accErr <- err
			return
		}
		accepted <- c
	}()

	client, err := s.Net.Dial(ctx, addr)
	if err != nil {
		return err
	}
	defer client.Close()

	select {
	case <-accepted:
	case err := <-accErr:
		return fmt.Errorf("accept: %w", err)
	}

	s.Net.Partition(addr)
	s.Emit(Event{Kind: EventFault, Msg: "partition agent:1"})
	if err := client.Send(ctx, []byte("heartbeat")); !errors.Is(err, network.ErrPartitioned) {
		return fmt.Errorf("send during partition: want ErrPartitioned, got %v", err)
	}

	s.Net.Heal(addr)
	s.Emit(Event{Kind: EventNetwork, Msg: "heal agent:1"})
	if err := client.Send(ctx, []byte("heartbeat")); err != nil {
		return fmt.Errorf("send after heal: %w", err)
	}
	return nil
}
