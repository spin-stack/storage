package dst

import (
	"context"
	"errors"
	"fmt"

	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
)

// fencingScenarios are the arms of the two-signal rule: what a host does when it has
// stopped being able to confirm that it owns the volumes it is serving.
// There is no checker beside them, and that is a finding rather than an omission. A
// checker here would read the *grounds* of a give-up — the epoch the host held against
// the epoch the bucket recorded — and this package requires a planted bug that breaks
// production behaviour, injected into the simulated I/O and never into the code. No I/O
// fault can produce a wrongful give-up: the only fault that reaches this decision is a
// stale or failed read of the epoch object, and both of those make the rule *keep*
// serving, which is arm 3 and arm 4 below and is safe. A checker that cannot be made to
// fire is the decoration commitCheckers declined to add for the same reason.
//
// What arm 4 now also reaches is the isolation response — a host that can read neither
// path pauses its guest rather than serve on a guess — and that is its own scenario
// (scenarios_isolation.go), because its subject is the whole loop and not this one rule.
//
// The invariant that a host given up on cannot corrupt the history is not left unchecked
// either way: it is effective-single-writer, and its subject is the compare-and-set on
// HEAD, which is where this system's fence is actually enforced.
func fencingScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "an-isolated-host-keeps-its-guest", Run: scenarioIsolatedHostKeepsItsGuest},
	}
}

// scenarioIsolatedHostKeepsItsGuest drives the real rule (agent.Superseded) against the
// real epoch object in the simulated store, across the three things a lapsed lease can
// mean.
//
// The subject is a decision that costs a tenant their VM when it is wrong in one
// direction and risks a second writer when it is wrong in the other, and the arms below
// are the three states the object store can be in, not three shapes of the same state:
//
//  1. the epoch has not moved — nobody was granted the volume, and stopping the guest is
//     a VM killed for a partition of the management path alone;
//  2. the epoch has moved — the Control Plane granted the volume elsewhere, and this is
//     the only fact in the system that says so without going through the Control Plane;
//  3. the store cannot be read — this host cannot tell (1) from (2), and what it does
//     then is the design's open question, asserted here so that it is a decision.
//
// The fourth arm is the one a unit test cannot reach and is why this is a DST scenario at
// all: a *stale read* of the epoch object. A replica one version behind hands this host
// the epoch it held, so the rule reads "not superseded" while the fleet has in fact moved
// on. That must not be a fork, and it is not — the compare-and-set on HEAD is what
// actually fences, and this signal only decides whether a guest is stopped early.
func scenarioIsolatedHostKeepsItsGuest(s *Sim) error {
	ctx := context.Background()
	volumeID := ids.NewAt(1<<40, s.Rand).String()
	wit := descriptor.EpochWitness{Store: s.Store}
	held := []agent.VolumeStatus{{VolumeID: volumeID, Epoch: 1}}

	// Arm 0: the volume has no epoch object at all, which is every volume placed before
	// the object existed. "No record" is not "granted to somebody else".
	if confirmed, _ := agent.Superseded(ctx, wit, held); len(confirmed) != 0 {
		return errors.New("a volume with no recorded epoch was read as granted elsewhere")
	}
	s.Emit(Event{Kind: EventNote, Msg: "no epoch recorded: not superseded"})

	// Arm 1: the grant this host holds is the one the bucket records.
	if err := descriptor.WriteEpoch(ctx, s.Store, volumeID, 1); err != nil {
		return fmt.Errorf("recording the epoch this host holds: %w", err)
	}
	confirmed, unconfirmed := agent.Superseded(ctx, wit, held)
	if len(confirmed) != 0 || len(unconfirmed) != 1 {
		return fmt.Errorf("an isolated host was told to give up: confirmed=%d unconfirmed=%d", len(confirmed), len(unconfirmed))
	}

	// Arm 2: the Control Plane granted the volume to another host, which writes the epoch
	// **before** it moves the catalog (descriptor.WriteEpoch) — so this object is the
	// earliest point at which the successor is visible to anyone.
	if err := descriptor.WriteEpoch(ctx, s.Store, volumeID, 2); err != nil {
		return fmt.Errorf("recording the successor's grant: %w", err)
	}
	confirmed, unconfirmed = agent.Superseded(ctx, wit, held)
	if len(confirmed) != 1 || len(unconfirmed) != 0 {
		return fmt.Errorf("a superseded host kept its volume: confirmed=%d unconfirmed=%d", len(confirmed), len(unconfirmed))
	}

	// Arm 3: a replica one version behind. The successor exists and this host cannot see
	// it, so it keeps serving — which is safe and is worth stating, because the reflex is
	// to read it as the fencing signal having failed. It has not: nothing this host writes
	// can enter the published history without winning the compare-and-set on HEAD, and it
	// cannot win one against a HEAD the successor has moved.
	s.Store.InjectStaleRead(descriptor.EpochKey(volumeID))
	s.Emit(Event{Kind: EventFault, Msg: "the epoch object reads one version behind"})
	confirmed, _ = agent.Superseded(ctx, wit, held)
	if len(confirmed) != 0 {
		return errors.New("a stale read of the epoch object confirmed a supersession it could not see")
	}
	s.Store.ClearStaleRead(descriptor.EpochKey(volumeID))

	// Arm 4: the store itself cannot be read. This host can tell nothing, and it keeps
	// serving — the open question in STATUS.md, pinned so that changing it is deliberate.
	s.Store.InjectThrottleKey(descriptor.EpochKey(volumeID), 1)
	s.Emit(Event{Kind: EventFault, Msg: "the epoch object cannot be read at all"})
	confirmed, unconfirmed = agent.Superseded(ctx, wit, held)
	if len(confirmed) != 0 || len(unconfirmed) != 1 {
		return fmt.Errorf("a host that could read nothing decided something: confirmed=%d unconfirmed=%d", len(confirmed), len(unconfirmed))
	}

	// And with no witness at all — an Agent started without an object store — nothing can
	// be confirmed, so nothing is given up.
	if confirmed, _ = agent.Superseded(ctx, nil, held); len(confirmed) != 0 {
		return errors.New("an Agent with no object store confirmed a supersession out of nothing")
	}
	return nil
}
