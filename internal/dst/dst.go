// Package dst is the focused Deterministic Simulation Testing harness (§25.1).
// It drives the simulated I/O interfaces (internal/simio/sim) from a seed, records
// a deterministic event trace, and runs invariant checkers. The contract is: the
// same seed and scenario produce an identical trace and an identical pass/fail
// outcome (INV-02), and every checker failure prints the reproducing seed.
//
// The harness is focused, not a full green-threads scheduler: scenarios are
// sequential programs that use a seeded PRNG for fault decisions and the sim
// clock's quiescence signal (PendingTimers) to synchronize deterministically.
// This is enough to make the mandatory scenario set (fencing/partition, lost PUT,
// crash around fdatasync/PUT/ACK, clock drift) reproducible from commit 1; the
// data-path arms are filled in by later phases.
package dst

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// simEpoch is a fixed, deterministic wall-clock origin for every simulation. It
// is a constant (not real time), so runs are reproducible.
const simEpoch = 1_700_000_000

// EventKind classifies a recorded event.
type EventKind string

const (
	EventClock       EventKind = "clock"
	EventDisk        EventKind = "disk"
	EventObject      EventKind = "object"
	EventNetwork     EventKind = "network"
	EventNote        EventKind = "note"
	EventDelete      EventKind = "delete"
	EventFault       EventKind = "fault"
	EventRecovery    EventKind = "recovery"
	EventWatermark   EventKind = "watermark"
	EventLeavesHost  EventKind = "leaves-host"
	EventDurableAck  EventKind = "durable-ack"
	EventPromotion   EventKind = "promotion"
	EventStalePublsh EventKind = "stale-publish"
	EventFailover    EventKind = "failover"
	EventSnapshot    EventKind = "snapshot"
	EventTruncate    EventKind = "truncate"
	EventIOClass     EventKind = "io-class"
	EventVolumeServe EventKind = "volume-serve"
)

// Event is one recorded step. Fields are typed and optional; only those relevant
// to the event's kind are set. Checkers read these.
type Event struct {
	Step int
	Kind EventKind
	Msg  string
	// Clock events:
	Mono clock.Instant
	// Object/Delete events:
	Key       string
	Permanent bool // Delete: whether it was a permanent (irreversible) delete
	// Watermark events (§5.6):
	Local     uint64
	Durable   uint64
	Published uint64
	// LeavesHost events (§5.10): true if cleartext guest data was detected in bytes
	// bound for outside the host (a violation).
	ClearLeak bool
	// DurableAck events (§12.2): whether the host lease was valid at the instant a
	// FLUSH was ACKed as durable. Must always be true (INV-06).
	LeaseValid bool
	// Promotion events (§12.3): whether epoch N+1 was granted before FENCING_WAIT
	// elapsed. Must always be false (INV-11).
	EarlyGrant bool
	// StalePublish events (§12.4): whether a fenced/stale-epoch writer managed to
	// publish. Must always be false (INV-10).
	StalePublishOK bool
	// Failover events (§12): the fenced writer's ACKed-durable sequence and the
	// promoted writer's recovered prefix. Recovered must be >= AckedDurable (INV-09).
	AckedDurable uint64
	Recovered    uint64
	// Snapshot events (§5.2, §19): whether a published snapshot was later observed to
	// change (must be false, INV-16).
	SnapshotMutated bool
	// Truncate events (§21.1): the sequence local WAL was reclaimed to, and the
	// verified published point. TruncatedUpTo must be <= Published (INV-13).
	TruncatedUpTo uint64
	// IOClass events (§5.9): whether a background op was granted while a
	// foreground/flush op was in flight. Granted-while-high must never be true (INV-17).
	BgGranted    bool
	HighInFlight bool
	// VolumeServe events (§16, §12.3): whether the Agent still had something to answer
	// a fenced volume's requests with. Must always be false (INV-10's Agent half).
	ServedAfterFence bool
}

// String renders an event deterministically for the trace.
func (e Event) String() string {
	switch e.Kind {
	case EventClock:
		return fmt.Sprintf("%04d clock mono=%d %s", e.Step, e.Mono, e.Msg)
	case EventDelete:
		return fmt.Sprintf("%04d delete key=%s permanent=%t", e.Step, e.Key, e.Permanent)
	case EventWatermark:
		return fmt.Sprintf("%04d watermark pub=%d dur=%d loc=%d", e.Step, e.Published, e.Durable, e.Local)
	case EventLeavesHost:
		return fmt.Sprintf("%04d leaves-host clear_leak=%t %s", e.Step, e.ClearLeak, e.Msg)
	case EventDurableAck:
		return fmt.Sprintf("%04d durable-ack seq=%d lease_valid=%t", e.Step, e.Durable, e.LeaseValid)
	case EventPromotion:
		return fmt.Sprintf("%04d promotion early_grant=%t %s", e.Step, e.EarlyGrant, e.Msg)
	case EventStalePublsh:
		return fmt.Sprintf("%04d stale-publish succeeded=%t", e.Step, e.StalePublishOK)
	case EventFailover:
		return fmt.Sprintf("%04d failover acked_durable=%d recovered=%d", e.Step, e.AckedDurable, e.Recovered)
	case EventSnapshot:
		return fmt.Sprintf("%04d snapshot mutated=%t %s", e.Step, e.SnapshotMutated, e.Msg)
	case EventTruncate:
		return fmt.Sprintf("%04d truncate up_to=%d published=%d", e.Step, e.TruncatedUpTo, e.Published)
	case EventVolumeServe:
		return fmt.Sprintf("%04d volume-serve vol=%s served_after_fence=%t", e.Step, e.Key, e.ServedAfterFence)
	case EventIOClass:
		return fmt.Sprintf("%04d io-class bg_granted=%t high_in_flight=%t", e.Step, e.BgGranted, e.HighInFlight)
	case EventObject:
		return fmt.Sprintf("%04d object key=%s %s", e.Step, e.Key, e.Msg)
	default:
		return fmt.Sprintf("%04d %s %s", e.Step, e.Kind, e.Msg)
	}
}

// Sim bundles a seeded world: PRNG plus the simulated interfaces, an event
// recorder, and the registered checkers.
type Sim struct {
	Seed  int64
	Rand  *rand.Rand
	Clock *sim.Clock
	Disk  *sim.Disk
	Store *sim.ObjectStore
	Net   *sim.Network

	checkers []Checker
	events   []Event
	trace    []string
}

// Emit records an event, feeds it to every checker, and appends to the trace.
func (s *Sim) Emit(e Event) {
	e.Step = len(s.events)
	s.events = append(s.events, e)
	for _, c := range s.checkers {
		c.Observe(e)
	}
	s.trace = append(s.trace, e.String())
}

// Tick advances the deterministic clock by d and records the new monotonic time.
func (s *Sim) Tick(d time.Duration) {
	s.Clock.Advance(d)
	s.Emit(Event{Kind: EventClock, Mono: s.Clock.Now(), Msg: fmt.Sprintf("advance=%s", d)})
}

// Notef records a free-form note in the trace.
func (s *Sim) Notef(format string, args ...any) {
	s.Emit(Event{Kind: EventNote, Msg: fmt.Sprintf(format, args...)})
}

// Scenario is a deterministic program driven by the harness. Returning an error
// fails the run (a scenario-level assertion failure).
type Scenario func(*Sim) error

// Result is the outcome of a single run.
type Result struct {
	Seed  int64
	Trace []string
	Err   error // scenario error or the first checker violation
}

// TraceString joins the trace for printing.
func (r Result) TraceString() string { return strings.Join(r.Trace, "\n") }

// Run executes sc against a fresh world built from seed, with the given checkers,
// then evaluates every checker. It never touches real time, sockets, or disk.
func Run(seed int64, sc Scenario, checkers ...Checker) Result {
	s := &Sim{
		Seed:     seed,
		Rand:     rand.New(rand.NewSource(seed)),
		Clock:    sim.NewClock(time.Unix(simEpoch, 0).UTC()),
		Disk:     sim.NewDisk(),
		Store:    sim.NewObjectStore(),
		Net:      sim.NewNetwork(),
		checkers: checkers,
	}

	if err := sc(s); err != nil {
		return Result{Seed: seed, Trace: s.trace, Err: fmt.Errorf("scenario failed (reproduce with seed=%d): %w", seed, err)}
	}
	for _, c := range s.checkers {
		if err := c.Check(); err != nil {
			return Result{Seed: seed, Trace: s.trace, Err: fmt.Errorf("checker %q failed (reproduce with seed=%d): %w", c.Name(), seed, err)}
		}
	}
	return Result{Seed: seed, Trace: s.trace}
}
