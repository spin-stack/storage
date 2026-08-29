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

// The list is short, and everything a data path used to emit is off it. The kinds that
// went — watermark, leaves-host, stale-publish, truncate, volume-serve, durable-read,
// carry, refusal — were the vocabulary the local block engine's scenarios spoke, and
// each named a fact only that engine could state. A kind nothing emits is a kind the
// next author writes a scenario against and finds no checker for, so they leave with
// their producers; the commit protocol declares the ones it needs, in the commit that
// emits them.
const (
	EventClock   EventKind = "clock"
	EventNetwork EventKind = "network"
	EventNote    EventKind = "note"
	EventDelete  EventKind = "delete"
	EventFault   EventKind = "fault"
	// EventPublish is one host's attempt at v6 §9's protocol, successful or not. It
	// carries the manifest's own parent rather than what the scenario believed, so a
	// checker reads what the protocol produced.
	EventPublish EventKind = "publish"
	// EventPut is an object arriving at the store, with the bytes. It is how a checker
	// asserts on what really left the host instead of on a claim about it.
	EventPut EventKind = "put"
	// EventSeal is QEMU switching a volume's guest onto a new tip, which is what makes
	// the old one a complete, read-only layer. It is a fact about the simulated world
	// and not about the code under test: noticing it is the reconciler's job, and a
	// checker that read the reconciler's own record of it could not tell a layer that
	// was lost from a layer that never existed.
	EventSeal EventKind = "seal"
)

// Event is one recorded step. Fields are typed and optional; only those relevant
// to the event's kind are set. Checkers read these.
type Event struct {
	Step int
	Kind EventKind
	Msg  string
	// Clock events:
	Mono clock.Instant
	// Delete and Put events:
	Key       string
	Permanent bool // Delete: whether it was a permanent (irreversible) delete
	// Put events: the bytes as the store received them. Deliberately not rendered in
	// the trace — a layer is the one object here measured in kilobytes.
	Body []byte
	// Publish events:
	Host           string
	CommitID       string
	ParentCommitID string
	// LayerID is the layer a commit carries, read back from the manifest the protocol
	// wrote, and on a Seal event the layer that has just stopped being the tip.
	LayerID string
	// OK is whether the operation succeeded, for Publish and Put alike.
	OK bool
}

// String renders an event deterministically for the trace.
func (e Event) String() string {
	switch e.Kind {
	case EventClock:
		return fmt.Sprintf("%04d clock mono=%d %s", e.Step, e.Mono, e.Msg)
	case EventDelete:
		return fmt.Sprintf("%04d delete key=%s permanent=%t", e.Step, e.Key, e.Permanent)
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
