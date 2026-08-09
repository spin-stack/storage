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
	EventStalePublsh EventKind = "stale-publish"
	EventTruncate    EventKind = "truncate"
	EventVolumeServe EventKind = "volume-serve"
	EventDurableRead EventKind = "durable-read"
	EventCarry       EventKind = "carry"
	EventRefusal     EventKind = "refusal"
)

// CarryPhase says what a carry event states about one WAL record: what a guest was
// promised, or what the device was found to hold afterwards.
type CarryPhase string

const (
	// CarryPromised: the record's fdatasync returned, so a guest's fsync was ACKed on
	// it. Emitted once per (volume, sequence), with the plaintext the guest wrote.
	CarryPromised CarryPhase = "promised"
	// CarrySurvived: the record was found on the device by a scan, under the epoch the
	// event names, with the plaintext it reads back as.
	CarrySurvived CarryPhase = "survived"
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
	// StalePublish events (§12.4): whether a second incarnation of a volume managed to
	// publish over the first's image. Must always be false — under ADR-0026 this is all
	// that is left of INV-10, and it is a compare-and-set on one object.
	StalePublishOK bool
	// Truncate events (§21.1): the sequence local WAL was reclaimed to, and the
	// verified published point. TruncatedUpTo must be <= Published (INV-13).
	TruncatedUpTo uint64
	// VolumeServe events (§16, §12.3): whether the Agent still had something to answer
	// a fenced volume's requests with. Must always be false (INV-10's Agent half).
	ServedAfterFence bool
	// DurableRead events (§5.8): whether a range the volume ACKed as durable came back
	// as zeros after a restart. Must always be false (INV-08 from the guest's side).
	ZerosAfterRestart bool
	// ForeignBytesAfterRestart is the same violation wearing different clothes: the
	// read was answered, and with neither the guest's bytes nor zeros. Zeros are the
	// shape a *missing* base has; this is the shape a base rebuilt *wrongly* has —
	// undecrypted ciphertext being the case that shipped, since GCM leaves the length
	// intact and nothing downstream re-checks the plaintext CRC. Must always be false.
	ForeignBytesAfterRestart bool

	// Carry events (wal.Log.CarryForward): one WAL record either promised to a guest
	// or found on the device afterwards. Key carries the volume id.
	CarryPhase CarryPhase
	// Epoch is the epoch the record was promised under (promised) or is filed under
	// (survived). Sequence is the record's, which a carry must never change.
	Epoch    uint64
	Sequence uint64
	// Digest is over the *plaintext* — what the guest wrote, or what the record on the
	// device reads back as once opened. A resealed record has different bytes on disk
	// and the same digest; a copied ciphertext has the same bytes and a different one,
	// which is the whole reason the digest is not taken over the encoded record.
	Digest string
	// Refusal events: the word the *catalog* holds about a volume its host is not
	// serving, once the report has crossed the wire, and whether that volume still has a
	// socket bound on the host. Key carries the volume id.
	//
	// The two travel on one event because the failure they describe is a conjunction:
	// either half alone is satisfied by an implementation that got the other badly
	// wrong — a volume with no device that vanished from the wire, or one the fleet
	// knows is refused that is still handing a guest a device that errors.
	Refusal     string
	SocketBound bool

	// Scan groups the survived events of one observation of one device. It is what
	// scopes "the same sequence twice under one (volume, epoch)" to a single moment,
	// rather than to a record legitimately seen again by a later scan.
	Scan uint64
	// Settled marks the observation taken after the carry was given its last chance to
	// finish. Only those decide whether a record was lost; an earlier scan is a
	// half-finished state on purpose.
	Settled bool
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
	case EventStalePublsh:
		return fmt.Sprintf("%04d stale-publish succeeded=%t", e.Step, e.StalePublishOK)
	case EventTruncate:
		return fmt.Sprintf("%04d truncate up_to=%d published=%d", e.Step, e.TruncatedUpTo, e.Published)
	case EventDurableRead:
		return fmt.Sprintf("%04d durable-read vol=%s zeros_after_restart=%t foreign_bytes_after_restart=%t",
			e.Step, e.Key, e.ZerosAfterRestart, e.ForeignBytesAfterRestart)
	case EventVolumeServe:
		return fmt.Sprintf("%04d volume-serve vol=%s served_after_fence=%t", e.Step, e.Key, e.ServedAfterFence)
	case EventRefusal:
		return fmt.Sprintf("%04d refusal vol=%s catalog=%q socket_bound=%t", e.Step, e.Key, e.Refusal, e.SocketBound)
	case EventCarry:
		return fmt.Sprintf("%04d carry %s vol=%s epoch=%d seq=%d digest=%s scan=%d settled=%t",
			e.Step, e.CarryPhase, e.Key, e.Epoch, e.Sequence, e.Digest, e.Scan, e.Settled)
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
