// Package lifecycle holds the typed vocabularies for everything in the system that
// has a lifecycle: host fleet states (§28.1), volume ownership states (§7) and
// snapshot states (§19).
//
// §16's Agent-side per-volume machine is deliberately not here. It was written
// first, so that "Phases 02/03 implement the doc's machine rather than reinventing
// one", and then the Agent was built and reinvented nothing: it imports exactly one
// symbol from this package (lifecycle.VolumeActive) and tracks a volume's serving
// state in internal/agent, per volume, next to the WAL and the lease it actually
// depends on. Nine constants, a transition table and two tests spent three phases
// describing a machine no process ran. It is deleted rather than kept as
// documentation because a vocabulary in this package is a claim that some store or
// boundary parses values into it, and this one made that claim falsely — which is
// exactly how a reader concludes the Agent has states it does not have.
//
// Each vocabulary is a distinct named type with an explicit transition table taken
// from the architecture document. That buys three things a bare string cannot:
//
//   - a state from the wrong vocabulary does not compile;
//   - the zero value is not a valid state, so a field nobody set is caught rather
//     than silently reading as ACTIVE;
//   - an illegal lifecycle move (§7's "promotion always passes through FENCING_WAIT",
//     §19's "a PUBLISHED snapshot never changes") is a returned error at the store
//     boundary, and Predecessors feeds the SQL guard so Postgres enforces the same
//     rule atomically.
//
// The package is dependency-free on purpose: it is imported by metadata, the Control
// Plane, placement, and the data path, and must never pull them back in.
package lifecycle

import (
	"errors"
	"fmt"
)

// Errors.
var (
	// ErrInvalidTransition means the lifecycle forbids this move.
	ErrInvalidTransition = errors.New("lifecycle: invalid state transition")
	// ErrUnknownState means a value from outside Go is not part of the vocabulary.
	ErrUnknownState = errors.New("lifecycle: unknown state")
)

// name constrains a vocabulary to a string-backed type: the stored representation is
// the string, so the DB, JSON descriptors, and traces stay human-readable.
type name interface{ ~string }

// machine is a table-driven transition set. A state may always transition to itself
// (an idempotent re-write of the same value is not a lifecycle move).
type machine[S name] struct {
	kind  string
	all   []S
	edges map[S]map[S]bool
}

func newMachine[S name](kind string, all []S, edges map[S][]S) *machine[S] {
	m := &machine[S]{kind: kind, all: all, edges: make(map[S]map[S]bool, len(edges))}
	for from, tos := range edges {
		set := make(map[S]bool, len(tos))
		for _, to := range tos {
			set[to] = true
		}
		m.edges[from] = set
	}
	return m
}

func (m *machine[S]) valid(s S) bool {
	for _, candidate := range m.all {
		if candidate == s {
			return true
		}
	}
	return false
}

func (m *machine[S]) allows(from, to S) bool {
	if !m.valid(from) || !m.valid(to) {
		return false
	}
	if from == to {
		return true
	}
	return m.edges[from][to]
}

func (m *machine[S]) transition(from, to S) error {
	if m.allows(from, to) {
		return nil
	}
	return fmt.Errorf("%w: %s %q -> %q", ErrInvalidTransition, m.kind, string(from), string(to))
}

func (m *machine[S]) parse(raw string) (S, error) {
	s := S(raw)
	if !m.valid(s) {
		var zero S
		return zero, fmt.Errorf("%w: %s %q", ErrUnknownState, m.kind, raw)
	}
	return s, nil
}

// predecessors returns every state that may legally become `to`, including `to`.
func (m *machine[S]) predecessors(to S) []S {
	if !m.valid(to) {
		return nil
	}
	out := []S{to}
	for _, from := range m.all {
		if from != to && m.edges[from][to] {
			out = append(out, from)
		}
	}
	return out
}

func names[S name](states []S) []string {
	out := make([]string, len(states))
	for i, s := range states {
		out[i] = string(s)
	}
	return out
}

// --- Host fleet state (§28.1) ---

// HostState is a host's placement/maintenance state. Only ACTIVE takes new work.
type HostState string

// Host states (§28.1).
const (
	HostActive   HostState = "ACTIVE"
	HostCordoned HostState = "CORDONED"
	HostDraining HostState = "DRAINING"
	HostDead     HostState = "DEAD"
)

var hostMachine = newMachine("host state",
	[]HostState{HostActive, HostCordoned, HostDraining, HostDead},
	map[HostState][]HostState{
		// Cordon stops new placement; drain evacuates; dead is the fenced-out host.
		HostActive:   {HostCordoned, HostDraining, HostDead},
		HostCordoned: {HostActive, HostDraining, HostDead},
		HostDraining: {HostActive, HostCordoned, HostDead},
		// A dead host is still evacuated (the common case), and a repaired one returns.
		HostDead: {HostActive, HostCordoned, HostDraining},
	})

// HostStates returns every host state.
func HostStates() []HostState { return hostMachine.all }

// ParseHostState converts a stored value, rejecting anything else.
func ParseHostState(raw string) (HostState, error) { return hostMachine.parse(raw) }

func (s HostState) String() string { return string(s) }

// Valid reports whether s is a declared host state.
func (s HostState) Valid() bool { return hostMachine.valid(s) }

// AcceptsPlacement is the §28.1/§28.2 rule: only an ACTIVE host takes new volumes.
func (s HostState) AcceptsPlacement() bool { return s == HostActive }

// Serving reports whether the fleet still counts this host as a writer, which is
// every state but DEAD. Taking new work and serving what you already hold are two
// different questions: a CORDONED host is not given new volumes and a DRAINING one
// is being evacuated, but both still ACK for the volumes they hold, so both keep
// their lease (§12.6). DEAD is the Control Plane asserting the writer is gone — the
// assertion promotion accepts as a reason to skip the fencing wait — so it is the
// one state in which a lease must not be renewed.
func (s HostState) Serving() bool { return s.Valid() && s != HostDead }

// ServingHostStateNames is Serving as stored strings — the store's SQL predicate.
func ServingHostStateNames() []string {
	var serving []HostState
	for _, s := range hostMachine.all {
		if s.Serving() {
			serving = append(serving, s)
		}
	}
	return names(serving)
}

// CanTransitionTo reports whether the fleet lifecycle allows this move.
func (s HostState) CanTransitionTo(to HostState) bool { return hostMachine.allows(s, to) }

// Transition returns ErrInvalidTransition unless the move is allowed.
func (s HostState) Transition(to HostState) error { return hostMachine.transition(s, to) }

// Predecessors returns the states that may become s (including s).
func (s HostState) Predecessors() []HostState { return hostMachine.predecessors(s) }

// PredecessorNames is Predecessors as stored strings — the store's SQL guard.
func (s HostState) PredecessorNames() []string { return names(s.Predecessors()) }

// --- Why a host is cordoned, and who may change it (ADR-0013 §3, §5) ---

// CordonReason is why a host is CORDONED, and — because in this system the reason
// and the actor are the same thing — the authority a write to hosts.state carries.
//
// It exists because cordon stopped being something only a human does. ADR-0013 §3
// makes the Control Plane cordon a host whose device passes 70% used, so an operator
// looking at a CORDONED host can no longer assume somebody meant it; and, in the
// other direction, the automatic loop must never clear a cordon a human put there
// for a reason it cannot see (a failing NIC, a kernel it is about to reboot).
// A cordon with no recorded cause is a cordon nobody can safely undo.
//
// One value serves both questions on purpose. A second column — `cordoned_by`
// alongside `cordon_reason` — was rejected: two columns that must agree are two
// columns that can disagree, and nothing in this system would ever set them to
// different things. There is exactly one automatic actor and exactly one human one.
type CordonReason string

// Cordon reasons. The zero value means "not cordoned": it is a stored value, never
// an argument, so a caller that forgot to say who it is fails rather than silently
// writing a cordon with no cause (see Authority).
const (
	CordonNone     CordonReason = ""
	CordonOperator CordonReason = "OPERATOR"
	CordonPressure CordonReason = "DEVICE_PRESSURE"
)

var cordonReasons = []CordonReason{CordonNone, CordonOperator, CordonPressure}

// cordonOverwrite is the authority table: for a write made *for* the key reason,
// the stored reasons it may replace.
//
// The asymmetry is the whole point. A human outranks the pressure loop, so an
// operator write lands whatever the host currently says. The pressure loop does not
// outrank a human, so it may only touch a host that is uncordoned or that it
// cordoned itself — which is what stops the 70% rule from un-cordoning a host a
// human took out of service deliberately.
var cordonOverwrite = map[CordonReason][]CordonReason{
	CordonOperator: {CordonNone, CordonOperator, CordonPressure},
	CordonPressure: {CordonNone, CordonPressure},
}

// ErrCordonHeld means a write was refused because the host's cordon was placed by an
// authority this writer does not outrank — in practice, the pressure loop meeting a
// cordon an operator set.
var ErrCordonHeld = errors.New("lifecycle: cordon held by a higher authority")

// CordonReasons returns every stored reason, including CordonNone.
func CordonReasons() []CordonReason { return cordonReasons }

// ParseCordonReason converts a stored value, rejecting anything else.
func ParseCordonReason(raw string) (CordonReason, error) {
	r := CordonReason(raw)
	if !r.Valid() {
		return "", fmt.Errorf("%w: cordon reason %q", ErrUnknownState, raw)
	}
	return r, nil
}

func (r CordonReason) String() string { return string(r) }

// Valid reports whether r is a declared reason (CordonNone included).
func (r CordonReason) Valid() bool {
	for _, candidate := range cordonReasons {
		if candidate == r {
			return true
		}
	}
	return false
}

// Authority reports whether r may be given as the reason for a write. CordonNone
// cannot: "nobody" is a state a host can be in, not an actor that can ask for one.
func (r CordonReason) Authority() bool { return len(cordonOverwrite[r]) > 0 }

// MayOverwrite reports whether a write made for reason r may replace a host whose
// cordon currently records `current`.
func (r CordonReason) MayOverwrite(current CordonReason) bool {
	for _, allowed := range cordonOverwrite[r] {
		if allowed == current {
			return true
		}
	}
	return false
}

// OverwritableNames is MayOverwrite as stored strings — the store's SQL predicate,
// the same move PredecessorNames makes for the transition table. The rule has to be
// *in* the statement that writes the state: a read-then-write in Go leaves a window
// in which an operator's cordon lands between the two and the pressure loop clears
// it anyway, which is the one outcome this whole type exists to prevent.
func (r CordonReason) OverwritableNames() []string { return names(cordonOverwrite[r]) }

// --- Why a volume is not being served by the host that holds it ---

// Refusal is why the Agent that holds a volume is not serving it. It is the fleet-side
// name for the fail-closed decisions the data path makes at attach: the image the
// catalog says exists is not in the bucket, the volume came back below the sequence a
// guest was told was durable, the read view never resolved, the host has no key for an
// encrypted volume, or the host's lease lapsed and it gave the device up.
//
// **It is a closed vocabulary and not a free string.** Both are defensible and the
// choice is worth writing down. A string needs no schema change when a new refusal
// appears and carries the Agent's own sentence — but the string that would actually be
// stored is `err.Error()`, which embeds a volume id and a sequence number, so no two
// rows ever compare equal, the `-fleet-status` column becomes a vocabulary nobody
// controls, and the first alert anyone writes on it matches a substring. Every value
// here is a decision made at a named line in internal/agent, so a new refusal is a new
// code path in this repository and extending the vocabulary is the same commit: the
// cost of the closed set falls on the person who is already editing both sides.
//
// The sentence an operator's next step needs — which sequence, which key — is carried
// alongside as free text that nothing branches on (metadata.Volume.RefusalDetail). Same
// split as a snapshot's id and its error message.
//
// RefusalNone is the zero value and it means "this host is serving the volume". That is
// what makes the field self-clearing: every accepted report writes it, so a volume that
// starts serving again overwrites the reason rather than needing anything to notice.
type Refusal string

// Refusals. The empty value is stored, never argued: a report that names no refusal is
// a host saying it is serving the volume.
const (
	RefusalNone Refusal = ""
	// RefusalImageMissing: the catalog says the volume published an image and the
	// object store holds none (agent.ErrImageMissing).
	RefusalImageMissing Refusal = "IMAGE_MISSING"
	// RefusalDurabilityLost: replay came back below the sequence the catalog last
	// recorded as ACKed to a guest (agent.ErrDurabilityLost).
	RefusalDurabilityLost Refusal = "DURABILITY_LOST"
	// RefusalNoReadView: the read view never resolved, so every read fails and the
	// session will not be published (agent.ErrNoReadView, wal.ErrBaseUnavailable).
	RefusalNoReadView Refusal = "NO_READ_VIEW"
	// RefusalNoKey: the catalog says the volume is encrypted and this Agent holds no
	// KEK, or holds the wrong one (agent.ErrNoKEK).
	RefusalNoKey Refusal = "NO_KEY"
	// RefusalLeaseLost: the host lease lapsed on the Agent's own monotonic clock, so
	// it gave the device up rather than keep answering for a volume it can no longer
	// confirm it owns.
	RefusalLeaseLost Refusal = "LEASE_LOST"
	// RefusalAttachFailed: everything else that stopped the runtime from starting — a
	// socket that could not be bound, a WAL that would not resume, the host's own
	// -max-volumes ceiling. It is a catch-all on purpose: without one, a refusal with
	// no enum value of its own would be invisible again, which is the whole failure
	// this vocabulary exists to close.
	RefusalAttachFailed Refusal = "ATTACH_FAILED"
)

var refusals = []Refusal{
	RefusalNone, RefusalImageMissing, RefusalDurabilityLost, RefusalNoReadView,
	RefusalNoKey, RefusalLeaseLost, RefusalAttachFailed,
}

// Refusals returns every stored value, RefusalNone included.
func Refusals() []Refusal { return refusals }

// ParseRefusal converts a stored value, rejecting anything else.
func ParseRefusal(raw string) (Refusal, error) {
	r := Refusal(raw)
	if !r.Valid() {
		return "", fmt.Errorf("%w: volume refusal %q", ErrUnknownState, raw)
	}
	return r, nil
}

func (r Refusal) String() string { return string(r) }

// Valid reports whether r is a declared value (RefusalNone included).
func (r Refusal) Valid() bool {
	for _, candidate := range refusals {
		if candidate == r {
			return true
		}
	}
	return false
}

// Refused reports whether r says the volume is not being served. It is the predicate
// -fleet-status counts with, and it is a method rather than `!= ""` at each call site
// so that a value added above is covered by whoever forgets to update a comparison.
func (r Refusal) Refused() bool { return r != RefusalNone && r.Valid() }

// RefusalNames is the vocabulary as stored strings — the schema's CHECK list, and what
// a test compares the two against so the Go type and the column cannot drift.
func RefusalNames() []string { return names(refusals) }

// --- Volume ownership state, Control Plane side (§7) ---

// VolumeState is the Control Plane's view of a volume's writer ownership (§7).
type VolumeState string

// Volume states (§7 writer-failover state machine).
const (
	VolumeActive           VolumeState = "ACTIVE"
	VolumePrimarySuspected VolumeState = "PRIMARY_SUSPECTED"
	VolumeFencingWait      VolumeState = "FENCING_WAIT"
	VolumeRecoveryRequired VolumeState = "RECOVERY_REQUIRED"
	VolumeRecovering       VolumeState = "RECOVERING"
	VolumeDetached         VolumeState = "DETACHED"
)

var volumeMachine = newMachine("volume state",
	[]VolumeState{VolumeActive, VolumePrimarySuspected, VolumeFencingWait,
		VolumeRecoveryRequired, VolumeRecovering, VolumeDetached},
	map[VolumeState][]VolumeState{
		VolumeDetached:         {VolumeActive},
		VolumeActive:           {VolumePrimarySuspected, VolumeDetached},
		VolumePrimarySuspected: {VolumeActive, VolumeFencingWait},
		// §7: a new writer is never promoted on a missed heartbeat alone — the path
		// out of FENCING_WAIT leads to recovery, never straight back to serving.
		VolumeFencingWait:      {VolumeRecoveryRequired},
		VolumeRecoveryRequired: {VolumeRecovering, VolumeDetached},
		VolumeRecovering:       {VolumeActive, VolumeRecoveryRequired},
	})

// VolumeStates returns every Control-Plane volume state.
func VolumeStates() []VolumeState { return volumeMachine.all }

// ParseVolumeState converts a stored value, rejecting anything else.
func ParseVolumeState(raw string) (VolumeState, error) { return volumeMachine.parse(raw) }

func (s VolumeState) String() string { return string(s) }

// Valid reports whether s is a declared volume state.
func (s VolumeState) Valid() bool { return volumeMachine.valid(s) }

// CanTransitionTo reports whether the §7 machine allows this move.
func (s VolumeState) CanTransitionTo(to VolumeState) bool { return volumeMachine.allows(s, to) }

// Transition returns ErrInvalidTransition unless the move is allowed.
func (s VolumeState) Transition(to VolumeState) error { return volumeMachine.transition(s, to) }

// Predecessors returns the states that may become s (including s).
func (s VolumeState) Predecessors() []VolumeState { return volumeMachine.predecessors(s) }

// PredecessorNames is Predecessors as stored strings — the store's SQL guard.
func (s VolumeState) PredecessorNames() []string { return names(s.Predecessors()) }

// --- Snapshot state (§19) ---

// SnapshotState is a snapshot's catalog state (§19).
type SnapshotState string

// Snapshot states (§19: CREATING → PUBLISHED | FAILED → DELETING).
const (
	SnapshotCreating  SnapshotState = "CREATING"
	SnapshotPublished SnapshotState = "PUBLISHED"
	SnapshotFailed    SnapshotState = "FAILED"
	SnapshotDeleting  SnapshotState = "DELETING"
)

var snapshotMachine = newMachine("snapshot state",
	[]SnapshotState{SnapshotCreating, SnapshotPublished, SnapshotFailed, SnapshotDeleting},
	map[SnapshotState][]SnapshotState{
		SnapshotCreating: {SnapshotPublished, SnapshotFailed},
		// INV-16: once PUBLISHED the snapshot never changes; the only move left is
		// deletion, which is the catalog side of the GC's reversible marking (§21.3).
		SnapshotPublished: {SnapshotDeleting},
		SnapshotFailed:    {SnapshotDeleting},
		SnapshotDeleting:  {},
	})

// SnapshotStates returns every snapshot state.
func SnapshotStates() []SnapshotState { return snapshotMachine.all }

// Unfinished reports whether something is still owed on a snapshot in this state.
// CREATING is owed by the Agent serving the volume; DELETING is owed by a reclaim
// ADR-0026 deleted, so nothing will ever move it and a snapshot that reaches it
// stays there — which is why it belongs in the same answer rather than being read as
// "on its way out".
//
// PUBLISHED and FAILED are finished: one is the result, the other is a request that
// is over. Neither is a terminal state of the machine (both may still become
// DELETING), so this is deliberately not `len(successors) == 0` — that predicate
// would call a permanently stuck DELETING snapshot finished and a published one
// outstanding, exactly backwards.
func (s SnapshotState) Unfinished() bool {
	return s == SnapshotCreating || s == SnapshotDeleting
}

// UnfinishedSnapshotStateNames is the Unfinished set as stored strings — a store's
// SQL filter. Derived from the vocabulary rather than written out, so the SQL and
// the Go predicate above cannot disagree.
func UnfinishedSnapshotStateNames() []string {
	var out []SnapshotState
	for _, s := range SnapshotStates() {
		if s.Unfinished() {
			out = append(out, s)
		}
	}
	return names(out)
}

// ParseSnapshotState converts a stored value, rejecting anything else.
func ParseSnapshotState(raw string) (SnapshotState, error) { return snapshotMachine.parse(raw) }

func (s SnapshotState) String() string { return string(s) }

// Valid reports whether s is a declared snapshot state.
func (s SnapshotState) Valid() bool { return snapshotMachine.valid(s) }

// CanTransitionTo reports whether the §19 lifecycle allows this move.
func (s SnapshotState) CanTransitionTo(to SnapshotState) bool { return snapshotMachine.allows(s, to) }

// Transition returns ErrInvalidTransition unless the move is allowed.
func (s SnapshotState) Transition(to SnapshotState) error { return snapshotMachine.transition(s, to) }

// Predecessors returns the states that may become s (including s).
func (s SnapshotState) Predecessors() []SnapshotState { return snapshotMachine.predecessors(s) }

// PredecessorNames is Predecessors as stored strings — the store's SQL guard.
func (s SnapshotState) PredecessorNames() []string { return names(s.Predecessors()) }

// There are no reconciliation operations here any more. §7's OperationKind
// (attach|detach|clone|resize|drain|recovery|flatten|gc) and OperationPhase
// (PENDING…SUCCEEDED) were the vocabulary of the `operations` table, and both went
// with it: ADR-0026 withdrew the drain, the promotion and the recovery those rows
// converged, and nothing outside a test ever wrote one. The kinds that describe work
// V1 still does — a snapshot, a clone — were never phases of an operation row; they
// are one Control-Plane call each, and what they are waiting on is the snapshot's own
// §19 state.
//
// There is no durability mode here any more. §14.8 once let a volume choose between
// ACKing a FLUSH on the local fdatasync and ACKing it only once a verified object
// existed; ADR-0026 withdrew the remote half, so the local ACK is the *only* contract
// and there is nothing left to select. The enum outlived it by a whole increment —
// stored in a column, carried on the wire, written into the descriptor, validated at
// three boundaries — and no reader anywhere branched on it. A mode nobody can select
// is not an option kept open, it is a second contract that has to be kept correct for
// free, and the day someone re-reads it the code will silently promise durability the
// data path stopped providing. Reintroducing a choice means reintroducing the
// mechanism that honours it, and that is ADR-0026's decision to reopen, not a field's.
