// Package lifecycle holds the typed vocabularies for everything in the system that
// has a lifecycle: host fleet states (§28.1), volume ownership states (§7) and
// snapshot states (§19).
//
// §16's Agent-side per-volume machine is deliberately not here: the Agent imports one symbol
// (lifecycle.VolumeActive) and tracks serving state per volume in internal/agent. A
// vocabulary in this package claims some store parses values into it; that one did not.
//
// Each vocabulary is a distinct named type with the architecture document's transition table:
// a state from the wrong vocabulary does not compile, the zero value is not a valid state (an
// unset field is caught rather than reading as ACTIVE), and an illegal move (§7's "promotion
// always passes through FENCING_WAIT", §19's "a PUBLISHED snapshot never changes") is an error
// at the store boundary — Predecessors feeds the SQL guard so Postgres enforces it atomically.
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

// CordonReason is why a host is CORDONED and — because here the reason and the actor are the
// same thing — the authority a write to hosts.state carries. Cordon is no longer only a
// human's: ADR-0013 §3 cordons a host past 70% used, and the automatic loop must never clear
// a cordon a human set for a reason it cannot see.
//
// Rejected: a second `cordoned_by` column — two columns that must agree are two columns that
// can disagree, and there is exactly one automatic actor and one human one.
type CordonReason string

// Cordon reasons. The zero value means "not cordoned": it is a stored value, never
// an argument, so a caller that forgot to say who it is fails rather than silently
// writing a cordon with no cause (see Authority).
const (
	CordonNone     CordonReason = ""
	CordonOperator CordonReason = "OPERATOR"
	CordonPressure CordonReason = "DEVICE_PRESSURE"
	// CordonStalledPublish: a volume on this host has a sealed layer it tried and failed
	// to publish (v6 §11). Separate from DEVICE_PRESSURE because it is a different fact
	// with a different fix — the disk is fine and the object store is not — and because a
	// host can be in one without the other. It is what stops the fleet placing more
	// volumes on a machine where every one of them would inherit a broken RPO.
	CordonStalledPublish CordonReason = "STALLED_PUBLISH"
)

var cordonReasons = []CordonReason{CordonNone, CordonOperator, CordonPressure, CordonStalledPublish}

// cordonOverwrite is the authority table: for a write made *for* the key reason, the stored
// reasons it may replace. The asymmetry is the point — an operator write lands on anything,
// while the pressure loop may only touch a host that is uncordoned or that it cordoned itself,
// so the 70% rule cannot un-cordon a host a human took out of service.
var cordonOverwrite = map[CordonReason][]CordonReason{
	CordonOperator: {CordonNone, CordonOperator, CordonPressure, CordonStalledPublish},
	// The two automatic reasons may replace each other: both are the Control Plane's own
	// decision from a number the host reported, and a host that is both full and unable to
	// publish should read as whichever it currently is rather than as whichever happened
	// first. Neither touches an operator's.
	CordonPressure:       {CordonNone, CordonPressure, CordonStalledPublish},
	CordonStalledPublish: {CordonNone, CordonPressure, CordonStalledPublish},
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

// OverwritableNames is MayOverwrite as stored strings — the store's SQL predicate. The rule
// has to be *in* the write: a read-then-write in Go leaves a window where an operator's cordon
// lands between the two and the pressure loop clears it anyway.
func (r CordonReason) OverwritableNames() []string { return names(cordonOverwrite[r]) }

// --- Why a volume is not being served by the host that holds it ---

// Refusal is why the Agent that holds a volume is not serving it — the fleet-side name for
// the data path's fail-closed decisions at attach.
//
// A closed vocabulary and not a free string: the string that would actually be stored is
// `err.Error()`, which embeds a volume id and a sequence number, so no two rows compare
// equal, the -fleet-status column becomes a vocabulary nobody controls, and the first alert
// written on it matches a substring. Every value here is a decision at a named line in
// internal/agent, so extending the vocabulary is the same commit. The sentence an operator
// needs — which sequence, which key — rides alongside as free text nothing branches on
// (metadata.Volume.RefusalDetail).
//
// RefusalNone is the zero value and means "this host is serving the volume", which is what
// makes the field self-clearing: every accepted report writes it.
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
	// RefusalPublishFenced: the compare-and-set on this volume's HEAD lost, so another
	// host published a commit for it and this one is not its writer (commit.ErrHeadMoved).
	//
	// Its own value rather than the catch-all above, because it is the only refusal here
	// that is answered by looking at *ownership* instead of at the host that reported it:
	// who else believes they own this volume, and why was this host not fenced first.
	RefusalPublishFenced Refusal = "PUBLISH_FENCED"
)

var refusals = []Refusal{
	RefusalNone, RefusalImageMissing, RefusalDurabilityLost, RefusalNoReadView,
	RefusalNoKey, RefusalLeaseLost, RefusalAttachFailed, RefusalPublishFenced,
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
		// DELETING, which is the catalog recording that the snapshot was asked to go
		// (§21.3). Nothing in this tree removes its objects afterwards.
		SnapshotPublished: {SnapshotDeleting},
		SnapshotFailed:    {SnapshotDeleting},
		SnapshotDeleting:  {},
	})

// SnapshotStates returns every snapshot state.
func SnapshotStates() []SnapshotState { return snapshotMachine.all }

// Unfinished reports whether something is still owed on a snapshot in this state. CREATING
// is owed by the Agent; DELETING is owed by a reclaim ADR-0026 deleted, so a snapshot that
// reaches it stays there. Deliberately not `len(successors) == 0`: that would call a stuck
// DELETING snapshot finished and a PUBLISHED one outstanding, exactly backwards.
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

// No OperationKind/OperationPhase: §7's operations table went with ADR-0026, and the work
// V1 still does (a snapshot, a clone) is one Control-Plane call each, waiting on the
// snapshot's own §19 state.
//
// No durability mode: ADR-0026 withdrew the remote ACK, so the local ACK is the only
// contract and nothing can select anything. Reintroducing a choice means reintroducing the
// mechanism that honours it, which is ADR-0026's decision to reopen.
