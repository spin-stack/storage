// Package lifecycle holds the typed vocabularies for everything in the system that
// has a lifecycle: host fleet states (§28.1), volume ownership states (§7), the
// Agent's per-volume machine (§16), snapshot states (§19), reconciliation operation
// kinds and phases (§7, §8), and the per-volume durability mode (§14.8).
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

// --- Volume state, Agent side (§16) ---

// AgentVolumeState is the Volume Agent's per-volume machine (§16). It is a different
// vocabulary from VolumeState on purpose: the Agent knows states the Control Plane
// never sees (ATTACHING, SNAPSHOTTING, SELF_FENCED) and vice versa.
type AgentVolumeState string

// Agent per-volume states (§16).
const (
	AgentDetached         AgentVolumeState = "DETACHED"
	AgentAttaching        AgentVolumeState = "ATTACHING"
	AgentActive           AgentVolumeState = "ACTIVE"
	AgentSnapshotting     AgentVolumeState = "SNAPSHOTTING"
	AgentSelfFenced       AgentVolumeState = "SELF_FENCED"
	AgentFenced           AgentVolumeState = "FENCED"
	AgentRecoveryRequired AgentVolumeState = "RECOVERY_REQUIRED"
	AgentRecovering       AgentVolumeState = "RECOVERING"
	AgentFailed           AgentVolumeState = "FAILED"
)

var agentMachine = newMachine("agent volume state",
	[]AgentVolumeState{AgentDetached, AgentAttaching, AgentActive, AgentSnapshotting,
		AgentSelfFenced, AgentFenced, AgentRecoveryRequired, AgentRecovering, AgentFailed},
	map[AgentVolumeState][]AgentVolumeState{
		AgentDetached:     {AgentAttaching},
		AgentAttaching:    {AgentActive, AgentFailed, AgentDetached},
		AgentActive:       {AgentSnapshotting, AgentSelfFenced, AgentFenced, AgentDetached},
		AgentSnapshotting: {AgentActive, AgentFailed},
		// A fenced volume waits for instructions; it never resumes serving on its own
		// (§16, §12.2) — the way back is a full recovery into a new epoch.
		AgentSelfFenced:       {AgentRecoveryRequired, AgentDetached},
		AgentFenced:           {AgentRecoveryRequired, AgentDetached},
		AgentRecoveryRequired: {AgentRecovering, AgentDetached},
		AgentRecovering:       {AgentActive, AgentRecoveryRequired, AgentFailed},
		AgentFailed:           {AgentRecoveryRequired, AgentDetached},
	})

// AgentVolumeStates returns every Agent-side volume state.
func AgentVolumeStates() []AgentVolumeState { return agentMachine.all }

// ParseAgentVolumeState converts a stored value, rejecting anything else.
func ParseAgentVolumeState(raw string) (AgentVolumeState, error) { return agentMachine.parse(raw) }

func (s AgentVolumeState) String() string { return string(s) }

// Valid reports whether s is a declared Agent volume state.
func (s AgentVolumeState) Valid() bool { return agentMachine.valid(s) }

// Serving reports whether the guest data path may be ACKed in this state (§12.2).
func (s AgentVolumeState) Serving() bool { return s == AgentActive || s == AgentSnapshotting }

// CanTransitionTo reports whether the §16 machine allows this move.
func (s AgentVolumeState) CanTransitionTo(to AgentVolumeState) bool {
	return agentMachine.allows(s, to)
}

// Transition returns ErrInvalidTransition unless the move is allowed.
func (s AgentVolumeState) Transition(to AgentVolumeState) error {
	return agentMachine.transition(s, to)
}

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

// --- Reconciliation operations (§7, §8) ---

// OperationKind is what a reconciled operation does (§8 `operations.kind`).
type OperationKind string

// Operation kinds (§8).
const (
	OpAttach   OperationKind = "attach"
	OpDetach   OperationKind = "detach"
	OpClone    OperationKind = "clone"
	OpResize   OperationKind = "resize"
	OpDrain    OperationKind = "drain"
	OpRecovery OperationKind = "recovery"
	OpFlatten  OperationKind = "flatten"
	OpGC       OperationKind = "gc"
)

var kindMachine = newMachine("operation kind",
	[]OperationKind{OpAttach, OpDetach, OpClone, OpResize, OpDrain, OpRecovery, OpFlatten, OpGC},
	nil) // a kind never changes: an operation is what it was created as.

// OperationKinds returns every operation kind.
func OperationKinds() []OperationKind { return kindMachine.all }

// ParseOperationKind converts a stored value, rejecting anything else.
func ParseOperationKind(raw string) (OperationKind, error) { return kindMachine.parse(raw) }

func (k OperationKind) String() string { return string(k) }

// Valid reports whether k is a declared operation kind.
func (k OperationKind) Valid() bool { return kindMachine.valid(k) }

// OperationPhase is where a long-running reconciled operation stands (§7). It is
// deliberately generic: what a drain is *doing* belongs in current_state, not in a
// bespoke phase word per operation kind.
type OperationPhase string

// Operation phases (§7 reconciliation).
const (
	OpPending   OperationPhase = "PENDING"
	OpRunning   OperationPhase = "RUNNING"
	OpCanceling OperationPhase = "CANCELING"
	OpCanceled  OperationPhase = "CANCELED"
	OpSucceeded OperationPhase = "SUCCEEDED"
	OpFailed    OperationPhase = "FAILED"
)

var phaseMachine = newMachine("operation phase",
	[]OperationPhase{OpPending, OpRunning, OpCanceling, OpCanceled, OpSucceeded, OpFailed},
	map[OperationPhase][]OperationPhase{
		OpPending: {OpRunning, OpCanceling, OpFailed},
		OpRunning: {OpSucceeded, OpFailed, OpCanceling},
		// A failed pass is a retryable state, not an outcome: the reconciler runs the
		// operation again (§7).
		OpFailed: {OpRunning, OpCanceling},
		// A cancellation is honored at the next safe boundary, so the pass in flight
		// may still complete successfully.
		OpCanceling: {OpCanceled, OpSucceeded, OpFailed},
		OpCanceled:  {},
		OpSucceeded: {},
	})

// OperationPhases returns every operation phase.
func OperationPhases() []OperationPhase { return phaseMachine.all }

// ParseOperationPhase converts a stored value, rejecting anything else.
func ParseOperationPhase(raw string) (OperationPhase, error) { return phaseMachine.parse(raw) }

func (p OperationPhase) String() string { return string(p) }

// Valid reports whether p is a declared operation phase.
func (p OperationPhase) Valid() bool { return phaseMachine.valid(p) }

// Terminal reports whether the operation is finished for good.
func (p OperationPhase) Terminal() bool { return p == OpSucceeded || p == OpCanceled }

// CanTransitionTo reports whether the reconciliation lifecycle allows this move.
func (p OperationPhase) CanTransitionTo(to OperationPhase) bool { return phaseMachine.allows(p, to) }

// Transition returns ErrInvalidTransition unless the move is allowed.
func (p OperationPhase) Transition(to OperationPhase) error { return phaseMachine.transition(p, to) }

// Predecessors returns the phases that may become p (including p).
func (p OperationPhase) Predecessors() []OperationPhase { return phaseMachine.predecessors(p) }

// PredecessorNames is Predecessors as stored strings — the store's SQL guard.
func (p OperationPhase) PredecessorNames() []string { return names(p.Predecessors()) }

// --- Durability mode (§14.8) ---

// Durability is the per-volume FLUSH/FUA ACK contract as the Control Plane stores it
// (§14.8). The data path's counterpart is wal.DurabilityMode; wal owns the single
// mapping between the two so they cannot drift.
type Durability string

// Durability modes (§14.8).
const (
	DurabilityRemote Durability = "remote"
	DurabilityLocal  Durability = "local"
)

var durabilityMachine = newMachine("durability",
	[]Durability{DurabilityRemote, DurabilityLocal}, nil)

// Durabilities returns every durability mode.
func Durabilities() []Durability { return durabilityMachine.all }

// ParseDurability converts a stored value, rejecting anything else.
func ParseDurability(raw string) (Durability, error) { return durabilityMachine.parse(raw) }

func (d Durability) String() string { return string(d) }

// Valid reports whether d is a declared durability mode.
func (d Durability) Valid() bool { return durabilityMachine.valid(d) }

// Remote reports whether FLUSH/FUA ACKs require verified S3 durability (§14.8).
func (d Durability) Remote() bool { return d == DurabilityRemote }
