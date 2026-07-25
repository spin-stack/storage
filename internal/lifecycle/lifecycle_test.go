package lifecycle_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/lifecycle"
)

// TestHostStateTransitions is §28.1: a host is cordoned before it is drained, a dead
// host can still be evacuated, and a repaired host can come back.
func TestHostStateTransitions(t *testing.T) {
	tests := []struct {
		name    string
		from    lifecycle.HostState
		to      lifecycle.HostState
		allowed bool
	}{
		{"active can be cordoned", lifecycle.HostActive, lifecycle.HostCordoned, true},
		{"active can go straight to draining", lifecycle.HostActive, lifecycle.HostDraining, true},
		{"cordoned can be drained", lifecycle.HostCordoned, lifecycle.HostDraining, true},
		{"cordoned can be uncordoned", lifecycle.HostCordoned, lifecycle.HostActive, true},
		{"a dead host can still be evacuated", lifecycle.HostDead, lifecycle.HostDraining, true},
		{"a repaired host returns to active", lifecycle.HostDead, lifecycle.HostActive, true},
		{"draining is idempotent", lifecycle.HostDraining, lifecycle.HostDraining, true},
		{"unknown source state", lifecycle.HostState("NOPE"), lifecycle.HostActive, false},
		{"unknown target state", lifecycle.HostActive, lifecycle.HostState("PUBLISHED"), false},
		{"the zero value is not a state", lifecycle.HostState(""), lifecycle.HostActive, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.from.CanTransitionTo(tc.to); got != tc.allowed {
				t.Fatalf("%s -> %s: allowed=%v, want %v", tc.from, tc.to, got, tc.allowed)
			}
			err := tc.from.Transition(tc.to)
			if tc.allowed && err != nil {
				t.Fatalf("Transition returned %v", err)
			}
			if !tc.allowed && !errors.Is(err, lifecycle.ErrInvalidTransition) {
				t.Fatalf("want ErrInvalidTransition, got %v", err)
			}
		})
	}
}

// TestOnlyActiveHostsAcceptPlacement is the §28.1/§28.2 rule placement depends on.
func TestOnlyActiveHostsAcceptPlacement(t *testing.T) {
	for _, s := range lifecycle.HostStates() {
		want := s == lifecycle.HostActive
		if got := s.AcceptsPlacement(); got != want {
			t.Fatalf("%s.AcceptsPlacement() = %v, want %v", s, got, want)
		}
	}
	if lifecycle.HostState("").AcceptsPlacement() {
		t.Fatal("the zero value must never accept placement")
	}
}

// TestVolumePromotionAlwaysPassesFencingWait is §7: "no se promueve un nuevo writer
// solo por un heartbeat vencido: siempre se transita por FENCING_WAIT".
func TestVolumePromotionAlwaysPassesFencingWait(t *testing.T) {
	tests := []struct {
		name    string
		from    lifecycle.VolumeState
		to      lifecycle.VolumeState
		allowed bool
	}{
		{"suspect after active", lifecycle.VolumeActive, lifecycle.VolumePrimarySuspected, true},
		{"suspicion clears", lifecycle.VolumePrimarySuspected, lifecycle.VolumeActive, true},
		{"suspect enters the wait", lifecycle.VolumePrimarySuspected, lifecycle.VolumeFencingWait, true},
		{"the wait leads to recovery", lifecycle.VolumeFencingWait, lifecycle.VolumeRecoveryRequired, true},
		{"recovery runs and completes", lifecycle.VolumeRecovering, lifecycle.VolumeActive, true},
		{"failed recovery goes back to required", lifecycle.VolumeRecovering, lifecycle.VolumeRecoveryRequired, true},

		{"suspicion cannot skip the fencing wait", lifecycle.VolumePrimarySuspected, lifecycle.VolumeRecoveryRequired, false},
		{"the fencing wait is not a shortcut to active", lifecycle.VolumeFencingWait, lifecycle.VolumeActive, false},
		{"a detached volume is not recovered in place", lifecycle.VolumeDetached, lifecycle.VolumeRecovering, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.from.CanTransitionTo(tc.to); got != tc.allowed {
				t.Fatalf("%s -> %s: allowed=%v, want %v", tc.from, tc.to, got, tc.allowed)
			}
		})
	}
}

// TestAgentVolumeStateMachine is §16, the Agent's per-volume machine. It is defined
// here so Phases 02/03 implement the doc's machine rather than reinventing one.
func TestAgentVolumeStateMachine(t *testing.T) {
	tests := []struct {
		name    string
		from    lifecycle.AgentVolumeState
		to      lifecycle.AgentVolumeState
		allowed bool
	}{
		{"attach", lifecycle.AgentDetached, lifecycle.AgentAttaching, true},
		{"attached", lifecycle.AgentAttaching, lifecycle.AgentActive, true},
		{"snapshot in background", lifecycle.AgentActive, lifecycle.AgentSnapshotting, true},
		{"snapshot done", lifecycle.AgentSnapshotting, lifecycle.AgentActive, true},
		{"lease expired on the monotonic clock", lifecycle.AgentActive, lifecycle.AgentSelfFenced, true},
		{"fenced by an epoch notification", lifecycle.AgentActive, lifecycle.AgentFenced, true},
		{"fenced waits for instructions", lifecycle.AgentFenced, lifecycle.AgentRecoveryRequired, true},
		{"recovery completes into the new epoch", lifecycle.AgentRecovering, lifecycle.AgentActive, true},
		{"recovery can fail", lifecycle.AgentRecovering, lifecycle.AgentFailed, true},

		{"a fenced volume never resumes serving", lifecycle.AgentSelfFenced, lifecycle.AgentActive, false},
		{"attaching does not snapshot", lifecycle.AgentAttaching, lifecycle.AgentSnapshotting, false},
		{"a failed volume needs recovery, not a jump to active", lifecycle.AgentFailed, lifecycle.AgentActive, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.from.CanTransitionTo(tc.to); got != tc.allowed {
				t.Fatalf("%s -> %s: allowed=%v, want %v", tc.from, tc.to, got, tc.allowed)
			}
		})
	}
}

// TestPublishedSnapshotNeverGoesBack is INV-16 expressed in the vocabulary: once
// PUBLISHED, the only way out is deletion (§19).
func TestPublishedSnapshotNeverGoesBack(t *testing.T) {
	tests := []struct {
		name    string
		from    lifecycle.SnapshotState
		to      lifecycle.SnapshotState
		allowed bool
	}{
		{"creating publishes", lifecycle.SnapshotCreating, lifecycle.SnapshotPublished, true},
		{"creating can fail", lifecycle.SnapshotCreating, lifecycle.SnapshotFailed, true},
		{"published can be deleted", lifecycle.SnapshotPublished, lifecycle.SnapshotDeleting, true},
		{"failed can be deleted", lifecycle.SnapshotFailed, lifecycle.SnapshotDeleting, true},

		{"published never returns to creating", lifecycle.SnapshotPublished, lifecycle.SnapshotCreating, false},
		{"published never becomes failed", lifecycle.SnapshotPublished, lifecycle.SnapshotFailed, false},
		{"deleting is terminal", lifecycle.SnapshotDeleting, lifecycle.SnapshotPublished, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.from.CanTransitionTo(tc.to); got != tc.allowed {
				t.Fatalf("%s -> %s: allowed=%v, want %v", tc.from, tc.to, got, tc.allowed)
			}
		})
	}
}

// TestOperationPhaseLifecycle is the reconciliation lifecycle every long-running
// operation shares (§7): a pass may fail and be retried, cancellation is a request
// that resolves, and a finished operation never resurrects.
func TestOperationPhaseLifecycle(t *testing.T) {
	tests := []struct {
		name    string
		from    lifecycle.OperationPhase
		to      lifecycle.OperationPhase
		allowed bool
	}{
		{"start", lifecycle.OpPending, lifecycle.OpRunning, true},
		{"a failed pass is retried by the reconciler", lifecycle.OpFailed, lifecycle.OpRunning, true},
		{"running can fail", lifecycle.OpRunning, lifecycle.OpFailed, true},
		{"running completes", lifecycle.OpRunning, lifecycle.OpSucceeded, true},
		{"cancellation is requested while running", lifecycle.OpRunning, lifecycle.OpCanceling, true},
		{"cancellation is requested before it starts", lifecycle.OpPending, lifecycle.OpCanceling, true},
		{"cancellation resolves", lifecycle.OpCanceling, lifecycle.OpCanceled, true},
		{"a canceling pass may still finish the volume it was on", lifecycle.OpCanceling, lifecycle.OpSucceeded, true},

		{"succeeded is terminal", lifecycle.OpSucceeded, lifecycle.OpRunning, false},
		{"canceled is terminal", lifecycle.OpCanceled, lifecycle.OpRunning, false},
		{"no jumping from pending to done", lifecycle.OpPending, lifecycle.OpSucceeded, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.from.CanTransitionTo(tc.to); got != tc.allowed {
				t.Fatalf("%s -> %s: allowed=%v, want %v", tc.from, tc.to, got, tc.allowed)
			}
		})
	}
	// Terminal phases report themselves as such, so a reconciler can stop cheaply.
	for _, p := range lifecycle.OperationPhases() {
		want := p == lifecycle.OpSucceeded || p == lifecycle.OpCanceled
		if got := p.Terminal(); got != want {
			t.Fatalf("%s.Terminal() = %v, want %v", p, got, want)
		}
	}
}

// TestPredecessorsDriveTheStoreGuard: the transition table is also what the SQL
// predicate is built from, so it must list the legal previous states (including the
// target itself, for an idempotent re-write).
func TestPredecessorsDriveTheStoreGuard(t *testing.T) {
	got := lifecycle.HostDraining.PredecessorNames()
	want := map[string]bool{"ACTIVE": true, "CORDONED": true, "DRAINING": true, "DEAD": true}
	if len(got) != len(want) {
		t.Fatalf("predecessors of DRAINING = %v, want %v", got, want)
	}
	for _, s := range got {
		if !want[s] {
			t.Fatalf("unexpected predecessor %q of DRAINING", s)
		}
	}
	// A state nothing may become would make the guard match no row; assert the one
	// case that matters: ACTIVE can be reached, but not from FENCING_WAIT (§7).
	for _, s := range lifecycle.VolumeActive.Predecessors() {
		if s == lifecycle.VolumeFencingWait {
			t.Fatal("FENCING_WAIT must not be a predecessor of ACTIVE (§7)")
		}
	}
}

// TestTransitionReportsTheVocabularyItRefused: the error names which machine and
// which move was refused, so a log line is actionable without a debugger.
func TestTransitionReportsTheVocabularyItRefused(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantSub string
	}{
		{"volume", lifecycle.VolumeFencingWait.Transition(lifecycle.VolumeActive), `volume state "FENCING_WAIT" -> "ACTIVE"`},
		{"agent volume", lifecycle.AgentSelfFenced.Transition(lifecycle.AgentActive), `agent volume state "SELF_FENCED" -> "ACTIVE"`},
		{"snapshot", lifecycle.SnapshotPublished.Transition(lifecycle.SnapshotCreating), `snapshot state "PUBLISHED" -> "CREATING"`},
		{"operation phase", lifecycle.OpCanceled.Transition(lifecycle.OpRunning), `operation phase "CANCELED" -> "RUNNING"`},
		{"host", lifecycle.HostActive.Transition(lifecycle.HostState("GONE")), `host state "ACTIVE" -> "GONE"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.err, lifecycle.ErrInvalidTransition) {
				t.Fatalf("want ErrInvalidTransition, got %v", tc.err)
			}
			if !strings.Contains(tc.err.Error(), tc.wantSub) {
				t.Fatalf("error %q does not name the refused move %q", tc.err, tc.wantSub)
			}
		})
	}
	// The legal moves return nil.
	for _, err := range []error{
		lifecycle.VolumeRecovering.Transition(lifecycle.VolumeActive),
		lifecycle.AgentAttaching.Transition(lifecycle.AgentActive),
		lifecycle.SnapshotCreating.Transition(lifecycle.SnapshotPublished),
		lifecycle.OpRunning.Transition(lifecycle.OpSucceeded),
	} {
		if err != nil {
			t.Fatalf("a legal transition returned %v", err)
		}
	}
}

// TestServingStates is §12.2: only a volume the Agent is actually serving may ACK
// guest I/O — a fenced or attaching one may not.
func TestServingStates(t *testing.T) {
	serving := map[lifecycle.AgentVolumeState]bool{
		lifecycle.AgentActive:       true,
		lifecycle.AgentSnapshotting: true, // §19: the snapshot does not quiesce the guest
	}
	for _, s := range lifecycle.AgentVolumeStates() {
		if got := s.Serving(); got != serving[s] {
			t.Fatalf("%s.Serving() = %v, want %v", s, got, serving[s])
		}
	}
}

// TestPredecessorsOfAnUnknownStateIsEmpty: the SQL guard must not degrade into
// "match anything" when handed a value outside the vocabulary.
func TestPredecessorsOfAnUnknownStateIsEmpty(t *testing.T) {
	if got := lifecycle.HostState("NOPE").PredecessorNames(); len(got) != 0 {
		t.Fatalf("predecessors of an unknown state = %v, want none", got)
	}
	if got := lifecycle.OperationPhase("STARTED").PredecessorNames(); len(got) != 0 {
		t.Fatalf("predecessors of an unknown phase = %v, want none", got)
	}
	// A legal target lists itself plus its predecessors, as strings for the guard.
	if got := lifecycle.OpRunning.PredecessorNames(); len(got) != 3 {
		t.Fatalf("predecessors of RUNNING = %v, want PENDING/RUNNING/FAILED", got)
	}
	if got := lifecycle.VolumeActive.PredecessorNames(); len(got) == 0 {
		t.Fatal("ACTIVE must list its predecessors")
	}
}

// TestParseRejectsAnythingElse: values arriving from outside Go (a DB row, a JSON
// descriptor, a CLI flag) are parsed, so a bad value fails at the boundary.
func TestParseRejectsAnythingElse(t *testing.T) {
	tests := []struct {
		name  string
		parse func(string) error
	}{
		{"HostState", func(s string) error { _, err := lifecycle.ParseHostState(s); return err }},
		{"VolumeState", func(s string) error { _, err := lifecycle.ParseVolumeState(s); return err }},
		{"AgentVolumeState", func(s string) error { _, err := lifecycle.ParseAgentVolumeState(s); return err }},
		{"SnapshotState", func(s string) error { _, err := lifecycle.ParseSnapshotState(s); return err }},
		{"OperationPhase", func(s string) error { _, err := lifecycle.ParseOperationPhase(s); return err }},
		{"OperationKind", func(s string) error { _, err := lifecycle.ParseOperationKind(s); return err }},
		{"Durability", func(s string) error { _, err := lifecycle.ParseDurability(s); return err }},
	}
	bad := []string{"", " ", "active", "ACTIVE ", "nonsense", "0"}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, raw := range bad {
				if err := tc.parse(raw); !errors.Is(err, lifecycle.ErrUnknownState) {
					t.Fatalf("parse(%q): want ErrUnknownState, got %v", raw, err)
				}
			}
		})
	}
}

// TestParseRoundTripsEveryValue: every declared value parses back to itself, so the
// constant list, the transition table, and the DB CHECKs cannot drift apart.
func TestParseRoundTripsEveryValue(t *testing.T) {
	for _, s := range lifecycle.HostStates() {
		if got, err := lifecycle.ParseHostState(s.String()); err != nil || got != s {
			t.Fatalf("HostState %q: got %q err=%v", s, got, err)
		}
	}
	for _, s := range lifecycle.VolumeStates() {
		if got, err := lifecycle.ParseVolumeState(s.String()); err != nil || got != s {
			t.Fatalf("VolumeState %q: got %q err=%v", s, got, err)
		}
	}
	for _, s := range lifecycle.AgentVolumeStates() {
		if got, err := lifecycle.ParseAgentVolumeState(s.String()); err != nil || got != s {
			t.Fatalf("AgentVolumeState %q: got %q err=%v", s, got, err)
		}
	}
	for _, s := range lifecycle.SnapshotStates() {
		if got, err := lifecycle.ParseSnapshotState(s.String()); err != nil || got != s {
			t.Fatalf("SnapshotState %q: got %q err=%v", s, got, err)
		}
	}
	for _, p := range lifecycle.OperationPhases() {
		if got, err := lifecycle.ParseOperationPhase(p.String()); err != nil || got != p {
			t.Fatalf("OperationPhase %q: got %q err=%v", p, got, err)
		}
	}
	for _, k := range lifecycle.OperationKinds() {
		if got, err := lifecycle.ParseOperationKind(k.String()); err != nil || got != k {
			t.Fatalf("OperationKind %q: got %q err=%v", k, got, err)
		}
	}
	for _, d := range lifecycle.Durabilities() {
		if got, err := lifecycle.ParseDurability(d.String()); err != nil || got != d {
			t.Fatalf("Durability %q: got %q err=%v", d, got, err)
		}
	}
}

// TestValidRejectsTheZeroValue: a struct field nobody set must not look like a state.
func TestValidRejectsTheZeroValue(t *testing.T) {
	var (
		h  lifecycle.HostState
		v  lifecycle.VolumeState
		a  lifecycle.AgentVolumeState
		s  lifecycle.SnapshotState
		p  lifecycle.OperationPhase
		k  lifecycle.OperationKind
		d  lifecycle.Durability
		ok = []bool{h.Valid(), v.Valid(), a.Valid(), s.Valid(), p.Valid(), k.Valid(), d.Valid()}
	)
	for i, valid := range ok {
		if valid {
			t.Fatalf("zero value %d reported itself valid", i)
		}
	}
}

// TestDurabilityIsRemoteByDefaultOnlyWhenAsked documents the §14.8 pair; the mapping
// to the data path's mode is asserted in the wal package.
func TestDurability(t *testing.T) {
	if !lifecycle.DurabilityRemote.Remote() || lifecycle.DurabilityLocal.Remote() {
		t.Fatal("Remote() must distinguish the two §14.8 modes")
	}
	if len(lifecycle.Durabilities()) != 2 {
		t.Fatalf("§14.8 defines exactly two modes, got %v", lifecycle.Durabilities())
	}
}
