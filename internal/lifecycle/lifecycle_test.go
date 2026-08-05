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
	// A legal target lists itself plus its predecessors, as strings for the guard.
	if got := lifecycle.SnapshotPublished.PredecessorNames(); len(got) != 2 {
		t.Fatalf("predecessors of PUBLISHED = %v, want CREATING/PUBLISHED", got)
	}
	if got := lifecycle.VolumeActive.PredecessorNames(); len(got) == 0 {
		t.Fatal("ACTIVE must list its predecessors")
	}
}

// TestCordonAuthorityIsAsymmetric is ADR-0013 §5 as a table: a human outranks the
// pressure loop and the pressure loop does not outrank a human. It is stated once
// here because both stores read it — the sim as a Go check, Postgres as the
// OverwritableNames predicate of the UPDATE — and a symmetric table would let the
// 70%-used rule return a host to service that an operator took out of it.
func TestCordonAuthorityIsAsymmetric(t *testing.T) {
	tests := []struct {
		name    string
		writer  lifecycle.CordonReason
		current lifecycle.CordonReason
		want    bool
	}{
		{"an operator may cordon a host nobody cordoned", lifecycle.CordonOperator, lifecycle.CordonNone, true},
		{"an operator may take over a pressure cordon", lifecycle.CordonOperator, lifecycle.CordonPressure, true},
		{"an operator may replace an operator's", lifecycle.CordonOperator, lifecycle.CordonOperator, true},
		{"pressure may cordon a host nobody cordoned", lifecycle.CordonPressure, lifecycle.CordonNone, true},
		{"pressure may change its own cordon", lifecycle.CordonPressure, lifecycle.CordonPressure, true},
		{"pressure may not touch an operator's", lifecycle.CordonPressure, lifecycle.CordonOperator, false},
		{"nobody writes with no authority", lifecycle.CordonNone, lifecycle.CordonNone, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.writer.MayOverwrite(tc.current); got != tc.want {
				t.Fatalf("%q.MayOverwrite(%q) = %v, want %v", tc.writer, tc.current, got, tc.want)
			}
			// The SQL predicate is generated from the same table, so it has to agree
			// with the Go answer for every pair — that is the point of deriving it
			// rather than writing the reason list into the query by hand.
			var inNames bool
			for _, n := range tc.writer.OverwritableNames() {
				if n == tc.current.String() {
					inNames = true
				}
			}
			if inNames != tc.want {
				t.Fatalf("%q.OverwritableNames() = %v, disagrees with MayOverwrite(%q)",
					tc.writer, tc.writer.OverwritableNames(), tc.current)
			}
		})
	}
}

// TestCordonNoneIsNotAnAuthority: the zero value is a state a host can be in, never
// an actor that can ask for one. A store handed it must refuse rather than write a
// cordon with no recorded author, which is the one cordon nobody can safely undo.
func TestCordonNoneIsNotAnAuthority(t *testing.T) {
	for _, r := range lifecycle.CordonReasons() {
		want := r != lifecycle.CordonNone
		if got := r.Authority(); got != want {
			t.Fatalf("%q.Authority() = %v, want %v", r, got, want)
		}
		if !r.Valid() {
			t.Fatalf("%q is returned by CordonReasons but is not Valid", r)
		}
		if got, err := lifecycle.ParseCordonReason(r.String()); err != nil || got != r {
			t.Fatalf("CordonReason %q: got %q err=%v", r, got, err)
		}
	}
	// CordonNone parses (it is what an uncordoned row stores); nonsense does not.
	for _, raw := range []string{"operator", "OPERATOR ", "PRESSURE", "nonsense"} {
		if _, err := lifecycle.ParseCordonReason(raw); !errors.Is(err, lifecycle.ErrUnknownState) {
			t.Fatalf("ParseCordonReason(%q): want ErrUnknownState, got %v", raw, err)
		}
	}
	if got := lifecycle.CordonReason("NOPE").OverwritableNames(); len(got) != 0 {
		t.Fatalf("an unknown reason must overwrite nothing, got %v", got)
	}
}

// TestUnfinishedSnapshotsAreTheOnesSomethingIsOwedOn pins the partition an operator's
// fleet-wide read is built on, and it is deliberately not "the states with no
// successor": PUBLISHED still has one (DELETING) and is finished, while DELETING has
// none and is not — under ADR-0026 nothing reclaims a snapshot, so a row that reaches
// it stays there and stays somebody's problem. Reading the machine's shape instead of
// this table would get both of those backwards.
func TestUnfinishedSnapshotsAreTheOnesSomethingIsOwedOn(t *testing.T) {
	want := map[lifecycle.SnapshotState]bool{
		lifecycle.SnapshotCreating:  true, // owed by the Agent serving the volume
		lifecycle.SnapshotDeleting:  true, // owed by a reclaim that no longer exists
		lifecycle.SnapshotPublished: false,
		lifecycle.SnapshotFailed:    false,
	}
	for _, s := range lifecycle.SnapshotStates() {
		if got := s.Unfinished(); got != want[s] {
			t.Fatalf("%q.Unfinished() = %v, want %v", s, got, want[s])
		}
	}
	// The store's SQL filter is generated from the same predicate, so it has to agree
	// with it for every value — that is the point of deriving it rather than writing
	// the state list into the query by hand.
	names := lifecycle.UnfinishedSnapshotStateNames()
	for _, s := range lifecycle.SnapshotStates() {
		var listed bool
		for _, n := range names {
			if n == s.String() {
				listed = true
			}
		}
		if listed != s.Unfinished() {
			t.Fatalf("UnfinishedSnapshotStateNames() = %v, disagrees with %q.Unfinished()", names, s)
		}
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
}

// TestValidRejectsTheZeroValue: a struct field nobody set must not look like a state.
func TestValidRejectsTheZeroValue(t *testing.T) {
	var (
		h  lifecycle.HostState
		v  lifecycle.VolumeState
		a  lifecycle.AgentVolumeState
		s  lifecycle.SnapshotState
		ok = []bool{h.Valid(), v.Valid(), a.Valid(), s.Valid()}
	)
	for i, valid := range ok {
		if valid {
			t.Fatalf("zero value %d reported itself valid", i)
		}
	}
}
