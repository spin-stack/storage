package recovery_test

import (
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/qcow"
)

// TestAGenerationNamingNoCommitIsRefusedRatherThanIndexed.
//
// The ancestry arrives as a repeated message on the wire, so a generation carrying an empty
// commit id is a shape any producer can emit — a Control Plane bug, a peer of another
// version, a row read half-written. cpserver refuses one it can see; nothing downstream may
// assume it did.
//
// The rebuild walks that generation to an empty history and then takes the last manifest of
// it. Refused, because the alternative is not a wrong chain: it is an index out of range in
// the reconcile loop, which takes the Agent down for every volume on the host and not only
// for this one. Asserted as a returned error rather than as "it did not panic": a panic
// fails the test on its own, and this pins that the caller is also told why.
func TestAGenerationNamingNoCommitIsRefusedRatherThanIndexed(t *testing.T) {
	t.Parallel()
	l := newLineage(t)
	parent := l.volume()
	commits, _ := l.publish(parent, 1)
	child := l.cloneOf(parent)
	l.newHost()

	for _, tt := range []struct {
		name     string
		ancestry []qcow.Ancestor
	}{
		{"the only generation", []qcow.Ancestor{{VolumeID: parent, CommitID: ""}}},
		{"one generation of several", []qcow.Ancestor{
			{VolumeID: parent, CommitID: ""},
			{VolumeID: parent, CommitID: commits[0]},
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := l.rec.RestoreFrom(t.Context(), qcow.Lineage{
				VolumeID: child, Ancestry: tt.ancestry,
			}, virtualSize)
			if !errors.Is(err, recovery.ErrIncomplete) {
				t.Fatalf("restoring a clone whose ancestry names no commit = %v, want ErrIncomplete", err)
			}
		})
	}
}
