package qcow

import (
	"reflect"
	"testing"
)

// These are in the package because what they test is one call, case by case, where a case
// is a struct literal. What the derivation does over a run — rotations, a crash, a
// publisher — is asserted through the Manager by this package's adversary tests, and over
// simulated I/O by internal/dst's no-sealed-layer-is-chained-past.
func TestSealedLayersAreDerivedFromTheTipAndTheCommits(t *testing.T) {
	t.Parallel()
	commit := func(layers ...string) []CommitLayer {
		out := make([]CommitLayer, 0, len(layers))
		for _, l := range layers {
			out = append(out, CommitLayer{CommitID: "c-" + l, LayerID: l})
		}
		return out
	}
	tests := []struct {
		name  string
		state State
		tip   string
		want  []string
	}{
		{
			name:  "a volume that has only ever had one layer owes nothing",
			state: State{Layers: []string{"a"}},
			tip:   "a",
		},
		{
			name: "the layer under the tip is sealed, whether or not anything recorded it",
			// The crash this closes: the rotation switched QEMU to `b` and died before
			// state.json landed, so nothing says `a` was sealed. It is derivable anyway.
			state: State{Layers: []string{"b", "a"}},
			tip:   "b",
			want:  []string{"a"},
		},
		{
			name:  "a published layer is not owed",
			state: State{Layers: []string{"b", "a"}, Commits: commit("a")},
			tip:   "b",
		},
		{
			name: "the walk stops at the newest published layer",
			// Everything under a published commit is published with it, so `x` is not a
			// question even though it is in the list.
			state: State{Layers: []string{"d", "c", "b", "a", "x"}, Commits: commit("a")},
			tip:   "d",
			want:  []string{"b", "c"},
		},
		{
			name: "a layer above the tip is not sealed",
			// Rotate moves the pointer before it tells QEMU to switch. QEMU still has
			// `a` open, so `b` is the file the guest is *not* writing to yet and `a` is
			// the file it is: neither is publishable, and returning `a` would publish a
			// file a guest is writing into.
			state: State{Layers: []string{"b", "a"}, Commits: nil},
			tip:   "a",
		},
		{
			name: "a tip the list has never heard of owes nothing",
			// A restored chain: the new tip is not in the list until it is observed, and
			// the layers under it are the object store's, not this host's to publish.
			state: State{Layers: []string{"old"}, Commits: commit("old")},
			tip:   "fresh",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.state.SealedBelow(tt.tip)
			if len(got) == 0 && len(tt.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SealedBelow(%q) = %v, want %v", tt.tip, got, tt.want)
			}
		})
	}
}

// TestTheTipIsRecordedOnceAndTheListIsBounded. The record is written by the cycle that
// observes the tip, so it is asked on every cycle and must answer "nothing to write" for
// all but the one after a rotation: an fsync of a file and its directory per volume per
// heartbeat, for a line already there, is real I/O bought for nothing.
func TestTheTipIsRecordedOnceAndTheListIsBounded(t *testing.T) {
	t.Parallel()
	var st State
	if !st.ObserveTip("a") {
		t.Fatal("the first tip was not news")
	}
	if st.ObserveTip("a") {
		t.Error("the same tip was written down a second time")
	}
	st.ObserveTip("b")
	if want := []string{"b", "a"}; !reflect.DeepEqual(st.Layers, want) {
		t.Fatalf("the list is %v, want %v — newest first", st.Layers, want)
	}
	// Published `a`, so nothing older than it can ever be a question again.
	st.Layers = []string{"c", "b", "a", "z"}
	st.trimLayers("a")
	if want := []string{"c", "b", "a"}; !reflect.DeepEqual(st.Layers, want) {
		t.Errorf("after trimming at the published layer the list is %v, want %v", st.Layers, want)
	}
}
