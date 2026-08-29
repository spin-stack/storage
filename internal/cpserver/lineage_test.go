package cpserver_test

import (
	"strings"
	"testing"

	"connectrpc.com/connect"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
)

// lineage writes a root and two clones over it, the way controlplane.Clone does: each
// child names the snapshot it was taken from, and each snapshot names the volume it was
// taken of and the commit it froze. `rootCommit` empty leaves the oldest snapshot without
// one, which is a snapshot the host that owns it has not finished taking.
func lineage(t *testing.T, f *fixture, rootCommit string) {
	t.Helper()
	f.createVolume(t, metadata.Volume{VolumeID: "vol-root", SizeBytes: 1 << 30, BlockSize: 4096,
		CurrentEpoch: 1, PrimaryHostID: hostA})
	snap := metadata.Snapshot{SnapshotID: "snap-root", VolumeID: "vol-root", Epoch: 1,
		CommitID: rootCommit, State: lifecycle.SnapshotPublished, RequestID: "req-1"}
	if rootCommit == "" {
		snap.State = lifecycle.SnapshotCreating
	}
	if err := f.md.CreateSnapshot(t.Context(), f.term, snap); err != nil {
		t.Fatal(err)
	}
	f.createVolume(t, metadata.Volume{VolumeID: "vol-middle", SizeBytes: 1 << 30, BlockSize: 4096,
		CurrentEpoch: 1, PrimaryHostID: hostA, ChainDepth: 1, ParentSnapshotID: "snap-root"})
	if err := f.md.CreateSnapshot(t.Context(), f.term, metadata.Snapshot{
		SnapshotID: "snap-middle", VolumeID: "vol-middle", ParentSnapshotID: "snap-root", Epoch: 1,
		CommitID: "commit-middle", State: lifecycle.SnapshotPublished, RequestID: "req-2",
	}); err != nil {
		t.Fatal(err)
	}
	f.createVolume(t, metadata.Volume{VolumeID: "vol-leaf", SizeBytes: 1 << 30, BlockSize: 4096,
		CurrentEpoch: 1, PrimaryHostID: hostA, ChainDepth: 2, ParentSnapshotID: "snap-middle"})
}

// TestTheDesiredStateSpellsOutTheWholeAncestry.
//
// The Agent is told and cannot look a lineage up (ADR-0021), so everything a rebuild needs
// about the generations under a volume has to be on this wire. One generation was all it
// carried, and a grandchild built from it comes back missing everything its oldest
// ancestor wrote, reporting success.
//
// The order is asserted, not just the membership: a chain is rebuilt oldest first, each
// layer repointed at the one below it, so the same two pairs the other way round build a
// chain whose bytes are in the wrong order at every offset the two generations both wrote.
func TestTheDesiredStateSpellsOutTheWholeAncestry(t *testing.T) {
	f := newFixture(t)
	lineage(t, f, "commit-root")

	resp, err := f.srv.GetDesiredState(t.Context(), connect.NewRequest(&storagev1.GetDesiredStateRequest{HostId: hostA}))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string][][2]string{}
	for _, v := range resp.Msg.GetVolumes() {
		for _, a := range v.GetAncestry() {
			got[v.GetVolumeId()] = append(got[v.GetVolumeId()], [2]string{a.GetVolumeId(), a.GetCommitId()})
		}
	}
	want := map[string][][2]string{
		"vol-middle": {{"vol-root", "commit-root"}},
		"vol-leaf":   {{"vol-root", "commit-root"}, {"vol-middle", "commit-middle"}},
	}
	for id, w := range want {
		if len(got[id]) != len(w) {
			t.Fatalf("%s was sent %d generations of ancestry, want %d: %v", id, len(got[id]), len(w), got[id])
		}
		for i := range w {
			if got[id][i] != w[i] {
				t.Errorf("%s's ancestry at %d is %v, want %v (oldest first)", id, i, got[id][i], w[i])
			}
		}
	}
	// And a volume nobody cloned is sent none, which is what tells the Agent to start it
	// empty rather than to look for objects under somebody else's id.
	if n := len(got["vol-root"]); n != 0 {
		t.Errorf("a root volume was sent %d generations of ancestry", n)
	}
}

// TestAnUnfinishedAncestorRefusesTheWholeDesiredState.
//
// A snapshot in CREATING names no commit, so there is no point in that ancestor's history
// to rebuild to. Refused, and refused for an ancestor two generations down rather than
// only for the nearest one: served without it, the clone's chain is missing that
// generation's bytes and reads them as zeros — indistinguishable from a volume nobody
// wrote to, which is the failure the whole lineage link exists to prevent.
func TestAnUnfinishedAncestorRefusesTheWholeDesiredState(t *testing.T) {
	f := newFixture(t)
	lineage(t, f, "")

	_, err := f.srv.GetDesiredState(t.Context(), connect.NewRequest(&storagev1.GetDesiredStateRequest{HostId: hostA}))
	if err == nil {
		t.Fatal("a host was handed desired state naming an ancestor with no commit to rebuild to")
	}
	if !strings.Contains(err.Error(), "snap-root") || !strings.Contains(err.Error(), "names no commit") {
		t.Errorf("the refusal does not say which ancestor is unfinished: %v", err)
	}
}
