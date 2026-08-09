//go:build e2e

package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/testinfra"
)

// TestACloneIsPlacedWhereItsDataIs is §20's placement rule 1 through the real binaries:
// the host that took the snapshot still holds its data on local NVMe, so the clone is
// created there and that host's Agent picks it up and serves it.
//
// Under ADR-0026 this is most of the boot-time story, because a cross-host clone pays a
// full download from the object store with no warm standby and no lazy loading to
// shorten it. The evidence is the Agent's own line: a clone that recovered its read view
// "cloned_from" the snapshot read through the parent's objects rather than starting
// blank, which is the difference DEV-0007 was about.
func TestACloneIsPlacedWhereItsDataIs(t *testing.T) {
	d := start(t)
	agent := d.startAgent(t, "volume-agent")
	d.waitForHost(t)
	d.seedVolume(t)

	volumeID, snapshotID, cloneID := aCloneOfASnapshot(t, d, agent)

	if got := volumeEpoch(t, d, cloneID); got != 1 {
		t.Fatalf("clone %s is at epoch %d, want 1 (a fresh active child, §20)", cloneID, got)
	}
	waitFor(t, 30*time.Second, "the clone's socket", func() bool {
		_, err := os.Stat(filepath.Join(d.sockDir, cloneID+".sock"))
		return err == nil
	})
	agent.WaitForLine(t, "cloned_from="+snapshotID, 30*time.Second)
	t.Logf("clone %s of snapshot %s is served on the host that took it (parent %s)", cloneID, snapshotID, volumeID)
}

// TestACloneThatStoppedOnceIsServedAgain is the session no lane had ever run, and it did
// not work: **a clone worked exactly once** (the chain-depth decision).
//
// A clone has no image of its own until it stops, so every clone this repository had ever
// read — the two DST scenarios, the test above — was reading its *first* session, where
// the parent's snapshot simply is the base. The moment a clone stops, it publishes an
// image, and the next start found both an image and a parent link and refused to serve:
// `image.Load` returns an unlayered map, `cow.SetBase` will not put a base under one, and
// every read came back ErrBaseUnavailable. The Agent then could not publish that session
// either, because a volume with no read view must not overwrite the manifest it failed to
// read.
//
// Both halves are asserted here, and they are different observables on purpose:
//
//   - the second Agent prints its recovery line **for the clone's volume id** — the line
//     fetchBase only reaches once a read view exists;
//   - and the second Agent stops with exit status 0, which is `Stop`'s own assertion.
//     That is the durability half: exit 1 is what a clone whose read view never resolved
//     produces, since its session stays in a local WAL nobody will read.
//
// What this lane cannot say is what the bytes are — no guest boots here. The byte-level
// statement (a restarted clone reads the ranges only its parent ever wrote, the ranges it
// wrote itself, and its own overwrite of a shared range) is
// `TestACloneThatStoppedOnceStartsAgainAndReadsBothHalves` in `internal/agent`, against a
// real VolumeManager, a real WAL and a real device. This one says the deployment does it.
func TestACloneThatStoppedOnceIsServedAgain(t *testing.T) {
	d := start(t)
	first := d.startAgent(t, "agent-1")
	d.waitForHost(t)
	d.seedVolume(t)

	_, _, cloneID := aCloneOfASnapshot(t, d, first)

	// The clone has to be *served* before it can be stopped, and a clone appearing in the
	// desired state is a reconcile cycle short of that. Without this wait the Agent stops
	// with nothing of the clone's to publish, and the assertion below on its manifest is
	// what catches it — which is how this was found rather than by reading the code.
	waitFor(t, 60*time.Second, "the clone's socket", func() bool {
		_, err := os.Stat(filepath.Join(d.sockDir, cloneID+".sock"))
		return err == nil
	})
	waitForLineWith(t, first, 60*time.Second,
		"read view recovered from the object store", "volume_id="+cloneID)

	// Stopping is what publishes (ADR-0026), and publishing is the whole difference
	// between the two sessions: after this the clone has an image *and* a parent.
	first.Stop(t, 60*time.Second)
	manifest := "image/" + cloneID + "/manifest.json"
	if keys := storeKeys(t, d, manifest); len(keys) != 1 {
		t.Fatalf("the clone has no image of its own after its host stopped (%v): the restart below would be the first-boot case again, which already worked", keys)
	}

	// A second incarnation, re-attaching at the same epoch (ADR-0024) with the previous
	// session's WAL still on the data directory — a restart, as a supervisor performs it.
	second := d.startAgent(t, "agent-2")
	waitForLineWith(t, second, 60*time.Second,
		"read view recovered from the object store", "volume_id="+cloneID)

	// And it can hand the session on. Stop asserts the exit status, which is the only
	// number a supervisor reads: a clone whose base never resolved leaves its session in
	// the local WAL and exits 1.
	second.Stop(t, 60*time.Second)
}

// aCloneOfASnapshot drives the whole lineage through the real binaries — snapshot the
// seeded volume, clone the snapshot, wait for the host to be given it — and returns the
// parent, the snapshot and the clone. Both tests here need it; the second needs it before
// its subject even begins.
func aCloneOfASnapshot(t *testing.T, d *deployment, agent *testinfra.Process) (volumeID, snapshotID, cloneID string) {
	t.Helper()
	volumeID = waitForServedVolume(t, d)
	agent.WaitForLine(t, "serving volume", 30*time.Second)

	// The request travels as desired state, so the desired state is where both halves of
	// it are observed: the id on its way to the host, and the field clearing once the
	// Control Plane has recorded the answer. Waiting on the *bucket* instead is not
	// enough and this lane proved it — the object lands a reconcile cycle before the
	// catalog row leaves CREATING, and a clone attempted in that window is refused.
	d.requestSnapshot(t, volumeID)
	waitFor(t, 30*time.Second, "the request to reach the host", func() bool {
		for _, v := range servedVolumes(t, d) {
			if v.GetVolumeId() == volumeID && v.GetPendingSnapshotId() != "" {
				snapshotID = v.GetPendingSnapshotId()
				return true
			}
		}
		return false
	})
	waitFor(t, 60*time.Second, "the snapshot to be recorded as published", func() bool {
		for _, v := range servedVolumes(t, d) {
			if v.GetVolumeId() == volumeID {
				return v.GetPendingSnapshotId() == ""
			}
		}
		return false
	})
	if keys := storeKeys(t, d, "image/"+volumeID+"/snapshots/"); len(keys) != 1 {
		t.Fatalf("the request was recorded as done with %d manifests in the bucket: %v", len(keys), keys)
	}

	d.cloneSnapshot(t, snapshotID)

	// The clone appears in *this* host's desired state, which is the placement decision
	// as the fleet experiences it: nothing told the Control Plane which host to use.
	waitFor(t, 30*time.Second, "the clone in this host's desired state", func() bool {
		for _, v := range servedVolumes(t, d) {
			if v.GetVolumeId() != volumeID {
				cloneID = v.GetVolumeId()
				return true
			}
		}
		return false
	})
	return volumeID, snapshotID, cloneID
}

// waitForLineWith is WaitForLine for a claim that needs more than one substring on the
// *same* line. "read view recovered" alone is printed for every volume this host serves,
// so a single-substring wait would be satisfied by the clone's parent and prove nothing
// about the clone.
func waitForLineWith(t *testing.T, p *testinfra.Process, timeout time.Duration, parts ...string) {
	t.Helper()
	waitFor(t, timeout, "a line from "+p.Name+" carrying "+strings.Join(parts, " + "), func() bool {
		for _, line := range p.Output() {
			ok := true
			for _, want := range parts {
				if !strings.Contains(line, want) {
					ok = false
					break
				}
			}
			if ok {
				return true
			}
		}
		return false
	})
}
