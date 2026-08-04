//go:build e2e

package e2e

import (
	"os"
	"path/filepath"
	"testing"
	"time"
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

	volumeID := waitForServedVolume(t, d)
	agent.WaitForLine(t, "serving volume", 30*time.Second)

	// The request travels as desired state, so the desired state is where both halves of
	// it are observed: the id on its way to the host, and the field clearing once the
	// Control Plane has recorded the answer. Waiting on the *bucket* instead is not
	// enough and this lane proved it — the object lands a reconcile cycle before the
	// catalog row leaves CREATING, and a clone attempted in that window is refused.
	d.requestSnapshot(t, volumeID)
	var snapshotID string
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
	var cloneID string
	waitFor(t, 30*time.Second, "the clone in this host's desired state", func() bool {
		for _, v := range servedVolumes(t, d) {
			if v.GetVolumeId() != volumeID {
				cloneID = v.GetVolumeId()
				return true
			}
		}
		return false
	})
	if got := volumeEpoch(t, d, cloneID); got != 1 {
		t.Fatalf("clone %s is at epoch %d, want 1 (a fresh active child, §20)", cloneID, got)
	}

	waitFor(t, 30*time.Second, "the clone's socket", func() bool {
		_, err := os.Stat(filepath.Join(d.sockDir, cloneID+".sock"))
		return err == nil
	})
	agent.WaitForLine(t, "cloned_from="+snapshotID, 30*time.Second)
	t.Logf("clone %s of snapshot %s is served on the host that took it", cloneID, snapshotID)
}
