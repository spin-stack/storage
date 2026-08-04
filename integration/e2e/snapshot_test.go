//go:build e2e

package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/testinfra"
)

// TestASnapshotOfALiveVolumeIsPublished is §19's trigger with real binaries: a guest
// writes and fsyncs, an operator asks for a snapshot through the Control Plane, and the
// copy appears in the bucket **while the Agent is still serving the volume**.
//
// That last clause is the test. Everything else in this lane publishes at stop, so a
// snapshot that only appeared after the Agent exited would be the stop path wearing a
// different name. Here the Agent is never stopped: the object shows up under a running
// volume, which is the only evidence that the freeze-and-upload path exists at all.
//
// It also exercises the piece unit tests structurally cannot reach — the request travels
// Control Plane → desired state → Agent → object store across three processes, and every
// defect this project has shipped lived at exactly that kind of seam.
func TestASnapshotOfALiveVolumeIsPublished(t *testing.T) {
	kernel, initramfs := testinfra.GuestImages(t)
	_, _ = testinfra.QEMUPaths(t)

	d := start(t)
	agent := d.startAgent(t, "volume-agent")
	d.waitForHost(t)
	d.seedVolume(t)

	volumeID := waitForServedVolume(t, d)
	sock := filepath.Join(d.sockDir, volumeID+".sock")
	waitFor(t, 30*time.Second, fmt.Sprintf("the socket for %s", volumeID), func() bool {
		_, err := os.Stat(sock)
		return err == nil
	})
	agent.WaitForLine(t, "serving volume", 30*time.Second)

	code, out := testinfra.RunLinuxGuest(t, sock, kernel, initramfs)
	if !strings.Contains(out, "GUESTINIT-PASS") {
		t.Fatalf("the guest never reported a verdict (exit %d):\n%s", code, testinfra.VerdictLines(out))
	}

	prefix := "image/" + volumeID + "/snapshots/"
	if keys := storeKeys(t, d, prefix); len(keys) != 0 {
		t.Fatalf("%d snapshot(s) under %s before one was asked for: the assertion would be vacuous:\n%v",
			len(keys), prefix, keys)
	}

	d.requestSnapshot(t, volumeID)

	// The Agent polls the desired state, so this waits for a reconcile cycle plus an
	// upload. Nothing here stops the Agent.
	waitFor(t, 60*time.Second, "the snapshot to appear in the bucket", func() bool {
		return len(storeKeys(t, d, prefix)) > 0
	})
	agent.WaitForLine(t, "snapshot published", 30*time.Second)

	keys := storeKeys(t, d, prefix)
	if len(keys) != 1 {
		t.Fatalf("want exactly one snapshot manifest under %s, got %d: %v", prefix, len(keys), keys)
	}
	// And the volume is still being served: a snapshot that cost the guest its device
	// is not the §19 this claims to implement.
	if _, err := os.Stat(sock); err != nil {
		t.Fatalf("the volume stopped being served when it was snapshotted: %v", err)
	}
	t.Logf("a live volume was snapshotted into %s", keys[0])
}
