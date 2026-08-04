//go:build e2e

package e2e

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestAKilledAgentReAttachesAtTheSameEpoch is ADR-0024 as a deployment experiences it:
// SIGKILL, no cleanup, and a new process that finds a WAL on disk and a Control Plane
// still listing the volume at the epoch it was granted.
//
// The DST arm proves the numbering cannot collide. What only this lane can show is that
// the *process* comes back at all — that `Apply` resumes rather than refusing, and that
// nothing in the start-up path needs a clean shutdown to have happened.
func TestAKilledAgentReAttachesAtTheSameEpoch(t *testing.T) {
	d := start(t)
	first := d.startAgent(t, "agent-1")
	d.waitForHost(t)
	d.seedVolume(t)
	volumeID := waitForServedVolume(t, d)
	sock := filepath.Join(d.sockDir, volumeID+".sock")
	waitFor(t, 30*time.Second, "the first socket", func() bool {
		_, err := os.Stat(sock)
		return err == nil
	})
	epochBefore := volumeEpoch(t, d, volumeID)

	first.Kill(t)

	second := d.startAgent(t, "agent-2")
	second.WaitForLine(t, "key-encryption key loaded", startup)
	waitFor(t, 60*time.Second, "the volume served again", func() bool {
		for _, v := range servedVolumes(t, d) {
			if v.GetVolumeId() == volumeID {
				return true
			}
		}
		return false
	})
	if got := volumeEpoch(t, d, volumeID); got != epochBefore {
		t.Fatalf("the epoch moved across a restart: %d -> %d (ADR-0024 says it must not)", epochBefore, got)
	}
}
