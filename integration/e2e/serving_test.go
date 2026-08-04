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

// TestTheDeploymentServesAVolume is the slice, end to end, through the binaries: a
// Control Plane that was elected, a volume provisioned by the real provisioning path,
// an Agent that heartbeats, is handed that volume, and binds a socket for it.
//
// The assertion that matters is the *socket*: it is the only artefact that proves the
// Agent got as far as building a runtime. A heartbeat proves the process started; a
// desired-state answer proves the RPC works; only a bound socket proves a volume was
// opened, its keys unwrapped and its WAL created.
func TestTheDeploymentServesAVolume(t *testing.T) {
	d := start(t)
	// The Agent first: a volume references its primary host, and the host row is
	// created by the Agent's own heartbeat. Provisioning for a host the fleet has
	// never seen fails on the foreign key, which is the catalog saying the same thing.
	agent := d.startAgent(t, "volume-agent")
	d.waitForHost(t)
	d.seedVolume(t)

	volumeID := waitForServedVolume(t, d)
	sock := filepath.Join(d.sockDir, volumeID+".sock")
	waitFor(t, 30*time.Second, fmt.Sprintf("the socket for %s", volumeID), func() bool {
		_, err := os.Stat(sock)
		return err == nil
	})

	// And the Agent says what it built. Asserting on the log line rather than on a WAL
	// directory is deliberate: segments are created by the first append, and no guest
	// has written yet — a directory assertion would be waiting for something the
	// system correctly does not do.
	agent.WaitForLine(t, "serving volume", 30*time.Second)
	assertLogField(t, agent, "serving volume", "encrypted=true")
}

// assertLogField fails unless the process printed a line containing both needles. slog
// writes key=value, so this reads a structured field without parsing the format.
func assertLogField(t *testing.T, p *testinfra.Process, line, field string) {
	t.Helper()
	for _, l := range p.Output() {
		if strings.Contains(l, line) && strings.Contains(l, field) {
			return
		}
	}
	t.Fatalf("no %q line carried %q; got:\n%s", line, field, strings.Join(p.Output(), "\n"))
}
