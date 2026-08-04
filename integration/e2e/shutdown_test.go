//go:build e2e

package e2e

import (
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/testinfra"
)

// TestBothBinariesShutDownCleanly asks each binary to stop the way a supervisor does —
// SIGINT — and checks it actually stops. Until this test existed
// `testinfra.Process.Stop` had **no caller anywhere in the tree**: every lane either
// waited for a process that exits on its own or SIGKILLed one, so the entire clean
// shutdown path (`signal.NotifyContext`, the Control Plane's `-shutdown-grace`, the
// Agent's `defer volumes.Close()`) was code no test had ever asked to run.
//
// What it asserts is deliberately narrow, and narrower than the first version of it.
// That version also claimed that a second Agent claiming the same `--data-dir`
// afterwards proved `volumes.Close()` had run. It proves nothing: the kernel drops a
// flock when the process exits, however it exits, so the assertion passed with
// `volumes.Close()` deleted. Planting that bug is what found it. What is left is what a
// supervisor can actually tell apart:
//
//   - the process exits within the timeout after SIGINT (asserted inside Stop — a
//     daemon that ignores the signal hangs here, which is the bug the Control Plane
//     really had when SIGTERM was missing from its NotifyContext);
//   - it exits zero rather than crashing on the way down (also inside Stop);
//   - it prints its parting line, so an operator reading logs sees a shutdown rather
//     than a disappearance.
//
// Proven able to fail: dropping `os.Interrupt` from either binary's
// `signal.NotifyContext` makes Stop time out.
func TestBothBinariesShutDownCleanly(t *testing.T) {
	d := start(t)
	agent := d.startAgent(t, "agent-1")
	d.waitForHost(t)
	d.seedVolume(t)
	agent.WaitForLine(t, "serving volume", 60*time.Second)

	// Stopped while it is serving a volume, which is the state a supervisor restarts in
	// and the one where Close has runtimes to tear down.
	agent.Stop(t, 30*time.Second)
	assertSaid(t, agent, "volume-agent stopped")

	d.cp.Stop(t, 30*time.Second)
	assertSaid(t, d.cp, "control-plane stopped")
}

// assertSaid fails with the whole stream, because "the line is missing" and "the process
// printed a panic instead" need different fixes and the line alone cannot tell them apart.
func assertSaid(t *testing.T, p *testinfra.Process, want string) {
	t.Helper()
	for _, line := range p.Output() {
		if strings.Contains(line, want) {
			return
		}
	}
	t.Fatalf("%s never printed %q:\n%s", p.Name, want, strings.Join(p.Output(), "\n"))
}
