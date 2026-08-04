//go:build e2e

package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/testinfra"
)

// TestASecondAgentRefusesTheSameDataDir closes DEV-0014 where it is observable: two
// real processes and a real flock(2).
//
// §10 opens with "un proceso por host" and nothing enforced it. Two live Agents on one
// --data-dir both resume the same segment files and both append to them, and
// hostio.Listen unlinks a stale socket before binding — so the second silently steals
// the guest from the first rather than failing to bind. The simulated lock in the DST
// harness would only be checking the simulation's own map; what makes this true in
// production is the kernel, and this is the only place that is exercised.
//
// The first Agent is left running on purpose. A lock that refused *after* the holder
// died would be the worse bug: ADR-0024 requires a kill -9'd Agent's replacement to
// start, which is what TestAKilledAgentReAttachesAtTheSameEpoch (restart_test.go)
// covers.
func TestASecondAgentRefusesTheSameDataDir(t *testing.T) {
	d := start(t)
	first := d.startAgent(t, "agent-1")
	d.waitForHost(t)

	second := testinfra.Start(t, testinfra.ProcessConfig{
		Name: "agent-2",
		Path: d.agentBin,
		Args: append([]string{
			"-host-id", ids.New().String(),
			"-control-plane", d.cpURL,
			"-data-dir", d.dataDir, // the same one agent-1 holds
			"-vhost-socket-dir", d.sockDir,
			"-kek-file", d.kekFile,
		}, d.storeArgs()...),
		Env: d.agentEnv,
	})
	if err := second.Wait(t, 30*time.Second); err == nil {
		t.Fatal("a second Agent started against a data directory another one holds (§10, DEV-0014)")
	}
	// The message has to name the directory: the operator's next action is to find the
	// process holding it, and an errno would not help them do that.
	var said bool
	for _, line := range second.Output() {
		if strings.Contains(line, d.dataDir) && strings.Contains(line, "already using") {
			said = true
		}
	}
	if !said {
		t.Fatalf("the refusal did not name %s:\n%s", d.dataDir, strings.Join(second.Output(), "\n"))
	}

	// And the first is untouched — it still holds the directory and is still serving.
	d.seedVolume(t)
	first.WaitForLine(t, "serving volume", 60*time.Second)
}

// TestTheAgentWritesWhereItWasTold. The Disk is rooted at --data-dir and the manager's
// DataDir is a path inside *that* namespace, so passing the operator's absolute path in
// both places put every WAL under <data-dir>/<data-dir>/wal/... — consistent,
// restart-safe, and nowhere near where the operator was told to look. Nothing
// in-process could see it: every test gives the manager a Disk spanning a whole
// filesystem, where the two paths agree.
//
// The lock file is the witness because it is created at start-up, before any guest has
// written a byte: a WAL directory would only appear on the first append.
func TestTheAgentWritesWhereItWasTold(t *testing.T) {
	d := start(t)
	agent := d.startAgent(t, "volume-agent")
	agent.WaitForLine(t, "volume-agent starting", startup)

	waitFor(t, 30*time.Second, "the agent to claim its data directory", func() bool {
		_, err := os.Stat(filepath.Join(d.dataDir, "agent.lock"))
		return err == nil
	})
	// The doubled path, spelled out rather than globbed: <data-dir> holding a copy of
	// its own absolute path is the exact shape of the bug.
	doubled := filepath.Join(d.dataDir, strings.TrimPrefix(d.dataDir, string(filepath.Separator)))
	if _, err := os.Stat(doubled); err == nil {
		t.Fatalf("the Agent wrote under %s: --data-dir is being applied twice", doubled)
	}
}
