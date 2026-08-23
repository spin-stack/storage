//go:build e2e

package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/testinfra"
)

// TestASecondAgentRefusesTheSameDataDir closes DEV-0014 where it is observable: two
// real processes and a real flock(2).
//
// §10 opens with "un proceso por host" and nothing enforced it. Two live Agents on one
// --data-dir both open whatever local state is under it and both write to it; the second
// silently takes over from the first rather than failing. The simulated lock in the DST
// harness would only be checking the simulation's own map; what makes this true in
// production is the kernel, and this is the only place that is exercised.
//
// The lock moved into `main` when the volume manager that used to take it was withdrawn.
// That is the change this test guards: a claim on the data directory has to be taken by
// whatever process holds the directory, and Stage 1's qcow2 manager should take it back
// when it owns the directory's layout — this test is what says so out loud if it does not.
//
// The first Agent is left running on purpose. A lock that refused *after* the holder died
// would be the worse bug: the kernel drops an flock when a process dies, precisely so a
// kill -9'd Agent's replacement starts.
func TestASecondAgentRefusesTheSameDataDir(t *testing.T) {
	d := start(t)
	first := d.startAgent(t, "agent-1")
	d.waitForHost(t)

	second := testinfra.Start(t, testinfra.ProcessConfig{
		Name: "agent-2",
		Path: d.agentBin,
		Args: []string{
			"-host-id", ids.New().String(),
			"-control-plane", d.cpURL,
			"-data-dir", d.dataDir, // the same one agent-1 holds
			"-kek-file", d.kekFile,
		},
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

	// And the first is untouched — it still holds the directory and is still heartbeating.
	// Asserted on the catalog's clock rather than on the first Agent's log, because a
	// healthy Agent prints nothing after start-up: the loop logs a cycle only when one
	// fails. A timestamp that advances after the second process was refused is the fleet
	// observing that the holder survived the intruder, which is the half of this that a
	// lock releasing itself under contention would break.
	beat := lastHeartbeat(t, d, d.hostID)
	waitFor(t, 60*time.Second, "the first Agent to heartbeat again", func() bool {
		return lastHeartbeat(t, d, d.hostID).After(beat)
	})
	// And it was never the one refused. A lock that let go and re-took under contention
	// would put this line in the holder's log instead of the intruder's, and the
	// heartbeat above cannot tell the two apart.
	for _, line := range first.Output() {
		if strings.Contains(line, "already using") {
			t.Fatalf("the Agent holding %s was the one refused:\n%s", d.dataDir, line)
		}
	}
}

// lastHeartbeat reads the instant the catalog last heard from a host.
func lastHeartbeat(t *testing.T, d *deployment, hostID string) time.Time {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), d.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var at time.Time
	if err := pool.QueryRow(t.Context(),
		`SELECT last_heartbeat FROM hosts WHERE host_id = $1`, hostID).Scan(&at); err != nil {
		t.Fatalf("reading the host's last heartbeat: %v", err)
	}
	return at
}

// TestTheAgentWritesWhereItWasTold. The Disk is rooted at --data-dir and the paths handed
// to it are inside *that* namespace, so passing the operator's absolute path in both
// places put every file under <data-dir>/<data-dir>/... — consistent, restart-safe, and
// nowhere near where the operator was told to look. Nothing in-process could see it:
// every test gives the Disk a whole filesystem, where the two paths agree.
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
