package real_test

// The seam, driven with the real binary: a `volume-agent` that measured this machine and
// bounded one volume's read view with a share of it.
//
// Everything under this is already proven in-process — measureMemory against nine fake
// machines, agent.NewBudget against seven — and none of that would have caught the defect
// this file exists for, which is the one CLAUDE.md's table is entirely made of: a
// derivation nothing calls. `MaxViewBytes` was a 256 MiB constant for exactly that
// reason. Every unit test that wanted a view bound set one itself, so the number the
// *binary* handed its Logs was never anybody's assertion.
//
// It lives here, next to the file that reads /proc, for the reason the OTLP proof next
// door gives: internal/simio/real is where the syscall is, the lane runs in `task test`
// with no Docker, and integration/e2e — whose budget_test.go reads the other half of this
// same line — is gated behind containers. If this moves there it is a package rename.

import (
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/real"
)

// TestTheAgentBoundsItsReadViewFromTheMemoryItMeasured asserts on the line the process
// printed, and on nothing the process could tell itself: the memory figure has to match
// what this machine reports independently, the source has to be a file that exists, and
// the per-volume bound has to be that figure divided — not 256 MiB, which is what a
// constant would print and what every arithmetic-free assertion would accept.
func TestTheAgentBoundsItsReadViewFromTheMemoryItMeasured(t *testing.T) {
	bin := buildVolumeAgent(t)

	// A fan-out that is not the default, so a bound that ignored -max-volumes and a
	// bound that divided by it cannot produce the same number.
	const maxVolumes = 5

	agentProc := startAgent(t, bin,
		"-host-id", ids.New().String(),
		// Nothing is served, so the Control Plane is never reached; the start-up line is
		// printed before the first cycle. An unreachable URL keeps this test to the one
		// thing it is about.
		"-control-plane", "http://127.0.0.1:1",
		"-data-dir", t.TempDir(),
		"-vhost-socket-dir", shortSocketDir(t),
		"-object-store-dir", t.TempDir(),
		"-max-volumes", strconv.Itoa(maxVolumes),
		"-heartbeat-interval", "1h",
		"-retry-backoff", "1h",
		"-lease-ttl", "2h",
	)
	agentProc.waitForLine(t, "volume-agent starting")
	agentProc.stop(t)

	line := lastLineContaining(t, agentProc, "volume-agent starting")
	memory := intField(t, line, "memory_bytes")
	view := intField(t, line, "volume_view_share_bytes")
	source := stringField(t, line, "memory_source")

	// Measured here, in this test's own process, on the same machine: the Agent's number
	// has to be the machine's, not a default it fell back to.
	want, err := real.MeasureMemory()
	if err != nil {
		t.Fatalf("measuring this machine: %v", err)
	}
	if memory != want.LimitBytes {
		t.Fatalf("the Agent started with memory_bytes=%d; this machine reports %d from %s\n%s",
			memory, want.LimitBytes, want.Source, line)
	}
	// The source is what makes the number checkable by the operator who reads the line,
	// so it has to be a path that exists rather than a label.
	if _, err := os.Stat(source); err != nil { //nolint:usetesting // the file is /proc's, not the test's
		t.Fatalf("the Agent attributes its memory figure to %q, which cannot be read: %v\n%s", source, err, line)
	}

	// The arithmetic, restated from the machine rather than from the Agent: one volume's
	// share of the fraction of memory set aside for read views, halved because a view
	// being written costs twice its accounted size in RSS.
	expect := agent.Budget{MemoryBytes: want.LimitBytes, MaxVolumes: maxVolumes}.ViewShare()
	if view != expect {
		t.Fatalf("each of %d volumes may hold a %d-byte read view on a %d-byte machine, want %d\n%s",
			maxVolumes, view, memory, expect, line)
	}
	// The assertion that would have caught the constant. Every check above is satisfied
	// by a binary that computes a derivation and hands its Logs 256 MiB anyway, as long
	// as the machine happens to be the one the constant was sized for — so the shape is
	// pinned too: the bound is a division of *this* machine by *this* fan-out.
	if view*int64(maxVolumes)*agent.ViewRSSFactor > int64(agent.ViewRatio*float64(memory)) {
		t.Fatalf("%d volumes at %d bytes cost more RSS than the %g of %d bytes this Agent set aside for read views\n%s",
			maxVolumes, view, agent.ViewRatio, memory, line)
	}
	t.Logf("the Agent printed: %s", line)
}

func lastLineContaining(t *testing.T, p *agentProcess, want string) string {
	t.Helper()
	var line string
	for _, l := range strings.Split(p.output(), "\n") {
		if strings.Contains(l, want) {
			line = l
		}
	}
	if line == "" {
		t.Fatalf("the Agent printed no line containing %q:\n%s", want, p.output())
	}
	return line
}

// intField and stringField read one key=value out of a slog line. A missing field fails
// the test rather than returning a zero: an operator reads this line to find out why a
// guest is taking I/O errors on a host with free memory, and a field that stopped being
// printed is the same regression as one that was never computed.
func intField(t *testing.T, line, key string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(stringField(t, line, key), 10, 64)
	if err != nil {
		t.Fatalf("%s is not a number: %v\n%s", key, err, line)
	}
	return n
}

func stringField(t *testing.T, line, key string) string {
	t.Helper()
	for _, tok := range strings.Fields(line) {
		if name, value, ok := strings.Cut(tok, "="); ok && name == key {
			return value
		}
	}
	t.Fatalf("the start-up line carries no %s:\n%s", key, line)
	return ""
}
