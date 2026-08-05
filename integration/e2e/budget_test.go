//go:build e2e

package e2e

import (
	"strconv"
	"strings"
	"testing"
)

// TestTheAgentBoundsItsWritePathFromTheDeviceItMeasured is the seam this increment
// exists for, and it is the one nothing could see from inside a package.
//
// `grep -rn "Limits" cmd/` returned nothing: the production binary set no wal.Limits at
// all, so no real Agent had a write-path bound of any kind, while eight unit tests
// proved backpressure against limits they set themselves. That is this repository's
// recurring shape — machinery proven in tests, unwired in the binary — and the only
// thing that catches it is a test that reads what the *process* did.
//
// So the evidence is the line the Agent printed at start-up, and the assertions are
// about the arithmetic a reader of that line depends on:
//
//   - the device was measured (statfs of --data-dir's filesystem), not configured;
//   - the guests' budget is strictly below the device, which is the reserve — the
//     headroom that keeps a device full for guests from being a full device, so the
//     stop's fdatasync and the filesystem's own metadata still have somewhere to go
//     (ADR-0013 §2 as amended: the publish itself writes no local byte);
//   - each volume gets a share of that budget, not the whole of it. A host serving
//     -max-volumes volumes each bounded by the whole budget is the unsummed backlog
//     ADR-0013 §1 exists for, and it would satisfy every assertion that only checked
//     "a bound exists".
func TestTheAgentBoundsItsWritePathFromTheDeviceItMeasured(t *testing.T) {
	d := start(t)
	agent := d.startAgent(t, "volume-agent")
	agent.WaitForLine(t, "volume-agent starting", startup)

	var line string
	for _, l := range agent.Output() {
		if strings.Contains(l, "volume-agent starting") {
			line = l
		}
	}
	if line == "" {
		t.Fatalf("the Agent never said it was starting:\n%s", strings.Join(agent.Output(), "\n"))
	}

	device := field(t, line, "device_bytes")
	guest := field(t, line, "guest_budget_bytes")
	reserve := field(t, line, "reserve_bytes")
	share := field(t, line, "volume_share_bytes")
	maxVolumes := field(t, line, "max_volumes")

	switch {
	case device <= 0:
		t.Fatalf("the Agent started with a %d-byte device: it did not measure one\n%s", device, line)
	case guest <= 0 || guest >= device:
		t.Fatalf("guest budget %d of a %d-byte device: the reserve is not headroom\n%s", guest, device, line)
	case reserve <= 0 || guest+reserve > device:
		t.Fatalf("reserve %d with a %d-byte budget on a %d-byte device\n%s", reserve, guest, device, line)
	case share <= 0:
		t.Fatalf("each volume is bounded by %d bytes: that is no bound\n%s", share, line)
	case maxVolumes > 1 && share >= guest:
		t.Fatalf("each of %d volumes may hold %d bytes of a %d-byte budget: the budget is not divided\n%s",
			maxVolumes, share, guest, line)
	}
}

// field reads one key=value from a slog line. It fails the test rather than returning
// an error: a missing field means the Agent stopped printing something an operator
// reads to explain backpressure on a device that looks half empty, which is the same
// regression as never computing it.
func field(t *testing.T, line, key string) int64 {
	t.Helper()
	for _, tok := range strings.Fields(line) {
		name, value, ok := strings.Cut(tok, "=")
		if !ok || name != key {
			continue
		}
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			t.Fatalf("%s=%q is not a number: %v", key, value, err)
		}
		return n
	}
	t.Fatalf("the start-up line carries no %s:\n%s", key, line)
	return 0
}
