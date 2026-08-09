package agent_test

import (
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/ids"
)

// A host id is not free-form text: `hosts.host_id` is the `uuidv7` domain (INV-22) and
// the pg adapter parses the string before it ever reaches SQL. An Agent started with
// `-host-id host-1` therefore writes zero rows on every heartbeat — and because
// Loop.Run feeds the error straight into its backoff, it retries that forever while
// logging one line at startup. The operator sees a process that looks alive and a fleet
// that never learns the host exists.
//
// Refusing it at construction is the difference between a five-second fix and an
// afternoon. This is the boundary Parse belongs at: rejecting a bad value where it
// enters, rather than letting it flow inward.
func TestConfigRefusesAHostIDThatIsNotAUUIDv7(t *testing.T) {
	base := func() agent.Config {
		return agent.Config{
			HostID:            ids.New().String(),
			AgentVersion:      "test",
			MaxFormatVersion:  3,
			HeartbeatInterval: 5 * time.Second,
			RetryBackoff:      time.Second,
			LeaseTTL:          30 * time.Second,
		}
	}

	tests := []struct {
		name   string
		hostID string
		wantOK bool
	}{
		{name: "a v7 id is what the schema wants", hostID: ids.New().String(), wantOK: true},
		{name: "the shape an operator types by hand", hostID: "host-1"},
		{name: "empty", hostID: ""},
		{name: "a v4 uuid: right shape, wrong version (INV-22)", hostID: "9f1b2c3d-4e5f-4a6b-8c9d-0e1f2a3b4c5d"},
		{name: "a v7 id with whitespace, which SQL would not match", hostID: " " + ids.New().String()},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			cfg.HostID = tc.hostID
			err := cfg.Validate()
			if tc.wantOK {
				if err != nil {
					t.Fatalf("Validate rejected a valid v7 host id: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate accepted %q; every heartbeat with it would write zero rows", tc.hostID)
			}
			// The message has to name the fix. "invalid host id" sends an operator
			// looking at the Control Plane.
			if !strings.Contains(err.Error(), "host id") {
				t.Errorf("error does not mention the host id: %v", err)
			}
		})
	}
}
