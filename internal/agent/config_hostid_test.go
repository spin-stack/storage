package agent_test

import (
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/ids"
)

// A host id is not free-form text: `hosts.host_id` is the `uuidv7` domain (INV-22) and the
// pg adapter parses it before it reaches SQL, so `-host-id host-1` writes zero rows on
// every heartbeat — forever, since Loop.Run feeds the error into its backoff. The operator
// sees a process that looks alive and a fleet that never learns the host exists.
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

// TestConfigRefusesWiringThatWouldRunWrong walks the rest of Validate. Every field it
// checks is a wiring mistake in `main` that produces a process which starts, looks healthy
// and is wrong, so the refusal has to land on the flag.
func TestConfigRefusesWiringThatWouldRunWrong(t *testing.T) {
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
	if err := base().Validate(); err != nil {
		t.Fatalf("the base configuration must be valid, or every case below proves nothing: %v", err)
	}

	tests := []struct {
		name string
		// mut breaks exactly one thing, and the name says what an operator did.
		mut  func(*agent.Config)
		says string
	}{
		{"no version reported to the fleet (§27, INV-19)", func(c *agent.Config) { c.AgentVersion = "" }, "version"},
		{"a build that reads no format", func(c *agent.Config) { c.MaxFormatVersion = 0 }, "format version"},
		{"no cadence: the loop would spin", func(c *agent.Config) { c.HeartbeatInterval = 0 }, "heartbeat interval"},
		{"no backoff: a failing cycle would retry without pause", func(c *agent.Config) { c.RetryBackoff = 0 }, "retry backoff"},
		{"a backoff longer than the interval, which is not a backoff", func(c *agent.Config) { c.RetryBackoff = 10 * time.Second }, "retry backoff"},
		{"no lease TTL", func(c *agent.Config) { c.LeaseTTL = 0 }, "lease TTL"},
		// The one that is not obviously a typo: a TTL inside one heartbeat interval
		// lapses during normal operation, so a perfectly healthy host fences itself and
		// its guests lose their disks on a fleet where nothing is wrong.
		{"a lease that expires within one heartbeat", func(c *agent.Config) { c.LeaseTTL = 5 * time.Second }, "lease TTL"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			tc.mut(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("Validate accepted %+v", cfg)
			}
			if !strings.Contains(err.Error(), tc.says) {
				t.Errorf("error %q does not name what to fix (%q)", err, tc.says)
			}
		})
	}
}
