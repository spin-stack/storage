package lease_test

import (
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/simio/sim"
	"pgregory.net/rapid"
)

const ttl = 10 * time.Second

func newManager(t *testing.T) (*lease.Manager, *sim.Clock) {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	return lease.NewManager(clk, ttl), clk
}

func TestValidity(t *testing.T) {
	tests := []struct {
		name  string
		drive func(m *lease.Manager, clk *sim.Clock)
		valid bool
	}{
		{"ungranted is invalid", func(_ *lease.Manager, _ *sim.Clock) {}, false},
		{"granted is valid", func(m *lease.Manager, _ *sim.Clock) { m.Grant() }, true},
		{"valid within ttl", func(m *lease.Manager, clk *sim.Clock) { m.Grant(); clk.Advance(9 * time.Second) }, true},
		{"expired past ttl", func(m *lease.Manager, clk *sim.Clock) { m.Grant(); clk.Advance(11 * time.Second) }, false},
		{"renew resets the clock", func(m *lease.Manager, clk *sim.Clock) {
			m.Grant()
			clk.Advance(9 * time.Second)
			m.RenewAt(m.Generation(), clk.Now())
			clk.Advance(9 * time.Second) // 18s since grant, but only 9s since renew
		}, true},
		{"revoke invalidates", func(m *lease.Manager, _ *sim.Clock) { m.Grant(); m.Revoke() }, false},
		{"renew without grant stays invalid", func(m *lease.Manager, clk *sim.Clock) {
			m.RenewAt(m.Generation(), clk.Now())
		}, false},

		// A renewal that arrives after the lease has already lapsed must not bring it
		// back: the Control Plane's fencing deadline was computed from the renewal it
		// recorded, and by now a new writer may already hold the volume (INV-06/09).
		{"renew after expiry does not resurrect the lease", func(m *lease.Manager, clk *sim.Clock) {
			m.Grant()
			clk.Advance(11 * time.Second)
			m.RenewAt(m.Generation(), clk.Now())
		}, false},
		// The heartbeat reached the CP at T+8 (that is what last_renewal says) but the
		// response was stuck for 5s. Anchoring to the response instant would extend
		// validity to T+18 while the CP's deadline is T+18 too — and the lease has in
		// fact been dead since T+10.
		{"a renewal delayed past the ttl is refused", func(m *lease.Manager, clk *sim.Clock) {
			m.Grant()
			clk.Advance(8 * time.Second)
			sent := clk.Now()
			clk.Advance(5 * time.Second)
			m.RenewAt(m.Generation(), sent)
		}, false},
		// A stamp from the future can only come from a bug or a hostile response; it
		// must never extend the lease.
		{"a renewal stamped in the future is refused", func(m *lease.Manager, clk *sim.Clock) {
			m.Grant()
			clk.Advance(9 * time.Second)
			m.RenewAt(m.Generation(), clk.Now().Add(time.Hour))
			clk.Advance(2 * time.Second)
		}, false},
		// §12.2 / INV-06: a grant that was in flight when the Agent accepted a fence
		// must not re-arm the lease — the CP has promoted elsewhere by now.
		{"a grant issued before a revoke is ignored", func(m *lease.Manager, clk *sim.Clock) {
			m.Grant()
			gen := m.Generation()
			m.Revoke()
			m.GrantAt(gen, clk.Now())
		}, false},
		{"a renewal issued before a revoke is ignored", func(m *lease.Manager, clk *sim.Clock) {
			m.Grant()
			gen := m.Generation()
			m.Revoke()
			m.RenewAt(gen, clk.Now())
		}, false},
		// A genuine re-attach after a revoke is still allowed.
		{"a grant issued after a revoke re-arms", func(m *lease.Manager, _ *sim.Clock) {
			m.Grant()
			m.Revoke()
			m.Grant()
		}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, clk := newManager(t)
			tc.drive(m, clk)
			if got := m.Valid(); got != tc.valid {
				t.Fatalf("Valid() = %v, want %v", got, tc.valid)
			}
		})
	}
}

// TestRenewAtIsAnchoredToTheRequestInstant is the sharp edge of §12.2: the Agent's
// validity window is measured from the instant the renewal *left the host*, not from
// the instant the answer came back. Anchoring to the response lets a slow round trip
// push the Agent's window past the deadline the CP derived from last_renewal, and
// two writers ACK at once.
func TestRenewAtIsAnchoredToTheRequestInstant(t *testing.T) {
	m, clk := newManager(t)
	m.Grant()

	clk.Advance(8 * time.Second)
	sent := clk.Now() // the heartbeat leaves the host; the CP will stamp at or after this
	clk.Advance(time.Second)
	if !m.RenewAt(m.Generation(), sent) {
		t.Fatal("a renewal requested 1s ago must be accepted")
	}
	clk.Advance(time.Second) // T+10: still inside sent+ttl
	if !m.Valid() {
		t.Fatal("lease must be valid inside request instant + ttl")
	}
	clk.Advance(8 * time.Second) // T+18 == sent + ttl
	if m.Valid() {
		t.Fatal("validity outlived request_instant + ttl — the CP's fencing deadline was already passed")
	}
}

// TestAcceptance pins the accept/refuse decision itself, which is what an Agent
// branches on to decide whether to keep ACKing or self-fence.
func TestAcceptance(t *testing.T) {
	tests := []struct {
		name   string
		drive  func(m *lease.Manager, clk *sim.Clock) bool
		accept bool
	}{
		{"a fresh grant is accepted", func(m *lease.Manager, clk *sim.Clock) bool {
			return m.GrantAt(m.Generation(), clk.Now())
		}, true},
		{"a renewal without a grant is refused", func(m *lease.Manager, clk *sim.Clock) bool {
			return m.RenewAt(m.Generation(), clk.Now())
		}, false},
		{"a renewal of a live lease is accepted", func(m *lease.Manager, clk *sim.Clock) bool {
			m.Grant()
			clk.Advance(time.Second)
			return m.RenewAt(m.Generation(), clk.Now())
		}, true},
		{"a renewal of an expired lease is refused", func(m *lease.Manager, clk *sim.Clock) bool {
			m.Grant()
			clk.Advance(ttl + time.Second)
			return m.RenewAt(m.Generation(), clk.Now())
		}, false},
		{"a renewal older than the one already applied is refused", func(m *lease.Manager, clk *sim.Clock) bool {
			m.Grant()
			old := clk.Now()
			clk.Advance(3 * time.Second)
			m.RenewAt(m.Generation(), clk.Now())
			return m.RenewAt(m.Generation(), old) // reordered response
		}, false},
		{"a grant from a superseded generation is refused", func(m *lease.Manager, clk *sim.Clock) bool {
			gen := m.Generation()
			m.Revoke()
			return m.GrantAt(gen, clk.Now())
		}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, clk := newManager(t)
			if got := tc.drive(m, clk); got != tc.accept {
				t.Fatalf("accepted = %v, want %v", got, tc.accept)
			}
		})
	}
}

// TestValidityNeverOutlivesTheControlPlaneDeadline is the property behind INV-06 and
// INV-11: whatever the round-trip delays are, the Agent must stop being valid no
// later than cp_last_renewal + ttl — the instant from which the CP starts counting
// FENCING_WAIT. The CP records last_renewal when the request *arrives*; the Agent
// only ever anchors on when it *left*, so the Agent's window is a subset of the CP's.
func TestValidityNeverOutlivesTheControlPlaneDeadline(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
		m := lease.NewManager(clk, ttl)

		m.Grant()
		cpLastRenewal := clk.Now() // the CP stamped the grant at the same instant

		mustHold := func(where string) {
			rt.Helper()
			if m.Valid() && clk.Now() >= cpLastRenewal.Add(ttl) {
				rt.Fatalf("%s: lease still valid at %v, past the CP deadline %v",
					where, clk.Now(), cpLastRenewal.Add(ttl))
			}
		}

		for range rapid.IntRange(1, 8).Draw(rt, "heartbeats") {
			sent := clk.Now()
			// Request in flight; the CP stamps last_renewal when it lands.
			clk.Advance(time.Duration(rapid.IntRange(0, 6000).Draw(rt, "to_cp_ms")) * time.Millisecond)
			cpLastRenewal = clk.Now()
			mustHold("in flight")
			// Response in flight: a GC pause, a PG failover retry, a healing partition.
			clk.Advance(time.Duration(rapid.IntRange(0, 25000).Draw(rt, "back_ms")) * time.Millisecond)
			m.RenewAt(m.Generation(), sent)
			mustHold("after applying the renewal")
			clk.Advance(time.Duration(rapid.IntRange(0, 4000).Draw(rt, "idle_ms")) * time.Millisecond)
			mustHold("idle")
		}
	})
}

func TestRemaining(t *testing.T) {
	m, clk := newManager(t)
	if m.Remaining() != 0 {
		t.Fatal("ungranted lease has no remaining time")
	}
	m.Grant()
	clk.Advance(4 * time.Second)
	if got := m.Remaining(); got != 6*time.Second {
		t.Fatalf("remaining = %v, want 6s", got)
	}
	clk.Advance(20 * time.Second)
	if got := m.Remaining(); got != 0 {
		t.Fatalf("expired remaining = %v, want 0", got)
	}
}
