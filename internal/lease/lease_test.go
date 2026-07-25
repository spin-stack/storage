package lease_test

import (
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/simio/sim"
)

func newManager(t *testing.T) (*lease.Manager, *sim.Clock) {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	return lease.NewManager(clk, 10*time.Second), clk
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
			m.Renew()
			clk.Advance(9 * time.Second) // 18s since grant, but only 9s since renew
		}, true},
		{"revoke invalidates", func(m *lease.Manager, _ *sim.Clock) { m.Grant(); m.Revoke() }, false},
		{"renew without grant stays invalid", func(m *lease.Manager, _ *sim.Clock) { m.Renew() }, false},
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
