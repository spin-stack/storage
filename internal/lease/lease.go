// Package lease implements the Agent-side host lease on the monotonic clock (§12.2,
// §12.6). One lease per host covers every volume attached to it; the durable-ACK
// rule (a FLUSH/FUA is ACKed only while the lease is valid) is evaluated against
// this manager's monotonic view — never wall time — which is what makes fencing of
// a stale writer correct regardless of clock skew (§12.1).
package lease

import (
	"sync"
	"time"

	"github.com/spin-stack/storage/internal/simio/clock"
)

// Manager tracks a single host lease. It is safe for concurrent use: the data path
// calls Valid on every durable ACK while the heartbeat calls Renew.
type Manager struct {
	clk clock.Clock
	ttl time.Duration

	mu      sync.Mutex
	t0      clock.Instant // monotonic instant of the last accepted grant/renewal
	granted bool
}

// NewManager returns a lease manager timed by clk with the given TTL.
func NewManager(clk clock.Clock, ttl time.Duration) *Manager {
	return &Manager{clk: clk, ttl: ttl}
}

// Grant records a fresh lease grant, stamping t0 = monotonic now (§12.2 step 1).
func (m *Manager) Grant() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.t0 = m.clk.Now()
	m.granted = true
}

// Renew records a successful heartbeat renewal, advancing t0 (§12.2 step 2).
func (m *Manager) Renew() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.granted {
		m.t0 = m.clk.Now()
	}
}

// Valid reports whether the lease is still valid per the monotonic clock:
// granted and now - t0 < ttl (§12.2 step 3). This is a single timestamp
// comparison — no round-trips — as the durable-ACK rule requires.
func (m *Manager) Valid() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.granted && m.clk.Now().Sub(m.t0) < m.ttl
}

// Remaining reports the time left on the lease (<= 0 if invalid/expired).
func (m *Manager) Remaining() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.granted {
		return 0
	}
	rem := m.ttl - m.clk.Now().Sub(m.t0)
	if rem < 0 {
		return 0
	}
	return rem
}

// Revoke drops the lease (on detach or an accepted fence). Valid returns false
// until the next Grant.
func (m *Manager) Revoke() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.granted = false
}
