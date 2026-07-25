// Package lease implements the Agent-side host lease on the monotonic clock (§12.2,
// §12.6). One lease per host covers every volume attached to it; the durable-ACK
// rule (a FLUSH/FUA is ACKed only while the lease is valid) is evaluated against
// this manager's monotonic view — never wall time — which is what makes fencing of
// a stale writer correct regardless of clock skew (§12.1).
//
// Two properties make the Agent's validity window a *subset* of the window the
// Control Plane fences against, which is what INV-06/INV-09 rest on:
//
//   - a grant or renewal is anchored to the instant the request left the host
//     (GrantAt/RenewAt take it), never to the instant the answer came back. The CP
//     stamps last_renewal when the request arrives — at or after that instant — so
//     the Agent can only ever be valid for less than cp_last_renewal + ttl. Anchoring
//     to the response instead lets a slow round trip (GC pause, PG failover retry,
//     healing partition) push the Agent past the CP's fencing deadline;
//   - a response that was overtaken by a Revoke is ignored. Revoke bumps a
//     generation; a grant/renewal applied under an older generation is refused, so an
//     answer that was in flight when the Agent accepted a fence cannot re-arm it.
package lease

import (
	"sync"
	"time"

	"github.com/spin-stack/storage/internal/simio/clock"
)

// Manager tracks a single host lease. It is safe for concurrent use: the data path
// calls Valid on every durable ACK while the heartbeat calls RenewAt.
type Manager struct {
	clk clock.Clock
	ttl time.Duration

	mu      sync.Mutex
	t0      clock.Instant // monotonic instant at which the last accepted grant/renewal was *requested*
	granted bool
	gen     uint64 // bumped by Revoke; identifies the current lease incarnation
}

// NewManager returns a lease manager timed by clk with the given TTL.
func NewManager(clk clock.Clock, ttl time.Duration) *Manager {
	return &Manager{clk: clk, ttl: ttl}
}

// Generation returns the token identifying the current lease incarnation. An Agent
// reads it *before* sending a grant/renewal request and passes it back to
// GrantAt/RenewAt: if a Revoke happened in between, the response is ignored.
func (m *Manager) Generation() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gen
}

// Grant records a lease grant that was requested at this instant — an attach, or a
// test. An Agent applying a Control-Plane response must use GrantAt with the instant
// the request was sent, because the answer may be arbitrarily late.
func (m *Manager) Grant() { m.GrantAt(m.Generation(), m.clk.Now()) }

// GrantAt records a lease grant requested at sentAt under generation gen (§12.2
// step 1), and reports whether it was accepted. It is refused when the generation is
// stale (a Revoke overtook it), when sentAt is in the future, or when the request is
// already older than the TTL — such a grant would arm a lease the Control Plane has
// already counted as expired.
func (m *Manager) GrantAt(gen uint64, sentAt clock.Instant) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.acceptable(gen, sentAt) {
		return false
	}
	m.t0 = sentAt
	m.granted = true
	return true
}

// RenewAt records a heartbeat renewal requested at sentAt under generation gen
// (§12.2 step 2), and reports whether it was accepted. Beyond the checks GrantAt
// makes it refuses to resurrect a lease that already expired — by then the Control
// Plane's fencing deadline may have passed and another host may hold the volume —
// and refuses a response that arrived out of order, so t0 never moves backwards.
func (m *Manager) RenewAt(gen uint64, sentAt clock.Instant) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.granted || !m.acceptable(gen, sentAt) {
		return false
	}
	if m.expiredLocked() || sentAt < m.t0 {
		return false
	}
	m.t0 = sentAt
	return true
}

// acceptable holds the checks common to a grant and a renewal. Caller holds mu.
func (m *Manager) acceptable(gen uint64, sentAt clock.Instant) bool {
	if gen != m.gen {
		return false // a Revoke overtook this response (§12.2: an accepted fence is final)
	}
	now := m.clk.Now()
	if sentAt > now {
		return false // a stamp from the future can only shorten fencing; never trust it
	}
	return now.Sub(sentAt) < m.ttl
}

// expiredLocked reports whether the current lease has already lapsed. Caller holds mu.
func (m *Manager) expiredLocked() bool { return m.clk.Now().Sub(m.t0) >= m.ttl }

// Valid reports whether the lease is still valid per the monotonic clock:
// granted and now - t0 < ttl (§12.2 step 3). This is a single timestamp
// comparison — no round-trips — as the durable-ACK rule requires.
func (m *Manager) Valid() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.granted && !m.expiredLocked()
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

// Revoke drops the lease (on detach or an accepted fence) and starts a new
// generation, so any grant or renewal still in flight is ignored when it lands.
// Valid returns false until the next Grant.
func (m *Manager) Revoke() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.granted = false
	m.gen++
}
