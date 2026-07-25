// Package clock is the simulable time interface (§25.1, INV-01). Production code
// depends on Clock instead of the time package so that simulation can drive time
// deterministically. The monotonic source (Now) is the security-critical one: the
// durable-ACK rule (§12.2) is evaluated against it. The wall source (Wall) is only
// used for promotion-skew reasoning (§12.1) and may be perturbed independently in
// simulation.
package clock

import (
	"context"
	"time"
)

// Instant is a monotonic instant expressed as nanoseconds since a Clock's origin.
// Only differences between Instants from the same Clock are meaningful.
type Instant int64

// Add returns the instant d after i.
func (i Instant) Add(d time.Duration) Instant { return i + Instant(d) }

// Sub returns the duration elapsed from o to i.
func (i Instant) Sub(o Instant) time.Duration { return time.Duration(i - o) }

// Clock abstracts monotonic and wall time plus timers.
type Clock interface {
	// Now returns the current monotonic instant. Never decreases.
	Now() Instant
	// Wall returns the current wall-clock time (may be skewed in simulation).
	Wall() time.Time
	// NewTimer returns a one-shot timer that fires after d.
	NewTimer(d time.Duration) Timer
	// Sleep blocks until d has elapsed on this clock or ctx is done.
	Sleep(ctx context.Context, d time.Duration) error
}

// Timer is a one-shot timer.
type Timer interface {
	// C is the channel on which the firing instant is delivered.
	C() <-chan Instant
	// Stop prevents the timer from firing; it reports whether it did so before firing.
	Stop() bool
}
