// Package compliant uses no forbidden primitives directly. It stands in for a
// production package that depends on the simio interfaces by injection. The
// analyzer must produce zero diagnostics here.
package compliant

import "time"

// Clock is the kind of interface production code depends on instead of the time
// package (mirrors internal/simio/clock).
type Clock interface {
	Monotonic() int64
}

func elapsed(c Clock) int64 {
	start := c.Monotonic()
	return c.Monotonic() - start
}

// Using a local identifier named like a forbidden call must NOT be flagged: the
// analyzer resolves types, it does not match on names.
type fakeStdlib struct{}

func (fakeStdlib) Now() int64 { return 0 }

func useLocal() int64 {
	var f fakeStdlib
	return f.Now()
}

// A *method* on a stdlib type is not the package-level function of the same name.
// (time.Time).After and .Before compare two instants that a simio clock already
// produced — no clock is read, nothing to simulate — where time.After(d) reads the
// clock and returns a channel and is forbidden (see violating.go). Flagging the method
// made every timestamp comparison a violation.
func compareInstants(a, b time.Time) bool {
	if a.After(b) {
		return true
	}
	return b.Before(a)
}
