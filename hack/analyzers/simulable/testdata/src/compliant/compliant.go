// Package compliant uses no forbidden primitives directly. It stands in for a
// production package that depends on the simio interfaces by injection. The
// analyzer must produce zero diagnostics here.
package compliant

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
