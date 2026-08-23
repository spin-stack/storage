package dst

import "fmt"

// commitCheckers are the invariants v6 §9's publish protocol restores to this package.
//
// One, and the count is honest rather than a target. checkers.go's note said three
// checkers would come back with the commit protocol — effective-single-writer, HEAD
// moving only forward, and nothing leaving the host in cleartext. The first two are the
// same invariant seen twice: a HEAD that only ever moves to a commit whose parent it
// already held *is* effective single-writer, and splitting them would be two checkers
// reading one fact.
//
// The third is not here, and the reason is this package's own rule. A checker needs a
// planted bug that breaks production behaviour, and there is no way to make this system
// upload cleartext short of editing the sealing call itself — which planted_bug_test.go
// forbids, because a fault injected into the code under test proves the test can read
// the code, not that the code is right. Sealing is instead asserted where a fault can
// reach it: internal/commit's round trip, which shows the object in the bucket is not
// the plaintext, and internal/crypto's bit-flip and truncation properties.
func commitCheckers() []Checker {
	return []Checker{NewSingleWriterChecker()}
}

// SingleWriterChecker enforces INV-10: the published history is a chain, never a fork.
//
// It watches every publish that claimed success and requires the commit's parent to be
// the commit HEAD already held. The first one may have no parent, and exactly one may:
// a second "first commit" is two hosts that both read an empty HEAD and both won, which
// is the case create-only exists to stop.
//
// What it catches that an assertion inside a scenario cannot: the scenario asserts on
// the error the losing host got, and a backend whose preconditions are advisory hands
// that host a success. There is then no error anywhere to assert on — the only evidence
// is the shape of the history, which is what this reads.
type SingleWriterChecker struct {
	head      string
	published bool
	violation error
}

// NewSingleWriterChecker returns a fresh checker.
func NewSingleWriterChecker() *SingleWriterChecker { return &SingleWriterChecker{} }

func (c *SingleWriterChecker) Name() string { return "effective-single-writer" }

func (c *SingleWriterChecker) Observe(e Event) {
	if e.Kind != EventPublish || !e.OK || c.violation != nil {
		return
	}
	switch {
	case !c.published && e.ParentCommitID != "":
		c.violation = fmt.Errorf("step %d: %s published commit %s onto parent %s, and no commit had been published",
			e.Step, e.Host, e.CommitID, e.ParentCommitID)
	case c.published && e.ParentCommitID != c.head:
		// The fork. Two commits with one parent is two hosts' worth of a guest's writes,
		// each claiming to be the volume, and whichever HEAD ends up naming, the other
		// host's writes are gone with nothing having reported an error.
		c.violation = fmt.Errorf("step %d: %s published commit %s onto parent %q while HEAD held %q — the history forked",
			e.Step, e.Host, e.CommitID, e.ParentCommitID, c.head)
	default:
		c.head, c.published = e.CommitID, true
	}
}

func (c *SingleWriterChecker) Check() error { return c.violation }
