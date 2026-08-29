package dst

import "fmt"

// reconcileCheckers is v6 §11's ordering invariant: a sealed layer is published before
// any layer sealed after it. One checker, and it reads the one part of the reconciler a
// simulation can reach — see scenarios_reconcile.go for the part it cannot.
func reconcileCheckers() []Checker {
	return []Checker{NewSealOrderChecker()}
}

// SealOrderChecker enforces that the published history has no hole: when a commit lands
// for layer L, every layer sealed before L is already in the history.
//
// A hole is silent by construction. Each commit names only its parent, so a chain that
// skipped a layer reads back as a well-formed chain of the right depth, every manifest
// digest correct, every object present — and the guest writes that were in the skipped
// layer are gone. Nothing errors, at publish time or at recovery time; the only evidence
// is the order the layers left the host in against the order they were sealed in, which
// is what this reads.
//
// A publish naming a layer no seal event named is not this checker's subject and is
// skipped: `two-hosts-cannot-both-publish` publishes layers that were never rotated into
// being, and publishing a layer that is *not* sealed — the tip, with a guest still
// writing into it — is asserted through the Manager in internal/qcow's reconcile
// adversary lane, where the publisher can see which file it was handed.
type SealOrderChecker struct {
	sealed    []string
	published map[string]bool
	violation error
}

// NewSealOrderChecker returns a fresh checker.
func NewSealOrderChecker() *SealOrderChecker {
	return &SealOrderChecker{published: map[string]bool{}}
}

func (c *SealOrderChecker) Name() string { return "sealed-layers-publish-oldest-first" }

func (c *SealOrderChecker) Observe(e Event) {
	if c.violation != nil {
		return
	}
	switch {
	case e.Kind == EventSeal:
		c.sealed = append(c.sealed, e.LayerID)
	case e.Kind == EventPublish && e.OK:
		c.observePublish(e)
	}
}

func (c *SealOrderChecker) observePublish(e Event) {
	idx := -1
	for i, id := range c.sealed {
		if id == e.LayerID {
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}
	for _, older := range c.sealed[:idx] {
		if !c.published[older] {
			c.violation = fmt.Errorf("step %d: commit %s published layer %s while layer %s, sealed before it, is in no commit — the history skipped a layer and nothing failed",
				e.Step, e.CommitID, e.LayerID, older)
			return
		}
	}
	c.published[e.LayerID] = true
}

func (c *SealOrderChecker) Check() error { return c.violation }
