package dst

import (
	"fmt"
	"slices"
)

// sweepCheckers is the one thing that must hold of a rule that deletes files.
func sweepCheckers() []Checker { return []Checker{NewLiveLayerChecker()} }

// LiveLayerChecker: **no layer a guest is reading through is ever removed.**
//
// The rule under test decides from this host's own records — a layer file is removable
// when no volume here names it — and the records are exactly what a disk that lies about
// fsync corrupts. So a checker that recomputed the keep-set from those records would agree
// with the code for the same wrong reason and see nothing.
//
// It reads the scenario's own ledger instead. A Chain event is the simulated world stating
// what one volume's guest reads through, the way a Seal event states that QEMU has switched
// tips; a Sweep event is what the code actually removed. The two together answer the only
// question that matters here, and neither comes from the record.
//
// A layer that was live and is not any more is *not* protected: a volume that released its
// claim is precisely what makes its layers collectable, and a checker that never let go
// would forbid the reclamation this rule exists for. What it forbids is removing a file
// while some volume still reads through it.
type LiveLayerChecker struct {
	live      map[string][]string
	violation error
}

func NewLiveLayerChecker() *LiveLayerChecker {
	return &LiveLayerChecker{live: map[string][]string{}}
}

func (c *LiveLayerChecker) Name() string { return "no-live-layer-is-swept" }

func (c *LiveLayerChecker) Observe(e Event) {
	switch e.Kind {
	case EventChain:
		c.live[e.VolumeID] = slices.Clone(e.Layers)
	case EventSweep:
		if c.violation != nil {
			return
		}
		for volumeID, layers := range c.live {
			if slices.Contains(layers, e.LayerID) {
				c.violation = fmt.Errorf(
					"layer %s was removed while volume %s's guest reads through it (its chain is %v); a guest holding the open file goes on reading bytes nothing on this disk carries any more",
					e.LayerID, volumeID, layers)
				return
			}
		}
	}
}

func (c *LiveLayerChecker) Check() error { return c.violation }
