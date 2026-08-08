package dst_test

import (
	"sort"
	"testing"

	"github.com/spin-stack/storage/internal/dst"
)

// pinnedMandatorySet is the §25.1 gate, written out by name.
//
// MandatoryScenarios() is assembled at run time from four per-area functions
// (coreScenarios, harnessScenarios, walScenarios, agentScenarios) so that two
// increments can add a scenario without both editing one literal. That is the right
// trade for adding, and it is exactly the wrong shape for noticing a *removal*:
// TestMandatoryScenarios ranges over whatever the four functions return, so deleting an
// entry — or dropping an `append` line in a refactor — makes the gate run one fewer
// proof and stay green. Nothing outside this file would say a scenario had left.
//
// Pinned by NAME, not by count. A count is one integer that every branch adding a
// scenario has to bump, which turns a safety rail into a per-branch merge conflict and
// teaches everyone to edit it without reading it; worse, it cannot distinguish "one
// added, one removed" from "nothing happened". Names collide only when two branches
// touch the same scenario, which is when a human should be looking anyway.
//
// Adding a scenario: add its name here, in the same commit. Removing one: delete the
// name and add a dated line to the log below saying what happened to its subject —
// the same discipline TestPlantedBugCoverageIsNotSilentlyWeakened applies to its
// `wantBehavioural` constant, and for the same reason. A removal is the one thing this
// test cannot tell apart from the erosion it exists to catch, so the record, not the
// list, is what a reviewer reads.
//
// Removals:
//   - (none yet — the set has only grown since §25.1 was first implemented)
var pinnedMandatorySet = []string{
	// core (scenarios.go)
	"crash-around-fdatasync",
	"clock-drift-beyond-skew",
	"network-partition",
	"wal-write-path-no-put",
	"wal-backpressure",
	"encrypted-wal-no-plaintext-leak",
	"torn-append-leaves-nothing-behind",
	// harness (scenarios_harness.go)
	"disk-fills-under-sustained-write-with-s3-down",
	"restored-control-plane",
	// wal (scenarios_wal.go)
	"wal-segments-survive-a-crash-at-every-boundary",
	"guest-device-acks-durability-only-on-flush",
	// agent (scenarios_agent.go)
	"fenced-volume-stops-serving",
	"a-stopped-volume-comes-back-from-its-image",
	"a-clone-reads-through-its-parent",
	"a-clone-of-a-clone-reads-its-grandparents-bytes",
	"a-snapshot-of-a-live-volume-is-frozen",
	"two-hosts-cannot-both-publish-an-image",
	"a-rebuilt-catalog-can-serve-its-volumes",
	"a-volume-stopped-mid-fetch-still-publishes",
	"device-budget-holds-across-volumes",
}

// TestMandatorySetIsPinnedByName fails when MandatoryScenarios() and the pinned list
// disagree in either direction. Order is deliberately not asserted: a scenario moving
// between the per-area files changes nothing about what the gate proves.
func TestMandatorySetIsPinnedByName(t *testing.T) {
	got := map[string]int{}
	for _, sc := range dst.MandatoryScenarios() {
		got[sc.Name]++
	}

	// A duplicate name is not a naming nit here: TestMandatoryScenarios calls
	// t.Run(sc.Name, ...), so two scenarios sharing a name are reported as one subtest
	// and its "#01" sibling — a failure in either reads as a failure in the other.
	var dupes []string
	for name, n := range got {
		if n > 1 {
			dupes = append(dupes, name)
		}
	}
	if len(dupes) > 0 {
		sort.Strings(dupes)
		t.Errorf("MandatoryScenarios() returns duplicate names %v: subtest names must identify a scenario", dupes)
	}

	want := map[string]bool{}
	for _, name := range pinnedMandatorySet {
		want[name] = true
	}

	var added, removed []string
	for name := range got {
		if !want[name] {
			added = append(added, name)
		}
	}
	for name := range want {
		if got[name] == 0 {
			removed = append(removed, name)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)

	if len(added) > 0 {
		t.Errorf("MandatoryScenarios() has %v, not in pinnedMandatorySet: add the name in the same commit as the scenario", added)
	}
	if len(removed) > 0 {
		t.Errorf("pinnedMandatorySet has %v, no longer in MandatoryScenarios(): if the scenario really is gone, "+
			"delete the name and record what happened to its subject in the Removals log", removed)
	}
}
