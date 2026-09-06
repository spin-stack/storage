package dst_test

import (
	"sort"
	"testing"

	"github.com/spin-stack/storage/internal/dst"
)

// pinnedMandatorySet is the §25.1 gate, written out by name.
//
// MandatoryScenarios() is assembled at run time from the per-area functions
// (coreScenarios, harnessScenarios) so that two increments can add a scenario without
// both editing one literal. That is the right trade for adding, and it is exactly the wrong shape for
// noticing a *removal*: TestMandatoryScenarios ranges over whatever they return, so deleting an
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
//
//   - 2026-08-22, twenty of the twenty-two, with the local block engine. QEMU manages
//     the local copy-on-write format through qcow2 from here on, and what this system
//     keeps is immutable commits, publication and recovery — so every scenario whose
//     subject was a write-ahead log, a virtio device, a chunked image or the volume
//     manager over them went with that subject in one commit:
//
//     crash-around-fdatasync, wal-write-path-no-put, wal-backpressure,
//     encrypted-wal-no-plaintext-leak, torn-append-leaves-nothing-behind,
//     wal-segments-survive-a-crash-at-every-boundary,
//     guest-device-acks-durability-only-on-flush,
//     carry-forward-survives-a-crash-at-every-point,
//     disk-fills-under-sustained-write-with-s3-down (internal/wal),
//     fenced-volume-stops-serving, a-stopped-volume-comes-back-from-its-image,
//     a-clone-reads-through-its-parent, a-clone-of-a-clone-reads-its-grandparents-bytes,
//     a-snapshot-of-a-live-volume-is-frozen, two-hosts-cannot-both-publish-an-image,
//     a-rebuilt-catalog-can-serve-its-volumes, a-volume-stopped-mid-fetch-still-publishes,
//     device-budget-holds-across-volumes (internal/agent + internal/image),
//     a-refused-volume-has-no-socket-and-is-still-reported (internal/blockdev).
//
//     two-hosts-cannot-both-publish-an-image is the one worth naming twice: it was the
//     DST arm of the mutual-exclusion review zone. The primitive it exercised — the
//     object store's compare-and-set — is untouched and is still proven, by
//     internal/simio/objectstore/storetest, which `task backend:conformance` runs
//     blocking per backend. What went is the *image* publish it drove that CAS through.
//     The new commit protocol's HEAD compare-and-swap gets an arm of this shape back,
//     and it is a review-zone change when it does.
var pinnedMandatorySet = []string{
	"no-live-layer-is-swept",
	"two-hosts-cannot-both-publish",
	// core (scenarios.go)
	"clock-drift-beyond-skew",
	"network-partition",
	// reconcile (scenarios_reconcile.go)
	"no-sealed-layer-is-chained-past",
	// fencing (scenarios_fencing.go)
	"an-isolated-host-keeps-its-guest",
	// isolation (scenarios_isolation.go)
	"an-unconfirmed-host-pauses-its-guest",
	// harness (scenarios_harness.go)
	"restored-control-plane",
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
