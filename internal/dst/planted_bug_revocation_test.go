package dst

import "testing"

// ADR-0016 stage 1: the drain refuses the source's lease renewals for the duration of
// one volume's promotion, and not for the length of the drain.
//
// This one has no invariant checker of its own: the property is a *bound on a cost*,
// not a safety rule, so what proves it is the scenario's own measurement. The control
// is the scenario as it ships; the plant is the fix wave 2 rejected — renewals refused
// for as long as the host is DRAINING — which is a real change to production
// behaviour (faultMD.RenewHostLease consults the fleet state) and which the scenario
// catches at the point where a drained host must be serving its other volumes again.
func TestPlantedBugRevocationWindowSpansTheWholeDrain(t *testing.T) {
	if res := Run(24, revocationWindow(false), NewPromotionWaitChecker()); res.Err != nil {
		t.Fatalf("the unplanted scenario must pass: %v\n--- trace ---\n%s", res.Err, res.TraceString())
	}
	res := Run(24, revocationWindow(true), NewPromotionWaitChecker())
	if res.Err == nil {
		t.Fatal("a window that lasts as long as the drain was not caught: the volumes nobody is moving lose their durable ACKs for the whole evacuation")
	}
}
