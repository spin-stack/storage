package dst

import (
	"strings"
	"testing"
)

// carrySeeds is wider than the mandatory set's six on purpose. The seed decides how many
// records the stranded session holds, which decides how many segments it occupies, which
// decides how many crash points there are and where the segment boundaries fall relative
// to them — so a run at one seed proves the carry survives one shape of interruption, and
// this proves it survives the shapes.
// this proves it survives the shapes. Thirty-two is where it stops buying anything: the
// arm count is a function of the record count, which the seed draws from three values, so
// beyond a handful of seeds the runs differ in payloads and key material and not in shape.
// It costs about two seconds under -race, which is the budget a mandatory lane has.
const carrySeeds = 32

func TestCarryForwardSurvivesEveryCrashPointAcrossManySeeds(t *testing.T) {
	for seed := int64(1); seed <= carrySeeds; seed++ {
		res := Run(seed, scenarioCarryForwardCrashPoints, DefaultCheckers()...)
		if res.Err != nil {
			t.Fatalf("seed=%d: %v\n--- trace ---\n%s", seed, res.Err, res.TraceString())
		}
	}
}

// The scenario is worthless if it stops visiting crash points, and nothing in its own
// assertions would say so: an arm that dies before the carry writes anything passes every
// one of them. So the shape of the run is asserted directly — that the host really died
// at a double-digit number of distinct points, and that both the plaintext and the
// encrypted volume were carried at each.
func TestTheCarryMatrixIsNotEmpty(t *testing.T) {
	res := Run(7, scenarioCarryForwardCrashPoints, NewCarriedRecordChecker())
	if res.Err != nil {
		t.Fatalf("seed=7: %v", res.Err)
	}
	deaths := map[string]int{}
	settled := 0
	for _, line := range res.Trace {
		switch {
		case strings.Contains(line, "the host died on write"):
			switch {
			case strings.Contains(line, "carry-plaintext"):
				deaths["plaintext"]++
			case strings.Contains(line, "carry-sealed"):
				deaths["sealed"]++
			}
		case strings.Contains(line, "carry survived") && strings.Contains(line, "settled=true"):
			settled++
		}
	}
	for _, tag := range []string{"plaintext", "sealed"} {
		if deaths[tag] < 10 {
			t.Fatalf("the %s carry was interrupted at %d point(s); the scenario is meant to die at every write it performs",
				tag, deaths[tag])
		}
	}
	if settled == 0 {
		t.Fatal("no settled observation reached the event stream: the checker had nothing to judge")
	}
}

// The three rules of CarriedRecordChecker, each shown to fire on the stream that violates
// it and only on that one.
//
// This is not a planted-bug proof and does not pretend to be one — those live in
// planted_bug_carry_test.go and go through production code. It is the narrower thing a
// checker with more than one rule needs: the lost-record rule is the one the planted
// defect reaches, and without this the other two could be broken by an edit here and no
// test in the tree would notice.
func TestCarriedRecordCheckerRules(t *testing.T) {
	const vol = "v"
	promise := func(seq uint64, digest string) Event {
		return Event{Kind: EventCarry, CarryPhase: CarryPromised, Key: vol, Epoch: 2, Sequence: seq, Digest: digest}
	}
	survived := func(scan, epoch, seq uint64, digest string, settled bool) Event {
		return Event{Kind: EventCarry, CarryPhase: CarrySurvived, Key: vol, Epoch: epoch,
			Sequence: seq, Digest: digest, Scan: scan, Settled: settled}
	}

	tests := []struct {
		name   string
		events []Event
		want   string // a substring of the violation, or "" for no violation
	}{{
		name: "a carry that moved both records intact",
		events: []Event{
			promise(1, "aa"), promise(2, "bb"),
			survived(1, 3, 1, "aa", true), survived(1, 3, 2, "bb", true),
		},
	}, {
		name: "a record the carry lost",
		events: []Event{
			promise(1, "aa"), promise(2, "bb"),
			survived(1, 3, 1, "aa", true),
		},
		want: "sequence 2 was ACKed as durable and is on no epoch",
	}, {
		name: "a record whose bytes changed crossing the epoch",
		events: []Event{
			promise(1, "aa"),
			survived(1, 3, 1, "unopenable", true),
		},
		want: "must not change as it crosses an epoch",
	}, {
		name: "one sequence twice under one (volume, epoch): a reused nonce",
		events: []Event{
			promise(1, "aa"),
			survived(1, 3, 1, "aa", true), survived(1, 3, 1, "aa", true),
		},
		want: "one nonce over two payloads",
	}, {
		name: "the same sequence under the source and the granted epoch, mid-carry",
		events: []Event{
			promise(1, "aa"),
			survived(1, 2, 1, "aa", false), survived(1, 3, 1, "aa", false),
			survived(2, 3, 1, "aa", true),
		},
	}, {
		name: "a promise nothing ever looked for",
		events: []Event{
			promise(1, "aa"),
		},
		want: "no settled observation of it was recorded",
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := Run(1, func(s *Sim) error {
				for _, e := range tc.events {
					s.Emit(e)
				}
				return nil
			}, NewCarriedRecordChecker())
			switch {
			case tc.want == "" && res.Err != nil:
				t.Fatalf("a legitimate stream was rejected: %v", res.Err)
			case tc.want == "":
			case res.Err == nil:
				t.Fatalf("the checker accepted a stream violating %q", tc.want)
			case !strings.Contains(res.Err.Error(), tc.want):
				t.Fatalf("the checker said %v, which does not name %q", res.Err, tc.want)
			}
		})
	}
}
