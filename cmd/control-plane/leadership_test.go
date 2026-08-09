package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/real"
)

// TestALiveControlPlaneKeepsItsStampFresh is the other half of the superseded case, and
// the one an operator sees first.
//
// control_plane_leader.renewed_at was written by the election and never touched again,
// so the column said when this process *started*, not whether it is still there: a
// Control Plane SIGTERMed twenty-five seconds ago and one serving right now produced the
// same line, and there was no write anywhere that could tell them apart. The assertion
// is on the stamp in the catalog — what anything reading the fleet can see — and not on
// the loop having called something.
func TestALiveControlPlaneKeepsItsStampFresh(t *testing.T) {
	ctx := t.Context()

	// A clock that advances one second per reading: deterministic, no wall clock, and
	// every renewal is therefore stamped strictly later than the one before it. If the
	// loop stops renewing, the stamp stops moving — which is exactly the state being
	// tested for.
	base := time.Unix(1_700_000_000, 0).UTC()
	var reads atomic.Int64
	md := metasim.New(func() time.Time { return base.Add(time.Duration(reads.Add(1)) * time.Second) })

	term, err := md.AcquireLeadership(ctx, "cp-a")
	if err != nil {
		t.Fatal(err)
	}
	elected, err := md.GetLeader(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// A real clock at a millisecond cadence: what is under test is the loop, whose unit
	// is "did it renew again", not how long it waited.
	clk := real.NewClock()
	serving, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- renewLeadership(serving, md, clk, term, "cp-a", time.Millisecond) }()

	// Bounded by a count of looks rather than by a deadline, so "it never renews" fails
	// here instead of as the package's test timeout — and so this reads no wall clock of
	// its own (INV-01). Two thousand looks a millisecond apart is two thousand of the
	// loop's own intervals.
	var fresh time.Time
	for look := 0; ; look++ {
		leader, gerr := md.GetLeader(ctx)
		if gerr != nil {
			t.Fatal(gerr)
		}
		if leader.RenewedAt.After(elected.RenewedAt) {
			fresh = leader.RenewedAt
			if leader.Term != elected.Term || leader.HolderID != elected.HolderID {
				t.Fatalf("the renewal made this process a new leader: %s/term %d -> %s/term %d",
					elected.HolderID, elected.Term, leader.HolderID, leader.Term)
			}
			break
		}
		if look == 2000 {
			t.Fatalf("the leader's stamp never moved past its election (%s): a process that is serving is indistinguishable from one that is gone",
				elected.RenewedAt)
		}
		if serr := clk.Sleep(ctx, time.Millisecond); serr != nil {
			t.Fatal(serr)
		}
	}

	stop()
	if err := <-done; err != nil {
		t.Fatalf("the renewal loop ended a Control Plane that still holds its term: %v", err)
	}

	// And the term it renewed under is still the one every admin one-shot reads and
	// writes under: a renewal that re-elected would have made the stamp fresh and broken
	// -flatten-volume and -delete-volume at random instead.
	after, err := md.GetLeader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The stamp is asserted as "at or after the one we saw", not as an exact instant.
	// The loop renews on its own goroutine, so between reading `fresh` and stop() taking
	// effect it may land one more renewal — which is the loop working, not a defect. An
	// equality here failed on CI at 22:13:23 wanting 22:13:22, which is a test that
	// measures scheduling rather than the property it names.
	if after.Term != term || after.HolderID != elected.HolderID || after.RenewedAt.Before(fresh) {
		t.Fatalf("after shutdown the leader is %s/term %d stamped %s, want %s/term %d stamped at or after %s",
			after.HolderID, after.Term, after.RenewedAt, elected.HolderID, term, fresh)
	}
}
