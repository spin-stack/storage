package controlplane_test

import (
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/wal/format"
)

// ADR-0016 stage 1: the drain revokes the source's lease and refuses to renew it
// **for the duration of one volume's promotion**, and not a moment longer.
//
// The revocation on its own is what makes a healthy host evacuable — the drain will
// not promote a source whose lease is live, so on a host that is up and heartbeating
// the deadline keeps moving forward. But the lease is per host and the evacuation is
// per volume: once an Agent heartbeat exists, the same host renews it again seconds
// later (RenewHostLease refuses only a DEAD host) and the drain waits for ever.
//
// Refusing renewals for any DRAINING host is the fix wave 2 deliberately rejected: it
// stops the durable ACKs of every volume the host still holds, including the ones
// nobody is moving, which turns an orderly drain into an availability event for
// volumes that were fine. Stage 1 is the same mechanism with the blast radius cut to
// one promotion; the window is what has to be right.

// heartbeat is the source Agent doing what a healthy Agent does. It reports the
// Control Plane's answer so a test can say whether the renewal was refused.
func (w *drainWorld) heartbeat(t *testing.T, hostID string) error {
	t.Helper()
	return w.base.RenewHostLease(t.Context(), w.term, hostID, int(leaseTTL/time.Second))
}

// TestADrainIsNotWedgedByTheSourcesHeartbeat is the bug. The source is healthy and
// heartbeats between passes, exactly as it is supposed to. Every renewal re-arms the
// lease the drain revoked, the fencing wait measures from the new instant, and the
// evacuation never starts — a host that can never be drained, with nothing in the
// logs but a fencing wait that keeps not elapsing.
func TestADrainIsNotWedgedByTheSourcesHeartbeat(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)

	var (
		res      controlplane.DrainResult
		err      error
		refusals int
	)
	for range 24 {
		// The Agent heartbeats before every Control-Plane pass.
		if herr := w.heartbeat(t, cloneHostA); herr != nil {
			refusals++
		}
		res, err = w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
		if !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
			break
		}
		w.pastFencingWait()
	}
	if err != nil {
		t.Fatalf("a heartbeating source wedged the drain: %v", err)
	}
	if res.Phase != lifecycle.OpSucceeded {
		t.Fatalf("drain phase = %q, want SUCCEEDED", res.Phase)
	}
	if refusals == 0 {
		t.Fatal("no renewal was ever refused, so nothing stopped the source re-arming its lease")
	}
	for _, vol := range w.vols {
		v, _ := w.md.GetVolume(ctx, format.UUIDString(vol))
		if v.PrimaryHostID != destHost {
			t.Fatalf("volume %s did not move: %+v", format.UUIDString(vol), v)
		}
	}
}

// TestTheRevocationWindowIsBoundedToOnePromotion: the window exists so the source
// cannot re-arm the lease the drain just revoked, and for no other reason. While the
// drain is promoting a volume the source's renewals are refused; the moment that
// promotion is over — however it ended — they are accepted again.
//
// The closing half is the one that matters. A drain that dies with the window open
// leaves a host that cannot renew for any of its volumes, which is worse than the bug
// being fixed: the volumes nobody is moving stop being able to ACK a FLUSH, and
// nothing in the system will ever reopen it.
func TestTheRevocationWindowIsBoundedToOnePromotion(t *testing.T) {
	tests := []struct {
		name string
		// inside runs while the window is open, after the probe, to arrange the exit
		// path this case is about.
		inside func(t *testing.T, w *drainWorld)
		// drive runs the drain to that exit.
		drive func(t *testing.T, w *drainWorld)
	}{
		{
			name: "the promotion succeeds",
			drive: func(t *testing.T, w *drainWorld) {
				if _, err := w.reconcile(t, cloneHostA, drainOpID); err != nil {
					t.Fatalf("drain: %v", err)
				}
			},
		},
		{
			name: "the pass is cancelled",
			inside: func(t *testing.T, w *drainWorld) {
				if err := w.drainer.Cancel(t.Context(), w.term, drainOpID); err != nil {
					t.Errorf("cancel: %v", err)
				}
			},
			drive: func(t *testing.T, w *drainWorld) {
				// The outcome is not what this case is about — a cancellation landing
				// mid-move makes the progress write fail, which
				// TestDrainCancelBetweenTheMoveAndTheProgressWrite owns. What matters
				// here is that a pass which ends this way still closes its window.
				_, _ = w.reconcile(t, cloneHostA, drainOpID)
			},
		},
		{
			name: "the pass fails after the promotion",
			drive: func(t *testing.T, w *drainWorld) {
				// The epoch-boundary write fails: the pass dies inside the move, past
				// the point where the window was opened.
				w.faults.fail = failBoundaryWrite
				if _, err := w.reconcile(t, cloneHostA, drainOpID); !errors.Is(err, errBoundaryLost) {
					t.Fatalf("want errBoundaryLost, got %v", err)
				}
			},
		},
		{
			name: "the move is abandoned inside the window",
			inside: func(t *testing.T, w *drainWorld) {
				// The destination dies between the fence and the promotion, so the
				// move is abandoned rather than performed — with the window open.
				if err := w.base.SetHostState(t.Context(), w.term, destHost, lifecycle.HostDead); err != nil {
					t.Errorf("kill destination: %v", err)
				}
			},
			drive: func(t *testing.T, w *drainWorld) {
				if _, err := w.reconcile(t, cloneHostA, drainOpID); !errors.Is(err, controlplane.ErrDestinationHostUnusable) {
					t.Fatalf("want ErrDestinationHostUnusable, got %v", err)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newDrainWorld(t, 10*volSize)

			// The lease read of the host being fenced happens inside the window: it
			// is the drain revoking what this window keeps revoked.
			var (
				sawWindow bool
				inside    error
			)
			w.hooks.afterLease = func(hostID string) {
				if sawWindow || hostID != cloneHostA {
					return
				}
				sawWindow = true
				inside = w.heartbeat(t, cloneHostA)
				if tc.inside != nil {
					tc.inside(t, w)
				}
			}

			tc.drive(t, w)

			if !sawWindow {
				t.Fatal("the drain never reached a promotion, so this case tests nothing")
			}
			if !errors.Is(inside, metadata.ErrRenewalsBlocked) {
				t.Fatalf("the source renewed its lease during the promotion (%v); the drain's revocation is undone by the next heartbeat", inside)
			}

			// ...and once the promotion is over, by whatever path, it can again.
			w.clearFaults()
			if err := w.heartbeat(t, cloneHostA); err != nil {
				t.Fatalf("the window did not close: the source cannot renew after the pass ended: %v", err)
			}
		})
	}
}

// TestTheRevocationWindowExpiresOnItsOwn: the Control Plane dies with the window
// open. Nothing will run the closing write, so the window has to close itself —
// bounded by one lease_ttl + max_clock_skew, the length of the promotion it exists
// for. Without that, a crashed drain leaves the host unable to ACK for volumes nobody
// was moving, permanently, and the only cure is an operator noticing.
func TestTheRevocationWindowExpiresOnItsOwn(t *testing.T) {
	ctx := t.Context()
	w := newDrainWorld(t, 10*volSize)

	// A pass that opens the window and then dies before anything closes it. A panic,
	// not an error: a returned error lets the drain run its own tidy-up, which is
	// exactly the code a crash does not get to run.
	w.hooks.afterLease = func(hostID string) {
		if hostID == cloneHostA {
			panic(errProgressLost)
		}
	}
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("the pass was supposed to die with the window open")
			}
		}()
		_, _ = w.drainer.Drain(ctx, w.term, cloneHostA, drainOpID)
	}()
	w.hooks.afterLease = nil

	if err := w.heartbeat(t, cloneHostA); !errors.Is(err, metadata.ErrRenewalsBlocked) {
		t.Fatalf("the window was not open when the pass died: %v", err)
	}
	// One dwell later it is gone, with nobody having closed it.
	w.pastFencingWait()
	if err := w.heartbeat(t, cloneHostA); err != nil {
		t.Fatalf("a window nobody closed outlived the promotion it was opened for: %v", err)
	}
}

// TestTheWindowDoesNotStopTheDestinationRenewing: the window is aimed at the host
// being fenced. Blocking the destination would fence the writer the promotion just
// installed, which is the opposite of the point.
func TestTheWindowDoesNotStopTheDestinationRenewing(t *testing.T) {
	w := newDrainWorld(t, 10*volSize)
	var checked bool
	w.hooks.afterLease = func(hostID string) {
		if checked || hostID != cloneHostA {
			return
		}
		checked = true
		if err := w.heartbeat(t, destHost); err != nil {
			t.Errorf("the destination could not renew during the promotion: %v", err)
		}
	}
	if _, err := w.reconcile(t, cloneHostA, drainOpID); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if !checked {
		t.Fatal("the drain never reached a promotion, so this test checks nothing")
	}
}
