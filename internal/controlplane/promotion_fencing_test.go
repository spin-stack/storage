package controlplane_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// The fencing wait is the only thing standing between a promoted writer and the
// writer it replaces (§12.3, INV-11). These tests drive the cases where the
// promoter's information about the host it is fencing is missing, stale, or belongs
// to a different host than the one that is actually serving the volume.

const (
	fenceVol   = "00000000-0000-7000-8000-0000000000e1"
	fenceHostA = "00000000-0000-7000-8000-0000000000e2" // the source
	fenceHostB = "00000000-0000-7000-8000-0000000000e3"
	fenceHostC = "00000000-0000-7000-8000-0000000000e4"

	fenceTTL  = 10 * time.Second
	fenceSkew = 2 * time.Second
)

type fenceWorld struct {
	md     metadata.Store
	epochs *epoch.Store
	clk    *sim.Clock // the Control Plane's wall clock
	dbClk  *sim.Clock // the store's clock: what stamps last_renewal (§12.1)
	p      *controlplane.Promoter
	term   int64
}

// advance moves both clocks together. They are separate objects so that a test can
// skew one against the other — the case where the CP's container clock jumps and the
// database's does not — but by default they agree.
func (w *fenceWorld) advance(d time.Duration) {
	w.clk.Advance(d)
	w.dbClk.Advance(d)
}

// newFenceWorld builds a volume on fenceHostA at epoch 1. No lease row exists yet:
// each test states what the CP knows about the source.
func newFenceWorld(t *testing.T) *fenceWorld {
	t.Helper()
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	dbClk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	md := metasim.New(dbClk.Wall)
	epochs := epoch.NewStore(sim.NewObjectStore())

	term, err := md.AcquireLeadership(ctx, "cp")
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range []string{fenceHostA, fenceHostB, fenceHostC} {
		if err := md.UpsertHost(ctx, term, metadata.Host{HostID: h, State: lifecycle.HostActive}); err != nil {
			t.Fatal(err)
		}
	}
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: fenceVol, SizeBytes: 1 << 30, BlockSize: 65536,
		State: lifecycle.VolumeActive, CurrentEpoch: 1, PrimaryHostID: fenceHostA,
		DEKWrapped: []byte{1}, KEKID: "k",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := epochs.Init(ctx, fenceVol, 1); err != nil {
		t.Fatal(err)
	}
	return &fenceWorld{
		md: md, epochs: epochs, clk: clk, dbClk: dbClk, term: term,
		p: controlplane.NewPromoter(md, epochs, clk, fenceTTL, fenceSkew),
	}
}

func (w *fenceWorld) state(t *testing.T) lifecycle.VolumeState {
	t.Helper()
	v, err := w.md.GetVolume(t.Context(), fenceVol)
	if err != nil {
		t.Fatal(err)
	}
	return v.State
}

func (w *fenceWorld) unchanged(t *testing.T, wantEpoch int64, wantPrimary string) {
	t.Helper()
	ctx := t.Context()
	v, err := w.md.GetVolume(ctx, fenceVol)
	if err != nil {
		t.Fatal(err)
	}
	if v.CurrentEpoch != wantEpoch || v.PrimaryHostID != wantPrimary {
		t.Fatalf("volume = epoch %d on %s, want epoch %d on %s", v.CurrentEpoch, v.PrimaryHostID, wantEpoch, wantPrimary)
	}
	ep, _, err := w.epochs.Current(ctx, fenceVol)
	if err != nil || ep != uint64(wantEpoch) {
		t.Fatalf("epoch object = %d err=%v, want %d", ep, err, wantEpoch)
	}
}

// TestPromoteRefusesWhenTheSourceLeaseRecordIsMissing: after a PITR restore of the
// CP database (or an operator cleaning up host_leases) the source is still alive and
// its Agent-side lease is valid on its own monotonic clock for up to lease_ttl. A
// caller with no information about the source hands the promoter the zero instant.
// Treating "I know nothing" as "renewed at the epoch" makes FENCING_WAIT zero — two
// writers, at the moment the CP's view of the fleet is least trustworthy.
func TestPromoteRefusesWhenTheSourceLeaseRecordIsMissing(t *testing.T) {
	ctx := t.Context()
	w := newFenceWorld(t)
	w.advance(time.Hour) // however long the CP has been up, it never observed the lease

	_, err := w.p.Promote(ctx, w.term, fenceVol, time.Time{}, fenceHostB)
	if !errors.Is(err, controlplane.ErrSourceLeaseUnknown) {
		t.Fatalf("promote with no lease record: err = %v, want ErrSourceLeaseUnknown", err)
	}
	w.unchanged(t, 1, fenceHostA)
}

// TestPromoteOfAnObservedDeadSourceProceedsWithoutALeaseRow: the escape hatch. Once
// the fleet has recorded the host as DEAD the CP is asserting the writer is gone, and
// a volume must not be strandable by a missing lease row.
func TestPromoteOfAnObservedDeadSourceProceedsWithoutALeaseRow(t *testing.T) {
	ctx := t.Context()
	w := newFenceWorld(t)
	if err := w.md.SetHostState(ctx, w.term, fenceHostA, lifecycle.HostDead); err != nil {
		t.Fatal(err)
	}
	w.advance(time.Hour)

	got, err := w.p.Promote(ctx, w.term, fenceVol, time.Time{}, fenceHostB)
	if err != nil {
		t.Fatalf("promoting away from an observed-dead host: %v", err)
	}
	if got != 2 {
		t.Fatalf("new epoch = %d, want 2", got)
	}
}

// TestPromoteMeasuresTheWaitAgainstTheFencedHostsOwnLease: the caller passes an
// instant it read at some earlier point (or from the wrong host). The promoter must
// take the most conservative view — including the lease it can read itself for the
// host that is actually primary — never the caller's word alone.
func TestPromoteMeasuresTheWaitAgainstTheFencedHostsOwnLease(t *testing.T) {
	ctx := t.Context()
	w := newFenceWorld(t)

	stale := w.clk.Wall() // what the caller observed at T0
	w.advance(fenceTTL + fenceSkew + time.Second)
	// ... but the source renewed its lease in the meantime: it is alive and ACKing.
	if err := w.md.RenewHostLease(ctx, w.term, fenceHostA, int(fenceTTL/time.Second)); err != nil {
		t.Fatal(err)
	}

	if _, err := w.p.Promote(ctx, w.term, fenceVol, stale, fenceHostB); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("promote against a freshly renewed source lease: err = %v, want ErrFencingWaitNotElapsed", err)
	}
	w.unchanged(t, 1, fenceHostA)

	// Once the source's own lease can no longer be valid, the promotion goes ahead.
	w.advance(fenceTTL + fenceSkew + time.Second)
	if _, err := w.p.Promote(ctx, w.term, fenceVol, stale, fenceHostB); err != nil {
		t.Fatalf("promote after the source's lease expired: %v", err)
	}
}

// TestPromoteRefusesWhenTheVolumeMovedOnSinceTheCommandWasIssued: two drain passes (or
// two CP goroutines) both saw the volume on hostA. The first promotes it to hostB,
// which starts recovering and ACKing. The second must not then fence hostB with a
// zero wait just because the instant it carries belongs to hostA.
func TestPromoteRefusesWhenTheVolumeMovedOnSinceTheCommandWasIssued(t *testing.T) {
	ctx := t.Context()
	w := newFenceWorld(t)

	if err := w.md.RenewHostLease(ctx, w.term, fenceHostA, int(fenceTTL/time.Second)); err != nil {
		t.Fatal(err)
	}
	observed := w.clk.Wall()
	w.advance(fenceTTL + fenceSkew + time.Second)

	first, err := w.p.Promote(ctx, w.term, fenceVol, observed, fenceHostB)
	if err != nil {
		t.Fatal(err)
	}

	// The overtaken pass, still carrying hostA's instant, now aims at hostC.
	_, err = w.p.Promote(ctx, w.term, fenceVol, observed, fenceHostC)
	if err == nil {
		t.Fatal("an overtaken promotion fenced the writer that was just promoted, with no wait at all")
	}
	if !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("overtaken promotion: err = %v, want ErrFencingWaitNotElapsed", err)
	}
	w.unchanged(t, int64(first), fenceHostB)
}

// TestPromoteHonoursALeaseTTLLongerThanTheConfiguredOne: the TTL that matters is the
// one the *Agent* is counting down, which is what the lease row records. A CP
// configured with a shorter TTL than the lease it granted must not shorten the wait.
func TestPromoteHonoursALeaseTTLLongerThanTheConfiguredOne(t *testing.T) {
	ctx := t.Context()
	w := newFenceWorld(t)

	const granted = 60 * time.Second
	if err := w.md.RenewHostLease(ctx, w.term, fenceHostA, int(granted/time.Second)); err != nil {
		t.Fatal(err)
	}
	renewedAt := w.clk.Wall()

	w.advance(fenceTTL + fenceSkew + time.Second) // past the *configured* deadline only
	if _, err := w.p.Promote(ctx, w.term, fenceVol, renewedAt, fenceHostB); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("promote inside the granted 60s lease: err = %v, want ErrFencingWaitNotElapsed", err)
	}
	w.unchanged(t, 1, fenceHostA)

	w.advance(granted + fenceSkew)
	if _, err := w.p.Promote(ctx, w.term, fenceVol, renewedAt, fenceHostB); err != nil {
		t.Fatalf("promote after the granted lease expired: %v", err)
	}
}

// TestPromoteRefusesAHostThatCannotTakeTheVolume: the lease grant is the last of the
// three writes, and in PostgreSQL it has a foreign key. Discovering there that the
// host is unknown leaves the old writer fenced and nobody holding a lease — the
// volume cannot be ACKed for until a human notices. The simulated store has no such
// key, so without this check the two implementations disagree about what happens.
func TestPromoteRefusesAHostThatCannotTakeTheVolume(t *testing.T) {
	tests := []struct {
		name string
		host string
		prep func(t *testing.T, w *fenceWorld)
	}{
		{"unknown host", "00000000-0000-7000-8000-0000000000ee", nil},
		{"dead host", fenceHostB, func(t *testing.T, w *fenceWorld) {
			if err := w.md.SetHostState(t.Context(), w.term, fenceHostB, lifecycle.HostDead); err != nil {
				t.Fatal(err)
			}
		}},
		{"cordoned host", fenceHostB, func(t *testing.T, w *fenceWorld) {
			if err := w.md.SetHostState(t.Context(), w.term, fenceHostB, lifecycle.HostCordoned); err != nil {
				t.Fatal(err)
			}
		}},
		{"draining host", fenceHostB, func(t *testing.T, w *fenceWorld) {
			if err := w.md.SetHostState(t.Context(), w.term, fenceHostB, lifecycle.HostDraining); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			w := newFenceWorld(t)
			if err := w.md.RenewHostLease(ctx, w.term, fenceHostA, int(fenceTTL/time.Second)); err != nil {
				t.Fatal(err)
			}
			renewedAt := w.clk.Wall()
			w.advance(fenceTTL + fenceSkew + time.Second)
			if tc.prep != nil {
				tc.prep(t, w)
			}

			_, err := w.p.Promote(ctx, w.term, fenceVol, renewedAt, tc.host)
			if !errors.Is(err, controlplane.ErrDestinationHostUnusable) {
				t.Fatalf("promote to a %s: err = %v, want ErrDestinationHostUnusable", tc.name, err)
			}
			// Nothing may have moved: the old writer is still the one that can ACK.
			w.unchanged(t, 1, fenceHostA)
			if _, err := w.md.GetHostLease(ctx, tc.host); err == nil {
				t.Fatal("a lease was granted to a host that cannot take the volume")
			}
		})
	}
}

// unreadableLeases is a metadata store whose lease read is down — a PgBouncer
// hiccup, a failover, a timeout. "I could not read it" must not collapse into
// "there is none".
type unreadableLeases struct {
	metadata.Store
	err error
}

func (s unreadableLeases) GetHostLease(context.Context, string) (metadata.HostLease, error) {
	return metadata.HostLease{}, s.err
}

func TestPromoteRefusesWhenTheSourceLeaseCannotBeRead(t *testing.T) {
	ctx := t.Context()
	w := newFenceWorld(t)
	if err := w.md.RenewHostLease(ctx, w.term, fenceHostA, int(fenceTTL/time.Second)); err != nil {
		t.Fatal(err)
	}
	renewedAt := w.clk.Wall()
	w.advance(fenceTTL + fenceSkew + time.Second)

	down := errors.New("metadata unavailable")
	p := controlplane.NewPromoter(unreadableLeases{Store: w.md, err: down}, w.epochs, w.clk, fenceTTL, fenceSkew)
	if _, err := p.Promote(ctx, w.term, fenceVol, renewedAt, fenceHostB); !errors.Is(err, down) {
		t.Fatalf("promote with an unreadable lease: err = %v, want the read error", err)
	}
	w.unchanged(t, 1, fenceHostA)
}

// TestPromoteOfAVolumeWithNoPrimaryNeedsNoWait: nothing is serving it, so there is
// nobody to fence. This is the state a rebuilt volume record is in.
func TestPromoteOfAVolumeWithNoPrimaryNeedsNoWait(t *testing.T) {
	ctx := t.Context()
	w := newFenceWorld(t)
	if _, err := w.md.BumpVolumeEpoch(ctx, w.term, fenceVol, "", 1); err != nil {
		t.Fatal(err)
	}
	if _, etag, err := w.epochs.Current(ctx, fenceVol); err != nil {
		t.Fatal(err)
	} else if _, err := w.epochs.CompareAndAdvance(ctx, fenceVol, etag, 2); err != nil {
		t.Fatal(err)
	}

	got, err := w.p.Promote(ctx, w.term, fenceVol, time.Time{}, fenceHostB)
	if err != nil {
		t.Fatalf("promoting a volume nobody serves: %v", err)
	}
	if got != 3 {
		t.Fatalf("new epoch = %d, want 3", got)
	}
}

// TestPromoteFinishesAResumeOntoAHostThatWasCordonedMeanwhile: a cordon stops new
// placement, it does not stop a host serving what it already holds. A promotion that
// already moved the volume there must still be completable, or a crash plus a cordon
// strands the volume with no lease.
func TestPromoteFinishesAResumeOntoAHostThatWasCordonedMeanwhile(t *testing.T) {
	ctx := t.Context()
	w := newFenceWorld(t)
	if err := w.md.RenewHostLease(ctx, w.term, fenceHostA, int(fenceTTL/time.Second)); err != nil {
		t.Fatal(err)
	}
	renewedAt := w.clk.Wall()
	w.advance(fenceTTL + fenceSkew + time.Second)

	first, err := w.p.Promote(ctx, w.term, fenceVol, renewedAt, fenceHostB)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.md.SetHostState(ctx, w.term, fenceHostB, lifecycle.HostCordoned); err != nil {
		t.Fatal(err)
	}

	again, err := w.p.Promote(ctx, w.term, fenceVol, renewedAt, fenceHostB)
	if err != nil {
		t.Fatalf("finishing a promotion onto a cordoned host: %v", err)
	}
	if again != first {
		t.Fatalf("the retry granted epoch %d after %d", again, first)
	}
}

// TestPromoteRecordsTheFencingWaitDurably is the §7 machine, which until now nothing
// drove: a volume mid-fence was stored as ACTIVE, so a restarted Control Plane, a
// second reconciler pass or an operator saw a healthy volume and could start a
// competing promotion. The state has to be in PostgreSQL *before* the wait — the
// window it covers is exactly the minutes the wait lasts — and RECOVERY_REQUIRED
// after the epoch is granted, because §7 never lets a fenced volume go straight back
// to serving.
func TestPromoteRecordsTheFencingWaitDurably(t *testing.T) {
	ctx := t.Context()
	w := newFenceWorld(t)
	if err := w.md.RenewHostLease(ctx, w.term, fenceHostA, int(fenceTTL/time.Second)); err != nil {
		t.Fatal(err)
	}
	renewedAt := w.clk.Wall()

	if _, err := w.p.Promote(ctx, w.term, fenceVol, renewedAt, fenceHostB); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("promote inside the wait: err = %v, want ErrFencingWaitNotElapsed", err)
	}
	if got := w.state(t); got != lifecycle.VolumeFencingWait {
		t.Fatalf("volume state during the fencing wait = %q, want FENCING_WAIT", got)
	}

	w.advance(fenceTTL + fenceSkew + time.Second)
	if _, err := w.p.Promote(ctx, w.term, fenceVol, renewedAt, fenceHostB); err != nil {
		t.Fatalf("promote after the wait: %v", err)
	}
	if got := w.state(t); got != lifecycle.VolumeRecoveryRequired {
		t.Fatalf("volume state after the epoch was granted = %q, want RECOVERY_REQUIRED", got)
	}
	// And running it again — the reconciler always does — keeps it there.
	if _, err := w.p.Promote(ctx, w.term, fenceVol, renewedAt, fenceHostB); err != nil {
		t.Fatalf("re-running a completed promotion: %v", err)
	}
	if got := w.state(t); got != lifecycle.VolumeRecoveryRequired {
		t.Fatalf("volume state after a retry = %q, want RECOVERY_REQUIRED", got)
	}
}

// TestPromoteRefusesADetachedVolume: nothing is attached, so there is no writer to
// fence and no guest to serve. Promoting one would grant an epoch and a lease for a
// volume the Control Plane has already released.
func TestPromoteRefusesADetachedVolume(t *testing.T) {
	ctx := t.Context()
	w := newFenceWorld(t)
	if err := w.md.SetVolumeState(ctx, w.term, fenceVol, lifecycle.VolumeDetached); err != nil {
		t.Fatal(err)
	}
	if err := w.md.RenewHostLease(ctx, w.term, fenceHostA, int(fenceTTL/time.Second)); err != nil {
		t.Fatal(err)
	}
	renewedAt := w.clk.Wall()
	w.advance(fenceTTL + fenceSkew + time.Second)

	if _, err := w.p.Promote(ctx, w.term, fenceVol, renewedAt, fenceHostB); !errors.Is(err, controlplane.ErrVolumeNotPromotable) {
		t.Fatalf("promoting a DETACHED volume: err = %v, want ErrVolumeNotPromotable", err)
	}
	w.unchanged(t, 1, fenceHostA)
}

// TestAControlPlaneClockAheadOfTheDatabaseDoesNotGrantEarly is the twin of
// TestDriftOnlyLengthensTheWait, in the direction that loses data.
//
// `last_renewal` is stamped by the database's clock; the deadline derived from it was
// compared against the Control Plane's own wall clock. If the CP's clock jumps
// forward — an NTP correction, a VM restored from a snapshot, a bad RTC — the wait is
// shortened by exactly that offset, and epoch N+1 is granted while the old writer's
// monotonic lease is still valid. Only the safe direction was ever tested.
func TestAControlPlaneClockAheadOfTheDatabaseDoesNotGrantEarly(t *testing.T) {
	tests := []struct {
		name string
		skew time.Duration
		want error
	}{
		// Inside the configured bound: the promotion is simply not due yet on the
		// clock that stamped the row.
		{"a small offset does not shorten the wait", fenceSkew, controlplane.ErrFencingWaitNotElapsed},
		// Beyond it, the CP cannot reason about the deadline at all, in either
		// direction: it refuses rather than compute a wait from two clocks that
		// disagree by more than the protocol allows for (§12.1).
		{"a jump forward past max_clock_skew is refused", 10 * fenceTTL, controlplane.ErrClockOffsetTooLarge},
		{"a jump backwards past max_clock_skew is refused", -10 * fenceTTL, controlplane.ErrClockOffsetTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			w := newFenceWorld(t)
			if err := w.md.RenewHostLease(ctx, w.term, fenceHostA, int(fenceTTL/time.Second)); err != nil {
				t.Fatal(err)
			}
			renewedAt := w.dbClk.Wall()
			w.advance(3 * time.Second) // well inside lease_ttl + max_clock_skew
			w.clk.SetSkew(tc.skew)     // only the Control Plane's clock moves

			if _, err := w.p.Promote(ctx, w.term, fenceVol, renewedAt, fenceHostB); !errors.Is(err, tc.want) {
				t.Fatalf("promote with a CP clock offset of %v: err = %v, want %v", tc.skew, err, tc.want)
			}
			w.unchanged(t, 1, fenceHostA)

			// With the clocks agreeing again and the wait genuinely over, it proceeds.
			w.clk.SetSkew(0)
			w.advance(fenceTTL + fenceSkew)
			if _, err := w.p.Promote(ctx, w.term, fenceVol, renewedAt, fenceHostB); err != nil {
				t.Fatalf("promote once the wait really elapsed: %v", err)
			}
		})
	}
}
