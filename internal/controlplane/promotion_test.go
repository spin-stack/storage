package controlplane_test

import (
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

const (
	volID = "00000000-0000-7000-8000-000000000010"
	host1 = "00000000-0000-7000-8000-0000000000a1"
	host2 = "00000000-0000-7000-8000-0000000000a2"
)

func setup(t *testing.T) (*controlplane.Promoter, metadata.Store, *epoch.Store, *sim.Clock, int64) {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	md := metasim.New(clk.Wall)
	epochs := epoch.NewStore(sim.NewObjectStore())

	term, _ := md.AcquireLeadership(t.Context(), "cp")
	_ = md.UpsertHost(t.Context(), term, metadata.Host{HostID: host1, State: lifecycle.HostActive})
	_ = md.UpsertHost(t.Context(), term, metadata.Host{HostID: host2, State: lifecycle.HostActive})
	_ = md.CreateVolume(t.Context(), term, metadata.Volume{
		VolumeID: volID, State: lifecycle.VolumeActive, PrimaryHostID: host1, DEKWrapped: []byte{1}, KEKID: "k",
	})
	if _, err := epochs.Init(t.Context(), volID, 0); err != nil {
		t.Fatal(err)
	}
	p := controlplane.NewPromoter(md, epochs, clk, 10*time.Second, 2*time.Second)
	return p, md, epochs, clk, term
}

// TestFencingWaitEnforced is INV-11: promotion is refused until
// last_renewal + lease_ttl + max_clock_skew has elapsed.
func TestFencingWaitEnforced(t *testing.T) {
	ctx := t.Context()
	p, md, epochs, clk, term := setup(t)
	renewedAt := clk.Wall() // old primary's last lease renewal

	// Too early (0s elapsed) — refused.
	if _, err := p.Promote(ctx, term, volID, renewedAt, host2); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("promote-too-early: want ErrFencingWaitNotElapsed, got %v", err)
	}
	// Just before the deadline (11s < 12s) — still refused.
	clk.Advance(11 * time.Second)
	if _, err := p.Promote(ctx, term, volID, renewedAt, host2); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("promote just-before-deadline: want ErrFencingWaitNotElapsed, got %v", err)
	}
	// At/after the deadline (13s >= 12s) — granted.
	clk.Advance(2 * time.Second)
	newEpoch, err := p.Promote(ctx, term, volID, renewedAt, host2)
	if err != nil {
		t.Fatalf("promote after wait: %v", err)
	}
	if newEpoch != 1 {
		t.Fatalf("new epoch = %d, want 1", newEpoch)
	}
	// PG, the epoch object, and the primary all reflect the new epoch/host.
	if ep, _, _ := epochs.Current(ctx, volID); ep != 1 {
		t.Fatalf("epoch object = %d, want 1", ep)
	}
	v, _ := md.GetVolume(ctx, volID)
	if v.CurrentEpoch != 1 || v.PrimaryHostID != host2 {
		t.Fatalf("volume after promotion: %+v", v)
	}
}

// TestDriftOnlyLengthensTheWait is INV-11 / §12.1: a CP wall clock behind true time
// only makes the wait longer; it never grants early.
func TestDriftOnlyLengthensTheWait(t *testing.T) {
	ctx := t.Context()
	p, _, _, clk, term := setup(t)
	renewedAt := clk.Wall()

	clk.Advance(13 * time.Second) // past the 12s deadline in monotonic terms
	clk.SetSkew(-5 * time.Second) // CP wall clock runs 5s behind → now reads T0+8s

	if _, err := p.Promote(ctx, term, volID, renewedAt, host2); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		t.Fatalf("a lagging CP clock must wait longer, not grant early; got %v", err)
	}
	clk.SetSkew(0) // clock corrected → now reads T0+13s
	if _, err := p.Promote(ctx, term, volID, renewedAt, host2); err != nil {
		t.Fatalf("promote after correction: %v", err)
	}
}

// TestStaleWriterCannotPublishAfterPromotion is INV-10: once promoted, the old
// writer's epoch is fenced — its publish fails the epoch check and CAS, and a
// stale-term mutation affects 0 rows.
func TestStaleWriterCannotPublishAfterPromotion(t *testing.T) {
	ctx := t.Context()
	p, md, epochs, clk, term := setup(t)
	renewedAt := clk.Wall()

	// The old writer W1 holds epoch 0 and the epoch object's ETag at that time.
	_, w1ETag, _ := epochs.Current(ctx, volID)

	clk.Advance(13 * time.Second)
	if _, err := p.Promote(ctx, term, volID, renewedAt, host2); err != nil {
		t.Fatal(err)
	}

	// W1 tries to publish at epoch 0: the epoch check fences it.
	if err := epochs.Verify(ctx, volID, 0); !errors.Is(err, epoch.ErrEpochChanged) {
		t.Fatalf("stale writer Verify: want ErrEpochChanged, got %v", err)
	}
	// W1's CAS with its stale ETag loses.
	if _, err := epochs.CompareAndAdvance(ctx, volID, w1ETag, 99); !errors.Is(err, epoch.ErrCASConflict) {
		t.Fatalf("stale writer CAS: want ErrCASConflict, got %v", err)
	}
	// And a zombie CP (stale term) cannot mutate PG (§7).
	if _, err := md.BumpVolumeEpoch(ctx, term-1, volID, host1, 1); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("stale-term bump: want ErrStaleTerm, got %v", err)
	}
}
