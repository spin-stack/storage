package controlplane_test

import (
	"context"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// DEV-0004 (second half). Promotion writes to three places that can fail
// independently — the epoch in PostgreSQL, the epoch object in S3, and the lease —
// and the reconciler retries it after any crash. Each of these tests kills it at one
// boundary and runs it again: the result must be the same epoch, granted once.

type promoWorld struct {
	md     metadata.Store
	term   int64
	epochs *epoch.Store
	clk    *sim.Clock
	p      *controlplane.Promoter
}

const (
	promoVolume = "00000000-0000-7000-8000-0000000000c1"
	promoOld    = "00000000-0000-7000-8000-0000000000c2"
	promoNew    = "00000000-0000-7000-8000-0000000000c3"
)

func newPromoWorld(t *testing.T) *promoWorld {
	t.Helper()
	ctx := context.Background()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := sim.NewObjectStore()
	md := metasim.New(clk.Wall)
	term, _ := md.AcquireLeadership(ctx, "cp")

	for _, h := range []string{promoOld, promoNew} {
		if err := md.UpsertHost(ctx, term, metadata.Host{HostID: h, State: lifecycle.HostActive}); err != nil {
			t.Fatal(err)
		}
	}
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: promoVolume, SizeBytes: 1 << 30, BlockSize: 65536,
		State: lifecycle.VolumeActive, CurrentEpoch: 1, PrimaryHostID: promoOld,
		DEKWrapped: []byte{1}, KEKID: "k",
	}); err != nil {
		t.Fatal(err)
	}
	epochs := epoch.NewStore(store)
	if _, err := epochs.Init(ctx, promoVolume, 1); err != nil {
		t.Fatal(err)
	}
	clk.Advance(30 * time.Second) // past FENCING_WAIT
	return &promoWorld{
		md: md, term: term, epochs: epochs, clk: clk,
		p: controlplane.NewPromoter(md, epochs, clk, 10*time.Second, 2*time.Second),
	}
}

// TestPromoteIsIdempotent: the reconciler running the same promotion twice must not
// grant two epochs. A second epoch would fence the writer that was just promoted.
func TestPromoteIsIdempotent(t *testing.T) {
	ctx := context.Background()
	w := newPromoWorld(t)

	first, err := w.p.Promote(ctx, w.term, promoVolume, time.Time{}, promoNew)
	if err != nil {
		t.Fatal(err)
	}
	second, err := w.p.Promote(ctx, w.term, promoVolume, time.Time{}, promoNew)
	if err != nil {
		t.Fatalf("re-running a completed promotion must succeed: %v", err)
	}
	if first != second {
		t.Fatalf("promotion granted epoch %d then %d — the retry fenced the new writer", first, second)
	}
	v, _ := w.md.GetVolume(ctx, promoVolume)
	if uint64(v.CurrentEpoch) != first {
		t.Fatalf("volume epoch = %d, want %d", v.CurrentEpoch, first)
	}
	ep, _, err := w.epochs.Current(ctx, promoVolume)
	if err != nil || ep != first {
		t.Fatalf("epoch object = %d err=%v, want %d", ep, err, first)
	}
}

// TestPromoteResumesAfterCrashBetweenPGAndS3: the epoch was bumped in PostgreSQL and
// the process died before the S3 CAS. The retry must finish that epoch, not start
// another one.
func TestPromoteResumesAfterCrashBetweenPGAndS3(t *testing.T) {
	ctx := context.Background()
	w := newPromoWorld(t)

	// Simulate the first half having happened.
	bumped, err := w.md.BumpVolumeEpoch(ctx, w.term, promoVolume, promoNew)
	if err != nil {
		t.Fatal(err)
	}

	got, err := w.p.Promote(ctx, w.term, promoVolume, time.Time{}, promoNew)
	if err != nil {
		t.Fatalf("resume after a crash before the CAS: %v", err)
	}
	if got != uint64(bumped) {
		t.Fatalf("resume granted epoch %d, want the one already in PG (%d)", got, bumped)
	}
	ep, _, _ := w.epochs.Current(ctx, promoVolume)
	if ep != uint64(bumped) {
		t.Fatalf("epoch object = %d, want %d — the CAS half never completed", ep, bumped)
	}
	v, _ := w.md.GetVolume(ctx, promoVolume)
	if v.CurrentEpoch != bumped {
		t.Fatalf("the retry bumped the epoch again: %d", v.CurrentEpoch)
	}
}

// TestPromoteResumesAfterCrashBeforeTheLease: PG and S3 agree on the new epoch but
// the lease was never granted, so the promoted host cannot ACK. The retry must grant
// it rather than refuse because "the epoch is already there".
func TestPromoteResumesAfterCrashBeforeTheLease(t *testing.T) {
	ctx := context.Background()
	w := newPromoWorld(t)

	bumped, _ := w.md.BumpVolumeEpoch(ctx, w.term, promoVolume, promoNew)
	_, etag, _ := w.epochs.Current(ctx, promoVolume)
	if _, err := w.epochs.CompareAndAdvance(ctx, promoVolume, etag, uint64(bumped)); err != nil {
		t.Fatal(err)
	}

	if _, err := w.md.GetHostLease(ctx, promoNew); err == nil {
		t.Fatal("precondition: the new host must not hold a lease yet")
	}
	if _, err := w.p.Promote(ctx, w.term, promoVolume, time.Time{}, promoNew); err != nil {
		t.Fatalf("resume after a crash before the lease grant: %v", err)
	}
	if _, err := w.md.GetHostLease(ctx, promoNew); err != nil {
		t.Fatalf("the promoted host still has no lease: %v", err)
	}
}

// TestPromoteRefusesWhenSomeoneElseAdvancedFurther: if the epoch object moved past
// what this promotion is completing, another Control Plane won. Finishing our steps
// would fence the winner.
func TestPromoteRefusesWhenSomeoneElseAdvancedFurther(t *testing.T) {
	ctx := context.Background()
	w := newPromoWorld(t)

	// Another CP promoted twice while we were away.
	_, etag, _ := w.epochs.Current(ctx, promoVolume)
	if _, err := w.epochs.CompareAndAdvance(ctx, promoVolume, etag, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := w.p.Promote(ctx, w.term, promoVolume, time.Time{}, promoNew); err == nil {
		t.Fatal("promotion must refuse when the epoch object is ahead of what it would grant")
	}
}

// TestPromoteRefusesAnUnexpectedEpochGap: PostgreSQL more than one epoch ahead of the
// object is not a resume, it is corruption or a lost write. Guessing which epoch to
// finish would be inventing a fence.
func TestPromoteRefusesAnUnexpectedEpochGap(t *testing.T) {
	ctx := context.Background()
	w := newPromoWorld(t)

	for range 3 {
		if _, err := w.md.BumpVolumeEpoch(ctx, w.term, promoVolume, promoNew); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.p.Promote(ctx, w.term, promoVolume, time.Time{}, promoNew); err == nil {
		t.Fatal("a multi-epoch gap between PostgreSQL and the object must not be resumed silently")
	}
}

// TestPromoteRefusesBeforeTheFencingWait keeps the §12.3 order visible in the
// idempotent version: none of the resume logic runs before the wait elapses.
func TestPromoteRefusesBeforeTheFencingWait(t *testing.T) {
	ctx := context.Background()
	w := newPromoWorld(t)

	// A lease renewed "now" on the CP clock: the deadline is in the future.
	if _, err := w.p.Promote(ctx, w.term, promoVolume, w.clk.Wall(), promoNew); err == nil {
		t.Fatal("promotion must refuse before FENCING_WAIT")
	}
	v, _ := w.md.GetVolume(ctx, promoVolume)
	if v.CurrentEpoch != 1 {
		t.Fatalf("epoch moved to %d before the fencing wait", v.CurrentEpoch)
	}
}

// TestPromoteOfAMissingVolumeFails: the resume decision needs the row; without it
// there is nothing to be idempotent about.
func TestPromoteOfAMissingVolumeFails(t *testing.T) {
	ctx := context.Background()
	w := newPromoWorld(t)
	if _, err := w.p.Promote(ctx, w.term, "00000000-0000-7000-8000-0000000000ff", time.Time{}, promoNew); err == nil {
		t.Fatal("promoting a volume that does not exist must fail")
	}
}
