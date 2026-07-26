package controlplane_test

import (
	"context"
	"sync"
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
	// The source holds a lease stamped now. The promoter reads it itself — a caller
	// that passes the zero instant is saying "I know nothing about the source", which
	// is not the same as "its lease expired long ago".
	if err := md.RenewHostLease(ctx, term, promoOld, 10); err != nil {
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
	bumped, err := w.md.BumpVolumeEpoch(ctx, w.term, promoVolume, promoNew, 1)
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

	bumped, _ := w.md.BumpVolumeEpoch(ctx, w.term, promoVolume, promoNew, 1)
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

	for expected := range int64(3) {
		if _, err := w.md.BumpVolumeEpoch(ctx, w.term, promoVolume, promoNew, expected+1); err != nil {
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

// TestPromoteRefusesToFinishSomebodyElsesPromotion: PostgreSQL one epoch ahead of the
// object is only *our* interrupted promotion when the volume row already names the
// host we are promoting to. If it names somebody else, another promoter bumped the
// epoch and is about to CAS the object; finishing "the resume" would write our host
// into the epoch object while PostgreSQL records theirs, and both would believe they
// hold the same epoch — the two records of one promotion disagreeing about the owner.
func TestPromoteRefusesToFinishSomebodyElsesPromotion(t *testing.T) {
	ctx := context.Background()
	w := newPromoWorld(t)
	third := "00000000-0000-7000-8000-0000000000c4"
	if err := w.md.UpsertHost(ctx, w.term, metadata.Host{HostID: third, State: lifecycle.HostActive}); err != nil {
		t.Fatal(err)
	}

	// Another promoter got as far as the PostgreSQL bump and has not CASed yet.
	bumped, err := w.md.BumpVolumeEpoch(ctx, w.term, promoVolume, promoNew, 1)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := w.p.Promote(ctx, w.term, promoVolume, time.Time{}, third); err == nil {
		t.Fatal("a promoter finished another promoter's epoch and named itself the holder")
	}
	ep, _, err := w.epochs.Current(ctx, promoVolume)
	if err != nil {
		t.Fatal(err)
	}
	if ep != 1 {
		t.Fatalf("epoch object = %d, want the untouched 1", ep)
	}
	v, _ := w.md.GetVolume(ctx, promoVolume)
	if v.CurrentEpoch != bumped || v.PrimaryHostID != promoNew {
		t.Fatalf("volume = epoch %d on %s, want %d on the other promoter's host", v.CurrentEpoch, v.PrimaryHostID, bumped)
	}
}

// TestConcurrentPromotionsLeaveExactlyOneWriter is the whole point of the epoch: n
// Control-Plane goroutines (a reconciler pass, a drain, an operator) promote the same
// volume to n different hosts at the same moment. Exactly one may end up able to ACK
// and publish, and every durable record of the promotion — the volume row, the epoch
// object's number, the epoch object's holder, the lease — has to name that same host.
//
// A blind epoch increment burns one epoch per caller and leaves primary_host_id
// naming whoever ran last; an epoch object that does not name its holder lets a
// second host pass the publish check at the same number.
func TestConcurrentPromotionsLeaveExactlyOneWriter(t *testing.T) {
	ctx := context.Background()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	md := metasim.New(clk.Wall)
	term, err := md.AcquireLeadership(ctx, "cp")
	if err != nil {
		t.Fatal(err)
	}
	const source = "00000000-0000-7000-8000-0000000000d0"
	hosts := []string{
		"00000000-0000-7000-8000-0000000000d1",
		"00000000-0000-7000-8000-0000000000d2",
		"00000000-0000-7000-8000-0000000000d3",
		"00000000-0000-7000-8000-0000000000d4",
		"00000000-0000-7000-8000-0000000000d5",
	}
	for _, h := range append([]string{source}, hosts...) {
		if err := md.UpsertHost(ctx, term, metadata.Host{HostID: h, State: lifecycle.HostActive}); err != nil {
			t.Fatal(err)
		}
	}
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: promoVolume, SizeBytes: 1 << 30, BlockSize: 65536,
		State: lifecycle.VolumeActive, CurrentEpoch: 1, PrimaryHostID: source,
		DEKWrapped: []byte{1}, KEKID: "k",
	}); err != nil {
		t.Fatal(err)
	}
	epochs := epoch.NewStore(sim.NewObjectStore())
	if _, err := epochs.Init(ctx, promoVolume, 1); err != nil {
		t.Fatal(err)
	}
	if err := md.RenewHostLease(ctx, term, source, 10); err != nil {
		t.Fatal(err)
	}
	clk.Advance(30 * time.Second) // the source's lease can no longer be valid
	p := controlplane.NewPromoter(md, epochs, clk, 10*time.Second, 2*time.Second)

	var (
		mu      sync.Mutex
		winners []string
		wg      sync.WaitGroup
	)
	for _, host := range hosts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.Promote(ctx, term, promoVolume, time.Time{}, host); err == nil {
				mu.Lock()
				winners = append(winners, host)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(winners) != 1 {
		t.Fatalf("%d concurrent promotions succeeded, want exactly 1: %v", len(winners), winners)
	}
	winner := winners[0]

	v, err := md.GetVolume(ctx, promoVolume)
	if err != nil {
		t.Fatal(err)
	}
	if v.CurrentEpoch != 2 {
		t.Fatalf("volume epoch = %d after one promotion, want 2", v.CurrentEpoch)
	}
	if v.PrimaryHostID != winner {
		t.Fatalf("volume names %q as primary while %q won the promotion", v.PrimaryHostID, winner)
	}
	ep, _, err := epochs.Current(ctx, promoVolume)
	if err != nil {
		t.Fatal(err)
	}
	if ep != uint64(v.CurrentEpoch) {
		t.Fatalf("epoch object = %d, PostgreSQL = %d", ep, v.CurrentEpoch)
	}

	var publishers, leaseholders []string
	for _, host := range hosts {
		if verifyEpochHolder(t, epochs, ctx, promoVolume, ep, host) == nil {
			publishers = append(publishers, host)
		}
		if _, err := md.GetHostLease(ctx, host); err == nil {
			leaseholders = append(leaseholders, host)
		}
	}
	if len(publishers) != 1 || publishers[0] != winner {
		t.Fatalf("hosts able to publish at epoch %d: %v, want only %q", ep, publishers, winner)
	}
	if len(leaseholders) != 1 || leaseholders[0] != winner {
		t.Fatalf("hosts holding a lease: %v, want only %q", leaseholders, winner)
	}
}

// verifyEpochHolder asks the epoch store whether a host may publish at an epoch,
// through an optional interface so the missing check fails as a test rather than as a
// build break (the metadata Store contract states its own gaps the same way).
func verifyEpochHolder(t *testing.T, s *epoch.Store, ctx context.Context, volumeID string, expected uint64, holderID string) error {
	t.Helper()
	v, ok := any(s).(interface {
		VerifyHolder(ctx context.Context, volumeID string, expected uint64, holderID string) error
	})
	if !ok {
		t.Fatal("epoch.Store cannot say who holds an epoch: no VerifyHolder (§12.4)")
	}
	return v.VerifyHolder(ctx, volumeID, expected, holderID)
}
