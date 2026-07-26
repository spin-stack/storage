package epoch_test

import (
	"context"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// The epoch number alone says *when* a writer was fenced, never *who* was granted
// the epoch. That is enough while the number is the only thing that moves, and it
// stops being enough the moment two records of the same promotion can disagree about
// the owner: PostgreSQL's volumes.primary_host_id and the S3 epoch object are written
// by two different steps of §12.3, and a promoter that crashed, was overtaken, or
// lost the CAS can leave them naming different hosts at the same epoch. Both hosts
// then pass a number-only Verify and both publish into wal/<vol>/<epoch>/.
//
// So the epoch object names its holder and the publish gate checks it. These tests
// are written against optional interfaces so the gap fails as a test rather than as
// a build break, the way the metadata Store contract states its own missing methods.

type epochGranter interface {
	Grant(ctx context.Context, volumeID, fromETag string, newEpoch uint64, holderID string) (string, error)
}

type holderVerifier interface {
	VerifyHolder(ctx context.Context, volumeID string, expected uint64, holderID string) error
}

func grant(t *testing.T, s *epoch.Store, ctx context.Context, volumeID, fromETag string, newEpoch uint64, holderID string) (string, error) {
	t.Helper()
	g, ok := any(s).(epochGranter)
	if !ok {
		t.Fatal("epoch.Store cannot record who an epoch was granted to: no Grant (§12.4)")
	}
	return g.Grant(ctx, volumeID, fromETag, newEpoch, holderID)
}

func verifyHolder(t *testing.T, s *epoch.Store, ctx context.Context, volumeID string, expected uint64, holderID string) error {
	t.Helper()
	v, ok := any(s).(holderVerifier)
	if !ok {
		t.Fatal("epoch.Store cannot check who holds an epoch: no VerifyHolder (§12.4)")
	}
	return v.VerifyHolder(ctx, volumeID, expected, holderID)
}

const (
	hostX = "00000000-0000-7000-8000-0000000000f1"
	hostY = "00000000-0000-7000-8000-0000000000f2"
)

// TestOnlyTheGrantedHolderMayPublish: the publish gate of §12.4 must answer "you are
// not the holder", not just "the number still matches". A host that believes it was
// granted epoch 4 — because PostgreSQL says it is the primary at epoch 4, or because
// its own promotion returned before the CAS was re-read — is fenced by the object.
func TestOnlyTheGrantedHolderMayPublish(t *testing.T) {
	ctx := context.Background()
	s := epoch.NewStore(sim.NewObjectStore())
	etag, err := s.Init(ctx, vol, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant(t, s, ctx, vol, etag, 4, hostX); err != nil {
		t.Fatalf("granting epoch 4 to hostX: %v", err)
	}

	tests := []struct {
		name     string
		expected uint64
		holder   string
		want     error
	}{
		{"the holder at its own epoch", 4, hostX, nil},
		{"another host at the same epoch", 4, hostY, epoch.ErrNotHolder},
		{"the holder at a superseded epoch", 3, hostX, epoch.ErrEpochChanged},
		{"a host that was never granted anything", 4, "", epoch.ErrNotHolder},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := verifyHolder(t, s, ctx, vol, tc.expected, tc.holder); !errors.Is(err, tc.want) {
				t.Fatalf("VerifyHolder(%d, %q) = %v, want %v", tc.expected, tc.holder, err, tc.want)
			}
		})
	}
}

// TestAnUnheldEpochHasNoPublisher: a freshly initialised epoch object (a created or
// rebuilt volume) names nobody. Fail closed — "the record does not name you" — so a
// host cannot publish into an epoch it was never granted just because the number
// happens to match.
func TestAnUnheldEpochHasNoPublisher(t *testing.T) {
	ctx := context.Background()
	s := epoch.NewStore(sim.NewObjectStore())
	if _, err := s.Init(ctx, vol, 1); err != nil {
		t.Fatal(err)
	}
	if err := verifyHolder(t, s, ctx, vol, 1, hostX); !errors.Is(err, epoch.ErrNotHolder) {
		t.Fatalf("VerifyHolder against an unheld epoch = %v, want ErrNotHolder", err)
	}
	// The number-only gate is still available for callers that legitimately have no
	// holder to offer (rebuild-metadata reads the epoch of every volume it finds).
	if err := s.Verify(ctx, vol, 1); err != nil {
		t.Fatalf("Verify(1) on a freshly initialised object: %v", err)
	}
}

// TestTwoPromotersLeaveExactlyOnePublisher is the damage from the finding, end to
// end at this layer: promoter B advances 3 -> 4 for hostY inside the window
// promoter A's read spans. A either detects the unstable read or loses the CAS, and
// whichever way it goes, exactly one of the two hosts may publish afterwards.
func TestTwoPromotersLeaveExactlyOnePublisher(t *testing.T) {
	ctx := context.Background()
	backing := sim.NewObjectStore()
	h := &hookedStore{Store: backing}
	loser := epoch.NewStore(h)
	winner := epoch.NewStore(backing)

	etag, err := winner.Init(ctx, vol, 3)
	if err != nil {
		t.Fatal(err)
	}
	h.afterGet = func() {
		if _, err := grant(t, winner, ctx, vol, etag, 4, hostY); err != nil {
			t.Errorf("winner grant: %v", err)
		}
	}

	stored, casETag, err := loser.Current(ctx, vol)
	if err == nil {
		_, err = grant(t, loser, ctx, vol, casETag, stored+1, hostX)
	}
	if err == nil {
		t.Fatal("both promoters granted an epoch for the same volume")
	}

	publishers := 0
	for _, host := range []string{hostX, hostY} {
		if verifyHolder(t, winner, ctx, vol, 4, host) == nil {
			publishers++
		}
	}
	if publishers != 1 {
		t.Fatalf("%d hosts may publish at epoch 4, want exactly 1", publishers)
	}
}

// TestGrantIsStillForwardOnly: naming a holder must not become a way around the
// forward-only rule — re-granting the epoch a fenced writer already used would put
// two writers in one wal/<vol>/<epoch>/ namespace no matter who is named.
func TestGrantIsStillForwardOnly(t *testing.T) {
	ctx := context.Background()
	s := epoch.NewStore(sim.NewObjectStore())
	etag, err := s.Init(ctx, vol, 5)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant(t, s, ctx, vol, etag, 5, hostY); !errors.Is(err, epoch.ErrEpochNotAdvancing) {
		t.Fatalf("re-granting epoch 5 to another host: %v, want ErrEpochNotAdvancing", err)
	}
}
