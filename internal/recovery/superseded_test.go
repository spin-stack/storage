package recovery_test

import (
	"context"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// An epoch has a floor, and — once a promotion has recorded a boundary over it — a
// ceiling too. Only the floor was ever consulted.
//
// The shape: W1's PUT is still in flight (a slow backend, an SDK retry) when W2
// promotes, recovers up to D and writes the create-only recovery point of epoch N+1.
// The PUT then lands, extending epoch N's contiguous run past D. From that moment
// DurablePrefix(N) answers a number the live volume never had: sequences above D were
// written by a fenced writer and were never adopted by the epoch that succeeded it.
//
// Two things break at once. materialize.FromEpoch(vol, N) — the drain's and the warm
// standby's source (ADR-0008) — rebuilds a volume containing records epoch N+1 does
// not have, so the same bucket materializes two different volumes. And a drain
// resumed through finishMovedVolume recomputes prog.UpTo, finds it disagrees with the
// immutable boundary already stored, and fails hard and permanently for that volume.

// ceilingFaultStore fails the GET of exactly one epoch's recovery point, leaving the
// rest of the bucket readable — a throttled GET (§24) on the object that decides the
// ceiling, not on the one that decides the floor.
type ceilingFaultStore struct {
	objectstore.Store
	suffix string
}

func (s ceilingFaultStore) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasSuffix(key, s.suffix) {
		return nil, sim.ErrThrottled
	}
	return s.Store.Get(ctx, key)
}

// supersededWorld builds the exact ordering of the finding: epoch 1 durable through
// 4, epoch 2's boundary recorded at 4, and only then W1's fenced PUT of 5..6.
func supersededWorld(t *testing.T) (*sim.ObjectStore, [16]byte) {
	t.Helper()
	ctx := context.Background()
	store := sim.NewObjectStore()
	vol := vol7()

	for _, span := range [][2]uint64{{1, 2}, {3, 4}} {
		k, b := craftObject(t, vol, 1, span[0], span[1], nil)
		putObject(t, store, k, b)
	}
	// W2 recovers 4 and fixes the frontier. This object is create-only: 4 is the last
	// sequence of epoch 1 that anything will ever adopt.
	if err := recovery.WriteRecoveryPoint(ctx, store, vol, 2, 1, 4); err != nil {
		t.Fatal(err)
	}
	// W1's in-flight PUT completes afterwards. It is a perfectly valid object; it is
	// simply late, and its writer was fenced before it landed.
	k, b := craftObject(t, vol, 1, 5, 6, nil)
	putObject(t, store, k, b)
	return store, vol
}

// TestLateFencedPutCannotRaiseASupersededEpoch is the finding itself.
func TestLateFencedPutCannotRaiseASupersededEpoch(t *testing.T) {
	ctx := context.Background()
	store, vol := supersededWorld(t)

	got, err := recovery.DurablePrefix(ctx, store, vol, 1)
	if err != nil {
		t.Fatalf("DurablePrefix: %v", err)
	}
	if got != 4 {
		t.Fatalf("DurablePrefix(epoch 1) = %d, want 4 — sequences above the immutable "+
			"boundary of epoch 2 were written by a fenced writer and were never adopted", got)
	}
	if got, err := recovery.DurablePoint(ctx, store, vol, 1); err != nil || got != 4 {
		t.Fatalf("DurablePoint(epoch 1) = %d err=%v, want 4", got, err)
	}
}

// TestRecoveringASupersededEpochMatchesWhatItsSuccessorAdopted: recovering epoch 1 on
// its own must produce exactly the state epoch 2 started from. Anything else is two
// divergent volumes out of one bucket.
func TestRecoveringASupersededEpochMatchesWhatItsSuccessorAdopted(t *testing.T) {
	ctx := context.Background()
	store, vol := supersededWorld(t)

	view, durable, err := recovery.Recover(ctx, store, nil, vol, 1)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if durable != 4 {
		t.Fatalf("Recover(epoch 1) durable = %d, want 4", durable)
	}
	// craftObject writes "payload" at seq*512. Sequence 5 must not be in the view.
	buf := make([]byte, 7)
	view.Read(5*512, buf)
	for _, b := range buf {
		if b != 0 {
			t.Fatalf("a record epoch 2 never adopted leaked into the recovered view: %q", buf)
		}
	}
}

// TestASupersededEpochsCeilingIsNotGuessed: the ceiling is as load-bearing as the
// floor, so an unreadable one is an error rather than "no ceiling". Answering 6 here
// is what a resumed drain writes down and then contradicts for ever.
func TestASupersededEpochsCeilingIsNotGuessed(t *testing.T) {
	ctx := context.Background()
	base, vol := supersededWorld(t)

	store := ceilingFaultStore{Store: base, suffix: "/2/recovery-point.json"}
	if got, err := recovery.DurablePrefix(ctx, store, vol, 1); err == nil {
		t.Fatalf("DurablePrefix returned %d with no error while the successor's boundary "+
			"could not be read", got)
	}
}

// TestAnOpenEpochHasNoCeiling is the control: the newest epoch is still being written
// to, so its prefix must be free to grow.
func TestAnOpenEpochHasNoCeiling(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	vol := vol7()

	for _, span := range [][2]uint64{{1, 2}, {3, 4}, {5, 6}} {
		k, b := craftObject(t, vol, 1, span[0], span[1], nil)
		putObject(t, store, k, b)
	}
	if got, err := recovery.DurablePrefix(ctx, store, vol, 1); err != nil || got != 6 {
		t.Fatalf("DurablePrefix = %d err=%v, want 6 — an epoch with no successor is open", got, err)
	}
}

// TestASuccessorThatDidNotAdoptThisEpochIsNotItsCeiling: the boundary of epoch N+1
// only speaks for epoch N when it says so. One that records a different predecessor
// makes no claim about us and must not be read as one.
func TestASuccessorThatDidNotAdoptThisEpochIsNotItsCeiling(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	vol := vol7()

	for _, span := range [][2]uint64{{1, 2}, {3, 4}} {
		k, b := craftObject(t, vol, 2, span[0], span[1], nil)
		putObject(t, store, k, b)
	}
	// Epoch 3's boundary chains back to epoch 1, not to epoch 2.
	if err := recovery.WriteRecoveryPoint(ctx, store, vol, 3, 1, 2); err != nil {
		t.Fatal(err)
	}
	// Epoch 2 has no boundary of its own, so its floor is 1 and its prefix is 1..4.
	if got, err := recovery.DurablePrefix(ctx, store, vol, 2); err != nil || got != 4 {
		t.Fatalf("DurablePrefix(epoch 2) = %d err=%v, want 4", got, err)
	}
}
