package epoch_test

import (
	"context"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// hookedStore lets a test run a concurrent promoter's write inside the window
// another promoter's read of the epoch object spans. It is deliberately literal
// about the interleaving: no goroutines, no sleeps, so the trace is the same on
// every run (INV-02).
type hookedStore struct {
	objectstore.Store
	afterGet  func()
	afterHead func(n int)
	beforePut func()
	heads     int
}

func (s *hookedStore) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	if s.beforePut != nil {
		hook := s.beforePut
		s.beforePut = nil
		hook()
	}
	return s.Store.Put(ctx, key, data, opts)
}

func (s *hookedStore) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := s.Store.Get(ctx, key)
	if s.afterGet != nil {
		hook := s.afterGet
		s.afterGet = nil // fire once
		hook()
	}
	return b, err
}

func (s *hookedStore) Head(ctx context.Context, key string) (objectstore.ObjectInfo, error) {
	info, err := s.Store.Head(ctx, key)
	s.heads++
	if s.afterHead != nil {
		hook := s.afterHead
		s.afterHead = nil
		hook(s.heads)
	}
	return info, err
}

// TestCurrentIsAStableRead: Current returns the epoch body and the ETag a CAS will be
// made against. If those two come from different versions of the object, the caller
// pairs a stale epoch with a fresh ETag — and then *wins* the CAS, overwriting the
// epoch another promoter just granted (§12.4, INV-10). The read must be atomic, or
// detect that it was not.
func TestCurrentIsAStableRead(t *testing.T) {
	tests := []struct {
		name string
		// mutate installs the interleaving: the concurrent advance happens inside
		// the read window, at the named point.
		mutate func(h *hookedStore, advance func())
	}{
		{"another promoter advances after the body is read", func(h *hookedStore, advance func()) {
			h.afterGet = advance
		}},
		{"another promoter advances after the first metadata read", func(h *hookedStore, advance func()) {
			h.afterHead = func(int) { advance() }
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			backing := sim.NewObjectStore()
			h := &hookedStore{Store: backing}
			s := epoch.NewStore(h)
			direct := epoch.NewStore(backing)

			etag, err := direct.Init(ctx, vol, 3)
			if err != nil {
				t.Fatal(err)
			}
			advance := func() {
				if _, err := direct.CompareAndAdvance(ctx, vol, etag, 4); err != nil {
					t.Errorf("the winning promoter could not advance: %v", err)
				}
			}
			tc.mutate(h, advance)

			gotEpoch, gotETag, err := s.Current(ctx, vol)
			if err != nil {
				if !errors.Is(err, epoch.ErrCASConflict) {
					t.Fatalf("unstable read reported %v, want ErrCASConflict", err)
				}
				return // detected: the loser will re-read and see epoch 4
			}
			// If it did return a pair, that pair must be self-consistent. A stale ETag
			// is harmless — the CAS that follows simply loses. What must never happen
			// is a stale epoch carrying the *current* ETag: that CAS wins.
			live, liveETag, err := direct.Current(ctx, vol)
			if err != nil {
				t.Fatal(err)
			}
			if gotETag == liveETag && gotEpoch != live {
				t.Fatalf("Current returned epoch %d with the live ETag %q while the object holds epoch %d — "+
					"a stale epoch paired with a fresh ETag lets the loser win the CAS",
					gotEpoch, gotETag, live)
			}
		})
	}
}

// TestTwoPromotersCannotBothGrantTheSameEpoch is the damage the unstable read does:
// promoter B advances 3 -> 4 and grants it to its host while promoter A is reading.
// A must not be able to write epoch 4 a second time (for a different host); if it
// could, two writers would number objects into wal/<vol>/4/ independently and the
// contiguous prefix would belong to neither (INV-08/INV-10).
func TestTwoPromotersCannotBothGrantTheSameEpoch(t *testing.T) {
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
		if _, err := winner.CompareAndAdvance(ctx, vol, etag, 4); err != nil {
			t.Errorf("winner advance: %v", err)
		}
	}

	stored, casETag, err := loser.Current(ctx, vol)
	if err == nil {
		// The loser computed its target from what it read (3 + 1 = 4).
		_, err = loser.CompareAndAdvance(ctx, vol, casETag, stored+1)
	}
	if err == nil {
		t.Fatal("both promoters granted epoch 4: the second CAS overwrote the epoch the first one had already handed out")
	}

	ep, _, err := winner.Current(ctx, vol)
	if err != nil || ep != 4 {
		t.Fatalf("epoch object = %d err=%v, want the winner's 4", ep, err)
	}
}

// failingStore fails one operation of the read Current makes, so every leg of the
// stable read is known to surface its error instead of returning a pair built from a
// half-failed read.
type failingStore struct {
	objectstore.Store
	failGet bool
	headsOK int
	corrupt bool
	heads   int
}

var errBackend = errors.New("backend unavailable")

func (s *failingStore) Get(ctx context.Context, key string) ([]byte, error) {
	if s.failGet {
		return nil, errBackend
	}
	if s.corrupt {
		return []byte("{not json"), nil
	}
	return s.Store.Get(ctx, key)
}

func (s *failingStore) Head(ctx context.Context, key string) (objectstore.ObjectInfo, error) {
	s.heads++
	if s.heads > s.headsOK {
		return objectstore.ObjectInfo{}, errBackend
	}
	return s.Store.Head(ctx, key)
}

func TestCurrentReportsAFailedRead(t *testing.T) {
	tests := []struct {
		name  string
		store func(backing objectstore.Store) *failingStore
		want  error
	}{
		{"the first metadata read fails", func(b objectstore.Store) *failingStore {
			return &failingStore{Store: b, headsOK: 0}
		}, errBackend},
		{"the body read fails", func(b objectstore.Store) *failingStore {
			return &failingStore{Store: b, headsOK: 1, failGet: true}
		}, errBackend},
		{"the confirming metadata read fails", func(b objectstore.Store) *failingStore {
			return &failingStore{Store: b, headsOK: 1}
		}, errBackend},
		{"the body is not a valid epoch record", func(b objectstore.Store) *failingStore {
			return &failingStore{Store: b, headsOK: 2, corrupt: true}
		}, nil}, // a decode error, reported as such
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			backing := sim.NewObjectStore()
			if _, err := epoch.NewStore(backing).Init(ctx, vol, 2); err != nil {
				t.Fatal(err)
			}
			s := epoch.NewStore(tc.store(backing))
			ep, etag, err := s.Current(ctx, vol)
			if err == nil {
				t.Fatalf("Current returned epoch %d / %q on a failed read", ep, etag)
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			// A failed read must never be reported as a usable pair.
			if ep != 0 || etag != "" {
				t.Fatalf("Current returned (%d, %q) alongside %v", ep, etag, err)
			}
		})
	}
}

// TestCompareAndAdvanceStillLosesARaceAfterItsPreCheck: the forward-only check reads
// the object first, but the read is not what makes the advance safe — the If-Match
// is. A promoter that advances the object in the window between the two must still
// win, and ours must come back as a CAS conflict rather than overwriting it.
func TestCompareAndAdvanceStillLosesARaceAfterItsPreCheck(t *testing.T) {
	ctx := context.Background()
	backing := sim.NewObjectStore()
	h := &hookedStore{Store: backing}
	loser := epoch.NewStore(h)
	winner := epoch.NewStore(backing)

	etag, err := winner.Init(ctx, vol, 3)
	if err != nil {
		t.Fatal(err)
	}
	h.beforePut = func() {
		if _, err := winner.CompareAndAdvance(ctx, vol, etag, 4); err != nil {
			t.Errorf("winner advance: %v", err)
		}
	}

	if _, err := loser.CompareAndAdvance(ctx, vol, etag, 4); !errors.Is(err, epoch.ErrCASConflict) {
		t.Fatalf("racing advance: err = %v, want ErrCASConflict", err)
	}
	ep, _, err := winner.Current(ctx, vol)
	if err != nil || ep != 4 {
		t.Fatalf("epoch object = %d err=%v, want the winner's 4", ep, err)
	}
}

// TestCompareAndAdvanceRefusesToGoBackwards: epoch numbers are the namespace of the
// WAL objects (wal/<vol>/<epoch>/). Re-issuing one — after a bucket rollback, a
// restored backup, or a caller that recomputed a target from stale state — puts two
// writers in one key namespace, which recovery can only read as a truncated or
// spliced history. The store refuses it rather than trusting caller convention.
func TestCompareAndAdvanceRefusesToGoBackwards(t *testing.T) {
	tests := []struct {
		name     string
		to       uint64
		wantErr  error
		wantLeft uint64
	}{
		{"backwards", 4, epoch.ErrEpochNotAdvancing, 5},
		{"same epoch again", 5, epoch.ErrEpochNotAdvancing, 5},
		{"zero", 0, epoch.ErrEpochNotAdvancing, 5},
		{"forwards", 6, nil, 6},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := epoch.NewStore(sim.NewObjectStore())
			etag, err := s.Init(ctx, vol, 5)
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.CompareAndAdvance(ctx, vol, etag, tc.to)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("advance 5 -> %d: err = %v, want %v", tc.to, err, tc.wantErr)
			}
			ep, _, err := s.Current(ctx, vol)
			if err != nil || ep != tc.wantLeft {
				t.Fatalf("epoch object = %d err=%v, want %d", ep, err, tc.wantLeft)
			}
		})
	}
}
