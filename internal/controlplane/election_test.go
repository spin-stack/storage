package controlplane_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// The Control Plane term is the root of §7: every mutation is guarded by it, so a
// zombie affects 0 rows — as long as the term only ever moves forward. It lives in
// one row of one table and is derived from that row, so a restore of the database
// rewinds it, and the next election hands out a term a live leader is still using.
// ADR-0011 anchors it: a term is claimed create-only in the object store before it is
// used, which makes the claims, not the row, the authority on what has been issued.

// scriptedTerms is a Store whose elections return a fixed sequence — a database that
// was rewound hands the same term out twice, and nothing inside PostgreSQL can tell.
type scriptedTerms struct {
	metadata.Store
	terms []int64
	n     int
	calls int
}

func (s *scriptedTerms) AcquireLeadership(context.Context, string) (int64, error) {
	s.calls++
	if s.n >= len(s.terms) {
		return 0, errors.New("scripted terms exhausted")
	}
	t := s.terms[s.n]
	s.n++
	return t, nil
}

func (s *scriptedTerms) Now(context.Context) (time.Time, error) {
	return time.Unix(1_700_000_000, 0).UTC(), nil
}

// failingPuts is an object store whose Put always fails with err.
type failingPuts struct {
	objectstore.Store
	err error
}

func (s *failingPuts) Put(context.Context, string, []byte, objectstore.PutOptions) (objectstore.PutResult, error) {
	return objectstore.PutResult{}, s.err
}

func newElectorWorld(t *testing.T) (*metasim.Store, *sim.ObjectStore) {
	t.Helper()
	return metasim.New(func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }), sim.NewObjectStore()
}

// A term nobody can see is a term nobody can be stopped from re-issuing: the claim
// has to exist, be create-only, and name its holder before the term is returned.
func TestElectorClaimsTheTermBeforeReturningIt(t *testing.T) {
	ctx := context.Background()
	md, store := newElectorWorld(t)
	e := controlplane.NewElector(md, store)

	term, err := e.Acquire(ctx, "cp-a")
	if err != nil {
		t.Fatal(err)
	}
	if term != 1 {
		t.Fatalf("first term = %d, want 1", term)
	}
	body, err := store.Get(ctx, controlplane.TermClaimKey(term))
	if err != nil {
		t.Fatalf("the term was returned without a claim in the object store: %v", err)
	}
	claim, err := controlplane.ParseTermClaim(body)
	if err != nil {
		t.Fatal(err)
	}
	if claim.Term != term || claim.HolderID != "cp-a" {
		t.Fatalf("claim = %+v, want term %d held by cp-a", claim, term)
	}

	// Create-only: the claim cannot be overwritten by a later leader.
	_, err = store.Put(ctx, controlplane.TermClaimKey(term), []byte("{}"), objectstore.PutOptions{IfNoneMatch: true})
	if !errors.Is(err, objectstore.ErrPreconditionFailed) {
		t.Fatalf("the claim is overwritable: %v", err)
	}
}

// The incident itself: the database is restored to a point before the current
// leader's term, so an election hands out a term that is already in use. The elector
// must never return it — it climbs until it finds one nobody has claimed.
func TestARewoundDatabaseCannotReissueALiveTerm(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	// Terms 1 and 2 were issued and used. The restore rewinds the row, so the next
	// three elections hand out 1, 2 and 3 again.
	md := &scriptedTerms{terms: []int64{1, 2, 1, 2, 3}}
	e := controlplane.NewElector(md, store)

	first, err := e.Acquire(ctx, "cp-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.Acquire(ctx, "cp-b")
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 || second != 2 {
		t.Fatalf("terms = %d, %d; want 1, 2", first, second)
	}

	third, err := e.Acquire(ctx, "cp-c")
	if err != nil {
		t.Fatal(err)
	}
	if third <= second {
		t.Fatalf("a rewound database re-issued term %d while %d is still live", third, second)
	}
	if got := md.calls; got < 3 {
		t.Fatalf("the elector accepted a claimed term without retrying (%d elections)", got)
	}
}

// Fail closed: a process that cannot record its claim is not the leader. Returning a
// term here would be the fail-open shape the whole ADR exists to remove — the claim
// is the only record that outlives the database.
func TestElectorFailsClosedWhenTheClaimCannotBeWritten(t *testing.T) {
	ctx := context.Background()
	md, base := newElectorWorld(t)
	boom := errors.New("object store unreachable")
	e := controlplane.NewElector(md, &failingPuts{Store: base, err: boom})

	term, err := e.Acquire(ctx, "cp-a")
	if !errors.Is(err, boom) {
		t.Fatalf("Acquire error = %v, want the store's failure", err)
	}
	if term != 0 {
		t.Fatalf("Acquire returned term %d with an unwritten claim", term)
	}
}

// A store that refuses every claim must not loop forever: an operator needs an error,
// not a process that never becomes leader and never says why.
func TestElectorGivesUpRatherThanSpinning(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	var terms []int64
	for i := int64(1); i <= 4096; i++ {
		terms = append(terms, i)
		if _, err := store.Put(ctx, controlplane.TermClaimKey(i), []byte("{}"), objectstore.PutOptions{IfNoneMatch: true}); err != nil {
			t.Fatal(err)
		}
	}
	e := controlplane.NewElector(&scriptedTerms{terms: terms}, store)

	if _, err := e.Acquire(ctx, "cp-a"); !errors.Is(err, controlplane.ErrTermClaimExhausted) {
		t.Fatalf("Acquire error = %v, want ErrTermClaimExhausted", err)
	}
}

// The claims are what a later leader reads to know the high-water mark, so they must
// sort in numeric order as strings — the key is zero-padded for exactly that reason.
func TestTermClaimKeysSortNumerically(t *testing.T) {
	tests := []struct{ lo, hi int64 }{
		{1, 2}, {9, 10}, {99, 100}, {1, 1_000_000}, {4_294_967_295, 4_294_967_296},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%d<%d", tc.lo, tc.hi), func(t *testing.T) {
			if controlplane.TermClaimKey(tc.lo) >= controlplane.TermClaimKey(tc.hi) {
				t.Fatalf("%q does not sort before %q", controlplane.TermClaimKey(tc.lo), controlplane.TermClaimKey(tc.hi))
			}
		})
	}
}

// HighestClaimedTerm is the diagnostic the ADR promises an operator: the bucket's
// view of leadership, which is the one that survives the restore.
func TestHighestClaimedTermReadsTheBucketNotTheDatabase(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	e := controlplane.NewElector(&scriptedTerms{terms: []int64{1, 2, 3}}, store)

	if got, err := e.HighestClaimedTerm(ctx); err != nil || got != 0 {
		t.Fatalf("with no claims: %d, %v; want 0, nil", got, err)
	}
	for range 3 {
		if _, err := e.Acquire(ctx, "cp-a"); err != nil {
			t.Fatal(err)
		}
	}
	got, err := e.HighestClaimedTerm(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got != 3 {
		t.Fatalf("highest claimed term = %d, want 3", got)
	}
}

// An unreadable claim is not a zero: reporting 0 would tell an operator comparing the
// bucket against the database that nothing was ever issued.
func TestHighestClaimedTermRefusesAnUnreadableClaim(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	if _, err := store.Put(ctx, controlplane.TermClaimKey(7), []byte("not json"), objectstore.PutOptions{IfNoneMatch: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := controlplane.NewElector(nil, store).HighestClaimedTerm(ctx); err == nil {
		t.Fatal("an unreadable term claim was reported as no claim at all")
	}
}
