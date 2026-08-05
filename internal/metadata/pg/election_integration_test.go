//go:build integration

package pg_test

import (
	"testing"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/metadata/pg"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// TestATermIsNeverIssuedTwiceAcrossADatabaseRestore is ADR-0011 against a real
// PostgreSQL: the term is derived from one row, so restoring the database rewinds it
// and the next election hands out a term a live leader is still using — both then
// pass `(SELECT term ...) = N` on every mutation and neither is a zombie by any check
// the system has.
//
// The restore is reproduced by rewinding control_plane_leader.term directly rather
// than by pg_restore. That is deliberate and it is the whole of what a PITR does to
// this row; what it does not reproduce is the rest of the catalog moving back with
// it, which no assertion here depends on. Said plainly so nobody reads this as proof
// that a full restore was exercised.
func TestATermIsNeverIssuedTwiceAcrossADatabaseRestore(t *testing.T) {
	ctx := t.Context()
	pool := startPostgres(t)
	if _, err := pool.Exec(ctx, `TRUNCATE snapshots, volumes, host_leases, hosts, control_plane_leader`); err != nil {
		t.Fatalf("reset: %v", err)
	}
	store := pg.New(pool)
	bucket := sim.NewObjectStore()
	elector := controlplane.NewElector(store, bucket)

	a, err := elector.Acquire(ctx, "cp-a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := elector.Acquire(ctx, "cp-b")
	if err != nil {
		t.Fatal(err)
	}
	if b <= a {
		t.Fatalf("terms did not advance: %d then %d", a, b)
	}

	// The restore: the row goes back to before cp-a's term, while cp-b is still live
	// and still using term b.
	if _, err := pool.Exec(ctx, `UPDATE control_plane_leader SET term = $1 WHERE singleton`, a-1); err != nil {
		t.Fatalf("rewind: %v", err)
	}

	c, err := elector.Acquire(ctx, "cp-c")
	if err != nil {
		t.Fatal(err)
	}
	if c <= b {
		t.Fatalf("after the restore the elector issued term %d, which cp-b still holds", c)
	}

	// And the claims are the reason it could tell: every term ever issued is in the
	// bucket, whatever the database says.
	highest, err := elector.HighestClaimedTerm(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if highest != c {
		t.Fatalf("highest claimed term = %d, want %d", highest, c)
	}

	// cp-b is now the zombie: the row is at c, so its mutations affect 0 rows. That is
	// the §7 guard working again, which it was not doing between the restore and here.
	leader, err := store.GetLeader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if leader.Term != c || leader.HolderID != "cp-c" {
		t.Fatalf("leader = %+v, want term %d held by cp-c", leader, c)
	}
}
