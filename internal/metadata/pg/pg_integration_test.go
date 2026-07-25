//go:build integration

// Integration tests for the pg metadata adapter against a real Postgres 18 via
// TestContainers (ADR-0006, ADR-0007). It applies the real Atlas migrations. Run
// with: task test:integration (requires Docker). Excluded from the unit/lint lane.
package pg_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/metadata/pg"
	"github.com/spin-stack/storage/migrations"
)

func startPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:18-alpine",
		tcpostgres.WithDatabase("cp"),
		tcpostgres.WithUsername("cp"),
		tcpostgres.WithPassword("cp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	// Apply the real, versioned Atlas migrations in order.
	stmts, err := migrations.Ordered()
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range stmts {
		if _, err := pool.Exec(ctx, s); err != nil {
			t.Fatalf("apply migration %d: %v", i, err)
		}
	}
	return pool
}

func TestPGZombieCPCannotMutate(t *testing.T) {
	ctx := context.Background()
	store := pg.New(startPostgres(t))

	termA, err := store.AcquireLeadership(ctx, "cp-a")
	if err != nil {
		t.Fatal(err)
	}
	volID := ids.New().String()
	if err := store.CreateVolume(ctx, termA, metadata.Volume{
		VolumeID: volID, SizeBytes: 1 << 30, BlockSize: 65536, State: "ACTIVE",
		DEKWrapped: []byte{1, 2, 3}, KEKID: "kek-1",
	}); err != nil {
		t.Fatal(err)
	}

	termB, _ := store.AcquireLeadership(ctx, "cp-b") // termA now stale

	hostA, hostB := ids.New().String(), ids.New().String()
	// The promoted host must be registered before becoming primary (FK).
	if err := store.UpsertHost(ctx, termB, metadata.Host{HostID: hostB, State: "ACTIVE"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.BumpVolumeEpoch(ctx, termA, volID, hostA); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("stale term bump: want ErrStaleTerm, got %v", err)
	}
	epoch, err := store.BumpVolumeEpoch(ctx, termB, volID, hostB)
	if err != nil || epoch != 1 {
		t.Fatalf("current bump: epoch=%d err=%v", epoch, err)
	}
	v, _ := store.GetVolume(ctx, volID)
	if v.CurrentEpoch != 1 || v.PrimaryHostID != hostB {
		t.Fatalf("volume state wrong: %+v", v)
	}
}

func TestPGOperationIdempotency(t *testing.T) {
	ctx := context.Background()
	store := pg.New(startPostgres(t))
	op := metadata.Operation{
		OperationID:  ids.New().String(),
		Kind:         "attach",
		DesiredState: []byte(`{"x":1}`),
		CurrentState: []byte(`{}`),
		Phase:        "pending",
	}
	rec, err := store.RecordOperation(ctx, op)
	if err != nil || !rec {
		t.Fatalf("first record: rec=%v err=%v", rec, err)
	}
	rec, err = store.RecordOperation(ctx, op)
	if err != nil || rec {
		t.Fatalf("duplicate should report rec=false: rec=%v err=%v", rec, err)
	}
}

// TestPGFleetSurface exercises the §28.1/§28.2 fleet operations against real
// Postgres: cordon, capacity reservation/release with the non-negative guard, and
// the two listings a drain iterates over.
func TestPGFleetSurface(t *testing.T) {
	ctx := context.Background()
	store := pg.New(startPostgres(t))
	term, err := store.AcquireLeadership(ctx, "cp")
	if err != nil {
		t.Fatal(err)
	}

	hostA, hostB := ids.New().String(), ids.New().String()
	if hostB < hostA {
		hostA, hostB = hostB, hostA // ListHosts is ordered by id
	}
	for _, id := range []string{hostA, hostB} {
		if err := store.UpsertHost(ctx, term, metadata.Host{
			HostID: id, State: metadata.HostActive, NVMeTotalBytes: 1000,
		}); err != nil {
			t.Fatal(err)
		}
	}

	hosts, err := store.ListHosts(ctx)
	if err != nil || len(hosts) != 2 || hosts[0].HostID != hostA || hosts[1].HostID != hostB {
		t.Fatalf("ListHosts = %+v err=%v", hosts, err)
	}

	// Cordon (§28.1).
	if err := store.SetHostState(ctx, term, hostA, metadata.HostCordoned); err != nil {
		t.Fatal(err)
	}
	if h, _ := store.GetHost(ctx, hostA); h.State != metadata.HostCordoned {
		t.Fatalf("state = %q, want CORDONED", h.State)
	}
	if err := store.SetHostState(ctx, term, ids.New().String(), metadata.HostCordoned); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("SetHostState on a missing host: want ErrNotFound, got %v", err)
	}

	// Capacity accounting (§28.2): reserve, release, and refuse to go negative.
	if err := store.CommitHostCapacity(ctx, term, hostA, 700); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitHostCapacity(ctx, term, hostA, -200); err != nil {
		t.Fatal(err)
	}
	if h, _ := store.GetHost(ctx, hostA); h.NVMeCommittedBytes != 500 {
		t.Fatalf("committed = %d, want 500", h.NVMeCommittedBytes)
	}
	if err := store.CommitHostCapacity(ctx, term, hostA, -501); !errors.Is(err, metadata.ErrCapacityUnderflow) {
		t.Fatalf("over-release: want ErrCapacityUnderflow, got %v", err)
	}
	if h, _ := store.GetHost(ctx, hostA); h.NVMeCommittedBytes != 500 {
		t.Fatalf("failed release mutated committed to %d", h.NVMeCommittedBytes)
	}
	staleTerm := term
	term, _ = store.AcquireLeadership(ctx, "cp-b")
	if err := store.CommitHostCapacity(ctx, staleTerm, hostA, 1); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("stale-term commit: want ErrStaleTerm, got %v", err)
	}

	// Volumes by host: exactly the ones whose primary is that host.
	volID := ids.New().String()
	if err := store.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: volID, SizeBytes: 1 << 30, BlockSize: 65536, State: "ACTIVE",
		DEKWrapped: []byte{1}, KEKID: "k", PrimaryHostID: hostA,
	}); err != nil {
		t.Fatal(err)
	}
	vols, err := store.ListVolumesByHost(ctx, hostA)
	if err != nil || len(vols) != 1 || vols[0].VolumeID != volID {
		t.Fatalf("ListVolumesByHost(hostA) = %+v err=%v", vols, err)
	}
	if vols, _ := store.ListVolumesByHost(ctx, hostB); len(vols) != 0 {
		t.Fatalf("ListVolumesByHost(hostB) = %+v, want empty", vols)
	}
}

// TestPGRejectsNonV7 proves the DB-layer INV-22 enforcement: a v4 id is refused.
func TestPGRejectsNonV7(t *testing.T) {
	ctx := context.Background()
	store := pg.New(startPostgres(t))
	term, _ := store.AcquireLeadership(ctx, "cp")
	// A v1 UUID (version nibble 1) must be rejected by the CHECK constraint.
	err := store.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: "11111111-1111-1111-1111-111111111111", SizeBytes: 1, BlockSize: 65536,
		State: "ACTIVE", DEKWrapped: []byte{1}, KEKID: "k",
	})
	if err == nil {
		t.Fatal("Postgres must reject a non-v7 volume_id (INV-22 CHECK)")
	}
}
