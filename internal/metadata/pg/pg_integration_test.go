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
