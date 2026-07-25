//go:build integration

// Integration tests for the pg metadata adapter against a real Postgres via
// TestContainers (ADR-0006). Run with: task test:integration (requires Docker).
// These are excluded from the normal unit/lint lane by the build tag.
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

	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/metadata/pg"
	"github.com/spin-stack/storage/internal/schema"
)

func startPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:17-alpine",
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

	if _, err := pool.Exec(ctx, schema.SQL); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	return pool
}

// TestPGZombieCPCannotMutate runs the §7 verified-term property against real
// Postgres — the same contract the sim proves deterministically.
func TestPGZombieCPCannotMutate(t *testing.T) {
	ctx := context.Background()
	store := pg.New(startPostgres(t))

	termA, err := store.AcquireLeadership(ctx, "cp-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateVolume(ctx, termA, metadata.Volume{
		VolumeID: "v1", SizeBytes: 1 << 30, BlockSize: 65536, State: "ACTIVE",
		DEKWrapped: []byte{1, 2, 3}, KEKID: "kek-1",
	}); err != nil {
		t.Fatal(err)
	}

	termB, _ := store.AcquireLeadership(ctx, "cp-b") // termA now stale

	if _, err := store.BumpVolumeEpoch(ctx, termA, "v1", "host-a"); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("stale term bump: want ErrStaleTerm, got %v", err)
	}
	epoch, err := store.BumpVolumeEpoch(ctx, termB, "v1", "host-b")
	if err != nil || epoch != 1 {
		t.Fatalf("current bump: epoch=%d err=%v", epoch, err)
	}
	v, _ := store.GetVolume(ctx, "v1")
	if v.CurrentEpoch != 1 || v.PrimaryHostID != "host-b" {
		t.Fatalf("volume state wrong: %+v", v)
	}
}

func TestPGOperationIdempotency(t *testing.T) {
	ctx := context.Background()
	store := pg.New(startPostgres(t))
	op := metadata.Operation{
		OperationID:  "11111111-1111-1111-1111-111111111111",
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
