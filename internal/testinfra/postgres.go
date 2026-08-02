//go:build integration || e2e

package testinfra

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/spin-stack/storage/internal/schema"
)

// PostgresImage is the version everything runs against — the db:* tasks' database, the
// integration lane, and production (ADR-0019). Not a var: a lane that quietly ran a
// different major version would prove nothing about the one that ships.
const PostgresImage = "postgres:18-alpine"

// Postgres starts a Postgres 18 with `schema.sql` applied and returns its DSN.
//
// It returns a *connection string* rather than a pool because its caller is a
// subprocess: `control-plane -database-url`. A test that wants a pool opens one from
// the same DSN, which is also the honest arrangement — the binary and the test are two
// clients of one database, exactly as an operator's psql would be.
func Postgres(t *testing.T) string {
	t.Helper()
	// Not t.Context(): the container is terminated from t.Cleanup, which runs *after*
	// the test context is cancelled. A cancelled context there leaks the container for
	// the rest of the run.
	ctx := context.Background() //nolint:usetesting // see above

	container, err := tcpostgres.Run(ctx, PostgresImage,
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

	// The schema is built from the declared state itself (ADR-0019): the lane must run
	// against what `schema.sql` says, not against a migration chain that could have
	// drifted from it. pgx uses the simple protocol for an argument-less Exec, so the
	// whole multi-statement file goes in one round trip.
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, schema.SQL); err != nil {
		t.Fatalf("apply schema.sql: %v", err)
	}
	return dsn
}
