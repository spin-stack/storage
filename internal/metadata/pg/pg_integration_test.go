//go:build integration

// Integration tests for the pg metadata adapter against a real Postgres 18 via
// TestContainers (ADR-0006, ADR-0007). It applies the real Atlas migrations. Run
// with: task test:integration (requires Docker). Excluded from the unit/lint lane.
package pg_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/metadata/pg"
	"github.com/spin-stack/storage/migrations"
)

func startPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	// Not t.Context(): the container is terminated from t.Cleanup, which runs
	// *after* the test context is cancelled. A cancelled context there leaks the
	// container for the rest of the run.
	ctx := context.Background() //nolint:usetesting // see above
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
	ctx := t.Context()
	store := pg.New(startPostgres(t))

	termA, err := store.AcquireLeadership(ctx, "cp-a")
	if err != nil {
		t.Fatal(err)
	}
	volID := ids.New().String()
	if err := store.CreateVolume(ctx, termA, metadata.Volume{
		VolumeID: volID, SizeBytes: 1 << 30, BlockSize: 65536, State: lifecycle.VolumeActive,
		DEKWrapped: []byte{1, 2, 3}, KEKID: "kek-1",
	}, nil); err != nil {
		t.Fatal(err)
	}

	termB, _ := store.AcquireLeadership(ctx, "cp-b") // termA now stale

	hostA, hostB := ids.New().String(), ids.New().String()
	// The promoted host must be registered before becoming primary (FK).
	if err := store.UpsertHost(ctx, termB, metadata.Host{HostID: hostB, State: lifecycle.HostActive}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.BumpVolumeEpoch(ctx, termA, volID, hostA, 0); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("stale term bump: want ErrStaleTerm, got %v", err)
	}
	epoch, err := store.BumpVolumeEpoch(ctx, termB, volID, hostB, 0)
	if err != nil || epoch != 1 {
		t.Fatalf("current bump: epoch=%d err=%v", epoch, err)
	}
	v, _ := store.GetVolume(ctx, volID)
	if v.CurrentEpoch != 1 || v.PrimaryHostID != hostB {
		t.Fatalf("volume state wrong: %+v", v)
	}
}

func TestPGOperationIdempotency(t *testing.T) {
	ctx := t.Context()
	store := pg.New(startPostgres(t))
	term, err := store.AcquireLeadership(ctx, "cp")
	if err != nil {
		t.Fatal(err)
	}
	op := metadata.Operation{
		OperationID:  ids.New().String(),
		Kind:         lifecycle.OpAttach,
		DesiredState: []byte(`{"x":1}`),
		CurrentState: []byte(`{}`),
		Phase:        lifecycle.OpPending,
	}
	rec, err := store.RecordOperation(ctx, term, op)
	if err != nil || !rec {
		t.Fatalf("first record: rec=%v err=%v", rec, err)
	}
	rec, err = store.RecordOperation(ctx, term, op)
	if err != nil || rec {
		t.Fatalf("duplicate should report rec=false: rec=%v err=%v", rec, err)
	}

	// Visible progress of a long-running operation (§28.1).
	op.Phase = lifecycle.OpRunning
	op.CurrentState = []byte(`{"total":2,"moved":1}`)
	op.Error = "waiting for fencing"
	if err := store.UpdateOperation(ctx, term, op, nil); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetOperation(ctx, op.OperationID)
	if err != nil || got.Phase != lifecycle.OpRunning || got.Error != "waiting for fencing" {
		t.Fatalf("operation after update: %+v err=%v", got, err)
	}
	// jsonb round-trips by value, not byte-for-byte.
	var state map[string]int
	if err := json.Unmarshal(got.CurrentState, &state); err != nil || state["total"] != 2 || state["moved"] != 1 {
		t.Fatalf("current_state = %s err=%v", got.CurrentState, err)
	}
	op.OperationID = ids.New().String()
	if err := store.UpdateOperation(ctx, term, op, nil); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("update of a missing operation: want ErrNotFound, got %v", err)
	}
}

// TestPGFleetSurface exercises the §28.1/§28.2 fleet operations against real
// Postgres: cordon, capacity reservation/release with the non-negative guard, and
// the two listings a drain iterates over.
func TestPGFleetSurface(t *testing.T) {
	ctx := t.Context()
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
			HostID: id, State: lifecycle.HostActive, NVMeTotalBytes: 1000,
		}); err != nil {
			t.Fatal(err)
		}
	}

	hosts, err := store.ListHosts(ctx)
	if err != nil || len(hosts) != 2 || hosts[0].HostID != hostA || hosts[1].HostID != hostB {
		t.Fatalf("ListHosts = %+v err=%v", hosts, err)
	}

	// Cordon (§28.1).
	if err := store.SetHostState(ctx, term, hostA, lifecycle.HostCordoned); err != nil {
		t.Fatal(err)
	}
	if h, _ := store.GetHost(ctx, hostA); h.State != lifecycle.HostCordoned {
		t.Fatalf("state = %q, want CORDONED", h.State)
	}
	if err := store.SetHostState(ctx, term, ids.New().String(), lifecycle.HostCordoned); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("SetHostState on a missing host: want ErrNotFound, got %v", err)
	}

	// Capacity accounting (§28.2) is derived (ADR-0017): an empty host is committed
	// to nothing, and the number appears when a volume names it. The semantics are
	// pinned for both stores by the shared contract; this is the adapter's own round
	// trip through the host_committed_bytes view.
	if h, _ := store.GetHost(ctx, hostA); h.NVMeCommittedBytes != 0 {
		t.Fatalf("an empty host committed %d bytes", h.NVMeCommittedBytes)
	}
	staleTerm := term
	term, _ = store.AcquireLeadership(ctx, "cp-b")

	// Volumes by host: exactly the ones whose primary is that host.
	volID := ids.New().String()
	if err := store.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: volID, SizeBytes: 1 << 30, BlockSize: 65536, State: lifecycle.VolumeActive,
		DEKWrapped: []byte{1}, KEKID: "k", PrimaryHostID: hostA,
	}, nil); err != nil {
		t.Fatal(err)
	}
	if h, _ := store.GetHost(ctx, hostA); h.NVMeCommittedBytes != 1<<30 {
		t.Fatalf("committed = %d, want %d (the volume it now holds)", h.NVMeCommittedBytes, int64(1)<<30)
	}
	if err := store.CreateVolume(ctx, staleTerm, metadata.Volume{
		VolumeID: ids.New().String(), SizeBytes: 1, BlockSize: 65536, State: lifecycle.VolumeActive,
		DEKWrapped: []byte{1}, KEKID: "k",
	}, nil); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("stale-term create: want ErrStaleTerm, got %v", err)
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
	ctx := t.Context()
	store := pg.New(startPostgres(t))
	term, _ := store.AcquireLeadership(ctx, "cp")
	// A v1 UUID (version nibble 1) must be rejected by the CHECK constraint.
	err := store.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: "11111111-1111-1111-1111-111111111111", SizeBytes: 1, BlockSize: 65536,
		State: lifecycle.VolumeActive, DEKWrapped: []byte{1}, KEKID: "k",
	}, nil)
	if err == nil {
		t.Fatal("Postgres must reject a non-v7 volume_id (INV-22 CHECK)")
	}
}

// TestPGAcceptsEveryDeclaredLifecycleValue is the drift test between the Go
// vocabulary and the DB CHECK constraints: every value internal/lifecycle declares
// must be storable. Adding a state in Go and forgetting the migration fails here.
func TestPGAcceptsEveryDeclaredLifecycleValue(t *testing.T) {
	ctx := t.Context()
	pool := startPostgres(t)
	store := pg.New(pool)
	term, _ := store.AcquireLeadership(ctx, "cp")

	hostID := ids.New().String()
	if err := store.UpsertHost(ctx, term, metadata.Host{HostID: hostID, State: lifecycle.HostActive}); err != nil {
		t.Fatal(err)
	}
	volID := ids.New().String()
	if err := store.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: volID, SizeBytes: 1 << 30, BlockSize: 65536, State: lifecycle.VolumeActive,
		DEKWrapped: []byte{1}, KEKID: "k",
	}, nil); err != nil {
		t.Fatal(err)
	}
	snapID, reqID := ids.New().String(), ids.New().String()
	if err := store.CreateSnapshot(ctx, term, metadata.Snapshot{
		SnapshotID: snapID, VolumeID: volID, Epoch: 1, TargetSequence: 1,
		RootDigest: "d", State: lifecycle.SnapshotCreating, RequestID: reqID,
	}); err != nil {
		t.Fatal(err)
	}
	opID := ids.New().String()
	if _, err := store.RecordOperation(ctx, term, metadata.Operation{
		OperationID: opID, Kind: lifecycle.OpDrain, Phase: lifecycle.OpPending,
		DesiredState: []byte("{}"), CurrentState: []byte("{}"),
	}); err != nil {
		t.Fatal(err)
	}

	// Raw SQL on purpose: this asserts the constraint, not the Go guard.
	for _, s := range lifecycle.HostStates() {
		if _, err := pool.Exec(ctx, `UPDATE hosts SET state=$1 WHERE host_id=$2`, s.String(), hostID); err != nil {
			t.Fatalf("host state %q rejected by the DB: %v", s, err)
		}
	}
	for _, s := range lifecycle.VolumeStates() {
		if _, err := pool.Exec(ctx, `UPDATE volumes SET state=$1 WHERE volume_id=$2`, s.String(), volID); err != nil {
			t.Fatalf("volume state %q rejected by the DB: %v", s, err)
		}
	}
	for _, d := range lifecycle.Durabilities() {
		if _, err := pool.Exec(ctx, `UPDATE volumes SET durability=$1 WHERE volume_id=$2`, d.String(), volID); err != nil {
			t.Fatalf("durability %q rejected by the DB: %v", d, err)
		}
	}
	for _, s := range lifecycle.SnapshotStates() {
		if _, err := pool.Exec(ctx, `UPDATE snapshots SET state=$1 WHERE snapshot_id=$2`, s.String(), snapID); err != nil {
			t.Fatalf("snapshot state %q rejected by the DB: %v", s, err)
		}
	}
	for _, k := range lifecycle.OperationKinds() {
		if _, err := pool.Exec(ctx, `UPDATE operations SET kind=$1 WHERE operation_id=$2`, k.String(), opID); err != nil {
			t.Fatalf("operation kind %q rejected by the DB: %v", k, err)
		}
	}
	for _, p := range lifecycle.OperationPhases() {
		if _, err := pool.Exec(ctx, `UPDATE operations SET phase=$1 WHERE operation_id=$2`, p.String(), opID); err != nil {
			t.Fatalf("operation phase %q rejected by the DB: %v", p, err)
		}
	}
}

// TestPGRejectsValuesOutsideTheVocabulary: the CHECK constraints hold even for a
// client that never goes through the Go layer (a script, a manual psql session).
func TestPGRejectsValuesOutsideTheVocabulary(t *testing.T) {
	ctx := t.Context()
	pool := startPostgres(t)
	store := pg.New(pool)
	term, _ := store.AcquireLeadership(ctx, "cp")

	hostID := ids.New().String()
	if err := store.UpsertHost(ctx, term, metadata.Host{HostID: hostID, State: lifecycle.HostActive}); err != nil {
		t.Fatal(err)
	}
	volID := ids.New().String()
	if err := store.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: volID, SizeBytes: 1, BlockSize: 65536, State: lifecycle.VolumeActive,
		DEKWrapped: []byte{1}, KEKID: "k",
	}, nil); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		sql  string
		arg  any
	}{
		{"host state", `UPDATE hosts SET state=$1 WHERE host_id='` + hostID + `'`, "ZOMBIE"},
		{"volume state", `UPDATE volumes SET state=$1 WHERE volume_id='` + volID + `'`, "REBUILT"},
		{"durability", `UPDATE volumes SET durability=$1 WHERE volume_id='` + volID + `'`, "eventual"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := pool.Exec(ctx, tc.sql, tc.arg); err == nil {
				t.Fatalf("the DB accepted %q", tc.arg)
			}
		})
	}
}

// TestPGOperationPhaseGuardIsAtomic: the phase transition is enforced by the UPDATE
// predicate itself, so a terminal operation cannot be resurrected even under
// concurrent writers.
func TestPGOperationPhaseGuardIsAtomic(t *testing.T) {
	ctx := t.Context()
	store := pg.New(startPostgres(t))
	term, _ := store.AcquireLeadership(ctx, "cp")

	op := metadata.Operation{
		OperationID: ids.New().String(), Kind: lifecycle.OpDrain, Phase: lifecycle.OpPending,
		DesiredState: []byte("{}"), CurrentState: []byte("{}"),
	}
	if _, err := store.RecordOperation(ctx, term, op); err != nil {
		t.Fatal(err)
	}
	op.Phase = lifecycle.OpRunning
	if err := store.UpdateOperation(ctx, term, op, nil); err != nil {
		t.Fatal(err)
	}
	op.Phase = lifecycle.OpSucceeded
	if err := store.UpdateOperation(ctx, term, op, nil); err != nil {
		t.Fatal(err)
	}
	op.Phase = lifecycle.OpRunning
	if err := store.UpdateOperation(ctx, term, op, nil); !errors.Is(err, lifecycle.ErrInvalidTransition) {
		t.Fatalf("SUCCEEDED -> RUNNING: want ErrInvalidTransition, got %v", err)
	}
	got, _ := store.GetOperation(ctx, op.OperationID)
	if got.Phase != lifecycle.OpSucceeded {
		t.Fatalf("phase = %q after a refused transition", got.Phase)
	}
}

// TestPGEveryForeignKeyHasAnIndex is the structural rule from schema.sql: Postgres
// indexes the referenced side of a foreign key (the primary key) but never the
// referencing column, so without an explicit index every parent DELETE/UPDATE — a
// host being decommissioned, a volume removed — sequentially scans the child table
// while holding locks. This fails the moment a FK is added without its index.
func TestPGEveryForeignKeyHasAnIndex(t *testing.T) {
	ctx := t.Context()
	pool := startPostgres(t)

	const q = `
SELECT c.conrelid::regclass::text AS child_table, a.attname AS column_name, c.conname
  FROM pg_constraint c
  JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1]
 WHERE c.contype = 'f'
   AND c.connamespace = 'public'::regnamespace
   AND NOT EXISTS (
       SELECT 1 FROM pg_index i
        WHERE i.indrelid = c.conrelid
          AND i.indkey[0] = c.conkey[1]
   )
 ORDER BY 1, 2`
	rows, err := pool.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var unindexed []string
	for rows.Next() {
		var table, column, constraint string
		if err := rows.Scan(&table, &column, &constraint); err != nil {
			t.Fatal(err)
		}
		unindexed = append(unindexed, table+"."+column+" ("+constraint+")")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(unindexed) > 0 {
		t.Fatalf("foreign keys without a supporting index: %v", unindexed)
	}
}

// TestPGListVolumesByHostUsesItsIndex proves the composite index is not decorative:
// with a realistic row count the planner uses it for the drain's iteration query
// (§28.1) instead of scanning every volume in the fleet.
func TestPGListVolumesByHostUsesItsIndex(t *testing.T) {
	ctx := t.Context()
	pool := startPostgres(t)
	store := pg.New(pool)
	term, _ := store.AcquireLeadership(ctx, "cp")

	// Two hosts; one owns a handful of volumes, the other owns the rest.
	hotHost, coldHost := ids.New().String(), ids.New().String()
	for _, h := range []string{hotHost, coldHost} {
		if err := store.UpsertHost(ctx, term, metadata.Host{HostID: h, State: lifecycle.HostActive}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 2000 {
		host := coldHost
		if i%400 == 0 {
			host = hotHost
		}
		if err := store.CreateVolume(ctx, term, metadata.Volume{
			VolumeID: ids.New().String(), SizeBytes: 1 << 30, BlockSize: 65536,
			State: lifecycle.VolumeActive, PrimaryHostID: host, DEKWrapped: []byte{1}, KEKID: "k",
		}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `ANALYZE volumes`); err != nil {
		t.Fatal(err)
	}

	assertIndexed(t, pool, "volumes_primary_host_id_volume_id_idx", "volumes",
		`EXPLAIN SELECT * FROM volumes WHERE primary_host_id = $1 ORDER BY volume_id`, hotHost)
}

// TestPGListOperationsByHostUsesItsIndex is the same rule for the other filter +
// ORDER BY query. It runs before every pass of every drain, against a table that
// only grows: completed operations are history and nothing deletes them, so a
// sequential scan here gets slower for the rest of the cluster's life.
func TestPGListOperationsByHostUsesItsIndex(t *testing.T) {
	ctx := t.Context()
	pool := startPostgres(t)
	store := pg.New(pool)
	term, _ := store.AcquireLeadership(ctx, "cp")

	hotHost, coldHost := ids.New().String(), ids.New().String()
	for _, h := range []string{hotHost, coldHost} {
		if err := store.UpsertHost(ctx, term, metadata.Host{HostID: h, State: lifecycle.HostActive}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 2000 {
		host := coldHost
		if i%400 == 0 {
			host = hotHost
		}
		if _, err := store.RecordOperation(ctx, term, metadata.Operation{
			OperationID: ids.New().String(), Kind: lifecycle.OpDrain, HostID: host,
			Phase: lifecycle.OpSucceeded, DesiredState: []byte(`{}`), CurrentState: []byte(`{}`),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `ANALYZE operations`); err != nil {
		t.Fatal(err)
	}

	assertIndexed(t, pool, "operations_host_id_operation_id_idx", "operations",
		`EXPLAIN SELECT * FROM operations WHERE host_id = $1 ORDER BY operation_id`, hotHost)
}

// assertIndexed fails unless the planner reaches for index on table for query.
func assertIndexed(t *testing.T, pool *pgxpool.Pool, index, table, query string, args ...any) {
	t.Helper()
	rows, err := pool.Query(t.Context(), query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan += line + "\n"
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, index) {
		t.Fatalf("the planner did not use %s:\n%s", index, plan)
	}
	if strings.Contains(plan, "Seq Scan on "+table) {
		t.Fatalf("the query still scans the whole %s table:\n%s", table, plan)
	}
}
