//go:build integration

// Integration tests for the pg metadata adapter against a real Postgres 18 via
// TestContainers (ADR-0007). It builds the database from
// internal/schema/schema.sql — the declared state that is the source of truth
// (ADR-0019) — so what these tests run against is the artefact sqlc generates from
// and `task db:verify` checks, not a replay that could have drifted from it. Run
// with: task test:integration (requires Docker). Excluded from the unit/lint lane.
package pg_test

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
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
	"github.com/spin-stack/storage/internal/schema"
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

	// Build the schema from the declared state itself (pgx uses the simple protocol
	// for an argument-less Exec, so the whole multi-statement file goes in one go).
	if _, err := pool.Exec(ctx, schema.SQL); err != nil {
		t.Fatalf("apply schema.sql: %v", err)
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
		DEKWrapped: []byte{1, 2, 3}, KEKID: "kek-1", DEKKeyID: 1,
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
		DEKWrapped: []byte{1}, KEKID: "k", PrimaryHostID: hostA, DEKKeyID: 1,
	}, nil); err != nil {
		t.Fatal(err)
	}
	if h, _ := store.GetHost(ctx, hostA); h.NVMeCommittedBytes != 1<<30 {
		t.Fatalf("committed = %d, want %d (the volume it now holds)", h.NVMeCommittedBytes, int64(1)<<30)
	}
	if err := store.CreateVolume(ctx, staleTerm, metadata.Volume{
		VolumeID: ids.New().String(), SizeBytes: 1, BlockSize: 65536, State: lifecycle.VolumeActive,
		DEKWrapped: []byte{1}, KEKID: "k", DEKKeyID: 1,
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
		State: lifecycle.VolumeActive, DEKWrapped: []byte{1}, KEKID: "k", DEKKeyID: 1,
	}, nil)
	if err == nil {
		t.Fatal("Postgres must reject a non-v7 volume_id (INV-22 CHECK)")
	}
}

// TestPGRejectsNonV7OnEveryIdentityColumn is INV-22 on the whole schema rather than
// on the one column a Store method happens to reach. Every identity column carries
// the version-nibble rule, and the rule is the same rule: an id generated by a
// client that skipped internal/ids is refused wherever it is written.
//
// Raw SQL, one INSERT per column, so it asserts the database and not the adapter —
// and it drives a valid v7 row through the same statement first, so a schema that
// rejected *everything* could not pass by rejecting the v4 too.
func TestPGRejectsNonV7OnEveryIdentityColumn(t *testing.T) {
	ctx := t.Context()
	pool := startPostgres(t)

	// A row for each table, parameterised on the one id under test. The other ids in
	// the statement are freshly generated v7s, so the only thing that can make the
	// insert fail is the column being probed.
	tests := []struct {
		name   string
		insert string
		row    func(id string) []any
	}{
		{
			name:   "hosts.host_id",
			insert: `INSERT INTO hosts (host_id, state, last_heartbeat) VALUES ($1, 'ACTIVE', now())`,
			row:    func(id string) []any { return []any{id} },
		},
		{
			name: "volumes.volume_id",
			insert: `INSERT INTO volumes (volume_id, size_bytes, block_size, state, dek_wrapped, kek_id, dek_key_id)
			         VALUES ($1, 1, 65536, 'ACTIVE', '\x01', 'k', 1)`,
			row: func(id string) []any { return []any{id} },
		},
		{
			name: "snapshots.snapshot_id",
			insert: `INSERT INTO snapshots (snapshot_id, volume_id, epoch, target_sequence, root_digest, state, request_id)
			         VALUES ($1, $2, 1, 1, 'd', 'CREATING', $3)`,
			row: func(id string) []any { return []any{id, seedVolume, ids.New().String()} },
		},
		{
			name: "snapshots.request_id",
			insert: `INSERT INTO snapshots (snapshot_id, volume_id, epoch, target_sequence, root_digest, state, request_id)
			         VALUES ($2, $3, 1, 1, 'd', 'CREATING', $1)`,
			row: func(id string) []any { return []any{id, ids.New().String(), seedVolume} },
		},
		{
			name: "operations.operation_id",
			insert: `INSERT INTO operations (operation_id, kind, desired_state, current_state, phase)
			         VALUES ($1, 'drain', '{}', '{}', 'PENDING')`,
			row: func(id string) []any { return []any{id} },
		},
	}

	// The volume the snapshot rows hang off; its id is a valid v7.
	if _, err := pool.Exec(ctx,
		`INSERT INTO volumes (volume_id, size_bytes, block_size, state, dek_wrapped, kek_id, dek_key_id)
		 VALUES ($1, 1, 65536, 'ACTIVE', '\x01', 'k', 1)`, seedVolume); err != nil {
		t.Fatal(err)
	}

	// Every non-v7 version this project could plausibly be handed: v4 (the default
	// of most libraries), v1, and the nil UUID.
	rejected := []string{
		"5b1f9c1e-6c2f-4a1d-9f3a-2f6a1b2c3d4e", // v4
		"11111111-1111-1111-1111-111111111111", // v1
		"00000000-0000-0000-0000-000000000000", // nil
	}
	// The two root ids nothing writes yet are probed by UPDATE for the same reason
	// they are covered at all: a column whose rule arrives with its first writer
	// arrives without one.
	for _, col := range []string{"active_root_id", "published_root_id"} {
		tests = append(tests, struct {
			name   string
			insert string
			row    func(id string) []any
		}{
			name:   "volumes." + col,
			insert: `UPDATE volumes SET ` + col + ` = $1 WHERE volume_id = $2`,
			row:    func(id string) []any { return []any{id, seedVolume} },
		})
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := pool.Exec(ctx, tc.insert, tc.row(ids.New().String())...); err != nil {
				t.Fatalf("a v7 id must be accepted here: %v", err)
			}
			for _, bad := range rejected {
				if _, err := pool.Exec(ctx, tc.insert, tc.row(bad)...); err == nil {
					t.Fatalf("the database accepted %s in %s (INV-22)", bad, tc.name)
				}
			}
		})
	}
}

// seedVolume is the v7 volume id the snapshot cases above reference.
var seedVolume = ids.New().String()

// TestPGIdentityColumnsUseTheUUIDv7Domain is INV-22 stated once instead of once per
// column. The rule used to be a predicate copied onto every identity column, so it
// held for the columns somebody remembered and silently did not for a new one; as a
// domain it is a type, and a column gets the rule by being declared with it.
//
// The exemption is the foreign-key referencing columns: they can only hold a value
// that is already in a v7-checked primary key, so the rule reaches them
// transitively. Every other uuid column in the schema must carry the domain, which
// is what makes this fail when a table is added with a plain `uuid` identity column.
func TestPGIdentityColumnsUseTheUUIDv7Domain(t *testing.T) {
	ctx := t.Context()
	pool := startPostgres(t)

	const q = `
SELECT c.table_name, c.column_name, COALESCE(c.domain_name, '')
  FROM information_schema.columns c
 WHERE c.table_schema = 'public'
   AND c.udt_name = 'uuid'
   AND NOT EXISTS (
       SELECT 1 FROM pg_constraint fk
        WHERE fk.contype = 'f'
          AND fk.conrelid = (quote_ident(c.table_schema) || '.' || quote_ident(c.table_name))::regclass
          AND c.column_name = ANY (
              SELECT a.attname FROM pg_attribute a
               WHERE a.attrelid = fk.conrelid AND a.attnum = ANY (fk.conkey))
   )
 ORDER BY 1, 2`
	rows, err := pool.Query(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var plain []string
	var checked int
	for rows.Next() {
		var table, column, domain string
		if err := rows.Scan(&table, &column, &domain); err != nil {
			t.Fatal(err)
		}
		checked++
		if domain != "uuidv7" {
			plain = append(plain, table+"."+column)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if checked == 0 {
		t.Fatal("the query found no identity columns at all — it is asserting nothing")
	}
	if len(plain) > 0 {
		t.Fatalf("identity columns typed uuid instead of the uuidv7 domain: %v", plain)
	}
}

// TestPGWatermarkOrderIsAConstraint proves INV-03 is structural, not merely a rule
// the queries and the Go caller happen to follow: the row itself cannot be written
// out of order. It matters because published/durable/local is the number an operator
// reads during an incident to decide whether to accept data loss (§5.6), and a
// disordered triple is not a wrong number — it is three numbers that cannot all be
// true, from which no decision can be taken at all.
//
// Raw SQL on purpose: this asserts the constraint, not the Go guard above it.
func TestPGWatermarkOrderIsAConstraint(t *testing.T) {
	ctx := t.Context()
	pool := startPostgres(t)
	store := pg.New(pool)
	term, _ := store.AcquireLeadership(ctx, "cp")

	volID := ids.New().String()
	if err := store.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: volID, SizeBytes: 1 << 30, BlockSize: 65536, State: lifecycle.VolumeActive,
		DEKWrapped: []byte{1}, KEKID: "k", DEKKeyID: 1,
		LocalSequence: 100, DurableSequence: 90, PublishedSequence: 80,
	}, nil); err != nil {
		t.Fatal(err)
	}

	// Both ends of the predicate, each broken on its own.
	tests := []struct {
		name string
		sql  string
	}{
		{"published above durable", `UPDATE volumes SET published_sequence = 95 WHERE volume_id = $1`},
		{"durable above local", `UPDATE volumes SET durable_sequence = 101 WHERE volume_id = $1`},
		{"local below both", `UPDATE volumes SET local_sequence = 70 WHERE volume_id = $1`},
		// dek_key_id is supplied so this row fails for the reason the case is named
		// after. Without it the INSERT trips the NOT NULL first and the test would
		// pass while proving nothing about the watermark ordering.
		{"inserted out of order", `INSERT INTO volumes (volume_id, size_bytes, block_size, state,
			dek_wrapped, kek_id, dek_key_id, local_sequence, durable_sequence, published_sequence)
			VALUES ($1, 1, 65536, 'ACTIVE', '\x01', 'k', 1, 1, 2, 3)`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			arg := volID
			if strings.HasPrefix(tc.sql, "INSERT") {
				arg = ids.New().String()
			}
			if _, err := pool.Exec(ctx, tc.sql, arg); err == nil {
				t.Fatal("the database accepted watermarks out of order (INV-03)")
			}
		})
	}

	// The row is untouched, and a move that keeps the order is still allowed.
	v, err := store.GetVolume(ctx, volID)
	if err != nil {
		t.Fatal(err)
	}
	if v.LocalSequence != 100 || v.DurableSequence != 90 || v.PublishedSequence != 80 {
		t.Fatalf("a refused write still mutated the row: %+v", v)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE volumes SET local_sequence = 200, durable_sequence = 200, published_sequence = 200
		  WHERE volume_id = $1`, volID); err != nil {
		t.Fatalf("an ordered write must still be accepted: %v", err)
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
		DEKWrapped: []byte{1}, KEKID: "k", DEKKeyID: 1,
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
		DEKWrapped: []byte{1}, KEKID: "k", DEKKeyID: 1,
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
          -- A partial index does not do this job: the parent DELETE has to find
          -- *every* child row, and rows outside the predicate are not in it. Since
          -- the schema now carries partial indexes over the same leading columns
          -- (the live-operation ones), saying so is no longer hypothetical.
          AND i.indpred IS NULL
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
			State: lifecycle.VolumeActive, PrimaryHostID: host, DEKWrapped: []byte{1}, KEKID: "k", DEKKeyID: 1,
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

// TestPGListLiveOperationsByHostUsesItsPartialIndex is the same rule as the volumes
// one, for the query that runs before every pass of every drain. The table it reads
// only grows — completed operations are history and nothing deletes them — so the
// index that matters is the one over the live ones: a full index on (host_id,
// operation_id) still walks every operation the host has ever had.
func TestPGListLiveOperationsByHostUsesItsPartialIndex(t *testing.T) {
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
	// History, plus a couple of live operations on the hot host: the shape the drain
	// actually meets, where "what is happening here" is two rows inside thousands.
	for i := range fleetOperations {
		host := coldHost
		if i%400 == 0 {
			host = hotHost
		}
		op := metadata.Operation{
			OperationID: ids.New().String(), Kind: lifecycle.OpAttach, HostID: host,
			Phase: lifecycle.OpSucceeded, DesiredState: []byte(`{}`), CurrentState: []byte(`{}`),
		}
		if i == 0 || i == 400 {
			op.Phase = lifecycle.OpPending
		}
		if _, err := store.RecordOperation(ctx, term, op); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `ANALYZE operations`); err != nil {
		t.Fatal(err)
	}

	assertIndexed(t, pool, "operations_live_by_host_idx", "operations",
		`EXPLAIN SELECT * FROM operations
		  WHERE host_id = $1 AND phase NOT IN ('SUCCEEDED', 'CANCELED')
		  ORDER BY operation_id`, hotHost)
}

// TestPGLivePhaseSetsAgreeWithTheLifecycle is the price of the two partial indexes,
// paid in a test. "Live" is now written in two places — the transition table in
// internal/lifecycle, which is the authority, and the predicates of the indexes —
// and a schema that disagrees with the vocabulary is worse than no index at all: the
// planner would silently stop using it (an operation whose phase the predicate does
// not cover is invisible to the index), and the uniqueness that stops a second drain
// would stop applying to exactly the phase that drifted.
//
// It does not parse the predicate: it asks PostgreSQL to *evaluate* the real one,
// once per value of the vocabulary, and compares the answer with lifecycle's own.
func TestPGLivePhaseSetsAgreeWithTheLifecycle(t *testing.T) {
	ctx := t.Context()
	pool := startPostgres(t)

	predicate := func(index string) string {
		var expr string
		if err := pool.QueryRow(ctx,
			`SELECT pg_get_expr(indpred, indrelid) FROM pg_index WHERE indexrelid = $1::regclass`,
			index).Scan(&expr); err != nil {
			t.Fatalf("%s has no partial predicate to read: %v", index, err)
		}
		return expr
	}
	// Evaluating the predicate against a one-row relation that supplies the columns
	// it names is what makes this an agreement test rather than a spelling test.
	holds := func(expr, kind string, phase lifecycle.OperationPhase) bool {
		var ok bool
		q := `SELECT ` + expr + ` FROM (SELECT $1::text AS kind, $2::text AS phase) o`
		if err := pool.QueryRow(ctx, q, kind, phase.String()).Scan(&ok); err != nil {
			t.Fatalf("evaluating %q: %v", expr, err)
		}
		return ok
	}

	live := predicate("operations_live_by_host_idx")
	drain := predicate("operations_one_live_drain_per_host_idx")
	for _, phase := range lifecycle.OperationPhases() {
		for _, kind := range lifecycle.OperationKinds() {
			if got, want := holds(live, kind.String(), phase), !phase.Terminal(); got != want {
				t.Errorf("%s covers phase %s = %v, lifecycle says live = %v\n  %s",
					"operations_live_by_host_idx", phase, got, want, live)
			}
			want := !phase.Terminal() && kind == lifecycle.OpDrain
			if got := holds(drain, kind.String(), phase); got != want {
				t.Errorf("one-live-drain covers (%s, %s) = %v, want %v\n  %s",
					kind, phase, got, want, drain)
			}
		}
	}
}

// TestPGOneLiveDrainPerHostUnderConcurrency is the race the Control Plane's own
// check cannot close. Wave 3 read the host's operations and then wrote, which is not
// exclusion: two goroutines inside one leader can both pass the read. Here they
// both write, at once, and the database has to make exactly one of them win — with
// an error the loser can act on rather than an integrity code it can only log.
func TestPGOneLiveDrainPerHostUnderConcurrency(t *testing.T) {
	ctx := t.Context()
	store := pg.New(startPostgres(t))
	term, _ := store.AcquireLeadership(ctx, "cp")

	host := ids.New().String()
	if err := store.UpsertHost(ctx, term, metadata.Host{HostID: host, State: lifecycle.HostActive}); err != nil {
		t.Fatal(err)
	}

	const racers = 8
	start := make(chan struct{})
	errs := make(chan error, racers)
	ids_ := make([]string, racers)
	for i := range racers {
		ids_[i] = ids.New().String()
		go func() {
			<-start
			_, err := store.RecordOperation(ctx, term, metadata.Operation{
				OperationID: ids_[i], Kind: lifecycle.OpDrain, HostID: host,
				Phase: lifecycle.OpPending, DesiredState: []byte(`{}`), CurrentState: []byte(`{}`),
			})
			errs <- err
		}()
	}
	close(start)

	var won int
	for range racers {
		switch err := <-errs; {
		case err == nil:
			won++
		case errors.Is(err, metadata.ErrDrainInProgress):
		default:
			t.Errorf("the loser must be told why, not handed an opaque error: %v", err)
		}
	}
	if won != 1 {
		t.Fatalf("%d of %d concurrent drains were recorded, want exactly 1", won, racers)
	}

	live, err := store.ListLiveOperationsByHost(ctx, host)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 1 {
		t.Fatalf("the host holds %d live operations, want 1", len(live))
	}
}

// TestPGCommittedBytesViewDoesNotDeriveTheWholeFleet is the plan assertion the
// host_committed_bytes view has to earn. The derivation used to be inlined in four
// queries; as a view it is written once, and the risk that trade brings is that a
// reader asking about *one* host silently pays for all of them — a view whose
// aggregate is computed before the filter is applied is exactly that shape, and the
// drain reads this on every pass, for every host it considers.
//
// So the assertion is not "an index is used" (this derivation has never used one:
// both sums scan, and did before the view too — see the sibling index tests for the
// queries that do). It is that the filtered read costs a fraction of the fleet-wide
// one. If the filter stops being pushed into the derivation, the two converge and
// this fails.
func TestPGCommittedBytesViewDoesNotDeriveTheWholeFleet(t *testing.T) {
	ctx := t.Context()
	pool := startPostgres(t)
	store := pg.New(pool)
	term, _ := store.AcquireLeadership(ctx, "cp")

	// A fleet of a couple of hundred hosts, one of them holding volumes and one
	// in-flight plan aimed at it — the §28.2 numbers the drain reads.
	hot := ids.New().String()
	fleet := []string{hot}
	for range fleetHosts - 1 {
		fleet = append(fleet, ids.New().String())
	}
	for _, h := range fleet {
		if err := store.UpsertHost(ctx, term, metadata.Host{
			HostID: h, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 50,
		}); err != nil {
			t.Fatal(err)
		}
	}
	var inFlight string
	for i := range fleetVolumes {
		id := ids.New().String()
		host := fleet[i%len(fleet)]
		if err := store.CreateVolume(ctx, term, metadata.Volume{
			VolumeID: id, SizeBytes: 1 << 30, BlockSize: 65536, State: lifecycle.VolumeActive,
			PrimaryHostID: host, DEKWrapped: []byte{1}, KEKID: "k", DEKKeyID: 1,
		}, nil); err != nil {
			t.Fatal(err)
		}
		if i == 1 {
			inFlight = id // a volume on somebody else, being moved to hot
		}
	}
	plan := `{"volumes":[{"volume_id":"` + inFlight + `","to_host":"` + hot + `","stage":"MOVING"}]}`
	for i := range fleetOperations {
		op := metadata.Operation{
			OperationID: ids.New().String(), Kind: lifecycle.OpDrain, HostID: fleet[i%len(fleet)],
			Phase: lifecycle.OpSucceeded, DesiredState: []byte(`{}`), CurrentState: []byte(`{}`),
		}
		if i == 0 {
			op.Phase, op.CurrentState = lifecycle.OpRunning, []byte(plan)
		}
		if _, err := store.RecordOperation(ctx, term, op); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `ANALYZE hosts; ANALYZE volumes; ANALYZE operations`); err != nil {
		t.Fatal(err)
	}

	// The number is right before anything is said about how it was computed.
	h, err := store.GetHost(ctx, hot)
	if err != nil {
		t.Fatal(err)
	}
	own := int64(fleetVolumes/len(fleet)) << 30
	if h.NVMeCommittedBytes < own+(1<<30) {
		t.Fatalf("committed = %d, want at least its own volumes plus the one in flight", h.NVMeCommittedBytes)
	}

	one := explainBuffers(t, pool,
		`EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF)
		 SELECT committed_bytes FROM host_committed_bytes WHERE host_id = $1`, hot)
	all := explainBuffers(t, pool,
		`EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF)
		 SELECT committed_bytes FROM host_committed_bytes ORDER BY host_id`)
	if one == 0 || all == 0 {
		t.Fatalf("no buffer counts in the plans (one=%d all=%d): the assertion is empty", one, all)
	}
	t.Logf("committed bytes: %d buffers for one host, %d for all %d", one, all, fleetHosts)
	// A tenth is a wide margin on a fleet of fleetHosts: if the filter reaches the
	// derivation the ratio is about 1/fleetHosts, and if it does not it is 1.
	if one*10 >= all {
		t.Fatalf("asking about one host costs %d buffers and asking about all %d costs %d:\n"+
			"the view derives the whole fleet before the filter is applied", one, fleetHosts, all)
	}
}

// Fleet shape for the plan tests: hundreds of hosts (§28.2 says the fleet is that
// size), thousands of volumes and operations, so the planner sees a table worth
// making a decision about rather than one small enough that every plan is equal.
const (
	fleetHosts      = 200
	fleetVolumes    = 2000
	fleetOperations = 2000
)

// explainBuffers runs an EXPLAIN (ANALYZE, BUFFERS) and returns the total buffers
// the plan touched — the whole plan's cost in the one unit that does not depend on
// how busy the machine running the test is.
func explainBuffers(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	rows, err := pool.Query(t.Context(), query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	total := 0
	re := regexp.MustCompile(`shared hit=(\d+)(?: read=(\d+))?`)
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		// Only the top node's counters are cumulative; nested ones are included in
		// it, so the first match is the whole plan and the rest are its parts.
		if m := re.FindStringSubmatch(line); m != nil && total == 0 {
			hit, _ := strconv.Atoi(m[1])
			read, _ := strconv.Atoi(m[2])
			total = hit + read
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return total
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

// TestPGRejectsAnUnversionedDEK is volumes.dek_key_id's CHECK, on the database rather
// than on the Go guard in front of it.
//
// Both layers exist for the reason CheckWatermarkOrder's two layers do: the Go check
// gives a caller a sentinel it can branch on, and the constraint is what holds when a
// row is written by something that is not this adapter — a repair script, a restore, a
// future migration. Testing only the Go half would prove the guard, not the rule.
//
// 0 is the whole rule: on the WAL path KeyID 0 means "this record is plaintext"
// (§14.1), so a volume row carrying it describes a key the Agent must refuse at attach
// with the DEK already unwrapped.
func TestPGRejectsAnUnversionedDEK(t *testing.T) {
	ctx := t.Context()
	pool := startPostgres(t)
	store := pg.New(pool)
	term, _ := store.AcquireLeadership(ctx, "cp")

	// The adapter's own guard, first: a caller gets a sentinel, not a 23514.
	err := store.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: ids.New().String(), SizeBytes: 1, BlockSize: 65536,
		State: lifecycle.VolumeActive, DEKWrapped: []byte{1}, KEKID: "k",
	}, nil)
	if !errors.Is(err, metadata.ErrUnversionedDEK) {
		t.Fatalf("CreateVolume with DEKKeyID 0: want ErrUnversionedDEK, got %v", err)
	}

	// And the constraint behind it, reached with SQL the adapter cannot express.
	for _, tc := range []struct {
		name  string
		keyID int64
	}{
		{"zero is the plaintext marker", 0},
		{"negative is not a uint32", -1},
		{"past uint32 cannot round-trip the format field", 1 << 32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pool.Exec(ctx, `
				INSERT INTO volumes (volume_id, size_bytes, durability, block_size,
				                     current_epoch, state, dek_wrapped, kek_id, dek_key_id)
				VALUES ($1, 1, 'remote', 65536, 0, 'ACTIVE', '\x01', 'k', $2)`,
				ids.New().String(), tc.keyID)
			if err == nil {
				t.Fatalf("Postgres accepted dek_key_id = %d", tc.keyID)
			}
		})
	}
}
