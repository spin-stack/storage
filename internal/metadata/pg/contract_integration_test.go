//go:build integration

package pg_test

import (
	"context"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/metadata/metadatatest"
	"github.com/spin-stack/storage/internal/metadata/pg"
)

// TestPGStoreContract runs the shared metadata.Store contract against real
// Postgres. Its twin is TestSimStoreContract in the unit lane; between them they
// are what makes a DST fencing/idempotency proof transfer to production instead of
// being a statement about the in-memory store alone.
//
// One container is started for the whole contract and the tables are truncated
// between cases: the contract is about behaviour, not about container startup.
func TestPGStoreContract(t *testing.T) {
	ctx := t.Context()
	pool := startPostgres(t)
	metadatatest.RunContract(t, func(t *testing.T) metadata.Store {
		if _, err := pool.Exec(ctx,
			`TRUNCATE operations, snapshots, volumes, host_leases, hosts, control_plane_leader`); err != nil {
			t.Fatalf("reset: %v", err)
		}
		return pg.New(pool)
	})
}

// TestPGMalformedIDsAreRejected: the adapter parses string ids into uuid columns,
// and an id it cannot parse must be an error — never SQL NULL. A clone or promotion
// issued with a truncated host id would otherwise write primary_host_id = NULL: the
// volume then never appears in ListVolumesByHost, so a drain of that host reports
// success without evacuating it, and promotion's resume branch (v.PrimaryHostID ==
// newHost) can never match, so every retry burns another epoch.
func TestPGMalformedIDsAreRejected(t *testing.T) {
	ctx := t.Context()
	pool := startPostgres(t)
	store := pg.New(pool)
	term, err := store.AcquireLeadership(ctx, "cp")
	if err != nil {
		t.Fatal(err)
	}
	hostID, volID := ids.New().String(), ids.New().String()
	if err := store.UpsertHost(ctx, term, metadata.Host{HostID: hostID, State: lifecycle.HostActive}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: volID, SizeBytes: 1 << 30, BlockSize: 65536, State: lifecycle.VolumeActive,
		PrimaryHostID: hostID, DEKWrapped: []byte{1}, KEKID: "k", DEKKeyID: 1,
	}, nil); err != nil {
		t.Fatal(err)
	}

	const truncated = "00000000-0000-7000-8000-00000000" // a host id cut short

	tests := []struct {
		name string
		call func() error
	}{
		{"CreateVolume primary host", func() error {
			return store.CreateVolume(ctx, term, metadata.Volume{
				VolumeID: ids.New().String(), SizeBytes: 1, BlockSize: 65536,
				State: lifecycle.VolumeActive, PrimaryHostID: truncated,
				DEKWrapped: []byte{1}, KEKID: "k", DEKKeyID: 1,
			}, nil)
		}},
		{"CreateVolume standby host", func() error {
			return store.CreateVolume(ctx, term, metadata.Volume{
				VolumeID: ids.New().String(), SizeBytes: 1, BlockSize: 65536,
				State: lifecycle.VolumeActive, StandbyHostID: truncated,
				DEKWrapped: []byte{1}, KEKID: "k", DEKKeyID: 1,
			}, nil)
		}},
		{"BumpVolumeEpoch primary host", func() error {
			_, err := store.BumpVolumeEpoch(ctx, term, volID, truncated, 0)
			return err
		}},
		{"CreateSnapshot source host", func() error {
			return store.CreateSnapshot(ctx, term, metadata.Snapshot{
				SnapshotID: ids.New().String(), VolumeID: volID, Epoch: 1, TargetSequence: 1,
				RootDigest: "d", SourceHostID: truncated, State: lifecycle.SnapshotCreating,
				RequestID: ids.New().String(),
			})
		}},
		{"CreateSnapshot parent snapshot", func() error {
			return store.CreateSnapshot(ctx, term, metadata.Snapshot{
				SnapshotID: ids.New().String(), VolumeID: volID, ParentSnapshotID: truncated,
				Epoch: 1, TargetSequence: 1, RootDigest: "d", State: lifecycle.SnapshotCreating,
				RequestID: ids.New().String(),
			})
		}},
		{"RecordOperation volume", func() error {
			_, err := store.RecordOperation(ctx, term, metadata.Operation{
				OperationID: ids.New().String(), Kind: lifecycle.OpDrain, Phase: lifecycle.OpPending,
				VolumeID: truncated, DesiredState: []byte(`{}`), CurrentState: []byte(`{}`),
			})
			return err
		}},
		{"RecordOperation host", func() error {
			_, err := store.RecordOperation(ctx, term, metadata.Operation{
				OperationID: ids.New().String(), Kind: lifecycle.OpDrain, Phase: lifecycle.OpPending,
				HostID: truncated, DesiredState: []byte(`{}`), CurrentState: []byte(`{}`),
			})
			return err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, metadata.ErrInvalidID) {
				t.Fatalf("malformed id: want ErrInvalidID, got %v", err)
			}
		})
	}

	// And nothing was written with a NULL where an id belonged.
	var nulls int
	if err := pool.QueryRow(ctx, `
        SELECT (SELECT count(*) FROM volumes WHERE primary_host_id IS NULL AND standby_host_id IS NULL)
             + (SELECT count(*) FROM snapshots)
             + (SELECT count(*) FROM operations)`).Scan(&nulls); err != nil {
		t.Fatal(err)
	}
	if nulls != 0 {
		t.Fatalf("%d rows were written with a coerced NULL id", nulls)
	}
}

// TestPGVolumeStateGuardIsAtomic is the volume-lifecycle counterpart of
// TestPGOperationPhaseGuardIsAtomic: the §7 transition table must live in the
// UPDATE predicate, not in a read-modify-write in Go, or two Control Planes racing
// to react to the same suspicion can both win.
func TestPGVolumeStateGuardIsAtomic(t *testing.T) {
	ctx := t.Context()
	pool := startPostgres(t)
	store := pg.New(pool)
	// Asserted rather than called directly so the gap "the Store stores the §7
	// lifecycle but cannot express it" fails as a test, not as a build break.
	setter, ok := any(store).(interface {
		SetVolumeState(context.Context, int64, string, lifecycle.VolumeState) error
	})
	if !ok {
		t.Fatal("pg.Store cannot express the §7 volume lifecycle it stores: no SetVolumeState")
	}
	term, err := store.AcquireLeadership(ctx, "cp")
	if err != nil {
		t.Fatal(err)
	}
	volID := ids.New().String()
	if err := store.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: volID, SizeBytes: 1 << 30, BlockSize: 65536, State: lifecycle.VolumeActive,
		DEKWrapped: []byte{1}, KEKID: "k", DEKKeyID: 1,
	}, nil); err != nil {
		t.Fatal(err)
	}

	// Move it out from under the caller between its read and its write: the guard
	// has to be in the statement to notice.
	if err := setter.SetVolumeState(ctx, term, volID, lifecycle.VolumePrimarySuspected); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE volumes SET state='FENCING_WAIT' WHERE volume_id=$1`, volID); err != nil {
		t.Fatal(err)
	}
	// ACTIVE is a legal successor of PRIMARY_SUSPECTED but not of FENCING_WAIT (§7:
	// a new writer is never promoted on a missed heartbeat alone).
	if err := setter.SetVolumeState(ctx, term, volID, lifecycle.VolumeActive); !errors.Is(err, lifecycle.ErrInvalidTransition) {
		t.Fatalf("FENCING_WAIT -> ACTIVE: want ErrInvalidTransition, got %v", err)
	}
	v, err := store.GetVolume(ctx, volID)
	if err != nil {
		t.Fatal(err)
	}
	if v.State != lifecycle.VolumeFencingWait {
		t.Fatalf("state = %q after a refused transition", v.State)
	}
}

// TestPGSnapshotStateGuardIsAtomic is the §19 twin of TestPGVolumeStateGuardIsAtomic
// and TestPGOperationPhaseGuardIsAtomic: INV-16 says a PUBLISHED snapshot never
// changes, and that rule has to live in the UPDATE predicate. A read-modify-write in
// Go lets a publication that lands between the read and the write be overwritten by
// a concurrent cleanup pass marking the snapshot FAILED.
func TestPGSnapshotStateGuardIsAtomic(t *testing.T) {
	ctx := t.Context()
	pool := startPostgres(t)
	store := pg.New(pool)
	setter, ok := any(store).(interface {
		SetSnapshotState(context.Context, int64, string, lifecycle.SnapshotState) error
	})
	if !ok {
		t.Fatal("pg.Store cannot express the §19 snapshot lifecycle it stores: no SetSnapshotState")
	}
	term, err := store.AcquireLeadership(ctx, "cp")
	if err != nil {
		t.Fatal(err)
	}
	volID, snapID := ids.New().String(), ids.New().String()
	if err := store.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: volID, SizeBytes: 1 << 30, BlockSize: 65536, State: lifecycle.VolumeActive,
		DEKWrapped: []byte{1}, KEKID: "k", DEKKeyID: 1,
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateSnapshot(ctx, term, metadata.Snapshot{
		SnapshotID: snapID, VolumeID: volID, Epoch: 1, TargetSequence: 1, RootDigest: "d",
		State: lifecycle.SnapshotCreating, RequestID: ids.New().String(),
	}); err != nil {
		t.Fatal(err)
	}

	// The publication lands under the caller, between its read and its write.
	if _, err := pool.Exec(ctx,
		`UPDATE snapshots SET state='PUBLISHED' WHERE snapshot_id=$1`, snapID); err != nil {
		t.Fatal(err)
	}
	if err := setter.SetSnapshotState(ctx, term, snapID, lifecycle.SnapshotFailed); !errors.Is(err, lifecycle.ErrInvalidTransition) {
		t.Fatalf("PUBLISHED -> FAILED: want ErrInvalidTransition, got %v", err)
	}
	got, err := store.GetSnapshot(ctx, snapID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != lifecycle.SnapshotPublished {
		t.Fatalf("state = %q after a refused transition, want PUBLISHED", got.State)
	}
}
