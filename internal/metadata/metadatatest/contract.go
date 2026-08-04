// Package metadatatest is the shared contract every metadata.Store implementation
// must satisfy. It exists because the two implementations carry two
// different kinds of proof: metadata/sim carries the DST fencing and idempotency
// scenarios, metadata/pg carries production. A property proven against the sim is
// only a proof about production if both answer the same way — same branch taken,
// same error identity — for the operationally interesting cases: a zombie term, a
// row that is not there, a duplicated create, a watermark report that arrived late.
//
// RunContract is invoked from a sim unit test and from the integration-tagged pg
// test, so a divergence fails one of the two lanes instead of quietly invalidating
// a simulation proof.
//
// # The contract, in one place
//
//   - Term first. Every mutation validates the caller's Control-Plane term before
//     anything else (§7). A stale term — including term 0, before any election has
//     happened — is metadata.ErrStaleTerm even when the row is also missing, the
//     transition is also illegal, and the resize is also a shrink. A zombie CP must
//     learn that it is a zombie; every other diagnosis it could be handed is a lie
//     that sends a reconciler down the wrong branch.
//   - Then existence. A mutation naming a row that does not exist is
//     metadata.ErrNotFound, never ErrStaleTerm.
//   - Then the domain guard: lifecycle.ErrInvalidTransition, ErrShrinkNotAllowed,
//     ErrCapacityUnderflow, ErrWatermarkOrder.
//   - Creates are idempotent and never destructive: re-creating a volume never
//     lowers its epoch, blanks its ownership, shrinks it, rewinds its watermarks or
//     rewrites its lifecycle state, and re-creating a snapshot is a no-op (INV-16).
//
// # Deliberately not in the contract
//
//   - Identifier syntax. The sim treats ids as opaque strings (the DST harness and
//     the Control-Plane tests use short readable ids); Postgres stores uuid columns
//     with a UUIDv7 CHECK (INV-22). Only the *empty* id — never meaningful for a
//     primary key — is pinned here, as metadata.ErrInvalidID. Malformed-id handling
//     is pinned in the pg adapter's own integration test, where the parsing lives.
//   - Referential integrity for the volume→host and snapshot→volume ownership
//     columns. Postgres has foreign keys; the sim does not, and Control-Plane tests
//     legitimately attach a volume to a host that was never registered. That
//     divergence is real and reported, not papered over here.
package metadatatest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
)

// Fixture returns a fresh, empty Store — no leader, no rows.
type Fixture func(t *testing.T) metadata.Store

// RunContract runs every contract case against stores built by newStore.
func RunContract(t *testing.T, newStore Fixture) {
	t.Helper()
	cases := []struct {
		name string
		run  func(t *testing.T, s metadata.Store)
	}{
		{"MutationsBeforeAnyElectionAreStale", termZero},
		{"EveryMutationRejectsAStaleTerm", staleTerm},
		{"MissingRowsAreNotFound", missingRows},
		{"StaleTermWinsOverEveryOtherDiagnosis", staleTermWins},
		{"CreateVolumeRoundTripsEveryField", volumeRoundTrip},
		{"RecreatingAVolumeNeverRegresses", volumeRecreate},
		{"RecreatingASnapshotIsANoOp", snapshotRecreate},
		{"WatermarksAreOrderedAndNeverGoBackwards", watermarks},
		{"UpsertHostDoesNotClobberStateOrCapacity", upsertHost},
		{"RecordOperationSeparatesDuplicateFromStaleTerm", recordOperation},
		{"VolumeLifecycleIsExpressible", volumeLifecycle},
		{"SnapshotLifecycleIsExpressible", snapshotLifecycle},
		{"PendingSnapshotsFollowTheVolumeAndPublishOnce", pendingSnapshots},
		{"ConcurrentEpochBumpsAreSerialized", concurrentBumps},
		{"VolumePlacementIsChangeableAndClearingIsIdempotent", volumePlacement},
		{"EmptyIdentifiersAreRejected", emptyIDs},
		{"HostLeasesAreRevocableAndNotRenewableForADeadHost", hostLeases},
		{"CapacityIsDerivedAndTheBoundIsAPredicateOfTheWrite", capacity},
		{"LiveOperationsAreListableByHost", liveOperationsByHost},
		{"AHostHoldsOnlyOneLiveDrain", oneLiveDrainPerHost},
		{"TheStoreExposesTheClockThatStampsItsRows", authorityClock},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t, newStore(t))
		})
	}
}

// --- the lifecycle mutations the schema implies -----------------------------
//
// The volume (§7) and snapshot (§19) state machines are stored by every
// implementation and CHECK-constrained in Postgres, but until they are on the
// Store interface no caller can move a volume into FENCING_WAIT or a crashed
// snapshot out of CREATING. These assertions state that as a test failure rather
// than a compile error, so the gap is visible as "the Store cannot express the
// lifecycle it stores".

// errNotExpressible means the Store stores a lifecycle it has no mutation for.
var errNotExpressible = errors.New("metadatatest: lifecycle is stored but not expressible through the Store")

type volumeStateSetter interface {
	SetVolumeState(ctx context.Context, term int64, volumeID string, state lifecycle.VolumeState) error
}

type snapshotStateSetter interface {
	SetSnapshotState(ctx context.Context, term int64, snapshotID string, state lifecycle.SnapshotState) error
}

func setVolumeState(ctx context.Context, s metadata.Store, term int64, volumeID string, state lifecycle.VolumeState) error {
	m, ok := s.(volumeStateSetter)
	if !ok {
		return fmt.Errorf("%w: no SetVolumeState (§7 volume states)", errNotExpressible)
	}
	return m.SetVolumeState(ctx, term, volumeID, state)
}

func setSnapshotState(ctx context.Context, s metadata.Store, term int64, snapshotID string, state lifecycle.SnapshotState) error {
	m, ok := s.(snapshotStateSetter)
	if !ok {
		return fmt.Errorf("%w: no SetSnapshotState (§19 snapshot states)", errNotExpressible)
	}
	return m.SetSnapshotState(ctx, term, snapshotID, state)
}

type hostLeaseRevoker interface {
	RevokeHostLease(ctx context.Context, term int64, hostID string) error
}

func revokeHostLease(ctx context.Context, s metadata.Store, term int64, hostID string) error {
	m, ok := s.(hostLeaseRevoker)
	if !ok {
		return fmt.Errorf("%w: no RevokeHostLease (§12.6: nothing can take a lease back)", errNotExpressible)
	}
	return m.RevokeHostLease(ctx, term, hostID)
}

type authorityClocker interface {
	Now(ctx context.Context) (time.Time, error)
}

func now(ctx context.Context, s metadata.Store) (time.Time, error) {
	m, ok := s.(authorityClocker)
	if !ok {
		return time.Time{}, fmt.Errorf("%w: no Now (§12.1: the clock that stamps last_renewal)", errNotExpressible)
	}
	return m.Now(ctx)
}

type epochCASer interface {
	BumpVolumeEpoch(ctx context.Context, term int64, volumeID, primaryHostID string, expectedEpoch int64) (int64, error)
}

// bumpVolumeEpoch is the compare-and-set form of the epoch bump. A blind increment
// hands an epoch to whichever caller happened to run second, so the volume row ends
// up naming a primary that never CASed the S3 epoch object and never got a lease.
func bumpVolumeEpoch(ctx context.Context, s metadata.Store, term int64, volumeID, primaryHostID string, expectedEpoch int64) (int64, error) {
	m, ok := s.(epochCASer)
	if !ok {
		return 0, fmt.Errorf("%w: BumpVolumeEpoch takes no expected epoch (§12.3: the bump is a blind increment)", errNotExpressible)
	}
	return m.BumpVolumeEpoch(ctx, term, volumeID, primaryHostID, expectedEpoch)
}

// --- helpers ---------------------------------------------------------------

func id() string { return ids.New().String() }

// world is a small fixture: a leader, one host, one volume, one snapshot and one
// operation — so that "the row is missing" is never the reason a mutation fails.
type world struct {
	term int64
	host string
	vol  string
	snap string
	op   string
}

func newWorld(t *testing.T, s metadata.Store) world {
	t.Helper()
	ctx := t.Context()
	term, err := s.AcquireLeadership(ctx, "cp-a")
	if err != nil {
		t.Fatalf("AcquireLeadership: %v", err)
	}
	w := world{term: term, host: id(), vol: id(), snap: id(), op: id()}
	if err := s.UpsertHost(ctx, term, metadata.Host{
		HostID: w.host, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
	}); err != nil {
		t.Fatalf("UpsertHost: %v", err)
	}
	if err := s.CreateVolume(ctx, term, metadata.Volume{DEKKeyID: 1,
		VolumeID: w.vol, SizeBytes: 1 << 30, BlockSize: 65536, State: lifecycle.VolumeActive,
		PrimaryHostID: w.host, DEKWrapped: []byte{1}, KEKID: "kek",
	}, nil); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if err := s.CreateSnapshot(ctx, term, metadata.Snapshot{
		SnapshotID: w.snap, VolumeID: w.vol, Epoch: 1, TargetSequence: 10,
		RootDigest: "digest", State: lifecycle.SnapshotCreating, RequestID: id(),
	}); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if _, err := s.RecordOperation(ctx, term, metadata.Operation{
		OperationID: w.op, Kind: lifecycle.OpDrain, Phase: lifecycle.OpPending,
		VolumeID: w.vol, HostID: w.host, DesiredState: []byte(`{}`), CurrentState: []byte(`{}`),
	}); err != nil {
		t.Fatalf("RecordOperation: %v", err)
	}
	return w
}

// mutation is one term-guarded write, named as the Store method it calls.
type mutation struct {
	name string
	call func(ctx context.Context, s metadata.Store, term int64, w world) error
}

// everyMutation is the full mutating surface. A method added to the Store without
// a line here is a method whose term guard nobody checks.
func everyMutation() []mutation {
	return []mutation{
		{"UpsertHost", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.UpsertHost(ctx, term, metadata.Host{HostID: w.host, State: lifecycle.HostActive})
		}},
		{"SetHostState", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.SetHostState(ctx, term, w.host, lifecycle.HostCordoned)
		}},
		{"RenewHostLease", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.RenewHostLease(ctx, term, w.host, 10)
		}},
		{"BlockHostRenewals", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.BlockHostRenewals(ctx, term, w.host, time.Minute)
		}},
		{"UnblockHostRenewals", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.UnblockHostRenewals(ctx, term, w.host)
		}},
		{"RevokeHostLease", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return revokeHostLease(ctx, s, term, w.host)
		}},
		{"CreateVolume", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.CreateVolume(ctx, term, metadata.Volume{DEKKeyID: 1,
				VolumeID: id(), SizeBytes: 1, BlockSize: 65536, State: lifecycle.VolumeActive,
				DEKWrapped: []byte{1}, KEKID: "k",
			}, nil)
		}},
		{"BumpVolumeEpoch", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			_, err := bumpVolumeEpoch(ctx, s, term, w.vol, w.host, 0)
			return err
		}},
		{"UpdateWatermarks", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.UpdateWatermarks(ctx, term, w.vol, 3, 2, 1)
		}},
		{"ResizeVolume", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.ResizeVolume(ctx, term, w.vol, 1<<31)
		}},
		{"SetVolumeState", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return setVolumeState(ctx, s, term, w.vol, lifecycle.VolumePrimarySuspected)
		}},
		{"SetVolumePrimaryHost", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.SetVolumePrimaryHost(ctx, term, w.vol, "")
		}},
		{"CreateSnapshot", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.CreateSnapshot(ctx, term, metadata.Snapshot{
				SnapshotID: id(), VolumeID: w.vol, Epoch: 1, TargetSequence: 1,
				RootDigest: "d", State: lifecycle.SnapshotCreating, RequestID: id(),
			})
		}},
		{"SetSnapshotState", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return setSnapshotState(ctx, s, term, w.snap, lifecycle.SnapshotPublished)
		}},
		{"PublishSnapshot", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.PublishSnapshot(ctx, term, w.snap, 7, w.host, "image/k/snapshots/s.json")
		}},
		{"RecordOperation", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			_, err := s.RecordOperation(ctx, term, metadata.Operation{
				OperationID: id(), Kind: lifecycle.OpDrain, Phase: lifecycle.OpPending,
				DesiredState: []byte(`{}`), CurrentState: []byte(`{}`),
			})
			return err
		}},
		{"UpdateOperation", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.UpdateOperation(ctx, term, metadata.Operation{
				OperationID: w.op, Phase: lifecycle.OpRunning, CurrentState: []byte(`{}`),
			}, nil)
		}},
	}
}

// --- cases -----------------------------------------------------------------

// termZero: nothing may be written before an election. Term 0 is what a CP that
// never acquired leadership — or that read a zeroed config field — passes; treating
// it as valid lets an unelected process bump epochs and release capacity.
func termZero(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	// No AcquireLeadership: there is no leader at all.
	w := world{host: id(), vol: id(), snap: id(), op: id()}
	for _, m := range everyMutation() {
		t.Run(m.name, func(t *testing.T) {
			if err := m.call(ctx, s, 0, w); !errors.Is(err, metadata.ErrStaleTerm) {
				t.Fatalf("term 0 before any election: want ErrStaleTerm, got %v", err)
			}
		})
	}
}

// staleTerm: the §7 property over the whole mutating surface, with every row
// present so ErrNotFound cannot stand in for the answer.
func staleTerm(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)
	stale := w.term
	if _, err := s.AcquireLeadership(ctx, "cp-b"); err != nil {
		t.Fatal(err)
	}
	for _, m := range everyMutation() {
		t.Run(m.name, func(t *testing.T) {
			if err := m.call(ctx, s, stale, w); !errors.Is(err, metadata.ErrStaleTerm) {
				t.Fatalf("stale term: want ErrStaleTerm, got %v", err)
			}
		})
	}
}

// missingRows: a mutation naming a row that is not there is ErrNotFound. A
// reconciler that reads ErrStaleTerm here steps down and re-acquires leadership
// forever over a volume that was simply deleted.
func missingRows(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)
	ghostHost, ghostVol, ghostSnap, ghostOp := id(), id(), id(), id()

	tests := []mutation{
		{"SetHostState", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.SetHostState(ctx, term, ghostHost, lifecycle.HostCordoned)
		}},
		{"RenewHostLease", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.RenewHostLease(ctx, term, ghostHost, 10)
		}},
		{"BlockHostRenewals", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.BlockHostRenewals(ctx, term, ghostHost, time.Minute)
		}},
		{"UnblockHostRenewals", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.UnblockHostRenewals(ctx, term, ghostHost)
		}},
		{"RevokeHostLease", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return revokeHostLease(ctx, s, term, ghostHost)
		}},
		{"BumpVolumeEpoch", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			_, err := bumpVolumeEpoch(ctx, s, term, ghostVol, w.host, 0)
			return err
		}},
		{"UpdateWatermarks", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.UpdateWatermarks(ctx, term, ghostVol, 3, 2, 1)
		}},
		{"ResizeVolume", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.ResizeVolume(ctx, term, ghostVol, 1<<31)
		}},
		{"SetVolumeState", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return setVolumeState(ctx, s, term, ghostVol, lifecycle.VolumePrimarySuspected)
		}},
		{"SetVolumePrimaryHost", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.SetVolumePrimaryHost(ctx, term, ghostVol, w.host)
		}},
		{"SetSnapshotState", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return setSnapshotState(ctx, s, term, ghostSnap, lifecycle.SnapshotPublished)
		}},
		{"PublishSnapshot", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.PublishSnapshot(ctx, term, ghostSnap, 7, "", "k")
		}},
		{"UpdateOperation", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.UpdateOperation(ctx, term, metadata.Operation{
				OperationID: ghostOp, Phase: lifecycle.OpRunning, CurrentState: []byte(`{}`),
			}, nil)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(ctx, s, w.term, w); !errors.Is(err, metadata.ErrNotFound) {
				t.Fatalf("missing row: want ErrNotFound, got %v", err)
			}
		})
	}

	reads := []struct {
		name string
		call func() error
	}{
		{"GetHost", func() error { _, err := s.GetHost(ctx, ghostHost); return err }},
		{"GetHostLease", func() error { _, err := s.GetHostLease(ctx, ghostHost); return err }},
		{"GetVolume", func() error { _, err := s.GetVolume(ctx, ghostVol); return err }},
		{"GetSnapshot", func() error { _, err := s.GetSnapshot(ctx, ghostSnap); return err }},
		{"GetOperation", func() error { _, err := s.GetOperation(ctx, ghostOp); return err }},
	}
	for _, tc := range reads {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, metadata.ErrNotFound) {
				t.Fatalf("missing row: want ErrNotFound, got %v", err)
			}
		})
	}
}

// staleTermWins: when a zombie CP issues a call that is also wrong for a second
// reason, the term is the answer. Otherwise the zombie is told "shrink not allowed"
// or "no such volume" and concludes it is still the leader.
func staleTermWins(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)
	if err := s.ResizeVolume(ctx, w.term, w.vol, 1<<31); err != nil {
		t.Fatal(err)
	}
	stale := w.term
	if _, err := s.AcquireLeadership(ctx, "cp-b"); err != nil {
		t.Fatal(err)
	}
	ghost := id()

	tests := []struct {
		name string
		call func() error
	}{
		{"shrink under a stale term", func() error { return s.ResizeVolume(ctx, stale, w.vol, 1) }},
		{"missing volume under a stale term", func() error {
			_, err := bumpVolumeEpoch(ctx, s, stale, ghost, w.host, 0)
			return err
		}},
		{"wrong expected epoch under a stale term", func() error {
			_, err := bumpVolumeEpoch(ctx, s, stale, w.vol, w.host, 99)
			return err
		}},
		{"illegal host transition under a stale term", func() error {
			return s.SetHostState(ctx, stale, w.host, lifecycle.HostActive)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, metadata.ErrStaleTerm) {
				t.Fatalf("want ErrStaleTerm, got %v", err)
			}
		})
	}
}

// volumeRoundTrip: what CreateVolume accepts is what GetVolume returns. A field
// one implementation silently drops is a DST scenario that sets it up and a
// production run that does not have it — scenarioRecoveryAuthorityIsS3 seeds a
// deliberately wrong durable_sequence exactly this way.
func volumeRoundTrip(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)
	standby := id()
	if err := s.UpsertHost(ctx, w.term, metadata.Host{HostID: standby, State: lifecycle.HostActive}); err != nil {
		t.Fatal(err)
	}
	want := metadata.Volume{DEKKeyID: 1,
		VolumeID: id(), SizeBytes: 1 << 33,
		BlockSize: 65536, CurrentEpoch: 7, State: lifecycle.VolumeDetached,
		PrimaryHostID: w.host, StandbyHostID: standby, ChainDepth: 3,
		DEKWrapped: []byte{9, 8, 7}, KEKID: "kek-7",
		LocalSequence: 900, DurableSequence: 800, PublishedSequence: 700,
	}
	if err := s.CreateVolume(ctx, w.term, want, nil); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetVolume(ctx, want.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	if got.SizeBytes != want.SizeBytes ||
		got.BlockSize != want.BlockSize || got.CurrentEpoch != want.CurrentEpoch ||
		got.State != want.State || got.PrimaryHostID != want.PrimaryHostID ||
		got.StandbyHostID != want.StandbyHostID || got.ChainDepth != want.ChainDepth ||
		string(got.DEKWrapped) != string(want.DEKWrapped) || got.KEKID != want.KEKID ||
		got.LocalSequence != want.LocalSequence || got.DurableSequence != want.DurableSequence ||
		got.PublishedSequence != want.PublishedSequence {
		t.Fatalf("CreateVolume did not round-trip:\n got %+v\nwant %+v", got, want)
	}
}

// volumeRecreate: two operators run rebuild-metadata at once; both see ErrNotFound
// for the same volume and both create it. Whatever the loser's insert does, it must
// not lower the epoch, blank the owner, shrink the volume, rewind the watermarks or
// rewrite the lifecycle state — each of those hands the fleet to the wrong writer or
// hides data that is durable in S3.
func volumeRecreate(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)
	vol := id()
	if err := s.CreateVolume(ctx, w.term, metadata.Volume{DEKKeyID: 1,
		VolumeID: vol, SizeBytes: 1 << 30, BlockSize: 65536, CurrentEpoch: 5,
		State: lifecycle.VolumeActive, PrimaryHostID: w.host, DEKWrapped: []byte{1}, KEKID: "k",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateWatermarks(ctx, w.term, vol, 500, 400, 300); err != nil {
		t.Fatal(err)
	}

	// The losing rebuild: a fresh descriptor-shaped row — epoch 2, no owner, DETACHED.
	if err := s.CreateVolume(ctx, w.term, metadata.Volume{DEKKeyID: 1,
		VolumeID: vol, SizeBytes: 1 << 20, BlockSize: 65536, CurrentEpoch: 2,
		State: lifecycle.VolumeDetached, DEKWrapped: []byte{1}, KEKID: "k",
	}, nil); err != nil {
		t.Fatalf("re-creating an existing volume must converge, not abort: %v", err)
	}

	got, err := s.GetVolume(ctx, vol)
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case got.CurrentEpoch != 5:
		t.Fatalf("epoch went backwards to %d", got.CurrentEpoch)
	case got.PrimaryHostID != w.host:
		t.Fatalf("owner was blanked to %q", got.PrimaryHostID)
	case got.SizeBytes != 1<<30:
		t.Fatalf("volume shrank to %d", got.SizeBytes)
	case got.State != lifecycle.VolumeActive:
		t.Fatalf("lifecycle state was rewritten to %q", got.State)
	case got.LocalSequence != 500 || got.DurableSequence != 400 || got.PublishedSequence != 300:
		t.Fatalf("watermarks were rewound: %+v", got)
	}
}

// snapshotRecreate: a published snapshot never changes (INV-16), so re-recording it
// — the concurrent-rebuild case again — is a no-op, not an overwrite and not an
// abort that leaves the catalog half-built.
func snapshotRecreate(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)
	snapID, reqID := id(), id()
	first := metadata.Snapshot{
		SnapshotID: snapID, VolumeID: w.vol, Epoch: 3, TargetSequence: 42,
		RootDigest: "original", State: lifecycle.SnapshotPublished,
		ManifestKey: "snapshots/x/manifest.json", RequestID: reqID,
	}
	if err := s.CreateSnapshot(ctx, w.term, first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.RootDigest = "rewritten"
	second.TargetSequence = 1
	second.State = lifecycle.SnapshotCreating
	second.RequestID = id()
	if err := s.CreateSnapshot(ctx, w.term, second); err != nil {
		t.Fatalf("re-recording a snapshot must converge, not abort: %v", err)
	}
	got, err := s.GetSnapshot(ctx, snapID)
	if err != nil {
		t.Fatal(err)
	}
	if got.RootDigest != "original" || got.TargetSequence != 42 || got.State != lifecycle.SnapshotPublished {
		t.Fatalf("a published snapshot was mutated by a duplicate create: %+v", got)
	}
}

// watermarks: an epoch-N primary's report can be queued behind a retry and land
// after epoch N+1's writer has published its own. Promotion does not change the CP
// term, so the stale report passes the term guard — the store is the only thing left
// that can refuse to move durable_sequence backwards, and that number is what an
// operator uses during an incident to decide whether to accept data loss.
func watermarks(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)
	if err := s.UpdateWatermarks(ctx, w.term, w.vol, 100, 90, 80); err != nil {
		t.Fatal(err)
	}

	t.Run("a late report never lowers a watermark", func(t *testing.T) {
		if err := s.UpdateWatermarks(ctx, w.term, w.vol, 50, 40, 30); err != nil {
			t.Fatalf("a stale report is ignored, not an error: %v", err)
		}
		v, _ := s.GetVolume(ctx, w.vol)
		if v.LocalSequence != 100 || v.DurableSequence != 90 || v.PublishedSequence != 80 {
			t.Fatalf("watermarks went backwards: %+v", v)
		}
	})

	t.Run("out-of-order reports are rejected", func(t *testing.T) {
		tests := []struct {
			name                      string
			local, durable, published int64
		}{
			{"durable above local", 100, 200, 80},
			{"published above durable", 300, 90, 200},
			{"published above local", 100, 400, 500},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				err := s.UpdateWatermarks(ctx, w.term, w.vol, tc.local, tc.durable, tc.published)
				if !errors.Is(err, metadata.ErrWatermarkOrder) {
					t.Fatalf("want ErrWatermarkOrder, got %v", err)
				}
			})
		}
		v, _ := s.GetVolume(ctx, w.vol)
		if v.LocalSequence != 100 || v.DurableSequence != 90 || v.PublishedSequence != 80 {
			t.Fatalf("a rejected report still mutated the row: %+v", v)
		}
	})

	t.Run("a forward report advances", func(t *testing.T) {
		if err := s.UpdateWatermarks(ctx, w.term, w.vol, 400, 300, 200); err != nil {
			t.Fatal(err)
		}
		v, _ := s.GetVolume(ctx, w.vol)
		if v.LocalSequence != 400 || v.DurableSequence != 300 || v.PublishedSequence != 200 {
			t.Fatalf("forward report did not land: %+v", v)
		}
	})

	// The reporting path has always refused disorder; the *creating* path did not,
	// so INV-03 could be violated at birth by rebuild-metadata or by a test fixture
	// and no later report would ever repair it (each watermark only moves forward).
	// Postgres now refuses such a row outright; this is the same refusal one layer
	// up, so both stores answer with the same sentinel instead of one of them
	// answering with a constraint violation.
	t.Run("a create with out-of-order watermarks is rejected", func(t *testing.T) {
		tests := []struct {
			name                      string
			local, durable, published int64
		}{
			{"durable above local", 10, 20, 5},
			{"published above durable", 30, 9, 20},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				vol := id()
				err := s.CreateVolume(ctx, w.term, metadata.Volume{DEKKeyID: 1,
					VolumeID: vol, SizeBytes: 1 << 20, BlockSize: 65536,
					State: lifecycle.VolumeActive, DEKWrapped: []byte{1}, KEKID: "k",
					LocalSequence: tc.local, DurableSequence: tc.durable, PublishedSequence: tc.published,
				}, nil)
				if !errors.Is(err, metadata.ErrWatermarkOrder) {
					t.Fatalf("want ErrWatermarkOrder, got %v", err)
				}
				if _, err := s.GetVolume(ctx, vol); !errors.Is(err, metadata.ErrNotFound) {
					t.Fatalf("the refused volume was created anyway: %v", err)
				}
			})
		}
	})
}

// upsertHost: a host is cordoned and being drained; its next routine heartbeat
// carries State ACTIVE and whatever committed bytes the agent believes. If the
// heartbeat wins, AcceptsPlacement() starts handing the host new volumes while its
// own are being evacuated.
//
// Committed capacity is derived (ADR-0017), so the second half of this is now
// structural rather than a rule the write has to remember: whatever the agent puts
// in the field, the host still reports the volumes it holds. The assertion stays
// because "the heartbeat cannot rewrite the accounting" is the property, and which
// mechanism enforces it is an implementation detail that may change again.
func upsertHost(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s) // one ACTIVE host holding one 1 GiB volume
	const held = int64(1) << 30
	if err := s.SetHostState(ctx, w.term, w.host, lifecycle.HostCordoned); err != nil {
		t.Fatal(err)
	}

	// A heartbeat-shaped upsert: the agent's own view, which knows nothing about
	// cordoning and has its own idea of what the host is committed to.
	if err := s.UpsertHost(ctx, w.term, metadata.Host{
		HostID: w.host, State: lifecycle.HostActive, AgentVersion: "v2",
		MaxFormatVersion: 3, NVMeTotalBytes: 1 << 41, NVMeUsedBytes: 123,
		RemoteBacklogBytes: 456, NVMeCommittedBytes: 999,
	}); err != nil {
		t.Fatal(err)
	}
	h, err := s.GetHost(ctx, w.host)
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case h.State != lifecycle.HostCordoned:
		t.Fatalf("a heartbeat un-cordoned the host: state = %q", h.State)
	case h.NVMeCommittedBytes != held:
		t.Fatalf("a heartbeat changed the committed capacity: committed = %d, want %d", h.NVMeCommittedBytes, held)
	case h.AgentVersion != "v2" || h.MaxFormatVersion != 3 || h.NVMeTotalBytes != 1<<41 || h.NVMeUsedBytes != 123:
		t.Fatalf("a heartbeat did not update the fields it owns: %+v", h)
	case h.RemoteBacklogBytes != 456:
		// The remote backlog is reported by the host, so it is one of the fields a
		// heartbeat owns — unlike committed capacity above, which is derived from
		// the rows that say who holds what and is the Control Plane's (ADR-0017).
		t.Fatalf("a heartbeat did not update the remote backlog: %+v", h)
	}
}

// recordOperation: the one mutation whose whole purpose is admin idempotency
// (INV-21) must let the caller tell "already done" from "you are not the leader".
// An operator cancelling a drain through a CP that lost the election otherwise gets
// success while the real drain keeps promoting volumes.
func recordOperation(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)
	op := metadata.Operation{
		OperationID: id(), Kind: lifecycle.OpDrain, Phase: lifecycle.OpPending,
		DesiredState: []byte(`{}`), CurrentState: []byte(`{}`),
	}
	recorded, err := s.RecordOperation(ctx, w.term, op)
	if err != nil || !recorded {
		t.Fatalf("first record: recorded=%v err=%v", recorded, err)
	}
	recorded, err = s.RecordOperation(ctx, w.term, op)
	if err != nil || recorded {
		t.Fatalf("duplicate: want (false, nil), got (%v, %v)", recorded, err)
	}

	stale := w.term
	if _, err := s.AcquireLeadership(ctx, "cp-b"); err != nil {
		t.Fatal(err)
	}
	fresh := op
	fresh.OperationID = id()
	recorded, err = s.RecordOperation(ctx, stale, fresh)
	if !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("zombie CP: want ErrStaleTerm, got (%v, %v)", recorded, err)
	}
	if recorded {
		t.Fatal("zombie CP reported the operation as recorded")
	}
	if _, err := s.GetOperation(ctx, fresh.OperationID); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("zombie CP wrote a row: %v", err)
	}
}

// volumeLifecycle: the §7 states the schema declares must be reachable through the
// Store. A volume mid-promotion persisted as ACTIVE leaves a restarted or second CP
// with no durable signal that it is being fenced, so it can start a competing attach.
func volumeLifecycle(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)

	// §7's path to a new writer: ACTIVE -> PRIMARY_SUSPECTED -> FENCING_WAIT ->
	// RECOVERY_REQUIRED -> RECOVERING -> ACTIVE. Every step must be storable.
	path := []lifecycle.VolumeState{
		lifecycle.VolumePrimarySuspected, lifecycle.VolumeFencingWait,
		lifecycle.VolumeRecoveryRequired, lifecycle.VolumeRecovering, lifecycle.VolumeActive,
	}
	for _, want := range path {
		if err := setVolumeState(ctx, s, w.term, w.vol, want); err != nil {
			t.Fatalf("SetVolumeState(%s): %v", want, err)
		}
		v, _ := s.GetVolume(ctx, w.vol)
		if v.State != want {
			t.Fatalf("state = %q, want %q", v.State, want)
		}
	}

	// §7: the way out of a suspicion never leads straight back to serving, and the
	// guard belongs in the write, not in the caller.
	if err := setVolumeState(ctx, s, w.term, w.vol, lifecycle.VolumeRecovering); !errors.Is(err, lifecycle.ErrInvalidTransition) {
		t.Fatalf("ACTIVE -> RECOVERING: want ErrInvalidTransition, got %v", err)
	}
	if v, _ := s.GetVolume(ctx, w.vol); v.State != lifecycle.VolumeActive {
		t.Fatalf("a refused transition changed the state to %q", v.State)
	}
}

// snapshotLifecycle: a snapshot whose publication crashed must be movable to FAILED
// and then DELETING, or it stays CREATING forever, the catalog side of GC never sees
// it, and its objects are never collected.
func snapshotLifecycle(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s) // w.snap is CREATING

	if err := setSnapshotState(ctx, s, w.term, w.snap, lifecycle.SnapshotFailed); err != nil {
		t.Fatalf("CREATING -> FAILED: %v", err)
	}
	if err := setSnapshotState(ctx, s, w.term, w.snap, lifecycle.SnapshotDeleting); err != nil {
		t.Fatalf("FAILED -> DELETING: %v", err)
	}
	if got, _ := s.GetSnapshot(ctx, w.snap); got.State != lifecycle.SnapshotDeleting {
		t.Fatalf("state = %q, want DELETING", got.State)
	}

	// INV-16: a published snapshot never goes back to being built.
	other := id()
	if err := s.CreateSnapshot(ctx, w.term, metadata.Snapshot{
		SnapshotID: other, VolumeID: w.vol, Epoch: 1, TargetSequence: 1, RootDigest: "d",
		State: lifecycle.SnapshotPublished, RequestID: id(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := setSnapshotState(ctx, s, w.term, other, lifecycle.SnapshotCreating); !errors.Is(err, lifecycle.ErrInvalidTransition) {
		t.Fatalf("PUBLISHED -> CREATING: want ErrInvalidTransition, got %v", err)
	}
	if got, _ := s.GetSnapshot(ctx, other); got.State != lifecycle.SnapshotPublished {
		t.Fatalf("a refused transition changed the state to %q", got.State)
	}
}

// pendingSnapshots is the request half of §19: a CREATING snapshot is how a host is
// asked to freeze a volume, and PublishSnapshot is how the answer comes back.
//
// The two things it pins are the ones a plausible implementation gets wrong. The list
// follows the *volume's current primary*, not snapshots.source_host_id — which is empty
// until somebody takes it, so filtering on it would list nothing for anyone. And a
// second report of the same publication is a no-op rather than an error, because the
// Agent keeps reporting until the request stops arriving; a different sequence at the
// same id is refused, because INV-16 says a published snapshot never changes.
func pendingSnapshots(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s) // w.snap is CREATING on w.vol, whose primary is w.host

	pending, err := s.ListPendingSnapshots(ctx, w.host)
	if err != nil {
		t.Fatalf("ListPendingSnapshots: %v", err)
	}
	if len(pending) != 1 || pending[0].SnapshotID != w.snap {
		t.Fatalf("pending = %+v, want just %s", pending, w.snap)
	}
	if other, err := s.ListPendingSnapshots(ctx, id()); err != nil || len(other) != 0 {
		t.Fatalf("a host that serves nothing was asked for %d snapshots (err %v)", len(other), err)
	}

	const seq = 41
	key := "image/vol/snapshots/snap.json"
	if err := s.PublishSnapshot(ctx, w.term, w.snap, seq, w.host, key); err != nil {
		t.Fatalf("PublishSnapshot: %v", err)
	}
	got, err := s.GetSnapshot(ctx, w.snap)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != lifecycle.SnapshotPublished || got.TargetSequence != seq ||
		got.SourceHostID != w.host || got.ManifestKey != key {
		t.Fatalf("published snapshot = %+v", got)
	}
	// Published means no longer asked for: an Agent that kept being asked would keep
	// answering, and the loop would never close.
	if pending, err := s.ListPendingSnapshots(ctx, w.host); err != nil || len(pending) != 0 {
		t.Fatalf("a published snapshot is still pending: %+v (err %v)", pending, err)
	}

	if err := s.PublishSnapshot(ctx, w.term, w.snap, seq, w.host, key); err != nil {
		t.Fatalf("re-reporting the same publication: %v", err)
	}
	if err := s.PublishSnapshot(ctx, w.term, w.snap, seq+1, w.host, key); !errors.Is(err, lifecycle.ErrInvalidTransition) {
		t.Fatalf("re-reporting at a different sequence: want ErrInvalidTransition, got %v", err)
	}
	if got, _ := s.GetSnapshot(ctx, w.snap); got.TargetSequence != seq {
		t.Fatalf("a refused report moved the sequence to %d", got.TargetSequence)
	}
}

// concurrentBumps: an epoch is the fencing token, and a promotion decides which one
// to grant by reading the volume first. So the bump has to be a compare-and-set on
// what was read, not an increment: n promoters that all saw epoch e must produce one
// winner at e+1, not n epochs burnt in a row.
//
// The damage the blind increment does is not the wasted numbers. Each promoter CASes
// the S3 epoch object to the epoch *it* computed (e+1) and only one of those CASes
// wins, while every one of them has already written its own host into
// primary_host_id. The volume row then names a host that never won the object and
// never got a lease, and the drain's listing, promotion's resume branch and
// rebuild-metadata all read that row.
//
// This case replaces an earlier one that asserted the opposite — n bumps produce n
// distinct epochs — which pinned the blind increment as if it were the contract.
func concurrentBumps(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)
	const n = 8

	before, err := s.GetVolume(ctx, w.vol)
	if err != nil {
		t.Fatal(err)
	}
	hosts := make([]string, n)
	for i := range hosts {
		hosts[i] = id()
		if err := s.UpsertHost(ctx, w.term, metadata.Host{HostID: hosts[i], State: lifecycle.HostActive}); err != nil {
			t.Fatal(err)
		}
	}

	var (
		mu      sync.Mutex
		granted = map[string]int64{} // host -> epoch it was told it holds
		wg      sync.WaitGroup
	)
	for _, host := range hosts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e, err := bumpVolumeEpoch(ctx, s, w.term, w.vol, host, before.CurrentEpoch)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				granted[host] = e
			case errors.Is(err, metadata.ErrEpochConflict):
				// The volume moved on: this promoter must re-read and start again.
			default:
				t.Errorf("losing bump: want ErrEpochConflict, got %v", err)
			}
		}()
	}
	wg.Wait()

	if len(granted) != 1 {
		t.Fatalf("%d promoters were told they hold an epoch, want exactly 1: %v", len(granted), granted)
	}
	v, err := s.GetVolume(ctx, w.vol)
	if err != nil {
		t.Fatal(err)
	}
	for host, e := range granted {
		if e != before.CurrentEpoch+1 {
			t.Fatalf("winner was granted epoch %d, want %d", e, before.CurrentEpoch+1)
		}
		if v.CurrentEpoch != e {
			t.Fatalf("volume is at epoch %d while the winner holds %d", v.CurrentEpoch, e)
		}
		if v.PrimaryHostID != host {
			t.Fatalf("volume names %q as primary while %q won the epoch", v.PrimaryHostID, host)
		}
	}
}

// volumePlacement: primary_host_id used to be write-once. CreateVolume set it and its
// converging upsert protected it with COALESCE, so a volume could never be detached,
// never re-placed, and the volumes rebuild-metadata restores with no host could never
// be given one.
//
// The observable throughout is ListVolumesByHost, not the column: that query is what
// cpserver.GetDesiredState answers an Agent with, so "the volume left this host and
// arrived at that one" is asserted as the two hosts' desired states changing. A store
// that wrote the column and answered the listing from somewhere else would satisfy an
// assertion on GetVolume alone and still leave the old Agent serving.
func volumePlacement(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)
	other, third := id(), id()
	for _, h := range []string{other, third} {
		if err := s.UpsertHost(ctx, w.term, metadata.Host{
			HostID: h, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// The epoch the volume starts at, so the "detach grants no epoch" assertions below
	// compare against something the test did not assume.
	start, err := s.GetVolume(ctx, w.vol)
	if err != nil {
		t.Fatal(err)
	}

	// hostServes is the desired state of one host, as an Agent would receive it.
	hostServes := func(t *testing.T, host string) bool {
		t.Helper()
		vols, err := s.ListVolumesByHost(ctx, host)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range vols {
			if v.VolumeID == w.vol {
				return true
			}
		}
		return false
	}

	if !hostServes(t, w.host) {
		t.Fatal("the fixture volume is not in its own host's desired state")
	}

	t.Run("a stale term cannot detach", func(t *testing.T) {
		stale := w.term
		term, err := s.AcquireLeadership(ctx, "cp-zombie")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.SetVolumePrimaryHost(ctx, stale, w.vol, ""); !errors.Is(err, metadata.ErrStaleTerm) {
			t.Fatalf("detach under a stale term: want ErrStaleTerm, got %v", err)
		}
		if !hostServes(t, w.host) {
			t.Fatal("a zombie Control Plane took the volume out of its host's desired state")
		}
		w.term = term
	})

	t.Run("detaching takes the volume out of its host's desired state", func(t *testing.T) {
		if err := s.SetVolumePrimaryHost(ctx, w.term, w.vol, ""); err != nil {
			t.Fatalf("detach: %v", err)
		}
		if hostServes(t, w.host) {
			t.Fatal("the volume is still in the desired state of the host it was detached from")
		}
		v, err := s.GetVolume(ctx, w.vol)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case v.PrimaryHostID != "":
			t.Fatalf("the volume still names %q as its primary", v.PrimaryHostID)
		case v.State != lifecycle.VolumeDetached:
			// A volume with no writer that is still ACTIVE is one
			// controlplane.RequestSnapshot accepts and no Agent can ever take.
			t.Fatalf("a volume with no host is in state %q, want DETACHED", v.State)
		case v.CurrentEpoch != start.CurrentEpoch:
			// Detaching grants the fencing token to nobody, and the Agent's WAL lives
			// under <volume>/<epoch>: a bump would abandon the local records of a
			// volume that is meant to be re-attachable.
			t.Fatalf("detaching moved the epoch from %d to %d", start.CurrentEpoch, v.CurrentEpoch)
		}
	})

	t.Run("detaching twice is the state the caller asked for", func(t *testing.T) {
		if err := s.SetVolumePrimaryHost(ctx, w.term, w.vol, ""); err != nil {
			t.Fatalf("re-detaching an already detached volume must be a no-op, got %v", err)
		}
		v, err := s.GetVolume(ctx, w.vol)
		if err != nil {
			t.Fatal(err)
		}
		if v.PrimaryHostID != "" || v.State != lifecycle.VolumeDetached ||
			v.CurrentEpoch != start.CurrentEpoch {
			t.Fatalf("the second detach changed something: %+v", v)
		}
	})

	t.Run("a detached volume can be placed again", func(t *testing.T) {
		if err := s.SetVolumePrimaryHost(ctx, w.term, w.vol, other); err != nil {
			t.Fatalf("place: %v", err)
		}
		if !hostServes(t, other) {
			t.Fatal("the volume did not arrive in the new host's desired state")
		}
		if hostServes(t, w.host) {
			t.Fatal("the volume is in two hosts' desired states at once")
		}
		v, err := s.GetVolume(ctx, w.vol)
		if err != nil {
			t.Fatal(err)
		}
		if v.State != lifecycle.VolumeActive || v.CurrentEpoch != start.CurrentEpoch {
			t.Fatalf("placing a volume left it %+v", v)
		}
	})

	t.Run("placing it where it already is changes nothing", func(t *testing.T) {
		if err := s.SetVolumePrimaryHost(ctx, w.term, w.vol, other); err != nil {
			t.Fatalf("re-placing on the same host must be a no-op, got %v", err)
		}
		if !hostServes(t, other) {
			t.Fatal("the volume left the host it was re-placed on")
		}
	})

	t.Run("a straight hand-over is refused and writes nothing", func(t *testing.T) {
		err := s.SetVolumePrimaryHost(ctx, w.term, w.vol, third)
		if !errors.Is(err, metadata.ErrAlreadyPlaced) {
			t.Fatalf("A -> B in one write: want ErrAlreadyPlaced, got %v", err)
		}
		if hostServes(t, third) {
			t.Fatal("the refused hand-over still put the volume in the destination's desired state")
		}
		if !hostServes(t, other) {
			t.Fatal("the refused hand-over took the volume away from the host that holds it")
		}
	})
}

// hostLeases: the lease is the Agent's authority to ACK a FLUSH (§12.2), and until
// now nothing could take one back or refuse to issue one.
//
// The scenario is a host the Control Plane has already declared DEAD — the same
// assertion promotion accepts as "the old writer is gone, skip the fencing wait".
// If a routine heartbeat can still renew that host's lease, the CP contradicts
// itself: it re-arms the writer it just fenced while the new primary is materialising
// the epoch, and both ACK.
func hostLeases(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)

	if err := s.RenewHostLease(ctx, w.term, w.host, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetHostLease(ctx, w.host); err != nil {
		t.Fatal(err)
	}

	t.Run("revoking a lease takes it away", func(t *testing.T) {
		if err := revokeHostLease(ctx, s, w.term, w.host); err != nil {
			t.Fatalf("RevokeHostLease: %v", err)
		}
		if _, err := s.GetHostLease(ctx, w.host); !errors.Is(err, metadata.ErrNotFound) {
			t.Fatalf("lease after revoke: want ErrNotFound, got %v", err)
		}
	})

	t.Run("revoking again is a no-op", func(t *testing.T) {
		if err := revokeHostLease(ctx, s, w.term, w.host); err != nil {
			t.Fatalf("a second revoke must converge, not fail: %v", err)
		}
	})

	t.Run("a live host may take its lease back", func(t *testing.T) {
		if err := s.RenewHostLease(ctx, w.term, w.host, 10); err != nil {
			t.Fatalf("re-granting a lease to a live host: %v", err)
		}
	})

	t.Run("a dead host cannot renew", func(t *testing.T) {
		if err := s.SetHostState(ctx, w.term, w.host, lifecycle.HostDead); err != nil {
			t.Fatal(err)
		}
		if err := s.RenewHostLease(ctx, w.term, w.host, 10); !errors.Is(err, metadata.ErrHostNotServing) {
			t.Fatalf("renewing the lease of a DEAD host: want ErrHostNotServing, got %v", err)
		}
	})

	t.Run("a cordoned or draining host still renews", func(t *testing.T) {
		// A cordon stops new placement and a drain evacuates, but both hosts are
		// still serving the volumes they hold: taking their lease away for the whole
		// evacuation would stop the ACKs of volumes nobody is moving (ADR-0016).
		for _, state := range []lifecycle.HostState{lifecycle.HostCordoned, lifecycle.HostDraining} {
			if err := s.SetHostState(ctx, w.term, w.host, state); err != nil {
				t.Fatal(err)
			}
			if err := s.RenewHostLease(ctx, w.term, w.host, 10); err != nil {
				t.Fatalf("renewing the lease of a %s host: %v", state, err)
			}
		}
	})

	// ADR-0016 stage 1: the bounded version of that refusal. The Control Plane
	// revokes a lease to fence one volume, and the host's next heartbeat would put it
	// straight back; the window is how long that heartbeat is refused for.
	t.Run("a revocation window refuses renewals while it is open", func(t *testing.T) {
		if err := s.BlockHostRenewals(ctx, w.term, w.host, time.Minute); err != nil {
			t.Fatalf("BlockHostRenewals: %v", err)
		}
		if err := s.RenewHostLease(ctx, w.term, w.host, 10); !errors.Is(err, metadata.ErrRenewalsBlocked) {
			t.Fatalf("renewing inside the window: want ErrRenewalsBlocked, got %v", err)
		}
		h, err := s.GetHost(ctx, w.host)
		if err != nil {
			t.Fatal(err)
		}
		if h.RenewalsBlockedUntil.IsZero() {
			t.Fatal("the window is not visible on the host row")
		}
	})

	t.Run("closing it lets the host renew again", func(t *testing.T) {
		if err := s.UnblockHostRenewals(ctx, w.term, w.host); err != nil {
			t.Fatalf("UnblockHostRenewals: %v", err)
		}
		if err := s.RenewHostLease(ctx, w.term, w.host, 10); err != nil {
			t.Fatalf("renewing after the window closed: %v", err)
		}
		h, err := s.GetHost(ctx, w.host)
		if err != nil {
			t.Fatal(err)
		}
		if !h.RenewalsBlockedUntil.IsZero() {
			t.Fatalf("the window is still recorded as open: %v", h.RenewalsBlockedUntil)
		}
	})

	t.Run("closing a window that is not open is a no-op", func(t *testing.T) {
		if err := s.UnblockHostRenewals(ctx, w.term, w.host); err != nil {
			t.Fatalf("a second close must converge, not fail: %v", err)
		}
	})

	t.Run("a window that has expired stops refusing", func(t *testing.T) {
		// The Control Plane that opened it may not survive to close it, so the
		// deadline is the backstop: a host whose drain died must serve again.
		if err := s.BlockHostRenewals(ctx, w.term, w.host, -time.Minute); err != nil {
			t.Fatalf("BlockHostRenewals: %v", err)
		}
		if err := s.RenewHostLease(ctx, w.term, w.host, 10); err != nil {
			t.Fatalf("an expired window still refuses renewals: %v", err)
		}
	})

	t.Run("a heartbeat cannot close a window the Control Plane opened", func(t *testing.T) {
		if err := s.BlockHostRenewals(ctx, w.term, w.host, time.Minute); err != nil {
			t.Fatal(err)
		}
		// UpsertHost is the Agent reporting on itself; the window is the CP's.
		if err := s.UpsertHost(ctx, w.term, metadata.Host{
			HostID: w.host, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
		}); err != nil {
			t.Fatal(err)
		}
		if err := s.RenewHostLease(ctx, w.term, w.host, 10); !errors.Is(err, metadata.ErrRenewalsBlocked) {
			t.Fatalf("a heartbeat closed the window fencing one of its volumes: %v", err)
		}
		if err := s.UnblockHostRenewals(ctx, w.term, w.host); err != nil {
			t.Fatal(err)
		}
	})
}

// capacity is ADR-0017: committed capacity is derived from the rows that already
// say who holds what, and the §28.2 oversubscription bound is a predicate of the
// writes that place bytes on a host.
//
//	committed(host) = Σ size_bytes of the volumes whose primary is host
//	                + Σ size_bytes reserved by in-flight operation plans targeting it
//
// The bound has to live inside those writes and not before them, for the reason
// wave 3 established: placement.Choose evaluates it correctly and is pure, so two
// operations that read the fleet before either placed anything choose the same
// destination, both proceed, and the host ends up past MaxOversubscription ×
// NVMeTotalBytes with neither caller having made a mistake. Re-checking in Go
// narrows the window and keeps the race.
//
// What is *not* here any more is everything a ledger needed: the non-negative
// guard, the expected-value compare-and-set, the "did my own delta land?" proof. A
// derived value has no delta to apply twice.
func capacity(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s) // one ACTIVE host, 1 TiB of NVMe, holding one 1 GiB volume
	const (
		gib   = int64(1) << 30
		total = int64(1) << 40 // 1024 GiB
	)
	other := id()
	if err := s.UpsertHost(ctx, w.term, metadata.Host{
		HostID: other, State: lifecycle.HostActive, NVMeTotalBytes: total,
	}); err != nil {
		t.Fatal(err)
	}

	committed := func(t *testing.T, hostID string) int64 {
		t.Helper()
		h, err := s.GetHost(ctx, hostID)
		if err != nil {
			t.Fatal(err)
		}
		return h.NVMeCommittedBytes
	}

	t.Run("a host is charged for the volumes whose primary it is", func(t *testing.T) {
		if got := committed(t, w.host); got != gib {
			t.Fatalf("committed = %d, want %d (the world's one volume)", got, gib)
		}
		if got := committed(t, other); got != 0 {
			t.Fatalf("an empty host reports %d committed bytes", got)
		}
	})

	t.Run("an in-flight plan charges the destination before the volume is primary there", func(t *testing.T) {
		if err := s.UpdateOperation(ctx, w.term, metadata.Operation{
			OperationID: w.op, Phase: lifecycle.OpRunning,
			CurrentState: plan(w.vol, other, "MOVING"),
		}, nil); err != nil {
			t.Fatal(err)
		}
		if got := committed(t, other); got != gib {
			t.Fatalf("destination committed = %d, want %d — a volume in flight to it is not charged", got, gib)
		}
		// And it is still charged to the source, which is still serving it. Both
		// sides is the conservative direction: the alternative is two placements
		// that each believe they have the room.
		if got := committed(t, w.host); got != gib {
			t.Fatalf("source committed = %d, want %d while it still holds the volume", got, gib)
		}
	})

	t.Run("a settled entry reserves nothing", func(t *testing.T) {
		if err := s.UpdateOperation(ctx, w.term, metadata.Operation{
			OperationID: w.op, Phase: lifecycle.OpRunning,
			CurrentState: plan(w.vol, other, "DONE"),
		}, nil); err != nil {
			t.Fatal(err)
		}
		if got := committed(t, other); got != 0 {
			t.Fatalf("a DONE entry still reserves %d bytes", got)
		}
	})

	t.Run("a finished operation reserves nothing", func(t *testing.T) {
		if err := s.UpdateOperation(ctx, w.term, metadata.Operation{
			OperationID: w.op, Phase: lifecycle.OpRunning,
			CurrentState: plan(w.vol, other, "MOVING"),
		}, nil); err != nil {
			t.Fatal(err)
		}
		if got := committed(t, other); got != gib {
			t.Fatalf("setup: destination committed = %d, want %d", got, gib)
		}
		if err := s.UpdateOperation(ctx, w.term, metadata.Operation{
			OperationID: w.op, Phase: lifecycle.OpSucceeded,
			CurrentState: plan(w.vol, other, "MOVING"),
		}, nil); err != nil {
			t.Fatal(err)
		}
		if got := committed(t, other); got != 0 {
			t.Fatalf("a terminal operation still reserves %d bytes", got)
		}
	})

	t.Run("the bound is a predicate of the write that places a volume", func(t *testing.T) {
		tight := &metadata.CapacityBound{HostID: other, AddBytes: 2 * gib, Limit: gib}
		fresh := id()
		err := s.CreateVolume(ctx, w.term, metadata.Volume{DEKKeyID: 1,
			VolumeID: fresh, SizeBytes: 2 * gib, BlockSize: 65536, State: lifecycle.VolumeActive,
			PrimaryHostID: other, DEKWrapped: []byte{1}, KEKID: "k",
		}, tight)
		if !errors.Is(err, metadata.ErrCapacityExceeded) {
			t.Fatalf("CreateVolume past the bound = %v, want ErrCapacityExceeded", err)
		}
		if _, gerr := s.GetVolume(ctx, fresh); !errors.Is(gerr, metadata.ErrNotFound) {
			t.Fatalf("a refused placement wrote the volume anyway: %v", gerr)
		}
		// Exactly at the bound is admitted, and the same write with no bound at all
		// is not a placement decision and is never refused.
		if err := s.CreateVolume(ctx, w.term, metadata.Volume{DEKKeyID: 1,
			VolumeID: fresh, SizeBytes: 2 * gib, BlockSize: 65536, State: lifecycle.VolumeActive,
			PrimaryHostID: other, DEKWrapped: []byte{1}, KEKID: "k",
		}, &metadata.CapacityBound{HostID: other, AddBytes: 2 * gib, Limit: 2 * gib}); err != nil {
			t.Fatalf("a placement exactly at the bound was refused: %v", err)
		}
		if got := committed(t, other); got != 2*gib {
			t.Fatalf("destination committed = %d, want %d", got, 2*gib)
		}
	})

	t.Run("the bound is a predicate of the write that records a plan", func(t *testing.T) {
		// `other` now holds 2 GiB. A plan that would add the world's 1 GiB volume
		// under a 2 GiB ceiling has to be refused, progress and all: the entry *is*
		// the reservation.
		// A fresh operation: the one above was taken to SUCCEEDED, and a terminal
		// operation is refused by the lifecycle guard before the bound is reached.
		live := id()
		if _, rerr := s.RecordOperation(ctx, w.term, metadata.Operation{
			OperationID: live, Kind: lifecycle.OpDrain, Phase: lifecycle.OpPending,
			HostID: w.host, DesiredState: []byte(`{}`), CurrentState: []byte(`{}`),
		}); rerr != nil {
			t.Fatal(rerr)
		}
		before, err := s.GetOperation(ctx, live)
		if err != nil {
			t.Fatal(err)
		}
		err = s.UpdateOperation(ctx, w.term, metadata.Operation{
			OperationID: live, Phase: lifecycle.OpRunning,
			CurrentState: plan(w.vol, other, "MOVING"),
		}, &metadata.CapacityBound{HostID: other, AddBytes: gib, Limit: 2 * gib})
		if !errors.Is(err, metadata.ErrCapacityExceeded) {
			t.Fatalf("UpdateOperation past the bound = %v, want ErrCapacityExceeded", err)
		}
		after, err := s.GetOperation(ctx, live)
		if err != nil {
			t.Fatal(err)
		}
		if string(after.CurrentState) != string(before.CurrentState) || after.Phase != before.Phase {
			t.Fatalf("a refused reservation still wrote the progress: %+v", after)
		}
		if got := committed(t, other); got != 2*gib {
			t.Fatalf("destination committed = %d, want %d", got, 2*gib)
		}
	})
}

// plan builds the current_state a drain records for one volume in flight.
func plan(volumeID, toHost, stage string) []byte {
	return fmt.Appendf(nil, `{"total":1,"volumes":[{"volume_id":%q,"stage":%q,"to_host":%q}]}`,
		volumeID, stage, toHost)
}

// liveOperationsByHost: an operation id is the only handle GetOperation offers, and
// the question a reconciler has to answer before it starts work on a host — "is
// anything already happening here?" — arrives with a *different* id every time. The
// listing is what answers it, so its filter has to be exact in both directions: an
// operation belonging to another host, or to no host at all, must never be counted
// as work in progress here, and neither must one that has finished.
//
// Terminal operations are excluded by the listing itself rather than by the caller.
// The set of operations a host has ever had only grows — nothing deletes them — so a
// listing that returned all of them would make the check that runs before every
// drain pass get slower for the rest of the cluster's life.
func liveOperationsByHost(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s) // records one live drain operation for w.host

	other := id()
	if err := s.UpsertHost(ctx, w.term, metadata.Host{
		HostID: other, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
	}); err != nil {
		t.Fatal(err)
	}
	// A second live operation on the same host, one on another host, one recorded
	// with no host at all (a cancellation that arrived before the drain started),
	// and one that has finished. Only the first is work in progress here.
	//
	// The second one is an attach rather than a drain because a host may hold only
	// one live drain (that is the rule the drain exclusion used to enforce in Go);
	// what this case is about is the host filter, not the kind.
	mine, done := id(), id()
	for _, op := range []metadata.Operation{
		{OperationID: mine, Kind: lifecycle.OpAttach, HostID: w.host, Phase: lifecycle.OpRunning},
		{OperationID: id(), Kind: lifecycle.OpDrain, HostID: other, Phase: lifecycle.OpPending},
		{OperationID: id(), Kind: lifecycle.OpDrain, Phase: lifecycle.OpCanceling},
		{OperationID: done, Kind: lifecycle.OpAttach, HostID: w.host, Phase: lifecycle.OpPending},
	} {
		op.DesiredState, op.CurrentState = []byte(`{}`), []byte(`{}`)
		if _, err := s.RecordOperation(ctx, w.term, op); err != nil {
			t.Fatal(err)
		}
	}
	for _, phase := range []lifecycle.OperationPhase{lifecycle.OpRunning, lifecycle.OpSucceeded} {
		if err := s.UpdateOperation(ctx, w.term, metadata.Operation{
			OperationID: done, Kind: lifecycle.OpAttach, HostID: w.host, Phase: phase,
			DesiredState: []byte(`{}`), CurrentState: []byte(`{}`),
		}, nil); err != nil {
			t.Fatal(err)
		}
	}

	ops, err := s.ListLiveOperationsByHost(ctx, w.host)
	if err != nil {
		t.Fatalf("ListLiveOperationsByHost: %v", err)
	}
	got := make([]string, 0, len(ops))
	for _, op := range ops {
		if op.HostID != w.host {
			t.Fatalf("operation %s belongs to host %q", op.OperationID, op.HostID)
		}
		if op.Phase.Terminal() {
			t.Fatalf("operation %s is %s and is not live work", op.OperationID, op.Phase)
		}
		got = append(got, op.OperationID)
	}
	want := []string{w.op, mine}
	sort.Strings(want) // the listing is ordered by operation id (INV-02)
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("live operations for the host = %v, want %v", got, want)
	}
	if ops[0].Kind == "" || ops[0].Phase == "" {
		t.Fatalf("the listing must round-trip kind and phase: %+v", ops[0])
	}

	t.Run("a host with no operations lists none", func(t *testing.T) {
		ops, err := s.ListLiveOperationsByHost(ctx, id())
		if err != nil || len(ops) != 0 {
			t.Fatalf("unknown host: %d operations, err=%v", len(ops), err)
		}
	})

	t.Run("an empty host id is rejected", func(t *testing.T) {
		// Not "every operation nobody attached to a host": that set is exactly the
		// one a caller of this must never be handed.
		if _, err := s.ListLiveOperationsByHost(ctx, ""); !errors.Is(err, metadata.ErrInvalidID) {
			t.Fatalf("empty host id: want ErrInvalidID, got %v", err)
		}
	})
}

// oneLiveDrainPerHost: two evacuations of one host are not two halves of the same
// work. Each captures its own plan and promotes the same volumes, and whichever
// loses a race is left holding a destination reservation nobody will release —
// releasing it is the losing operation's own next step, and that step now fails for
// ever (§28.2, ADR-0017). The volumes are safe either way, since the promotion
// protocol serializes them; the accounting is not, and a host placement believes is
// full is a host that stays empty.
//
// The Control Plane checks this before it writes, but a check followed by a write is
// not exclusion: two goroutines inside one leader can both pass it. So the store
// itself refuses the second one, with a sentinel the caller can act on rather than
// an integrity error it can only log.
func oneLiveDrainPerHost(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s) // records one live drain for w.host

	second := metadata.Operation{
		OperationID: id(), Kind: lifecycle.OpDrain, HostID: w.host, Phase: lifecycle.OpPending,
		DesiredState: []byte(`{}`), CurrentState: []byte(`{}`),
	}
	if _, err := s.RecordOperation(ctx, w.term, second); !errors.Is(err, metadata.ErrDrainInProgress) {
		t.Fatalf("a second live drain: want ErrDrainInProgress, got %v", err)
	}
	if _, err := s.GetOperation(ctx, second.OperationID); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("the refused drain was recorded anyway: %v", err)
	}

	// The rule is about live *drains* of *this* host, and nothing wider.
	other := id()
	if err := s.UpsertHost(ctx, w.term, metadata.Host{
		HostID: other, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
	}); err != nil {
		t.Fatal(err)
	}
	allowed := []struct {
		name string
		op   metadata.Operation
	}{
		{"another kind on the same host", metadata.Operation{
			OperationID: id(), Kind: lifecycle.OpAttach, HostID: w.host, Phase: lifecycle.OpPending}},
		{"a drain of another host", metadata.Operation{
			OperationID: id(), Kind: lifecycle.OpDrain, HostID: other, Phase: lifecycle.OpPending}},
		{"a drain attached to no host", metadata.Operation{
			OperationID: id(), Kind: lifecycle.OpDrain, Phase: lifecycle.OpPending}},
	}
	for _, tc := range allowed {
		t.Run(tc.name, func(t *testing.T) {
			op := tc.op
			op.DesiredState, op.CurrentState = []byte(`{}`), []byte(`{}`)
			if _, err := s.RecordOperation(ctx, w.term, op); err != nil {
				t.Fatalf("must be allowed: %v", err)
			}
		})
	}

	t.Run("the host is drainable again once the owning operation finishes", func(t *testing.T) {
		for _, phase := range []lifecycle.OperationPhase{lifecycle.OpRunning, lifecycle.OpSucceeded} {
			if err := s.UpdateOperation(ctx, w.term, metadata.Operation{
				OperationID: w.op, Kind: lifecycle.OpDrain, HostID: w.host, Phase: phase,
				DesiredState: []byte(`{}`), CurrentState: []byte(`{}`),
			}, nil); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.RecordOperation(ctx, w.term, second); err != nil {
			t.Fatalf("a finished drain must not block the next one: %v", err)
		}
	})
}

// authorityClock: `last_renewal` is stamped by the store's clock, and the fencing
// deadline is derived from it. A Control Plane that compares that stamp against its
// own wall clock shortens the wait by exactly the offset between the two — an NTP
// correction, a VM restored from a snapshot, a bad RTC — so the promoter has to be
// able to ask the store what time it thinks it is.
func authorityClock(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)

	before, err := now(ctx, s)
	if err != nil {
		t.Fatalf("Now: %v", err)
	}
	if before.IsZero() {
		t.Fatal("Now returned the zero instant")
	}
	if err := s.RenewHostLease(ctx, w.term, w.host, 10); err != nil {
		t.Fatal(err)
	}
	l, err := s.GetHostLease(ctx, w.host)
	if err != nil {
		t.Fatal(err)
	}
	after, err := now(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	// The same clock, so the row's stamp is inside the interval bracketing it.
	// (Expressed with Before both ways round: the simulable analyzer cannot tell
	// time.Time.After from time.After, and INV-01 forbids the latter.)
	if l.LastRenewal.Before(before) || after.Before(l.LastRenewal) {
		t.Fatalf("last_renewal %v is outside [%v, %v] — Now is not the clock that stamps rows",
			l.LastRenewal, before, after)
	}
}

// emptyIDs: an unset config field reaches the store as "". A primary key is never
// meaningfully empty, so it is rejected rather than written — a row keyed on the
// empty string is invisible to every lookup that follows.
func emptyIDs(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)

	tests := []mutation{
		{"UpsertHost", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.UpsertHost(ctx, term, metadata.Host{HostID: "", State: lifecycle.HostActive})
		}},
		{"SetHostState", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.SetHostState(ctx, term, "", lifecycle.HostCordoned)
		}},
		{"RenewHostLease", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.RenewHostLease(ctx, term, "", 10)
		}},
		{"BlockHostRenewals", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.BlockHostRenewals(ctx, term, "", time.Minute)
		}},
		{"UnblockHostRenewals", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.UnblockHostRenewals(ctx, term, "")
		}},
		{"RevokeHostLease", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return revokeHostLease(ctx, s, term, "")
		}},
		{"CreateVolume", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.CreateVolume(ctx, term, metadata.Volume{DEKKeyID: 1,
				VolumeID: "", SizeBytes: 1, BlockSize: 65536, State: lifecycle.VolumeActive,
				DEKWrapped: []byte{1}, KEKID: "k",
			}, nil)
		}},
		{"BumpVolumeEpoch", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			_, err := bumpVolumeEpoch(ctx, s, term, "", w.host, 0)
			return err
		}},
		{"UpdateWatermarks", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.UpdateWatermarks(ctx, term, "", 3, 2, 1)
		}},
		{"ResizeVolume", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.ResizeVolume(ctx, term, "", 1<<31)
		}},
		{"SetVolumeState", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return setVolumeState(ctx, s, term, "", lifecycle.VolumePrimarySuspected)
		}},
		{"CreateSnapshot", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.CreateSnapshot(ctx, term, metadata.Snapshot{
				SnapshotID: "", VolumeID: w.vol, Epoch: 1, TargetSequence: 1, RootDigest: "d",
				State: lifecycle.SnapshotCreating, RequestID: id(),
			})
		}},
		{"SetSnapshotState", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return setSnapshotState(ctx, s, term, "", lifecycle.SnapshotPublished)
		}},
		{"PublishSnapshot", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.PublishSnapshot(ctx, term, "", 7, "", "k")
		}},
		{"RecordOperation", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			_, err := s.RecordOperation(ctx, term, metadata.Operation{
				OperationID: "", Kind: lifecycle.OpDrain, Phase: lifecycle.OpPending,
				DesiredState: []byte(`{}`), CurrentState: []byte(`{}`),
			})
			return err
		}},
		{"UpdateOperation", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.UpdateOperation(ctx, term, metadata.Operation{
				OperationID: "", Phase: lifecycle.OpRunning, CurrentState: []byte(`{}`),
			}, nil)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(ctx, s, w.term, w); !errors.Is(err, metadata.ErrInvalidID) {
				t.Fatalf("empty id: want ErrInvalidID, got %v", err)
			}
		})
	}
}
