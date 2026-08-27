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
//     happened — is metadata.ErrStaleTerm even when the row is also missing and the
//     transition is also illegal. A zombie CP must learn that it is a zombie; every
//     other diagnosis it could be handed is a lie that sends a reconciler down the
//     wrong branch.
//   - Then existence. A mutation naming a row that does not exist is
//     metadata.ErrNotFound, never ErrStaleTerm.
//   - Then the domain guard: lifecycle.ErrInvalidTransition, ErrEpochConflict,
//     ErrCapacityExceeded, ErrWatermarkOrder.
//   - Creates are idempotent and never destructive: re-creating a volume never
//     lowers its epoch, blanks its ownership, shrinks it, rewinds its watermarks or
//     rewrites its lifecycle state, and re-creating a snapshot is a no-op (INV-16).
//   - A volume's geometry is fixed at create: no mutation on the Store changes
//     size_bytes or block_size. V1 has no resize (metadata.Store carries why), and
//     the Agent, the WAL and descriptor.json all carry the number they were handed
//     when the volume was made.
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
	"reflect"
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
		{"VolumeGeometryIsImmutable", volumeGeometry},
		{"RecreatingASnapshotIsANoOp", snapshotRecreate},
		{"WatermarksAreOrderedAndNeverGoBackwards", watermarks},
		{"ARefusalClearsAndCannotBeWrittenByAFencedHost", refusals},
		{"UpsertHostDoesNotClobberStateOrCapacity", upsertHost},
		{"ACordonRecordsWhoPlacedItAndOutranksThePressureLoop", cordonAuthority},
		{"VolumeLifecycleIsExpressible", volumeLifecycle},
		{"SnapshotLifecycleIsExpressible", snapshotLifecycle},
		{"PendingSnapshotsFollowTheVolumeAndPublishOnce", pendingSnapshots},
		{"FleetWideReadsSeeTheRowsNoHostOwns", fleetWideReads},
		{"ConcurrentEpochBumpsAreSerialized", concurrentBumps},
		{"VolumePlacementIsChangeableAndClearingIsIdempotent", volumePlacement},
		{"AVolumeIsRemovedOnlyWhenNothingDescendsFromIt", volumeDelete},
		{"EmptyIdentifiersAreRejected", emptyIDs},
		{"HostLeasesRenewAndADeadHostCannotRenew", hostLeases},
		{"LeadershipRenewsWithoutMovingTheTerm", leadershipRenewal},
		{"CapacityIsDerivedAndTheBoundIsAPredicateOfTheWrite", capacity},
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

// world is a small fixture: a leader, one host, one volume and one snapshot — so
// that "the row is missing" is never the reason a mutation fails.
type world struct {
	term int64
	host string
	vol  string
	snap string
}

func newWorld(t *testing.T, s metadata.Store) world {
	t.Helper()
	ctx := t.Context()
	term, err := s.AcquireLeadership(ctx, "cp-a")
	if err != nil {
		t.Fatalf("AcquireLeadership: %v", err)
	}
	w := world{term: term, host: id(), vol: id(), snap: id()}
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
		// The leadership renewal is a mutation like any other and is guarded like one:
		// the process that lost the election learns it here, on the write it makes every
		// few seconds, rather than on the first Agent mutation that happens to arrive.
		{"RenewLeadership", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.RenewLeadership(ctx, term, "cp-a")
		}},
		{"UpsertHost", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.UpsertHost(ctx, term, metadata.Host{HostID: w.host, State: lifecycle.HostActive})
		}},
		{"SetHostState", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.SetHostState(ctx, term, w.host, lifecycle.HostCordoned, lifecycle.CordonOperator)
		}},
		{"RenewHostLease", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.RenewHostLease(ctx, term, w.host, 10)
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
		{"ClearVolumeParent", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.ClearVolumeParent(ctx, term, w.vol)
		}},
		{"SetVolumeRefusal", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.SetVolumeRefusal(ctx, term, w.vol, w.host, 0, lifecycle.RefusalImageMissing, "the bucket has no manifest")
		}},
		// DeleteVolume builds its own row and destroys that one. Every other entry
		// here operates on the fixture, and this one cannot: volumeGeometry runs the
		// whole surface and then reads w.vol back, so a delete of w.vol would make the
		// only destructive method in this interface look like a broken one. The create
		// is inside the closure rather than in the world for the same reason the term
		// cases still work — a stale term fails it first, and its error is the
		// ErrStaleTerm those cases are asserting on.
		{"DeleteVolume", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			doomed := id()
			if err := s.CreateVolume(ctx, term, metadata.Volume{DEKKeyID: 1,
				VolumeID: doomed, SizeBytes: 1, BlockSize: 65536, State: lifecycle.VolumeActive,
				DEKWrapped: []byte{1}, KEKID: "k",
			}, nil); err != nil {
				return err
			}
			return s.DeleteVolume(ctx, term, doomed)
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
	w := world{host: id(), vol: id(), snap: id()}
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
	ghostHost, ghostVol, ghostSnap := id(), id(), id()

	tests := []mutation{
		{"SetHostState", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.SetHostState(ctx, term, ghostHost, lifecycle.HostCordoned, lifecycle.CordonOperator)
		}},
		{"RenewHostLease", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.RenewHostLease(ctx, term, ghostHost, 10)
		}},
		{"BumpVolumeEpoch", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			_, err := bumpVolumeEpoch(ctx, s, term, ghostVol, w.host, 0)
			return err
		}},
		{"UpdateWatermarks", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.UpdateWatermarks(ctx, term, ghostVol, 3, 2, 1)
		}},
		{"SetVolumeState", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return setVolumeState(ctx, s, term, ghostVol, lifecycle.VolumePrimarySuspected)
		}},
		{"SetVolumePrimaryHost", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.SetVolumePrimaryHost(ctx, term, ghostVol, w.host)
		}},
		{"ClearVolumeParent", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.ClearVolumeParent(ctx, term, ghostVol)
		}},
		{"SetVolumeRefusal", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.SetVolumeRefusal(ctx, term, ghostVol, w.host, 0, lifecycle.RefusalImageMissing, "the bucket has no manifest")
		}},
		// A re-run of a delete that already finished lands here, and it is the reason
		// the sentinel matters rather than a detail of it: the command reads
		// ErrNotFound as "the catalog half is done" and carries on.
		{"DeleteVolume", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.DeleteVolume(ctx, term, ghostVol)
		}},
		{"SetSnapshotState", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return setSnapshotState(ctx, s, term, ghostSnap, lifecycle.SnapshotPublished)
		}},
		{"PublishSnapshot", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.PublishSnapshot(ctx, term, ghostSnap, 7, "", "k")
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
// reason, the term is the answer. Otherwise the zombie is told "no such volume" or
// "that is not the epoch you compared against" and concludes it is still the leader.
func staleTermWins(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)
	stale := w.term
	if _, err := s.AcquireLeadership(ctx, "cp-b"); err != nil {
		t.Fatal(err)
	}
	ghost := id()

	tests := []struct {
		name string
		call func() error
	}{
		{"missing volume under a stale term", func() error {
			_, err := bumpVolumeEpoch(ctx, s, stale, ghost, w.host, 0)
			return err
		}},
		{"wrong expected epoch under a stale term", func() error {
			_, err := bumpVolumeEpoch(ctx, s, stale, w.vol, w.host, 99)
			return err
		}},
		{"illegal host transition under a stale term", func() error {
			return s.SetHostState(ctx, stale, w.host, lifecycle.HostActive, lifecycle.CordonOperator)
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
		BlockSize: 65536, RPOTargetSeconds: 900, CurrentEpoch: 7, State: lifecycle.VolumeDetached,
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
		got.BlockSize != want.BlockSize || got.RPOTargetSeconds != want.RPOTargetSeconds ||
		got.CurrentEpoch != want.CurrentEpoch ||
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

// volumeGeometry: a volume's size and block size are what CreateVolume was given, for
// as long as the row exists. This is the standing half of "V1 does not resize" —
// metadata.Store says why the verb is gone, and this says the property that replaced
// it, in the one place both implementations are held to it.
//
// It runs *everyMutation*, which is the point and is why it is not a list of the
// methods that plausibly touch a volume. That list is the whole mutating surface and
// carries the rule that a method added to the Store without a line in it is a method
// whose term guard nobody checks; the same line now also asks whether the new method
// moved a geometry it had no business moving. A resize brought back as a store method
// and nothing else — the exact shape this deleted — fails here rather than passing a
// suite that never looked.
//
// Each mutation gets its own world, so this proves something about each method rather
// than about the order they happen to run in, and every one of them is required to
// succeed: a case that silently errored would assert that a write which never happened
// changed nothing.
func volumeGeometry(t *testing.T, s metadata.Store) {
	for _, m := range everyMutation() {
		t.Run(m.name, func(t *testing.T) {
			ctx := t.Context()
			w := newWorld(t, s)
			before, err := s.GetVolume(ctx, w.vol)
			if err != nil {
				t.Fatal(err)
			}
			if err := m.call(ctx, s, w.term, w); err != nil {
				t.Fatalf("%s under the current term: %v", m.name, err)
			}
			after, err := s.GetVolume(ctx, w.vol)
			if err != nil {
				t.Fatal(err)
			}
			if after.SizeBytes != before.SizeBytes || after.BlockSize != before.BlockSize {
				t.Fatalf("%s changed the volume's geometry: %d/%d -> %d/%d (V1 has no resize)",
					m.name, before.SizeBytes, before.BlockSize, after.SizeBytes, after.BlockSize)
			}
		})
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

// refusals: the storage rule that makes a refusal different from a watermark, stated as
// the four things a caller depends on.
//
// A watermark is the newest of a monotonic series, so GREATEST is right and a late report
// is harmless. A refusal is a *state*, and the two ways a state column goes wrong are
// both here: it fails to clear when the condition ends (the volume reads NOT SERVED for
// ever), and it is written by somebody whose opinion no longer counts (a host the fleet
// moved past marks a volume its successor is serving perfectly well). Neither is visible
// from an assertion on the returned error — both writes "succeed".
func refusals(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)
	// newWorld places the volume on w.host at epoch 0, which is the epoch its reports
	// are qualified by until something promotes it.
	const epoch = 0

	t.Run("a refusal from the volume's own host at its own epoch lands", func(t *testing.T) {
		if err := s.SetVolumeRefusal(ctx, w.term, w.vol, w.host, epoch,
			lifecycle.RefusalImageMissing, "published up to 512 and the bucket holds nothing"); err != nil {
			t.Fatal(err)
		}
		v, _ := s.GetVolume(ctx, w.vol)
		if v.Refusal != lifecycle.RefusalImageMissing || v.RefusalDetail == "" {
			t.Fatalf("the refusal was not recorded: %+v", v)
		}
	})

	t.Run("a report from another host does not write it", func(t *testing.T) {
		other := id()
		if err := s.UpsertHost(ctx, w.term, metadata.Host{
			HostID: other, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
		}); err != nil {
			t.Fatal(err)
		}
		// Not an error: it is a writer that has been fenced, and a caller that treated
		// this as a failure would retry it for ever.
		if err := s.SetVolumeRefusal(ctx, w.term, w.vol, other, epoch,
			lifecycle.RefusalLeaseLost, "somebody else's opinion"); err != nil {
			t.Fatalf("a report from a host that does not hold the volume is ignored, not an error: %v", err)
		}
		v, _ := s.GetVolume(ctx, w.vol)
		if v.Refusal != lifecycle.RefusalImageMissing {
			t.Fatalf("a host that does not hold the volume rewrote its refusal: %+v", v)
		}
	})

	t.Run("a report under a superseded epoch does not write it", func(t *testing.T) {
		if err := s.SetVolumeRefusal(ctx, w.term, w.vol, w.host, epoch+7,
			lifecycle.RefusalNone, ""); err != nil {
			t.Fatalf("a report under the wrong epoch is ignored, not an error: %v", err)
		}
		v, _ := s.GetVolume(ctx, w.vol)
		if v.Refusal != lifecycle.RefusalImageMissing {
			t.Fatalf("a report under a foreign epoch cleared the refusal: %+v", v)
		}
	})

	t.Run("the volume serving again clears it, detail and all", func(t *testing.T) {
		if err := s.SetVolumeRefusal(ctx, w.term, w.vol, w.host, epoch, lifecycle.RefusalNone, ""); err != nil {
			t.Fatal(err)
		}
		v, _ := s.GetVolume(ctx, w.vol)
		if v.Refusal != lifecycle.RefusalNone || v.RefusalDetail != "" {
			t.Fatalf("a healthy report did not clear the refusal: %+v", v)
		}
	})

	t.Run("a detail cannot outlive the refusal it explains", func(t *testing.T) {
		// The one shape the schema refuses outright. It is asserted through the Store
		// rather than in SQL so both implementations answer the same way, and because a
		// sentence with nothing to explain is exactly what the next reader believes.
		if err := s.SetVolumeRefusal(ctx, w.term, w.vol, w.host, epoch,
			lifecycle.RefusalNone, "a sentence about nothing"); err != nil {
			t.Fatal(err)
		}
		v, _ := s.GetVolume(ctx, w.vol)
		if v.RefusalDetail != "" {
			t.Fatalf("a detail survived with no refusal on it: %+v", v)
		}
	})

	t.Run("a value outside the vocabulary is refused", func(t *testing.T) {
		err := s.SetVolumeRefusal(ctx, w.term, w.vol, w.host, epoch, lifecycle.Refusal("NOPE"), "x")
		if !errors.Is(err, lifecycle.ErrUnknownState) {
			t.Fatalf("want ErrUnknownState, got %v", err)
		}
	})

	t.Run("placing the volume elsewhere clears it", func(t *testing.T) {
		if err := s.SetVolumeRefusal(ctx, w.term, w.vol, w.host, epoch,
			lifecycle.RefusalNoKey, "this host holds no KEK"); err != nil {
			t.Fatal(err)
		}
		// Detach: the refusal was a statement about a host this volume no longer has,
		// and a reason that outlives its cause is one the next reader will believe.
		if err := s.SetVolumePrimaryHost(ctx, w.term, w.vol, ""); err != nil {
			t.Fatal(err)
		}
		v, _ := s.GetVolume(ctx, w.vol)
		if v.Refusal != lifecycle.RefusalNone || v.RefusalDetail != "" {
			t.Fatalf("a detached volume still says its old host is refusing it: %+v", v)
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
	if err := s.SetHostState(ctx, w.term, w.host, lifecycle.HostCordoned, lifecycle.CordonOperator); err != nil {
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
	case h.CordonReason != lifecycle.CordonOperator:
		// The reason has to survive the heartbeat for the same reason the state
		// does: a CORDONED host whose reason a routine upsert blanked is a host an
		// operator cannot tell from one the 70% rule cordoned, and the pressure loop
		// would then be free to un-cordon it (ADR-0013 §3).
		t.Fatalf("a heartbeat erased the cordon reason: reason = %q", h.CordonReason)
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

// cordonAuthority: ADR-0013 §3 makes the Control Plane cordon a host whose device
// passes 70% used, which means state = 'CORDONED' stopped being evidence that a human
// meant it. Two properties follow, and both are about what the *store* refuses,
// because the rule has to be a predicate of the write: a read-then-write in the
// caller leaves a window in which an operator's cordon lands between the two and is
// cleared anyway, which is the one outcome the reason column exists to prevent.
func cordonAuthority(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)

	t.Run("a cordon records who placed it", func(t *testing.T) {
		if err := s.SetHostState(ctx, w.term, w.host, lifecycle.HostCordoned, lifecycle.CordonPressure); err != nil {
			t.Fatal(err)
		}
		h, err := s.GetHost(ctx, w.host)
		if err != nil {
			t.Fatal(err)
		}
		if h.State != lifecycle.HostCordoned || h.CordonReason != lifecycle.CordonPressure {
			t.Fatalf("state = %q reason = %q, want CORDONED / DEVICE_PRESSURE", h.State, h.CordonReason)
		}
	})

	t.Run("an operator outranks the pressure loop", func(t *testing.T) {
		// Re-stamping an existing cordon is how an operator takes ownership of one
		// the fleet placed: the host does not move, the authority does.
		if err := s.SetHostState(ctx, w.term, w.host, lifecycle.HostCordoned, lifecycle.CordonOperator); err != nil {
			t.Fatal(err)
		}
		h, err := s.GetHost(ctx, w.host)
		if err != nil {
			t.Fatal(err)
		}
		if h.CordonReason != lifecycle.CordonOperator {
			t.Fatalf("reason = %q, want OPERATOR", h.CordonReason)
		}
	})

	t.Run("the pressure loop may not clear an operator's cordon", func(t *testing.T) {
		err := s.SetHostState(ctx, w.term, w.host, lifecycle.HostActive, lifecycle.CordonPressure)
		if !errors.Is(err, lifecycle.ErrCordonHeld) {
			t.Fatalf("un-cordoning an operator's cordon: want ErrCordonHeld, got %v", err)
		}
		h, gerr := s.GetHost(ctx, w.host)
		if gerr != nil {
			t.Fatal(gerr)
		}
		// The error is not the property; the row is. A store that returns the right
		// error and writes anyway satisfies any assertion on err.
		if h.State != lifecycle.HostCordoned || h.CordonReason != lifecycle.CordonOperator {
			t.Fatalf("the refused write landed anyway: state = %q reason = %q", h.State, h.CordonReason)
		}
	})

	t.Run("leaving CORDONED clears the reason", func(t *testing.T) {
		if err := s.SetHostState(ctx, w.term, w.host, lifecycle.HostActive, lifecycle.CordonOperator); err != nil {
			t.Fatal(err)
		}
		h, err := s.GetHost(ctx, w.host)
		if err != nil {
			t.Fatal(err)
		}
		// A reason that outlives its cordon is a reason the next reader will believe,
		// and the next reader is the pressure loop deciding whether it may act.
		if h.State != lifecycle.HostActive || h.CordonReason != lifecycle.CordonNone {
			t.Fatalf("state = %q reason = %q, want ACTIVE and no reason", h.State, h.CordonReason)
		}
	})

	t.Run("a write with no authority is refused", func(t *testing.T) {
		err := s.SetHostState(ctx, w.term, w.host, lifecycle.HostCordoned, lifecycle.CordonNone)
		if !errors.Is(err, lifecycle.ErrUnknownState) {
			t.Fatalf("cordoning with no reason: want ErrUnknownState, got %v", err)
		}
		h, gerr := s.GetHost(ctx, w.host)
		if gerr != nil {
			t.Fatal(gerr)
		}
		if h.State != lifecycle.HostActive {
			t.Fatalf("an authorless cordon landed: state = %q", h.State)
		}
	})
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

// fleetWideReads: the two listings a human reads the catalog with, and the whole
// reason they are not the per-host ones with the argument dropped.
//
// Every read the Control Plane serves is scoped to a host, because every read it
// serves answers an Agent. That makes a volume with no primary, and a snapshot of a
// volume with no primary, invisible to the entire store: NULL matches no host id, and
// ListPendingSnapshots reaches the snapshot *through* volumes.primary_host_id. Those
// are exactly the rows an operator is looking for — rebuild-metadata restores every
// volume unplaced, and a snapshot stops making progress precisely when the volume it
// belongs to stops being served — so the case detaches the fixture volume first and
// then asks both questions.
func fleetWideReads(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s) // w.snap is CREATING on w.vol, whose primary is w.host

	// A volume placed nowhere, the shape rebuild-metadata restores.
	unplaced := id()
	if err := s.CreateVolume(ctx, w.term, metadata.Volume{DEKKeyID: 1,
		VolumeID: unplaced, SizeBytes: 1 << 30, BlockSize: 65536,
		State: lifecycle.VolumeDetached, DEKWrapped: []byte{1}, KEKID: "kek",
	}, nil); err != nil {
		t.Fatalf("CreateVolume with no primary: %v", err)
	}

	volumeIDs := func(t *testing.T, vols []metadata.Volume) []string {
		t.Helper()
		out := make([]string, 0, len(vols))
		for _, v := range vols {
			out = append(out, v.VolumeID)
		}
		return out
	}
	want := []string{w.vol, unplaced}
	sort.Strings(want) // both listings are ordered by volume id (INV-02)

	all, err := s.ListVolumes(ctx)
	if err != nil {
		t.Fatalf("ListVolumes: %v", err)
	}
	if got := volumeIDs(t, all); !reflect.DeepEqual(got, want) {
		t.Fatalf("ListVolumes = %v, want %v", got, want)
	}
	// The contrast, stated rather than assumed: the read the fleet actually runs on
	// cannot produce the unplaced volume, from any host.
	placed, err := s.ListVolumesByHost(ctx, w.host)
	if err != nil {
		t.Fatalf("ListVolumesByHost: %v", err)
	}
	if got := volumeIDs(t, placed); !reflect.DeepEqual(got, []string{w.vol}) {
		t.Fatalf("ListVolumesByHost = %v, want just the placed volume %s", got, w.vol)
	}
	for _, v := range all {
		if v.VolumeID == unplaced && v.PrimaryHostID != "" {
			t.Fatalf("the unplaced volume came back placed on %q", v.PrimaryHostID)
		}
	}

	// Detach the fixture volume: its CREATING snapshot now belongs to no host, which
	// is the state in which nothing will ever finish it.
	if err := s.SetVolumePrimaryHost(ctx, w.term, w.vol, ""); err != nil {
		t.Fatalf("detach: %v", err)
	}
	if pending, err := s.ListPendingSnapshots(ctx, w.host); err != nil || len(pending) != 0 {
		t.Fatalf("the per-host read still sees the snapshot of a detached volume: %+v (err %v)", pending, err)
	}
	snapshotIDs := func(t *testing.T, snaps []metadata.Snapshot) []string {
		t.Helper()
		out := make([]string, 0, len(snaps))
		for _, snap := range snaps {
			out = append(out, snap.SnapshotID)
		}
		return out
	}
	unfinished, err := s.ListUnfinishedSnapshots(ctx)
	if err != nil {
		t.Fatalf("ListUnfinishedSnapshots: %v", err)
	}
	if got := snapshotIDs(t, unfinished); !reflect.DeepEqual(got, []string{w.snap}) {
		t.Fatalf("unfinished = %v, want the stranded CREATING snapshot %s", got, w.snap)
	}

	// A published snapshot is finished and drops out; a DELETING one does not, because
	// under ADR-0026 nothing reclaims it and it stays there for ever.
	if err := setSnapshotState(ctx, s, w.term, w.snap, lifecycle.SnapshotPublished); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got, err := s.ListUnfinishedSnapshots(ctx); err != nil || len(got) != 0 {
		t.Fatalf("a PUBLISHED snapshot is still outstanding: %v (err %v)", snapshotIDs(t, got), err)
	}
	if err := setSnapshotState(ctx, s, w.term, w.snap, lifecycle.SnapshotDeleting); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got, err := s.ListUnfinishedSnapshots(ctx); err != nil ||
		!reflect.DeepEqual(snapshotIDs(t, got), []string{w.snap}) {
		t.Fatalf("a DELETING snapshot nothing reclaims = %v (err %v), want %s", snapshotIDs(t, got), err, w.snap)
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

// hostLeases: the lease is what a host renews to say it is still there (§12.6). It
// stopped being the Agent's authority to ACK a FLUSH when ADR-0026 withdrew the
// durable ACK gate, so what is left to prove here is one rule and its observation.
//
// The rule: an assertion the Control Plane made about a host outranks the host's own
// heartbeat. A host it has declared DEAD is a host it has said is gone — the same
// assertion a promotion would accept as "the old writer is finished" — so a routine
// renewal must not be able to put it back. CORDONED and DRAINING are deliberately
// the other way: both are still serving the volumes they hold.
//
// The observation: GetHostLease. Every claim below is checked by reading the lease
// back out of the store rather than by trusting what RenewHostLease returned, which
// is the only way this contract can tell a store that answers correctly from one
// that also writes correctly.
// leadershipRenewal: a leader can say "I am still here" without becoming a new leader.
//
// The two halves are one property. A renewal that moved the term would be
// AcquireLeadership with extra steps, and every admin one-shot in cmd/control-plane
// reads GetLeader and then writes under that term — a leader renewing every few seconds
// would make those writes fail at random. A renewal that did not refresh the stamp would
// leave the only durable evidence that this process is alive frozen at its election, and
// a Control Plane dead for five minutes indistinguishable from one that started five
// minutes ago.
//
// The wrong-holder case is not hypothetical: it is what the *superseded* process's next
// renewal is, and ErrStaleTerm is the answer that ends it. (The stale-*term* case is in
// everyMutation, with the rest of the §7 surface.)
func leadershipRenewal(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)

	before, err := s.GetLeader(ctx)
	if err != nil {
		t.Fatalf("GetLeader: %v", err)
	}
	if err := s.RenewLeadership(ctx, w.term, "cp-a"); err != nil {
		t.Fatalf("RenewLeadership by the current leader: %v", err)
	}
	after, err := s.GetLeader(ctx)
	if err != nil {
		t.Fatalf("GetLeader: %v", err)
	}
	if after.Term != before.Term || after.HolderID != before.HolderID {
		t.Fatalf("a renewal moved leadership from %s/term %d to %s/term %d",
			before.HolderID, before.Term, after.HolderID, after.Term)
	}
	// Not-before rather than after: the sim runs on a clock a DST scenario advances by
	// hand, and a fixture that never advances it would fail an assertion of strict
	// progress for a store that is behaving.
	if after.RenewedAt.Before(before.RenewedAt) {
		t.Fatalf("the renewal moved renewed_at backwards: %s -> %s", before.RenewedAt, after.RenewedAt)
	}
	// The term the renewal was made under is still usable: this is what the one-shots
	// depend on, and asserting it here is what stops a "renewal" that silently re-elects.
	if err := s.UpsertHost(ctx, w.term, metadata.Host{HostID: w.host, State: lifecycle.HostActive}); err != nil {
		t.Fatalf("a write under the renewed term: %v", err)
	}

	// The holder is checked, not only the term. Renewing under someone else's identity
	// is a process that has lost the election and does not know it.
	if err := s.RenewLeadership(ctx, w.term, "cp-imposter"); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("renewing under another holder's term: want ErrStaleTerm, got %v", err)
	}

	// And after a real takeover the old holder's renewal is refused, which is the whole
	// point: it is how a superseded Control Plane finds out, on a write it makes anyway,
	// instead of on the next mutation an Agent happens to ask it for.
	newTerm, err := s.AcquireLeadership(ctx, "cp-b")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RenewLeadership(ctx, w.term, "cp-a"); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("the superseded leader's renewal: want ErrStaleTerm, got %v", err)
	}
	if err := s.RenewLeadership(ctx, newTerm, "cp-b"); err != nil {
		t.Fatalf("the new leader's renewal: %v", err)
	}

	// The restart, which is the takeover a pilot actually performs: the *same* holder id
	// elected again, one term higher, while the process holding the old term is still
	// running. Only the term tells the two apart — the holder matches — so this is the
	// one case where a renewal that guarded on identity alone would keep a zombie alive
	// through every restart of the Control Plane.
	restarted, err := s.AcquireLeadership(ctx, "cp-b")
	if err != nil {
		t.Fatal(err)
	}
	if restarted == newTerm {
		t.Fatalf("re-electing the same holder did not move the term (%d): an election is not a renewal", restarted)
	}
	if err := s.RenewLeadership(ctx, newTerm, "cp-b"); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("the previous incarnation of the same holder: want ErrStaleTerm, got %v", err)
	}
}

func hostLeases(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)

	// A registered host that has never renewed holds no lease. Asserted here rather
	// than after a revocation, because nothing revokes any more — Block/Unblock/
	// RevokeHostLease went with the promotion that was their only
	// caller — and without this case the "no lease" answer would be a branch no test
	// reaches, which is how a store that invents a zero-valued lease would pass.
	if _, err := s.GetHostLease(ctx, w.host); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("a host that has never renewed: want ErrNotFound, got %v", err)
	}

	t.Run("renewing grants a lease with the TTL it was asked for", func(t *testing.T) {
		if err := s.RenewHostLease(ctx, w.term, w.host, 10); err != nil {
			t.Fatal(err)
		}
		l, err := s.GetHostLease(ctx, w.host)
		if err != nil {
			t.Fatalf("after renewing, the host holds no lease: %v", err)
		}
		if l.HostID != w.host || l.TTLSeconds != 10 {
			t.Fatalf("lease = %+v, want host %s with a 10s TTL", l, w.host)
		}
		if l.LastRenewal.IsZero() || l.GrantedAt.IsZero() {
			t.Fatalf("lease = %+v, want both instants stamped by the store's clock", l)
		}
	})

	t.Run("a dead host cannot renew", func(t *testing.T) {
		if err := s.SetHostState(ctx, w.term, w.host, lifecycle.HostDead, lifecycle.CordonOperator); err != nil {
			t.Fatal(err)
		}
		if err := s.RenewHostLease(ctx, w.term, w.host, 10); !errors.Is(err, metadata.ErrHostNotServing) {
			t.Fatalf("renewing the lease of a DEAD host: want ErrHostNotServing, got %v", err)
		}
	})

	t.Run("a cordoned or draining host still renews", func(t *testing.T) {
		// A cordon stops new placement and a drain evacuates, but both hosts are
		// still serving the volumes they hold: refusing their renewals would report
		// them dead to every reader of the fleet while they are working normally.
		for i, state := range []lifecycle.HostState{lifecycle.HostCordoned, lifecycle.HostDraining} {
			if err := s.SetHostState(ctx, w.term, w.host, state, lifecycle.CordonOperator); err != nil {
				t.Fatal(err)
			}
			before, err := s.GetHostLease(ctx, w.host)
			if err != nil {
				t.Fatal(err)
			}
			// A TTL no earlier iteration used, so "the renewal landed" is a claim
			// about this write and not about whatever the last one left behind.
			ttl := int32(20 + 10*i)
			if err := s.RenewHostLease(ctx, w.term, w.host, int(ttl)); err != nil {
				t.Fatalf("renewing the lease of a %s host: %v", state, err)
			}
			after, err := s.GetHostLease(ctx, w.host)
			if err != nil {
				t.Fatal(err)
			}
			// Asserting on the stored TTL and not only on the error: a store that
			// returned nil and wrote nothing would satisfy the line above, which is
			// the failure shape CLAUDE.md names.
			if after.TTLSeconds != ttl || before.TTLSeconds == after.TTLSeconds {
				t.Fatalf("%s host: lease TTL %d -> %d, want the renewal to have landed as %d",
					state, before.TTLSeconds, after.TTLSeconds, ttl)
			}
		}
	})
}

// capacity is ADR-0017: committed capacity is derived from the rows that already
// say who holds what, and the §28.2 oversubscription bound is a predicate of the
// writes that place bytes on a host.
//
//	committed(host) = Σ size_bytes of the volumes whose primary is host
//
// (ADR-0017's second term, what an in-flight operation plan had reserved on the host
// but not yet placed, went with the operations table it was read from.)
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

	t.Run("the fill ceiling is a predicate of that write too", func(t *testing.T) {
		// ADR-0013's second gap: the §28.2 arm above bounds what a host has been
		// *promised*, and this host has been promised nothing at all — while its own
		// report says 900 of its 1024 GiB are gone. Under ADR-0026 that is the
		// ordinary case rather than a corner: a session's WAL stays on the device
		// until the volume stops, and no reservation covers a byte of it. The used
		// bytes of other tenants of that filesystem count too, deliberately: no
		// truncation of ours frees them.
		full := id()
		if err := s.UpsertHost(ctx, w.term, metadata.Host{
			HostID: full, State: lifecycle.HostActive,
			NVMeTotalBytes: total, NVMeUsedBytes: 900 * gib,
		}); err != nil {
			t.Fatal(err)
		}
		vol := metadata.Volume{DEKKeyID: 1,
			VolumeID: id(), SizeBytes: gib, BlockSize: 65536, State: lifecycle.VolumeActive,
			PrimaryHostID: full, DEKWrapped: []byte{1}, KEKID: "k",
		}
		// Room enough for a thousand of these under the promise ceiling, and no room
		// on the device: the two arms have to be asked separately.
		err := s.CreateVolume(ctx, w.term, vol, &metadata.CapacityBound{
			HostID: full, AddBytes: gib, Limit: total, UsedLimit: 850 * gib,
		})
		if !errors.Is(err, metadata.ErrCapacityExceeded) {
			t.Fatalf("CreateVolume onto a device at 900/1024 GiB = %v, want ErrCapacityExceeded", err)
		}
		if _, gerr := s.GetVolume(ctx, vol.VolumeID); !errors.Is(gerr, metadata.ErrNotFound) {
			t.Fatalf("a placement refused by the fill ceiling wrote the volume anyway: %v", gerr)
		}
		// A bound that names no fill ceiling refuses everything a real device could
		// report. That is the fail-closed direction and it is load-bearing: bounds
		// are built by placement.Policy.Bound, and a hand-built one that forgot this
		// number must not silently place unbounded.
		if err := s.CreateVolume(ctx, w.term, vol, &metadata.CapacityBound{
			HostID: full, AddBytes: gib, Limit: total,
		}); !errors.Is(err, metadata.ErrCapacityExceeded) {
			t.Fatalf("CreateVolume under a bound with no fill ceiling = %v, want ErrCapacityExceeded", err)
		}
		// Raise the ceiling above what the host measures and the same write lands:
		// what changed is the policy, not the promises.
		if err := s.CreateVolume(ctx, w.term, vol, &metadata.CapacityBound{
			HostID: full, AddBytes: gib, Limit: total, UsedLimit: 950 * gib,
		}); err != nil {
			t.Fatalf("a placement onto a device inside its fill ceiling was refused: %v", err)
		}
		if got := committed(t, full); got != gib {
			t.Fatalf("destination committed = %d, want %d", got, gib)
		}
		// The measured arm reads the host's own report at the instant of the write,
		// not a copy the caller took: the next heartbeat says the device is fuller,
		// and the same bound that admitted a volume a moment ago stops admitting one.
		if err := s.UpsertHost(ctx, w.term, metadata.Host{
			HostID: full, State: lifecycle.HostActive,
			NVMeTotalBytes: total, NVMeUsedBytes: 960 * gib,
		}); err != nil {
			t.Fatal(err)
		}
		if err := s.CreateVolume(ctx, w.term, metadata.Volume{DEKKeyID: 1,
			VolumeID: id(), SizeBytes: gib, BlockSize: 65536, State: lifecycle.VolumeActive,
			PrimaryHostID: full, DEKWrapped: []byte{1}, KEKID: "k",
		}, &metadata.CapacityBound{
			HostID: full, AddBytes: gib, Limit: total, UsedLimit: 950 * gib,
		}); !errors.Is(err, metadata.ErrCapacityExceeded) {
			t.Fatalf("CreateVolume after a heartbeat past the ceiling = %v, want ErrCapacityExceeded", err)
		}
	})

	t.Run("placements racing for the last slot leave the host inside its ceiling", func(t *testing.T) {
		// This is the case the bound exists for, and the only one that can tell a
		// predicate of the write from a check in front of it. Every caller reads the
		// fleet before any of them has placed anything — placement.Choose is pure and
		// advisory, so they agree on the destination — and then they write at the same
		// instant. A check the store performs in Go passes for all of them, because at
		// the moment each one looks, nobody else's volume is there yet.
		//
		// It is not a store-implementation detail either. In PostgreSQL the predicate
		// alone is not enough: READ COMMITTED fixes each statement's snapshot before it
		// runs and the derived capacity is an aggregate over rows the statement does not
		// lock, so two overlapping INSERTs each affect one row and the host lands at
		// twice its ceiling. That is what the pg store's advisory lock is for, and this
		// case is what says so.
		//
		// Several rounds of many racers, because a scheduler is not an oracle: one round
		// can serialize by luck and prove nothing, and the point of the case is that
		// nothing is left to luck. Each round is a fresh host, so a round that fails
		// says which one did.
		for round := range 5 {
			racer := id()
			if err := s.UpsertHost(ctx, w.term, metadata.Host{
				HostID: racer, State: lifecycle.HostActive, NVMeTotalBytes: total,
			}); err != nil {
				t.Fatal(err)
			}
			var (
				wg    sync.WaitGroup
				start = make(chan struct{})
				errs  = make([]error, 32)
			)
			for i := range errs {
				wg.Add(1)
				go func() {
					defer wg.Done()
					// Room for exactly one of them, and every one of them was told so.
					bound := &metadata.CapacityBound{
						HostID: racer, AddBytes: gib, Limit: gib, UsedLimit: total,
					}
					vol := metadata.Volume{DEKKeyID: 1,
						VolumeID: id(), SizeBytes: gib, BlockSize: 65536, State: lifecycle.VolumeActive,
						PrimaryHostID: racer, DEKWrapped: []byte{1}, KEKID: "k",
					}
					<-start
					errs[i] = s.CreateVolume(ctx, w.term, vol, bound)
				}()
			}
			close(start)
			wg.Wait()

			placed := 0
			for _, err := range errs {
				switch {
				case err == nil:
					placed++
				case errors.Is(err, metadata.ErrCapacityExceeded):
				default:
					t.Fatalf("round %d: a racing placement failed for the wrong reason: %v", round, err)
				}
			}
			if placed != 1 {
				t.Fatalf("round %d: %d of %d racing placements landed, want exactly 1", round, placed, len(errs))
			}
			// The observable that matters is not which caller won but what the host
			// ends up holding: a destination past the ceiling it was placed against is
			// the defect, whatever the callers were told.
			if got := committed(t, racer); got != gib {
				t.Fatalf("round %d: the destination holds %d committed bytes against a ceiling of %d",
					round, got, gib)
			}
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
		{"RenewLeadership", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.RenewLeadership(ctx, term, "")
		}},
		{"UpsertHost", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.UpsertHost(ctx, term, metadata.Host{HostID: "", State: lifecycle.HostActive})
		}},
		{"SetHostState", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.SetHostState(ctx, term, "", lifecycle.HostCordoned, lifecycle.CordonOperator)
		}},
		{"RenewHostLease", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.RenewHostLease(ctx, term, "", 10)
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
		{"ClearVolumeParent", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.ClearVolumeParent(ctx, term, "")
		}},
		{"SetVolumeRefusal", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.SetVolumeRefusal(ctx, term, "", w.host, 0, lifecycle.RefusalNone, "")
		}},
		{"DeleteVolume", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.DeleteVolume(ctx, term, "")
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

// volumeDelete is the catalog half of the deletion decision: a volume leaves the
// catalog with its snapshots, and only when nothing still descends from them.
//
// It asserts the refusal by what it lets an operator *do* rather than by reading a
// column back. `ClearVolumeParent` — the write a FLATTEN needs and could not make —
// is proven here by a delete that was refused before it and succeeds after it, which
// is the whole reason that verb exists; asserting `parent_snapshot_id IS NULL` would
// have passed just as well against a verb that cleared the column and left the
// snapshot unreferenceable.
//
// The two descendant directions are separated on purpose, and the second is the one
// an implementation forgets: everything else in this tree talks about
// `volumes.parent_snapshot_id`, and a snapshot that descends from a snapshot — a
// clone that took one of its own — leaves a row only the other query sees.
func volumeDelete(t *testing.T, s metadata.Store) {
	ctx := t.Context()
	w := newWorld(t, s)

	// Two clones of the fixture's snapshot. cloneB also took a snapshot of its own,
	// which is what puts a row in the second direction.
	cloneA, cloneB, snapB := id(), id(), id()
	for _, c := range []string{cloneA, cloneB} {
		if err := s.CreateVolume(ctx, w.term, metadata.Volume{DEKKeyID: 1,
			VolumeID: c, SizeBytes: 1 << 30, BlockSize: 65536, State: lifecycle.VolumeActive,
			DEKWrapped: []byte{1}, KEKID: "kek", ChainDepth: 1, ParentSnapshotID: w.snap,
		}, nil); err != nil {
			t.Fatalf("CreateVolume(%s): %v", c, err)
		}
	}
	if err := s.CreateSnapshot(ctx, w.term, metadata.Snapshot{
		SnapshotID: snapB, VolumeID: cloneB, ParentSnapshotID: w.snap, Epoch: 1,
		TargetSequence: 20, RootDigest: "d", State: lifecycle.SnapshotCreating, RequestID: id(),
	}); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	// intact is the assertion that a refusal cost nothing: both rows still readable.
	intact := func(t *testing.T, why string) {
		t.Helper()
		if _, err := s.GetVolume(ctx, w.vol); err != nil {
			t.Fatalf("%s: the refused volume is gone: %v", why, err)
		}
		if _, err := s.GetSnapshot(ctx, w.snap); err != nil {
			t.Fatalf("%s: the refused volume's snapshot is gone: %v", why, err)
		}
	}

	t.Run("a volume another volume descends from is refused", func(t *testing.T) {
		if err := s.DeleteVolume(ctx, w.term, w.vol); !errors.Is(err, metadata.ErrHasDescendants) {
			t.Fatalf("delete with two clones: want ErrHasDescendants, got %v", err)
		}
		intact(t, "two clones descend from it")
	})

	t.Run("clearing one clone's link is not enough while the other holds it", func(t *testing.T) {
		if err := s.ClearVolumeParent(ctx, w.term, cloneA); err != nil {
			t.Fatalf("ClearVolumeParent(%s): %v", cloneA, err)
		}
		if err := s.DeleteVolume(ctx, w.term, w.vol); !errors.Is(err, metadata.ErrHasDescendants) {
			t.Fatalf("delete with one clone left: want ErrHasDescendants, got %v", err)
		}
		intact(t, "one clone still descends from it")
	})

	t.Run("a snapshot that descends from it refuses the delete too", func(t *testing.T) {
		if err := s.ClearVolumeParent(ctx, w.term, cloneB); err != nil {
			t.Fatalf("ClearVolumeParent(%s): %v", cloneB, err)
		}
		// No volume names the snapshot any more; snapB does.
		if err := s.DeleteVolume(ctx, w.term, w.vol); !errors.Is(err, metadata.ErrHasDescendants) {
			t.Fatalf("delete with only a descending snapshot left: want ErrHasDescendants, got %v", err)
		}
		intact(t, "a snapshot of a clone still descends from it")
	})

	t.Run("a flattened clone is still a volume, at depth zero", func(t *testing.T) {
		v, err := s.GetVolume(ctx, cloneA)
		if err != nil {
			t.Fatalf("the flattened clone is gone: %v", err)
		}
		// The depth is not decoration: §20.1's ceiling refuses a clone on it, so a
		// flatten that left it at 1 would have bought the copy and not the clonability.
		if v.ChainDepth != 0 || v.ParentSnapshotID != "" {
			t.Fatalf("after ClearVolumeParent the clone is still at depth %d under %q",
				v.ChainDepth, v.ParentSnapshotID)
		}
		// Idempotent: a delete flattens several descendants in one pass and an
		// operator re-runs the command it is not sure completed.
		if err := s.ClearVolumeParent(ctx, w.term, cloneA); err != nil {
			t.Fatalf("clearing twice: %v", err)
		}
	})

	t.Run("deleting the last descendant takes its snapshots with it", func(t *testing.T) {
		if err := s.DeleteVolume(ctx, w.term, cloneB); err != nil {
			t.Fatalf("DeleteVolume(%s): %v", cloneB, err)
		}
		if _, err := s.GetSnapshot(ctx, snapB); !errors.Is(err, metadata.ErrNotFound) {
			t.Fatalf("the deleted volume's snapshot is still in the catalog: %v", err)
		}
	})

	t.Run("with nothing descending, the volume and its snapshot go", func(t *testing.T) {
		if err := s.DeleteVolume(ctx, w.term, w.vol); err != nil {
			t.Fatalf("DeleteVolume(%s): %v", w.vol, err)
		}
		if _, err := s.GetVolume(ctx, w.vol); !errors.Is(err, metadata.ErrNotFound) {
			t.Fatalf("the deleted volume is still readable: %v", err)
		}
		if _, err := s.GetSnapshot(ctx, w.snap); !errors.Is(err, metadata.ErrNotFound) {
			t.Fatalf("the deleted volume's snapshot is still readable: %v", err)
		}
		// The fleet-wide read a human uses, and the per-host read an Agent is driven
		// by: a row that only GetVolume has stopped answering for is a row that is
		// still being served.
		vols, err := s.ListVolumes(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range vols {
			if v.VolumeID == w.vol {
				t.Fatal("ListVolumes still returns the deleted volume")
			}
		}
		served, err := s.ListVolumesByHost(ctx, w.host)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range served {
			if v.VolumeID == w.vol {
				t.Fatal("the deleted volume is still in its host's desired state")
			}
		}
	})

	t.Run("running the delete again says it is already done", func(t *testing.T) {
		if err := s.DeleteVolume(ctx, w.term, w.vol); !errors.Is(err, metadata.ErrNotFound) {
			t.Fatalf("second delete: want ErrNotFound, got %v", err)
		}
	})
}
