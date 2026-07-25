// Package metadatatest is the shared contract every metadata.Store implementation
// must satisfy. It exists because the two implementations (ADR-0006) carry two
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
	"sync"
	"testing"

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
		{"ConcurrentEpochBumpsAreSerialized", concurrentBumps},
		{"EmptyIdentifiersAreRejected", emptyIDs},
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
	ctx := context.Background()
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
	if err := s.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: w.vol, SizeBytes: 1 << 30, BlockSize: 65536, State: lifecycle.VolumeActive,
		PrimaryHostID: w.host, DEKWrapped: []byte{1}, KEKID: "kek",
	}); err != nil {
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
		{"CommitHostCapacity", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.CommitHostCapacity(ctx, term, w.host, 1)
		}},
		{"RenewHostLease", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.RenewHostLease(ctx, term, w.host, 10)
		}},
		{"CreateVolume", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.CreateVolume(ctx, term, metadata.Volume{
				VolumeID: id(), SizeBytes: 1, BlockSize: 65536, State: lifecycle.VolumeActive,
				DEKWrapped: []byte{1}, KEKID: "k",
			})
		}},
		{"BumpVolumeEpoch", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			_, err := s.BumpVolumeEpoch(ctx, term, w.vol, w.host)
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
		{"CreateSnapshot", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return s.CreateSnapshot(ctx, term, metadata.Snapshot{
				SnapshotID: id(), VolumeID: w.vol, Epoch: 1, TargetSequence: 1,
				RootDigest: "d", State: lifecycle.SnapshotCreating, RequestID: id(),
			})
		}},
		{"SetSnapshotState", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			return setSnapshotState(ctx, s, term, w.snap, lifecycle.SnapshotPublished)
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
			})
		}},
	}
}

// --- cases -----------------------------------------------------------------

// termZero: nothing may be written before an election. Term 0 is what a CP that
// never acquired leadership — or that read a zeroed config field — passes; treating
// it as valid lets an unelected process bump epochs and release capacity.
func termZero(t *testing.T, s metadata.Store) {
	ctx := context.Background()
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
	ctx := context.Background()
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
	ctx := context.Background()
	w := newWorld(t, s)
	ghostHost, ghostVol, ghostSnap, ghostOp := id(), id(), id(), id()

	tests := []mutation{
		{"SetHostState", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.SetHostState(ctx, term, ghostHost, lifecycle.HostCordoned)
		}},
		{"CommitHostCapacity", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.CommitHostCapacity(ctx, term, ghostHost, 1)
		}},
		{"RenewHostLease", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.RenewHostLease(ctx, term, ghostHost, 10)
		}},
		{"BumpVolumeEpoch", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			_, err := s.BumpVolumeEpoch(ctx, term, ghostVol, w.host)
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
		{"SetSnapshotState", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return setSnapshotState(ctx, s, term, ghostSnap, lifecycle.SnapshotPublished)
		}},
		{"UpdateOperation", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.UpdateOperation(ctx, term, metadata.Operation{
				OperationID: ghostOp, Phase: lifecycle.OpRunning, CurrentState: []byte(`{}`),
			})
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
	ctx := context.Background()
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
			_, err := s.BumpVolumeEpoch(ctx, stale, ghost, w.host)
			return err
		}},
		{"missing host under a stale term", func() error {
			return s.CommitHostCapacity(ctx, stale, ghost, 1)
		}},
		{"over-release under a stale term", func() error {
			return s.CommitHostCapacity(ctx, stale, w.host, -1)
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
	ctx := context.Background()
	w := newWorld(t, s)
	standby := id()
	if err := s.UpsertHost(ctx, w.term, metadata.Host{HostID: standby, State: lifecycle.HostActive}); err != nil {
		t.Fatal(err)
	}
	want := metadata.Volume{
		VolumeID: id(), SizeBytes: 1 << 33, Durability: lifecycle.DurabilityLocal,
		BlockSize: 65536, CurrentEpoch: 7, State: lifecycle.VolumeDetached,
		PrimaryHostID: w.host, StandbyHostID: standby, ChainDepth: 3,
		DEKWrapped: []byte{9, 8, 7}, KEKID: "kek-7",
		LocalSequence: 900, DurableSequence: 800, PublishedSequence: 700,
	}
	if err := s.CreateVolume(ctx, w.term, want); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetVolume(ctx, want.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	if got.SizeBytes != want.SizeBytes || got.Durability != want.Durability ||
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
	ctx := context.Background()
	w := newWorld(t, s)
	vol := id()
	if err := s.CreateVolume(ctx, w.term, metadata.Volume{
		VolumeID: vol, SizeBytes: 1 << 30, BlockSize: 65536, CurrentEpoch: 5,
		State: lifecycle.VolumeActive, PrimaryHostID: w.host, DEKWrapped: []byte{1}, KEKID: "k",
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateWatermarks(ctx, w.term, vol, 500, 400, 300); err != nil {
		t.Fatal(err)
	}

	// The losing rebuild: a fresh descriptor-shaped row — epoch 2, no owner, DETACHED.
	if err := s.CreateVolume(ctx, w.term, metadata.Volume{
		VolumeID: vol, SizeBytes: 1 << 20, BlockSize: 65536, CurrentEpoch: 2,
		State: lifecycle.VolumeDetached, DEKWrapped: []byte{1}, KEKID: "k",
	}); err != nil {
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
	ctx := context.Background()
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
	ctx := context.Background()
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
}

// upsertHost: a host is cordoned and being drained; its next routine heartbeat
// carries State ACTIVE and whatever committed bytes the agent believes. If the
// heartbeat wins, AcceptsPlacement() starts handing the host new volumes while its
// own are being evacuated, and the drain's later release underflows.
func upsertHost(t *testing.T, s metadata.Store) {
	ctx := context.Background()
	w := newWorld(t, s)
	if err := s.CommitHostCapacity(ctx, w.term, w.host, 700); err != nil {
		t.Fatal(err)
	}
	if err := s.SetHostState(ctx, w.term, w.host, lifecycle.HostCordoned); err != nil {
		t.Fatal(err)
	}

	// A heartbeat-shaped upsert: the agent's own view, which knows nothing about
	// cordoning or about the Control Plane's capacity ledger.
	if err := s.UpsertHost(ctx, w.term, metadata.Host{
		HostID: w.host, State: lifecycle.HostActive, AgentVersion: "v2",
		MaxFormatVersion: 3, NVMeTotalBytes: 1 << 41, NVMeUsedBytes: 123,
		NVMeCommittedBytes: 0,
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
	case h.NVMeCommittedBytes != 700:
		t.Fatalf("a heartbeat rewrote the capacity ledger: committed = %d", h.NVMeCommittedBytes)
	case h.AgentVersion != "v2" || h.MaxFormatVersion != 3 || h.NVMeTotalBytes != 1<<41 || h.NVMeUsedBytes != 123:
		t.Fatalf("a heartbeat did not update the fields it owns: %+v", h)
	}
}

// recordOperation: the one mutation whose whole purpose is admin idempotency
// (INV-21) must let the caller tell "already done" from "you are not the leader".
// An operator cancelling a drain through a CP that lost the election otherwise gets
// success while the real drain keeps promoting volumes.
func recordOperation(t *testing.T, s metadata.Store) {
	ctx := context.Background()
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
	ctx := context.Background()
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
	ctx := context.Background()
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

// concurrentBumps: an epoch is the fencing token. Two promoters racing must not
// both be told they hold the same epoch — one of them would then be recorded as the
// owner of a volume it never fenced, and every later read of primary_host_id (the
// drain's listing, promotion's resume branch, rebuild) would be wrong about it.
func concurrentBumps(t *testing.T, s metadata.Store) {
	ctx := context.Background()
	w := newWorld(t, s)
	const n = 8

	var (
		mu     sync.Mutex
		epochs = map[int64]int{}
		errs   []error
		wg     sync.WaitGroup
	)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e, err := s.BumpVolumeEpoch(ctx, w.term, w.vol, w.host)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			epochs[e]++
		}()
	}
	wg.Wait()
	if len(errs) > 0 {
		t.Fatalf("concurrent bumps errored: %v", errs)
	}
	for e, count := range epochs {
		if count != 1 {
			t.Fatalf("epoch %d was handed to %d promoters", e, count)
		}
	}
	if len(epochs) != n {
		t.Fatalf("%d distinct epochs for %d bumps", len(epochs), n)
	}
	v, _ := s.GetVolume(ctx, w.vol)
	if v.CurrentEpoch != n {
		t.Fatalf("current epoch = %d after %d bumps", v.CurrentEpoch, n)
	}
}

// emptyIDs: an unset config field reaches the store as "". A primary key is never
// meaningfully empty, so it is rejected rather than written — a row keyed on the
// empty string is invisible to every lookup that follows.
func emptyIDs(t *testing.T, s metadata.Store) {
	ctx := context.Background()
	w := newWorld(t, s)

	tests := []mutation{
		{"UpsertHost", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.UpsertHost(ctx, term, metadata.Host{HostID: "", State: lifecycle.HostActive})
		}},
		{"SetHostState", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.SetHostState(ctx, term, "", lifecycle.HostCordoned)
		}},
		{"CommitHostCapacity", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.CommitHostCapacity(ctx, term, "", 1)
		}},
		{"RenewHostLease", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.RenewHostLease(ctx, term, "", 10)
		}},
		{"CreateVolume", func(ctx context.Context, s metadata.Store, term int64, _ world) error {
			return s.CreateVolume(ctx, term, metadata.Volume{
				VolumeID: "", SizeBytes: 1, BlockSize: 65536, State: lifecycle.VolumeActive,
				DEKWrapped: []byte{1}, KEKID: "k",
			})
		}},
		{"BumpVolumeEpoch", func(ctx context.Context, s metadata.Store, term int64, w world) error {
			_, err := s.BumpVolumeEpoch(ctx, term, "", w.host)
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
			})
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
