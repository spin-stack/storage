package sim_test

import (
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/metadata/sim"
)

func newStore() *sim.Store {
	// Fixed clock; deterministic.
	return sim.New(func() time.Time { return time.Unix(1_700_000_000, 0).UTC() })
}

func TestLeadershipTermIncrements(t *testing.T) {
	ctx := t.Context()
	s := newStore()
	t1, err := s.AcquireLeadership(ctx, "cp-a")
	if err != nil || t1 != 1 {
		t.Fatalf("first term = %d err=%v", t1, err)
	}
	t2, _ := s.AcquireLeadership(ctx, "cp-b")
	if t2 != 2 {
		t.Fatalf("second term = %d, want 2", t2)
	}
	l, _ := s.GetLeader(ctx)
	if l.Term != 2 || l.HolderID != "cp-b" {
		t.Fatalf("leader = %+v", l)
	}
}

// TestZombieCPCannotMutate is the §7 property: a CP holding a stale term makes
// 0-row writes (ErrStaleTerm), so it cannot corrupt state.
func TestZombieCPCannotMutate(t *testing.T) {
	ctx := t.Context()
	s := newStore()

	termA, _ := s.AcquireLeadership(ctx, "cp-a")
	// Set up a volume under cp-a.
	if err := s.CreateVolume(ctx, termA, metadata.Volume{DEKKeyID: 1, VolumeID: "v1", State: lifecycle.VolumeActive, DEKWrapped: []byte{1}, KEKID: "k"}, nil); err != nil {
		t.Fatal(err)
	}

	// cp-b takes over; termA is now stale.
	termB, _ := s.AcquireLeadership(ctx, "cp-b")

	// The zombie cp-a cannot bump the epoch.
	if _, err := s.BumpVolumeEpoch(ctx, termA, "v1", "host-a", 0); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("stale term bump: want ErrStaleTerm, got %v", err)
	}
	// cp-b can.
	epoch, err := s.BumpVolumeEpoch(ctx, termB, "v1", "host-b", 0)
	if err != nil || epoch != 1 {
		t.Fatalf("current bump: epoch=%d err=%v", epoch, err)
	}
	// The volume reflects only cp-b's change.
	v, _ := s.GetVolume(ctx, "v1")
	if v.CurrentEpoch != 1 || v.PrimaryHostID != "host-b" {
		t.Fatalf("volume state wrong: %+v", v)
	}
}

func TestStaleTermRejectedAcrossMutations(t *testing.T) {
	ctx := t.Context()
	tests := []struct {
		name string
		mut  func(s *sim.Store, staleTerm int64) error
	}{
		{"UpsertHost", func(s *sim.Store, term int64) error {
			return s.UpsertHost(ctx, term, metadata.Host{HostID: "h", State: lifecycle.HostActive})
		}},
		{"RenewHostLease", func(s *sim.Store, term int64) error {
			return s.RenewHostLease(ctx, term, "h", 10)
		}},
		{"CreateVolume", func(s *sim.Store, term int64) error {
			return s.CreateVolume(ctx, term, metadata.Volume{DEKKeyID: 1, VolumeID: "v", State: lifecycle.VolumeActive}, nil)
		}},
		{"BumpVolumeEpoch", func(s *sim.Store, term int64) error {
			_, err := s.BumpVolumeEpoch(ctx, term, "v", "h", 0)
			return err
		}},
		{"UpdateWatermarks", func(s *sim.Store, term int64) error {
			return s.UpdateWatermarks(ctx, term, "v", 1, 1, 1)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newStore()
			stale, _ := s.AcquireLeadership(ctx, "cp-a")
			_, _ = s.AcquireLeadership(ctx, "cp-b") // stale is now old
			if err := tc.mut(s, stale); !errors.Is(err, metadata.ErrStaleTerm) {
				t.Fatalf("want ErrStaleTerm, got %v", err)
			}
		})
	}
}

// TestOperationIdempotency is §18: a duplicated admin request records once.
func TestOperationIdempotency(t *testing.T) {
	ctx := t.Context()
	s := newStore()
	term, _ := s.AcquireLeadership(ctx, "cp")
	op := metadata.Operation{OperationID: "req-1", Kind: lifecycle.OpAttach, DesiredState: []byte("{}"), CurrentState: []byte("{}"), Phase: lifecycle.OpPending}

	recorded, err := s.RecordOperation(ctx, term, op)
	if err != nil || !recorded {
		t.Fatalf("first record: recorded=%v err=%v", recorded, err)
	}
	recorded, err = s.RecordOperation(ctx, term, op)
	if err != nil || recorded {
		t.Fatalf("duplicate record should report recorded=false, got %v err=%v", recorded, err)
	}
}

// TestGettersRoundTripAndNotFound exercises every read path: a value is returned
// after it is written, and a missing key yields ErrNotFound.
func TestGettersRoundTripAndNotFound(t *testing.T) {
	ctx := t.Context()
	s := newStore()

	// No leader yet.
	if _, err := s.GetLeader(ctx); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("GetLeader empty: want ErrNotFound, got %v", err)
	}
	// Missing rows.
	if _, err := s.GetHost(ctx, "nope"); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("GetHost missing: %v", err)
	}
	if _, err := s.GetHostLease(ctx, "nope"); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("GetHostLease missing: %v", err)
	}
	if _, err := s.GetVolume(ctx, "nope"); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("GetVolume missing: %v", err)
	}
	if _, err := s.GetOperation(ctx, "nope"); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("GetOperation missing: %v", err)
	}

	term, _ := s.AcquireLeadership(ctx, "cp")

	if err := s.UpsertHost(ctx, term, metadata.Host{HostID: "h1", State: lifecycle.HostActive, AgentVersion: "v1"}); err != nil {
		t.Fatal(err)
	}
	h, err := s.GetHost(ctx, "h1")
	if err != nil || h.State != lifecycle.HostActive || h.AgentVersion != "v1" {
		t.Fatalf("GetHost: %+v err=%v", h, err)
	}

	if err := s.CreateVolume(ctx, term, metadata.Volume{DEKKeyID: 1, VolumeID: "v1", State: lifecycle.VolumeActive}, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateWatermarks(ctx, term, "v1", 10, 5, 3); err != nil {
		t.Fatal(err)
	}
	v, err := s.GetVolume(ctx, "v1")
	if err != nil || v.LocalSequence != 10 || v.DurableSequence != 5 || v.PublishedSequence != 3 {
		t.Fatalf("GetVolume after watermarks: %+v err=%v", v, err)
	}
	// UpdateWatermarks on a missing volume is ErrNotFound.
	if err := s.UpdateWatermarks(ctx, term, "absent", 1, 1, 1); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("UpdateWatermarks missing: %v", err)
	}
	// BumpVolumeEpoch on a missing volume is ErrNotFound.
	if _, err := s.BumpVolumeEpoch(ctx, term, "absent", "h1", 0); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("BumpVolumeEpoch missing: %v", err)
	}

	op := metadata.Operation{OperationID: "op1", Kind: lifecycle.OpAttach, DesiredState: []byte("{}"), CurrentState: []byte("{}"), Phase: lifecycle.OpPending}
	if _, err := s.RecordOperation(ctx, term, op); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetOperation(ctx, "op1")
	if err != nil || got.Kind != lifecycle.OpAttach {
		t.Fatalf("GetOperation: %+v err=%v", got, err)
	}
}

// TestStoreRejectsValuesOutsideTheVocabulary: the store is the authority for what a
// state *is*; a value from outside the lifecycle vocabulary never reaches a row.
func TestStoreRejectsValuesOutsideTheVocabulary(t *testing.T) {
	ctx := t.Context()
	tests := []struct {
		name string
		mut  func(s *sim.Store, term int64) error
	}{
		{"UpsertHost with an unknown state", func(s *sim.Store, term int64) error {
			return s.UpsertHost(ctx, term, metadata.Host{HostID: "h", State: lifecycle.HostState("NOPE")})
		}},
		{"UpsertHost with the zero state", func(s *sim.Store, term int64) error {
			return s.UpsertHost(ctx, term, metadata.Host{HostID: "h"})
		}},
		{"SetHostState with a state from another vocabulary", func(s *sim.Store, term int64) error {
			if err := s.UpsertHost(ctx, term, metadata.Host{HostID: "h", State: lifecycle.HostActive}); err != nil {
				return err
			}
			return s.SetHostState(ctx, term, "h", lifecycle.HostState("PUBLISHED"))
		}},
		{"CreateVolume with the zero state", func(s *sim.Store, term int64) error {
			return s.CreateVolume(ctx, term, metadata.Volume{DEKKeyID: 1, VolumeID: "v"}, nil)
		}},
		{"CreateSnapshot with an unknown state", func(s *sim.Store, term int64) error {
			return s.CreateSnapshot(ctx, term, metadata.Snapshot{SnapshotID: "s", State: lifecycle.SnapshotState("DONE")})
		}},
		{"RecordOperation with an unknown kind", func(s *sim.Store, term int64) error {
			_, err := s.RecordOperation(ctx, term, metadata.Operation{
				OperationID: "op", Kind: lifecycle.OperationKind("teleport"), Phase: lifecycle.OpPending,
			})
			return err
		}},
		{"RecordOperation with an unknown phase", func(s *sim.Store, term int64) error {
			_, err := s.RecordOperation(ctx, term, metadata.Operation{
				OperationID: "op", Kind: lifecycle.OpDrain, Phase: lifecycle.OperationPhase("STARTED"),
			})
			return err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newStore()
			term, _ := s.AcquireLeadership(ctx, "cp")
			if err := tc.mut(s, term); !errors.Is(err, lifecycle.ErrUnknownState) {
				t.Fatalf("want ErrUnknownState, got %v", err)
			}
		})
	}
}

// TestUpdateOperationEnforcesThePhaseLifecycle: a finished operation cannot be
// resurrected, and cancellation cannot rewrite a terminal outcome (§7).
func TestUpdateOperationEnforcesThePhaseLifecycle(t *testing.T) {
	ctx := t.Context()
	s := newStore()
	term, _ := s.AcquireLeadership(ctx, "cp")
	op := metadata.Operation{
		OperationID: "op-1", Kind: lifecycle.OpDrain, Phase: lifecycle.OpPending,
		DesiredState: []byte("{}"), CurrentState: []byte("{}"),
	}
	if _, err := s.RecordOperation(ctx, term, op); err != nil {
		t.Fatal(err)
	}

	op.Phase = lifecycle.OpRunning
	if err := s.UpdateOperation(ctx, term, op, nil); err != nil {
		t.Fatalf("PENDING -> RUNNING: %v", err)
	}
	op.Phase = lifecycle.OpSucceeded
	if err := s.UpdateOperation(ctx, term, op, nil); err != nil {
		t.Fatalf("RUNNING -> SUCCEEDED: %v", err)
	}

	op.Phase = lifecycle.OpRunning
	if err := s.UpdateOperation(ctx, term, op, nil); !errors.Is(err, lifecycle.ErrInvalidTransition) {
		t.Fatalf("SUCCEEDED -> RUNNING: want ErrInvalidTransition, got %v", err)
	}
	op.Phase = lifecycle.OpCanceling
	if err := s.UpdateOperation(ctx, term, op, nil); !errors.Is(err, lifecycle.ErrInvalidTransition) {
		t.Fatalf("SUCCEEDED -> CANCELING: want ErrInvalidTransition, got %v", err)
	}
	got, _ := s.GetOperation(ctx, "op-1")
	if got.Phase != lifecycle.OpSucceeded {
		t.Fatalf("a refused transition changed the phase to %q", got.Phase)
	}
}

// TestListHostsIsSortedAndComplete: the fleet surface used by placement must be
// deterministic (INV-02), so ListHosts returns every host ordered by id.
func TestListHostsIsSortedAndComplete(t *testing.T) {
	ctx := t.Context()
	s := newStore()
	term, _ := s.AcquireLeadership(ctx, "cp")

	for _, id := range []string{"h-c", "h-a", "h-b"} {
		if err := s.UpsertHost(ctx, term, metadata.Host{HostID: id, State: lifecycle.HostActive}); err != nil {
			t.Fatal(err)
		}
	}
	hosts, err := s.ListHosts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, h := range hosts {
		got = append(got, h.HostID)
	}
	if len(got) != 3 || got[0] != "h-a" || got[1] != "h-b" || got[2] != "h-c" {
		t.Fatalf("ListHosts = %v, want sorted [h-a h-b h-c]", got)
	}
}

// TestSetHostState is cordon/drain (§28.1): a term-guarded state transition.
func TestSetHostState(t *testing.T) {
	ctx := t.Context()
	s := newStore()
	term, _ := s.AcquireLeadership(ctx, "cp")
	if err := s.UpsertHost(ctx, term, metadata.Host{HostID: "h1", State: lifecycle.HostActive}); err != nil {
		t.Fatal(err)
	}

	if err := s.SetHostState(ctx, term, "h1", lifecycle.HostCordoned); err != nil {
		t.Fatal(err)
	}
	h, _ := s.GetHost(ctx, "h1")
	if h.State != lifecycle.HostCordoned {
		t.Fatalf("state = %q, want CORDONED", h.State)
	}

	// A zombie CP cannot cordon or uncordon (§7).
	stale := term
	_, _ = s.AcquireLeadership(ctx, "cp-b")
	if err := s.SetHostState(ctx, stale, "h1", lifecycle.HostActive); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("stale SetHostState: want ErrStaleTerm, got %v", err)
	}
	if h, _ := s.GetHost(ctx, "h1"); h.State != lifecycle.HostCordoned {
		t.Fatalf("zombie CP changed the state to %q", h.State)
	}
}

// TestDerivedCapacity is the §28.2 accounting after ADR-0017: the sim's committed
// bytes are a function of the volumes and in-flight plans that name the host, so
// there is nothing to reserve, nothing to release, and nothing that can be applied
// twice. What is left to check here is that the sim sums the same two terms the
// host_committed_bytes view sums in SQL (the shared contract pins the semantics for
// both stores; this is the sim's own arithmetic).
func TestDerivedCapacity(t *testing.T) {
	ctx := t.Context()
	s := newStore()
	term, _ := s.AcquireLeadership(ctx, "cp")
	for _, h := range []string{"h1", "h2"} {
		if err := s.UpsertHost(ctx, term, metadata.Host{HostID: h, State: lifecycle.HostActive, NVMeTotalBytes: 1000}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.CreateVolume(ctx, term, metadata.Volume{DEKKeyID: 1,
		VolumeID: "v1", SizeBytes: 400, State: lifecycle.VolumeActive, PrimaryHostID: "h1",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if h, _ := s.GetHost(ctx, "h1"); h.NVMeCommittedBytes != 400 {
		t.Fatalf("committed = %d, want 400 (the volume it holds)", h.NVMeCommittedBytes)
	}
	if h, _ := s.GetHost(ctx, "h2"); h.NVMeCommittedBytes != 0 {
		t.Fatalf("an empty host committed %d", h.NVMeCommittedBytes)
	}

	// A plan in flight charges its destination before the volume is primary there.
	if _, err := s.RecordOperation(ctx, term, metadata.Operation{
		OperationID: "op1", Kind: lifecycle.OpDrain, HostID: "h1", Phase: lifecycle.OpRunning,
		DesiredState: []byte(`{}`),
		CurrentState: []byte(`{"volumes":[{"volume_id":"v1","stage":"MOVING","to_host":"h2"}]}`),
	}); err != nil {
		t.Fatal(err)
	}
	if h, _ := s.GetHost(ctx, "h2"); h.NVMeCommittedBytes != 400 {
		t.Fatalf("destination committed = %d, want 400 (in flight to it)", h.NVMeCommittedBytes)
	}

	// Once it is primary there the plan stops charging: exactly one, never two.
	if _, err := s.BumpVolumeEpoch(ctx, term, "v1", "h2", 0); err != nil {
		t.Fatal(err)
	}
	if h, _ := s.GetHost(ctx, "h2"); h.NVMeCommittedBytes != 400 {
		t.Fatalf("destination committed = %d, want 400 — charged twice", h.NVMeCommittedBytes)
	}
	if h, _ := s.GetHost(ctx, "h1"); h.NVMeCommittedBytes != 0 {
		t.Fatalf("source committed = %d, want 0 — the move itself is the release", h.NVMeCommittedBytes)
	}

	// The §28.2 bound is a predicate of the write that places a volume.
	err := s.CreateVolume(ctx, term, metadata.Volume{DEKKeyID: 1,
		VolumeID: "v2", SizeBytes: 700, State: lifecycle.VolumeActive, PrimaryHostID: "h2",
	}, &metadata.CapacityBound{HostID: "h2", AddBytes: 700, Limit: 1000})
	if !errors.Is(err, metadata.ErrCapacityExceeded) {
		t.Fatalf("past the bound: want ErrCapacityExceeded, got %v", err)
	}
	if _, gerr := s.GetVolume(ctx, "v2"); !errors.Is(gerr, metadata.ErrNotFound) {
		t.Fatalf("a refused placement wrote the volume: %v", gerr)
	}
	if err := s.CreateVolume(ctx, term, metadata.Volume{DEKKeyID: 1,
		VolumeID: "v2", SizeBytes: 600, State: lifecycle.VolumeActive, PrimaryHostID: "h2",
	}, &metadata.CapacityBound{HostID: "h2", AddBytes: 600, Limit: 1000}); err != nil {
		t.Fatalf("exactly at the bound was refused: %v", err)
	}

	// A bound naming a host nobody registered is ErrNotFound, not a silent pass.
	if err := s.CreateVolume(ctx, term, metadata.Volume{DEKKeyID: 1,
		VolumeID: "v3", SizeBytes: 1, State: lifecycle.VolumeActive,
	}, &metadata.CapacityBound{HostID: "absent", AddBytes: 1, Limit: 1000}); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("bound on a missing host: want ErrNotFound, got %v", err)
	}
	if err := s.SetHostState(ctx, term, "absent", lifecycle.HostCordoned); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("missing host SetHostState: want ErrNotFound, got %v", err)
	}
}

// TestListVolumesByHost is what drain iterates over (§28.1): exactly the volumes
// whose primary is that host, in a deterministic order.
func TestListVolumesByHost(t *testing.T) {
	ctx := t.Context()
	s := newStore()
	term, _ := s.AcquireLeadership(ctx, "cp")

	vols := []metadata.Volume{
		{VolumeID: "v-b", DEKKeyID: 1, State: lifecycle.VolumeActive, PrimaryHostID: "h1"},
		{VolumeID: "v-a", DEKKeyID: 1, State: lifecycle.VolumeActive, PrimaryHostID: "h1"},
		{VolumeID: "v-c", DEKKeyID: 1, State: lifecycle.VolumeActive, PrimaryHostID: "h2"},
		{VolumeID: "v-d", DEKKeyID: 1, State: lifecycle.VolumeActive}, // unattached
	}
	for _, v := range vols {
		if err := s.CreateVolume(ctx, term, v, nil); err != nil {
			t.Fatal(err)
		}
	}

	got, err := s.ListVolumesByHost(ctx, "h1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].VolumeID != "v-a" || got[1].VolumeID != "v-b" {
		t.Fatalf("ListVolumesByHost(h1) = %+v, want sorted [v-a v-b]", got)
	}
	if got, _ := s.ListVolumesByHost(ctx, "h3"); len(got) != 0 {
		t.Fatalf("ListVolumesByHost(h3) = %+v, want empty", got)
	}
}

// TestHostLeaseRenewal: a lease is a fencing token (§12.6), so it is granted only
// to a host that is actually registered. This test previously renewed a lease for
// an id that was never upserted and asserted success — the sim invented a lease
// where Postgres raises a foreign-key error, which is exactly the kind of
// divergence the shared contract now forbids.
func TestHostLeaseRenewal(t *testing.T) {
	ctx := t.Context()
	s := newStore()
	term, _ := s.AcquireLeadership(ctx, "cp")

	if err := s.RenewHostLease(ctx, term, "host-1", 10); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("lease for an unregistered host: want ErrNotFound, got %v", err)
	}
	if err := s.UpsertHost(ctx, term, metadata.Host{HostID: "host-1", State: lifecycle.HostActive}); err != nil {
		t.Fatal(err)
	}
	if err := s.RenewHostLease(ctx, term, "host-1", 10); err != nil {
		t.Fatal(err)
	}
	l, err := s.GetHostLease(ctx, "host-1")
	if err != nil || l.TTLSeconds != 10 || l.HostID != "host-1" {
		t.Fatalf("lease = %+v err=%v", l, err)
	}
}
