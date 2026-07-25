package sim_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/metadata/sim"
)

func newStore() *sim.Store {
	// Fixed clock; deterministic.
	return sim.New(func() time.Time { return time.Unix(1_700_000_000, 0).UTC() })
}

func TestLeadershipTermIncrements(t *testing.T) {
	ctx := context.Background()
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
	ctx := context.Background()
	s := newStore()

	termA, _ := s.AcquireLeadership(ctx, "cp-a")
	// Set up a volume under cp-a.
	if err := s.CreateVolume(ctx, termA, metadata.Volume{VolumeID: "v1", State: "ACTIVE", DEKWrapped: []byte{1}, KEKID: "k"}); err != nil {
		t.Fatal(err)
	}

	// cp-b takes over; termA is now stale.
	termB, _ := s.AcquireLeadership(ctx, "cp-b")

	// The zombie cp-a cannot bump the epoch.
	if _, err := s.BumpVolumeEpoch(ctx, termA, "v1", "host-a"); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("stale term bump: want ErrStaleTerm, got %v", err)
	}
	// cp-b can.
	epoch, err := s.BumpVolumeEpoch(ctx, termB, "v1", "host-b")
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
	ctx := context.Background()
	s := newStore()
	stale, _ := s.AcquireLeadership(ctx, "cp-a")
	_, _ = s.AcquireLeadership(ctx, "cp-b") // stale is now old

	if err := s.UpsertHost(ctx, stale, metadata.Host{HostID: "h"}); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("UpsertHost stale: %v", err)
	}
	if err := s.RenewHostLease(ctx, stale, "h", 10); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("RenewHostLease stale: %v", err)
	}
	if err := s.CreateVolume(ctx, stale, metadata.Volume{VolumeID: "v"}); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("CreateVolume stale: %v", err)
	}
}

// TestOperationIdempotency is §18: a duplicated admin request records once.
func TestOperationIdempotency(t *testing.T) {
	ctx := context.Background()
	s := newStore()
	op := metadata.Operation{OperationID: "req-1", Kind: "attach", DesiredState: []byte("{}"), CurrentState: []byte("{}"), Phase: "pending"}

	recorded, err := s.RecordOperation(ctx, op)
	if err != nil || !recorded {
		t.Fatalf("first record: recorded=%v err=%v", recorded, err)
	}
	recorded, err = s.RecordOperation(ctx, op)
	if err != nil || recorded {
		t.Fatalf("duplicate record should report recorded=false, got %v err=%v", recorded, err)
	}
}

func TestHostLeaseRenewal(t *testing.T) {
	ctx := context.Background()
	s := newStore()
	term, _ := s.AcquireLeadership(ctx, "cp")
	if err := s.RenewHostLease(ctx, term, "host-1", 10); err != nil {
		t.Fatal(err)
	}
	l, err := s.GetHostLease(ctx, "host-1")
	if err != nil || l.TTLSeconds != 10 || l.HostID != "host-1" {
		t.Fatalf("lease = %+v err=%v", l, err)
	}
}
