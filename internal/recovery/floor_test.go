package recovery_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// The epoch's floor is what stops a bucket that lost its early objects from reading
// as a healthy prefix. It comes from the recovery-point object, and every failure to
// read that object was being swallowed: any error at all collapsed the floor back to
// sequence 1.
//
// The consequence is not a smaller answer, it is a wrong one. The epoch's objects
// legitimately start above 1, so with the floor at 1 nothing reaches it and the
// contiguous run returns nothing — DurablePrefix reports 0 with a *nil* error, giving
// the caller nothing to retry on. Fed into a drain, that 0 becomes the create-only
// recovery point of the next epoch and buries every ACKed write below the new floor
// permanently.

// rpFaultStore is a backend that cannot serve the recovery-point object intact:
// either the GET fails (§24 throttling, a transient failure) or the body comes back
// damaged. Neither is "there is no boundary".
type rpFaultStore struct {
	objectstore.Store
	err     error // non-nil: fail the GET
	corrupt bool  // true: hand back something that is not a recovery point
}

func (s rpFaultStore) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasSuffix(key, "recovery-point.json") {
		switch {
		case s.err != nil:
			return nil, s.err
		case s.corrupt:
			return []byte(`{"prev_epoch": ` /* truncated mid-object */), nil
		}
	}
	return s.Store.Get(ctx, key)
}

// TestUnreadableBoundaryIsAnErrorNotAFloorOfOne: with the boundary unreadable the
// durable point is unknown. Reporting a number — any number — is the failure.
func TestUnreadableBoundaryIsAnErrorNotAFloorOfOne(t *testing.T) {
	ctx := t.Context()
	vol := vol7()

	// Epoch 2 legitimately starts at 8: the promotion recorded a boundary at 7.
	base := sim.NewObjectStore()
	if err := recovery.WriteRecoveryPoint(ctx, base, vol, 2, 1, 7); err != nil {
		t.Fatal(err)
	}
	k, b := craftObject(t, vol, 2, 8, 9, nil)
	putObject(t, base, k, b)

	// Control: read intact, the epoch is durable to 9.
	if got, err := recovery.DurablePrefix(ctx, base, vol, 2); err != nil || got != 9 {
		t.Fatalf("setup: DurablePrefix = %d err=%v, want 9", got, err)
	}

	tests := []struct {
		name  string
		store objectstore.Store
	}{
		{"the GET is throttled", rpFaultStore{Store: base, err: sim.ErrThrottled}},
		{"the boundary object is damaged", rpFaultStore{Store: base, corrupt: true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := recovery.DurablePrefix(ctx, tc.store, vol, 2)
			if err == nil {
				t.Fatalf("DurablePrefix returned %d with no error: an unreadable boundary "+
					"silently became a floor of 1, and 0 is a number a drain will write down", got)
			}
			if errors.Is(err, objectstore.ErrNotFound) {
				t.Fatal("a store failure must never be reported as a missing boundary")
			}

			if got, err := recovery.DurablePoint(ctx, tc.store, vol, 2); err == nil {
				t.Fatalf("DurablePoint returned %d with no error", got)
			}
		})
	}
}

// TestMissingBoundaryStillMeansTheFirstEpoch is the control the fix must not break:
// a genuinely absent recovery-point object is the ordinary case for a volume's first
// epoch, and it means floor 1 — not an error.
func TestMissingBoundaryStillMeansTheFirstEpoch(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	k, b := craftObject(t, vol, 1, 1, 3, nil)
	putObject(t, store, k, b)

	got, err := recovery.DurablePrefix(ctx, store, vol, 1)
	if err != nil {
		t.Fatalf("a first epoch has no boundary and that is not an error: %v", err)
	}
	if got != 3 {
		t.Fatalf("DurablePrefix = %d, want 3", got)
	}
}
