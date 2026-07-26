package sim_test

import (
	"context"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// InjectThrottle counts *every* operation, so a scenario that wants "the boundary PUT
// fails twice" has to know exactly how many unrelated reads the code under test makes
// first. That coupling is why no seed-driven fault has ever been aimed at a specific
// step of the promotion protocol: the moment the production code adds a HEAD, the
// fault lands somewhere else and the scenario passes for the wrong reason.
func TestInjectThrottleKeyOnlyHitsThatKey(t *testing.T) {
	ctx := context.Background()
	s := sim.NewObjectStore()
	if _, err := s.Put(ctx, "other", []byte("v"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	s.InjectThrottleKey("boundary", 2)

	// Unrelated keys are untouched however many times they are read.
	for range 5 {
		if _, err := s.Get(ctx, "other"); err != nil {
			t.Fatalf("an unrelated key must not be throttled: %v", err)
		}
	}
	// The targeted key fails exactly n times, whatever the operation.
	if _, err := s.Put(ctx, "boundary", []byte("v"), objectstore.PutOptions{}); !errors.Is(err, sim.ErrThrottled) {
		t.Fatalf("attempt 1 on the targeted key: want ErrThrottled, got %v", err)
	}
	if _, err := s.Head(ctx, "boundary"); !errors.Is(err, sim.ErrThrottled) {
		t.Fatalf("attempt 2 on the targeted key: want ErrThrottled, got %v", err)
	}
	if _, err := s.Put(ctx, "boundary", []byte("v"), objectstore.PutOptions{}); err != nil {
		t.Fatalf("attempt 3 must succeed once the budget is spent: %v", err)
	}
	// A throttled PUT must not have persisted anything on the way through.
	if got, err := s.Get(ctx, "boundary"); err != nil || string(got) != "v" {
		t.Fatalf("Get after the retry = %q err=%v", got, err)
	}
}

// A throttled operation is a refusal, not a partial effect: the create-only PUT that
// follows must still see the key as absent.
func TestThrottledPutLeavesNoObject(t *testing.T) {
	ctx := context.Background()
	s := sim.NewObjectStore()
	s.InjectThrottleKey("k", 1)

	if _, err := s.Put(ctx, "k", []byte("first"), objectstore.PutOptions{IfNoneMatch: true}); !errors.Is(err, sim.ErrThrottled) {
		t.Fatalf("want ErrThrottled, got %v", err)
	}
	if _, err := s.Put(ctx, "k", []byte("first"), objectstore.PutOptions{IfNoneMatch: true}); err != nil {
		t.Fatalf("the create-only retry must succeed: %v", err)
	}
}
