package epoch_test

import (
	"context"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

const vol = "00000000-0000-7000-8000-000000000001"

func TestInitCurrentAndCreateOnly(t *testing.T) {
	ctx := context.Background()
	s := epoch.NewStore(sim.NewObjectStore())

	etag, err := s.Init(ctx, vol, 3)
	if err != nil || etag == "" {
		t.Fatalf("init: etag=%q err=%v", etag, err)
	}
	ep, gotETag, err := s.Current(ctx, vol)
	if err != nil || ep != 3 || gotETag != etag {
		t.Fatalf("current: ep=%d etag=%q err=%v", ep, gotETag, err)
	}
	// Init is create-only.
	if _, err := s.Init(ctx, vol, 9); !errors.Is(err, objectstore.ErrPreconditionFailed) {
		t.Fatalf("second init should fail create-only, got %v", err)
	}
}

func TestCompareAndAdvance(t *testing.T) {
	ctx := context.Background()
	s := epoch.NewStore(sim.NewObjectStore())
	etag, _ := s.Init(ctx, vol, 1)

	newETag, err := s.CompareAndAdvance(ctx, vol, etag, 2)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	ep, _, _ := s.Current(ctx, vol)
	if ep != 2 {
		t.Fatalf("epoch = %d, want 2", ep)
	}
	// A stale ETag loses the CAS (INV-10 single-writer tiebreak).
	if _, err := s.CompareAndAdvance(ctx, vol, etag, 3); !errors.Is(err, epoch.ErrCASConflict) {
		t.Fatalf("stale CAS should conflict, got %v", err)
	}
	_ = newETag
}

func TestVerify(t *testing.T) {
	ctx := context.Background()
	s := epoch.NewStore(sim.NewObjectStore())
	etag, _ := s.Init(ctx, vol, 5)

	if err := s.Verify(ctx, vol, 5); err != nil {
		t.Fatalf("verify matching epoch: %v", err)
	}
	_, _ = s.CompareAndAdvance(ctx, vol, etag, 6)
	// A writer that still thinks it is epoch 5 has been fenced.
	if err := s.Verify(ctx, vol, 5); !errors.Is(err, epoch.ErrEpochChanged) {
		t.Fatalf("verify stale epoch: want ErrEpochChanged, got %v", err)
	}
}
