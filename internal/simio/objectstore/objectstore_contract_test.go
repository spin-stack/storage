package objectstore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/objectstore/storetest"
	"github.com/spin-stack/storage/internal/simio/real"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// TestContract runs the shared objectstore.Store contract (storetest) against both
// in-process implementations. The S3-backed store runs the same contract against a
// real backend in the integration lane (integration/backend).
func TestContract(t *testing.T) {
	t.Run("sim", func(t *testing.T) {
		storetest.RunContract(t, func(*testing.T) objectstore.Store { return sim.NewObjectStore() })
	})
	t.Run("real-filesystem", func(t *testing.T) {
		storetest.RunContract(t, func(t *testing.T) objectstore.Store {
			rs, err := real.NewObjectStore(t.TempDir())
			if err != nil {
				t.Fatalf("real objectstore: %v", err)
			}
			return rs
		})
	})
}

// --- sim-specific fault injection ---

// TestSimLostResponseIsIdempotentOnRetry is the §14.5 scenario: a PUT persists
// but its response is lost; the idempotent retry with If-None-Match sees the
// object already there (412), HEADs it, and the checksums match => success.
func TestSimLostResponseIsIdempotentOnRetry(t *testing.T) {
	ctx := context.Background()
	s := sim.NewObjectStore()
	key := "wal/v/0/1-1-hash.wal"
	data := []byte("the-batch-bytes")

	s.InjectLostResponse(key)
	_, err := s.Put(ctx, key, data, objectstore.PutOptions{IfNoneMatch: true})
	if !errors.Is(err, sim.ErrLostResponse) {
		t.Fatalf("want ErrLostResponse, got %v", err)
	}
	// The object nonetheless persisted.
	_, err = s.Put(ctx, key, data, objectstore.PutOptions{IfNoneMatch: true})
	if !errors.Is(err, objectstore.ErrPreconditionFailed) {
		t.Fatalf("retry want ErrPreconditionFailed (already present), got %v", err)
	}
	// HEAD + checksum confirm the same content => idempotent success.
	info, err := s.Head(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != int64(len(data)) {
		t.Fatalf("size mismatch after lost response: %d", info.Size)
	}
}

func TestSimThrottle(t *testing.T) {
	ctx := context.Background()
	s := sim.NewObjectStore()
	s.InjectThrottle(2)
	if _, err := s.Put(ctx, "k", []byte("v"), objectstore.PutOptions{}); !errors.Is(err, sim.ErrThrottled) {
		t.Fatalf("op 1 want throttled, got %v", err)
	}
	if _, err := s.Get(ctx, "k"); !errors.Is(err, sim.ErrThrottled) {
		t.Fatalf("op 2 want throttled, got %v", err)
	}
	// Third op recovers.
	if _, err := s.Put(ctx, "k", []byte("v"), objectstore.PutOptions{}); err != nil {
		t.Fatalf("op 3 should recover, got %v", err)
	}
}

func TestSimEventualList(t *testing.T) {
	ctx := context.Background()
	s := sim.NewObjectStore()
	s.SetEventualList(true)
	_, _ = s.Put(ctx, "wal/1", []byte("a"), objectstore.PutOptions{})

	// GET sees it immediately (strong read-after-write).
	if _, err := s.Get(ctx, "wal/1"); err != nil {
		t.Fatalf("GET should be strongly consistent: %v", err)
	}
	// LIST does not, until settled.
	if got, _ := s.List(ctx, "wal/"); len(got) != 0 {
		t.Fatalf("eventual LIST should not yet show the object: %+v", got)
	}
	s.Settle()
	if got, _ := s.List(ctx, "wal/"); len(got) != 1 {
		t.Fatalf("after settle LIST should show the object: %+v", got)
	}
}
