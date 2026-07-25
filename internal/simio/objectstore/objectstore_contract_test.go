package objectstore_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/real"
	"github.com/spin-stack/storage/internal/simio/sim"
)

func impls(t *testing.T) map[string]objectstore.Store {
	t.Helper()
	rs, err := real.NewObjectStore(t.TempDir())
	if err != nil {
		t.Fatalf("real objectstore: %v", err)
	}
	return map[string]objectstore.Store{
		"real": rs,
		"sim":  sim.NewObjectStore(),
	}
}

func TestPutGetHead(t *testing.T) {
	ctx := context.Background()
	for name, s := range impls(t) {
		t.Run(name, func(t *testing.T) {
			key := "wal/vol/0/1-1-abcd.wal"
			data := []byte("encrypted-record-bytes")
			res, err := s.Put(ctx, key, data, objectstore.PutOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if res.ETag == "" {
				t.Fatal("expected an ETag")
			}
			got, err := s.Get(ctx, key)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("get mismatch: %q err=%v", got, err)
			}
			info, err := s.Head(ctx, key)
			if err != nil || info.Size != int64(len(data)) || info.ETag != res.ETag {
				t.Fatalf("head mismatch: %+v err=%v", info, err)
			}
		})
	}
}

func TestGetMissingIsNotFound(t *testing.T) {
	ctx := context.Background()
	for name, s := range impls(t) {
		t.Run(name, func(t *testing.T) {
			_, err := s.Get(ctx, "absent")
			if !errors.Is(err, objectstore.ErrNotFound) {
				t.Fatalf("want ErrNotFound, got %v", err)
			}
		})
	}
}

func TestIfNoneMatchIsCreateOnly(t *testing.T) {
	ctx := context.Background()
	for name, s := range impls(t) {
		t.Run(name, func(t *testing.T) {
			key := "k"
			opts := objectstore.PutOptions{IfNoneMatch: true}
			if _, err := s.Put(ctx, key, []byte("v1"), opts); err != nil {
				t.Fatalf("first create: %v", err)
			}
			_, err := s.Put(ctx, key, []byte("v2"), opts)
			if !errors.Is(err, objectstore.ErrPreconditionFailed) {
				t.Fatalf("want ErrPreconditionFailed on second create, got %v", err)
			}
			// Unchanged.
			got, _ := s.Get(ctx, key)
			if string(got) != "v1" {
				t.Fatalf("create-only should not overwrite: got %q", got)
			}
		})
	}
}

func TestIfMatchCAS(t *testing.T) {
	ctx := context.Background()
	for name, s := range impls(t) {
		t.Run(name, func(t *testing.T) {
			key := "epoch"
			res, _ := s.Put(ctx, key, []byte("epoch-1"), objectstore.PutOptions{})
			// Wrong ETag fails.
			if _, err := s.Put(ctx, key, []byte("epoch-2"), objectstore.PutOptions{IfMatch: "wrong"}); !errors.Is(err, objectstore.ErrPreconditionFailed) {
				t.Fatalf("want precondition failed on bad CAS, got %v", err)
			}
			// Correct ETag succeeds.
			if _, err := s.Put(ctx, key, []byte("epoch-2"), objectstore.PutOptions{IfMatch: res.ETag}); err != nil {
				t.Fatalf("CAS with correct ETag: %v", err)
			}
		})
	}
}

func TestListByPrefixSorted(t *testing.T) {
	ctx := context.Background()
	for name, s := range impls(t) {
		t.Run(name, func(t *testing.T) {
			_, _ = s.Put(ctx, "wal/v/0/2.wal", []byte("b"), objectstore.PutOptions{})
			_, _ = s.Put(ctx, "wal/v/0/1.wal", []byte("a"), objectstore.PutOptions{})
			_, _ = s.Put(ctx, "other/x", []byte("x"), objectstore.PutOptions{})
			got, err := s.List(ctx, "wal/v/0/")
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 2 || got[0].Key != "wal/v/0/1.wal" || got[1].Key != "wal/v/0/2.wal" {
				t.Fatalf("unexpected list: %+v", got)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	for name, s := range impls(t) {
		t.Run(name, func(t *testing.T) {
			_, _ = s.Put(ctx, "k", []byte("v"), objectstore.PutOptions{})
			if err := s.Delete(ctx, "k"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Get(ctx, "k"); !errors.Is(err, objectstore.ErrNotFound) {
				t.Fatalf("want not found after delete, got %v", err)
			}
		})
	}
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
