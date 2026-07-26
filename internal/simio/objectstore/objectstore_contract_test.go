package objectstore_test

import (
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
	ctx := t.Context()
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
	ctx := t.Context()
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
	ctx := t.Context()
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

// Finding 1, structural half of INV-14. "A GC mistake costs a restore, not the data"
// is only true if every implementation the GC can be handed can actually reverse a
// mark — including the S3-backed one, which is the store production runs against and
// which had no Restore at all. Restore is part of objectstore.Store, so this is a
// compile-time fact rather than a convention: a new implementation that cannot
// reverse a mark does not build.
var (
	_ objectstore.Store = (*sim.ObjectStore)(nil)
	_ objectstore.Store = (*real.ObjectStore)(nil)
	_ objectstore.Store = (*real.S3Store)(nil)
)

// TestDeleteIsReversibleAcrossImplementations is the structural half of INV-14
// (DEV-0006): every store must implement Delete as a reversible mark, so a GC
// mistake costs a restore and not the data. The S3-backed store satisfies this via
// the backend's delete markers, asserted in the integration lane.
func TestDeleteIsReversibleAcrossImplementations(t *testing.T) {
	ctx := t.Context()
	impls := map[string]objectstore.Store{"sim": sim.NewObjectStore()}
	rs, err := real.NewObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	impls["real-filesystem"] = rs

	for name, s := range impls {
		t.Run(name, func(t *testing.T) {
			if _, err := s.Put(ctx, "wal/v/1/x.wal", []byte("payload"), objectstore.PutOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := s.Delete(ctx, "wal/v/1/x.wal"); err != nil {
				t.Fatal(err)
			}
			// Hidden from every read path, including listings.
			if _, err := s.Get(ctx, "wal/v/1/x.wal"); !errors.Is(err, objectstore.ErrNotFound) {
				t.Fatalf("get after delete: %v", err)
			}
			if objs, err := s.List(ctx, "wal/"); err != nil || len(objs) != 0 {
				t.Fatalf("list after delete: %+v err=%v", objs, err)
			}
			// A second delete finds nothing to mark.
			if err := s.Delete(ctx, "wal/v/1/x.wal"); !errors.Is(err, objectstore.ErrNotFound) {
				t.Fatalf("double delete: %v", err)
			}
			// And the bytes are still there.
			if err := s.Restore(ctx, "wal/v/1/x.wal"); err != nil {
				t.Fatalf("restore: %v", err)
			}
			body, err := s.Get(ctx, "wal/v/1/x.wal")
			if err != nil || string(body) != "payload" {
				t.Fatalf("restored body = %q err=%v", body, err)
			}
			// Writing over a marked key behaves like a new version (create-only sees
			// the object as absent, matching S3 on a versioned bucket).
			if err := s.Delete(ctx, "wal/v/1/x.wal"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Put(ctx, "wal/v/1/x.wal", []byte("v2"), objectstore.PutOptions{IfNoneMatch: true}); err != nil {
				t.Fatalf("create-only over a marked key must succeed: %v", err)
			}
			if body, _ := s.Get(ctx, "wal/v/1/x.wal"); string(body) != "v2" {
				t.Fatalf("body after rewrite = %q", body)
			}
			// Finding 7. This test used to stop here, which blessed the one outcome an
			// operator must never get: the un-GC runbook step returning the *rewritten*
			// bytes as though they were the marked ones. The marked version is not what
			// a restore would surface any more, so the restore has to say so.
			if err := s.Restore(ctx, "wal/v/1/x.wal"); !errors.Is(err, objectstore.ErrRestoreSuperseded) {
				t.Fatalf("restore after a rewrite = %v, want ErrRestoreSuperseded", err)
			}
		})
	}
}
