// Package storetest is the objectstore.Store contract as an executable spec: one
// set of assertions every implementation must satisfy — the deterministic sim, the
// filesystem-backed store, and the S3-backed one running against a real backend in
// a container. Writing it once is the point: an implementation that drifts from the
// others fails here rather than in a recovery.
package storetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// NewStore returns a fresh, empty store for one subtest.
type NewStore func(t *testing.T) objectstore.Store

// RunContract runs the full contract against the implementation newStore builds.
func RunContract(t *testing.T, newStore NewStore) {
	t.Helper()
	ctx := context.Background()

	t.Run("put/get/head round trip", func(t *testing.T) {
		s := newStore(t)
		key := "wal/vol/0/1-1-abcd.wal"
		data := []byte("encrypted-record-bytes")

		res, err := s.Put(ctx, key, data, objectstore.PutOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if res.ETag == "" {
			t.Fatal("expected an ETag; the CAS protocol (§12.4) needs one")
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

	t.Run("get of a missing key is ErrNotFound", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Get(ctx, "absent"); !errors.Is(err, objectstore.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
		if _, err := s.Head(ctx, "absent"); !errors.Is(err, objectstore.ErrNotFound) {
			t.Fatalf("head: want ErrNotFound, got %v", err)
		}
	})

	t.Run("If-None-Match is create-only", func(t *testing.T) {
		s := newStore(t)
		opts := objectstore.PutOptions{IfNoneMatch: true}
		if _, err := s.Put(ctx, "k", []byte("v1"), opts); err != nil {
			t.Fatalf("first create: %v", err)
		}
		if _, err := s.Put(ctx, "k", []byte("v2"), opts); !errors.Is(err, objectstore.ErrPreconditionFailed) {
			t.Fatalf("want ErrPreconditionFailed on second create, got %v", err)
		}
		if got, _ := s.Get(ctx, "k"); string(got) != "v1" {
			t.Fatalf("create-only must not overwrite: got %q", got)
		}
	})

	t.Run("If-Match is a compare-and-swap", func(t *testing.T) {
		s := newStore(t)
		res, err := s.Put(ctx, "epoch", []byte("epoch-1"), objectstore.PutOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Put(ctx, "epoch", []byte("epoch-2"), objectstore.PutOptions{IfMatch: `"wrong"`}); !errors.Is(err, objectstore.ErrPreconditionFailed) {
			t.Fatalf("want ErrPreconditionFailed on a stale CAS, got %v", err)
		}
		if got, _ := s.Get(ctx, "epoch"); string(got) != "epoch-1" {
			t.Fatalf("a failed CAS must not write: got %q", got)
		}
		if _, err := s.Put(ctx, "epoch", []byte("epoch-2"), objectstore.PutOptions{IfMatch: res.ETag}); err != nil {
			t.Fatalf("CAS with the current ETag: %v", err)
		}
	})

	t.Run("list by prefix, sorted", func(t *testing.T) {
		s := newStore(t)
		for _, k := range []string{"wal/v/0/2.wal", "wal/v/0/1.wal", "other/x"} {
			if _, err := s.Put(ctx, k, []byte(k), objectstore.PutOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		got, err := s.List(ctx, "wal/v/0/")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0].Key != "wal/v/0/1.wal" || got[1].Key != "wal/v/0/2.wal" {
			t.Fatalf("unexpected list: %+v", got)
		}
	})

	t.Run("delete then get is ErrNotFound", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Put(ctx, "k", []byte("v"), objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, "k"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get(ctx, "k"); !errors.Is(err, objectstore.ErrNotFound) {
			t.Fatalf("want ErrNotFound after delete, got %v", err)
		}
	})

	t.Run("delete of a missing key is ErrNotFound", func(t *testing.T) {
		s := newStore(t)
		if err := s.Delete(ctx, "never-there"); !errors.Is(err, objectstore.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	})

	t.Run("binary payloads round trip unchanged", func(t *testing.T) {
		s := newStore(t)
		payload := bytes.Repeat([]byte{0x00, 0xFF, 0x7F, 0xAB}, 4096)
		if _, err := s.Put(ctx, "bin", payload, objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		got, err := s.Get(ctx, "bin")
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("binary round trip changed the bytes (err=%v)", err)
		}
	})

	t.Run("an empty object is a real object", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Put(ctx, "empty", nil, objectstore.PutOptions{IfNoneMatch: true}); err != nil {
			t.Fatal(err)
		}
		info, err := s.Head(ctx, "empty")
		if err != nil || info.Size != 0 {
			t.Fatalf("head of an empty object: %+v err=%v", info, err)
		}
		if got, err := s.Get(ctx, "empty"); err != nil || len(got) != 0 {
			t.Fatalf("get of an empty object: %q err=%v", got, err)
		}
	})

	// Finding 6. Create-only is what makes a retried WAL upload harmless (INV-21) and
	// a published manifest immutable (INV-16); the If-Match CAS is the §12.4 epoch
	// fence (INV-10). Both are claims about *concurrent* writers, and neither had a
	// single concurrent test — a check-then-act implementation passes every
	// sequential assertion above while admitting two winners, silently, with err ==
	// nil for both. Two promoters both winning CompareAndAdvance is split brain.
	t.Run("create-only admits exactly one concurrent writer", func(t *testing.T) {
		s := newStore(t)
		const writers = 16
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			winners []string
			losers  []error
			start   = make(chan struct{})
		)
		for i := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start // release every writer at once, not as they are scheduled
				body := fmt.Sprintf("writer-%d", i)
				_, err := s.Put(ctx, "wal/contended.wal", []byte(body), objectstore.PutOptions{IfNoneMatch: true})
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					winners = append(winners, body)
					return
				}
				losers = append(losers, err)
			}()
		}
		close(start)
		wg.Wait()

		if len(winners) != 1 {
			t.Fatalf("create-only admitted %d concurrent writers, want exactly 1", len(winners))
		}
		for _, err := range losers {
			if !errors.Is(err, objectstore.ErrPreconditionFailed) {
				t.Fatalf("a loser got %v, want ErrPreconditionFailed", err)
			}
		}
		got, err := s.Get(ctx, "wal/contended.wal")
		if err != nil || string(got) != winners[0] {
			t.Fatalf("stored body = %q err=%v, want the winner's %q", got, err, winners[0])
		}
	})

	t.Run("If-Match admits exactly one concurrent winner", func(t *testing.T) {
		s := newStore(t)
		res, err := s.Put(ctx, "epoch", []byte("epoch-1"), objectstore.PutOptions{})
		if err != nil {
			t.Fatal(err)
		}
		const contenders = 16
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			winners []string
			losers  []error
			start   = make(chan struct{})
		)
		for i := range contenders {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start // release every contender at once
				body := fmt.Sprintf("epoch-%d", i+2)
				_, err := s.Put(ctx, "epoch", []byte(body), objectstore.PutOptions{IfMatch: res.ETag})
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					winners = append(winners, body)
					return
				}
				losers = append(losers, err)
			}()
		}
		close(start)
		wg.Wait()

		if len(winners) != 1 {
			t.Fatalf("%d concurrent CAS operations won on one ETag, want exactly 1 — that is split brain", len(winners))
		}
		for _, err := range losers {
			if !errors.Is(err, objectstore.ErrPreconditionFailed) {
				t.Fatalf("a fenced contender got %v, want ErrPreconditionFailed", err)
			}
		}
		if got, _ := s.Get(ctx, "epoch"); string(got) != winners[0] {
			t.Fatalf("stored body = %q, want the winner's %q", got, winners[0])
		}
	})

	// Finding 3. An ETag is a CAS token and nothing else. S3 returns a quoted MD5 and
	// a multipart object's is an MD5-of-MD5s with a `-N` suffix; both in-process
	// stores happen to use SHA-256. Any code that compares an ETag against a content
	// hash it computed reports a byte-perfect object as divergent on a real backend —
	// which is the §14.5 lost-response path, i.e. the most common S3 failure there is.
	// The contract pins only what an ETag is allowed to promise.
	t.Run("an ETag is opaque, stable, and changes with the content", func(t *testing.T) {
		s := newStore(t)
		first, err := s.Put(ctx, "k", []byte("v1"), objectstore.PutOptions{})
		if err != nil {
			t.Fatal(err)
		}
		info, err := s.Head(ctx, "k")
		if err != nil {
			t.Fatal(err)
		}
		if info.ETag != first.ETag {
			t.Fatalf("HEAD ETag %q != PUT ETag %q; a CAS token that is not stable is not a CAS token", info.ETag, first.ETag)
		}
		second, err := s.Put(ctx, "k", []byte("v2-longer"), objectstore.PutOptions{IfMatch: first.ETag})
		if err != nil {
			t.Fatalf("CAS with the ETag we were handed must work: %v", err)
		}
		if second.ETag == first.ETag {
			t.Fatal("the ETag did not change when the content did; a stale CAS would then succeed")
		}
		if _, err := s.Put(ctx, "k", []byte("v3"), objectstore.PutOptions{IfMatch: first.ETag}); !errors.Is(err, objectstore.ErrPreconditionFailed) {
			t.Fatalf("CAS on the superseded ETag: %v, want ErrPreconditionFailed", err)
		}
	})

	// Finding 8. The GC runs on a schedule and an operator can run it by hand at the
	// same time. Whether Delete of a key another pass already marked is an error or a
	// no-op decides whether the second pass abandons the rest of the bucket, so the
	// contract has to state it rather than leave each implementation to choose.
	t.Run("delete of an already-marked key is ErrNotFound", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Put(ctx, "k", []byte("v"), objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, "k"); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, "k"); !errors.Is(err, objectstore.ErrNotFound) {
			t.Fatalf("second delete: %v, want ErrNotFound", err)
		}
	})

	// Findings 1 and 7. INV-14 says a GC mistake costs a restore, not the data. That
	// argument only holds if Restore exists on the store the GC actually runs
	// against, and if it either returns the marked bytes or says why it cannot.
	t.Run("a mark is reversible", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Put(ctx, "wal/v/1/x.wal", []byte("acked-payload"), objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, "wal/v/1/x.wal"); err != nil {
			t.Fatal(err)
		}
		if err := s.Restore(ctx, "wal/v/1/x.wal"); err != nil {
			t.Fatalf("restore: %v", err)
		}
		if got, err := s.Get(ctx, "wal/v/1/x.wal"); err != nil || string(got) != "acked-payload" {
			t.Fatalf("restored body = %q err=%v", got, err)
		}
	})

	t.Run("restore of a key that was never marked is ErrNotFound", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Put(ctx, "k", []byte("v"), objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := s.Restore(ctx, "k"); !errors.Is(err, objectstore.ErrNotFound) {
			t.Fatalf("restore of a live key: %v, want ErrNotFound", err)
		}
		if err := s.Restore(ctx, "absent"); !errors.Is(err, objectstore.ErrNotFound) {
			t.Fatalf("restore of a missing key: %v, want ErrNotFound", err)
		}
	})

	// Finding 7. The un-GC runbook is "restore the version the sweep marked". If
	// anything wrote the key again in the meantime — the Agent's idempotent retry, a
	// rebuild, another operator — the marked version is no longer the one a restore
	// would surface. Returning the *new* bytes there is the worst outcome available:
	// the operator believes the data is back and the volume is reconstructed from
	// content that was never what was marked. Refuse, distinctly.
	t.Run("restore after the key was rewritten is refused, not silently wrong", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Put(ctx, "wal/v/1/x.wal", []byte("the-marked-bytes"), objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, "wal/v/1/x.wal"); err != nil {
			t.Fatal(err)
		}
		// A writer rewrites the key; on a versioned bucket this stacks a new current
		// version over the delete marker.
		if _, err := s.Put(ctx, "wal/v/1/x.wal", []byte("different"), objectstore.PutOptions{IfNoneMatch: true}); err != nil {
			t.Fatalf("create-only over a marked key must succeed: %v", err)
		}
		err := s.Restore(ctx, "wal/v/1/x.wal")
		if !errors.Is(err, objectstore.ErrRestoreSuperseded) {
			t.Fatalf("restore after a rewrite: %v, want ErrRestoreSuperseded", err)
		}
		if got, _ := s.Get(ctx, "wal/v/1/x.wal"); string(got) != "different" {
			t.Fatalf("a refused restore must not change the object: %q", got)
		}
	})
}
