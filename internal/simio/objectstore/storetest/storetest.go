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
}
