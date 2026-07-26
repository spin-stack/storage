package wal_test

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// The uploader reconciled a lost PUT response by comparing the stored object's ETag
// against a SHA-256 it computed. That only works because both in-process stores
// happen to use SHA-256 as their ETag. S3 does not: it returns a quoted MD5, and a
// multipart object's ETag is an MD5-of-MD5s with a `-N` suffix — the conformance
// suite already documents this. On a real backend the first lost response would make
// every retry report ErrDivergentObject for a byte-perfect object, and since the
// batch is retained the volume would never advance its durable sequence again.
//
// s3ETagStore is the sim store wearing S3's ETag shape.
type s3ETagStore struct {
	*sim.ObjectStore
	multipart bool
}

func (s *s3ETagStore) etag(data []byte) string {
	sum := md5.Sum(data)
	e := `"` + hex.EncodeToString(sum[:]) + `"`
	if s.multipart {
		e = `"` + hex.EncodeToString(sum[:]) + `-3"`
	}
	return e
}

func (s *s3ETagStore) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	res, err := s.ObjectStore.Put(ctx, key, data, opts)
	if err != nil {
		return res, err
	}
	return objectstore.PutResult{ETag: s.etag(data)}, nil
}

func (s *s3ETagStore) Head(ctx context.Context, key string) (objectstore.ObjectInfo, error) {
	info, err := s.ObjectStore.Head(ctx, key)
	if err != nil {
		return info, err
	}
	body, err := s.Get(ctx, key)
	if err != nil {
		return info, err
	}
	info.ETag = s.etag(body)
	return info, nil
}

func batchFor(t *testing.T, payload string) *wal.ClosedBatch {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := [16]byte{3}
	b := wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig())
	rec := wal.Record{Type: 1, Epoch: 1, Sequence: 1, Offset: 0, Length: uint32(len(payload)), Payload: []byte(payload)}
	enc, err := rec.Encode()
	if err != nil {
		t.Fatal(err)
	}
	b.Append(1, enc, false)
	b.Flush()
	pending := b.Pending()
	if len(pending) != 1 {
		t.Fatalf("expected one closed batch, got %d", len(pending))
	}
	return pending[0]
}

// TestLostResponseReconcilesOnABackendWithS3ETags: the object in the store is
// byte-for-byte what we uploaded, so the retry must report idempotent success.
func TestLostResponseReconcilesOnABackendWithS3ETags(t *testing.T) {
	for _, multipart := range []bool{false, true} {
		name := "md5 etag"
		if multipart {
			name = "multipart etag"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			store := &s3ETagStore{ObjectStore: sim.NewObjectStore(), multipart: multipart}
			cb := batchFor(t, "batch-bytes")

			store.InjectLostResponse(cb.Object().Key)
			key, err := wal.NewUploader(store, 5).Upload(ctx, cb)
			if err != nil {
				t.Fatalf("a lost response over an identical object must reconcile, got %v", err)
			}
			if key != cb.Object().Key {
				t.Fatalf("key = %q", key)
			}
		})
	}
}

// TestDivergentObjectIsStillDetected is the other half: reconciling must not become
// "assume it is fine". A different body at the same key is a hard failure (§14.5).
func TestDivergentObjectIsStillDetected(t *testing.T) {
	ctx := t.Context()
	store := &s3ETagStore{ObjectStore: sim.NewObjectStore()}
	cb := batchFor(t, "batch-bytes")

	// Somebody else already wrote different content at this key.
	if _, err := store.Put(ctx, cb.Object().Key, []byte("not the same object"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := wal.NewUploader(store, 5).Upload(ctx, cb); !errors.Is(err, wal.ErrDivergentObject) {
		t.Fatalf("want ErrDivergentObject for different content at the key, got %v", err)
	}
}
