package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// Upload errors.
var (
	// ErrDivergentObject means an object already exists at the deterministic key
	// with different content — corruption or a bug, a hard fail (§14.5).
	ErrDivergentObject = errors.New("wal: divergent object at key (same range, different content)")
	// ErrUploadRetriesExhausted means the retry budget ran out on transient errors.
	ErrUploadRetriesExhausted = errors.New("wal: upload retries exhausted")
)

// Uploader PUTs closed batches to the object store idempotently (§14.5): create-only
// with If-None-Match; on a lost response it retries, and a 412 is reconciled by HEAD
// + checksum (idempotent success) or a hard fail on divergence. It is provider-
// agnostic: any non-precondition error is treated as transient and retried.
type Uploader struct {
	store       objectstore.Store
	maxAttempts int
}

// NewUploader returns an uploader with a bounded retry budget (never infinite).
func NewUploader(store objectstore.Store, maxAttempts int) *Uploader {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	return &Uploader{store: store, maxAttempts: maxAttempts}
}

// Upload stores the batch's WAL object idempotently and returns its key once the
// object is verified present with the expected content.
func (u *Uploader) Upload(ctx context.Context, cb *ClosedBatch) (string, error) {
	obj := cb.Object()

	var lastErr error
	for attempt := 0; attempt < u.maxAttempts; attempt++ {
		_, err := u.store.Put(ctx, obj.Key, obj.Data, objectstore.PutOptions{IfNoneMatch: true})
		switch {
		case err == nil:
			return obj.Key, nil
		case errors.Is(err, objectstore.ErrPreconditionFailed):
			// Already present: reconcile against what is stored (§14.5). The ETag
			// cannot decide this — it is a CAS token, not a content digest: S3
			// returns a quoted MD5, and a multipart object's ETag is an MD5-of-MD5s
			// with a `-N` suffix. Comparing our SHA-256 against it would report a
			// byte-perfect object as divergent and wedge the volume forever. So the
			// size is the cheap check and the bytes are the decisive one; this runs
			// only on the rare lost-response path.
			info, herr := u.store.Head(ctx, obj.Key)
			if herr != nil {
				lastErr = herr
				continue
			}
			if info.Size != int64(len(obj.Data)) {
				return "", ErrDivergentObject
			}
			stored, gerr := u.store.Get(ctx, obj.Key)
			if gerr != nil {
				lastErr = gerr
				continue
			}
			if !bytes.Equal(stored, obj.Data) {
				return "", ErrDivergentObject
			}
			return obj.Key, nil // idempotent success
		default:
			// Transient (lost response, throttle, ...): retry within budget.
			lastErr = err
		}
	}
	return "", fmt.Errorf("%w: %v", ErrUploadRetriesExhausted, lastErr)
}
