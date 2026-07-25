package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

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
		// The caller is gone: spending the rest of the budget on a backend that is
		// already being torn down helps nobody, and the error it would return
		// ("retries exhausted") describes the wrong failure.
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if err := u.spanClaimed(ctx, obj.Key); err != nil {
			if errors.Is(err, ErrDivergentObject) {
				return "", err
			}
			lastErr = err
			continue
		}
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

// spanClaimed enforces INV-21 / §14.5 ("same range, different hash ⇒ hard fail")
// where it is actually decidable. The create-only PUT cannot decide it: the key
// *embeds* the content hash, so two objects claiming the same sequence span with
// different content land on two different keys, both succeed, both pass recovery's
// integrity check, and Recover applies both — the volume's content then depends on
// SHA-prefix sort order, with no diagnostic anywhere. The span is what must be
// unique, so that is what is checked: everything under the key minus its hash suffix.
//
// It costs one prefix LIST per attempt, which returns at most a handful of keys. The
// alternative is a bucket in which two writers' versions of sequences 1..N coexist
// silently, which is the exact scenario the invariant exists to catch.
func (u *Uploader) spanClaimed(ctx context.Context, key string) error {
	dash := strings.LastIndex(key, "-")
	if dash < 0 {
		return nil
	}
	infos, err := u.store.List(ctx, key[:dash+1])
	if err != nil {
		return err // transient: the caller retries within its budget
	}
	for _, info := range infos {
		if info.Key != key {
			return fmt.Errorf("%w: %s already claims this sequence span", ErrDivergentObject, info.Key)
		}
	}
	return nil
}
