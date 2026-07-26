package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// Upload errors.
var (
	// ErrDivergentObject means an object already exists at the deterministic key
	// with different content — corruption or a bug, a hard fail (§14.5).
	ErrDivergentObject = errors.New("wal: divergent object at key (same range, different content)")
	// ErrUploadRetriesExhausted means the retry budget ran out on transient errors.
	ErrUploadRetriesExhausted = errors.New("wal: upload retries exhausted")
	// ErrOverlappingSpan means this uploader already published an object covering
	// part — but not all — of this batch's sequence range in the same (volume,
	// epoch). Two objects claiming one sequence is INV-21, and unlike the foreign
	// writer case it is decidable here without asking the backend anything.
	ErrOverlappingSpan = errors.New("wal: batch overlaps a sequence span this uploader already published")
)

// Backoff is the wait between upload attempts. It is exponential from Base, doubling
// each attempt and capped at Max (0 = uncapped). A zero Base means no wait, which is
// what an uploader built without WithBackoff does.
//
// There is deliberately no jitter. Two things rule it out: a DST run must be
// reproducible from its seed, and a jittered delay would have to draw from an
// injected source of randomness for that to hold; and spreading a fleet's retries so
// they do not resonate is a property of the fleet, not of one volume's WAL — a caller
// that wants it can supply its own schedule per volume.
type Backoff struct {
	// Base is the wait before the second attempt. 0 disables waiting entirely.
	Base time.Duration
	// Max caps the wait. 0 means no cap.
	Max time.Duration
}

// Delay returns the wait before the given attempt, counting the first attempt as 0.
// Delay(0) is always 0: the first try is what the caller asked for, not a retry.
func (b Backoff) Delay(attempt int) time.Duration {
	if attempt < 1 || b.Base <= 0 {
		return 0
	}
	d := b.Base
	for range attempt - 1 {
		d *= 2
		if d <= 0 || (b.Max > 0 && d >= b.Max) { // overflow or past the cap
			return b.Max
		}
	}
	if b.Max > 0 && d > b.Max {
		return b.Max
	}
	return d
}

// Uploader PUTs closed batches to the object store idempotently (§14.5): create-only
// with If-None-Match; on a lost response it retries, and a 412 is reconciled by HEAD
// + checksum (idempotent success) or a hard fail on divergence. It is provider-
// agnostic: any non-precondition error is treated as transient and retried.
type Uploader struct {
	store       objectstore.Store
	maxAttempts int

	// clk/backoff space the retries out. Without them the budget is spent inside a
	// single throttling window (see UploaderOption); nil clk means no waiting.
	clk     clock.Clock
	backoff Backoff

	// claimed is every sequence span this uploader has published, per (volume,
	// epoch). See selfOverlap: it is the half of INV-21 the write path can decide on
	// its own. It grows by one entry per WAL object, in step with Log.uploaded, and
	// is bounded by the same thing — a log that has published enough objects for
	// this to matter has long since been checkpointed and rebuilt.
	claimed map[spanKey][]span
}

// spanKey scopes a claimed span to the sequence space it belongs to. Sequences belong
// to a volume (§12.5), and an epoch boundary is what makes the successor's records
// different records, so two volumes — or two epochs — both writing 1..3 do not
// overlap. One uploader may serve several of each.
type spanKey struct {
	volume [16]byte
	epoch  uint64
}

// span is an inclusive sequence range [first, last].
type span struct{ first, last uint64 }

// overlaps reports whether two spans share a sequence without being the same span. An
// identical span is not an overlap: it is the idempotent re-upload of a batch whose
// PUT response was lost, which §14.5 requires to succeed.
func (s span) overlaps(o span) bool {
	if s == o {
		return false
	}
	return s.first <= o.last && o.first <= s.last
}

// UploaderOption configures an Uploader at construction.
type UploaderOption func(*Uploader)

// WithBackoff spaces retries out on the injected clock (§25.1, INV-01: never
// time.Sleep). It is opt-in rather than the default for two reasons: an uploader
// built without a clock cannot wait at all, and the wait is only correct if the
// caller's clock actually advances — under a simulated clock driven step by step, a
// waiting uploader is a stopped one until the scenario advances time.
func WithBackoff(clk clock.Clock, b Backoff) UploaderOption {
	return func(u *Uploader) {
		u.clk = clk
		u.backoff = b
	}
}

// NewUploader returns an uploader with a bounded retry budget (never infinite).
func NewUploader(store objectstore.Store, maxAttempts int, opts ...UploaderOption) *Uploader {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	u := &Uploader{store: store, maxAttempts: maxAttempts}
	for _, opt := range opts {
		opt(u)
	}
	return u
}

// wait pauses before a retry. It returns the context's error if the caller is gone,
// which is the same reason the attempt loop checks ctx: spending the schedule on a
// backend that is already being torn down helps nobody.
func (u *Uploader) wait(ctx context.Context, attempt int) error {
	if u.clk == nil {
		return nil
	}
	d := u.backoff.Delay(attempt)
	if d <= 0 {
		return nil
	}
	return u.clk.Sleep(ctx, d)
}

// Upload stores the batch's WAL object idempotently and returns its key once the
// object is verified present with the expected content.
func (u *Uploader) Upload(ctx context.Context, cb *ClosedBatch) (string, error) {
	if err := u.selfOverlap(cb); err != nil {
		return "", err
	}
	obj := cb.Object()

	var lastErr error
	for attempt := 0; attempt < u.maxAttempts; attempt++ {
		// The caller is gone: spending the rest of the budget on a backend that is
		// already being torn down helps nobody, and the error it would return
		// ("retries exhausted") describes the wrong failure.
		if err := ctx.Err(); err != nil {
			return "", err
		}
		// Space the retries out. Without this the whole budget is spent inside the
		// throttling window the first attempt already lost to: N attempts cost N
		// round trips, not N × anything the backend needs to recover.
		if err := u.wait(ctx, attempt); err != nil {
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
			u.claim(cb)
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
			u.claim(cb)
			return obj.Key, nil // idempotent success
		default:
			// Transient (lost response, throttle, ...): retry within budget.
			lastErr = err
		}
	}
	return "", fmt.Errorf("%w: %v", ErrUploadRetriesExhausted, lastErr)
}

// selfOverlap closes the half of INV-21 the write path can decide: a batch whose
// sequence range partially overlaps one this uploader already published. It needs no
// I/O, so it is not blinded by a lagging listing, and it costs a slice scan per
// upload against a slice with one entry per object already written.
//
// It is the failure the write path can actually see: a batcher accounting bug, or a
// Resume that re-queued records an object already covers. What it does NOT decide is
// an overlap produced by *another* writer in the same (volume, epoch) — see
// spanClaimed for that argument.
func (u *Uploader) selfOverlap(cb *ClosedBatch) error {
	this := span{first: cb.First, last: cb.Last}
	for _, other := range u.claimed[spanKey{volume: cb.VolumeID, epoch: cb.Epoch}] {
		if other.overlaps(this) {
			return fmt.Errorf("%w: %d-%d overlaps the published %d-%d (violates INV-21)",
				ErrOverlappingSpan, this.first, this.last, other.first, other.last)
		}
	}
	return nil
}

// claim records a span as published. Only a verified upload claims one: a failed PUT
// must stay retryable, and its span must stay available to the retry.
func (u *Uploader) claim(cb *ClosedBatch) {
	if u.claimed == nil {
		u.claimed = map[spanKey][]span{}
	}
	k := spanKey{volume: cb.VolumeID, epoch: cb.Epoch}
	this := span{first: cb.First, last: cb.Last}
	for _, other := range u.claimed[k] {
		if other == this {
			return // an idempotent re-upload of the same batch
		}
	}
	u.claimed[k] = append(u.claimed[k], this)
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
//
// It decides *identical* spans only, and that is a decision, not an oversight. A
// partial overlap from a foreign writer ({1-3} against this batch's {2-5}) lands on an
// unrelated key prefix, and finding it would mean listing the whole epoch and parsing
// every key's range on every upload. That check would still be wrong twice over: LIST
// is eventually consistent (§6.1), so it answers "nothing there" precisely in the
// window a fenced predecessor's object was just written — the case it exists for — and
// treating any listed overlap as fatal would permanently wedge a writer that restarted
// and harmlessly re-batched records it had already published, which
// recovery.VerifyAgreement deliberately allows when the shared records are
// byte-identical. Two writers in one (volume, epoch) is a fencing failure (§12.2,
// INV-06) and is caught where the answer must be right: recovery reads a complete
// listing and fails with ErrAmbiguousSequence. selfOverlap covers what this uploader
// can know without asking anyone.
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
