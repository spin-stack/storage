package wal_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

func remoteLog(t *testing.T, store *sim.ObjectStore) *wal.Log {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	vol := [16]byte{2}
	l := wal.NewLog(d, "wal", clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(
		wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(store, 5),
		leaseOK{},
	)
	return l
}

func TestFlushUploadsAndAdvancesDurable(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	l := remoteLog(t, store)

	_, _ = l.Write(0, []byte("aaaa"), 0)
	_, _ = l.Write(8, []byte("bbbb"), 0)
	if l.Watermarks().Durable != 0 {
		t.Fatal("durable should be 0 before flush")
	}
	if err := l.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if l.Watermarks().Durable != 2 {
		t.Fatalf("durable should be 2 after flush, got %d", l.Watermarks().Durable)
	}
	objs, _ := store.List(ctx, "wal/")
	if len(objs) != 1 {
		t.Fatalf("expected 1 WAL object, got %d", len(objs))
	}
}

// TestFlushDoesNotAdvanceDurableOnUploadFailure is INV-07: durable_sequence must
// not move past what is verified in S3.
func TestFlushDoesNotAdvanceDurableOnUploadFailure(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	l := remoteLog(t, store)
	_, _ = l.Write(0, []byte("data"), 0)

	store.InjectThrottle(5) // exhausts the uploader's 5-attempt budget, then clears
	if err := l.Flush(ctx); err == nil {
		t.Fatal("flush should fail when uploads fail")
	}
	if l.Watermarks().Durable != 0 {
		t.Fatalf("durable must not advance on upload failure, got %d", l.Watermarks().Durable)
	}
	if objs, _ := store.List(ctx, "wal/"); len(objs) != 0 {
		t.Fatalf("no object should be durable, got %d", len(objs))
	}

	// Recover: the batch was retained; a second flush succeeds.
	if err := l.Flush(ctx); err != nil {
		t.Fatalf("second flush should succeed after throttle clears: %v", err)
	}
	if l.Watermarks().Durable != 1 {
		t.Fatalf("durable should be 1 after recovery, got %d", l.Watermarks().Durable)
	}
}

// TestUploadIdempotentOnLostResponse is INV-21: a PUT that persisted but lost its
// response reconciles on retry via 412 + HEAD.
func TestUploadIdempotentOnLostResponse(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := [16]byte{3}
	b := wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig())
	b.Append(1, rec(1, []byte("payload")), false)
	b.Flush()
	cb := b.Pending()[0]

	store.InjectLostResponse(cb.Object().Key)

	up := wal.NewUploader(store, 5)
	if _, err := up.Upload(ctx, cb); err != nil {
		t.Fatalf("upload should succeed idempotently despite lost response: %v", err)
	}
	// Exactly one object, and a second upload is still idempotent.
	if _, err := up.Upload(ctx, cb); err != nil {
		t.Fatalf("re-upload should be idempotent: %v", err)
	}
	if objs, _ := store.List(ctx, "wal/"); len(objs) != 1 {
		t.Fatalf("expected exactly 1 object, got %d", len(objs))
	}
}

// TestUploadDivergenceHardFails: a different object already at the key is a hard
// fail (§14.5).
func TestUploadDivergenceHardFails(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := [16]byte{4}
	b := wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig())
	b.Append(1, rec(1, []byte("real payload")), false)
	b.Flush()
	cb := b.Pending()[0]
	obj := cb.Object()

	// Pre-place a DIFFERENT object (same key, wrong content).
	_, _ = store.Put(ctx, obj.Key, append([]byte("junk"), obj.Data...), objectstore.PutOptions{})

	up := wal.NewUploader(store, 5)
	if _, err := up.Upload(ctx, cb); !errors.Is(err, wal.ErrDivergentObject) {
		t.Fatalf("want ErrDivergentObject, got %v", err)
	}
}

// TestUploadRefusesASecondObjectForTheSameSpan is INV-21 / §14.5 — "same range,
// different hash ⇒ hard fail". The rule is unreachable through the key, because the
// key *embeds* the content hash: two objects claiming sequences 1..N with different
// content get different keys, so both pass the create-only PUT, both pass recovery's
// integrity check, and Recover applies both. The volume's content is then decided by
// SHA-prefix sort order, with no diagnostic anywhere. The span, not the key, is what
// must be unique.
func TestUploadRefusesASecondObjectForTheSameSpan(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := [16]byte{14}

	batch := func(payload string) *wal.ClosedBatch {
		b := wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig())
		b.Append(1, rec(1, []byte(payload)), false)
		b.Append(2, rec(2, []byte(payload)), false)
		b.Flush()
		return b.Pending()[0]
	}
	first, second := batch("the records W1 wrote"), batch("what W2 put in 1..2")
	if first.Object().Key == second.Object().Key {
		t.Fatal("test setup: the two batches must differ in content")
	}

	up := wal.NewUploader(store, 5)
	if _, err := up.Upload(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := up.Upload(ctx, second); !errors.Is(err, wal.ErrDivergentObject) {
		t.Fatalf("a second object claiming sequences 1..2 with different content must hard-fail, got %v", err)
	}
	if objs, _ := store.List(ctx, "wal/"); len(objs) != 1 {
		t.Fatalf("two objects now claim the same span; recovery picks one by sort order (%d objects)", len(objs))
	}
}

// TestUploadStopsOnACancelledContext: the retry loop is the one place that keeps
// issuing requests after the caller is gone. On a cancelled context it must return at
// the first check instead of spending its whole budget against a backend that is
// already being torn down.
func TestUploadStopsOnACancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	b := wal.NewBatcher(clk, [16]byte{15}, 1, 0, wal.DefaultBatchConfig())
	b.Append(1, rec(1, []byte("payload")), false)
	b.Flush()

	store.InjectThrottle(10) // every attempt would be a retryable failure
	if _, err := wal.NewUploader(store, 5).Upload(ctx, b.Pending()[0]); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled upload must report the cancellation, got %v", err)
	}
}

func TestEncryptedRemoteObjectsAreCiphertext(t *testing.T) {
	// End-to-end: encrypted + remote — the uploaded object contains no cleartext.
	ctx := t.Context()
	store := sim.NewObjectStore()
	l := remoteLog(t, store)
	// A versioned DEK: KeyID 0 is the on-disk marker for "plaintext record", so a
	// zero-KeyID DEK produces objects that no recovery can read (see
	// TestUnversionedDEKIsRefusedAtWriteTime). The canary check is the same.
	dek, err := crypto.GenerateDEK(&ramp{b: 11}, 7)
	if err != nil {
		t.Fatal(err)
	}
	l.EnableEncryption(&wal.Encryption{DEK: dek, VolumeID: [16]byte{2}})
	_ = format.FormatVersion // keep format import used

	canary := []byte("PLAINTEXT-SHOULD-NOT-APPEAR")
	_, _ = l.Write(0, canary, 0)
	if err := l.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	objs, _ := store.List(ctx, "wal/")
	if len(objs) != 1 {
		t.Fatalf("expected 1 object, got %d", len(objs))
	}
	data, _ := store.Get(ctx, objs[0].Key)
	if contains(data, canary) {
		t.Fatal("uploaded WAL object contains cleartext — INV-15 violation")
	}
}

func contains(haystack, needle []byte) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if string(haystack[i:i+len(needle)]) == string(needle) {
			return true
		}
	}
	return false
}

// leaseOK is the fence for tests that are not about fencing: remote durability
// requires a lease checker (DEV-0004), and these hold a valid one.
type leaseOK struct{}

func (leaseOK) Valid() bool { return true }
