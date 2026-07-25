package wal_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

func remoteLog(t *testing.T, store *sim.ObjectStore) *wal.Log {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	f, _ := d.Create("wal/active.wal")
	vol := [16]byte{2}
	l := wal.NewLog(f, clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(
		wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(store, 5),
	)
	return l
}

func TestFlushUploadsAndAdvancesDurable(t *testing.T) {
	ctx := context.Background()
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
	ctx := context.Background()
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
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := [16]byte{3}
	b := wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig())
	b.Append(1, rec(1, []byte("payload")), false)
	b.Flush()
	cb := b.Pending()[0]

	key, _, _ := cb.Object()
	store.InjectLostResponse(key)

	up := wal.NewUploader(store, 5)
	if err := up.Upload(ctx, cb); err != nil {
		t.Fatalf("upload should succeed idempotently despite lost response: %v", err)
	}
	// Exactly one object, and a second upload is still idempotent.
	if err := up.Upload(ctx, cb); err != nil {
		t.Fatalf("re-upload should be idempotent: %v", err)
	}
	if objs, _ := store.List(ctx, "wal/"); len(objs) != 1 {
		t.Fatalf("expected exactly 1 object, got %d", len(objs))
	}
}

// TestUploadDivergenceHardFails: a different object already at the key is a hard
// fail (§14.5).
func TestUploadDivergenceHardFails(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := [16]byte{4}
	b := wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig())
	b.Append(1, rec(1, []byte("real payload")), false)
	b.Flush()
	cb := b.Pending()[0]
	key, data, _ := cb.Object()

	// Pre-place a DIFFERENT object (same key, wrong content).
	_, _ = store.Put(ctx, key, append([]byte("junk"), data...), objectstore.PutOptions{})

	up := wal.NewUploader(store, 5)
	if err := up.Upload(ctx, cb); !errors.Is(err, wal.ErrDivergentObject) {
		t.Fatalf("want ErrDivergentObject, got %v", err)
	}
}

func TestEncryptedRemoteObjectsAreCiphertext(t *testing.T) {
	// End-to-end: encrypted + remote — the uploaded object contains no cleartext.
	ctx := context.Background()
	store := sim.NewObjectStore()
	l := remoteLog(t, store)
	l.EnableEncryption(&wal.Encryption{VolumeID: [16]byte{2}}) // zero DEK is fine for the canary check
	_ = format.FormatVersion                                   // keep format import used

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
