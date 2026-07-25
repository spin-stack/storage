//go:build integration

package backend_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/real"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// makeVersionedBucket creates a bucket with versioning Enabled, the way §10 requires
// every production bucket to be.
func makeVersionedBucket(t *testing.T, ctx context.Context, c *s3.Client, name string) {
	t.Helper()
	makeBucket(t, ctx, c, name, false)
	if _, err := c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(name),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatalf("enable versioning on %s: %v", name, err)
	}
}

// Finding 1. INV-14 says a GC mark is reversible because the bucket is versioned.
// Nothing verified the bucket actually is, and the S3-backed store had no Restore at
// all — so the operator action the entire invariant rests on did not exist on the
// production path. These are the two halves: refuse a bucket we cannot undo a delete
// on, and be able to undo one on a bucket we accepted.

func TestNewS3StoreRefusesABucketWithoutVersioning(t *testing.T) {
	be := backendConfig(t)
	ctx := context.Background()
	c := be.Client()

	tests := []struct {
		name   string
		bucket string
		setup  func(t *testing.T, bucket string)
	}{
		{
			name:   "versioning never enabled",
			bucket: "unversioned-bucket",
			setup:  func(t *testing.T, b string) { makeBucket(t, ctx, c, b, false) },
		},
		{
			name:   "versioning suspended by an operator",
			bucket: "suspended-bucket",
			setup: func(t *testing.T, b string) {
				makeVersionedBucket(t, ctx, c, b)
				if _, err := c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
					Bucket:                  aws.String(b),
					VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusSuspended},
				}); err != nil {
					t.Fatalf("suspend versioning: %v", err)
				}
			},
		},
		{
			name:   "bucket does not exist",
			bucket: "no-such-bucket-at-all",
			setup:  func(*testing.T, string) {},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup(t, tc.bucket)
			_, err := real.NewS3Store(ctx, real.S3Config{
				Bucket: tc.bucket, Endpoint: be.Endpoint, Region: be.Region,
				AccessKey: be.AccessKey, SecretKey: be.SecretKey,
			})
			if !errors.Is(err, real.ErrBucketNotVersioned) {
				t.Fatalf("err = %v, want ErrBucketNotVersioned — on this bucket every GC mark is permanent", err)
			}
		})
	}
}

// TestS3StoreDeleteIsAVersionedDeleteMarker: the mark the GC places must be a delete
// marker over a retained version, and Restore must bring exactly those bytes back.
// This is the runbook step INV-14 promises, executed against a real backend.
func TestS3StoreDeleteIsAVersionedDeleteMarker(t *testing.T) {
	ctx := context.Background()
	be := backendConfig(t)
	store := newVersionedS3Store(t, be, "gc-restore")
	const key = "wal/v/1/1-1-acked.wal"
	payload := []byte("bytes an Agent ACKed to the guest")

	if _, err := store.Put(ctx, key, payload, objectstore.PutOptions{IfNoneMatch: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, key); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("a marked object must read as absent: %v", err)
	}

	// The bytes are still there, behind a delete marker — not destroyed.
	versions, err := be.Client().ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(store.Bucket()), Prefix: aws.String(key),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(versions.Versions) != 1 {
		t.Fatalf("%d retained versions, want 1 — the GC destroyed the object", len(versions.Versions))
	}
	if len(versions.DeleteMarkers) != 1 {
		t.Fatalf("%d delete markers, want 1", len(versions.DeleteMarkers))
	}

	if err := store.Restore(ctx, key); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := store.Get(ctx, key)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("restored body = %q err=%v, want the ACKed bytes", got, err)
	}
}

// Finding 11. The fencing race on real S3 answers 409 ConditionalRequestConflict as
// well as 412, and epoch.CompareAndAdvance branches on the sentinel. Race conditional
// PUTs *through S3Store* — not through a raw client — so the mapping is what is under
// test, not the backend.
func TestConditionalWritesThroughS3StoreMapEveryLoser(t *testing.T) {
	ctx := context.Background()
	be := backendConfig(t)
	store := newVersionedS3Store(t, be, "conditional-through-store")

	res, err := store.Put(ctx, "volumes/v/epoch", []byte(`{"epoch":1}`), objectstore.PutOptions{IfNoneMatch: true})
	if err != nil {
		t.Fatal(err)
	}

	const contenders = 16
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		winners int
		losers  []error
		start   = make(chan struct{})
	)
	for i := range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := store.Put(ctx, "volumes/v/epoch", fmt.Appendf(nil, `{"epoch":%d}`, i+2),
				objectstore.PutOptions{IfMatch: res.ETag})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				winners++
				return
			}
			losers = append(losers, err)
		}()
	}
	close(start)
	wg.Wait()

	if winners != 1 {
		t.Fatalf("%d promoters won the same CAS, want exactly 1", winners)
	}
	for _, err := range losers {
		if !errors.Is(err, objectstore.ErrPreconditionFailed) {
			t.Fatalf("a fenced promoter got %v, want ErrPreconditionFailed — the 'I was fenced' branch is never taken", err)
		}
	}
}

// Finding 3. Every proof about the §14.5 lost-response path — the single most common
// S3 failure — was written against the sim, whose ETag is a SHA-256. S3's is a quoted
// MD5, and a multipart object's is an MD5-of-MD5s with a `-N` suffix. Run the real
// uploader against the real backend so "idempotent retry" is a fact about the thing
// production uses, not about the simulator.
func TestWALUploaderIsIdempotentAgainstARealBackend(t *testing.T) {
	ctx := context.Background()
	be := backendConfig(t)
	store := newVersionedS3Store(t, be, "wal-uploader")

	var vol [16]byte
	vol[6], vol[8] = 0x70, 0x80
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())

	batch := func(payload []byte, records int) *wal.ClosedBatch {
		t.Helper()
		b := wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig())
		for seq := 1; seq <= records; seq++ {
			enc, err := wal.Record{Sequence: uint64(seq), Epoch: 1, Payload: payload}.Encode()
			if err != nil {
				t.Fatal(err)
			}
			b.Append(uint64(seq), enc, false)
		}
		b.Flush()
		return b.Pending()[0]
	}

	up := wal.NewUploader(store, 3)

	tests := []struct {
		name    string
		payload []byte
		records int
	}{
		{"a small batch", []byte("a short encrypted record"), 4},
		// Above the SDK's 5 MiB multipart threshold, where the ETag stops even
		// pretending to be a content hash.
		{"a batch past the multipart threshold", bytes.Repeat([]byte("payload-"), 1<<19), 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cb := batch(tc.payload, tc.records)
			key, err := up.Upload(ctx, cb)
			if err != nil {
				t.Fatalf("first upload: %v", err)
			}
			// The §14.5 case: the PUT persisted, the response was lost, the Agent
			// retries the identical batch. It must reconcile to success.
			again, err := up.Upload(ctx, cb)
			if err != nil {
				t.Fatalf("retry after a lost response must be an idempotent success, got %v", err)
			}
			if again != key {
				t.Fatalf("retry returned key %q, want %q", again, key)
			}

			// Different bytes at the same deterministic key is corruption, and must
			// stay a hard fail rather than be reconciled away.
			if _, err := store.Put(ctx, key+".divergent", []byte("not the batch"), objectstore.PutOptions{IfNoneMatch: true}); err != nil {
				t.Fatal(err)
			}
			other := batch(append([]byte("different-"), tc.payload...), tc.records)
			if _, err := store.Put(ctx, other.Object().Key, []byte("someone else's bytes"), objectstore.PutOptions{IfNoneMatch: true}); err != nil {
				t.Fatal(err)
			}
			if _, err := up.Upload(ctx, other); !errors.Is(err, wal.ErrDivergentObject) {
				t.Fatalf("divergent object at a deterministic key: %v, want ErrDivergentObject", err)
			}
		})
	}
}
