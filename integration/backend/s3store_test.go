//go:build integration

package backend_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/image"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/real"
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
	ctx := t.Context()
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
	ctx := t.Context()
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
	ctx := t.Context()
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

// TestTheImageIsIdempotentAgainstARealBackend is §6.1 for the format that actually
// leaves the host now.
//
// It replaced the WAL uploader's version, which went with the uploader (ADR-0026
// increment 4.5). The properties did not change and they are properties of the
// *backend*, not of the producer: identical bytes at a deterministic key reconcile to
// success when a response is lost, and If-Match must actually fence a stale writer.
//
// The producer is internal/image because that is what production writes. A conformance
// suite exercising a byte source production no longer uses would certify the wrong thing.
func TestTheImageIsIdempotentAgainstARealBackend(t *testing.T) {
	ctx := t.Context()
	be := backendConfig(t)
	store := newVersionedS3Store(t, be, "image")

	tests := []struct {
		name    string
		tag     byte
		payload []byte
	}{
		{"a small image", 0x01, []byte("a short guest extent")},
		// Above the SDK's 5 MiB multipart threshold, where the ETag stops even
		// pretending to be a content hash.
		{"an image past the multipart threshold", 0x02, bytes.Repeat([]byte("payload-"), 1<<19)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var vol [16]byte
			vol[6], vol[8] = 0x70, 0x80
			vol[15] = tc.tag

			view := cow.NewIntervalMap()
			view.Overwrite(0, tc.payload)

			etag, err := image.Publish(ctx, store, rand.Reader, nil, image.OwnLineage(vol), view, nil, 1, "")
			if err != nil {
				t.Fatalf("first publish: %v", err)
			}
			// Republishing an unchanged view must reconcile rather than fail: the chunks
			// are already there under their own digests.
			if _, err := image.Publish(ctx, store, rand.Reader, nil, image.OwnLineage(vol), view, nil, 2, etag); err != nil {
				t.Fatalf("republishing an unchanged image must succeed, got %v", err)
			}
			loaded, _, _, err := image.Load(ctx, store, nil, image.OwnLineage(vol), nil)
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			got := make([]byte, len(tc.payload))
			loaded.Read(0, got)
			if !bytes.Equal(got, tc.payload) {
				t.Fatal("the image did not survive a real backend round trip")
			}
			// The fence: a stale ETag is refused rather than silently replacing the
			// manifest. On a real backend this is the whole of V1's fencing.
			if _, err := image.Publish(ctx, store, rand.Reader, nil, image.OwnLineage(vol), view, nil, 3, etag); !errors.Is(err, image.ErrSuperseded) {
				t.Fatalf("a stale ETag published against a real backend: %v, want ErrSuperseded", err)
			}
		})
	}
}
