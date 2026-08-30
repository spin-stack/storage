//go:build integration

package backend_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/real"
	"github.com/spin-stack/storage/internal/testinfra"
)

// makeVersionedBucket creates a bucket with versioning Enabled, the way §10 requires
// every production bucket to be.
func makeVersionedBucket(t *testing.T, ctx context.Context, be testinfra.ObjectStoreBackend, name string) string {
	t.Helper()
	bucket := be.MakeBucket(t, ctx, name, false)
	if _, err := be.Client().PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatalf("enable versioning on %s: %v", bucket, err)
	}
	return bucket
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
		name string
		// setup returns the bucket to point the store at, created or not.
		setup func(t *testing.T) string
	}{
		{
			name:  "versioning never enabled",
			setup: func(t *testing.T) string { return be.MakeBucket(t, ctx, "unversioned-bucket", false) },
		},
		{
			name: "versioning suspended by an operator",
			setup: func(t *testing.T) string {
				b := makeVersionedBucket(t, ctx, be, "suspended-bucket")
				if _, err := c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
					Bucket:                  aws.String(b),
					VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusSuspended},
				}); err != nil {
					t.Fatalf("suspend versioning: %v", err)
				}
				return b
			},
		},
		{
			name:  "bucket does not exist",
			setup: func(*testing.T) string { return be.Bucket("no-such-bucket-at-all") },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bucket := tc.setup(t)
			_, err := real.NewS3Store(ctx, real.S3Config{
				Bucket: bucket, Endpoint: be.Endpoint, Region: be.Region,
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
