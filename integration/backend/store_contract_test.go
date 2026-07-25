//go:build integration

package backend_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/objectstore/storetest"
	"github.com/spin-stack/storage/internal/simio/real"
)

// TestS3StoreSatisfiesTheContract runs the *same* contract the sim and the
// filesystem store run in the unit lane, against the S3-backed wrapper talking to a
// real backend. This is what makes the wrapper substitutable: if S3's semantics and
// our sim's ever diverge, the DST proofs would be reasoning about a store that does
// not exist, and this is where that shows up.
func TestS3StoreSatisfiesTheContract(t *testing.T) {
	be := backendConfig(t)
	admin := be.Client()
	ctx := context.Background()

	var n int
	storetest.RunContract(t, func(t *testing.T) objectstore.Store {
		// A fresh bucket per subtest: no shared state between scenarios.
		n++
		bucket := fmt.Sprintf("contract-%d", n)
		if _, err := admin.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("create bucket: %v", err)
		}
		store, err := real.NewS3Store(real.S3Config{
			Bucket:    bucket,
			Endpoint:  be.Endpoint,
			Region:    be.Region,
			AccessKey: be.AccessKey,
			SecretKey: be.SecretKey,
		})
		if err != nil {
			t.Fatalf("new s3 store: %v", err)
		}
		return store
	})
}

// TestS3StoreListPaginates: the wrapper must hide S3's 1000-key page cap, because
// recovery derives the durable point from a full prefix listing (§22.1). A store
// that returned one page would silently shorten the contiguous prefix.
func TestS3StoreListPaginates(t *testing.T) {
	be := backendConfig(t)
	ctx := context.Background()
	bucket := "contract-pagination"
	if _, err := be.Client().CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	store, err := real.NewS3Store(real.S3Config{
		Bucket: bucket, Endpoint: be.Endpoint, Region: be.Region,
		AccessKey: be.AccessKey, SecretKey: be.SecretKey,
	})
	if err != nil {
		t.Fatal(err)
	}

	const objects = 1100
	for i := range objects {
		key := fmt.Sprintf("wal/v/1/%06d.wal", i)
		if _, err := store.Put(ctx, key, []byte(key), objectstore.PutOptions{}); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	got, err := store.List(ctx, "wal/v/1/")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != objects {
		t.Fatalf("List returned %d objects, want %d — the wrapper is not paginating", len(got), objects)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1].Key >= got[i].Key {
			t.Fatalf("List is not sorted across pages: %q then %q", got[i-1].Key, got[i].Key)
		}
	}
}

// TestS3StoreChecksumWhenRequiredAlsoWorks proves the escape hatch in S3Config is
// real: a backend version that rejects the SDK's default CRC trailers can be served
// by flipping one field, with no other code change.
func TestS3StoreChecksumWhenRequiredAlsoWorks(t *testing.T) {
	be := backendConfig(t)
	ctx := context.Background()
	bucket := "contract-checksum-required"
	if _, err := be.Client().CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	store, err := real.NewS3Store(real.S3Config{
		Bucket: bucket, Endpoint: be.Endpoint, Region: be.Region,
		AccessKey: be.AccessKey, SecretKey: be.SecretKey,
		ChecksumWhenRequired: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "k", []byte("payload"), objectstore.PutOptions{IfNoneMatch: true}); err != nil {
		t.Fatalf("put with checksums only when required: %v", err)
	}
	if got, err := store.Get(ctx, "k"); err != nil || string(got) != "payload" {
		t.Fatalf("get = %q err=%v", got, err)
	}
}
