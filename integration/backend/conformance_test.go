//go:build integration

// Package backend_test is the object-store conformance suite of §6.1/§25.4:
// blocking for every backend version we enable. It asserts the exact S3 semantics
// the protocol is built on, against a real backend in a container — not against
// documentation, and not against our own simulator.
//
// Run with: task backend:conformance
package backend_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/spin-stack/storage/internal/testinfra"
)

// backendUnderTest starts the pinned backend and returns a client for it.
func backendUnderTest(t *testing.T) (*s3.Client, context.Context) {
	t.Helper()
	return backendConfig(t).Client(), context.Background()
}

// backendConfig starts the pinned backend and returns its connection details, for
// tests that need to build their own client (different SDK options).
func backendConfig(t *testing.T) testinfra.ObjectStoreBackend {
	t.Helper()
	return testinfra.RustFS(t, os.Getenv("RUSTFS_IMAGE"))
}

// apiErrorCode returns the S3 error code of err ("PreconditionFailed", ...).
func apiErrorCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

func makeBucket(t *testing.T, ctx context.Context, c *s3.Client, name string, objectLock bool) {
	t.Helper()
	in := &s3.CreateBucketInput{Bucket: aws.String(name)}
	if objectLock {
		in.ObjectLockEnabledForBucket = aws.Bool(true)
	}
	if _, err := c.CreateBucket(ctx, in); err != nil {
		t.Fatalf("create bucket %s: %v", name, err)
	}
}

func put(ctx context.Context, c *s3.Client, bucket, key, body string, mut func(*s3.PutObjectInput)) (*s3.PutObjectOutput, error) {
	in := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(body),
	}
	if mut != nil {
		mut(in)
	}
	return c.PutObject(ctx, in)
}

// TestCreateOnlyPutIsAtomic is §14.5 + INV-21: every durable publication (WAL
// object, manifest, checkpoint, recovery-point) is written create-only, so a
// retried PUT after a lost response cannot overwrite what is already there.
func TestCreateOnlyPutIsAtomic(t *testing.T) {
	c, ctx := backendUnderTest(t)
	makeBucket(t, ctx, c, "create-only", false)

	first, err := put(ctx, c, "create-only", "wal/1.wal", "first", func(in *s3.PutObjectInput) {
		in.IfNoneMatch = aws.String("*")
	})
	if err != nil {
		t.Fatalf("create-only PUT of a new key must succeed: %v", err)
	}
	if aws.ToString(first.ETag) == "" {
		t.Fatal("PUT returned no ETag; the CAS protocol (§12.4) needs one")
	}

	_, err = put(ctx, c, "create-only", "wal/1.wal", "second", func(in *s3.PutObjectInput) {
		in.IfNoneMatch = aws.String("*")
	})
	if code := apiErrorCode(err); code != "PreconditionFailed" {
		t.Fatalf("create-only PUT over an existing key must fail with PreconditionFailed, got %q (err=%v)", code, err)
	}

	// And the original bytes are untouched — the failed write is not partial.
	got, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("create-only"), Key: aws.String("wal/1.wal")})
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	body, _ := io.ReadAll(got.Body)
	if string(body) != "first" {
		t.Fatalf("object body = %q, want the original %q", body, "first")
	}
}

// TestCompareAndSwapOnETag is §12.4: the epoch object is advanced with If-Match,
// so two Control Planes racing to promote cannot both win.
func TestCompareAndSwapOnETag(t *testing.T) {
	c, ctx := backendUnderTest(t)
	makeBucket(t, ctx, c, "cas", false)

	created, err := put(ctx, c, "cas", "volumes/v1/epoch", `{"epoch":1}`, nil)
	if err != nil {
		t.Fatal(err)
	}
	etag := aws.ToString(created.ETag)

	// The holder of the current ETag advances the epoch.
	advanced, err := put(ctx, c, "cas", "volumes/v1/epoch", `{"epoch":2}`, func(in *s3.PutObjectInput) {
		in.IfMatch = aws.String(etag)
	})
	if err != nil {
		t.Fatalf("CAS with the current ETag must succeed: %v", err)
	}
	if aws.ToString(advanced.ETag) == etag {
		t.Fatal("a successful CAS must change the ETag, or the next CAS cannot be ordered")
	}

	// The loser of the race still holds the old ETag and must be refused.
	_, err = put(ctx, c, "cas", "volumes/v1/epoch", `{"epoch":99}`, func(in *s3.PutObjectInput) {
		in.IfMatch = aws.String(etag)
	})
	if code := apiErrorCode(err); code != "PreconditionFailed" {
		t.Fatalf("CAS with a stale ETag must fail with PreconditionFailed, got %q (err=%v)", code, err)
	}

	head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("cas"), Key: aws.String("volumes/v1/epoch")})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(head.ETag) != aws.ToString(advanced.ETag) {
		t.Fatal("the losing CAS changed the object")
	}
}

// TestHeadResolvesALostPutResponse is the §23 "PUT succeeded, response lost" case:
// the client cannot tell whether its write landed, so it HEADs the key. The backend
// must answer with the stored object's identity so the retry can be resolved
// without a double effect (INV-21).
func TestHeadResolvesALostPutResponse(t *testing.T) {
	c, ctx := backendUnderTest(t)
	makeBucket(t, ctx, c, "lost-response", false)

	const body = "batch-bytes"
	written, err := put(ctx, c, "lost-response", "wal/7.wal", body, func(in *s3.PutObjectInput) {
		in.IfNoneMatch = aws.String("*")
	})
	if err != nil {
		t.Fatal(err)
	}

	// Pretend the response never arrived: HEAD tells us the object is there and
	// which bytes it holds.
	head, err := c.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("lost-response"), Key: aws.String("wal/7.wal"),
	})
	if err != nil {
		t.Fatalf("HEAD after a lost PUT response must find the object: %v", err)
	}
	if aws.ToString(head.ETag) != aws.ToString(written.ETag) {
		t.Fatalf("HEAD ETag %q != PUT ETag %q", aws.ToString(head.ETag), aws.ToString(written.ETag))
	}
	if head.ContentLength == nil || *head.ContentLength != int64(len(body)) {
		t.Fatalf("HEAD ContentLength = %v, want %d", head.ContentLength, len(body))
	}

	// The blind retry is refused rather than duplicating the write.
	_, err = put(ctx, c, "lost-response", "wal/7.wal", body, func(in *s3.PutObjectInput) {
		in.IfNoneMatch = aws.String("*")
	})
	if code := apiErrorCode(err); code != "PreconditionFailed" {
		t.Fatalf("the retry must be refused, got %q", code)
	}
}

// TestVersioningKeepsDeletedObjectsRecoverable is what INV-14 stands on: the GC
// only marks, and a mark is a delete marker over a version that still exists.
func TestVersioningKeepsDeletedObjectsRecoverable(t *testing.T) {
	c, ctx := backendUnderTest(t)
	makeBucket(t, ctx, c, "versioned", false)

	if _, err := c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String("versioned"),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatalf("versioning must be supported (§6.1): %v", err)
	}
	status, err := c.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String("versioned")})
	if err != nil || status.Status != types.BucketVersioningStatusEnabled {
		t.Fatalf("versioning status = %v err=%v", status.Status, err)
	}

	if _, err := put(ctx, c, "versioned", "orphan.wal", "payload", nil); err != nil {
		t.Fatal(err)
	}
	del, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("versioned"), Key: aws.String("orphan.wal"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !aws.ToBool(del.DeleteMarker) {
		t.Fatal("a delete on a versioned bucket must create a delete marker, not remove data (§21.3)")
	}

	versions, err := c.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String("versioned"), Prefix: aws.String("orphan.wal"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(versions.Versions) != 1 || len(versions.DeleteMarkers) != 1 {
		t.Fatalf("want 1 version + 1 delete marker, got %d/%d", len(versions.Versions), len(versions.DeleteMarkers))
	}

	// Reversible: the object's bytes are still readable by version id.
	restored, err := c.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("versioned"), Key: aws.String("orphan.wal"),
		VersionId: versions.Versions[0].VersionId,
	})
	if err != nil {
		t.Fatalf("a marked object must still be recoverable: %v", err)
	}
	defer restored.Body.Close()
	if body, _ := io.ReadAll(restored.Body); string(body) != "payload" {
		t.Fatalf("recovered body = %q", body)
	}
}

// TestObjectLockRefusesPermanentDeletion is the structural half of INV-14: even a
// caller that asks for a permanent delete cannot destroy data under retention.
func TestObjectLockRefusesPermanentDeletion(t *testing.T) {
	c, ctx := backendUnderTest(t)
	makeBucket(t, ctx, c, "locked", true)

	cfg, err := c.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{Bucket: aws.String("locked")})
	if err != nil || cfg.ObjectLockConfiguration == nil ||
		cfg.ObjectLockConfiguration.ObjectLockEnabled != types.ObjectLockEnabledEnabled {
		t.Fatalf("Object Lock must be supported (§6.1, §10): cfg=%v err=%v", cfg, err)
	}

	if _, err := put(ctx, c, "locked", "manifest.json", "immutable", nil); err != nil {
		t.Fatal(err)
	}
	versions, err := c.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String("locked"), Prefix: aws.String("manifest.json"),
	})
	if err != nil || len(versions.Versions) == 0 {
		t.Fatalf("list versions: %v", err)
	}
	versionID := versions.Versions[0].VersionId

	retainUntil := mustTime("2030-01-01T00:00:00Z")
	if _, err := c.PutObjectRetention(ctx, &s3.PutObjectRetentionInput{
		Bucket: aws.String("locked"), Key: aws.String("manifest.json"),
		Retention: &types.ObjectLockRetention{
			Mode: types.ObjectLockRetentionModeGovernance, RetainUntilDate: aws.Time(retainUntil),
		},
	}); err != nil {
		t.Fatalf("GOVERNANCE retention must be settable: %v", err)
	}

	// The permanent delete of that version must be refused — this is the property
	// the GC's credentials rely on, enforced by the backend rather than by us.
	_, err = c.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("locked"), Key: aws.String("manifest.json"), VersionId: versionID,
	})
	if err == nil {
		t.Fatal("a version under GOVERNANCE retention must not be permanently deletable")
	}
	if code := apiErrorCode(err); code != "AccessDenied" {
		t.Logf("note: refusal code is %q (AccessDenied on this backend); the requirement is only that it is refused", code)
	}
}

// TestListSeesAFreshPut covers the §22.1 recovery scan: the durable point is the
// end of the contiguous prefix found by LIST, so a just-uploaded object must be
// listable. (Recovery also tolerates lag by design; this records what the backend
// actually offers, which is read-after-write here.)
func TestListSeesAFreshPut(t *testing.T) {
	c, ctx := backendUnderTest(t)
	makeBucket(t, ctx, c, "listing", false)

	for _, key := range []string{"wal/v/1/1-1.wal", "wal/v/1/2-2.wal", "wal/v/1/3-3.wal"} {
		if _, err := put(ctx, c, "listing", key, key, nil); err != nil {
			t.Fatal(err)
		}
	}
	out, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("listing"), Prefix: aws.String("wal/v/1/"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Contents) != 3 {
		t.Fatalf("LIST right after PUT returned %d objects, want 3", len(out.Contents))
	}
	// Keys come back sorted, which is what the contiguous-prefix scan assumes.
	var keys []string
	for _, o := range out.Contents {
		keys = append(keys, aws.ToString(o.Key))
	}
	if !sortedAscending(keys) {
		t.Fatalf("LIST must return keys in ascending order, got %v", keys)
	}
}

// TestGetOfAMissingKeyIsNotFound: recovery distinguishes "gap" from "error".
func TestGetOfAMissingKeyIsNotFound(t *testing.T) {
	c, ctx := backendUnderTest(t)
	makeBucket(t, ctx, c, "missing", false)

	_, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("missing"), Key: aws.String("nope")})
	if code := apiErrorCode(err); code != "NoSuchKey" {
		t.Fatalf("GET of a missing key must be NoSuchKey, got %q (err=%v)", code, err)
	}
	_, err = c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("missing"), Key: aws.String("nope")})
	if code := apiErrorCode(err); code != "NotFound" {
		t.Fatalf("HEAD of a missing key must be NotFound, got %q (err=%v)", code, err)
	}
}

// TestChecksumRoundTrip: the uploader stores a payload digest in the key (§14.2);
// the backend must return byte-identical content for the divergence check to mean
// anything.
func TestChecksumRoundTrip(t *testing.T) {
	c, ctx := backendUnderTest(t)
	makeBucket(t, ctx, c, "checksum", false)

	payload := bytes.Repeat([]byte{0xAB, 0x00, 0xFF, 0x7F}, 4096) // 16 KiB, binary
	if _, err := c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("checksum"), Key: aws.String("wal/bin.wal"), Body: bytes.NewReader(payload),
	}); err != nil {
		t.Fatal(err)
	}
	got, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("checksum"), Key: aws.String("wal/bin.wal")})
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	back, _ := io.ReadAll(got.Body)
	if !bytes.Equal(back, payload) {
		t.Fatalf("round trip changed %d bytes", len(payload))
	}
}

func sortedAscending(s []string) bool {
	for i := 1; i < len(s); i++ {
		if s[i-1] > s[i] {
			return false
		}
	}
	return true
}

func mustTime(rfc3339 string) time.Time {
	ts, err := time.Parse(time.RFC3339, rfc3339)
	if err != nil {
		panic(err)
	}
	return ts
}
