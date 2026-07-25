//go:build integration

// Edge cases of the object-store backend. The conformance suite (§6.1) asserts the
// semantics the protocol *requires*; this file probes the places where an
// S3-compatible backend and the official AWS SDK are known to disagree, so we find
// them now instead of during a recovery.
package backend_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestCreateOnlyIsExclusiveUnderConcurrency: the create-only PUT is what makes a
// duplicated WAL upload harmless (INV-21). Simulated concurrency proves nothing
// about the backend — this races real clients at one key.
func TestCreateOnlyIsExclusiveUnderConcurrency(t *testing.T) {
	c, ctx := backendUnderTest(t)
	makeBucket(t, ctx, c, "race-create", false)

	const writers = 16
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		winners int
		codes   []string
	)
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := put(ctx, c, "race-create", "wal/contended.wal", fmt.Sprintf("writer-%d", i), func(in *s3.PutObjectInput) {
				in.IfNoneMatch = aws.String("*")
			})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				winners++
				return
			}
			codes = append(codes, apiErrorCode(err))
		}()
	}
	wg.Wait()

	if winners != 1 {
		t.Fatalf("create-only PUT admitted %d concurrent writers, want exactly 1", winners)
	}
	for _, code := range codes {
		if code != "PreconditionFailed" {
			t.Fatalf("a loser got %q, want PreconditionFailed", code)
		}
	}
}

// TestCASIsExclusiveUnderConcurrency is the §12.4 fence under a real race: many
// Control Planes holding the same ETag, one advance.
func TestCASIsExclusiveUnderConcurrency(t *testing.T) {
	c, ctx := backendUnderTest(t)
	makeBucket(t, ctx, c, "race-cas", false)

	created, err := put(ctx, c, "race-cas", "epoch", `{"epoch":1}`, nil)
	if err != nil {
		t.Fatal(err)
	}
	etag := aws.ToString(created.ETag)

	const contenders = 16
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		winners int
	)
	for i := range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := put(ctx, c, "race-cas", "epoch", fmt.Sprintf(`{"epoch":%d}`, i+2), func(in *s3.PutObjectInput) {
				in.IfMatch = aws.String(etag)
			})
			if err == nil {
				mu.Lock()
				winners++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if winners != 1 {
		t.Fatalf("%d concurrent CAS operations succeeded on the same ETag, want exactly 1", winners)
	}
}

// TestCreateOnlyOnAVersionedBucket is the trap that would have bitten us in
// production: every production bucket has versioning enabled (§10), and on a
// versioned bucket "create-only" has to keep meaning "fail if any live version
// exists" — otherwise a retried upload silently stacks a second version and the
// idempotency argument of INV-21 collapses.
func TestCreateOnlyOnAVersionedBucket(t *testing.T) {
	c, ctx := backendUnderTest(t)
	makeBucket(t, ctx, c, "versioned-create-only", false)
	if _, err := c.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String("versioned-create-only"),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := put(ctx, c, "versioned-create-only", "manifest.json", "v1", func(in *s3.PutObjectInput) {
		in.IfNoneMatch = aws.String("*")
	}); err != nil {
		t.Fatal(err)
	}
	_, err := put(ctx, c, "versioned-create-only", "manifest.json", "v2", func(in *s3.PutObjectInput) {
		in.IfNoneMatch = aws.String("*")
	})
	if code := apiErrorCode(err); code != "PreconditionFailed" {
		t.Fatalf("create-only on a versioned bucket must still refuse, got %q (err=%v)", code, err)
	}

	versions, err := c.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String("versioned-create-only"), Prefix: aws.String("manifest.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(versions.Versions) != 1 {
		t.Fatalf("the refused PUT created %d versions, want 1 — an immutable manifest must not stack versions",
			len(versions.Versions))
	}
}

// TestConditionalWriteAgainstAMissingKey records how the backend answers a CAS on a
// key that does not exist. The epoch store has to tell "not initialised yet" from
// "someone else moved it", and the two cases have different codes.
func TestConditionalWriteAgainstAMissingKey(t *testing.T) {
	c, ctx := backendUnderTest(t)
	makeBucket(t, ctx, c, "cas-missing", false)

	_, err := put(ctx, c, "cas-missing", "absent", "x", func(in *s3.PutObjectInput) {
		in.IfMatch = aws.String(`"00000000000000000000000000000000"`)
	})
	if err == nil {
		t.Fatal("If-Match against a missing key must fail")
	}
	code := apiErrorCode(err)
	if code != "NoSuchKey" && code != "PreconditionFailed" && code != "NotFound" {
		t.Fatalf("unexpected code %q for If-Match on a missing key (err=%v)", code, err)
	}
	t.Logf("backend answers If-Match on a missing key with %q — epoch.CompareAndAdvance must treat this as 'not initialised'", code)
}

// TestListPaginatesBeyondOneThousand: recovery derives the durable point from a
// LIST of a WAL prefix (§22.1). S3 caps a page at 1000 keys, so a volume with more
// objects than that is exactly where a naive scan silently truncates the "contiguous
// prefix" and declares data lost.
func TestListPaginatesBeyondOneThousand(t *testing.T) {
	c, ctx := backendUnderTest(t)
	makeBucket(t, ctx, c, "pagination", false)

	const objects = 1100
	var wg sync.WaitGroup
	sem := make(chan struct{}, 32)
	for i := range objects {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			key := fmt.Sprintf("wal/v/1/%06d.wal", i)
			if _, err := put(ctx, c, "pagination", key, key, nil); err != nil {
				t.Errorf("put %s: %v", key, err)
			}
		}()
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}

	// One page is capped...
	first, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("pagination"), Prefix: aws.String("wal/v/1/"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Contents) >= objects {
		t.Logf("note: backend returned %d keys in one page (no 1000 cap)", len(first.Contents))
	}
	if !aws.ToBool(first.IsTruncated) && len(first.Contents) != objects {
		t.Fatalf("page reports not-truncated with %d of %d keys — a scan would silently lose data",
			len(first.Contents), objects)
	}

	// ...and the paginator sees every key, in order.
	var (
		seen  int
		last  string
		pager = s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{
			Bucket: aws.String("pagination"), Prefix: aws.String("wal/v/1/"),
		})
	)
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range page.Contents {
			key := aws.ToString(o.Key)
			if key <= last {
				t.Fatalf("keys out of order across pages: %q after %q", key, last)
			}
			last = key
			seen++
		}
	}
	if seen != objects {
		t.Fatalf("paginated LIST saw %d keys, want %d", seen, objects)
	}
}

// TestMultipartUploadAndItsETag: a checkpoint can exceed the SDK's multipart
// threshold. A multipart object's ETag is not a content MD5 (it carries a -N
// suffix), so any code that treats an ETag as a checksum breaks here — and CAS must
// still work on such an object.
func TestMultipartUploadAndItsETag(t *testing.T) {
	c, ctx := backendUnderTest(t)
	makeBucket(t, ctx, c, "multipart", false)

	// 12 MiB with a 5 MiB part size → 3 parts.
	payload := bytes.Repeat([]byte("checkpoint-segment-"), 12*1024*1024/19)
	uploader := manager.NewUploader(c, func(u *manager.Uploader) {
		u.PartSize = 5 * 1024 * 1024
		u.Concurrency = 3
	})
	out, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String("multipart"), Key: aws.String("checkpoints/big.json"),
		Body: bytes.NewReader(payload),
	})
	if err != nil {
		t.Fatalf("multipart upload: %v", err)
	}
	etag := aws.ToString(out.ETag)
	t.Logf("multipart ETag: %s", etag)

	got, err := c.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("multipart"), Key: aws.String("checkpoints/big.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	back, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(back, payload) {
		t.Fatalf("multipart round trip returned %d bytes, want %d (and equal)", len(back), len(payload))
	}

	// CAS on a multipart object still behaves.
	if _, err := put(ctx, c, "multipart", "checkpoints/big.json", "replaced", func(in *s3.PutObjectInput) {
		in.IfMatch = aws.String(etag)
	}); err != nil {
		t.Fatalf("CAS with a multipart ETag must work: %v", err)
	}
}

// TestRangeGet is what lazy loading (§22.4) and partial checkpoint reads need.
func TestRangeGet(t *testing.T) {
	c, ctx := backendUnderTest(t)
	makeBucket(t, ctx, c, "ranges", false)

	body := strings.Repeat("0123456789", 1000) // 10 000 bytes
	if _, err := put(ctx, c, "ranges", "seg", body, nil); err != nil {
		t.Fatal(err)
	}
	out, err := c.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("ranges"), Key: aws.String("seg"), Range: aws.String("bytes=100-199"),
	})
	if err != nil {
		t.Fatalf("range GET: %v", err)
	}
	defer out.Body.Close()
	chunk, _ := io.ReadAll(out.Body)
	if len(chunk) != 100 || string(chunk) != body[100:200] {
		t.Fatalf("range GET returned %d bytes (%q…)", len(chunk), string(chunk[:min(len(chunk), 16)]))
	}
	if out.ContentRange == nil {
		t.Fatal("range GET must report Content-Range")
	}
}

// TestZeroByteObject: a summary or manifest can legitimately be empty; an empty
// object must round-trip rather than 404.
func TestZeroByteObject(t *testing.T) {
	c, ctx := backendUnderTest(t)
	makeBucket(t, ctx, c, "empty", false)

	if _, err := put(ctx, c, "empty", "zero", "", func(in *s3.PutObjectInput) {
		in.IfNoneMatch = aws.String("*")
	}); err != nil {
		t.Fatalf("zero-byte create-only PUT: %v", err)
	}
	head, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("empty"), Key: aws.String("zero")})
	if err != nil {
		t.Fatalf("HEAD of a zero-byte object: %v", err)
	}
	if head.ContentLength == nil || *head.ContentLength != 0 {
		t.Fatalf("ContentLength = %v, want 0", head.ContentLength)
	}
}

// TestDeleteOfAMissingKeyIsNotAnError matches S3: the GC's mark must be idempotent
// (§21.3) — re-marking an object it already marked cannot become a hard failure.
func TestDeleteOfAMissingKeyIsNotAnError(t *testing.T) {
	c, ctx := backendUnderTest(t)
	makeBucket(t, ctx, c, "idempotent-delete", false)

	if _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("idempotent-delete"), Key: aws.String("never-existed"),
	}); err != nil {
		t.Fatalf("DELETE of a missing key must succeed (S3 semantics), got %v", err)
	}
}

// TestSDKChecksumModesBothWork is the client-choice question: aws-sdk-go-v2 sends
// CRC checksums (and aws-chunked trailers) by default, which is the classic source
// of breakage against S3-compatible backends. Both settings must work here, so the
// knob exists if a future backend version needs it.
func TestSDKChecksumModesBothWork(t *testing.T) {
	be := backendConfig(t)

	tests := []struct {
		name string
		mode aws.RequestChecksumCalculation
	}{
		{"when_supported (SDK default)", aws.RequestChecksumCalculationWhenSupported},
		{"when_required", aws.RequestChecksumCalculationWhenRequired},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			client := be.ClientWith(func(o *s3.Options) {
				o.RequestChecksumCalculation = tc.mode
			})
			bucket := fmt.Sprintf("checksum-mode-%d", i)
			makeBucket(t, ctx, client, bucket, false)

			if _, err := put(ctx, client, bucket, "obj", "payload", func(in *s3.PutObjectInput) {
				in.IfNoneMatch = aws.String("*")
			}); err != nil {
				t.Fatalf("PUT with checksum mode %v: %v", tc.mode, err)
			}
			got, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String("obj")})
			if err != nil {
				t.Fatal(err)
			}
			defer got.Body.Close()
			if body, _ := io.ReadAll(got.Body); string(body) != "payload" {
				t.Fatalf("body = %q", body)
			}
		})
	}
}
