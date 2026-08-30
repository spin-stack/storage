//go:build integration || e2e

package testinfra

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/spin-stack/storage/internal/ids"
)

// BackendEnv selects which backend the conformance suite certifies this run.
// Unset means the pinned container: the suite has to stay runnable on a laptop
// with no cloud account, because a gate nobody can run locally stops being run.
const BackendEnv = "SPIN_CONFORMANCE_BACKEND"

// bucketPrefix names every bucket a conformance run creates, so `task s3:aws:sweep`
// can find what an interrupted run left behind. Buckets are global and permanent
// until deleted; an unrecognizable leftover is one nobody dares remove.
const bucketPrefix = "spin-conf-"

// ObjectStore returns the backend under test: the pinned container by default,
// real AWS S3 when BackendEnv says so.
//
// The suite runs against both because they answer differently on exactly the
// questions the protocol rests on — 409 vs 412 for a lost conditional write is the
// one that already cost us a finding. A container that agrees with the
// documentation proves the documentation, not S3.
func ObjectStore(t *testing.T) ObjectStoreBackend {
	t.Helper()
	switch os.Getenv(BackendEnv) {
	case "", "rustfs":
		return RustFS(t, os.Getenv("RUSTFS_IMAGE"))
	case "aws":
		return AWS(t)
	default:
		t.Fatalf("%s=%q: want \"rustfs\" or \"aws\"", BackendEnv, os.Getenv(BackendEnv))
		return ObjectStoreBackend{}
	}
}

// AWS returns real S3, addressed through the ambient credential chain (env,
// config file, instance role) — the suite never carries a key of its own.
//
// Region comes from that same chain: buckets are created in whatever region the
// operator is signed in to, so the run certifies the region they actually deploy
// to rather than one this file picked.
func AWS(t *testing.T) ObjectStoreBackend {
	t.Helper()
	// Not t.Context(): buckets are purged from t.Cleanup, which runs after the
	// test context is cancelled — a cancelled context there leaks real buckets.
	ctx := context.Background() //nolint:usetesting // see above

	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		t.Fatalf("no AWS credentials for %s=aws: %v", BackendEnv, err)
	}
	if cfg.Region == "" {
		t.Fatalf("no AWS region: set AWS_REGION or a region in ~/.aws/config")
	}
	return ObjectStoreBackend{
		Region:    cfg.Region,
		namespace: runNamespace(),
		aws:       true,
		awsConfig: cfg,
	}
}

// runNamespace is what keeps two runs — and two developers — off each other's
// buckets in a namespace that is global to all of S3. The v7 prefix is the run's
// timestamp, so leftovers sort oldest-first when a human goes looking.
func runNamespace() string {
	return strings.ReplaceAll(ids.New().String(), "-", "")[:20]
}

// Bucket maps a name the suite asks for onto the bucket this run actually uses.
func (b ObjectStoreBackend) Bucket(name string) string {
	if b.namespace == "" {
		return name
	}
	return bucketPrefix + b.namespace + "-" + name
}

// MakeBucket creates one bucket for the test and returns its real name, which is
// the only name the test may use. On a backend that outlives the process it also
// registers the purge: a versioned bucket keeps every version and delete marker,
// and a bucket that still holds one cannot be deleted.
func (b ObjectStoreBackend) MakeBucket(t *testing.T, ctx context.Context, name string, objectLock bool) string {
	t.Helper()
	bucket := b.Bucket(name)
	in := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	if objectLock {
		in.ObjectLockEnabledForBucket = aws.Bool(true)
	}
	// us-east-1 is the only region CreateBucket refuses a LocationConstraint for.
	if b.aws && b.Region != "us-east-1" {
		in.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(b.Region),
		}
	}
	if _, err := b.Client().CreateBucket(ctx, in); err != nil {
		t.Fatalf("create bucket %s: %v", bucket, err)
	}
	if b.aws {
		// Not t.Context(): see AWS.
		t.Cleanup(func() { purgeBucket(t, context.Background(), b.Client(), bucket, objectLock) }) //nolint:usetesting // see above
	}
	return bucket
}

// purgeBucket empties a bucket of every version and delete marker and removes it.
// It reports rather than fails: a failed purge leaves money and a name behind, but
// failing the test here would blame the purge for a defect the test did not find.
func purgeBucket(t *testing.T, ctx context.Context, c *s3.Client, bucket string, objectLock bool) {
	t.Helper()
	pages := s3.NewListObjectVersionsPaginator(c, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			t.Logf("purge %s: list versions: %v", bucket, err)
			return
		}
		var doomed []types.ObjectIdentifier
		for _, v := range page.Versions {
			doomed = append(doomed, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
		}
		for _, m := range page.DeleteMarkers {
			doomed = append(doomed, types.ObjectIdentifier{Key: m.Key, VersionId: m.VersionId})
		}
		if len(doomed) == 0 {
			continue
		}
		in := &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{Objects: doomed, Quiet: aws.Bool(true)},
		}
		if objectLock {
			// GOVERNANCE retention is what the object-lock test asserts is
			// unbypassable without this permission; the suite holds it so its own
			// fixtures can go. S3 rejects the header outright on a bucket without
			// Object Lock, so it goes only where there is something to bypass.
			in.BypassGovernanceRetention = aws.Bool(true)
		}
		if _, err := c.DeleteObjects(ctx, in); err != nil {
			t.Logf("purge %s: delete %d versions: %v", bucket, len(doomed), err)
			return
		}
	}
	if _, err := c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Logf("purge %s: delete bucket: %v", bucket, err)
	}
}
