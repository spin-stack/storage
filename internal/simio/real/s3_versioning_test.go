package real

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Finding 1. INV-14 is stated as *structural*: "the interface deliberately cannot
// reach permanent deletion", so a GC reachability bug costs a restore and not the
// data. Every word of that rests on the bucket being versioned — on an unversioned
// bucket, or one whose versioning an operator Suspended, DeleteObject destroys the
// object outright and the GC becomes the incident the invariant promises it cannot
// be. Nothing checked. A bucket created by tooling that forgets the versioning call
// is indistinguishable, at every layer, from a correct one until the first sweep.
//
// The store therefore refuses to exist on a bucket it cannot delete reversibly on.

type versioningStub struct {
	status types.BucketVersioningStatus
	err    error
	bucket string
}

func (v *versioningStub) GetBucketVersioning(_ context.Context, in *s3.GetBucketVersioningInput, _ ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	v.bucket = *in.Bucket
	if v.err != nil {
		return nil, v.err
	}
	return &s3.GetBucketVersioningOutput{Status: v.status}, nil
}

func TestRequireVersioning(t *testing.T) {
	tests := []struct {
		name string
		stub versioningStub
		want error // nil means the bucket is acceptable
	}{
		{"enabled", versioningStub{status: types.BucketVersioningStatusEnabled}, nil},
		{"suspended by an operator", versioningStub{status: types.BucketVersioningStatusSuspended}, ErrBucketNotVersioned},
		{"never configured", versioningStub{status: ""}, ErrBucketNotVersioned},
		{"the bucket does not exist", versioningStub{err: apiErr("NoSuchBucket")}, ErrBucketNotVersioned},
		{"we are not allowed to ask", versioningStub{err: apiErr("AccessDenied")}, ErrBucketNotVersioned},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stub := tc.stub
			err := requireVersioning(t.Context(), &stub, "volumes")
			switch {
			case tc.want == nil && err != nil:
				t.Fatalf("a versioned bucket must be accepted: %v", err)
			case tc.want != nil && !errors.Is(err, tc.want):
				t.Fatalf("err = %v, want %v — an unversioned bucket makes every GC mark permanent", err, tc.want)
			}
			if stub.bucket != "volumes" {
				t.Fatalf("asked about bucket %q, want %q", stub.bucket, "volumes")
			}
		})
	}
}

// TestNewS3StoreRefusesAnUnversionedBucket: the check has to live in the
// constructor. A method a caller may forget to call is not a structural guarantee,
// and INV-14 is claimed as structural.
func TestNewS3StoreRefusesAnUnversionedBucket(t *testing.T) {
	if _, err := NewS3Store(t.Context(), S3Config{}); err == nil {
		t.Fatal("a store with no bucket must not be constructible")
	}
}
