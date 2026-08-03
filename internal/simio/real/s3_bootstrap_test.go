package real

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/smithy-go"
)

// bucketAlreadyOurs decides whether a failed CreateBucket is the normal case or a reason
// to stop, and both ways of getting it wrong are expensive.
//
// Too strict — not recognising "it is already there and it is yours" — and every run
// after the first refuses to start, which is every run in a real deployment. Too loose —
// treating any CreateBucket failure as benign — and a name owned by somebody else is
// versioned and written into by us, which for a system whose object store is the recovery
// authority (§5.8) means a volume's durable state lives in a bucket we do not control.
//
// So the exact set of codes is the decision, and this is what pins it.
func TestBucketAlreadyOursRecognisesOnlyTheTwoOwnershipCodes(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			// The normal case on S3 itself: the bucket exists and the caller owns it.
			name: "BucketAlreadyOwnedByYou",
			err:  &smithy.GenericAPIError{Code: "BucketAlreadyOwnedByYou", Message: "already owned"},
			want: true,
		},
		{
			// What most S3-compatible backends answer instead, including the RustFS the
			// conformance suite runs against — which is why both are accepted.
			name: "BucketAlreadyExists",
			err:  &smithy.GenericAPIError{Code: "BucketAlreadyExists", Message: "exists"},
			want: true,
		},
		{
			// The dangerous one. On real S3 this is a name somebody else took, and
			// continuing would version and write into their bucket.
			name: "AccessDenied is not ours",
			err:  &smithy.GenericAPIError{Code: "AccessDenied", Message: "denied"},
			want: false,
		},
		{
			name: "InvalidBucketName is not ours",
			err:  &smithy.GenericAPIError{Code: "InvalidBucketName", Message: "bad name"},
			want: false,
		},
		{
			// A transport failure carries no API code at all. Reporting it as "already
			// ours" would turn an unreachable endpoint into a successful bootstrap.
			name: "a plain error is not an ownership answer",
			err:  errors.New("connection refused"),
			want: false,
		},
		{
			name: "no error is not an ownership answer",
			err:  nil,
			want: false,
		},
		{
			// The SDK wraps API errors on the way out, so the check has to unwrap.
			name: "wrapped ownership errors still count",
			err: errors.Join(errors.New("operation error S3: CreateBucket"),
				&smithy.GenericAPIError{Code: "BucketAlreadyOwnedByYou", Message: "already owned"}),
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := bucketAlreadyOurs(tc.err); got != tc.want {
				t.Fatalf("bucketAlreadyOurs(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

// The region is part of the SigV4 signature, so an empty one does not mean "no region",
// it means a signature computed over the empty string — which most compatible backends
// ignore and AWS rejects. us-east-1 is the value the SDK and every compatible backend
// treat as the default.
func TestRegionOrDefault(t *testing.T) {
	if got := regionOrDefault(""); got != "us-east-1" {
		t.Fatalf("regionOrDefault(\"\") = %q, want us-east-1", got)
	}
	if got := regionOrDefault("eu-west-3"); got != "eu-west-3" {
		t.Fatalf("regionOrDefault(\"eu-west-3\") = %q, want it unchanged", got)
	}
}

// EnsureBucket refuses an empty bucket name before it reaches the network. Without this
// the SDK builds a request against the endpoint's root and the failure comes back as
// something about the URL, which sends an operator to the wrong flag.
func TestEnsureBucketRequiresABucketName(t *testing.T) {
	_, err := EnsureBucket(t.Context(), S3Config{Endpoint: "http://127.0.0.1:1"})
	if err == nil {
		t.Fatal("EnsureBucket accepted an empty bucket name")
	}
	if !strings.Contains(err.Error(), "Bucket is required") {
		t.Fatalf("the refusal must name the field an operator has to set: %v", err)
	}
}
