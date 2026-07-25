package real

import (
	"errors"
	"net/http"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// Finding 11. translate() is the whole boundary between S3's error vocabulary and
// the two sentinels the protocol branches on, and it had no unit test at all: every
// mapping was asserted only indirectly, against the one pinned backend, in the
// Docker-gated lane. Two things fall out of that:
//
//   - AWS answers a *concurrent* conditional write with 409 ConditionalRequestConflict,
//     not 412. That is the §12.4 fencing race on real S3: the promoter that lost gets
//     an unmapped, opaque error, epoch.CompareAndAdvance never returns ErrCASConflict,
//     and the "I was fenced" branch is never taken. Fail-closed, so not data loss —
//     but the fencing path behaves differently from every proof written about it and
//     is undiagnosable during an incident;
//   - NoSuchBucket was mapped to ErrNotFound, i.e. to "this object is not there".
//     Recovery reads that as "nothing was written yet" and reports an empty durable
//     prefix for a volume whose data is intact, on nothing worse than a typo'd bucket.
//
// The retryable codes matter just as much in the negative: the uploader retries
// anything that is not a precondition failure (§14.5), so a throttle must NOT come
// back as a sentinel.

func apiErr(code string) error {
	return &smithy.GenericAPIError{Code: code, Message: code + " message"}
}

func respErr(status int, code string) error {
	return &awshttp.ResponseError{
		ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
			Err:      apiErr(code),
		},
	}
}

func TestTranslateMapsTheS3ErrorVocabulary(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want error // the sentinel it must map to; nil means "must stay opaque"
	}{
		{"nil stays nil", nil, nil},
		{"a plain error is passed through", errors.New("dial tcp: connection refused"), nil},

		{"PreconditionFailed is a lost conditional write", apiErr("PreconditionFailed"), objectstore.ErrPreconditionFailed},
		{"ConditionalRequestConflict is AWS's 409 for the same race", apiErr("ConditionalRequestConflict"), objectstore.ErrPreconditionFailed},
		{"a 412 with an unknown code is still a precondition failure", respErr(http.StatusPreconditionFailed, "SomethingElse"), objectstore.ErrPreconditionFailed},
		{"a 409 with an unknown code is still a conflict", respErr(http.StatusConflict, "SomethingElse"), objectstore.ErrPreconditionFailed},

		{"NoSuchKey is a missing object", apiErr("NoSuchKey"), objectstore.ErrNotFound},
		{"NotFound is a missing object", apiErr("NotFound"), objectstore.ErrNotFound},
		{"NoSuchBucket is a missing bucket, not a missing key", apiErr("NoSuchBucket"), objectstore.ErrBucketNotFound},

		{"SlowDown stays opaque so the caller retries", apiErr("SlowDown"), nil},
		{"ServiceUnavailable stays opaque", apiErr("ServiceUnavailable"), nil},
		{"RequestTimeout stays opaque", apiErr("RequestTimeout"), nil},
		{"InternalError stays opaque", apiErr("InternalError"), nil},
		{"AccessDenied stays opaque", apiErr("AccessDenied"), nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := translate(tc.err)
			if tc.want == nil {
				if tc.err == nil && got != nil {
					t.Fatalf("translate(nil) = %v", got)
				}
				for _, sentinel := range []error{objectstore.ErrPreconditionFailed, objectstore.ErrNotFound, objectstore.ErrBucketNotFound} {
					if errors.Is(got, sentinel) {
						t.Fatalf("translate(%v) = %v, which reads as %v — the caller would stop retrying", tc.err, got, sentinel)
					}
				}
				return
			}
			if !errors.Is(got, tc.want) {
				t.Fatalf("translate(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestNoSuchBucketIsNotAMissingKey states the consequence separately, because it is
// the one an operator would never diagnose: a bucket typo reading as "the volume has
// no objects yet".
func TestNoSuchBucketIsNotAMissingKey(t *testing.T) {
	got := translate(apiErr("NoSuchBucket"))
	if errors.Is(got, objectstore.ErrNotFound) {
		t.Fatalf("NoSuchBucket = %v, which recovery reads as an empty durable prefix", got)
	}
}
