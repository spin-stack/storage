package real

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Config documents its credential fields as "empty means the SDK's default credential
// chain (instance role, env, config file)". That was not true: NewS3Store builds the
// client with s3.New(opts), and s3.New resolves nothing — an Options with no Credentials
// provider signs nothing, so every request left the host **unsigned**. A backend with
// auth answers 403 on the first call and the operator is told the bucket is not
// versioned (see the other test in this file).
//
// The fix has to be the documented behaviour rather than an error, because the two ways
// this will actually be deployed — an instance role in AWS, AWS_* in the environment for
// a local RustFS — both go through that chain.
func TestCredentialsFallBackToTheEnvironmentWhenNoneAreConfigured(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "from-the-environment")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "and-its-secret")

	provider, err := resolveCredentials(t.Context(), S3Config{Bucket: "b"})
	if err != nil {
		t.Fatalf("resolving credentials: %v", err)
	}
	if provider == nil {
		t.Fatal("no credentials provider: requests would go out unsigned")
	}
	creds, err := provider.Retrieve(t.Context())
	if err != nil {
		t.Fatalf("retrieving credentials: %v", err)
	}
	if creds.AccessKeyID != "from-the-environment" {
		t.Errorf("access key = %q, want the one in the environment", creds.AccessKeyID)
	}
}

// Static credentials still win, so a config file or flags do not need the environment.
func TestConfiguredCredentialsWinOverTheEnvironment(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "from-the-environment")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "and-its-secret")

	provider, err := resolveCredentials(t.Context(), S3Config{Bucket: "b", AccessKey: "explicit", SecretKey: "s"})
	if err != nil {
		t.Fatalf("resolving credentials: %v", err)
	}
	creds, err := provider.Retrieve(t.Context())
	if err != nil {
		t.Fatalf("retrieving credentials: %v", err)
	}
	if creds.AccessKeyID != "explicit" {
		t.Errorf("access key = %q, want the configured one", creds.AccessKeyID)
	}
}

// requireVersioning is right to refuse a bucket it cannot verify — INV-14 rests on the
// delete marker being reversible, and TestRequireVersioning pins that decision. This is
// about the sentence it produces, which is the first thing a misconfigured deployment
// sees, because NewS3Store runs the check in its constructor.
//
// Leading with "bucket versioning is not Enabled" when the truth is "your credentials
// were refused" costs an operator the hour it takes to stop looking at the bucket
// policy. The cause was already in the message, buried after the verdict; what is wrong
// is which one the sentence asserts.
func TestVersioningCheckLeadsWithWhatActuallyHappened(t *testing.T) {
	tests := []struct {
		name      string
		stub      versioningStub
		wantLead  string
		wantCause string
	}{
		{
			name:      "a real versioning verdict names the status",
			stub:      versioningStub{status: types.BucketVersioningStatusSuspended},
			wantLead:  "versioning",
			wantCause: "Suspended",
		},
		{
			name:      "a refused request is not a versioning verdict",
			stub:      versioningStub{err: apiErr("AccessDenied")},
			wantLead:  "could not determine",
			wantCause: "AccessDenied",
		},
		{
			name:      "neither is an unreachable backend",
			stub:      versioningStub{err: errors.New("dial tcp: connection refused")},
			wantLead:  "could not determine",
			wantCause: "connection refused",
		},
		{
			name:      "nor a bucket that is not there yet",
			stub:      versioningStub{err: apiErr("NoSuchBucket")},
			wantLead:  "could not determine",
			wantCause: "NoSuchBucket",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stub := tc.stub
			err := requireVersioning(t.Context(), &stub, "the-bucket")
			if err == nil {
				t.Fatal("requireVersioning accepted a bucket it could not verify")
			}
			// Failing closed is not up for negotiation: every one of these still has
			// to be ErrBucketNotVersioned, because none of them proves a delete would
			// be reversible.
			if !errors.Is(err, ErrBucketNotVersioned) {
				t.Fatalf("stopped failing closed: %v", err)
			}
			msg := err.Error()
			if !strings.Contains(msg, tc.wantLead) {
				t.Errorf("message does not lead with %q: %v", tc.wantLead, err)
			}
			if !strings.Contains(msg, tc.wantCause) {
				t.Errorf("message does not carry the cause %q: %v", tc.wantCause, err)
			}
			if !strings.Contains(msg, "the-bucket") {
				t.Errorf("message does not name the bucket: %v", err)
			}
		})
	}
}
