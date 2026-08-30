package real_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/spin-stack/storage/internal/simio/real"
)

// EnsureBucket is an operator's first run, and it could only ever work in one region.
//
// CreateBucket takes the region in the endpoint it is sent to *and* in a location
// constraint in the body, and AWS requires them to agree: without the constraint it
// answers 400 IllegalLocationConstraintException — "The unspecified location constraint
// is incompatible for the region specific endpoint this request was sent to" — for every
// region except us-east-1, whose constraint is the empty one and must therefore be left
// out. So `-s3-create-bucket` worked in us-east-1 and nowhere else, which is the flag a
// deployment uses exactly once and cannot get past.
//
// Found by pointing the e2e lane at real S3 in us-west-2. No container catches it: they
// have one region and ignore the constraint.
func TestEnsureBucketPlacesTheBucketInItsRegion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		region string
		// want is the constraint the body must carry, empty for none at all.
		want string
	}{
		{"a region-specific endpoint needs the matching constraint", "us-west-2", "us-west-2"},
		{"us-east-1 is the one region whose constraint must be absent", "us-east-1", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var (
				mu   sync.Mutex
				body string
			)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut && r.URL.Query().Has("versioning") {
					return
				}
				if r.Method == http.MethodPut {
					b, _ := io.ReadAll(r.Body)
					mu.Lock()
					body = string(b)
					mu.Unlock()
					return
				}
				w.Header().Set("Content-Type", "application/xml")
				_, _ = w.Write([]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`))
			}))
			defer srv.Close()

			// Endpoint set, so this exercises the request shape and not AWS itself; the
			// constraint is chosen from the region either way.
			if _, err := real.EnsureBucket(t.Context(), real.S3Config{
				Bucket: "b", Endpoint: srv.URL, Region: tc.region,
				AccessKey: "k", SecretKey: "s",
			}); err != nil {
				t.Fatal(err)
			}

			mu.Lock()
			defer mu.Unlock()
			got := strings.Contains(body, "<LocationConstraint>"+tc.region+"</LocationConstraint>")
			switch {
			case tc.want != "" && !got:
				t.Fatalf("CreateBucket carried no location constraint for %s; AWS answers IllegalLocationConstraintException. Body: %s", tc.region, body)
			case tc.want == "" && strings.Contains(body, "LocationConstraint"):
				t.Fatalf("CreateBucket carried a constraint in us-east-1, which AWS refuses. Body: %s", body)
			}
		})
	}
}
