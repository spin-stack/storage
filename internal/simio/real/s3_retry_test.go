package real_test

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/real"
)

// A Put retries and a PutStream does not, and the two halves are one decision.
//
// PutStream gives up SigV4's payload signature and the SDK's retries to be able to send a
// body it cannot hold; neither concession has anything to do with a request whose body is
// already in memory. When Put was written as a call to PutStream it inherited both, and
// what that cost is the conditional write: an object store answering a burst of concurrent
// CASes with a transient 503 turns, under one attempt, into a loser reporting a transport
// error instead of ErrPreconditionFailed — so a *fenced host reads it as a network
// hiccup* and goes on serving. It reached the conformance lane as three failures in eight
// runs, which is a race nothing here should be waiting on.
//
// So it is pinned deterministically: a backend that answers 503 once and then the truth.
func TestAPutRetriesAndAPutStreamDoesNot(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		put  func(*real.S3Store) error
		// want is what the caller must see after one 503 followed by a 412.
		want error
	}{{
		name: "a Put has its bytes and can be sent again",
		put: func(s *real.S3Store) error {
			_, err := s.Put(t.Context(), "volumes/v/epoch", []byte("x"), objectstore.PutOptions{IfMatch: "etag"})
			return err
		},
		want: objectstore.ErrPreconditionFailed,
	}, {
		name: "a PutStream cannot rewind, so the 503 is the answer",
		put: func(s *real.S3Store) error {
			_, err := s.PutStream(t.Context(), "layers/sha256/ab/cd/ef", bytes.NewReader([]byte("x")), 1,
				objectstore.PutOptions{IfMatch: "etag"})
			return err
		},
		want: nil, // any error but ErrPreconditionFailed; asserted below
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var puts atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPut {
					// The bucket-versioning probe the constructor makes (INV-14).
					w.Header().Set("Content-Type", "application/xml")
					_, _ = w.Write([]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`))
					return
				}
				if puts.Add(1) == 1 {
					// What RustFS answers a burst of concurrent conditional writes, and
					// what the SDK is meant to retry.
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = w.Write([]byte(`<Error><Code>ServiceUnavailable</Code></Error>`))
					return
				}
				w.WriteHeader(http.StatusPreconditionFailed)
				_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code></Error>`))
			}))
			defer srv.Close()

			store, err := real.NewS3Store(t.Context(), real.S3Config{
				Bucket: "b", Endpoint: srv.URL, Region: "us-east-1",
				AccessKey: "k", SecretKey: "s",
			})
			if err != nil {
				t.Fatal(err)
			}

			err = tc.put(store)
			if tc.want != nil {
				if !errors.Is(err, tc.want) {
					t.Fatalf("got %v, want %v — a caller cannot tell being fenced from a hiccup", err, tc.want)
				}
				if n := puts.Load(); n < 2 {
					t.Fatalf("the 503 was never retried (%d PUTs), so the server's real answer was never asked for", n)
				}
				return
			}
			if errors.Is(err, objectstore.ErrPreconditionFailed) {
				t.Fatalf("a stream was replayed after a 503; the SDK cannot rewind it, so this passed by accident")
			}
			if !strings.Contains(err.Error(), "503") && !strings.Contains(err.Error(), "ServiceUnavailable") {
				t.Fatalf("the 503 did not reach the caller: %v", err)
			}
			if n := puts.Load(); n != 1 {
				t.Fatalf("a stream was sent %d times; it can only be sent once", n)
			}
		})
	}
}
