package real_test

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/real"
)

// singlePutCeiling is what S3 answers a PutObject whose Content-Length is over 5 GiB:
// 400 EntityTooLarge, decided on the header, before a byte of the body is read. Measured
// against real S3, not read in the documentation.
//
// It is planted here at a size a test can actually send, because the size is not the
// point — the point is that a ceiling exists at all and that a layer can cross it. A
// compacted root is bounded by the guest's disk and by nothing else (qcow/compact.go),
// so any volume larger than the ceiling had a compaction that could never publish: not a
// transient failure, but the same failure every cycle, for ever, while the chain walks to
// MaxLayers and the volume quietly stops committing.
//
// RustFS accepts a single PUT of any size, which is why the container lane certified a
// path that S3 refuses.
const plantedCeiling = 6 << 20

// ceilingBackend is an S3 that refuses a single PUT over plantedCeiling and serves
// multipart properly. It is deliberately not a full S3: it answers exactly the four
// requests an upload makes, and anything else is a failure rather than a default.
type ceilingBackend struct {
	mu     sync.Mutex
	parts  map[int][]byte
	body   []byte // what the object ended up holding
	single int    // single PutObject attempts
	refuse int    // ...of which were refused for size
}

func (b *ceilingBackend) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		b.mu.Lock()
		defer b.mu.Unlock()
		switch {
		case r.Method == http.MethodGet && q.Has("versioning"):
			// The bucket-versioning probe the constructor makes (INV-14).
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`))

		case r.Method == http.MethodPost && q.Has("uploads"):
			b.parts = map[int][]byte{}
			writeXML(t, w, "InitiateMultipartUploadResult", struct {
				Bucket, Key, UploadId string
			}{"b", r.URL.Path, "upload-1"})

		case r.Method == http.MethodPut && q.Has("partNumber"):
			n, err := strconv.Atoi(q.Get("partNumber"))
			if err != nil {
				t.Errorf("part number %q: %v", q.Get("partNumber"), err)
			}
			part, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read part %d: %v", n, err)
			}
			b.parts[n] = part
			w.Header().Set("ETag", fmt.Sprintf("%q", fmt.Sprintf("part-%d", n)))

		case r.Method == http.MethodPost && q.Has("uploadId"):
			// The conditional write rides on the complete, which is the whole reason a
			// create-only layer upload can be multipart at all.
			if r.Header.Get("If-None-Match") != "*" {
				t.Errorf("CompleteMultipartUpload carried If-None-Match %q, want \"*\" — create-only was dropped on the way to multipart",
					r.Header.Get("If-None-Match"))
			}
			for i := 1; i <= len(b.parts); i++ {
				b.body = append(b.body, b.parts[i]...)
			}
			writeXML(t, w, "CompleteMultipartUploadResult", struct {
				Bucket, Key, ETag string
			}{"b", r.URL.Path, `"complete-etag-2"`})

		case r.Method == http.MethodPut:
			b.single++
			if r.ContentLength > plantedCeiling {
				b.refuse++
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`<Error><Code>EntityTooLarge</Code><Message>Your proposed upload exceeds the maximum allowed size</Message></Error>`))
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read body: %v", err)
			}
			b.body = body
			w.Header().Set("ETag", `"single-etag-1"`)

		default:
			t.Errorf("unexpected request %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
			w.WriteHeader(http.StatusNotImplemented)
		}
	}
}

func writeXML(t *testing.T, w http.ResponseWriter, name string, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/xml")
	enc := xml.NewEncoder(w)
	if err := enc.EncodeElement(v, xml.StartElement{Name: xml.Name{Local: name}}); err != nil {
		t.Errorf("encode %s: %v", name, err)
	}
	_ = enc.Flush()
}

// TestPutStreamCrossesTheSinglePutCeiling: a layer larger than one PUT must still reach
// the bucket, and must still be create-only when it gets there.
func TestPutStreamCrossesTheSinglePutCeiling(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		size int
		// wantParts is how the body must travel: 0 means one PutObject.
		wantParts bool
	}{
		{"under the ceiling, one PUT as before", 4 << 20, false},
		{"over the ceiling, in parts", 16 << 20, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			be := &ceilingBackend{}
			srv := httptest.NewServer(be.handler(t))
			defer srv.Close()

			store, err := real.NewS3Store(t.Context(), real.S3Config{
				Bucket: "b", Endpoint: srv.URL, Region: "us-east-1",
				AccessKey: "k", SecretKey: "s",
			})
			if err != nil {
				t.Fatal(err)
			}

			want := make([]byte, tc.size)
			for i := range want {
				want[i] = byte(i*7 + i>>11)
			}
			res, err := store.PutStream(t.Context(), "layers/sha256/ab/cd/ef",
				&countingReader{b: want}, int64(tc.size), objectstore.PutOptions{IfNoneMatch: true})
			if err != nil {
				t.Fatalf("PutStream of %d bytes: %v", tc.size, err)
			}
			if res.ETag == "" {
				t.Fatal("no ETag: a caller that CASes on this has nothing to hold")
			}

			be.mu.Lock()
			defer be.mu.Unlock()
			if len(be.body) != tc.size {
				t.Fatalf("the bucket holds %d bytes, want %d", len(be.body), tc.size)
			}
			for i := range want {
				if be.body[i] != want[i] {
					t.Fatalf("byte %d differs: the parts were assembled wrong", i)
				}
			}
			if tc.wantParts {
				if be.refuse == 0 && be.single > 0 {
					t.Fatalf("%d single PUTs and none refused: the planted ceiling never fired", be.single)
				}
				if len(be.parts) < 2 {
					t.Fatalf("%d parts: a body over the ceiling went as one request", len(be.parts))
				}
				return
			}
			if len(be.parts) != 0 {
				t.Fatalf("a body under the ceiling was split into %d parts", len(be.parts))
			}
		})
	}
}

// countingReader is a plain sequential reader: a layer arrives as a stream, and nothing
// downstream may depend on it being seekable.
type countingReader struct {
	b   []byte
	off int
}

func (r *countingReader) Read(p []byte) (int, error) {
	if r.off >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.off:])
	r.off += n
	return n, nil
}
