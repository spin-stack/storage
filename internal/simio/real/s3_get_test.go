package real_test

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/real"
)

// rangeBackend serves one object by range and records how the reader asked for it.
type rangeBackend struct {
	object []byte

	mu       sync.Mutex
	ranges   [][2]int64 // every range served, in the order the requests arrived
	inFlight int
	peak     int
	// hold, when set, blocks the request for the given offset until released, so a
	// range that arrives late can be shown not to overtake the ones before it.
	hold map[int64]chan struct{}
	// holdAll blocks every range until it is closed, which is what makes the size of
	// the in-flight window observable rather than a matter of timing.
	holdAll chan struct{}
	unRange bool // answer without Content-Range, to exercise the fallback
}

func (b *rangeBackend) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Has("versioning") {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`))
			return
		}
		start, end := parseRange(r.Header.Get("Range"), int64(len(b.object)))
		if start >= int64(len(b.object)) && len(b.object) > 0 {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}

		b.mu.Lock()
		b.ranges = append(b.ranges, [2]int64{start, end})
		b.inFlight++
		if b.inFlight > b.peak {
			b.peak = b.inFlight
		}
		gate := b.hold[start]
		all := b.holdAll
		b.mu.Unlock()

		if all != nil {
			<-all
		}
		if gate != nil {
			<-gate
		}

		body := b.object[start : end+1]
		if !b.unRange {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(b.object)))
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)

		b.mu.Lock()
		b.inFlight--
		b.mu.Unlock()
	}
}

func parseRange(h string, size int64) (int64, int64) {
	spec := strings.TrimPrefix(h, "bytes=")
	lo, hi, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, size - 1
	}
	start, _ := strconv.ParseInt(lo, 10, 64)
	end, _ := strconv.ParseInt(hi, 10, 64)
	if end >= size {
		end = size - 1
	}
	return start, end
}

func storeOver(t *testing.T, h http.Handler) *real.S3Store {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	store, err := real.NewS3Store(t.Context(), real.S3Config{
		Bucket: "b", Endpoint: srv.URL, Region: "us-east-1",
		AccessKey: "k", SecretKey: "s",
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + i>>9)
	}
	return b
}

// A layer comes back in one piece and in order, however many requests it took.
//
// The order is not an aesthetic preference: the caller digests the bytes as stored with
// SHA-256 and unseals them frame by frame, both of which have to see the object in
// sequence. A parallel fetch that delivered ranges as they landed would force the whole
// sealed layer onto disk first — for a compacted root, the guest's disk twice over.
func TestGetStreamReadsRangesInParallelAndYieldsThemInOrder(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		size int
		// wantRequests is how many ranges the object must take: 1 when it fits.
		wantRequests int
	}{
		{"an object that fits in one chunk is one request", 4 << 20, 1},
		{"a layer takes as many as it needs", 40 << 20, 5},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			be := &rangeBackend{object: payload(tc.size)}
			store := storeOver(t, be.handler())

			rc, size, err := store.GetStream(t.Context(), "layers/sha256/x")
			if err != nil {
				t.Fatal(err)
			}
			defer rc.Close()
			if size != int64(tc.size) {
				t.Fatalf("GetStream reports %d bytes, the object is %d", size, tc.size)
			}
			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != tc.size {
				t.Fatalf("read %d bytes of %d", len(got), tc.size)
			}
			for i := range got {
				if got[i] != be.object[i] {
					t.Fatalf("byte %d differs: the ranges were handed over out of order", i)
				}
			}

			be.mu.Lock()
			defer be.mu.Unlock()
			if len(be.ranges) != tc.wantRequests {
				t.Fatalf("%d range requests, want %d", len(be.ranges), tc.wantRequests)
			}
		})
	}
}

// The point of the exercise: the ranges of one layer are in flight together. Asserting
// that two ever overlapped is not enough — a strictly serial reader still overlaps the
// request it is issuing with the response it is draining, so that assertion passed with
// the window set to one. What is asserted is the width of the window itself: every range
// is held in the backend, so they pile up until the window is full and cannot go further.
func TestGetStreamHasSeveralRangesInFlight(t *testing.T) {
	t.Parallel()
	be := &rangeBackend{object: payload(128 << 20), holdAll: make(chan struct{})}
	store := storeOver(t, be.handler())

	// The first range is the size probe, issued before the reader exists; release it,
	// then hold everything the reader goes on to ask for.
	go func() {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			be.mu.Lock()
			full := be.inFlight >= wantInFlight
			be.mu.Unlock()
			if full {
				break
			}
			time.Sleep(time.Millisecond)
		}
		close(be.holdAll)
	}()

	rc, _, err := store.GetStream(t.Context(), "layers/sha256/x")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatal(err)
	}

	be.mu.Lock()
	defer be.mu.Unlock()
	if be.peak < wantInFlight {
		t.Fatalf("peak of %d ranges in flight, want at least %d: the window is narrower than it claims, and most of the host's bandwidth goes unused",
			be.peak, wantInFlight)
	}
}

// wantInFlight is below the configured window on purpose: what is being pinned is that
// several ranges travel together, not the exact number, which is a tuning decision.
const wantInFlight = 4

// A range that is slow holds up the ones behind it and nothing else — the caller sees a
// gap, never a reordering.
func TestGetStreamDoesNotLetALateRangeOvertake(t *testing.T) {
	t.Parallel()
	gate := make(chan struct{})
	be := &rangeBackend{
		object: payload(40 << 20),
		hold:   map[int64]chan struct{}{8 << 20: gate},
	}
	store := storeOver(t, be.handler())

	rc, _, err := store.GetStream(t.Context(), "layers/sha256/x")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	done := make(chan []byte, 1)
	go func() {
		got, _ := io.ReadAll(rc)
		done <- got
	}()

	// The reader must not be able to finish while the second range is held, however many
	// of the later ones have already landed.
	select {
	case got := <-done:
		t.Fatalf("read finished with the second range still outstanding: %d bytes", len(got))
	default:
	}
	close(gate)

	got := <-done
	for i := range got {
		if got[i] != be.object[i] {
			t.Fatalf("byte %d differs: a later range overtook the one it was held behind", i)
		}
	}
	if len(got) != len(be.object) {
		t.Fatalf("read %d bytes of %d", len(got), len(be.object))
	}
}

// An object with no Content-Range at all — some S3-compatible backends answer a range
// they served in full without one — must not be read as zero-length.
func TestGetStreamWithoutContentRangeReadsWhatCameBack(t *testing.T) {
	t.Parallel()
	be := &rangeBackend{object: payload(3 << 20), unRange: true}
	store := storeOver(t, be.handler())

	rc, size, err := store.GetStream(t.Context(), "layers/sha256/x")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(be.object)) || len(got) != len(be.object) {
		t.Fatalf("size=%d read=%d, object is %d", size, len(got), len(be.object))
	}
}

// A missing key must arrive as the sentinel the recovery path branches on, not as some
// transport error: recovery reads ErrNotFound as "nothing was written yet".
func TestGetStreamOfAMissingKeyIsNotFound(t *testing.T) {
	t.Parallel()
	store := storeOver(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Has("versioning") {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
	}))
	if _, _, err := store.GetStream(t.Context(), "gone"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}
