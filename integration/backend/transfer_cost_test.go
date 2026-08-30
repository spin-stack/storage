//go:build integration

package backend_test

import (
	"fmt"
	"io"
	"os"
	"testing"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ids"
)

// TestMeasureWhatALayerCostsBothWays times a layer out to the bucket and back, over sizes
// that reach past a gigabyte.
//
// It exists because two claims were being made on arithmetic. A layer is published as
// several parts at once and fetched as several ranges at once, and both windows were sized
// by reasoning about bandwidth-delay products rather than by watching bytes move. And
// nothing large had ever crossed the wire at all: the ceiling on a single PUT was proven
// against a planted one in an httptest, and the largest object this suite had ever really
// stored was measured in megabytes.
//
// The download is the interesting half. A restore is the time a guest is not running, and
// it is spent almost entirely here — the local work that follows, repointing each layer at
// its parent, is milliseconds against a network measured in seconds. So the rate this
// prints is the floor under any recovery-time number, and the one to watch when the window
// constants move.
//
// It is not in the gate and asserts nothing about the numbers, for the same reason
// measure:publish does not: a measurement that fails a build is a threshold in disguise.
// Run it with `task measure:transfer` or `task measure:transfer:aws`.
func TestMeasureWhatALayerCostsBothWays(t *testing.T) {
	if os.Getenv("MEASURE") == "" {
		t.Skip("set MEASURE=1 (or run `task measure:transfer`): this is a measurement, not an assertion")
	}
	be := backendConfig(t)
	store := newVersionedS3Store(t, be, "measure-transfer")
	ctx := t.Context()

	// The largest crosses S3's ceiling on a single PUT, which is the size the multipart
	// path exists for and the one no lane had ever actually stored: the ceiling was proven
	// against a planted one in an httptest, never against the real refusal.
	const largest = 6 << 30

	sizes := []int64{16 << 20, 64 << 20, 256 << 20, 1 << 30, largest}
	type row struct {
		size             int64
		up, down         time.Duration
		upRate, downRate float64
	}
	var rows []row
	for _, size := range sizes {
		vol := ids.New().String()
		enc, err := crypto.NewEncryption(crypto.DEK{Key: [crypto.DEKSize]byte{5}, KeyID: 1}, [16]byte(uuid.MustParse(vol)))
		if err != nil {
			t.Fatal(err)
		}

		up := time.Now()
		res, err := commit.Publish(ctx, store, enc, &synthetic{size: size}, commit.Request{
			VolumeID: vol, CommitID: ids.New().String(), LayerID: ids.New().String(),
			Epoch: 1, VirtualSize: 4 << 30,
		})
		if err != nil {
			t.Fatalf("publishing %s: %v", capacity(size), err)
		}
		upTook := time.Since(up)

		m, err := commit.ReadManifest(ctx, store, vol, res.CommitID)
		if err != nil {
			t.Fatal(err)
		}
		// Fetch into io.Discard: what is being timed is the bucket and the unsealing, and
		// writing a gigabyte to the test machine's disk would measure that instead.
		down := time.Now()
		if err := commit.Fetch(ctx, store, enc, m, io.Discard); err != nil {
			t.Fatalf("fetching %s back: %v", capacity(size), err)
		}
		downTook := time.Since(down)

		rows = append(rows, row{
			size: size, up: upTook, down: downTook,
			upRate:   float64(size) / upTook.Seconds() / (1 << 20),
			downRate: float64(size) / downTook.Seconds() / (1 << 20),
		})
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintf(w, "\n=== what a layer costs both ways, against %s\n", endpointName(be.Endpoint))
	_, _ = fmt.Fprintln(w, "LAYER\tPUBLISH\tMiB/s\tFETCH\tMiB/s")
	for _, r := range rows {
		_, _ = fmt.Fprintf(w, "%s\t%s\t%.0f\t%s\t%.0f\n", capacity(r.size),
			r.up.Round(time.Millisecond), r.upRate,
			r.down.Round(time.Millisecond), r.downRate)
	}
	last := rows[len(rows)-1]
	_, _ = fmt.Fprintf(w, "\nat %s: %.0f MiB/s out, %.0f MiB/s back\n",
		capacity(last.size), last.upRate, last.downRate)
	_, _ = fmt.Fprintf(w, "a %s volume of one layer would take %s to recover, before any local work\n",
		capacity(last.size), time.Duration(float64(last.size)/(last.downRate*(1<<20))*float64(time.Second)).Round(time.Millisecond))
	_ = w.Flush()
}

// synthetic is the layer's plaintext, generated as it is read. Six gibibytes of it in a
// slice is six gibibytes of the test machine's memory, which is the allocation the code
// under measurement exists to avoid — holding it here to measure not holding it there
// would be its own joke. Incompressible and position-dependent, so nothing in the path can
// be fast for a reason that would not hold for a guest's disk.
// Seekable because Publish makes two passes over the layer — one to measure and hash what
// sealing produces, one to send it — which is the pair the seal-drift check compares. The
// bytes are a function of their offset, so seeking is arithmetic and the two passes see
// the same layer by construction.
type synthetic struct {
	size int64
	off  int64
}

func (s *synthetic) Read(p []byte) (int, error) {
	if s.off >= s.size {
		return 0, io.EOF
	}
	if rest := s.size - s.off; int64(len(p)) > rest {
		p = p[:rest]
	}
	for i := range p {
		s.off++
		p[i] = byte(s.off*7 + s.off>>13)
	}
	return len(p), nil
}

func (s *synthetic) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		s.off = offset
	case io.SeekCurrent:
		s.off += offset
	case io.SeekEnd:
		s.off = s.size + offset
	}
	if s.off < 0 || s.off > s.size {
		return 0, fmt.Errorf("synthetic: seek to %d, outside [0,%d]", s.off, s.size)
	}
	return s.off, nil
}

// endpointName names the backend for the header. Real S3 has no endpoint of its own here —
// it is whatever the credential chain resolved — and printing an empty string invited the
// reader to think the measurement had run against nothing.
func endpointName(endpoint string) string {
	if endpoint == "" {
		return "real S3"
	}
	return endpoint
}
