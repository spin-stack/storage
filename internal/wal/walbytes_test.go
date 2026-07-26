package wal_test

import (
	"testing"

	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// walBytes concatenates the record bytes of every segment of a volume's WAL: what the
// single-file WAL used to hold in one file, minus the per-segment headers.
//
// Tests that assert on the on-disk bytes — INV-15's ciphertext canary, the size a
// rejected append must not change — go through this rather than through a file handle
// the WAL no longer has. It reads the directory rather than the Log's own bookkeeping,
// so it can also contradict it.
func walBytes(t *testing.T, d *sim.Disk, root string, vol [16]byte, epoch uint64) []byte {
	t.Helper()
	names, err := wal.SegmentFiles(d, root, vol, epoch)
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	var out []byte
	for _, name := range names {
		f, err := d.Open(name)
		if err != nil {
			t.Fatalf("open segment %s: %v", name, err)
		}
		size, err := f.Size()
		if err != nil {
			t.Fatalf("size of %s: %v", name, err)
		}
		buf := make([]byte, size)
		if size > 0 {
			if _, err := f.ReadAt(buf, 0); err != nil {
				t.Fatalf("read %s: %v", name, err)
			}
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close %s: %v", name, err)
		}
		if len(buf) < format.SegmentHeaderSize {
			continue
		}
		out = append(out, buf[format.SegmentHeaderSize:]...)
	}
	return out
}

// walSize is what a log occupies on the device, segment headers included.
func walSize(t *testing.T, l *wal.Log) int64 {
	t.Helper()
	n, err := l.LocalBytes()
	if err != nil {
		t.Fatalf("local bytes: %v", err)
	}
	return n
}
