package cow_test

import (
	"bytes"
	"testing"

	"github.com/spin-stack/storage/internal/cow"
)

func read(m *cow.IntervalMap, off uint64, n int) []byte {
	buf := make([]byte, n)
	m.Read(off, buf)
	return buf
}

func TestOverwriteAndRead(t *testing.T) {
	m := cow.NewIntervalMap()
	m.Overwrite(0, []byte{1, 2, 3, 4})
	if got := read(m, 0, 4); !bytes.Equal(got, []byte{1, 2, 3, 4}) {
		t.Fatalf("got %v", got)
	}
	// Unwritten reads as zero.
	if got := read(m, 2, 4); !bytes.Equal(got, []byte{3, 4, 0, 0}) {
		t.Fatalf("partial/zero read got %v", got)
	}
}

func TestNewestWinsOnOverlap(t *testing.T) {
	m := cow.NewIntervalMap()
	m.Overwrite(0, []byte{1, 1, 1, 1, 1, 1})
	m.Overwrite(2, []byte{9, 9}) // overwrites middle
	if got := read(m, 0, 6); !bytes.Equal(got, []byte{1, 1, 9, 9, 1, 1}) {
		t.Fatalf("newest-wins failed: %v", got)
	}
}

func TestClearReadsAsZero(t *testing.T) {
	m := cow.NewIntervalMap()
	m.Overwrite(0, []byte{5, 5, 5, 5})
	m.Clear(1, 2) // discard offsets 1..2
	if got := read(m, 0, 4); !bytes.Equal(got, []byte{5, 0, 0, 5}) {
		t.Fatalf("clear should zero the range: %v", got)
	}
	// Memory holds only the two surviving 1-byte extents.
	if m.Bytes() != 2 {
		t.Fatalf("expected 2 live bytes after clear, got %d", m.Bytes())
	}
}

func TestSplitAcrossThreeReads(t *testing.T) {
	m := cow.NewIntervalMap()
	m.Overwrite(10, bytes.Repeat([]byte{7}, 10)) // [10,20)
	m.Clear(13, 4)                               // punch a hole [13,17)
	want := []byte{7, 7, 7, 0, 0, 0, 0, 7, 7, 7}
	if got := read(m, 10, 10); !bytes.Equal(got, want) {
		t.Fatalf("hole-punch mismatch: got %v want %v", got, want)
	}
}
