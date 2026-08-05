package cow_test

import (
	"bytes"
	"testing"

	"pgregory.net/rapid"

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
	if got := m.Cost().Bytes; got != 2 {
		t.Fatalf("expected 2 live bytes after clear, got %d", got)
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

// A layered map is what makes a truncated WAL resumable: the base is the view
// recovered from the object store (everything up to the durable point), and the layer
// on top is what the local segments still hold. The local layer is newer, so it wins —
// and the two must compose without either one being able to show data the other
// superseded.

func TestBaseShowsThroughWhereTheLayerIsEmpty(t *testing.T) {
	base := cow.NewIntervalMap()
	base.Overwrite(0, []byte("from-the-object-store"))

	m := cow.NewIntervalMapOver(base)
	buf := make([]byte, 21)
	m.Read(0, buf)
	if string(buf) != "from-the-object-store" {
		t.Fatalf("read %q, want the base's bytes", buf)
	}
}

func TestTheLayerWinsOverTheBase(t *testing.T) {
	base := cow.NewIntervalMap()
	base.Overwrite(0, []byte("AAAAAAAA"))

	m := cow.NewIntervalMapOver(base)
	m.Overwrite(2, []byte("bb"))

	buf := make([]byte, 8)
	m.Read(0, buf)
	if string(buf) != "AAbbAAAA" {
		t.Fatalf("read %q, want AAbbAAAA — the newer layer must win", buf)
	}
}

// The one that is easy to get wrong, and the reason a layered map needs to record what
// it cleared rather than merely forgetting it. A DISCARD in the local layer over a range
// the base holds must read as zero: "I have nothing here" and "this was discarded" are
// different statements, and only the second one may hide the base (§14.6).
func TestAClearInTheLayerHidesTheBase(t *testing.T) {
	base := cow.NewIntervalMap()
	base.Overwrite(0, []byte("AAAAAAAA"))

	m := cow.NewIntervalMapOver(base)
	m.Clear(2, 4)

	buf := make([]byte, 8)
	m.Read(0, buf)
	if want := "AA\x00\x00\x00\x00AA"; string(buf) != want {
		t.Fatalf("read %q, want %q — a discarded range must not resurrect the base", buf, want)
	}
}

func TestAWriteAfterAClearShowsTheWrite(t *testing.T) {
	base := cow.NewIntervalMap()
	base.Overwrite(0, []byte("AAAAAAAA"))

	m := cow.NewIntervalMapOver(base)
	m.Clear(0, 8)
	m.Overwrite(3, []byte("cc"))

	buf := make([]byte, 8)
	m.Read(0, buf)
	if want := "\x00\x00\x00cc\x00\x00\x00"; string(buf) != want {
		t.Fatalf("read %q, want %q", buf, want)
	}
}

func TestAClearAfterAWriteHidesBoth(t *testing.T) {
	base := cow.NewIntervalMap()
	base.Overwrite(0, []byte("AAAAAAAA"))

	m := cow.NewIntervalMapOver(base)
	m.Overwrite(0, []byte("bbbbbbbb"))
	m.Clear(2, 4)

	buf := make([]byte, 8)
	m.Read(0, buf)
	if want := "bb\x00\x00\x00\x00bb"; string(buf) != want {
		t.Fatalf("read %q, want %q", buf, want)
	}
}

// Adjacent and overlapping clears must not accumulate: a guest discarding a volume in
// 4 KiB steps would otherwise grow a tombstone list proportional to the volume, which is
// the memory profile this package exists to avoid.
func TestClearsAreMerged(t *testing.T) {
	base := cow.NewIntervalMap()
	base.Overwrite(0, make([]byte, 4096))

	m := cow.NewIntervalMapOver(base)
	for off := range uint64(8) {
		m.Clear(off*512, 512)
	}
	if got := m.Cost().Cleared; got != 1 {
		t.Fatalf("%d tombstones for eight adjacent clears, want 1", got)
	}
}

// A map with no base keeps its old behaviour exactly: nothing is recorded, because
// there is nothing underneath that a tombstone could hide.
func TestAnUnlayeredMapRecordsNoTombstones(t *testing.T) {
	m := cow.NewIntervalMap()
	m.Overwrite(0, []byte("AAAA"))
	m.Clear(0, 4)
	if got := m.Cost().Cleared; got != 0 {
		t.Fatalf("%d tombstones without a base, want 0", got)
	}
	buf := make([]byte, 4)
	m.Read(0, buf)
	if want := "\x00\x00\x00\x00"; string(buf) != want {
		t.Fatalf("read %q, want %q", buf, want)
	}
}

// TestLayeringEqualsFlattening is the algebraic statement of what a base is worth: a
// layered map must answer exactly what one map would, had the base's operations and the
// layer's been applied to it in that order. Anything else means a resumed volume reads
// differently from one that never restarted — which is the whole class of bug increment
// 5 exists to close.
func TestLayeringEqualsFlattening(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		const size = 64
		type op struct {
			off   uint64
			n     uint64
			write bool
			b     byte
		}
		genOps := rapid.SliceOfN(rapid.Custom(func(t *rapid.T) op {
			off := rapid.Uint64Range(0, size-1).Draw(t, "off")
			return op{
				off:   off,
				n:     rapid.Uint64Range(1, size-off).Draw(t, "n"),
				write: rapid.Bool().Draw(t, "write"),
				b:     rapid.ByteRange(1, 255).Draw(t, "b"),
			}
		}), 0, 12)

		baseOps := genOps.Draw(t, "baseOps")
		layerOps := genOps.Draw(t, "layerOps")

		apply := func(m *cow.IntervalMap, ops []op) {
			for _, o := range ops {
				if o.write {
					m.Overwrite(o.off, bytes.Repeat([]byte{o.b}, int(o.n)))
				} else {
					m.Clear(o.off, o.n)
				}
			}
		}

		base := cow.NewIntervalMap()
		apply(base, baseOps)
		layered := cow.NewIntervalMapOver(base)
		apply(layered, layerOps)

		flat := cow.NewIntervalMap()
		apply(flat, baseOps)
		apply(flat, layerOps)

		got, want := make([]byte, size), make([]byte, size)
		layered.Read(0, got)
		flat.Read(0, want)
		if !bytes.Equal(got, want) {
			t.Fatalf("layered %x != flattened %x", got, want)
		}
	})
}
