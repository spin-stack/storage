package cow_test

import (
	"bytes"
	"testing"

	"pgregory.net/rapid"

	"github.com/spin-stack/storage/internal/cow"
)

// Ranges is what lets a volume be serialised: it says *where* the map answers with data,
// and Read says what those bytes are. Getting it wrong is not a lossy image, it is a
// wrong one — a range reported that the map would read as zeros writes zeros over the
// real bytes on the next boot, and a range omitted loses them.
//
// The property is the only honest statement of correctness, and it is the same one §25.2
// asks of every format: for *any* sequence of writes and discards over a base,
// reconstructing a map from Ranges+Read answers identically to the original, everywhere.
func TestRangesReconstructTheView(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		const size = 4096

		base := cow.NewIntervalMap()
		for range rapid.IntRange(0, 6).Draw(rt, "base writes") {
			off := uint64(rapid.IntRange(0, size-1).Draw(rt, "off"))
			n := rapid.IntRange(1, 512).Draw(rt, "len")
			if off+uint64(n) > size {
				n = size - int(off)
			}
			base.Overwrite(off, bytes.Repeat([]byte{byte(rapid.IntRange(1, 255).Draw(rt, "b"))}, n))
		}

		m := cow.NewIntervalMapOver(base)
		for range rapid.IntRange(0, 10).Draw(rt, "ops") {
			off := uint64(rapid.IntRange(0, size-1).Draw(rt, "off2"))
			n := rapid.IntRange(1, 512).Draw(rt, "len2")
			if off+uint64(n) > size {
				n = size - int(off)
			}
			if rapid.Bool().Draw(rt, "discard") {
				m.Clear(off, uint64(n))
			} else {
				m.Overwrite(off, bytes.Repeat([]byte{byte(rapid.IntRange(1, 255).Draw(rt, "b2"))}, n))
			}
		}

		// Rebuild from what Ranges reports, reading the bytes through the map itself.
		rebuilt := cow.NewIntervalMap()
		for _, r := range m.Ranges() {
			buf := make([]byte, r.Length)
			m.Read(r.Offset, buf)
			rebuilt.Overwrite(r.Offset, buf)
		}

		want := make([]byte, size)
		got := make([]byte, size)
		m.Read(0, want)
		rebuilt.Read(0, got)
		if !bytes.Equal(want, got) {
			for i := range want {
				if want[i] != got[i] {
					rt.Fatalf("byte %d: original %d, rebuilt %d\nranges: %v", i, want[i], got[i], m.Ranges())
				}
			}
		}
	})
}

// A range that reads as all zeros is still a range: the map answers there, and the
// distinction between "written zeros" and "never written" is not one an image can keep
// without also keeping the tombstones. Reporting it is the safe direction — it costs
// bytes in the image, not correctness on the next boot.
func TestRangesReportsWrittenZeros(t *testing.T) {
	m := cow.NewIntervalMap()
	m.Overwrite(0, make([]byte, 512))
	if got := m.Ranges(); len(got) != 1 || got[0].Offset != 0 || got[0].Length != 512 {
		t.Fatalf("Ranges() = %v, want one range at 0..512", got)
	}
}

// A discard over a base must not appear: the map reads zeros there, and an image that
// carried the base's older bytes would resurrect data the guest erased (§14.6).
func TestRangesExcludesDiscardedBaseRanges(t *testing.T) {
	base := cow.NewIntervalMap()
	base.Overwrite(0, bytes.Repeat([]byte{0xAA}, 1024))

	m := cow.NewIntervalMapOver(base)
	m.Clear(256, 512)

	for _, r := range m.Ranges() {
		if r.Offset < 768 && r.Offset+r.Length > 256 {
			buf := make([]byte, r.Length)
			m.Read(r.Offset, buf)
			for i, b := range buf {
				if off := r.Offset + uint64(i); off >= 256 && off < 768 && b != 0 {
					t.Fatalf("range %v carries %d at offset %d, inside a discard", r, b, off)
				}
			}
		}
	}
}
