package cow_test

import (
	"bytes"
	"testing"

	"pgregory.net/rapid"

	"github.com/spin-stack/storage/internal/cow"
)

// A delta plus the thing it inherits is the view it was taken from, everywhere.
//
// This is the property that makes it legal for a volume to write down its own layers and
// nothing else. `image.Publish` serialises `DeltaOver(ancestry)` and a later boot rebuilds
// the volume by laying that delta back over the same ancestry (`agent.fetchBase`), so if
// this equality does not hold at every offset the volume comes back as something other
// than what it was — and the two ways it can be wrong are the two ways nobody notices.
// A range the delta omits comes back as whatever the ancestry holds there, which for a
// clone is its parent's older bytes; a range the delta claims and should not comes back
// covering an ancestor that was supposed to show through.
//
// The reconstruction reads the bytes back **through the original view**, exactly as
// `uploadChunks` does, because Ranges says where and Read says what: a rebuild that
// consulted the layers directly would be a second implementation of the layering rules
// and would agree with a broken one.
//
// The base is layered rather than flat on purpose: an ancestry is a chain of snapshots
// (agent.parentView composes one per link), and a delta taken over the *top* of that
// chain must leave every layer of it alone, not only the nearest.
func TestADeltaPlusItsAncestryIsTheWholeView(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		const size = 4096

		// Two ancestors, so "stops at the ancestry" and "stops at the nearest ancestor"
		// are different answers and a delta that confused them fails here.
		oldest := drawWrites(rt, cow.NewIntervalMap(), size, "oldest")
		ancestry := drawOps(rt, cow.NewIntervalMapOver(oldest), size, "nearest")

		view := drawOps(rt, cow.NewIntervalMapOver(ancestry), size, "own")

		d, err := view.DeltaOver(ancestry)
		if err != nil {
			rt.Fatalf("DeltaOver: %v", err)
		}

		// The reader's half: the ancestry as it stands, with this volume's delta laid
		// back over it. This is agent.fetchBase's composition and image.loadManifest's,
		// spelled out so the property is about the data and not about those functions.
		rebuilt := cow.NewIntervalMapOver(ancestry)
		for _, r := range d.Discarded {
			rebuilt.Clear(r.Offset, r.Length)
		}
		for _, r := range d.Data {
			buf := make([]byte, r.Length)
			view.Read(r.Offset, buf)
			rebuilt.Overwrite(r.Offset, buf)
		}

		want, got := make([]byte, size), make([]byte, size)
		view.Read(0, want)
		rebuilt.Read(0, got)
		if !bytes.Equal(want, got) {
			for i := range want {
				if want[i] != got[i] {
					rt.Fatalf("byte %d: the view holds %d, the delta over its ancestry holds %d\ndata: %v\ndiscarded: %v",
						i, want[i], got[i], d.Data, d.Discarded)
				}
			}
		}

		// Data and Discarded must not overlap: a reader applies both, and a range that is
		// in each has an outcome that depends on the order it applies them in.
		for _, a := range d.Data {
			for _, b := range d.Discarded {
				if a.Offset < b.Offset+b.Length && b.Offset < a.Offset+a.Length {
					rt.Fatalf("range %v is reported as data and as discarded", a)
				}
			}
		}
	})
}

// A delta states this volume's layers and **not** its ancestry's, which is the whole point
// of taking one: it is what stops a clone's first stop from writing down a copy of
// everything it inherited.
//
// Asserted as a set of offsets rather than as a byte count, because a delta that reported
// the right *size* and the wrong ranges would satisfy a count.
func TestADeltaStatesThisVolumesLayersAndNotItsAncestrys(t *testing.T) {
	const (
		ancestorOnly = uint64(0)
		ownOnly      = uint64(2048)
		overwritten  = uint64(4096)
		discarded    = uint64(6144)
		n            = 512
	)

	ancestry := cow.NewIntervalMap()
	ancestry.Overwrite(ancestorOnly, bytes.Repeat([]byte{0xA1}, n))
	ancestry.Overwrite(overwritten, bytes.Repeat([]byte{0xA1}, n))
	ancestry.Overwrite(discarded, bytes.Repeat([]byte{0xA1}, n))

	view := cow.NewIntervalMapOver(ancestry)
	view.Overwrite(ownOnly, bytes.Repeat([]byte{0xC2}, n))
	view.Overwrite(overwritten, bytes.Repeat([]byte{0xC2}, n))
	view.Clear(discarded, n)

	d, err := view.DeltaOver(ancestry)
	if err != nil {
		t.Fatalf("DeltaOver: %v", err)
	}

	assertCovers(t, "data", d.Data, []uint64{ownOnly, overwritten}, []uint64{ancestorOnly, discarded})
	assertCovers(t, "discarded", d.Discarded, []uint64{discarded}, []uint64{ancestorOnly, ownOnly, overwritten})

	// And the same view flattened is the other answer, which is what publishing used to
	// write down: every range the ancestry holds, under this volume's name.
	assertCovers(t, "the flattened view", view.Ranges(),
		[]uint64{ancestorOnly, ownOnly, overwritten}, []uint64{discarded})
}

// Everything is this volume's when it inherits nothing, and a tombstone over nothing is
// not reported: with no layer below, "I hold nothing here" and "this was discarded" are
// the same statement, and storing the second in every manifest for ever buys a
// distinction no reader can act on.
func TestAViewThatInheritsNothingStatesAllOfItself(t *testing.T) {
	base := cow.NewIntervalMap()
	base.Overwrite(0, bytes.Repeat([]byte{0xA1}, 512))

	view := cow.NewIntervalMapOver(base)
	view.Overwrite(2048, bytes.Repeat([]byte{0xC2}, 512))
	view.Clear(4096, 512)

	d, err := view.DeltaOver(nil)
	if err != nil {
		t.Fatalf("DeltaOver(nil): %v", err)
	}
	assertCovers(t, "data", d.Data, []uint64{0, 2048}, []uint64{4096})
	if len(d.Discarded) != 0 {
		t.Errorf("a view that inherits nothing reported %v as discarded; there is nothing underneath for a tombstone to hide", d.Discarded)
	}
}

// A view that does not sit over the map it was told it inherits from is refused, and the
// refusal is the point: the plausible answer — the whole chain, flattened — is a manifest
// claiming another volume's ranges as this one's, which no reader can detect.
func TestADeltaOverAMapTheViewDoesNotSitOverIsRefused(t *testing.T) {
	ancestry := cow.NewIntervalMap()
	ancestry.Overwrite(0, bytes.Repeat([]byte{0xA1}, 512))
	stranger := cow.NewIntervalMap()

	view := cow.NewIntervalMapOver(ancestry)
	view.Overwrite(1024, bytes.Repeat([]byte{0xC2}, 512))

	if _, err := view.DeltaOver(stranger); err == nil {
		t.Error("a delta was taken over a map the view does not sit over; it would have claimed the ancestry's ranges")
	}
	if _, err := view.DeltaOver(view); err == nil {
		t.Error("a view inherited from itself; its delta would have been empty and its whole content lost")
	}
	// An unlayered map does not consult a base at all, so a caller that hands it one is
	// making a statement about a chain that is not there.
	flat := cow.NewIntervalMap()
	if _, err := flat.DeltaOver(ancestry); err == nil {
		t.Error("an unlayered map accepted an ancestry; nothing it reports comes from one")
	}
}

func drawWrites(rt *rapid.T, m *cow.IntervalMap, size int, label string) *cow.IntervalMap {
	for range rapid.IntRange(0, 6).Draw(rt, label+" writes") {
		off, n := drawSpan(rt, size, label)
		m.Overwrite(off, bytes.Repeat([]byte{byte(rapid.IntRange(1, 255).Draw(rt, label+" b"))}, n))
	}
	return m
}

func drawOps(rt *rapid.T, m *cow.IntervalMap, size int, label string) *cow.IntervalMap {
	for range rapid.IntRange(0, 8).Draw(rt, label+" ops") {
		off, n := drawSpan(rt, size, label)
		if rapid.Bool().Draw(rt, label+" discard") {
			m.Clear(off, uint64(n))
			continue
		}
		m.Overwrite(off, bytes.Repeat([]byte{byte(rapid.IntRange(1, 255).Draw(rt, label+" b"))}, n))
	}
	return m
}

func drawSpan(rt *rapid.T, size int, label string) (uint64, int) {
	off := rapid.IntRange(0, size-1).Draw(rt, label+" off")
	n := rapid.IntRange(1, 512).Draw(rt, label+" len")
	if off+n > size {
		n = size - off
	}
	return uint64(off), n
}

// assertCovers checks that every offset in covered falls inside some reported range and
// no offset in absent does. Offsets rather than a whole-range comparison because merging
// is legal and expected — two touching ranges are one — so an equality on the slice would
// assert the merger's arithmetic instead of the statement being made.
func assertCovers(t *testing.T, what string, rs []cow.Range, covered, absent []uint64) {
	t.Helper()
	in := func(off uint64) bool {
		for _, r := range rs {
			if off >= r.Offset && off < r.Offset+r.Length {
				return true
			}
		}
		return false
	}
	for _, off := range covered {
		if !in(off) {
			t.Errorf("%s does not cover offset %d; reported %v", what, off, rs)
		}
	}
	for _, off := range absent {
		if in(off) {
			t.Errorf("%s covers offset %d and should not; reported %v", what, off, rs)
		}
	}
}
