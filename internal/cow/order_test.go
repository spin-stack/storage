package cow

import (
	"bytes"
	"fmt"
	"testing"

	"pgregory.net/rapid"
)

// `extents` is sorted by start and non-overlapping. Every comment in this package says so
// and, until this test, nothing checked it — which was survivable only while both mutation
// paths re-sorted the whole slice and rebuilt it from scratch on every call.
//
// **A read-based assertion is not a check on the order, and that is why this test has to
// exist separately.** Read visits every extent and copies whichever ones cover the range,
// later ones over earlier — so a stale extent left overlapping a newer one is usually
// painted over and the read is right by accident. Measured, not argued: with the lower
// edge of `removeRange`'s window planted to compare against the wrong end of an extent —
// which leaves overlapping extents behind on every trim — `TestNewestWinsOnOverlap` and
// every other deterministic read test in this package still passed, while this test failed
// on rapid's first draw and named the pair. `TestLayeringEqualsFlattening` did eventually
// catch it, but only indirectly and only after enough random operations for a *later*
// search to be misled by the disorder.
//
// The order is not decoration. `subtract` and `extentSpans` in ranges.go walk the extents
// assuming ascending disjoint spans, and the binary searches in `insert` and `removeRange`
// need it to find the window they touch at all.
//
// So the invariant is checked directly, on the field, after arbitrary sequences of the two
// operations that can break it — including the awkward ones the address space is kept
// tiny to force: a write that splits an extent in two, a clear that trims one end, a write
// that exactly covers three older extents.
func TestTheExtentsStaySortedAndDisjoint(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		const size = 64
		m := NewIntervalMapOver(NewIntervalMap())

		n := rapid.IntRange(0, 40).Draw(t, "ops")
		for range n {
			off := rapid.Uint64Range(0, size-1).Draw(t, "off")
			// From zero, not from one. A zero-length WRITE is a record the WAL encodes
			// and replays like any other — `TestWALSegmentReplayProperty` draws them —
			// and it used to put an *empty* extent in the map: a region covering nothing,
			// which reads as if it were not there, costs a whole ExtentOverheadBytes of
			// the volume's view budget, and makes two extents compare equal on start.
			// Every property test in this package drew from 1 and never saw it.
			length := rapid.Uint64Range(0, size-off).Draw(t, "len")
			what := "clear"
			if rapid.Bool().Draw(t, "write") {
				what = "write"
				m.Overwrite(off, bytes.Repeat([]byte{7}, int(length)))
			} else {
				m.Clear(off, length)
			}
			if err := m.checkOrder(); err != nil {
				t.Fatalf("after %s [%d,%d): %v", what, off, off+length, err)
			}
		}
	})
}

// The same statement about tombstones. `cover` merges what it touches and `uncover` splits
// what it cuts, and `subtract` in ranges.go walks the result assuming ascending disjoint
// spans — a pair that overlapped there would silently drop a range from a manifest.
func TestTheTombstonesStaySortedAndDisjoint(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		const size = 64
		m := NewIntervalMapOver(NewIntervalMap())

		n := rapid.IntRange(0, 40).Draw(t, "ops")
		for range n {
			off := rapid.Uint64Range(0, size-1).Draw(t, "off")
			// From zero, not from one. A zero-length WRITE is a record the WAL encodes
			// and replays like any other — `TestWALSegmentReplayProperty` draws them —
			// and it used to put an *empty* extent in the map: a region covering nothing,
			// which reads as if it were not there, costs a whole ExtentOverheadBytes of
			// the volume's view budget, and makes two extents compare equal on start.
			// Every property test in this package drew from 1 and never saw it.
			length := rapid.Uint64Range(0, size-off).Draw(t, "len")
			if rapid.Bool().Draw(t, "write") {
				m.Overwrite(off, bytes.Repeat([]byte{7}, int(length)))
			} else {
				m.Clear(off, length)
			}
			var prev uint64
			for i, c := range m.cleared {
				if c.start >= c.end {
					t.Fatalf("tombstone %d is empty or inverted: [%d,%d)", i, c.start, c.end)
				}
				if i > 0 && c.start < prev {
					t.Fatalf("tombstone %d starts at %d, inside or before the one ending at %d", i, c.start, prev)
				}
				prev = c.end
			}
		}
	})
}

// checkOrder states the invariant once so both the property test and the deterministic
// cases below can assert it. It reports the first violation rather than a bool: which pair
// broke is the whole content of the failure.
func (m *IntervalMap) checkOrder() error {
	for i, x := range m.extents {
		if len(x.data) == 0 {
			return fmt.Errorf("extent %d at %d is empty; an empty extent is not a region, and it makes two starts compare equal", i, x.start)
		}
		if i == 0 {
			continue
		}
		if prev := m.extents[i-1]; x.start < prev.end() {
			return fmt.Errorf("extent %d is [%d,%d) but extent %d already covers [%d,%d): out of order or overlapping",
				i, x.start, x.end(), i-1, prev.start, prev.end())
		}
	}
	return nil
}

// The shapes the property test reaches only by luck, pinned as cases so a failure names
// what it is. Each one is a write or a clear landing in a different position relative to
// the extents already there, which is the whole set of branches `removeRange` has.
func TestTheOrderSurvivesEveryShapeOfOverlap(t *testing.T) {
	// Three separated extents to land on: [10,20), [30,40), [50,60).
	base := func() *IntervalMap {
		m := NewIntervalMap()
		for _, off := range []uint64{10, 30, 50} {
			m.Overwrite(off, bytes.Repeat([]byte{1}, 10))
		}
		return m
	}
	tests := []struct {
		name  string
		drive func(m *IntervalMap)
		want  int // extents afterwards
	}{
		{"before everything", func(m *IntervalMap) { m.Overwrite(0, bytes.Repeat([]byte{2}, 5)) }, 4},
		{"in the gap", func(m *IntervalMap) { m.Overwrite(22, bytes.Repeat([]byte{2}, 5)) }, 4},
		{"after everything", func(m *IntervalMap) { m.Overwrite(70, bytes.Repeat([]byte{2}, 5)) }, 4},
		{"abutting the front", func(m *IntervalMap) { m.Overwrite(5, bytes.Repeat([]byte{2}, 5)) }, 4},
		{"splitting the middle one", func(m *IntervalMap) { m.Overwrite(33, bytes.Repeat([]byte{2}, 2)) }, 5},
		{"trimming a head", func(m *IntervalMap) { m.Overwrite(28, bytes.Repeat([]byte{2}, 5)) }, 4},
		{"trimming a tail", func(m *IntervalMap) { m.Overwrite(38, bytes.Repeat([]byte{2}, 5)) }, 4},
		{"replacing one exactly", func(m *IntervalMap) { m.Overwrite(30, bytes.Repeat([]byte{2}, 10)) }, 3},
		{"swallowing all three", func(m *IntervalMap) { m.Overwrite(0, bytes.Repeat([]byte{2}, 70)) }, 1},
		{"spanning two and the gap", func(m *IntervalMap) { m.Overwrite(15, bytes.Repeat([]byte{2}, 20)) }, 4},
		{"clearing the middle one", func(m *IntervalMap) { m.Clear(30, 10) }, 2},
		{"clearing a hole in one", func(m *IntervalMap) { m.Clear(33, 2) }, 4},
		{"clearing everything", func(m *IntervalMap) { m.Clear(0, 70) }, 0},
		{"clearing a gap only", func(m *IntervalMap) { m.Clear(22, 5) }, 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := base()
			tc.drive(m)
			if err := m.checkOrder(); err != nil {
				t.Fatalf("%v", err)
			}
			if got := len(m.extents); got != tc.want {
				t.Fatalf("%d extents afterwards, want %d: %v", got, tc.want, m.extents)
			}
		})
	}
}
