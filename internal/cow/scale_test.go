package cow

import (
	"bytes"
	"fmt"
	"testing"
)

// These benchmarks exist to answer one question with numbers instead of intuition: **does
// this structure need an ordered index?**
//
// A review observed that Read was a linear scan of every extent of every layer, and that
// Overwrite rebuilt and re-sorted the extent slice, and proposed a tree. Both observations
// were correct; whether they mattered was an empirical question, because the read view now
// has a bound — `agent.Budget.ViewShare`, one volume's share of the memory the Agent
// measured — so `n` is capped where it used to be capped only by the OOM killer.
//
// The verdict, and the numbers these produced, are recorded on the IntervalMap type. The
// short version is that the write path was two orders of magnitude worse than the read
// path at the same `n`, and that both were fixed by using the ordering the slice already
// maintained rather than by replacing the slice. What these benchmarks are *for*, from
// here on, is keeping that true: every read row is flat in `extents`, and a row that
// starts sloping is the regression.
//
// The bound is what makes the numbers interpretable, so it is the x-axis:
//
//   - 16 MiB is a 2 GiB container serving 16 volumes (0.25 × 2 GiB ÷ 16 ÷ 2). It is the
//     small end an operator actually runs, and the one the adversarial shape caps.
//   - 256 MiB is a 32 GiB host serving 16 volumes — the old `wal.DefaultMaxViewBytes`
//     constant, now the same division applied to a bigger machine.
//
// The bounds are written out rather than imported: `internal/agent` and `internal/wal`
// both import this package, so a test that took the numbers from their source would be an
// import cycle. `TestTheBoundsTheseBenchmarksUseAreStillTheDerivation` is what keeps the
// copies honest — it fails if the arithmetic behind either moves.
const (
	smallBoundBytes = 16 << 20  // a 2 GiB container across 16 volumes
	largeBoundBytes = 256 << 20 // a 32 GiB host across 16 volumes
)

// extentsThatFitIn is how many extents of `size` payload a view may hold under `bound`,
// which is Cost.Memory inverted: every extent costs its payload plus the measured
// ExtentOverheadBytes of structure. It is the conversion that turns a memory bound into
// the `n` these benchmarks scan.
func extentsThatFitIn(bound int64, size int) int {
	return int(bound / int64(size+ExtentOverheadBytes))
}

// The bound this file is anchored to is a division, not a constant, and it lives in
// another package that cannot be imported from here. So the arithmetic is restated: if
// `agent.ViewRatio`, `agent.ViewRSSFactor` or `agent.DefaultMaxVolumes` moves, the numbers
// recorded on IntervalMap were measured at the wrong `n` and this fails.
//
// It asserts on the two machines the numbers were taken at, because "16 MiB" and "256 MiB"
// are only meaningful as *those* machines' answers.
func TestTheBoundsTheseBenchmarksUseAreStillTheDerivation(t *testing.T) {
	// agent.ViewRatio, agent.ViewRSSFactor, agent.DefaultMaxVolumes.
	const (
		viewRatio     = 0.25
		viewRSSFactor = 2
		maxVolumes    = 16
	)
	share := func(memory int64) int64 {
		return int64(viewRatio*float64(memory)) / maxVolumes / viewRSSFactor
	}
	for _, tc := range []struct {
		name   string
		memory int64
		want   int64
	}{
		{"a 2 GiB container", 2 << 30, smallBoundBytes},
		{"a 32 GiB host", 32 << 30, largeBoundBytes},
	} {
		if got := share(tc.memory); got != tc.want {
			t.Errorf("%s across %d volumes gives each a %d-byte view; this file is anchored to %d",
				tc.name, maxVolumes, got, tc.want)
		}
	}
}

// **Sequential writes do not produce fewer extents than sparse ones, and that is the first
// finding.** The premise the index was argued against — "sequential 4 KiB writes give few,
// large, merged extents" — is false of this structure: `insert` appends and sorts, and
// nothing anywhere coalesces an extent with the one it abuts. Only `Ranges`/`DeltaOver`
// merge, and they merge on the way *out*, into a fresh slice, leaving the map untouched.
//
// So `n` is the number of distinct writes the guest has issued, whatever their offsets,
// and the two shapes below differ in address space and not in cost. That is why every
// benchmark here carries both: if a future change makes them differ, the pair says so.
func TestAdjacentWritesAreNotCoalesced(t *testing.T) {
	seq := fill(shapeSequential, 8, blockSize)
	sparse := fill(shapeSparse, 8, blockSize)

	if got := seq.Cost().Extents; got != 8 {
		t.Fatalf("8 abutting 4 KiB writes left %d extents; if they now coalesce, the benchmarks' n is no longer the write count", got)
	}
	if seq.Cost() != sparse.Cost() {
		t.Fatalf("sequential costs %+v and sparse costs %+v: the two shapes have stopped being the same size", seq.Cost(), sparse.Cost())
	}
	// The merge that does exist, so the two statements are not confused: it is on output.
	if got := len(seq.Ranges()); got != 1 {
		t.Fatalf("Ranges reported %d ranges for 8 abutting writes, want 1 merged range", got)
	}
}

type shape int

const (
	// shapeSequential is a guest writing a file: each write abuts the last.
	shapeSequential shape = iota
	// shapeSparse is the adversarial shape — distinct offsets scattered across a large
	// address space, one extent each, which is what a bound on view memory caps.
	shapeSparse
	// shapeDescending writes the same offsets as shapeSparse in reverse. Reads cannot
	// tell it from shapeSparse; the mutation path can, because every insert lands at the
	// front of the slice instead of the end.
	shapeDescending
)

func (s shape) String() string {
	switch s {
	case shapeSequential:
		return "sequential"
	case shapeSparse:
		return "sparse"
	default:
		return "descending"
	}
}

// offsets is where the i-th of n writes of `size` bytes lands.
func (s shape) offsets(n, size int) func(i int) uint64 {
	// Far enough apart that no two writes touch, and not a power of two, so a shape that
	// happened to be cache-friendly on one stride is not what is being measured.
	const sparseStride = 1 << 20
	switch s {
	case shapeSequential:
		return func(i int) uint64 { return uint64(i) * uint64(size) }
	case shapeSparse:
		return func(i int) uint64 { return uint64(i) * sparseStride }
	default:
		return func(i int) uint64 { return uint64(n-1-i) * sparseStride }
	}
}

func fill(s shape, n, size int) *IntervalMap {
	m := NewIntervalMap()
	data := bytes.Repeat([]byte{0xA5}, size)
	at := s.offsets(n, size)
	for i := range n {
		m.Overwrite(at(i), data)
	}
	return m
}

// BenchmarkReadAtExtentCount is the review's claim, measured: is a read's cost a function
// of the extent count? It was — 4,000 extents cost 1.3 µs and 64,000 cost 31 µs — and it
// is not any more. Read binary-searches the ordering the slice maintains, so every row
// here is the same handful of nanoseconds and the series is flat by construction. A row
// that starts climbing with `extents` again means the search was lost.
//
// The x-axis is chosen so the two bounds fall inside it: 3,971 extents is a 16 MiB view of
// 4 KiB extents, 63,550 is a 256 MiB one (BenchmarkReadAtTheBound pins those exactly).
//
// Run: go test ./internal/cow/ -run XXX -bench ReadAtExtentCount
func BenchmarkReadAtExtentCount(b *testing.B) {
	buf := make([]byte, blockSize)
	for _, s := range []shape{shapeSequential, shapeSparse} {
		for _, n := range []int{100, 1000, 4000, 16000, 64000} {
			m := fill(s, n, blockSize)
			b.Run(fmt.Sprintf("%s/extents=%d", s, n), func(b *testing.B) {
				for b.Loop() {
					m.Read(0, buf)
				}
			})
		}
	}
}

// BenchmarkReadWhereTheDataIsNot is the case an index makes free: a read of a range no
// extent covers. It used to cost the same as a read every extent surrounds — 2.7 µs at
// 4,000 extents, 31 µs at 64,000 — because Read visited all of `extents` and skipped each
// one with a comparison. It is now the binary search and nothing else, which is the
// cheapest row in this file and the one that should stay cheapest.
func BenchmarkReadWhereTheDataIsNot(b *testing.B) {
	buf := make([]byte, blockSize)
	for _, n := range []int{4000, 64000} {
		m := fill(shapeSparse, n, blockSize)
		// Past the last extent: nothing to copy, everything to scan.
		off := uint64(n) << 20
		b.Run(fmt.Sprintf("extents=%d", n), func(b *testing.B) {
			for b.Loop() {
				m.Read(off, buf)
			}
		})
	}
}

// BenchmarkReadAtTheBound is the anchor: what one 4 KiB guest read costs on a view that
// has filled its memory bound, at the two machines the bound is derived for and at two
// write sizes. The reported `extents` metric is the count that fits, so the benchmark
// prints the conversion as well as the latency.
//
// 512 B is not a hypothetical worst case: it is the smallest write a virtio-blk guest can
// issue, and `removeRange` can leave remainders smaller still. 512 B × 256 MiB is the
// corner of the four, at 419,430 extents, and it is the one that decides whether an
// ordered index is needed: everything cheaper than it is bounded by the rows above.
func BenchmarkReadAtTheBound(b *testing.B) {
	buf := make([]byte, blockSize)
	for _, bound := range []struct {
		name  string
		bytes int64
	}{
		{"16MiB", smallBoundBytes},
		{"256MiB", largeBoundBytes},
	} {
		for _, size := range []int{blockSize, 512} {
			n := extentsThatFitIn(bound.bytes, size)
			m := fill(shapeSparse, n, size)
			if cost := m.Cost().Memory(); cost > bound.bytes {
				b.Fatalf("%d extents of %d bytes cost %d, over the %d-byte bound they were sized to fill", n, size, cost, bound.bytes)
			}
			b.Run(fmt.Sprintf("bound=%s/write=%dB", bound.name, size), func(b *testing.B) {
				for b.Loop() {
					m.Read(0, buf)
				}
				// After the loop, not before: b.Loop resets the timer on its first call
				// and ResetTimer clears the reported metrics with it, so a count reported
				// up front is silently dropped from the output.
				b.ReportMetric(float64(n), "extents")
			})
		}
	}
}

// BenchmarkReadThroughALayerChain separates the two things a deep chain does. Total extent
// count is held constant and only the depth moves, so what this measures is the *layer*
// term: the recursion, and the fact that every layer repaints the buffer the one above it
// is about to overwrite.
//
// **This is now the only term in a read that grows with anything.** Extent count stopped
// being one when Read learned to binary-search; depth did not, because there is no index
// over layers and nothing collapses the chain. Measured here at roughly 18 ns per layer
// over a fixed 4,032 extents, which is small — and it is 64 snapshots' worth of small,
// against a `controlplane.maxChainDepth` policy that is the only thing bounding it.
//
// BenchmarkReadAtDepth in cost_test.go asks a different question — one extent per layer,
// depth growing — and the two together say whether depth costs anything beyond the extents
// it carries.
func BenchmarkReadThroughALayerChain(b *testing.B) {
	buf := make([]byte, blockSize)
	const total = 4032 // a 16 MiB view of 4 KiB extents, divisible by every depth below
	for _, layers := range []int{1, 8, 64} {
		data := bytes.Repeat([]byte{0xA5}, blockSize)
		view := NewIntervalMap()
		for l := range layers {
			if l > 0 {
				view = freeze(view)
			}
			for i := range total / layers {
				view.Overwrite(uint64(l*(total/layers)+i)<<20, data)
			}
		}
		if got := view.Cost(); got.Layers != layers || got.Extents != total {
			b.Fatalf("built %+v, wanted %d layers holding %d extents between them", got, layers, total)
		}
		b.Run(fmt.Sprintf("extents=%d/layers=%d", total, layers), func(b *testing.B) {
			for b.Loop() {
				view.Read(0, buf)
			}
		})
	}
}

// BenchmarkOverwriteRewritingAnExistingExtent is the *steady-state* mutation: the guest
// rewrites a block it has already written, so `removeRange` drops exactly one extent and
// `insert` puts one back. The extent count does not move, which is what makes it
// measurable with b.Loop at a fixed `n`.
//
// It is the cheap half of the mutation path and it carries the whole of it: the search for
// the window, the splice that removes it, and the ordered insert that puts the new extent
// back. This is the row that was 96 µs at 4,000 extents when the same operation rebuilt
// the whole slice and sorted it twice.
func BenchmarkOverwriteRewritingAnExistingExtent(b *testing.B) {
	data := bytes.Repeat([]byte{0x5A}, blockSize)
	for _, n := range []int{100, 1000, 4000, 16000} {
		m := fill(shapeSparse, n, blockSize)
		at := shapeSparse.offsets(n, blockSize)
		b.Run(fmt.Sprintf("extents=%d", n), func(b *testing.B) {
			i := 0
			for b.Loop() {
				m.Overwrite(at(i%n), data)
				i++
			}
			if got := m.Cost().Extents; got != n {
				b.Fatalf("the map holds %d extents after rewriting in place; it held %d", got, n)
			}
		})
	}
}

// BenchmarkOverwriteAtTheBound is BenchmarkReadAtTheBound's counterpart, and the reason it
// exists is that the read row and the write row stopped agreeing. A read is now a binary
// search and is flat across all four corners; a write is a binary search *and a memmove of
// the extent records past it*, because this is a slice. So the write is still linear in
// the extents that follow the one it lands on, and these four numbers are where that
// matters or does not.
//
// This is the measurement that would justify a tree, if anything does. Nothing else in
// this file is still sloping in a way a tree would fix.
func BenchmarkOverwriteAtTheBound(b *testing.B) {
	data := bytes.Repeat([]byte{0x5A}, blockSize)
	for _, bound := range []struct {
		name  string
		bytes int64
	}{
		{"16MiB", smallBoundBytes},
		{"256MiB", largeBoundBytes},
	} {
		for _, size := range []int{blockSize, 512} {
			n := extentsThatFitIn(bound.bytes, size)
			m := fill(shapeSparse, n, size)
			at := shapeSparse.offsets(n, size)
			b.Run(fmt.Sprintf("bound=%s/write=%dB", bound.name, size), func(b *testing.B) {
				i := 0
				for b.Loop() {
					// Rewriting an existing extent keeps the count at n. The payload is
					// 4 KiB whatever the extents are, because that is the write a guest
					// issues; at 512 B it swallows several extents and leaves one.
					m.Overwrite(at(i%n), data)
					i++
				}
				b.ReportMetric(float64(n), "extents")
			})
		}
	}
}

// BenchmarkFillToExtentCount is the case b.Loop cannot express, because the thing being
// measured is `n` growing: a guest writing blocks it has never written before, which is
// every guest that is filling a volume. One benchmark iteration is one whole fill, and
// `ns/write` is what a single guest write costs on average over it.
//
// Reading it: `ns/write` constant across the rows means a write costs the same whatever
// the map already holds, which is what makes filling a view linear instead of quadratic.
// It was 11.9 µs / 30.4 µs / 152.3 µs down these three sparse rows — the definition of
// quadratic — and is now about a microsecond at all three, which is the 4 KiB payload copy
// the structure owes its caller and very little else.
//
// The descending shape is the one that still slopes, and it is honest about the structure
// this is: every insert lands at the front, so the whole extent slice is memmoved, and at
// 16,000 extents that shows as 4 µs against the ascending 1 µs. A tree would not do that.
// Nothing in the system produces the shape — it is a guest writing strictly backwards,
// never revisiting — and the row is here so that if something ever does, the cost is
// already measured rather than discovered.
func BenchmarkFillToExtentCount(b *testing.B) {
	for _, s := range []shape{shapeSparse, shapeDescending} {
		for _, n := range []int{1000, 4000, 16000} {
			b.Run(fmt.Sprintf("%s/extents=%d", s, n), func(b *testing.B) {
				var m *IntervalMap
				for b.Loop() {
					m = fill(s, n, blockSize)
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/write")
				if got := m.Cost().Extents; got != n {
					b.Fatalf("filled to %d extents, wanted %d", got, n)
				}
			})
		}
	}
}
