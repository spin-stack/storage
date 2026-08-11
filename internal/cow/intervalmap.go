// Package cow implements the copy-on-write read side: an interval map of the WAL
// extents this session has written over whatever lies underneath (§13.2). It stores
// only live written extents (newest wins); DISCARD/WRITE_ZEROES clear the range so it
// reads back as zero (§14.6), which keeps memory proportional to the working set,
// not to the volume size. Nothing shrinks it on a timer or in the background: what a
// session wrote stays here until the guest discards it or the volume stops.
//
// This sentence used to promise a second structure — "and, in Increment 4.4, the 64 KiB
// segment active map" (§13.3). `cow.ActiveMap` was built and then deleted on 2026-08-02
// with `SegmentIndex` and the roaring-bitmap dependency: nothing outside its own tests
// ever read one. A package doc that still lists it sends a reader looking for a file
// that is not here, which is the cheapest kind of wrong and the easiest to leave.
package cow

import (
	"errors"
	"slices"
	"sort"
)

// extent is a contiguous written region [start, start+len(data)).
type extent struct {
	start uint64
	data  []byte
}

func (e extent) end() uint64 { return e.start + uint64(len(e.data)) }

// span is a half-open range [start, end) with no data of its own.
type span struct{ start, end uint64 }

// IntervalMap resolves reads over recent WAL extents. It is not safe for
// concurrent use; the owning volume serializes access.
//
// A map may sit *over* a base. That is what makes a truncated WAL resumable: the base
// is the view recovered from the object store — everything up to the durable point —
// and this layer is what the local segments still hold, which is newer and therefore
// wins. Without it, `Read` on a resumed log answers zeros for every range whose local
// segments truncation has reclaimed, silently.
//
// The layering is why `cleared` exists. Over a base, "I hold nothing here" and "this
// range was discarded" stop being the same statement: the first must let the base show
// through, the second must not (§14.6). A tombstone carries no data, so discarding a
// terabyte costs one span — the memory profile this package is built around.
//
// # It is a sorted slice, and the sorting is the index
//
// A review proposed replacing this with an ordered tree, on the grounds that Read scanned
// every extent and Overwrite rebuilt and re-sorted the slice. Both observations were
// right. What a benchmark then said (`scale_test.go`, on a 13th Gen i9-13900HK, measured
// at the *bound* — one volume's share of a machine's memory, `agent.Budget.ViewShare`) is
// that the costs were not where the argument put them, and that a tree was not the fix:
//
//	one 4 KiB guest READ                          extents      before      after
//	  16 MiB bound, 4 KiB writes                    3,971    1,300 ns      70 ns
//	  16 MiB bound,  512 B writes                  26,214    9,000 ns      52 ns
//	 256 MiB bound, 4 KiB writes                   63,550   29,600 ns      71 ns
//	 256 MiB bound,  512 B writes                 419,430  215,000 ns      57 ns
//
//	one 4 KiB guest WRITE over a block it holds
//	  16 MiB bound, 4 KiB writes                    3,971   95,716 ns   4,615 ns
//	  16 MiB bound,  512 B writes                  26,214  482,219 ns  29,124 ns  (n≈16,000 before)
//	 256 MiB bound, 4 KiB writes                   63,550           —  211,646 ns
//	 256 MiB bound,  512 B writes                 419,430           —  1,512 µs
//
//	one 4 KiB guest WRITE of a block it does not hold, averaged over a whole fill
//	   4,000 extents                                        30,382 ns   1,139 ns
//	  16,000 extents                                       152,336 ns     965 ns
//
// The two dashes are not omissions: before this change, building a 63,550-extent map took
// tens of seconds and a 419,430-extent one was not reachable in a benchmark at all, which
// is itself the finding on the row below it.
//
// Three things came out of that, in the order they matter:
//
//  1. **The write path was the emergency, not the read path.** At the 16 MiB bound a
//     single 4 KiB guest write cost 96 µs of CPU — more than the NVMe underneath it — and
//     the cost was linear in the whole map for a write touching one extent of it, so a
//     guest filling its own view spent quadratic CPU doing it and `Log.Resume`, which
//     replays every WAL record through Overwrite, paid the same on every restart. The
//     read at that same bound was 1.3 µs. The concern was formed about the cheaper half.
//  2. **The fix for both was to stop discarding the order and start using it.** The slice
//     was sorted on every mutation and then searched linearly on every read, which is the
//     worst arrangement available: pay for the invariant, use it for nothing. Binary
//     search on both paths made reads flat in extent count — the 419,430-extent corner
//     went from 215 µs to under 100 ns — and left per-write cost at about a microsecond,
//     which is the 4 KiB payload copy this structure owes its caller.
//  3. **A tree would have bought the same asymptotics for a rewrite of everything.**
//     Sorted-and-disjoint is what a tree would have maintained anyway; it was already
//     maintained here. What was missing was not a data structure, it was
//     TestTheExtentsStaySortedAndDisjoint — nothing checked the invariant, so nothing
//     could safely depend on it.
//
// # What is left, stated as numbers rather than as reassurance
//
// **The write path is still linear, and at the largest derived bound it is bad.** A write
// binary-searches in log n and then memmoves every extent record past the one it lands on,
// because this is a slice: 4.6 µs at the 16 MiB bound, and 1.5 *milliseconds* at 256 MiB
// with 512-byte writes (BenchmarkOverwriteAtTheBound). That corner needs a guest writing
// only sector-sized blocks to distinct offsets until it has 419,430 of them, which the
// bound then stops — but it burns a core to get there, on a host shared with fifteen other
// volumes. This is the one measurement that would justify a tree, and it is the only thing
// in this structure still sloping in a way a tree would fix. It is left for the increment
// that has the DST arm and the review to go with it; the benchmark is already here, so the
// improvement will be a diff of two numbers rather than an argument.
//
// **Depth.** Read still recurses into every layer, Freeze adds one per snapshot, and
// nothing collapses the chain (TestASnapshottedVolumeHoldsOneCopyPerSnapshot). Now that
// extent count is flat, depth is the only term left in a *read* that grows with anything —
// about 18 ns a layer — and `controlplane.maxChainDepth` is the policy holding it down.
type IntervalMap struct {
	extents []extent // sorted by start, non-overlapping
	base    *IntervalMap
	// layered is set at construction and never cleared. It, not `base != nil`, is what
	// decides whether Clear records a tombstone — a layer that replayed a DISCARD
	// before its base arrived would otherwise have recorded nothing, and installing the
	// base later would uncover exactly the range the guest discarded.
	layered bool
	cleared []span // sorted, non-overlapping, merged; recorded only when layered

	// liveBytes is the sum of len(e.data) over extents, maintained by insert and
	// removeRange rather than recomputed. See Cost for why it is maintained at all: the
	// number is read at the WAL's flush cadence, and a fold over every extent at that
	// cadence is a scan of the whole working set per guest fsync.
	liveBytes int64
}

// NewIntervalMap returns an empty map.
func NewIntervalMap() *IntervalMap { return &IntervalMap{} }

// NewIntervalMapOver returns an empty map layered over base. Reads fall through to base
// wherever this layer holds nothing and has not cleared the range. A nil base is the
// same as NewIntervalMap.
//
// The base is read, never written: this map does not take ownership of it, and layering
// two maps over one base is legal.
func NewIntervalMapOver(base *IntervalMap) *IntervalMap {
	return &IntervalMap{base: base, layered: true}
}

// SetBase installs the base of a map created by NewIntervalMapOver. It is how a view
// that has been serving from local segments alone adopts the one recovered from the
// object store, which is the whole point of a base layer.
//
// Only legal on a layered map: an unlayered one has been discarding its tombstones, so
// giving it a base now would uncover every range it was told to discard.
func (m *IntervalMap) SetBase(base *IntervalMap) error {
	if !m.layered {
		return errors.New("cow: this map was not built to take a base (use NewIntervalMapOver)")
	}
	m.base = base
	return nil
}

// Cost is what one volume's read view costs, in the numbers that move independently of
// each other. A single number cannot answer both questions an operator has, and this
// package is where they are cheap to answer honestly.
//
//   - Bytes is memory: the payload of every live extent in the chain. It is what grows
//     with the working set.
//   - Extents is the other half of memory: an extent record is ~40 bytes of Go (a uint64
//     and a slice header) plus its allocation — so a million single-sector extents cost
//     about 32 MiB of structure over 4 GiB of payload, and reporting Bytes alone would
//     call that free. It is *not* read latency, and it used to be: Read scanned every
//     extent of every layer, which cost 215 µs for one 4 KiB read against a view of
//     419,430 extents. It now binary-searches the ordering the slice already maintains,
//     and the same read is under 100 ns at every extent count from 100 to 419,430
//     (BenchmarkReadAtTheBound).
//   - Layers is the depth of the chain. It is the number that moves on its own — Freeze
//     (§19) adds a layer and not one byte, so a volume snapshotted a hundred times has the
//     same Bytes and a hundred times the read path — and, since Extents stopped being a
//     latency term, it is *the* latency term: a read still recurses into every layer,
//     measured at roughly 18 ns of depth per layer over a chain holding a fixed 4,032
//     extents (BenchmarkReadThroughALayerChain).
//   - Cleared is the tombstone count. It gets no series of its own: a span is 16 bytes
//     and `cover` merges adjacent ones, so this is bounded by construction rather than
//     by the working set (TestClearsAreMerged is the proof), and a fourth per-volume
//     series that can only ever be small is cardinality bought for nothing.
//
// Rejected: reporting a single `bytes` gauge, which is the shape the deleted `Bytes()`
// had. It answers "how much memory" and silently answers "the read path is fine" for a
// volume whose reads have become a hundred-layer walk.
type Cost struct {
	Bytes   int64
	Extents int
	Layers  int
	Cleared int
}

// ExtentOverheadBytes is what one extent costs on top of its payload, and it is
// measured. Two maps of 100,000 extents each, one holding 4 KiB per extent and one
// holding 512 B — same extent count, eight times the payload — were built and their live
// heap read after a forced GC:
//
//	extents  Bytes      live heap  live-Bytes  per extent  process RSS
//	100,000  409.6 MB   415.0 MB   5.424 MB    54.2 B      874.7 MB
//	100,000   51.2 MB    56.6 MB   5.427 MB    54.3 B      130.4 MB
//	    100  838.9 MB   840.6 MB   1.699 MB    (16 KB)     842.1 MB
//
// The structure term is 54 bytes per extent and does not move with the payload: that is
// the isolation. The 32-byte record in `extents`, the spare capacity `append` keeps ahead
// of it, and the allocator's rounding.
//
// **128, not 54, and the third column is why.** What kills a host is RSS, not the live
// heap, and RSS is where the mutation path shows up: removeRange builds a fresh extent
// slice while the old one is still reachable and append doubles underneath it, so up to
// four copies of the 32-byte record are resident where one is live. Charging 128 makes
// RSS/Memory 2.07 and 2.04 in the two rows above — one constant across an eight-fold
// change in extent density — where charging the live-heap 54 gives 2.11 and 2.30. The
// number that predicts the failure is the one to bound.
//
// Why the term is charged at all: Bytes alone says a million 512-byte extents and one
// 512 MB extent cost the same, and the rows above say they do not. Worse, the map has no
// floor on extent size — removeRange leaves a one-byte remainder when a write lands one
// byte inside an existing extent — so a guest can drive Extents up while Bytes stays
// flat, and a bound watching only Bytes would never see it coming.
const ExtentOverheadBytes = 128

// Memory is what this view costs, in the units that predict the host's RSS: every payload
// byte plus the structure describing it. It is what a memory bound compares against, and
// it is deliberately *not* what read_view_bytes reports — the gauge answers "how much has
// the guest written", this answers "what is that costing".
//
// Measured (see ExtentOverheadBytes), the process RSS attributable to a view is **twice**
// this, and that factor is the Go runtime's, not this package's: a guest writing distinct
// blocks churns the extent slice on every write, so the heap sits at its GOGC goal of
// twice the live heap. A view that is *not* being written — a frozen base, the `big` row
// above — costs 1x, because nothing allocates against it. A caller sizing this against
// real RAM must divide by 2; it is documented here rather than multiplied in, because it
// is a property of the runtime a GOGC or GOMEMLIMIT change moves.
func (c Cost) Memory() int64 {
	return c.Bytes + int64(c.Extents)*ExtentOverheadBytes
}

// Cost reports what this view costs, following the whole base chain.
//
// **It is O(layers), not O(extents), and that is the point.** The number an Agent
// records is read at the WAL's flush cadence — every guest fsync — and the structure it
// describes holds one entry per distinct written region of a live volume. Folding over
// those entries to answer "how big are you" would make the measurement scale with the
// thing it measures, which is a performance defect wearing observability's clothes: the
// fuller the volume, the more the metric costs. So each layer maintains its own byte
// count as it is mutated (one add in insert, one subtract per overlap in removeRange),
// and this walks the chain to sum them.
//
// Walking the chain is not free either — but its length *is* the depth being reported,
// it changes only at Freeze, and a chain long enough for the walk to matter is already
// the condition the Layers gauge exists to make visible.
//
// Two limits, deliberately not papered over. Two maps may share one base ("layering two
// maps over one base is legal"), and each will count that base's bytes as its own, so
// summing Cost across volumes double-counts a shared parent image. And the caller must
// hold whatever serializes mutation of this map — it is exactly as concurrency-unsafe as
// Read, and for the same reason.
func (m *IntervalMap) Cost() Cost {
	c := Cost{Bytes: m.liveBytes, Extents: len(m.extents), Layers: 1, Cleared: len(m.cleared)}
	for b := m.base; b != nil; b = b.base {
		c.Bytes += b.liveBytes
		c.Extents += len(b.extents)
		c.Cleared += len(b.cleared)
		c.Layers++
	}
	return c
}

// Overwrite records a WRITE of data at offset, superseding any overlap (newest wins).
//
// A zero-length write records nothing, which is not a special case so much as the only
// reading of it: it supersedes no byte and ends no tombstone. It used to insert an *empty*
// extent — [offset, offset) — and that was not harmless. It charged the volume a whole
// ExtentOverheadBytes of its read-view bound for a region covering nothing, and it put two
// extents in the slice with the same start, which is what the binary searches in Read and
// removeRange use to order them. `TestWALSegmentReplayProperty` produces these: the WAL
// encodes and replays a zero-length WRITE like any other record.
func (m *IntervalMap) Overwrite(offset uint64, data []byte) {
	if len(data) == 0 {
		return
	}
	end := offset + uint64(len(data))
	m.removeRange(offset, end)
	// A write over a tombstone ends the tombstone: this layer holds the bytes now, and
	// leaving the range marked as discarded would zero the write on the next read.
	m.uncover(offset, end)
	cp := append([]byte(nil), data...)
	m.insert(extent{start: offset, data: cp})
}

// Clear removes any written extent in [offset, offset+length); the range then reads
// as zero. Used for DISCARD and WRITE_ZEROES (§14.6).
//
// Over a base it also records a tombstone, because dropping this layer's extents would
// otherwise uncover the base's older bytes — a DISCARD that resurrects data instead of
// erasing it. With no base there is nothing underneath and nothing to record.
func (m *IntervalMap) Clear(offset uint64, length uint64) {
	end := offset + length
	m.removeRange(offset, end)
	if m.layered {
		m.cover(offset, end)
	}
}

// Read fills buf from the mapped extents starting at offset; bytes with no live
// extent read as zero.
//
// It visits only the extents the read overlaps, per layer. The scan-everything version
// this replaces cost 215 µs for one 4 KiB guest read against a view of 419,430 extents —
// which is not a hypothetical: it is exactly what a 256 MiB read-view bound holds when the
// guest writes 512-byte blocks, and 256 MiB is what a 32 GiB host gives each of 16 volumes
// (agent.Budget.ViewShare). 215 µs of CPU to answer a read that the NVMe underneath would
// have served in a fraction of it is the case that justified an index; the four corners of
// bound × write size are in BenchmarkReadAtTheBound.
//
// The index is the ordering the structure already maintains, not a tree. That is the whole
// finding: `extents` is sorted and disjoint, so the first extent a read can touch is a
// binary search away and the last is the first one starting past its end. A tree would buy
// the same asymptotics for a rewrite of every mutation path.
//
// What it costs is that the ordering becomes load-bearing for *reads*, where it was only
// load-bearing for mutation before: out-of-order extents used to give a wrong byte, and
// now give a missing one. That is a fair trade only because the invariant is checked
// directly — TestTheExtentsStaySortedAndDisjoint — rather than asserted in a comment,
// which is what it was until this change.
//
// The tombstone loop below is deliberately left as a full scan. `cover` merges every span
// it touches, so `cleared` is bounded by construction rather than by the working set
// (TestClearsAreMerged), and a binary search over a list that cannot grow is complexity
// bought for nothing.
func (m *IntervalMap) Read(offset uint64, buf []byte) {
	readEnd := offset + uint64(len(buf))
	if m.layered {
		// The older layer paints the background; everything below overwrites it.
		if m.base != nil {
			m.base.Read(offset, buf)
		} else {
			clear(buf)
		}
		for _, c := range m.cleared {
			if c.end <= offset || c.start >= readEnd {
				continue
			}
			from, to := max64(c.start, offset), min64(c.end, readEnd)
			clear(buf[from-offset : to-offset])
		}
	} else {
		clear(buf)
	}
	for _, e := range m.extents[m.firstPast(offset):] {
		if e.start >= readEnd {
			break
		}
		from := max64(e.start, offset)
		to := min64(e.end(), readEnd)
		copy(buf[from-offset:to-offset], e.data[from-e.start:to-e.start])
	}
}

// firstPast is the index of the first extent whose end is past s — the first one an
// operation on [s, ...) can touch, and len(extents) when there is none.
//
// It is a binary search, and it is correct only because `extents` is sorted by start and
// disjoint: ends are then increasing too, so "does this extent end at or before s" is
// monotone across the slice. TestTheExtentsStaySortedAndDisjoint is what holds that up.
func (m *IntervalMap) firstPast(s uint64) int {
	i, _ := slices.BinarySearchFunc(m.extents, s, func(x extent, s uint64) int {
		if x.end() <= s {
			return -1
		}
		return 1
	})
	return i
}

// removeRange trims/splits/drops any extent overlapping [s, e).
//
// It touches only the extents that overlap, found by binary search, and splices them out
// in one move. The obvious version — walk all of `extents`, copy the survivors into a
// fresh slice, sort it — was measured at 96 µs for one 4 KiB guest write against a view
// holding 3,971 extents, which is what a 16 MiB read-view bound holds and what a 2 GiB
// container gives each of 16 volumes. See BenchmarkOverwriteRewritingAnExistingExtent:
// the cost was linear in the *whole* map for a write that touches one extent of it, so a
// guest filling its view spent quadratic CPU doing it, and `Log.Resume` — which replays
// every WAL record through Overwrite — paid the same on every restart.
func (m *IntervalMap) removeRange(s, e uint64) {
	if s >= e {
		return
	}
	lo := m.firstPast(s)
	// One past the last, which is the first extent that starts at or after e. Scanned
	// rather than searched: these are exactly the extents being removed, so the walk is
	// bounded by the write's own width, not by the map's size.
	hi := lo
	for hi < len(m.extents) && m.extents[hi].start < e {
		hi++
	}
	if lo == hi {
		return // nothing overlaps: the common case for a guest writing where it has not written
	}
	// At most two: only the first and last extent of the window can survive in part, and
	// then only if the window's edge falls inside them.
	remainder := make([]extent, 0, 2)
	for _, x := range m.extents[lo:hi] {
		// What leaves is exactly the overlap: the two remainders below re-add the rest.
		// Counted here, on the extent being dropped, rather than by re-folding what
		// survives at the end — the fold is what Cost exists not to do.
		m.liveBytes -= int64(min64(x.end(), e) - max64(x.start, s))
		// The remainders are *copied* out of x, not re-sliced from it. A re-slice keeps
		// the whole original payload alive — so an extent whose middle was overwritten
		// or discarded goes on holding every byte it was allocated with, while
		// liveBytes counts only what survived. A guest that writes in megabytes and
		// punches holes in kilobytes would then report kilobytes and hold megabytes,
		// which makes any bound on Cost.Bytes a bound on nothing (that is what
		// Limits.MaxViewBytes rests on). TestTrimmingAnExtentReleasesWhatItNoLongerHolds
		// measures the heap and fails at 128 MiB held against 32 KiB reported.
		//
		// It costs a copy of the surviving bytes per trimmed extent, bounded by the
		// extent's own size, on a path that is already moving the extent record.
		// The alternative — count cap(x.data) in liveBytes and keep the re-slice — was
		// rejected: it makes the number honest by making the memory permanent, and two
		// remainders of one array would each have to claim the same bytes.
		// Left remainder [x.start, s).
		if x.start < s {
			remainder = append(remainder, extent{start: x.start, data: slices.Clone(x.data[:s-x.start])})
		}
		// Right remainder [e, x.end()).
		if x.end() > e {
			remainder = append(remainder, extent{start: e, data: slices.Clone(x.data[e-x.start:])})
		}
	}
	// The remainders are already in ascending order — the left one comes from the first
	// extent of the window and the right one from the last — so replacing the window with
	// them keeps the slice sorted without re-establishing it.
	m.extents = slices.Replace(m.extents, lo, hi, remainder...)
}

// insert puts x at its ordered position. Its caller has already removed everything x
// overlaps, so no extent shares its start and the position is unambiguous.
//
// Appending and re-sorting is what this did, and it was the other half of the 96 µs
// write: a full sort.Slice — with a closure per comparison and a reflect-based swap per
// move — over the whole map, to place one record that a binary search finds in log n.
// For the shape a guest most often has, writing past everything it has written, the
// insert is now an append and moves nothing at all.
func (m *IntervalMap) insert(x extent) {
	m.liveBytes += int64(len(x.data))
	i, _ := slices.BinarySearchFunc(m.extents, x.start, func(a extent, start uint64) int {
		switch {
		case a.start < start:
			return -1
		case a.start > start:
			return 1
		default:
			return 0
		}
	})
	m.extents = slices.Insert(m.extents, i, x)
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// cover records [s, e) as explicitly zeroed in this layer, merging with any tombstone it
// touches so adjacent clears stay one span.
func (m *IntervalMap) cover(s, e uint64) {
	if s >= e {
		return
	}
	out := make([]span, 0, len(m.cleared)+1)
	for _, c := range m.cleared {
		if c.end < s || c.start > e {
			out = append(out, c)
			continue
		}
		s, e = min64(s, c.start), max64(e, c.end)
	}
	out = append(out, span{start: s, end: e})
	sort.Slice(out, func(i, j int) bool { return out[i].start < out[j].start })
	m.cleared = out
}

// uncover drops [s, e) from the tombstones, trimming and splitting the spans it cuts.
func (m *IntervalMap) uncover(s, e uint64) {
	if s >= e || len(m.cleared) == 0 {
		return
	}
	// A fresh slice, not m.cleared[:0]: the split case below appends two spans for one
	// input, which would overwrite the next element of the backing array before the
	// loop reads it — silently dropping a tombstone, and with it a discarded range that
	// would then read as whatever the base holds. Found by TestLayeringEqualsFlattening.
	out := make([]span, 0, len(m.cleared)+1)
	for _, c := range m.cleared {
		switch {
		case c.end <= s || c.start >= e: // untouched
			out = append(out, c)
		case c.start < s && c.end > e: // split
			out = append(out, span{c.start, s}, span{e, c.end})
		case c.start < s: // trim the tail
			out = append(out, span{c.start, s})
		case c.end > e: // trim the head
			out = append(out, span{e, c.end})
		}
	}
	m.cleared = out
}
