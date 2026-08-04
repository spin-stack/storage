// Package cow implements the copy-on-write read side: an interval map of the
// not-yet-objectized WAL extents (§13.2). It stores only live written extents (newest
// wins); DISCARD/WRITE_ZEROES clear the range so it reads back as zero (§14.6), which
// keeps memory proportional to the working set, not to the volume size.
//
// This sentence used to promise a second structure — "and, in Increment 4.4, the 64 KiB
// segment active map" (§13.3). `cow.ActiveMap` was built and then deleted on 2026-08-02
// with `SegmentIndex` and the roaring-bitmap dependency: nothing outside its own tests
// ever read one. A package doc that still lists it sends a reader looking for a file
// that is not here, which is the cheapest kind of wrong and the easiest to leave.
package cow

import (
	"errors"
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
// segments truncation has reclaimed, silently (BUILD-INVENTORY increment 5).
//
// The layering is why `cleared` exists. Over a base, "I hold nothing here" and "this
// range was discarded" stop being the same statement: the first must let the base show
// through, the second must not (§14.6). A tombstone carries no data, so discarding a
// terabyte costs one span — the memory profile this package is built around.
type IntervalMap struct {
	extents []extent // sorted by start, non-overlapping
	base    *IntervalMap
	// layered is set at construction and never cleared. It, not `base != nil`, is what
	// decides whether Clear records a tombstone — a layer that replayed a DISCARD
	// before its base arrived would otherwise have recorded nothing, and installing the
	// base later would uncover exactly the range the guest discarded.
	layered bool
	cleared []span // sorted, non-overlapping, merged; recorded only when layered
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
// object store, which is the whole of BUILD-INVENTORY increment 5.
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

// ClearedSpans reports how many tombstones this layer holds. It exists so a test can
// assert that adjacent clears merge rather than accumulate — a guest discarding a volume
// in 4 KiB steps must not grow a list proportional to the volume.
func (m *IntervalMap) ClearedSpans() int { return len(m.cleared) }

// Bytes reports the total live extent bytes held (metric active_map_bytes proxy).
func (m *IntervalMap) Bytes() int {
	n := 0
	for _, e := range m.extents {
		n += len(e.data)
	}
	return n
}

// Overwrite records a WRITE of data at offset, superseding any overlap (newest wins).
func (m *IntervalMap) Overwrite(offset uint64, data []byte) {
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
	for _, e := range m.extents {
		if e.end() <= offset || e.start >= readEnd {
			continue
		}
		from := max64(e.start, offset)
		to := min64(e.end(), readEnd)
		copy(buf[from-offset:to-offset], e.data[from-e.start:to-e.start])
	}
}

// removeRange trims/splits/drops any extent overlapping [s, e).
func (m *IntervalMap) removeRange(s, e uint64) {
	if s >= e {
		return
	}
	var kept []extent
	for _, x := range m.extents {
		if x.end() <= s || x.start >= e {
			kept = append(kept, x) // no overlap
			continue
		}
		// Left remainder [x.start, s).
		if x.start < s {
			kept = append(kept, extent{start: x.start, data: x.data[:s-x.start]})
		}
		// Right remainder [e, x.end()).
		if x.end() > e {
			kept = append(kept, extent{start: e, data: x.data[e-x.start:]})
		}
	}
	sort.Slice(kept, func(i, j int) bool { return kept[i].start < kept[j].start })
	m.extents = kept
}

func (m *IntervalMap) insert(x extent) {
	m.extents = append(m.extents, x)
	sort.Slice(m.extents, func(i, j int) bool { return m.extents[i].start < m.extents[j].start })
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
