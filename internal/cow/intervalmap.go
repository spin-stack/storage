// Package cow implements the copy-on-write read side: an interval map of the
// not-yet-objectized WAL extents (§13.2) and, in Increment 4.4, the 64 KiB segment
// active map. The interval map stores only live written extents (newest wins);
// DISCARD/WRITE_ZEROES clear the range so it reads back as zero (§14.6), which keeps
// memory proportional to the working set, not to the volume size.
package cow

import "sort"

// extent is a contiguous written region [start, start+len(data)).
type extent struct {
	start uint64
	data  []byte
}

func (e extent) end() uint64 { return e.start + uint64(len(e.data)) }

// IntervalMap resolves reads over recent WAL extents. It is not safe for
// concurrent use; the owning volume serializes access.
type IntervalMap struct {
	extents []extent // sorted by start, non-overlapping
}

// NewIntervalMap returns an empty map.
func NewIntervalMap() *IntervalMap { return &IntervalMap{} }

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
	m.removeRange(offset, offset+uint64(len(data)))
	cp := append([]byte(nil), data...)
	m.insert(extent{start: offset, data: cp})
}

// Clear removes any written extent in [offset, offset+length); the range then reads
// as zero. Used for DISCARD and WRITE_ZEROES (§14.6).
func (m *IntervalMap) Clear(offset uint64, length uint64) {
	m.removeRange(offset, offset+length)
}

// Read fills buf from the mapped extents starting at offset; bytes with no live
// extent read as zero.
func (m *IntervalMap) Read(offset uint64, buf []byte) {
	for i := range buf {
		buf[i] = 0
	}
	readEnd := offset + uint64(len(buf))
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
