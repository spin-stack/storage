package cow

import "sort"

// Range is a half-open region [Offset, Offset+Length) the map answers with data.
type Range struct {
	Offset uint64
	Length uint64
}

// Ranges reports where this map answers with data, flattened over its base: the base's
// ranges minus this layer's tombstones, plus this layer's own extents, merged.
//
// It exists so a volume can be serialised. Ranges says *where*, Read says *what* — and
// splitting it that way is deliberate, because Read already implements the layering and
// tombstone rules correctly and a serialiser that re-derived the bytes would be a second
// implementation of them.
//
// Both directions of an error here are silent and expensive on the next boot. A range
// reported that the map would read as zeros writes zeros over the real bytes; a range
// omitted loses them. So a range whose bytes happen to be zero is still reported: the
// distinction between "written zeros" and "never written" cannot survive an image that
// does not also carry tombstones, and reporting it costs bytes rather than correctness.
func (m *IntervalMap) Ranges() []Range {
	var out []Range
	if m.layered && m.base != nil {
		// What shows through from underneath is whatever the base holds and this layer
		// has not discarded. A DISCARD over a base that resurrected the base's older
		// bytes is exactly §14.6's failure.
		out = subtract(m.base.Ranges(), m.cleared)
	}
	for _, e := range m.extents {
		out = append(out, Range{Offset: e.start, Length: uint64(len(e.data))})
	}
	return merge(out)
}

// subtract removes every cleared span from rs. Both inputs are sorted and
// non-overlapping, which is what the map maintains.
func subtract(rs []Range, cleared []span) []Range {
	if len(cleared) == 0 {
		return rs
	}
	var out []Range
	for _, r := range rs {
		start, end := r.Offset, r.Offset+r.Length
		cur := start
		for _, c := range cleared {
			if c.end <= cur || c.start >= end {
				continue
			}
			if c.start > cur {
				out = append(out, Range{Offset: cur, Length: c.start - cur})
			}
			if c.end > cur {
				cur = c.end
			}
			if cur >= end {
				break
			}
		}
		if cur < end {
			out = append(out, Range{Offset: cur, Length: end - cur})
		}
	}
	return out
}

// merge sorts and coalesces touching or overlapping ranges, so an image does not carry
// one chunk per WRITE for a guest that wrote sequentially.
func merge(rs []Range) []Range {
	if len(rs) < 2 {
		return rs
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i].Offset < rs[j].Offset })
	out := rs[:1]
	for _, r := range rs[1:] {
		last := &out[len(out)-1]
		if r.Offset <= last.Offset+last.Length {
			if end := r.Offset + r.Length; end > last.Offset+last.Length {
				last.Length = end - last.Offset
			}
			continue
		}
		out = append(out, r)
	}
	return out
}
