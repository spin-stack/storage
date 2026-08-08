package cow

import (
	"errors"
	"sort"
)

// Range is a half-open region [Offset, Offset+Length) the map answers with data.
type Range struct {
	Offset uint64
	Length uint64
}

// Delta is what a stack of layers states *on its own account*, leaving everything below a
// chosen point to whoever owns it. It is how a volume writes down its own contribution to
// a chain instead of a flattened copy of the chain (image.Manifest).
//
// The two halves are not redundant, and the second is the whole reason this type exists
// where Ranges did not suffice. Data is where these layers answer out of bytes of their
// own. Discarded is where they answer with **zeros they meant** — a range a guest
// DISCARDed over something the layers underneath still hold. A range in neither means
// "ask the layer below", which is the sentence a delta is written in.
//
// Drop Discarded and the format loses the only word it has for erasure: absence would
// mean "defer", a reader would compose the layer over its base, and every range the guest
// discarded would come back as the older bytes underneath it (§14.6). That is data
// resurrection, and it is worse than losing the write — the guest freed those blocks and
// something else may have been told it owns them.
type Delta struct {
	// Data is where these layers hold bytes, merged, in ascending offset order. Read
	// answers each of these offsets out of a layer at or above the inherited point.
	Data []Range
	// Discarded is where these layers explicitly read as zero, merged, in ascending
	// offset order. Disjoint from Data by construction: a range this stack holds bytes
	// for is not a range it discarded.
	Discarded []Range
}

// DeltaOver reports what the layers above inherited state, without flattening inherited
// itself into the answer. A nil inherited means the whole chain belongs to this caller,
// which is every volume that descends from nothing.
//
// It is the serialising half of the layering, and it is the reason Ranges is now written
// in terms of it: Read implements the layering and tombstone rules correctly, and a
// second implementation of them in a serialiser is exactly the kind of divergence that
// shows up as a wrong byte on somebody's next boot.
//
// # Not reaching inherited is refused rather than flattened
//
// A chain that does not pass through the map it was told it inherits from produces a
// perfectly plausible delta — the whole chain's, which is what this code did before there
// were deltas — and that delta is a different statement from the one the caller asked
// for. Refusing costs a stop; guessing costs a manifest that claims another volume's
// ranges as its own, which nothing downstream can detect.
func (m *IntervalMap) DeltaOver(inherited *IntervalMap) (Delta, error) {
	if inherited != nil && m == inherited {
		return Delta{}, errors.New("cow: a view cannot inherit from itself; its whole content would be somebody else's")
	}
	d, reached := m.delta(inherited)
	if !reached {
		return Delta{}, errors.New("cow: this view does not sit over the map it was told it inherits from")
	}
	if inherited == nil {
		// Nothing underneath, so "I hold nothing here" and "this range was discarded" are
		// the same statement and only one of them is worth storing. Reporting the
		// tombstone anyway would be pure accumulation: a root volume would carry every
		// range it ever discarded in every manifest it ever wrote, for a distinction no
		// reader of those manifests can act on.
		d.Discarded = nil
	}
	return d, nil
}

// delta accumulates from the bottom of the chain upward and reports whether it reached
// stop. Reaching it is DeltaOver's to insist on; a partial answer here would be
// indistinguishable from a complete one.
func (m *IntervalMap) delta(stop *IntervalMap) (Delta, bool) {
	if m == stop {
		return Delta{}, true
	}
	if m == nil {
		return Delta{}, false
	}
	var d Delta
	reached := stop == nil
	if m.layered {
		// Only a layered map recurses. An unlayered one answers every read out of its own
		// extents — Read clears the buffer and never consults m.base — so its base is not
		// part of what it states, and neither is anything below it.
		d, reached = m.base.delta(stop)
		// This layer's tombstones hide what the layers below it stated, and state
		// themselves. A DISCARD over a base that resurrected the base's older bytes is
		// §14.6's failure exactly.
		d.Data = subtract(d.Data, m.cleared)
		d.Discarded = merge(append(d.Discarded, spanRanges(m.cleared)...))
	}
	// This layer's own extents win over both, which is newest-wins plus the rule
	// Overwrite already enforces inside one layer: a write over a tombstone ends the
	// tombstone, because this layer holds the bytes now.
	d.Data = merge(append(d.Data, extentRanges(m.extents)...))
	d.Discarded = subtract(d.Discarded, extentSpans(m.extents))
	return d, reached
}

// Ranges reports where this map answers with data, flattened over its whole base chain:
// the base's ranges minus this layer's tombstones, plus this layer's own extents.
//
// It is DeltaOver(nil) with the tombstones dropped, and it is kept as its own name
// because "how big is this view" is a different question from "what does this volume
// state", and two callers ask the first (agent.liveBytes, for the size an operator is
// shown, and this package's own tests).
//
// Both directions of an error here are silent and expensive on the next boot. A range
// reported that the map would read as zeros writes zeros over the real bytes; a range
// omitted loses them. So a range whose bytes happen to be zero is still reported.
func (m *IntervalMap) Ranges() []Range {
	d, _ := m.delta(nil)
	return d.Data
}

// spanRanges converts tombstones to ranges. They are already sorted, merged and
// non-overlapping — cover maintains that — so nothing is re-established here.
func spanRanges(cs []span) []Range {
	out := make([]Range, 0, len(cs))
	for _, c := range cs {
		out = append(out, Range{Offset: c.start, Length: c.end - c.start})
	}
	return out
}

func extentRanges(es []extent) []Range {
	out := make([]Range, 0, len(es))
	for _, e := range es {
		out = append(out, Range{Offset: e.start, Length: uint64(len(e.data))})
	}
	return out
}

func extentSpans(es []extent) []span {
	out := make([]span, 0, len(es))
	for _, e := range es {
		out = append(out, span{start: e.start, end: e.end()})
	}
	return out
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
