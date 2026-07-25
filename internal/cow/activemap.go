package cow

import "github.com/RoaringBitmap/roaring/v2"

// SegmentSize is the CoW segment granularity (§4, §13.1): 64 KiB. The WAL logs
// real guest extents; segments are the unit of objectization and the active map.
const SegmentSize = 64 * 1024

// LocationKind says where a present segment's data currently lives. Actual
// fetching from these sources is Phase 06/10; Phase 04 records the classification.
type LocationKind uint8

const (
	// LocLocalSegment: an objectized segment cached on local NVMe.
	LocLocalSegment LocationKind = iota
	// LocCheckpoint: inside a checkpoint object.
	LocCheckpoint
	// LocRemote: only in S3, must be fetched.
	LocRemote
)

// Location points at where a segment's bytes are.
type Location struct {
	Kind LocationKind
	Ref  string // object key / cache path, filled by later phases
}

// ActiveMap is the memory-bounded presence map over 64 KiB segments (§13.3): a
// roaring bitmap of present segment indices plus a location table only for present
// segments. A 1 TiB volume has 16M possible segments; nothing here allocates per
// possible segment — memory tracks the working set. Not safe for concurrent use.
type ActiveMap struct {
	present *roaring.Bitmap
	loc     map[uint32]Location
}

// NewActiveMap returns an empty active map.
func NewActiveMap() *ActiveMap {
	return &ActiveMap{present: roaring.New(), loc: map[uint32]Location{}}
}

// SegmentIndex returns the 64 KiB segment index containing a byte offset. The
// uint32 index supports volumes up to 256 TiB (2^32 segments), well beyond MVP.
func SegmentIndex(offset uint64) uint32 { return uint32(offset / SegmentSize) }

// SegmentRange returns the inclusive range of segment indices spanning
// [offset, offset+length).
func SegmentRange(offset, length uint64) (first, last uint32) {
	if length == 0 {
		i := SegmentIndex(offset)
		return i, i
	}
	return SegmentIndex(offset), SegmentIndex(offset + length - 1)
}

// Mark records that a segment is present at loc.
func (a *ActiveMap) Mark(seg uint32, loc Location) {
	a.present.Add(seg)
	a.loc[seg] = loc
}

// Discard marks a segment absent (its range reads as zero, §14.6).
func (a *ActiveMap) Discard(seg uint32) {
	a.present.Remove(seg)
	delete(a.loc, seg)
}

// Lookup returns a segment's location and whether it is present. An absent
// segment reads as zero.
func (a *ActiveMap) Lookup(seg uint32) (Location, bool) {
	l, ok := a.loc[seg]
	return l, ok
}

// Present reports whether a segment is present.
func (a *ActiveMap) Present(seg uint32) bool { return a.present.Contains(seg) }

// PresentCount is the number of present segments.
func (a *ActiveMap) PresentCount() uint64 { return a.present.GetCardinality() }

// Bytes estimates the map's memory footprint (bitmap + location table), for the
// active_map_bytes metric (§10.1, §13.3).
func (a *ActiveMap) Bytes() uint64 {
	// Bitmap serialized size + a coarse per-entry estimate for the location table.
	const perEntry = 8 /*key*/ + 1 /*kind*/ + 16 /*ref header*/
	return a.present.GetSizeInBytes() + uint64(len(a.loc))*perEntry
}
