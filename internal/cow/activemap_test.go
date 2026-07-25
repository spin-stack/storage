package cow_test

import (
	"testing"

	"github.com/spin-stack/storage/internal/cow"
)

func TestSegmentIndexAndRange(t *testing.T) {
	if cow.SegmentIndex(0) != 0 {
		t.Fatal("offset 0 -> segment 0")
	}
	if cow.SegmentIndex(cow.SegmentSize-1) != 0 {
		t.Fatal("last byte of segment 0 -> segment 0")
	}
	if cow.SegmentIndex(cow.SegmentSize) != 1 {
		t.Fatal("first byte of segment 1 -> segment 1")
	}
	// A 200 KiB write starting mid-segment spans multiple segments.
	first, last := cow.SegmentRange(cow.SegmentSize/2, 200*1024)
	if first != 0 || last != 3 {
		t.Fatalf("range = [%d,%d], want [0,3]", first, last)
	}
}

func TestMarkLookupDiscard(t *testing.T) {
	a := cow.NewActiveMap()
	a.Mark(5, cow.Location{Kind: cow.LocRemote, Ref: "wal/x"})
	if !a.Present(5) {
		t.Fatal("segment 5 should be present")
	}
	loc, ok := a.Lookup(5)
	if !ok || loc.Kind != cow.LocRemote || loc.Ref != "wal/x" {
		t.Fatalf("lookup mismatch: %+v ok=%v", loc, ok)
	}
	a.Discard(5)
	if a.Present(5) {
		t.Fatal("segment 5 should be absent after discard (reads as zero)")
	}
	if _, ok := a.Lookup(5); ok {
		t.Fatal("location should be gone after discard")
	}
}

// TestMemoryBoundedOverHugeVolume is the §13.3 property: a sparse working set over
// a 1 TiB volume (16M possible segments) stays small in memory — nothing allocates
// per possible segment.
func TestMemoryBoundedOverHugeVolume(t *testing.T) {
	a := cow.NewActiveMap()
	const scattered = 10_000
	const universe = 16_000_000 // 1 TiB / 64 KiB
	for i := range scattered {
		seg := uint32((i * (universe / scattered)) % universe) // spread across 1 TiB
		a.Mark(seg, cow.Location{Kind: cow.LocLocalSegment})
	}
	if a.PresentCount() != scattered {
		t.Fatalf("present count = %d, want %d", a.PresentCount(), scattered)
	}
	// Memory must be a small fraction of the 16M-entry universe, not proportional
	// to it. A naive [16M]bool would be 16 MB; assert we are well under 1 MB.
	if b := a.Bytes(); b > 1<<20 {
		t.Fatalf("active map uses %d bytes for %d scattered segments; expected < 1 MiB", b, scattered)
	}
}
