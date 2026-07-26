package recovery_test

import (
	"testing"

	"pgregory.net/rapid"

	"github.com/spin-stack/storage/internal/recovery"
)

// ContiguousEnd is the algebra the durable point is made of, and the whole durability
// argument rests on it answering "how far does the record stream reach from the floor,
// without a hole". It is worth proving against a model rather than against four
// hand-picked layouts: the layouts that break it (overlap, containment, duplicates,
// objects below the floor) are precisely the ones nobody thinks to write down.
//
// The model is a bitmap of covered sequences. The property is the definition: the
// answer is the last sequence of the unbroken run that starts at the floor, and
// floor-1 when the floor itself is not covered.

// modelEnd is the specification, written the slow and obvious way.
func modelEnd(covered map[uint64]bool, floor uint64) uint64 {
	last := floor - 1
	for seq := floor; covered[seq]; seq++ {
		last = seq
	}
	return last
}

func TestContiguousEndMatchesTheModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		floor := rapid.Uint64Range(1, 8).Draw(t, "floor")
		n := rapid.IntRange(0, 12).Draw(t, "objects")

		var objs []recovery.ObjectRun
		covered := map[uint64]bool{}
		for i := range n {
			first := rapid.Uint64Range(1, 24).Draw(t, "first")
			span := rapid.Uint64Range(0, 5).Draw(t, "span")
			last := first + span
			objs = append(objs, recovery.ObjectRun{
				Key: string(rune('a' + i)), First: first, Last: last,
			})
			for seq := first; seq <= last; seq++ {
				covered[seq] = true
			}
		}
		recovery.SortRuns(objs)

		if got, want := recovery.ContiguousEnd(objs, floor), modelEnd(covered, floor); got != want {
			t.Fatalf("ContiguousEnd = %d, the covered set reaches %d (floor %d)", got, want, floor)
		}
	})
}

// TestContiguousEndIsIndependentOfListingOrder: the answer may not depend on the order
// the backend happened to list the objects in. Two calls of the same bucket that
// disagree is the shape that makes one promotion write a floor another one contradicts.
func TestContiguousEndIsIndependentOfListingOrder(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(0, 10).Draw(t, "objects")
		var objs []recovery.ObjectRun
		for i := range n {
			first := rapid.Uint64Range(1, 16).Draw(t, "first")
			last := first + rapid.Uint64Range(0, 4).Draw(t, "span")
			objs = append(objs, recovery.ObjectRun{
				Key: string(rune('a' + i)), First: first, Last: last,
			})
		}
		shuffled := append([]recovery.ObjectRun(nil), objs...)
		rapid.Permutation(shuffled).Draw(t, "listing order")

		recovery.SortRuns(objs)
		recovery.SortRuns(shuffled)
		if a, b := recovery.ContiguousEnd(objs, 1), recovery.ContiguousEnd(shuffled, 1); a != b {
			t.Fatalf("the same objects listed in a different order answer %d and %d", a, b)
		}
	})
}

// TestAddingAnObjectNeverLowersTheDurablePoint: monotonicity. An object arriving late
// (a retried PUT, a slow backend) may extend the run or leave it alone; it may never
// shorten it. A durable point that can go down is a floor that can be written below
// what was already ACKed, and the recovery point is create-only.
func TestAddingAnObjectNeverLowersTheDurablePoint(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(0, 10).Draw(t, "objects")
		floor := rapid.Uint64Range(1, 4).Draw(t, "floor")
		var objs []recovery.ObjectRun
		for i := range n {
			first := rapid.Uint64Range(1, 16).Draw(t, "first")
			last := first + rapid.Uint64Range(0, 4).Draw(t, "span")
			objs = append(objs, recovery.ObjectRun{
				Key: string(rune('a' + i)), First: first, Last: last,
			})
		}
		recovery.SortRuns(objs)
		before := recovery.ContiguousEnd(objs, floor)

		extra := rapid.Uint64Range(1, 16).Draw(t, "late first")
		objs = append(objs, recovery.ObjectRun{
			Key: "late", First: extra, Last: extra + rapid.Uint64Range(0, 4).Draw(t, "late span"),
		})
		recovery.SortRuns(objs)
		if after := recovery.ContiguousEnd(objs, floor); after < before {
			t.Fatalf("a late object lowered the durable point from %d to %d", before, after)
		}
	})
}
