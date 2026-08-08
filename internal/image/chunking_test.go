package image_test

import (
	"crypto/rand"
	"testing"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// What DEV-0024 turned out to be about, pinned so the next reader measures instead of
// reasoning — because reasoning is what got it wrong. The entry, and the spec that fed
// it, both assumed a chunk is a padded 64 MiB unit and that a clone writing one sector
// therefore saves nothing. Neither is true of this tree, and the four cases below are the
// probe that established it.
//
// The assertion is on the *bucket* — how many chunk objects exist and how large they are
// — rather than on manifests, because the question is what storage a lineage costs. A
// manifest can name the same object four times and that is the answer, not a caveat.
func TestChunksAreExtentSizedAndDedupByContentAlone(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	var lineage [16]byte
	lineage[0] = 9

	publish := func(volByte byte, build func(m *cow.IntervalMap)) {
		t.Helper()
		v := cow.NewIntervalMap()
		build(v)
		var vol [16]byte
		vol[0] = volByte
		if _, err := image.Publish(ctx, store, rand.Reader, nil,
			image.Ident{Volume: vol, Lineage: lineage}, v, nil, 1, ""); err != nil {
			t.Fatalf("publishing volume %d: %v", volByte, err)
		}
	}

	const small = 512
	// 1. A 512-byte write. If chunks were cut on a grid this would be a padded cell.
	publish(1, func(m *cow.IntervalMap) { m.Overwrite(1<<20, make([]byte, small)) })
	// 2. Another volume of the same lineage, same bytes, same offset.
	publish(2, func(m *cow.IntervalMap) { m.Overwrite(1<<20, make([]byte, small)) })
	// 3. The same bytes at a *different* offset — the case that shows the key is the
	//    content and nothing else, because a key that mixed in the offset would differ.
	publish(3, func(m *cow.IntervalMap) { m.Overwrite(1<<20+small, make([]byte, small)) })
	// 4. Two adjacent 4 KiB writes, which cow merges into one extent — so this is one
	//    8 KiB chunk rather than two of 4 KiB, and that is why boundaries depend on write
	//    history.
	publish(4, func(m *cow.IntervalMap) {
		m.Overwrite(0, make([]byte, 4096))
		m.Overwrite(4096, make([]byte, 4096))
	})

	objs, err := store.List(ctx, "chunks/")
	if err != nil {
		t.Fatal(err)
	}
	sizes := map[int]int{}
	for _, o := range objs {
		body, err := store.Get(ctx, o.Key)
		if err != nil {
			t.Fatal(err)
		}
		sizes[len(body)]++
	}
	// Four publishes, two objects: the 512-byte one shared by volumes 1, 2 and 3, and the
	// merged 8 KiB one from volume 4.
	if len(objs) != 2 {
		t.Fatalf("four publishes left %d chunk objects, want 2 — sizes %v", len(objs), sizes)
	}
	if sizes[small] != 1 {
		t.Errorf("no %d-byte chunk: a small write was padded, so chunks are not extent-sized (sizes %v)", small, sizes)
	}
	if sizes[8192] != 1 {
		t.Errorf("no 8192-byte chunk: two adjacent 4 KiB writes did not merge into one extent (sizes %v)", sizes)
	}
}
