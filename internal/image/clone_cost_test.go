package image_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// What a clone costs, in bytes that are actually in the bucket.
//
// This file was the scales CHUNK-ADDRESSING-SPEC §8 was weighed on: it measured the
// duplicate a clone paid when chunks were keyed `image/<volume>/chunks/<digest>` — a full
// copy of the inherited dataset at the clone's first stop, 8 MiB for a 512-byte write.
// The owner decided (2026-08-07) and the chunk store moved to the lineage, so the same
// scales now weigh what that bought, case for case. The numbers below are the ones that
// changed, and they are asserted as equalities rather than ratios so a reader can put
// their own golden image's size in.
//
// **Everything is asserted on the bucket**, never on the manifest `image` returns: a
// `uploadChunks` that built the right manifest and transferred the wrong bytes satisfies
// every assertion about a manifest, and transferring bytes is the whole subject.
//
// Three numbers, because the decision needs three different questions answered and one
// number answers none of them honestly:
//
//   - **transferred** — what this operation handed to the store, from a decorator over
//     `Put`. It is what a stop spends its time on, and it is what an operator watching a
//     clone hang for two minutes is waiting for.
//   - **resident** — what the bucket holds afterwards, from `List`. It is what the bucket
//     is billed for, and it is not the same number: a chunk the `Head` skip avoids
//     re-uploading is resident and costs no transfer.
//   - **distinct** — what the same content would occupy in one *bucket-wide*
//     content-addressed namespace, folded by the digest each key ends in. It used to be
//     the saving on the table; now the lineage prefix has taken that saving and what is
//     left of `resident - distinct` is duplication between volumes that share no key and
//     therefore cannot share an object.
//
// The sizes here are small enough to run in the unit lane and the cost is exactly linear
// in the parent's dataset — every assertion below is an equality against the dataset size,
// not a ratio — so multiply by whatever a real golden image weighs.

const (
	// datasetBytes is one parent's data. It stays under image.MaxChunkBytes on purpose:
	// a golden image smaller than 64 MiB is *one chunk*, which is the case that decides
	// how much the lineage prefix can save (see TestACloneFirstStopPaysForWhatItTouched).
	datasetBytes = 8 << 20
	// regionBytes is one region of a fragmented parent — a guest filesystem that has been
	// used rather than freshly imaged. Eight of them make the same dataset out of eight
	// chunks instead of one, which is the only variable that changes the answer.
	regionBytes = 1 << 20
	sectorBytes = 512
	// sealOverhead is what <nonce:12><ct><tag:16> adds to a chunk object. It is per
	// *chunk*, so a fragmented volume pays it eight times, which is the one way the
	// measurements below are not exactly linear in the data.
	sealOverhead = crypto.NonceSize + crypto.TagSize
)

// sealed is the size of the object a plaintext run of n bytes becomes.
func sealed(n int64) int64 { return n + sealOverhead }

type putRecord struct {
	key   string
	bytes int64
}

// meteredStore records what every PUT carried, so a measurement can attribute bytes to
// the operation that paid for them rather than to a volume's whole life.
//
// It counts every PUT, including one the store refuses on a precondition, because the
// bytes leave the host either way — a refused create-only PUT is a transfer that bought
// nothing, and hiding it would flatter exactly the case (two writers, same content) the
// content-addressing is defended by.
type meteredStore struct {
	objectstore.Store
	puts []putRecord
}

func metered(s objectstore.Store) *meteredStore { return &meteredStore{Store: s} }

func (s *meteredStore) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	s.puts = append(s.puts, putRecord{key: key, bytes: int64(len(data))})
	return s.Store.Put(ctx, key, data, opts)
}

// mark and chunksSince bracket one publish. Chunk objects only: a manifest is structural
// metadata whose size is decided by how many chunks there are, and folding it into the
// data cost would make a fragmented volume look like it stored more guest data.
func (s *meteredStore) mark() int { return len(s.puts) }

// The prefix a caller passes is a *lineage's* chunk prefix (image.ChunksPrefix), not a
// volume's: since the key space moved, "what did this volume's stop transfer" and "under
// whose prefix did it land" are different questions, and every clone below answers the
// second with its parent's id.
func (s *meteredStore) chunksSince(mark int, prefix string) (objects int, transferred int64) {
	for _, p := range s.puts[mark:] {
		if strings.HasPrefix(p.key, prefix) {
			objects++
			transferred += p.bytes
		}
	}
	return objects, transferred
}

// bucketChunks folds every chunk object in the bucket two ways: what is there, and what
// one bucket-wide content-addressed namespace would have held for the same content.
//
// The fold is by the digest the key ends in, which is the plaintext digest — so two
// volumes holding identical bytes collapse to one entry here and to as many objects as
// there are *lineages* holding them. `resident - distinct` used to be what moving the key
// space would save; now it is what is left after moving it, which is the duplication
// between lineages that only a bucket-wide DEK could remove (see the package doc for why
// that is not on offer).
func bucketChunks(t *testing.T, store objectstore.Store) (objects int, resident, distinct int64) {
	t.Helper()
	// "chunks/" and not "image/": chunk objects are no longer under any volume's image
	// prefix, they are under their lineage's. The literal is spelled out rather than
	// taken from image.ChunksPrefix because this measures *where the bytes landed*, and
	// deriving the place from the code under measurement would measure nothing.
	objs, err := store.List(t.Context(), "chunks/")
	if err != nil {
		t.Fatalf("listing the bucket: %v", err)
	}
	byDigest := map[string]int64{}
	for _, o := range objs {
		objects++
		resident += o.Size
		byDigest[o.Key[strings.LastIndex(o.Key, "/")+1:]] = o.Size
	}
	for _, n := range byDigest {
		distinct += n
	}
	return objects, resident, distinct
}

// volumeID gives each role in a measurement its own id, so a key names the volume that
// paid for it. The version and variant nibbles come from vol7 (INV-22).
func volumeID(n byte) [16]byte {
	v := vol7()
	v[0] = n
	return v
}

// publishParent writes a volume's data, publishes its image and freezes a snapshot under
// snapID — the shape controlplane.Clone descends from. The snapshot names chunks the
// volume's own image already stored, so it transfers a manifest and no chunk bytes; that
// is asserted once, in TestASecondStopPaysOnlyForWhatItTouched, rather than assumed here.
func publishParent(t *testing.T, store objectstore.Store, vol [16]byte, write func(*cow.IntervalMap), snapID string) {
	t.Helper()
	view := cow.NewIntervalMap()
	write(view)
	// A parent descends from nothing, so it is its own lineage root and its chunks name
	// the prefix every clone below it will write into.
	if _, err := image.Publish(t.Context(), store, rand.Reader, encFor(t, vol), image.OwnLineage(vol), view, 1, ""); err != nil {
		t.Fatalf("publishing the parent's image: %v", err)
	}
	if _, err := image.PublishSnapshot(t.Context(), store, rand.Reader, encFor(t, vol), image.OwnLineage(vol), view, 1, snapID); err != nil {
		t.Fatalf("publishing the parent's snapshot: %v", err)
	}
}

// cloneView is what an Agent hands image.Publish at a clone's first stop.
//
// agent.fetchBase makes the parent's snapshot the base *outright* in its ErrNotPublished
// arm and wal.Log's own map is the layer over it, so this is that stack built directly.
// Building it here rather than driving a VolumeManager keeps the subject of the
// measurement `uploadChunks` walking `view.Ranges()`; internal/agent's
// TestAnOperatorIsToldWhenAStopIsCopyingItsParentsDataset drives the real manager over the
// same shape and its bucket numbers agree.
func cloneView(t *testing.T, store objectstore.Store, parent [16]byte, snapID string) *cow.IntervalMap {
	t.Helper()
	return cloneViewOver(t, store, image.OwnLineage(parent), snapID)
}

// cloneViewOver is cloneView for an ancestor that is not the root of its own lineage —
// the middle link of a chain, whose snapshot manifest lives under its own prefix and
// whose chunks live under the root's.
func cloneViewOver(t *testing.T, store objectstore.Store, parent image.Ident, snapID string) *cow.IntervalMap {
	t.Helper()
	// The parent's DEK, which a clone inherits (controlplane.Clone). The Ident says both
	// halves of where the snapshot is: the manifest under the parent's own prefix, the
	// chunks under the lineage's — which for a parent that descends from nothing are the
	// same id.
	base, _, err := image.LoadSnapshot(t.Context(), store, encFor(t, parent.Volume), parent, snapID)
	if err != nil {
		t.Fatalf("loading the parent's snapshot: %v", err)
	}
	return cow.NewIntervalMapOver(base)
}

func fill(v *cow.IntervalMap, off uint64, n int, b byte) {
	v.Overwrite(off, bytes.Repeat([]byte{b}, n))
}

// A clone's first stop pays for the chunks it **touched**, and for nothing else.
//
// This is the property the key-space change was bought for, and the four cases are the
// same four the old key space was priced on, so the two sets of numbers can be read
// against each other:
//
//   - a clone that stops without writing anything transfers **zero bytes**. It used to
//     transfer its parent's whole dataset.
//   - a clone that writes one sector into a **one-chunk** parent still pays the whole
//     8 MiB, and no key space can give that back: its chunk holds different bytes, so it
//     is a different digest and a genuinely new object. With MaxChunkBytes at 64 MiB the
//     golden image the workflow is built around is one chunk, so this is the case to
//     price a fleet from.
//   - the same 8 MiB in eight regions is eight chunks, and the same single-sector write
//     leaves seven of them untouched: the clone transfers one region, not eight.
//   - a clone that writes past the end of what it inherited transfers only its own new
//     range.
//
// So "the storage cost of a clone becomes proportional to what the clone wrote" is true
// at **chunk granularity**, which is the honest form of CHUNK-ADDRESSING-SPEC §3's claim
// and the form the measurement supports.
//
// Every clone here publishes with `Lineage: parent` — that is what a clone's Ident is,
// and the Agent derives it by walking (agent.fetchBase). Plant `Lineage: clone` in the
// Publish below and every case goes back to the old numbers, which is the plant that
// proves these assertions are about the key space and not about arithmetic.
func TestACloneFirstStopPaysForWhatItTouched(t *testing.T) {
	contiguous := func(v *cow.IntervalMap) { fill(v, 0, datasetBytes, 0xA1) }
	fragmented := func(v *cow.IntervalMap) {
		// Eight regions with gaps, which is what a used filesystem looks like and what
		// makes Ranges() report eight runs instead of one. Each region holds different
		// bytes: eight identical regions would collapse to one object under the *same*
		// volume's prefix, because the key is the digest — real dedup, and not the
		// question here.
		for i := range uint64(datasetBytes / regionBytes) {
			fill(v, i*2*regionBytes, regionBytes, byte(0xA1+i))
		}
	}

	tests := []struct {
		name string
		// parent and clone are the two volumes' own writes. The clone's run over a base
		// that already holds the parent's, exactly as a clone's first session does.
		parent, clone func(*cow.IntervalMap)
		// what the clone's first stop transferred into the lineage's chunk store
		wantObjects     int
		wantTransferred int64
		// what the bucket then holds, and what one bucket-wide content-addressed
		// namespace would have held for the same content
		wantResident, wantDistinct int64
		why                        string
	}{
		{
			name: "a clone that stops without writing anything",
			// Nothing at all, where the per-volume key space charged a full duplicate.
			parent: contiguous, clone: func(*cow.IntervalMap) {},
			wantObjects: 0, wantTransferred: 0,
			wantResident: sealed(datasetBytes), wantDistinct: sealed(datasetBytes),
			why: "a clone that wrote nothing stores nothing: its manifest names its parent's chunk",
		},
		{
			name:   "a clone that writes one sector into a one-chunk parent",
			parent: contiguous, clone: func(v *cow.IntervalMap) { fill(v, 0, sectorBytes, 0xC2) },
			wantObjects: 1, wantTransferred: sealed(datasetBytes),
			// resident == distinct: the clone's chunk holds different bytes from the
			// parent's, so it is new content and not a duplicate of anything.
			wantResident: 2 * sealed(datasetBytes), wantDistinct: 2 * sealed(datasetBytes),
			why: "512 bytes of new information still cost 8 MiB, because the chunk they fall in is 8 MiB",
		},
		{
			name:   "a clone that writes one sector into an eight-chunk parent",
			parent: fragmented, clone: func(v *cow.IntervalMap) { fill(v, 0, sectorBytes, 0xC2) },
			// One region, not eight: the other seven are byte-identical to the parent's
			// and the Head skip finds them under the lineage's prefix.
			wantObjects: 1, wantTransferred: sealed(regionBytes),
			wantResident: (datasetBytes/regionBytes + 1) * sealed(regionBytes),
			wantDistinct: (datasetBytes/regionBytes + 1) * sealed(regionBytes),
			why:          "the same data in eight chunks means the clone pays for one of them",
		},
		{
			name:   "a clone that overwrites every byte it inherited",
			parent: contiguous, clone: func(v *cow.IntervalMap) { fill(v, 0, datasetBytes, 0xC2) },
			wantObjects: 1, wantTransferred: sealed(datasetBytes),
			wantResident: 2 * sealed(datasetBytes), wantDistinct: 2 * sealed(datasetBytes),
			why: "it pays for everything, and every byte it uploaded is a byte it wrote",
		},
		{
			name:   "a clone that writes past the end of what it inherited",
			parent: contiguous, clone: func(v *cow.IntervalMap) { fill(v, 2*datasetBytes, sectorBytes, 0xC2) },
			// Two ranges, one new object: the inherited run is skipped whole.
			wantObjects: 1, wantTransferred: sealed(sectorBytes),
			wantResident: sealed(datasetBytes) + sealed(sectorBytes),
			wantDistinct: sealed(datasetBytes) + sealed(sectorBytes),
			why:          "a disjoint write costs the disjoint write",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := metered(sim.NewObjectStore())
			parent, clone := volumeID(0xAA), volumeID(0xCC)
			const snapID = "01930000-0000-7000-8000-00000000000a"

			publishParent(t, store, parent, tc.parent, snapID)

			view := cloneView(t, store, parent, snapID)
			tc.clone(view)
			at := store.mark()
			// A clone's Ident: its own manifest, its parent's chunk store.
			cloneID := image.Ident{Volume: clone, Lineage: parent}
			if _, err := image.Publish(t.Context(), store, rand.Reader, encFor(t, clone), cloneID, view, 2, ""); err != nil {
				t.Fatalf("the clone's first stop: %v", err)
			}

			objects, transferred := store.chunksSince(at, image.ChunksPrefix(parent))
			all, resident, distinct := bucketChunks(t, store)
			t.Logf("the clone's first stop transferred %d chunk objects, %d bytes; the bucket holds %d objects, %d bytes, of which %d bytes are distinct content (%s)",
				objects, transferred, all, resident, distinct, tc.why)

			if objects != tc.wantObjects || transferred != tc.wantTransferred {
				t.Errorf("the clone's first stop transferred %d objects / %d bytes, want %d / %d",
					objects, transferred, tc.wantObjects, tc.wantTransferred)
			}
			if resident != tc.wantResident {
				t.Errorf("the bucket holds %d chunk bytes, want %d", resident, tc.wantResident)
			}
			if distinct != tc.wantDistinct {
				t.Errorf("the same content is %d distinct bytes, want %d — %s", distinct, tc.wantDistinct, tc.why)
			}
		})
	}
}

// A stop transfers what that stop touched, and a superseded chunk is never reclaimed.
//
// The first half is the key-space change seen from the other end: a clone's *first* stop
// used to be the expensive one and is now free, so what is left to measure is the ordinary
// cost of writing — a stop pays for the chunks its writes fell in, whole, and pays it
// again every time it touches them.
//
// The second half is what nothing deletes. A later stop re-uploads a touched chunk under a
// new digest, and **nothing in this repository deletes an object** (the objectstore.Store
// interface has a Delete; no production caller reaches it). So the superseded copy stays
// for ever — a cost per *stop*, unbounded in time, next to which the per-link duplication
// the chain-depth decision was about was a single payment.
//
// **Reachability is now a lineage-wide question, and that is the part a reader should take
// away.** The unreferenced chunk below is only unreferenced because *no manifest in the
// lineage* names it: the parent's image, the parent's snapshot, the clone's image and the
// clone's snapshot all have to be folded together to say so. Under the old key space one
// volume's manifests answered it. This is exactly the cost DELETION-AND-RECLAIM-SPEC §3
// prices for reclaim, made concrete.
func TestASecondStopPaysOnlyForWhatItTouched(t *testing.T) {
	store := metered(sim.NewObjectStore())
	parent, clone := volumeID(0xAA), volumeID(0xCC)
	cloneID := image.Ident{Volume: clone, Lineage: parent}
	const parentSnap = "01930000-0000-7000-8000-00000000000a"
	const cloneSnap = "01930000-0000-7000-8000-00000000000c"
	chunks := image.ChunksPrefix(parent)

	publishParent(t, store, parent, func(v *cow.IntervalMap) { fill(v, 0, datasetBytes, 0xA1) }, parentSnap)

	view := cloneView(t, store, parent, parentSnap)
	at := store.mark()
	etag, err := image.Publish(t.Context(), store, rand.Reader, encFor(t, clone), cloneID, view, 2, "")
	if err != nil {
		t.Fatalf("the clone's first stop: %v", err)
	}
	if objs, first := store.chunksSince(at, chunks); first != 0 {
		t.Errorf("the clone's first stop transferred %d chunk objects / %d bytes, want 0 — every chunk it inherited is already in its lineage's store", objs, first)
	}

	// A snapshot of the clone, immediately.
	at = store.mark()
	if _, err := image.PublishSnapshot(t.Context(), store, rand.Reader, encFor(t, clone), cloneID, view, 2, cloneSnap); err != nil {
		t.Fatalf("snapshotting the clone: %v", err)
	}
	if objs, n := store.chunksSince(at, chunks); n != 0 {
		t.Errorf("a snapshot taken straight after the first stop transferred %d chunk objects / %d bytes, want 0", objs, n)
	}

	// And a second stop that changed nothing.
	at = store.mark()
	etag, err = image.Publish(t.Context(), store, rand.Reader, encFor(t, clone), cloneID, view, 3, etag)
	if err != nil {
		t.Fatalf("the clone's second stop: %v", err)
	}
	if objs, n := store.chunksSince(at, chunks); n != 0 {
		t.Errorf("a second stop with no writes transferred %d chunk objects / %d bytes, want 0", objs, n)
	}

	// A stop that wrote one sector pays for the whole chunk that sector fell in.
	fill(view, 0, sectorBytes, 0xC2)
	at = store.mark()
	etag, err = image.Publish(t.Context(), store, rand.Reader, encFor(t, clone), cloneID, view, 4, etag)
	if err != nil {
		t.Fatalf("the clone's third stop: %v", err)
	}
	if _, again := store.chunksSince(at, chunks); again != sealed(datasetBytes) {
		t.Errorf("a stop that wrote one sector transferred %d bytes, want %d", again, sealed(datasetBytes))
	}

	// And a fourth stop, touching the same chunk somewhere else, leaves the third stop's
	// chunk named by nothing at all. It has to be the *clone's own* superseded chunk: the
	// one the third stop replaced is its parent's, and its parent still names it.
	fill(view, sectorBytes, sectorBytes, 0xC3)
	if _, err := image.Publish(t.Context(), store, rand.Reader, encFor(t, clone), cloneID, view, 5, etag); err != nil {
		t.Fatalf("the clone's fourth stop: %v", err)
	}

	// The bucket, from the outside, folded against every manifest in the lineage.
	objs, err := store.List(t.Context(), chunks)
	if err != nil {
		t.Fatal(err)
	}
	named := map[string]bool{}
	for _, man := range lineageManifests(t, store, parent, parentSnap, cloneID, cloneSnap) {
		for _, c := range man.Chunks {
			named[c.Digest] = true
		}
	}
	var orphaned int64
	for _, o := range objs {
		if !named[o.Key[strings.LastIndex(o.Key, "/")+1:]] {
			orphaned += o.Size
		}
	}
	t.Logf("after four stops the lineage's chunk store holds %d objects; %d bytes are named by no manifest in the lineage and nothing deletes them", len(objs), orphaned)
	if orphaned != sealed(datasetBytes) {
		t.Errorf("%d bytes in the lineage's chunk store are unreferenced, want %d — a superseded chunk is never reclaimed", orphaned, sealed(datasetBytes))
	}
}

// lineageManifests is every manifest that can name a chunk in this lineage: each volume's
// image and each volume's snapshot. Nothing in the tree does this fold yet — reclaim is
// DELETION-AND-RECLAIM-SPEC's, unimplemented — which is why it is spelled out here rather
// than called.
func lineageManifests(t *testing.T, store objectstore.Store, parent [16]byte, parentSnap string, clone image.Ident, cloneSnap string) []image.Manifest {
	t.Helper()
	var out []image.Manifest
	_, parentImage, _, err := image.Load(t.Context(), store, encFor(t, parent), image.OwnLineage(parent))
	if err != nil {
		t.Fatalf("loading the parent's image: %v", err)
	}
	_, cloneImage, _, err := image.Load(t.Context(), store, encFor(t, clone.Volume), clone)
	if err != nil {
		t.Fatalf("loading the clone's image: %v", err)
	}
	out = append(out, parentImage, cloneImage)
	for _, snap := range []struct {
		vol [16]byte
		id  string
	}{{parent, parentSnap}, {clone.Volume, cloneSnap}} {
		man, err := image.ReadSnapshotManifest(t.Context(), store, snap.vol, snap.id)
		if err != nil {
			t.Fatalf("reading snapshot %s of %x: %v", snap.id, snap.vol[0], err)
		}
		out = append(out, man)
	}
	return out
}

// One copy per **lineage**, which is the sentence this whole increment changed.
//
// The same three shapes cost what they cost because of what they share a key with. A chain
// of three and a golden image with three clones now hold **one** copy of the dataset each,
// where the per-volume key space made them three and four. Two volumes that were never
// related still hold two copies of identical bytes, and that is not a defect to fix later:
// they have different DEKs, so they cannot share an object, and the only way they could
// would be a bucket-wide key — which is what "volume delete = crypto-shred" is spent on.
//
// *Plant:* publish the clones with `Lineage: clone` and the first two cases go back to
// three and four copies.
func TestTheBucketHoldsOneCopyPerLineage(t *testing.T) {
	data := func(v *cow.IntervalMap) { fill(v, 0, datasetBytes, 0xA1) }
	const rootSnap = "01930000-0000-7000-8000-00000000000a"
	const midSnap = "01930000-0000-7000-8000-00000000000b"

	// publishClone stops a clone of parent's snapshot, having written nothing, and
	// optionally freezes a snapshot of it for the next link. root is the lineage's, which
	// for a clone of a clone is *not* its parent.
	publishClone := func(t *testing.T, store objectstore.Store, root, parent [16]byte, snapID string, clone [16]byte, ownSnap string) {
		t.Helper()
		view := cloneViewOver(t, store, image.Ident{Volume: parent, Lineage: root}, snapID)
		id := image.Ident{Volume: clone, Lineage: root}
		if _, err := image.Publish(t.Context(), store, rand.Reader, encFor(t, clone), id, view, 2, ""); err != nil {
			t.Fatalf("stopping clone %x: %v", clone[0], err)
		}
		if ownSnap == "" {
			return
		}
		if _, err := image.PublishSnapshot(t.Context(), store, rand.Reader, encFor(t, clone), id, view, 2, ownSnap); err != nil {
			t.Fatalf("snapshotting clone %x: %v", clone[0], err)
		}
	}

	tests := []struct {
		name  string
		build func(*testing.T, objectstore.Store)
		// copies is how many byte-identical copies of the same dataset the bucket ends
		// up holding.
		copies int64
		why    string
	}{
		{
			name: "a chain of three",
			build: func(t *testing.T, store objectstore.Store) {
				root := volumeID(0xA0)
				publishParent(t, store, root, data, rootSnap)
				publishClone(t, store, root, root, rootSnap, volumeID(0xB0), midSnap)
				publishClone(t, store, root, volumeID(0xB0), midSnap, volumeID(0xC0), "")
			},
			copies: 1,
			why:    "every link writes into the root's chunk store and finds the bytes already there",
		},
		{
			name: "one golden image, three clones",
			build: func(t *testing.T, store objectstore.Store) {
				root := volumeID(0xA0)
				publishParent(t, store, root, data, rootSnap)
				for _, id := range []byte{0xB1, 0xB2, 0xB3} {
					publishClone(t, store, root, root, rootSnap, volumeID(id), "")
				}
			},
			copies: 1,
			why:    "§2's primary workflow: N clones of one image is one copy",
		},
		{
			name: "two volumes that were never related and hold the same bytes",
			build: func(t *testing.T, store objectstore.Store) {
				publishParent(t, store, volumeID(0xA0), data, rootSnap)
				publishParent(t, store, volumeID(0xD0), data, midSnap)
			},
			copies: 2,
			why:    "different DEKs cannot share a ciphertext, so identical plaintexts are two objects",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := sim.NewObjectStore()
			tc.build(t, store)

			objects, resident, distinct := bucketChunks(t, store)
			t.Logf("%d chunk objects, %d resident bytes, %d distinct bytes (%s)", objects, resident, distinct, tc.why)

			if want := tc.copies * sealed(datasetBytes); resident != want {
				t.Errorf("the bucket holds %d chunk bytes, want %d (%d copies)", resident, want, tc.copies)
			}
			if distinct != sealed(datasetBytes) {
				t.Errorf("the bucket holds %d distinct bytes, want %d — every copy is byte-identical", distinct, sealed(datasetBytes))
			}
		})
	}
}

// A chunk's identity is not stable under a change to where a *range* starts, and that is
// the second thing a content-addressed key space would not fix.
//
// `uploadChunks` chunks a range relative to its own offset — `for off := r.Offset` — and
// `cow.Ranges` merges runs that touch. So a single sector written into the gap between two
// regions joins them into one range, and the two chunks that used to cover them are
// replaced by one chunk covering the join. 512 bytes of new information rewrite 2 MiB, and
// the 2 MiB they replaced stay in the bucket for ever.
//
// It is measured here rather than argued because it changes what option 2 is worth: sharing
// a chunk store between a parent and its clones only saves the chunks whose *boundaries*
// survive, and a guest that fills a hole in its filesystem moves boundaries.
func TestAWriteThatJoinsTwoRegionsRewritesBothOfThem(t *testing.T) {
	store := metered(sim.NewObjectStore())
	vol := volumeID(0xA1)

	// Two regions with exactly one sector between them.
	view := cow.NewIntervalMap()
	fill(view, 0, regionBytes, 0xA1)
	fill(view, regionBytes+sectorBytes, regionBytes, 0xA2)

	at := store.mark()
	etag, err := image.Publish(t.Context(), store, rand.Reader, encFor(t, vol), image.OwnLineage(vol), view, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if objs, n := store.chunksSince(at, image.ChunksPrefix(vol)); objs != 2 || n != 2*sealed(regionBytes) {
		t.Fatalf("the first stop transferred %d objects / %d bytes, want 2 / %d", objs, n, 2*sealed(regionBytes))
	}

	// One sector, into the gap.
	fill(view, regionBytes, sectorBytes, 0xA3)
	at = store.mark()
	if _, err := image.Publish(t.Context(), store, rand.Reader, encFor(t, vol), image.OwnLineage(vol), view, 2, etag); err != nil {
		t.Fatal(err)
	}
	objs, transferred := store.chunksSince(at, image.ChunksPrefix(vol))
	joined := sealed(2*regionBytes + sectorBytes)
	t.Logf("a %d-byte write into the gap between two regions transferred %d object(s), %d bytes", sectorBytes, objs, transferred)
	if objs != 1 || transferred != joined {
		t.Errorf("the joining write transferred %d objects / %d bytes, want 1 / %d — the two regions became one range and were re-chunked whole",
			objs, transferred, joined)
	}

	_, resident, distinct := bucketChunks(t, store)
	if want := 2*sealed(regionBytes) + joined; resident != want {
		t.Errorf("the bucket holds %d chunk bytes, want %d", resident, want)
	}
	if distinct != resident {
		t.Errorf("%d of %d resident bytes are duplicate content, want none — the re-chunked object shares no digest with what it replaced",
			resident-distinct, resident)
	}
}
