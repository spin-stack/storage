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
// CHUNK-ADDRESSING-SPEC §8 puts one question to a human — is `chain_depth` a structure or
// a label — and recommends *label*: keep flattening, pay a duplicate per link. That
// recommendation is a price, and until this file nothing in the tree had ever weighed it.
// A recommendation to pay a cost nobody measured is not a recommendation, so these are the
// scales.
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
//   - **distinct** — what the same content would occupy in *one* content-addressed
//     namespace, folded by the digest each key ends in. `resident - distinct` is exactly
//     what CHUNK-ADDRESSING-SPEC §3's option 2 would save, computable from outside the
//     process only because a chunk's key *is* the digest of its plaintext
//     (`image.chunkKey`), and it is the number the whole decision turns on.
//
// The sizes here are small enough to run in the unit lane and the cost is exactly linear
// in the parent's dataset — every assertion below is an equality against the dataset size,
// not a ratio — so multiply by whatever a real golden image weighs.

const (
	// datasetBytes is one parent's data. It stays under image.MaxChunkBytes on purpose:
	// a golden image smaller than 64 MiB is *one chunk*, which is the case that decides
	// how much option 2 could ever save (see TestACloneFirstStopCopiesWhatItInherited).
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

func (s *meteredStore) chunksSince(mark int, prefix string) (objects int, transferred int64) {
	for _, p := range s.puts[mark:] {
		if strings.HasPrefix(p.key, prefix+"chunks/") {
			objects++
			transferred += p.bytes
		}
	}
	return objects, transferred
}

// bucketChunks folds every chunk object in the bucket two ways: what is there, and what
// one content-addressed namespace would have held for the same content.
//
// The fold is by the digest the key ends in, which is the plaintext digest — so two
// volumes holding identical bytes collapse to one entry here and to two objects in the
// bucket. That gap is the measurement.
func bucketChunks(t *testing.T, store objectstore.Store) (objects int, resident, distinct int64) {
	t.Helper()
	objs, err := store.List(t.Context(), "image/")
	if err != nil {
		t.Fatalf("listing the bucket: %v", err)
	}
	byDigest := map[string]int64{}
	for _, o := range objs {
		i := strings.LastIndex(o.Key, "/chunks/")
		if i < 0 {
			continue // a manifest or a snapshot: structural, and it carries no guest data
		}
		objects++
		resident += o.Size
		byDigest[o.Key[i+len("/chunks/"):]] = o.Size
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
// snapID — the shape controlplane.Clone descends from. The snapshot shares the volume's
// own chunk store, so it transfers a manifest and no chunk bytes; that is asserted once,
// in TestASecondStopPaysOnlyForWhatItTouched, rather than assumed here.
func publishParent(t *testing.T, store objectstore.Store, vol [16]byte, write func(*cow.IntervalMap), snapID string) {
	t.Helper()
	view := cow.NewIntervalMap()
	write(view)
	if _, err := image.Publish(t.Context(), store, rand.Reader, encFor(t, vol), vol, view, 1, ""); err != nil {
		t.Fatalf("publishing the parent's image: %v", err)
	}
	if _, err := image.PublishSnapshot(t.Context(), store, rand.Reader, encFor(t, vol), vol, view, 1, snapID); err != nil {
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
	// The parent's DEK, which a clone inherits (controlplane.Clone), bound to the parent's
	// id because the chunk AAD names the volume the chunks were sealed for.
	base, _, err := image.LoadSnapshot(t.Context(), store, encFor(t, parent), parent, snapID)
	if err != nil {
		t.Fatalf("loading the parent's snapshot: %v", err)
	}
	return cow.NewIntervalMapOver(base)
}

func fill(v *cow.IntervalMap, off uint64, n int, b byte) {
	v.Overwrite(off, bytes.Repeat([]byte{b}, n))
}

// A clone's first stop writes its parent's whole dataset under its own prefix — and how
// much of that a content-addressed namespace could have saved depends on something the
// spec never names: how many chunks the parent's data is in.
//
// The four cases are the ones an owner has to price. Two of them are the extremes and they
// are 8 MiB apart on the same 8 MiB of data:
//
//   - a parent whose data is one contiguous run is **one chunk**, so a clone that writes a
//     single sector changes that chunk's content and its digest. Its 8 MiB is not a
//     duplicate of anything: `resident == distinct`, and option 2 saves *nothing at all*.
//   - the same 8 MiB in eight regions is eight chunks, and the same single-sector write
//     leaves seven of them byte-identical to the parent's. Option 2 saves seven eighths.
//
// So the saving is bounded by the chunk granularity, not by what the clone wrote, and
// CHUNK-ADDRESSING-SPEC §3's "the storage cost of a clone becomes proportional to what the
// clone wrote" holds only for a volume whose data is in many chunks. With MaxChunkBytes at
// 64 MiB, the golden image the whole workflow is built around is one chunk.
func TestACloneFirstStopCopiesWhatItInherited(t *testing.T) {
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
		// what the clone's first stop transferred under its own prefix
		wantObjects     int
		wantTransferred int64
		// what the bucket then holds, and what one content-addressed namespace would
		// have held for the same content
		wantResident, wantDistinct int64
		why                        string
	}{
		{
			name: "a clone that stops without writing anything",
			// The full duplicate for zero new information, which is the case the
			// flattening is least defensible in — and the only one where content
			// addressing would save all of it.
			parent: contiguous, clone: func(*cow.IntervalMap) {},
			wantObjects: 1, wantTransferred: sealed(datasetBytes),
			wantResident: 2 * sealed(datasetBytes), wantDistinct: sealed(datasetBytes),
			why: "a clone pays for its parent's whole dataset before it has written a byte",
		},
		{
			name:   "a clone that writes one sector into a one-chunk parent",
			parent: contiguous, clone: func(v *cow.IntervalMap) { fill(v, 0, sectorBytes, 0xC2) },
			wantObjects: 1, wantTransferred: sealed(datasetBytes),
			// resident == distinct: the clone's chunk holds different bytes from the
			// parent's, so nothing in this bucket is a duplicate by content and a
			// content-addressed namespace saves zero.
			wantResident: 2 * sealed(datasetBytes), wantDistinct: 2 * sealed(datasetBytes),
			why: "512 bytes of new information cost 8 MiB, and no key space can give it back",
		},
		{
			name:   "a clone that writes one sector into an eight-chunk parent",
			parent: fragmented, clone: func(v *cow.IntervalMap) { fill(v, 0, sectorBytes, 0xC2) },
			wantObjects: datasetBytes / regionBytes, wantTransferred: (datasetBytes / regionBytes) * sealed(regionBytes),
			wantResident: 2 * (datasetBytes / regionBytes) * sealed(regionBytes),
			// Nine distinct digests: seven regions both volumes share, plus the one the
			// clone rewrote in each of its two versions.
			wantDistinct: (datasetBytes/regionBytes + 1) * sealed(regionBytes),
			why:          "the same data in eight chunks makes seven eighths of the copy recoverable",
		},
		{
			name:   "a clone that overwrites every byte it inherited",
			parent: contiguous, clone: func(v *cow.IntervalMap) { fill(v, 0, datasetBytes, 0xC2) },
			wantObjects: 1, wantTransferred: sealed(datasetBytes),
			wantResident: 2 * sealed(datasetBytes), wantDistinct: 2 * sealed(datasetBytes),
			why: "it pays nothing *extra*: every byte it uploaded is a byte it wrote",
		},
		{
			name:   "a clone that writes past the end of what it inherited",
			parent: contiguous, clone: func(v *cow.IntervalMap) { fill(v, 2*datasetBytes, sectorBytes, 0xC2) },
			// Two ranges, so two chunks: the inherited run untouched, and the clone's own.
			wantObjects: 2, wantTransferred: sealed(datasetBytes) + sealed(sectorBytes),
			wantResident: 2*sealed(datasetBytes) + sealed(sectorBytes),
			wantDistinct: sealed(datasetBytes) + sealed(sectorBytes),
			why:          "a disjoint write leaves the inherited chunk identical, so this one is a true duplicate",
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
			if _, err := image.Publish(t.Context(), store, rand.Reader, encFor(t, clone), clone, view, 2, ""); err != nil {
				t.Fatalf("the clone's first stop: %v", err)
			}

			objects, transferred := store.chunksSince(at, image.Prefix(clone))
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

// The duplicate is paid **once per clone**, not once per publish and not once per
// snapshot — and then it is paid again, in full, for every chunk a later session touches.
//
// CHUNK-ADDRESSING-SPEC §3 prices option 1 as "a full duplicate of the inherited data per
// link, paid at the clone's first stop **and at every snapshot of the clone**". The second
// half is wrong, and the mechanism that makes it wrong is the `Head` skip in
// `uploadChunks`: after the first stop the chunks are already under the clone's own
// prefix, so a snapshot and an unchanged restop transfer no chunk bytes at all.
//
// What the spec does *not* price is the other end of it. A later stop re-uploads every
// chunk it touched under a new digest, and **nothing in this repository deletes an
// object** — `grep -rn '\.Delete(' --include=*.go internal/ cmd/` outside the store
// implementations and their conformance suite reaches nothing. So the superseded copy
// stays for ever. That is a cost per *stop*, unbounded in time, where the duplication the
// chain-depth decision is about is a cost per *link*, paid once.
func TestASecondStopPaysOnlyForWhatItTouched(t *testing.T) {
	store := metered(sim.NewObjectStore())
	parent, clone := volumeID(0xAA), volumeID(0xCC)
	const parentSnap = "01930000-0000-7000-8000-00000000000a"
	const cloneSnap = "01930000-0000-7000-8000-00000000000c"

	publishParent(t, store, parent, func(v *cow.IntervalMap) { fill(v, 0, datasetBytes, 0xA1) }, parentSnap)

	view := cloneView(t, store, parent, parentSnap)
	at := store.mark()
	etag, err := image.Publish(t.Context(), store, rand.Reader, encFor(t, clone), clone, view, 2, "")
	if err != nil {
		t.Fatalf("the clone's first stop: %v", err)
	}
	if _, first := store.chunksSince(at, image.Prefix(clone)); first != sealed(datasetBytes) {
		t.Fatalf("the clone's first stop transferred %d bytes, want %d", first, sealed(datasetBytes))
	}

	// A snapshot of the clone, immediately: the spec says this pays the duplicate again.
	at = store.mark()
	if _, err := image.PublishSnapshot(t.Context(), store, rand.Reader, encFor(t, clone), clone, view, 2, cloneSnap); err != nil {
		t.Fatalf("snapshotting the clone: %v", err)
	}
	if objs, n := store.chunksSince(at, image.Prefix(clone)); n != 0 {
		t.Errorf("a snapshot taken straight after the first stop transferred %d chunk objects / %d bytes, want 0 — the Head skip means the duplicate is paid once per clone, not once per snapshot", objs, n)
	}

	// And a second stop that changed nothing.
	at = store.mark()
	etag, err = image.Publish(t.Context(), store, rand.Reader, encFor(t, clone), clone, view, 3, etag)
	if err != nil {
		t.Fatalf("the clone's second stop: %v", err)
	}
	if objs, n := store.chunksSince(at, image.Prefix(clone)); n != 0 {
		t.Errorf("a second stop with no writes transferred %d chunk objects / %d bytes, want 0", objs, n)
	}

	// A second stop that wrote one sector pays for the whole chunk that sector fell in,
	// and the chunk it replaced stays in the bucket with nothing naming it.
	fill(view, 0, sectorBytes, 0xC2)
	at = store.mark()
	if _, err := image.Publish(t.Context(), store, rand.Reader, encFor(t, clone), clone, view, 4, etag); err != nil {
		t.Fatalf("the clone's third stop: %v", err)
	}
	_, again := store.chunksSince(at, image.Prefix(clone))
	if again != sealed(datasetBytes) {
		t.Errorf("a stop that wrote one sector transferred %d bytes, want %d", again, sealed(datasetBytes))
	}

	// The bucket, from the outside: the clone's prefix holds two chunks and its manifest
	// names one. The other is unreachable and permanent.
	objs, err := store.List(t.Context(), image.Prefix(clone)+"chunks/")
	if err != nil {
		t.Fatal(err)
	}
	_, man, _, err := image.Load(t.Context(), store, encFor(t, clone), clone)
	if err != nil {
		t.Fatal(err)
	}
	named := map[string]bool{}
	for _, c := range man.Chunks {
		named[c.Digest] = true
	}
	var orphaned int64
	for _, o := range objs {
		if !named[o.Key[strings.LastIndex(o.Key, "/")+1:]] {
			orphaned += o.Size
		}
	}
	t.Logf("after three stops the clone's prefix holds %d chunk objects; %d bytes are named by no manifest and nothing deletes them", len(objs), orphaned)
	if orphaned != sealed(datasetBytes) {
		t.Errorf("%d bytes under the clone's prefix are unreferenced, want %d — a superseded chunk is never reclaimed", orphaned, sealed(datasetBytes))
	}
}

// One copy per volume, whatever the lineage — including no lineage at all.
//
// The three shapes cost the same, which is the finding: the duplication is a property of
// the **key space**, not of cloning. `chunkKey` is `image/<volume>/chunks/<digest>`, so two
// volumes holding byte-identical data hold two objects however they came to hold it. A
// fleet booting N machines off one golden image pays N copies whether the volumes are
// clones of it or were created and written independently, and answering
// CHUNK-ADDRESSING-SPEC §8 "structure" would not change that at all: a chain walk changes
// what a *clone* reads, and the two unrelated volumes below are not a chain.
func TestTheBucketHoldsOneCopyPerVolumeWhateverTheLineage(t *testing.T) {
	data := func(v *cow.IntervalMap) { fill(v, 0, datasetBytes, 0xA1) }
	const rootSnap = "01930000-0000-7000-8000-00000000000a"
	const midSnap = "01930000-0000-7000-8000-00000000000b"

	// publishClone stops a clone of parent's snapshot, having written nothing, and
	// optionally freezes a snapshot of it for the next link.
	publishClone := func(t *testing.T, store objectstore.Store, parent [16]byte, snapID string, clone [16]byte, ownSnap string) {
		t.Helper()
		view := cloneView(t, store, parent, snapID)
		if _, err := image.Publish(t.Context(), store, rand.Reader, encFor(t, clone), clone, view, 2, ""); err != nil {
			t.Fatalf("stopping clone %x: %v", clone[0], err)
		}
		if ownSnap == "" {
			return
		}
		if _, err := image.PublishSnapshot(t.Context(), store, rand.Reader, encFor(t, clone), clone, view, 2, ownSnap); err != nil {
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
				publishParent(t, store, volumeID(0xA0), data, rootSnap)
				publishClone(t, store, volumeID(0xA0), rootSnap, volumeID(0xB0), midSnap)
				publishClone(t, store, volumeID(0xB0), midSnap, volumeID(0xC0), "")
			},
			copies: 3,
			why:    "each link flattens the whole inherited dataset under its own prefix",
		},
		{
			name: "one golden image, three clones",
			build: func(t *testing.T, store objectstore.Store) {
				publishParent(t, store, volumeID(0xA0), data, rootSnap)
				for _, id := range []byte{0xB1, 0xB2, 0xB3} {
					publishClone(t, store, volumeID(0xA0), rootSnap, volumeID(id), "")
				}
			},
			copies: 4,
			why:    "§2's primary workflow: N clones of one image is N+1 copies",
		},
		{
			name: "two volumes that were never related and hold the same bytes",
			build: func(t *testing.T, store objectstore.Store) {
				publishParent(t, store, volumeID(0xA0), data, rootSnap)
				publishParent(t, store, volumeID(0xD0), data, midSnap)
			},
			copies: 2,
			why:    "no clone, no chain, same duplication — it is the key space, not the lineage",
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
	etag, err := image.Publish(t.Context(), store, rand.Reader, encFor(t, vol), vol, view, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if objs, n := store.chunksSince(at, image.Prefix(vol)); objs != 2 || n != 2*sealed(regionBytes) {
		t.Fatalf("the first stop transferred %d objects / %d bytes, want 2 / %d", objs, n, 2*sealed(regionBytes))
	}

	// One sector, into the gap.
	fill(view, regionBytes, sectorBytes, 0xA3)
	at = store.mark()
	if _, err := image.Publish(t.Context(), store, rand.Reader, encFor(t, vol), vol, view, 2, etag); err != nil {
		t.Fatal(err)
	}
	objs, transferred := store.chunksSince(at, image.Prefix(vol))
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
