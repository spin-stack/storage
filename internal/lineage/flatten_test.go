package lineage_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/lineage"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

const block = 4096

// The five offsets a flatten has to get right, and each one says something the others
// cannot. A flatten that wrote down only the volume's own layers passes none of them; one
// that wrote down the ancestry and dropped the erasure passes four; one that composed the
// chain upside down passes three.
const (
	offRootOnly   = int64(0)         // the root wrote it and nothing above touched it
	offMiddleOnly = int64(1 * block) // the middle link wrote it
	offErased     = int64(2 * block) // the root wrote it, the middle link discarded it
	offShared     = int64(3 * block) // the root wrote it, the volume itself overwrote it
	offOwn        = int64(4 * block) // only the volume itself wrote it
	// The erasure the ancestry does not know about: the root holds bytes here and *this*
	// volume discarded them. It is the one offset whose answer depends on the flattened
	// manifest carrying its own tombstones, since every other erasure in this chain is
	// already stated by the layer that made it.
	offOwnErased = int64(5 * block)
)

// TestAFlattenedVolumeReadsWhatItReadBefore is the whole promise, asserted as bytes at
// offsets rather than as a shape of a manifest.
//
// Before the flatten the volume answers those five offsets by walking a chain of three and
// laying its own delta over it. After the flatten it must answer them **out of its own
// image alone** — the composition is what a flatten removes — and the second half of the
// test is what makes "alone" mean something: every object of every ancestor is deleted, so
// a read that still worked through one of them cannot.
//
// The lineage is encrypted, and that is not decoration. A flattened image is re-sealed
// under a new lineage root, because the chunk AAD binds the root (image's package doc), so
// a flatten that moved the objects and not the seal produces a volume whose every chunk
// fails to authenticate. Unencrypted, that whole half of the change would be untested.
func TestAFlattenedVolumeReadsWhatItReadBefore(t *testing.T) {
	store := sim.NewObjectStore()
	dek := newDEK(t)
	vol, enc := buildLineage(t, store, dek)

	// What the volume answered before anything was rewritten, read the way its Agent reads
	// it: the ancestry composed, its own image laid over that.
	want := map[int64]byte{
		offRootOnly:   0xA1,
		offMiddleOnly: 0xB2,
		offErased:     0x00,
		offShared:     0xC3,
		offOwn:        0xC3,
		offOwnErased:  0x00,
	}
	before, _ := readThroughTheChain(t, store, enc, vol)
	assertReads(t, before, want, "before the flatten")

	res, err := lineage.Flatten(t.Context(), store, rand.Reader, enc, vol.id, 7)
	if err != nil {
		t.Fatalf("Flatten: %v", err)
	}
	if res.Ancestors != 2 {
		t.Errorf("the flatten left %d ancestors behind, want the 2 the chain had", res.Ancestors)
	}

	// Alone: no base, and the volume's own id as its own lineage root, which is what its
	// Agent will resolve for it the moment the descriptor below says it descends from
	// nothing.
	alone, _, _, err := image.Load(t.Context(), store, enc, image.OwnLineage(vol.u), nil)
	if err != nil {
		t.Fatalf("loading the flattened image on its own: %v", err)
	}
	assertReads(t, alone, want, "after the flatten, from the volume's own image alone")

	// The bucket says so too: the link is gone and the depth with it, which is what returns
	// the volume to a place controlplane.Clone will clone from again.
	d, err := descriptor.Read(t.Context(), store, vol.id)
	if err != nil {
		t.Fatalf("reading the flattened volume's descriptor: %v", err)
	}
	if d.ParentSnapshotID != "" || d.ParentVolumeID != "" || d.ChainDepth != 0 {
		t.Errorf("the descriptor still says the volume descends from snapshot %q of %q at depth %d",
			d.ParentSnapshotID, d.ParentVolumeID, d.ChainDepth)
	}

	// And now the half that makes "self-contained" a fact rather than a claim: every object
	// either ancestor owns goes away — their manifests, their snapshots and their
	// descriptors — and the volume is read again. A flatten that left one range resolving
	// through an ancestor fails here and nowhere else.
	//
	// Their *chunks* are deliberately not deleted, and the distinction is the whole of what
	// step 2 bought: a chunk belongs to the lineage, not to the volume that wrote it
	// (image.ChunksPrefix), so deleting the objects under chunks/<old root>/ would take the
	// bytes of every volume in the chain including the ancestors' own. What a flatten makes
	// this volume independent of is everything the ancestors *name*, which is what a delete
	// of an ancestor removes first.
	for _, anc := range vol.chain {
		deletePrefix(t, store, image.Prefix(anc.ID))
		deletePrefix(t, store, "volumes/"+anc.Volume+"/")
	}
	after, _, _, err := image.Load(t.Context(), store, enc, image.OwnLineage(vol.u), nil)
	if err != nil {
		t.Fatalf("loading the flattened image with every ancestor object deleted: %v", err)
	}
	assertReads(t, after, want, "after every ancestor's manifests, snapshots and descriptor were deleted")
}

// TestAFlattenedImageReadsTheSameWhenItIsStillLayered is the window, and it is the reason
// the flattened manifest carries erasures a root volume's manifest never would.
//
// The manifest is written before the descriptor stops naming a parent, so for the length of
// one PUT the bucket holds a self-contained image belonging to a volume it still says has an
// ancestry. cow.DeltaOver drops tombstones when it is told there is nothing underneath; the
// flatten therefore publishes over an empty map rather than over nil.
//
// **Only offOwnErased can tell the difference, and finding that out is what the fixture is
// shaped by.** An erasure an ancestor made is stated by that ancestor's own manifest, so the
// composed ancestry answers it as zeros whatever this manifest says — planting "publish over
// nil" against a fixture whose only erasure was the middle link's went green, which is a test
// that proved nothing. What is at stake is a range *this volume* discarded over bytes an
// ancestor holds: absence in a layered read means "ask the layer below", so the guest would
// be handed back the blocks it freed (§14.6).
//
// Asserted on the composition rather than on the manifest's `discarded` field, because the
// field is the mechanism and what must be true is that the two readings of one object agree.
func TestAFlattenedImageReadsTheSameWhenItIsStillLayered(t *testing.T) {
	store := sim.NewObjectStore()
	dek := newDEK(t)
	vol, enc := buildLineage(t, store, dek)
	want := map[int64]byte{
		offRootOnly: 0xA1, offMiddleOnly: 0xB2, offErased: 0x00,
		offShared: 0xC3, offOwn: 0xC3, offOwnErased: 0x00,
	}

	if _, err := lineage.Flatten(t.Context(), store, rand.Reader, enc, vol.id, 7); err != nil {
		t.Fatalf("Flatten: %v", err)
	}

	// The reader that has not noticed yet: it walks the chain the catalog still describes
	// and lays the flattened image over it. The chunks are found under the *old* root here
	// only because this fixture asks for them there — which is exactly the point, since it
	// isolates the format question from the key-space one.
	ancestry, err := lineage.Compose(t.Context(), store, enc, vol.root, vol.chain, cow.NewIntervalMap())
	if err != nil {
		t.Fatalf("composing the ancestry the stale reader would compose: %v", err)
	}
	layered, _, _, err := image.Load(t.Context(), store, enc, image.OwnLineage(vol.u), ancestry)
	if err != nil {
		t.Fatalf("loading the flattened image over the ancestry: %v", err)
	}
	assertReads(t, layered, want, "the flattened image read by a reader that still lays it over the chain")
}

// TestAFlattenIsRefusedRatherThanGuessedAt covers the three states in which a flatten would
// be wrong, and each refusal is a different kind of wrong.
//
// The snapshot case is the one worth reading twice. A snapshot is immutable (§5.2, INV-16),
// so a snapshot this volume published while it was a clone is a delta over an ancestry that
// can never be rewritten. Flattening the volume would clear the link a clone of that
// snapshot walks through, and that clone would read zeros for everything the ancestry held —
// while `controlplane.Clone` admitted it at depth 0, because the ceiling compares the parent
// volume's depth. It is the sentence track D's ceiling asked FLATTEN not to break.
func TestAFlattenIsRefusedRatherThanGuessedAt(t *testing.T) {
	tests := []struct {
		name string
		// breaks puts the bucket into the state under test and returns nothing; the volume
		// it acts on is the one buildLineage produced.
		breaks func(t *testing.T, store objectstore.Store, vol builtVolume, enc *wal.Encryption)
		// published is what the catalog says the volume reached, which is the fact the
		// bucket cannot supply. buildLineage publishes its image at sequence 7.
		published int64
		want      error
	}{
		{
			name: "the volume's own image is gone and the catalog says it published one",
			breaks: func(t *testing.T, store objectstore.Store, vol builtVolume, enc *wal.Encryption) {
				t.Helper()
				if err := store.Delete(t.Context(), image.ManifestKey(vol.u)); err != nil {
					t.Fatalf("deleting the volume's own manifest: %v", err)
				}
			},
			published: 7,
			want:      lineage.ErrImageMissing,
		},
		{
			name: "the volume has published a snapshot of its own, which is an immutable delta",
			breaks: func(t *testing.T, store objectstore.Store, vol builtVolume, enc *wal.Encryption) {
				t.Helper()
				view, ancestry := readThroughTheChain(t, store, enc, vol)
				if _, err := image.PublishSnapshot(t.Context(), store, rand.Reader, enc,
					image.Ident{Volume: vol.u, Lineage: vol.root}, view, ancestry, 1, ids.New().String()); err != nil {
					t.Fatalf("publishing a snapshot of the volume: %v", err)
				}
			},
			published: 7,
			want:      lineage.ErrHasSnapshots,
		},
		{
			name: "a previous flatten left objects under the volume's own lineage",
			breaks: func(t *testing.T, store objectstore.Store, vol builtVolume, enc *wal.Encryption) {
				t.Helper()
				if _, err := store.Put(t.Context(), image.ChunksPrefix(vol.u)+"deadbeef", []byte("x"),
					objectstore.PutOptions{}); err != nil {
					t.Fatalf("planting a chunk under the volume's own lineage: %v", err)
				}
			},
			published: 7,
			want:      lineage.ErrAlreadyStarted,
		},
		{
			name: "the volume descends from nothing",
			breaks: func(t *testing.T, store objectstore.Store, vol builtVolume, enc *wal.Encryption) {
				t.Helper()
				d, err := descriptor.Read(t.Context(), store, vol.id)
				if err != nil {
					t.Fatalf("reading the descriptor: %v", err)
				}
				d.ParentSnapshotID, d.ParentVolumeID = "", ""
				if err := descriptor.Write(t.Context(), store, d); err != nil {
					t.Fatalf("rewriting the descriptor: %v", err)
				}
			},
			published: 7,
			want:      lineage.ErrSelfContained,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := sim.NewObjectStore()
			dek := newDEK(t)
			vol, enc := buildLineage(t, store, dek)
			start := listAll(t, store)

			tc.breaks(t, store, vol, enc)
			between := listAll(t, store)

			_, err := lineage.Flatten(t.Context(), store, rand.Reader, enc, vol.id, tc.published)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Flatten returned %v, want %v", err, tc.want)
			}
			// A refusal that had already rewritten the manifest would be worse than no
			// refusal at all, so the bucket is compared rather than the error alone.
			if after := listAll(t, store); after != between {
				t.Errorf("a refused flatten changed the bucket:\nbefore the case was set up:\n%s\nafter the refusal:\n%s", start, after)
			}
		})
	}
}

// TestACloneThatNeverPublishedIsFlattenedIntoItsAncestry is the other half of the case
// above, and the two are worth reading together: **the bucket is in exactly the same state
// in both, and only the catalog number differs.**
//
// "There is no manifest for this volume" is one answer from the object store to two
// questions. For a clone that has never stopped cleanly it means the volume owns no layer
// yet and its whole content is the ancestry — the useful case, since a clone that was made
// and never booted is precisely the one an operator flattens to unblock a delete of its
// parent. For a clone whose image was deleted it means the volume's own writes are missing,
// and flattening on that reading writes the ancestry down as the volume's whole content and
// publishes it: every byte the clone ever wrote, dropped, with the descriptor rewritten so
// nothing will ever look for the old layer again.
//
// So the assertion here is bytes, not an error: the flatten succeeds and the flattened image
// answers with the ancestry's values at every offset — including offOwn, which is 0x00
// because the layer that held 0xC3 genuinely does not exist for a volume that never
// published.
func TestACloneThatNeverPublishedIsFlattenedIntoItsAncestry(t *testing.T) {
	store := sim.NewObjectStore()
	dek := newDEK(t)
	vol, enc := buildLineage(t, store, dek)

	// The same object the refusal case removes: the volume's own layer leaves the bucket.
	if err := store.Delete(t.Context(), image.ManifestKey(vol.u)); err != nil {
		t.Fatalf("deleting the volume's own manifest: %v", err)
	}

	res, err := lineage.Flatten(t.Context(), store, rand.Reader, enc, vol.id, 0)
	if err != nil {
		t.Fatalf("flattening a clone the catalog says never published: %v", err)
	}
	if res.Ancestors != 2 {
		t.Errorf("the flatten left %d ancestors behind, want the 2 the chain had", res.Ancestors)
	}

	alone, _, _, err := image.Load(t.Context(), store, enc, image.OwnLineage(vol.u), nil)
	if err != nil {
		t.Fatalf("loading the flattened image on its own: %v", err)
	}
	assertReads(t, alone, map[int64]byte{
		offRootOnly:   0xA1,
		offMiddleOnly: 0xB2,
		offErased:     0x00,
		offShared:     0xA1, // the volume's own overwrite is the layer it never published
		offOwn:        0x00,
		offOwnErased:  0xA1, // likewise its own erasure
	}, "a clone that never published, flattened into its ancestry")
}

// builtVolume is the volume under test and everything a caller needs to read it the way its
// Agent would: its ids, the lineage root its chunks live under, the chain above it, and the
// composed ancestry its own image is a delta over.
type builtVolume struct {
	id    string
	u     [16]byte
	root  [16]byte
	chain []lineage.Ancestor
}

// buildLineage writes a chain of three into the bucket through the production publisher, and
// returns the youngest.
//
// Every manifest is a delta, because that is what a session of each of those volumes
// produces since publishing stopped flattening: the root states its own ranges, the middle
// link states one write and one erasure over the root, and the volume under test states one
// write and one overwrite over both. Nothing here anticipates a format — image.Publish and
// image.PublishSnapshot are what write it.
func buildLineage(t *testing.T, store objectstore.Store, dek crypto.DEK) (builtVolume, *wal.Encryption) {
	t.Helper()

	// The root: its own lineage, its own chunks, no tombstones (it has nothing underneath).
	rootID, rootU := newVolume(t)
	rootView := cow.NewIntervalMap()
	rootView.Overwrite(uint64(offRootOnly), repeat(0xA1))
	rootView.Overwrite(uint64(offErased), repeat(0xA1))
	rootView.Overwrite(uint64(offShared), repeat(0xA1))
	rootView.Overwrite(uint64(offOwnErased), repeat(0xA1))
	rootSnap := publishSnapshotOf(t, store, dek, rootU, rootU, rootView, nil)
	writeDescriptor(t, store, rootID, "", "", 0)

	// The middle link: one write of its own and one erasure over a range the root holds.
	midID, midU := newVolume(t)
	rootBase := loadSnapshot(t, store, dek, rootU, rootU, rootSnap, nil)
	midView := cow.NewIntervalMapOver(rootBase)
	midView.Overwrite(uint64(offMiddleOnly), repeat(0xB2))
	midView.Clear(uint64(offErased), block)
	midSnap := publishSnapshotOf(t, store, dek, midU, rootU, midView, rootBase)
	writeDescriptor(t, store, midID, rootSnap, rootID, 1)

	// The volume under test: an image of its own, which is a delta over both.
	volID, volU := newVolume(t)
	ancestry := loadSnapshot(t, store, dek, midU, rootU, midSnap,
		loadSnapshot(t, store, dek, rootU, rootU, rootSnap, nil))
	own := cow.NewIntervalMapOver(ancestry)
	own.Overwrite(uint64(offOwn), repeat(0xC3))
	own.Overwrite(uint64(offShared), repeat(0xC3))
	own.Clear(uint64(offOwnErased), block)
	enc := encryptionFor(t, dek, volU)
	if _, err := image.Publish(t.Context(), store, rand.Reader, enc,
		image.Ident{Volume: volU, Lineage: rootU}, own, ancestry, 7, ""); err != nil {
		t.Fatalf("publishing the volume's own image: %v", err)
	}
	writeDescriptor(t, store, volID, midSnap, midID, 2)

	return builtVolume{
		id: volID, u: volU, root: rootU,
		chain: []lineage.Ancestor{
			{Volume: midID, ID: midU, Snapshot: midSnap},
			{Volume: rootID, ID: rootU, Snapshot: rootSnap},
		},
	}, enc
}

// readThroughTheChain composes the volume the way agent.fetchBase does: the ancestry, then
// its own image over it. It is the reading a flatten has to preserve.
func readThroughTheChain(t *testing.T, store objectstore.Store, enc *wal.Encryption, vol builtVolume) (view, ancestry *cow.IntervalMap) {
	t.Helper()
	ancestry, err := lineage.Compose(t.Context(), store, enc, vol.root, vol.chain, nil)
	if err != nil {
		t.Fatalf("composing the ancestry: %v", err)
	}
	view, _, _, err = image.Load(t.Context(), store, enc, image.Ident{Volume: vol.u, Lineage: vol.root}, ancestry)
	if err != nil {
		t.Fatalf("loading the volume's image over its ancestry: %v", err)
	}
	return view, ancestry
}

func assertReads(t *testing.T, view *cow.IntervalMap, want map[int64]byte, when string) {
	t.Helper()
	for _, off := range []int64{offRootOnly, offMiddleOnly, offErased, offShared, offOwn, offOwnErased} {
		got := make([]byte, block)
		view.Read(uint64(off), got)
		if !bytes.Equal(got, repeat(want[off])) {
			t.Errorf("%s: offset %d reads %#x..., want %#x repeated", when, off, got[:8], want[off])
		}
	}
}

func repeat(b byte) []byte { return bytes.Repeat([]byte{b}, block) }

func newVolume(t *testing.T) (string, [16]byte) {
	t.Helper()
	id := ids.New().String()
	u, err := ids.Parse(id)
	if err != nil {
		t.Fatalf("volume %q is not a uuid: %v", id, err)
	}
	return id, [16]byte(u)
}

func newDEK(t *testing.T) crypto.DEK {
	t.Helper()
	dek, err := crypto.GenerateDEK(rand.Reader, 3)
	if err != nil {
		t.Fatalf("generating the lineage's DEK: %v", err)
	}
	return dek
}

// encryptionFor binds the one DEK a whole lineage shares to a volume, which is what
// controlplane.Clone arranges by handing a clone its parent's wrapped key.
func encryptionFor(t *testing.T, dek crypto.DEK, u [16]byte) *wal.Encryption {
	t.Helper()
	enc, err := wal.NewEncryption(dek, u)
	if err != nil {
		t.Fatalf("binding the DEK: %v", err)
	}
	return enc
}

func publishSnapshotOf(t *testing.T, store objectstore.Store, dek crypto.DEK, vol, root [16]byte,
	view, inherited *cow.IntervalMap,
) string {
	t.Helper()
	snapID := ids.New().String()
	if _, err := image.PublishSnapshot(t.Context(), store, rand.Reader, encryptionFor(t, dek, vol),
		image.Ident{Volume: vol, Lineage: root}, view, inherited, 1, snapID); err != nil {
		t.Fatalf("publishing a snapshot: %v", err)
	}
	return snapID
}

func loadSnapshot(t *testing.T, store objectstore.Store, dek crypto.DEK, vol, root [16]byte,
	snapID string, base *cow.IntervalMap,
) *cow.IntervalMap {
	t.Helper()
	view, _, err := image.LoadSnapshotOver(t.Context(), store, encryptionFor(t, dek, vol),
		image.Ident{Volume: vol, Lineage: root}, snapID, base)
	if err != nil {
		t.Fatalf("loading snapshot %s: %v", snapID, err)
	}
	return view
}

// writeDescriptor writes the object controlplane.Provision and controlplane.Clone write. It
// is what states a lineage: the walk reads it, and FLATTEN rewrites it.
func writeDescriptor(t *testing.T, store objectstore.Store, volumeID, parentSnapshot, parentVolume string, depth int32) {
	t.Helper()
	if err := descriptor.Write(t.Context(), store, descriptor.Descriptor{
		VolumeID: volumeID, SizeBytes: 32 * block, BlockSize: 512, CurrentEpoch: 1,
		ChainDepth: depth, KEKID: "kek-test", DEKKeyID: 3,
		ParentSnapshotID: parentSnapshot, ParentVolumeID: parentVolume,
	}); err != nil {
		t.Fatalf("writing the descriptor of %s: %v", volumeID, err)
	}
}

func deletePrefix(t *testing.T, store objectstore.Store, prefix string) {
	t.Helper()
	objs, err := store.List(t.Context(), prefix)
	if err != nil {
		t.Fatalf("listing %s: %v", prefix, err)
	}
	if len(objs) == 0 {
		t.Fatalf("nothing to delete under %s: the fixture is not what this test thinks it is", prefix)
	}
	for _, o := range objs {
		if err := store.Delete(t.Context(), o.Key); err != nil {
			t.Fatalf("deleting %s: %v", o.Key, err)
		}
	}
}

// listAll is the bucket as an outside observer sees it, for the assertion that a refusal
// changed nothing.
func listAll(t *testing.T, store objectstore.Store) string {
	t.Helper()
	objs, err := store.List(t.Context(), "")
	if err != nil {
		t.Fatalf("listing the bucket: %v", err)
	}
	var b strings.Builder
	for _, o := range objs {
		b.WriteString(o.Key)
		b.WriteString(" ")
		b.WriteString(o.ETag)
		b.WriteString("\n")
	}
	return b.String()
}
