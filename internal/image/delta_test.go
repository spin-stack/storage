package image_test

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"testing"

	"pgregory.net/rapid"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// A manifest states its own volume's layers, and a reader that lays it back over the
// ancestry gets the volume it published.
//
// Everything in this file goes through the store: the manifest is read out of the bucket
// and parsed here, never taken from what Publish returned, because "the publisher built
// the right structure" and "the right bytes are in the bucket" are the two claims a format
// has to keep apart. It is the same rule the chunk-cost measurements follow.
const deltaVolumeSize = 8192

// ancestryOf publishes a snapshot holding exactly the ranges write puts in it, and returns
// the view a reader gets back for it. A parent is a root: it descends from nothing, so it
// inherits nothing and states all of itself.
func ancestryOf(t *testing.T, store objectstore.Store, parent [16]byte, snapID string, write func(*cow.IntervalMap)) *cow.IntervalMap {
	t.Helper()
	view := cow.NewIntervalMap()
	write(view)
	if _, err := image.PublishSnapshot(t.Context(), store, rand.Reader, encFor(t, parent), image.OwnLineage(parent), view, nil, 1, snapID); err != nil {
		t.Fatalf("publishing the parent's snapshot: %v", err)
	}
	loaded, _, err := image.LoadSnapshot(t.Context(), store, encFor(t, parent), image.OwnLineage(parent), snapID)
	if err != nil {
		t.Fatalf("loading the parent's snapshot: %v", err)
	}
	return loaded
}

// readManifestObject is what the bucket holds at key, decoded here rather than obtained
// from the writer. A publisher that returned a correct Manifest and stored something else
// satisfies every assertion made on its return value.
func readManifestObject(t *testing.T, store objectstore.Store, key string) image.Manifest {
	t.Helper()
	body, err := store.Get(t.Context(), key)
	if err != nil {
		t.Fatalf("reading %s: %v", key, err)
	}
	var man image.Manifest
	if err := json.Unmarshal(body, &man); err != nil {
		t.Fatalf("parsing %s: %v", key, err)
	}
	return man
}

// A range the guest erased stays erased, across a stop and a start, and the ancestor's
// bytes do not come back.
//
// **This is the one that has to hold before publishing may stop flattening at all.** While
// a manifest was the volume's whole flattened view, a DISCARD was expressed as *absence*
// and nothing else needed to survive: absence read as zeros. In a delta, absence means
// "ask the layer below" — that is what makes a delta a delta — so the same absence would
// hand the guest the parent's older bytes at every offset it had freed. Data
// resurrection, §14.6, and worse than losing the erasure: the guest returned those blocks
// and something else may already have been told it owns them.
//
// The assertion is pointed straight at that: the offset the clone discarded must read as
// zeros, and the parent's byte at that offset is named in the failure so the message says
// *resurrection* rather than "unexpected bytes".
//
// # Where the DISCARD comes from, since no guest can issue one
//
// VIRTIO_BLK_F_DISCARD is not offered (internal/vhost/features.go), so a guest's DISCARD
// cannot reach a volume today; `wal.Log.Discard` exists and has no production caller. What
// a DISCARD *is*, once one arrives, is `cow.IntervalMap.Clear` on the log's read view —
// `Log.appendClear` calls exactly that — so that is what this drives, on a view composed
// the way `agent.fetchBase` composes one. The feature bit is a wire negotiation and a
// configuration-space change; the format has to be right before it is offered, not after.
func TestADiscardedRangeIsNotResurrectedByTheAncestryUnderneathIt(t *testing.T) {
	const (
		untouched = uint64(0)    // the parent wrote it; the clone leaves it alone
		erased    = uint64(2048) // the parent wrote it; the clone DISCARDs it
		ownWrite  = uint64(4096) // only the clone ever writes here
		n         = 512
	)
	store := sim.NewObjectStore()
	parent, clone := volumeID(0xAA), volumeID(0xCC)
	cloneID := image.Ident{Volume: clone, Lineage: parent}
	const snapID = "01930000-0000-7000-8000-00000000000a"

	ancestry := ancestryOf(t, store, parent, snapID, func(v *cow.IntervalMap) {
		fill(v, untouched, n, 0xA1)
		fill(v, erased, n, 0xA1)
	})

	// The clone's session: its own layer over the ancestry, one write and one erasure.
	view := cow.NewIntervalMapOver(ancestry)
	view.Clear(erased, n)
	fill(view, ownWrite, n, 0xC2)

	// Before the stop, the running volume already reads zeros there. If this failed the
	// rest would be about the wrong thing.
	assertReads(t, view, erased, 0x00, "a discarded range reads as zeros in the session that discarded it")

	if _, err := image.Publish(t.Context(), store, rand.Reader, encFor(t, clone), cloneID, view, ancestry, 2, ""); err != nil {
		t.Fatalf("the clone's stop: %v", err)
	}

	// What the bucket says. The erasure has to be *in the manifest*: a delta that named
	// only its chunks would be a manifest with no word for the erasure at all.
	man := readManifestObject(t, store, image.ManifestKey(clone))
	if !spansCover(man.Discarded, erased) {
		t.Fatalf("the clone's manifest records %v as discarded, and not the range at %d it erased — the parent's bytes are what the next boot will read there",
			man.Discarded, erased)
	}
	for _, c := range man.Chunks {
		if c.Offset == untouched {
			t.Errorf("the clone's manifest names the range at %d, which only its parent ever wrote", untouched)
		}
	}

	// The restart: nothing in memory, everything from the bucket. The ancestry is
	// composed first and the clone's image is laid over it, which is what
	// agent.parentView and agent.fetchBase do in that order.
	again, _, err := image.LoadSnapshot(t.Context(), store, encFor(t, parent), image.OwnLineage(parent), snapID)
	if err != nil {
		t.Fatalf("re-loading the parent's snapshot: %v", err)
	}
	restarted, _, _, err := image.Load(t.Context(), store, encFor(t, clone), cloneID, again)
	if err != nil {
		t.Fatalf("re-loading the clone's image: %v", err)
	}

	assertReads(t, restarted, erased, 0x00,
		"a range the guest DISCARDed reads as its parent's bytes again after a restart: the erasure was resurrected")
	assertReads(t, restarted, untouched, 0xA1, "a restarted clone still reads the ranges only its parent ever wrote")
	assertReads(t, restarted, ownWrite, 0xC2, "a restarted clone reads what it wrote itself")
}

// §25.2 for the field this increment added: publish/load over an ancestry is exact for any
// sequence of writes and discards, and a truncated manifest is always detected.
//
// The round trip is the property that matters — the composed view a reader rebuilds must
// answer identically at every offset, or a volume comes back as something other than what
// it was — and it is checked over both a clone's image and its ancestry so a delta that
// happened to be right for one shape is not mistaken for a format.
//
// # What the truncation arm proves, and what this format still cannot detect
//
// Every prefix of the stored manifest fails the load, which is the total half: a manifest
// is JSON, so a cut anywhere leaves bytes that do not parse, and a partially-read manifest
// can never be mistaken for a shorter volume.
//
// **A bit flip inside a manifest is a different matter and this format does not detect
// one.** The digest protects a chunk's *contents*; nothing protects the manifest naming
// them. Flipping one bit of a decimal digit yields another decimal digit — `"offset":1024`
// becomes `"offset":1025` — and the load succeeds with data at the wrong place. That
// exposure is not new: `chunks[].offset` has had it since the format existed, and
// `discarded` inherits exactly it and no more. It is *worse in consequence* now, because a
// moved tombstone uncovers an ancestor's bytes rather than misplacing this volume's, and
// that is the reason to write it down here rather than leave it implied. Closing it is
// `descriptor.frame`/`unframe` applied to this object — a digest line over the bytes as
// stored, which the descriptor already proved must be over the *stored* bytes and not a
// re-encoding — and it is a second on-S3 format change that this increment deliberately
// did not fold into itself.
func TestADeltaOverAnAncestryRoundTripsAndATruncatedManifestIsRefused(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		ctx := t.Context()
		store := sim.NewObjectStore()
		parent, clone := volumeID(0xAA), volumeID(0xCC)
		cloneID := image.Ident{Volume: clone, Lineage: parent}
		const snapID = "01930000-0000-7000-8000-00000000000a"

		// The ancestry, published as a delta of its own (a root inherits nothing).
		ancestryView := cow.NewIntervalMap()
		drawInto(rt, ancestryView, "parent")
		if _, err := image.PublishSnapshot(ctx, store, rand.Reader, encFor(t, parent), image.OwnLineage(parent), ancestryView, nil, 1, snapID); err != nil {
			rt.Fatalf("publishing the parent's snapshot: %v", err)
		}
		ancestry, _, err := image.LoadSnapshot(ctx, store, encFor(t, parent), image.OwnLineage(parent), snapID)
		if err != nil {
			rt.Fatalf("loading the parent's snapshot: %v", err)
		}

		view := cow.NewIntervalMapOver(ancestry)
		drawInto(rt, view, "clone")
		if _, err := image.Publish(ctx, store, rand.Reader, encFor(t, clone), cloneID, view, ancestry, 2, ""); err != nil {
			rt.Fatalf("the clone's stop: %v", err)
		}

		loaded, _, _, err := image.Load(ctx, store, encFor(t, clone), cloneID, ancestry)
		if err != nil {
			rt.Fatalf("Load: %v", err)
		}
		want, got := make([]byte, deltaVolumeSize), make([]byte, deltaVolumeSize)
		view.Read(0, want)
		loaded.Read(0, got)
		if !bytes.Equal(want, got) {
			for i := range want {
				if want[i] != got[i] {
					rt.Fatalf("byte %d: the clone published %d, its image over its ancestry reads %d", i, want[i], got[i])
				}
			}
		}

		// Truncation, at a drawn cut. Every prefix must fail; a manifest that parsed
		// short would describe a volume with ranges missing, and missing reads as the
		// ancestry underneath.
		key := image.ManifestKey(clone)
		body, err := store.Get(ctx, key)
		if err != nil {
			rt.Fatal(err)
		}
		cut := rapid.IntRange(0, len(body)-1).Draw(rt, "cut")
		if _, err := store.Put(ctx, key, body[:cut], objectstore.PutOptions{}); err != nil {
			rt.Fatal(err)
		}
		if _, _, _, err := image.Load(ctx, store, encFor(t, clone), cloneID, ancestry); err == nil {
			rt.Fatalf("a manifest truncated to %d of %d bytes loaded without complaint", cut, len(body))
		}
	})
}

// drawInto draws writes and discards over an 8 KiB volume, the same shape drawView draws
// but into a caller's map — a layered one, so the discards are tombstones rather than
// being dropped on the floor.
func drawInto(rt *rapid.T, m *cow.IntervalMap, label string) {
	for range rapid.IntRange(0, 10).Draw(rt, label+" ops") {
		off := uint64(rapid.IntRange(0, deltaVolumeSize-1).Draw(rt, label+" off"))
		n := rapid.IntRange(1, 1024).Draw(rt, label+" len")
		if off+uint64(n) > deltaVolumeSize {
			n = deltaVolumeSize - int(off)
		}
		if rapid.Bool().Draw(rt, label+" discard") {
			m.Clear(off, uint64(n))
			continue
		}
		m.Overwrite(off, bytes.Repeat([]byte{byte(rapid.IntRange(1, 255).Draw(rt, label+" b"))}, n))
	}
}

func spansCover(spans []image.Span, off uint64) bool {
	for _, s := range spans {
		if off >= s.Offset && off < s.Offset+s.Length {
			return true
		}
	}
	return false
}

func assertReads(t *testing.T, view *cow.IntervalMap, off uint64, want byte, why string) {
	t.Helper()
	got := make([]byte, 512)
	view.Read(off, got)
	if !bytes.Equal(got, bytes.Repeat([]byte{want}, len(got))) {
		t.Fatalf("offset %d reads %#x, want %#x repeated — %s", off, got[:8], want, why)
	}
}
