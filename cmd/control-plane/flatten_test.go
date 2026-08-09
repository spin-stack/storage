package main

import (
	"bytes"
	"crypto/rand"
	"errors"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/lineage"
)

// cloneOffset is a range only the clone ever wrote: newDeleteWorld's parent session wrote
// at 0, so a flatten that dropped the clone's own layer answers zeros here and nowhere else.
const cloneOffset = uint64(64 * 1024)

// TestFlattenRefusesWhenTheClonesOwnImageIsGone is the command's half of the rule, and the
// reason it is asserted here rather than only in internal/lineage: the number that decides
// it lives in the catalog, and this function is the only thing that reads both.
//
// The clone has stopped cleanly once, so it owns a layer and the catalog counts it. Then its
// manifest leaves the bucket — a stray delete, a lifecycle expiry, a restore that missed a
// key. A flatten that read "no manifest" as "this clone never published" would compose the
// parent, publish that as the clone's whole content and rewrite the descriptor, and the
// block the clone wrote would be gone with an exit code of 0.
//
// So the assertions are the three things an operator has left to repair with: the refusal
// names the volume, the descriptor still says what the clone descends from, and the catalog
// row still does too. A flatten that half-ran before reporting the failure would take the
// second of those away, and restoring one object would no longer be enough.
func TestFlattenRefusesWhenTheClonesOwnImageIsGone(t *testing.T) {
	ctx := t.Context()
	w := newDeleteWorld(t)
	clone := w.cloneOfTheSnapshot(t)

	// The clone's own session: one block at an offset its parent never wrote, published
	// the way an Agent's teardown publishes it and reported the way its watermark report
	// reports it. Those two together are the state a clone that has been booted once is in
	// — the layer in the bucket, and the catalog's count of it.
	own := image.Ident{Volume: clone.u, Lineage: w.volumeU}
	view := cow.NewIntervalMap()
	view.Overwrite(cloneOffset, bytes.Repeat([]byte{0xC3}, 4096))
	if _, err := image.Publish(ctx, w.store, rand.Reader, clone.enc, own, view, nil, 9, ""); err != nil {
		t.Fatalf("publishing the clone's own image: %v", err)
	}
	if err := w.md.UpdateWatermarks(ctx, w.term, clone.id, 9, 9, 9); err != nil {
		t.Fatal(err)
	}
	if err := w.md.SetVolumePrimaryHost(ctx, w.term, clone.id, ""); err != nil {
		t.Fatal(err)
	}

	// The loss. Only the manifest goes; the clone's chunks stay where they are, which is
	// what makes restoring the one object a repair rather than a wish.
	if err := w.store.Delete(ctx, image.ManifestKey(clone.u)); err != nil {
		t.Fatalf("deleting the clone's manifest: %v", err)
	}

	err := flatten(ctx, w.md, w.store, w.kekFile, clone.id, w.term)
	if !errors.Is(err, lineage.ErrImageMissing) {
		t.Fatalf("-flatten-volume on a clone whose image was deleted returned %v, want lineage.ErrImageMissing", err)
	}
	if !strings.Contains(err.Error(), clone.id) {
		t.Errorf("the refusal does not name the volume an operator has to repair: %v", err)
	}

	// Nothing moved. The descriptor is what a re-run and a -rebuild-metadata both read, so
	// a refusal that had already cleared the parent link would have destroyed the only
	// record of what the clone reads through.
	d, derr := descriptor.Read(ctx, w.store, clone.id)
	if derr != nil {
		t.Fatalf("reading the clone's descriptor after the refusal: %v", derr)
	}
	if d.ParentSnapshotID != w.snapshotID {
		t.Errorf("the refused flatten left the descriptor naming parent snapshot %q, want %q", d.ParentSnapshotID, w.snapshotID)
	}
	v, verr := w.md.GetVolume(ctx, clone.id)
	if verr != nil {
		t.Fatal(verr)
	}
	if v.ParentSnapshotID != w.snapshotID {
		t.Errorf("the refused flatten cleared the catalog's lineage for %s", clone.id)
	}
}
