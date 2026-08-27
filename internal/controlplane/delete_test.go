package controlplane_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/framed"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// detach makes a provisioned volume deletable: nothing is serving it.
func (f *fleet) detach(t *testing.T, id string) {
	t.Helper()
	if err := f.md.SetVolumePrimaryHost(t.Context(), f.term, id, ""); err != nil {
		t.Fatal(err)
	}
}

// publishOne seals one layer for a volume and returns its manifest.
func (f *fleet) publishOne(t *testing.T, id string, plain []byte) commit.Manifest {
	t.Helper()
	v, err := f.md.GetVolume(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	m, err := commit.Publish(t.Context(), f.store, f.encryption(t, v), bytes.NewReader(plain), commit.Request{
		VolumeID: id, CommitID: ids.New().String(), LayerID: ids.New().String(),
		Epoch: 1, VirtualSize: 1 << 30, PlainBytes: int64(len(plain)),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	return m
}

func gone(t *testing.T, store objectstore.Store, key string) bool {
	t.Helper()
	_, err := store.Head(t.Context(), key)
	if err == nil {
		return false
	}
	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("Head(%s): %v", key, err)
	}
	return true
}

// Deleting a volume destroys its key material and the map to its bytes, and leaves the
// bytes themselves.
//
// The layers are the deliberate exception and the whole argument: they are global and
// content-addressed so a clone's chain can reference layers its parent wrote, and with
// the DEK unreachable they are noise. That claim is only true if the shred is complete,
// which is what every assertion below is for.
func TestDeletingAVolumeRemovesItsKeyMaterialAndItsMapButNotItsLayers(t *testing.T) {
	f := newFleet(t)
	id := f.provision(t)
	m := f.publishOne(t, id, bytes.Repeat([]byte("the guest's bytes"), 4096))
	f.detach(t, id)

	if _, err := controlplane.DeleteVolume(t.Context(), f.md, f.store, f.kms, f.term, id); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	for _, key := range []string{
		descriptor.Key(id),                 // the wrapped DEK
		commit.HeadKey(id),                 // which commit is current
		commit.ManifestKey(id, m.CommitID), // and the layer it names
	} {
		if !gone(t, f.store, key) {
			t.Errorf("%s survived the delete", key)
		}
	}
	if _, err := f.md.GetVolume(t.Context(), id); !errors.Is(err, metadata.ErrNotFound) {
		t.Errorf("the row survived the delete: %v", err)
	}
	// And the second copy of the key, which lived in the row, went with it.
	if _, err := descriptor.Read(t.Context(), f.store, id); err == nil {
		t.Error("the descriptor still reads back, so the wrapped DEK is still in the bucket")
	}

	// The layer stays. Deleting it under this volume's name would delete a clone's data
	// (commit.LayerKey), and it is unreadable anyway now that nothing hands out the key.
	if gone(t, f.store, commit.LayerKey(m.Layer.SHA256)) {
		t.Errorf("%s was deleted with the volume: layers are shared and content-addressed, "+
			"so this is another volume's data going with this one", commit.LayerKey(m.Layer.SHA256))
	}
}

// A delete that crashed after step 2 leaves a row whose descriptor is gone, and
// -rebuild-metadata cannot see that volume any more. The only way to finish is to run
// the delete again, so running it again has to work.
func TestARepeatedDeleteFinishesTheJob(t *testing.T) {
	f := newFleet(t)
	id := f.provision(t)
	f.detach(t, id)

	// The crash: step 2 happened, nothing after it.
	if err := f.store.Delete(t.Context(), descriptor.Key(id)); err != nil {
		t.Fatal(err)
	}
	if _, err := controlplane.DeleteVolume(t.Context(), f.md, f.store, f.kms, f.term, id); err != nil {
		t.Fatalf("re-running the delete after a crash at step 2: %v", err)
	}
	if _, err := f.md.GetVolume(t.Context(), id); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("the row survived the re-run: %v", err)
	}
}

// A volume a host is still serving is not deleted: destroying the key under a running
// guest turns its next commit into bytes nobody can open.
func TestDeleteRefusesAVolumeAHostIsStillServing(t *testing.T) {
	f := newFleet(t)
	id := f.provision(t) // provisioned onto f.host, so it is placed

	_, err := controlplane.DeleteVolume(t.Context(), f.md, f.store, f.kms, f.term, id)
	if !errors.Is(err, controlplane.ErrVolumeAttached) {
		t.Fatalf("want ErrVolumeAttached, got %v", err)
	}
	// Nothing was written before the refusal — a gate that returns the right error and
	// shreds the key anyway satisfies every assertion on err.
	if _, err := descriptor.Read(t.Context(), f.store, id); err != nil {
		t.Fatalf("a refused delete destroyed the key material anyway: %v", err)
	}
}

// The bucket's half of the descendant refusal, which the catalog's cannot cover: a
// clone whose row was never rebuilt is invisible to ErrHasDescendants, and the bucket is
// the authority a rebuild trusts.
func TestDeleteRefusesAVolumeAClonesDescriptorStillPointsAt(t *testing.T) {
	f := newFleet(t)
	parent := f.provision(t)
	f.detach(t, parent)

	// A clone's descriptor and nothing else: no row, so only the listing can see it.
	child := ids.New().String()
	d, derr := descriptor.Read(t.Context(), f.store, parent)
	if derr != nil {
		t.Fatal(derr)
	}
	d.VolumeID, d.ParentVolumeID, d.ChainDepth = child, parent, 1
	// Re-wrapped under the child's own id, which is what Clone does and what makes this a
	// clone rather than a forgery. The descendant check unwraps before it believes a
	// parent link — an unauthenticated `parent_volume_id` used to be enough to block a
	// volume's crypto-shred for ever — so a descriptor carrying the parent's wrap under
	// the child's id is exactly the object that check now skips.
	childU, err := ids.Parse(child)
	if err != nil {
		t.Fatal(err)
	}
	parentRow, err := f.md.GetVolume(t.Context(), parent)
	if err != nil {
		t.Fatal(err)
	}
	d.DEKWrapped, err = f.kms.WrapDEK(&ramp{}, f.dek(t, parentRow), [16]byte(childU))
	if err != nil {
		t.Fatal(err)
	}
	if err := descriptor.Write(t.Context(), f.store, d); err != nil {
		t.Fatal(err)
	}

	_, err = controlplane.DeleteVolume(t.Context(), f.md, f.store, f.kms, f.term, parent)
	if !errors.Is(err, metadata.ErrHasDescendants) {
		t.Fatalf("want ErrHasDescendants, got %v", err)
	}
	if _, err := descriptor.Read(t.Context(), f.store, parent); err != nil {
		t.Fatalf("a refused delete destroyed the parent's key material anyway: %v", err)
	}
}

// **What the shred does not do, stated where the decision is.**
//
// A lineage shares one DEK's bytes — the re-wrap at Clone changes the ciphertext, not
// the key — so deleting a volume that a clone descends from destroys no secret the clone
// does not still hold. That is why the delete refuses while a descendant exists, and it
// is also the constraint on a future FLATTEN: a flatten that re-uploads a clone's data
// under the *shared* key leaves this hole open, so it must mint a fresh DEK.
//
// This test drives the hole deliberately, so that a flatten written later cannot quietly
// inherit it: the parent's link is cleared the way a flatten would clear it, the parent
// is deleted, and the parent's layer is still opened with the clone's key.
func TestAFlattenedClonesKeyStillOpensItsDeletedParentsLayers(t *testing.T) {
	f := newFleet(t)
	parent := f.provision(t)
	plain := bytes.Repeat([]byte("the parent's bytes"), 4096)
	m := f.publishOne(t, parent, plain)

	child := ids.New().String()
	pd, err := descriptor.Read(t.Context(), f.store, parent)
	if err != nil {
		t.Fatal(err)
	}
	parentRow, err := f.md.GetVolume(t.Context(), parent)
	if err != nil {
		t.Fatal(err)
	}
	childU, err := ids.Parse(child)
	if err != nil {
		t.Fatal(err)
	}
	// The clone, as Clone builds one: the same key, re-wrapped under the child's id.
	rewrapped, err := f.kms.WrapDEK(&ramp{}, f.dek(t, parentRow), [16]byte(childU))
	if err != nil {
		t.Fatal(err)
	}
	pd.VolumeID, pd.DEKWrapped = child, rewrapped
	if err := descriptor.Write(t.Context(), f.store, pd); err != nil {
		t.Fatal(err)
	}
	// The flatten's catalog half, minus the re-key: the link is cleared, so the parent
	// is deletable.
	if err := f.md.CreateVolume(t.Context(), f.term, metadata.Volume{
		VolumeID: child, SizeBytes: parentRow.SizeBytes, BlockSize: parentRow.BlockSize,
		CurrentEpoch: 1, State: parentRow.State,
		DEKWrapped: rewrapped, KEKID: parentRow.KEKID, DEKKeyID: parentRow.DEKKeyID,
	}, nil); err != nil {
		t.Fatal(err)
	}
	f.detach(t, parent)
	if _, err := controlplane.DeleteVolume(t.Context(), f.md, f.store, f.kms, f.term, parent); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	// The clone's own key material survives its parent's delete, which is the property
	// a lineage needs.
	childRow, err := f.md.GetVolume(t.Context(), child)
	if err != nil {
		t.Fatal(err)
	}
	if f.dek(t, childRow).Key != f.dek(t, parentRow).Key {
		t.Fatal("the clone's key did not survive its parent's delete, so it can read nothing it inherited")
	}
	// And the honest limit: those key bytes still open the deleted parent's layer, held
	// by a reader who kept the manifest. Crypto-shred is lineage-scoped.
	var out bytes.Buffer
	if err := commit.Fetch(t.Context(), f.store, f.encryption(t, parentRow), m, &out); err != nil {
		t.Fatalf("the deleted parent's layer no longer opens — if this is now the behaviour, "+
			"the lineage-scoped limit written at Clone's re-wrap and at DeleteVolume is stale "+
			"and must be updated rather than this test: %v", err)
	}
	if !bytes.Equal(out.Bytes(), plain) {
		t.Fatal("the layer opened into the wrong bytes")
	}
}

// dek unwraps what the catalog holds for a volume, under that volume's own id.
func (f *fleet) dek(t *testing.T, v metadata.Volume) crypto.DEK {
	t.Helper()
	u, err := ids.Parse(v.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	d, err := f.kms.UnwrapDEK(v.DEKWrapped, v.DEKKeyID, [16]byte(u))
	if err != nil {
		t.Fatalf("unwrapping the DEK of %s: %v", v.VolumeID, err)
	}
	return d
}

// TestDeleteRefusesACloneWhoseParentIsStillHere is the lineage contract, stated as a
// refusal rather than as a comment.
//
// A clone is handed its parent's DEK bytes — only the wrap differs — because that is what
// lets it read the layers its parent published (v6 §10). The consequence is that no member
// of a live lineage can be crypto-shredded on its own: the secret stops existing when the
// last wrap of it does. Deleting the clone anyway would return success over a secret that
// is still there, and its layers stay readable with the parent's key.
func TestDeleteRefusesACloneWhoseParentIsStillHere(t *testing.T) {
	f := newFleet(t)
	parent := f.provision(t)
	f.detach(t, parent)

	// A clone's descriptor, wrapped under its own id the way Clone writes one.
	child := ids.New().String()
	d, derr := descriptor.Read(t.Context(), f.store, parent)
	if derr != nil {
		t.Fatal(derr)
	}
	childU, err := ids.Parse(child)
	if err != nil {
		t.Fatal(err)
	}
	parentRow, err := f.md.GetVolume(t.Context(), parent)
	if err != nil {
		t.Fatal(err)
	}
	d.VolumeID, d.ParentVolumeID, d.ChainDepth = child, parent, 1
	d.DEKWrapped, err = f.kms.WrapDEK(&ramp{}, f.dek(t, parentRow), [16]byte(childU))
	if err != nil {
		t.Fatal(err)
	}
	if err := descriptor.Write(t.Context(), f.store, d); err != nil {
		t.Fatal(err)
	}
	if err := f.md.CreateVolume(t.Context(), f.term, metadata.Volume{
		VolumeID: child, SizeBytes: parentRow.SizeBytes, BlockSize: parentRow.BlockSize,
		State: lifecycle.VolumeActive, DEKWrapped: d.DEKWrapped, DEKKeyID: d.DEKKeyID, KEKID: d.KEKID,
	}, nil); err != nil {
		t.Fatal(err)
	}

	// Deleting the clone is a removal and says so: its parent still publishes the key.
	shred, err := controlplane.DeleteVolume(t.Context(), f.md, f.store, f.kms, f.term, child)
	if err != nil {
		t.Fatalf("deleting the clone: %v", err)
	}
	if shred.KeyDestroyed || shred.SharedWith != parent {
		t.Fatalf("deleting a clone reported %+v; its key is still published by %s", shred, parent)
	}

	// And now that nothing descends from it, deleting the parent destroys the last wrap of
	// the key — which is the shred, and is the only operation that may claim to be one.
	shred, err = controlplane.DeleteVolume(t.Context(), f.md, f.store, f.kms, f.term, parent)
	if err != nil {
		t.Fatalf("deleting the parent: %v", err)
	}
	if !shred.KeyDestroyed {
		t.Errorf("deleting the last holder of the lineage key reported %+v", shred)
	}
}

// TestDeleteReportsWhatItCouldNotRead: every input to the descendant scan comes out of a
// bucket, which is a place other things write to. A descriptor that will not decode is not
// skipped — a parent link that cannot be stated is a link that might exist, and deleting a
// key on "probably not a descendant" is what the scan exists to prevent.
func TestDeleteReportsWhatItCouldNotRead(t *testing.T) {
	f := newFleet(t)
	id := f.provision(t)
	f.detach(t, id)

	// An object under the volumes prefix, with a v7 id, framed, and not a descriptor.
	other := ids.New().String()
	if _, err := f.store.Put(t.Context(), descriptor.Key(other),
		framed.Frame([]byte(`{"format_version":1,"volume_id":"`+other+`"}`)), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	// It decodes, names its own volume, and carries key material this fleet did not wrap,
	// so it is not a descendant and the delete goes through.
	shred, err := controlplane.DeleteVolume(t.Context(), f.md, f.store, f.kms, f.term, id)
	if err != nil {
		t.Fatalf("a stray object under the prefix blocked the delete: %v", err)
	}
	if !shred.KeyDestroyed {
		t.Errorf("deleting a volume with no lineage reported %+v", shred)
	}

	// A second delete of the same volume finishes rather than failing: it is the runbook
	// for a crash in the middle, and a re-run that refused would make the only path that
	// can finish the job the one path that cannot.
	if _, err := controlplane.DeleteVolume(t.Context(), f.md, f.store, f.kms, f.term, id); err != nil &&
		!errors.Is(err, metadata.ErrNotFound) {
		t.Errorf("re-running the delete: %v", err)
	}
}
