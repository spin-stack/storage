package controlplane_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// A fleet: one leading Control Plane, one host, one KEK, one deterministic byte source.
type fleet struct {
	md    *metasim.Store
	store *sim.ObjectStore
	kms   *crypto.DevKMS
	p     *controlplane.Provisioner
	term  int64
	host  string
}

func newFleet(t *testing.T) *fleet {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	md := metasim.New(clk.Wall)
	store := sim.NewObjectStore()
	term, err := md.AcquireLeadership(t.Context(), "cp-1")
	if err != nil {
		t.Fatal(err)
	}
	host := ids.New().String()
	if err := md.UpsertHost(t.Context(), term, metadata.Host{
		HostID: host, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
	}); err != nil {
		t.Fatal(err)
	}
	kms := testKMS(t)
	return &fleet{md: md, store: store, kms: kms,
		p: controlplane.NewProvisioner(md, store, kms, &ramp{}), term: term, host: host}
}

func (f *fleet) provision(t *testing.T) string {
	t.Helper()
	v, err := f.p.Provision(t.Context(), f.term, controlplane.VolumeSpec{
		SizeBytes: 1 << 30, BlockSize: 4096, HostID: f.host,
	})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	return v.VolumeID
}

// A single PUT of one unauthenticated object re-keys a live volume.
//
// `volumes/<id>/descriptor.json` carries the wrapped DEK and is only digest-framed —
// framed.go says out loud that the digest is not authentication, "anything that can
// write the object can write a matching digest". RebuildMetadata reads it and calls
// CreateVolume, whose conflict path is documented as converging *without regressing*:
// the epoch, the size, the watermarks and the ownership columns all keep the higher or
// existing value, `state` is not touched, `parent_snapshot_id` is never cleared.
//
// The three key columns are the only ones that are taken wholesale from the bucket.
func TestAdversaryDescriptorSwapRekeysALiveVolume(t *testing.T) {
	f := newFleet(t)
	victim := f.provision(t)
	attacker := f.provision(t)

	real, err := f.md.GetVolume(t.Context(), victim)
	if err != nil {
		t.Fatal(err)
	}
	mine, err := f.md.GetVolume(t.Context(), attacker)
	if err != nil {
		t.Fatal(err)
	}

	// One PutObject on the victim's prefix: everything the descriptor says about the
	// volume is left alone except which key opens it.
	d, err := descriptor.Read(t.Context(), f.store, victim)
	if err != nil {
		t.Fatal(err)
	}
	d.DEKWrapped = mine.DEKWrapped
	d.DEKKeyID = mine.DEKKeyID
	if err := descriptor.Write(t.Context(), f.store, d); err != nil {
		t.Fatal(err)
	}

	// The operator's ordinary repair after losing (or half-losing) the catalog. It must
	// refuse: the swapped wrap was not sealed for this volume, and the KEK is the only
	// witness that can say so. Refusing the *whole* rebuild rather than skipping the
	// volume is deliberate — see controlplane.checkKey.
	if _, err := controlplane.RebuildMetadata(t.Context(), f.md, f.store, f.kms, f.term); err == nil {
		t.Fatal("a rebuild recorded a wrapped DEK that was not sealed for the volume it was found under")
	} else if !errors.Is(err, crypto.ErrUnwrap) {
		t.Fatalf("the rebuild refused, but not because the key failed to open: %v", err)
	}

	after, err := f.md.GetVolume(t.Context(), victim)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after.DEKWrapped, real.DEKWrapped) {
		t.Fatalf("volume %s was re-keyed by a bucket write: the catalog now hands its Agent "+
			"the wrapped DEK of volume %s. Every layer already published under the real key "+
			"is unopenable, and every layer published after this is sealed with a key whoever "+
			"wrote that object chose.", victim, attacker)
	}
}

// The same swap makes the volume's already-published history unopenable.
//
// This is the other half of the first test and the one with teeth: the layers in the
// bucket were sealed with the real DEK, and after the swap the only key anything can
// reach is the attacker's. `Commit() -> SUCCESS` promised those bytes were
// reconstructible without the host; nothing on the read path can even say why they are
// not any more, because a wrapped DEK carries no statement of the volume it belongs to
// (DevKMS.WrapDEK binds the 4-byte key version as AAD and nothing else).
func TestAdversaryDescriptorSwapOrphansThePublishedHistory(t *testing.T) {
	f := newFleet(t)
	victim := f.provision(t)
	attacker := f.provision(t)

	real, err := f.md.GetVolume(t.Context(), victim)
	if err != nil {
		t.Fatal(err)
	}
	enc := f.encryption(t, real)

	plain := bytes.Repeat([]byte("the guest's bytes"), 4096)
	layerID := ids.New().String()
	m, err := commit.Publish(t.Context(), f.store, enc, bytes.NewReader(plain), commit.Request{
		VolumeID: victim, CommitID: ids.New().String(), LayerID: layerID,
		Epoch: 1, VirtualSize: 1 << 30, PlainBytes: int64(len(plain)),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	mine, err := f.md.GetVolume(t.Context(), attacker)
	if err != nil {
		t.Fatal(err)
	}
	d, err := descriptor.Read(t.Context(), f.store, victim)
	if err != nil {
		t.Fatal(err)
	}
	d.DEKWrapped, d.DEKKeyID = mine.DEKWrapped, mine.DEKKeyID
	if err := descriptor.Write(t.Context(), f.store, d); err != nil {
		t.Fatal(err)
	}
	if _, err := controlplane.RebuildMetadata(t.Context(), f.md, f.store, f.kms, f.term); err == nil {
		t.Fatal("a rebuild recorded a wrapped DEK that was not sealed for the volume it was found under")
	}

	// What a recovery does: take the key material the catalog holds for this volume,
	// unwrap it, bind it to the volume, walk the chain.
	after, err := f.md.GetVolume(t.Context(), victim)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := commit.Fetch(t.Context(), f.store, f.encryption(t, after), m, &out); err != nil {
		t.Fatalf("the volume's published layer can no longer be opened with the key the "+
			"catalog holds for it, and one PUT of an unauthenticated descriptor is what did "+
			"it: %v", err)
	}
	if !bytes.Equal(out.Bytes(), plain) {
		t.Fatal("the layer opened into the wrong bytes")
	}
}

// encryption is publisher.encryption / recovery.encryption: unwrap what the catalog
// holds and bind it to the volume.
func (f *fleet) encryption(t *testing.T, v metadata.Volume) *crypto.Encryption {
	t.Helper()
	id, err := ids.Parse(v.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	dek, err := f.kms.UnwrapDEK(v.DEKWrapped, v.DEKKeyID, [16]byte(id))
	if err != nil {
		t.Fatalf("unwrapping the DEK of volume %s: %v", v.VolumeID, err)
	}
	enc, err := crypto.NewEncryption(dek, id)
	if err != nil {
		t.Fatal(err)
	}
	return enc
}

// Crypto-shred: the only delete verb this tree has leaves a usable key in the bucket.
//
// metadata.Store.DeleteVolume removes the row. Nothing removes
// `volumes/<id>/descriptor.json`, which carries `dek_wrapped` and `dek_key_id` — and
// nothing removes the layers either, which are content-addressed and global. So after a
// "delete", the ciphertext and the key that opens it are both still in the bucket, and
// anyone holding the deployment KEK reads the deleted volume back.
func TestAdversaryDeleteVolumeLeavesTheWrappedDEKInTheBucket(t *testing.T) {
	f := newFleet(t)
	id := f.provision(t)

	before, err := f.md.GetVolume(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.md.SetVolumeState(t.Context(), f.term, id, lifecycle.VolumeDetached); err != nil {
		t.Fatal(err)
	}
	// Detached from its host: deleting a volume a guest is still writing to is refused,
	// and metadata.Store says why that precondition belongs to the delete command rather
	// than to the row.
	if err := f.md.SetVolumePrimaryHost(t.Context(), f.term, id, ""); err != nil {
		t.Fatal(err)
	}
	// controlplane.DeleteVolume, not metadata.Store.DeleteVolume: the catalog cannot
	// reach the bucket, so the row-only verb could never satisfy this test. That is the
	// defect — deletion had no owner that could destroy the key material — and giving it
	// one is the fix, not a change of subject.
	if _, err := controlplane.DeleteVolume(t.Context(), f.md, f.store, f.kms, f.term, id); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
	// The row is gone too, or the delete only half happened.
	if _, err := f.md.GetVolume(t.Context(), id); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("the volume row survived the delete: %v", err)
	}

	d, err := descriptor.Read(t.Context(), f.store, id)
	if err != nil {
		// The shred worked: nothing left to unwrap.
		return
	}
	u, err := ids.Parse(id)
	if err != nil {
		t.Fatal(err)
	}
	dek, err := f.kms.UnwrapDEK(d.DEKWrapped, d.DEKKeyID, [16]byte(u))
	if err != nil {
		t.Fatalf("the descriptor survived the delete but its key no longer unwraps: %v", err)
	}
	original, err := f.kms.UnwrapDEK(before.DEKWrapped, before.DEKKeyID, [16]byte(u))
	if err != nil {
		t.Fatal(err)
	}
	if dek.Key == original.Key {
		t.Fatalf("volume %s was deleted and its DEK is still recoverable from %s: "+
			"the object store still hands out the wrapped key, and the layers it opens "+
			"are still there too", id, descriptor.Key(id))
	}
}

// The controls: the same machinery with nothing tampered with. They must pass, which is
// what says the three tests above are assertions about the system and not about the
// fixture.
func TestAdversaryControlsUntamperedBucket(t *testing.T) {
	f := newFleet(t)
	id := f.provision(t)
	before, err := f.md.GetVolume(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}

	plain := bytes.Repeat([]byte("the guest's bytes"), 4096)
	m, err := commit.Publish(t.Context(), f.store, f.encryption(t, before), bytes.NewReader(plain),
		commit.Request{
			VolumeID: id, CommitID: ids.New().String(), LayerID: ids.New().String(),
			Epoch: 1, VirtualSize: 1 << 30, PlainBytes: int64(len(plain)),
		})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if _, err := controlplane.RebuildMetadata(t.Context(), f.md, f.store, f.kms, f.term); err != nil {
		t.Fatal(err)
	}
	after, err := f.md.GetVolume(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after.DEKWrapped, before.DEKWrapped) {
		t.Fatal("a rebuild from an untampered bucket changed the wrapped DEK")
	}
	var out bytes.Buffer
	if err := commit.Fetch(t.Context(), f.store, f.encryption(t, after), m, &out); err != nil {
		t.Fatalf("Fetch after an untampered rebuild: %v", err)
	}
	if !bytes.Equal(out.Bytes(), plain) {
		t.Fatal("the layer opened into the wrong bytes")
	}
}
