package controlplane_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/placement"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// A second pass at the key material, after the wrap was bound to its volume. The wrap
// is the only thing that got a witness; everything else the bucket says about a volume
// is still taken on trust, and these are the places where that costs something.

// freshCatalog is the situation -rebuild-metadata exists for: PostgreSQL is gone and a
// new one is empty. It is not the same as re-running a rebuild against the catalog that
// is still there — every "converge, do not regress" rule in metadata.converge has
// nothing to converge *with* — and it is the only case the operator running this
// command is actually in.
func freshCatalog(t *testing.T) (*metasim.Store, int64) {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	md := metasim.New(clk.Wall)
	term, err := md.AcquireLeadership(t.Context(), "cp-after-the-loss")
	if err != nil {
		t.Fatal(err)
	}
	return md, term
}

// The epoch a rebuild restores must not be the epoch the volume had when it was *created*,
// and the epoch is the fencing token.
//
// descriptor.CurrentEpoch is written once, at Provision and at Clone; controlplane.Place
// grants every attach a fresh epoch through BumpVolumeEpoch. So a volume handed between
// hosts N times has epoch N+1 in the catalog and 1 in the bucket, and a rebuild from the
// bucket alone — the one path this object exists to serve — brings it back at an epoch every
// host that ever held it outranks. Against an empty catalog the converge path's GREATEST
// has nothing to compare with, and that is precisely the run.
func TestAdversaryRebuildRestoresAVolumeAtAFencedPredecessorsEpoch(t *testing.T) {
	f := newFleet(t)
	id := f.provision(t)

	// Three hand-overs, each an ordinary attach, driven through controlplane.Place rather
	// than BumpVolumeEpoch: a grant writes the new epoch to the bucket *before* it moves the
	// catalog, and a fixture that bumps the catalog alone constructs a state no Control Plane
	// can produce.
	epoch := int64(1)
	for range 3 {
		f.detach(t, id)
		placed, err := controlplane.Place(t.Context(), f.md, f.store, placement.Policy{}, f.term, id, f.host)
		if err != nil {
			t.Fatalf("granting the volume: %v", err)
		}
		epoch = placed.Epoch
	}
	if epoch <= 1 {
		t.Fatalf("the fixture never advanced the epoch: %d", epoch)
	}

	fresh, freshTerm := freshCatalog(t)
	if _, err := controlplane.RebuildMetadata(t.Context(), fresh, f.store, f.kms, freshTerm); err != nil {
		t.Fatalf("RebuildMetadata: %v", err)
	}
	got, err := fresh.GetVolume(t.Context(), id)
	if err != nil {
		t.Fatalf("the volume was not rebuilt: %v", err)
	}
	if got.CurrentEpoch < epoch {
		t.Fatalf("volume %s was fenced up to epoch %d and the rebuild brought it back at epoch %d. "+
			"Every host that ever held it holds a token this catalog will accept: the next attach "+
			"grants epoch %d, which a predecessor already used, and commit.Publish's epoch fence "+
			"compares one manifest's epoch against another",
			id, epoch, got.CurrentEpoch, got.CurrentEpoch+1)
	}
}

// Clone takes the new volume's id from its caller and writes it with CreateVolume, whose
// conflict path is a *merge* built for rebuild idempotency. Point it at a volume that
// already exists and the merge re-keys it: converge keeps the authority columns, while
// dek_wrapped, dek_key_id, chain_depth and the parent link come from the new record
// wholesale. Provision cannot be attacked this way — it mints its own id.
//
// It is the descriptor-swap outcome reached through the catalog, and worse in the one way
// that matters: the swapped-in wrap really is sealed for this volume, so checkKey
// authenticates it and no witness anywhere says the volume was re-keyed.
func TestAdversaryCloneOntoAnExistingVolumeIDRekeysThatVolume(t *testing.T) {
	f := newFleet(t)
	victim := f.provision(t)
	plain := bytes.Repeat([]byte("the victim's bytes"), 4096)
	m := f.publishOne(t, victim, plain)
	before, err := f.md.GetVolume(t.Context(), victim)
	if err != nil {
		t.Fatal(err)
	}

	source := f.provision(t)
	snap := publishSnapshotOf(t, f.md, f.term, source)

	// One clone, whose new-volume id is a volume that already exists and is serving a
	// guest. A mistyped id, a retried operator command, or anything that reuses a
	// pre-allocated id.
	if _, err := controlplane.Clone(t.Context(), f.md, f.store, f.kms, &ramp{},
		placement.Policy{}, nil, f.term, snap, victim); err == nil {
		t.Fatalf("Clone created volume %s on top of a volume that already exists", victim)
	}

	after, err := f.md.GetVolume(t.Context(), victim)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after.DEKWrapped, before.DEKWrapped) || after.DEKKeyID != before.DEKKeyID {
		t.Fatalf("volume %s was re-keyed by a clone that named it: the catalog now holds "+
			"another lineage's DEK for it", victim)
	}
	var out bytes.Buffer
	if err := commit.Fetch(t.Context(), f.store, f.encryption(t, after), m, &out); err != nil {
		t.Fatalf("the victim's published layer can no longer be opened with the key the catalog "+
			"holds for it: %v", err)
	}
	if !bytes.Equal(out.Bytes(), plain) {
		t.Fatal("the layer opened into the wrong bytes")
	}
}

// One PUT of a descriptor nobody authenticated blocks the crypto-shred of any volume in the
// fleet, permanently.
//
// DeleteVolume's bucket-side descendant check refuses if any object under
// descriptor.Prefix names the victim as its parent, and `parent_volume_id` is an
// unauthenticated field an adversary chooses. The operator cannot clear it: the error tells
// them to FLATTEN a volume that does not exist. The wrapped DEK is the one thing they
// cannot fake, which is what the check now holds every descriptor up to.
func TestAdversaryAForgedDescendantBlocksTheShredForever(t *testing.T) {
	f := newFleet(t)
	victim := f.provision(t)
	f.publishOne(t, victim, bytes.Repeat([]byte("the victim's bytes"), 4096))
	f.detach(t, victim)

	// Not a clone, not a volume: an object. Its wrapped DEK is 32 bytes of nothing,
	// which is what says no Control Plane holding this KEK ever created it.
	ghost := ids.New().String()
	if err := descriptor.Write(t.Context(), f.store, descriptor.Descriptor{
		VolumeID: ghost, SizeBytes: 1 << 30, BlockSize: 4096, CurrentEpoch: 1,
		KEKID: f.kms.KEKID(), DEKWrapped: bytes.Repeat([]byte{0xAA}, 60), DEKKeyID: 1,
		ParentSnapshotID: ids.New().String(), ParentVolumeID: victim,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := controlplane.DeleteVolume(t.Context(), f.md, f.store, f.kms, f.term, victim); err != nil {
		t.Fatalf("the crypto-shred of volume %s is refused because an unauthenticated object "+
			"under %s claims to descend from it. Nothing this fleet wrapped that key, and the "+
			"operator's only instruction is to FLATTEN a volume that does not exist: %v",
			victim, descriptor.Key(ghost), err)
	}
	if !gone(t, f.store, descriptor.Key(victim)) {
		t.Fatalf("%s survived", descriptor.Key(victim))
	}
}

// Deleting a clone shreds nothing while its parent is alive.
//
// The direction an operator actually performs — keep the golden image, delete the ephemeral
// clone — has no refusal and no warning. The clone's layers stay under the global layers/
// prefix and its key *bytes* are the parent's (§10), still wrapped and openable in the
// parent's descriptor, so "with the DEK gone the bytes are noise" is false for every clone
// the fleet deletes.
func TestAdversaryDeletingACloneShredsNothingWhileItsParentLives(t *testing.T) {
	f := newFleet(t)
	parent := f.provision(t)
	snap := publishSnapshotOf(t, f.md, f.term, parent)
	clone, err := controlplane.Clone(t.Context(), f.md, f.store, f.kms, &ramp{},
		placement.Policy{}, nil, f.term, snap, ids.New().String())
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}

	// The clone's guest writes, sealed under the clone's own id.
	plain := bytes.Repeat([]byte("what the clone wrote, and only the clone"), 512)
	m := f.publishOne(t, clone.VolumeID, plain)

	f.detach(t, clone.VolumeID)
	// The delete succeeds and *reports* that it shredded nothing, which is the contract this
	// test pins. Refusing outright was the first fix and it deadlocked deletion: a parent
	// cannot go while a descendant exists either, so a cloned lineage became undeletable.
	shred, err := controlplane.DeleteVolume(t.Context(), f.md, f.store, f.kms, f.term, clone.VolumeID)
	if err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
	if shred.KeyDestroyed {
		t.Fatal("the delete reported a crypto-shred for a volume whose key its parent still publishes")
	}
	if shred.SharedWith != parent {
		t.Errorf("the report names %q as the holder of the key, want %q", shred.SharedWith, parent)
	}

	// What is left in the bucket: the parent's descriptor, and the layer objects. The
	// manifest is what an adversary kept — an old backup, an access log, the Agent's own
	// copy — and a shred whose completeness depends on public metadata staying secret is
	// not a shred.
	parentRow, err := f.md.GetVolume(t.Context(), parent)
	if err != nil {
		t.Fatal(err)
	}
	pd, err := descriptor.Read(t.Context(), f.store, parent)
	if err != nil {
		t.Fatal(err)
	}
	pu, err := ids.Parse(parent)
	if err != nil {
		t.Fatal(err)
	}
	dek, err := f.kms.UnwrapDEK(pd.DEKWrapped, pd.DEKKeyID, [16]byte(pu))
	if err != nil {
		t.Fatalf("the parent's descriptor no longer unwraps: %v", err)
	}
	_ = parentRow
	cu, err := ids.Parse(clone.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	// The lineage key, bound to the *deleted* volume's id. Nothing forbids the pairing:
	// the bytes are the parent's and the id is public.
	enc, err := crypto.NewEncryption(dek, [16]byte(cu))
	if err != nil {
		t.Fatal(err)
	}
	// The parent's wrap, still in the bucket, still opens every byte the clone's guest wrote.
	// Asserted rather than merely observed: the day it stops being true is the day a clone
	// holds a key of its own and deleting one alone IS a shred.
	var out bytes.Buffer
	if err := commit.Fetch(t.Context(), f.store, enc, m, &out); err != nil {
		t.Fatalf("the clone's layers no longer open under the lineage key (%v). If a clone now "+
			"holds a key of its own, deleting one alone IS a shred and DeleteVolume's "+
			"ErrSharedLineage refusal has become wrong", err)
	}
	if !bytes.Equal(out.Bytes(), plain) {
		t.Fatal("the lineage key opened the clone's layer into something other than what its guest wrote")
	}
}

// An object under descriptor.Prefix that is not a descriptor at all stops the shred.
//
// descriptor.VolumeOfKey is what decides which keys under `volumes/` are volumes, and
// it answers "yes, volume \"\"" for `volumes//descriptor.json` — a key naming no volume
// at all, since every volume id is a v7 uuid. The delete's descendant scan then has to
// read it, cannot, and refuses rather than skipping, which is the right rule applied to
// an object that should never have reached it.
func TestAdversaryAStrayObjectUnderTheVolumesPrefixBlocksTheShred(t *testing.T) {
	f := newFleet(t)
	victim := f.provision(t)
	f.detach(t, victim)
	if _, err := f.store.Put(t.Context(), "volumes//descriptor.json", []byte("{}"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := controlplane.DeleteVolume(t.Context(), f.md, f.store, f.kms, f.term, victim); err != nil {
		t.Fatalf("one unparseable object under %s stopped the crypto-shred of an unrelated volume: %v",
			descriptor.Prefix, err)
	}
}
