package controlplane_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/placement"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// bucketWithAVolumeAndASnapshot builds the S3 side of a small fleet the way production
// writes it — a provisioned volume, a snapshot of it, and a clone of that snapshot — and
// returns the two volume ids.
//
// It writes through the real producers (`Provisioner`, `Clone`) rather than hand-rolling
// objects, because a rebuild that only works against fixtures somebody wrote for it is
// exactly the shape of test this project keeps finding.
//
// The snapshot is a catalog row and no object. It used to be both: `image.PublishSnapshot`
// wrote a manifest under the volume's prefix and the rebuild read the snapshot back out of
// it. That object layout went with the chunked image, so the row is all there is, and the
// rebuild no longer sees a snapshot at all — which is what the assertions below say.
func bucketWithAVolumeAndASnapshot(t *testing.T, md metadata.Store, store objectstore.Store, term int64) (vol, clone, snap string) {
	t.Helper()
	ctx := t.Context()
	addHost(t, md, term, cloneHostA, lifecycle.HostActive, 1<<40)

	kms := testKMS(t)
	p := controlplane.NewProvisioner(md, store, kms, &ramp{})
	v, err := p.Provision(ctx, term, controlplane.VolumeSpec{
		SizeBytes: 1 << 30, BlockSize: 4096, HostID: cloneHostA,
	})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}

	snap = ids.New().String()
	if err := md.CreateSnapshot(ctx, term, metadata.Snapshot{
		SnapshotID: snap, VolumeID: v.VolumeID, Epoch: 1,
		State: lifecycle.SnapshotPublished, RequestID: ids.New().String(),
	}); err != nil {
		t.Fatal(err)
	}

	c, err := controlplane.Clone(ctx, md, store, kms, &ramp{}, placement.Policy{}, nil, term, snap, ids.New().String())
	if err != nil {
		t.Fatalf("Clone: %v", err)
	}
	return v.VolumeID, c.VolumeID, snap
}

// INV-20: the catalog can be rebuilt from the bucket. This is why descriptors are
// written at all — without it a lost PostgreSQL is unrecoverable even though every byte
// of every volume is intact.
//
// The rebuild runs against an *empty* store of its own, so nothing it asserts can be
// answered by a row that was already there. That is the whole test: the second catalog
// is built from objects only.
func TestRebuildMetadataFromTheBucket(t *testing.T) {
	ctx := t.Context()
	md, store, term := cpStore(t)
	vol, clone, snap := bucketWithAVolumeAndASnapshot(t, md, store, term)
	before, err := md.GetVolume(ctx, vol)
	if err != nil {
		t.Fatal(err)
	}

	fresh, _, freshTerm := cpStore(t) // a database that has never seen this fleet
	sum, err := controlplane.RebuildMetadata(ctx, fresh, store, testKMS(t), freshTerm)
	if err != nil {
		t.Fatalf("RebuildMetadata: %v", err)
	}
	if sum.Volumes != 2 {
		t.Fatalf("rebuilt %+v, want 2 volumes", sum)
	}

	got, err := fresh.GetVolume(ctx, vol)
	if err != nil {
		t.Fatalf("the volume was not rebuilt: %v", err)
	}
	// Everything a volume needs to be opened again. The DEK and its version travel
	// together: a wrapped key paired with another key's version is a volume nothing can
	// open, and the failure would surface on the guest's first write.
	if got.SizeBytes != before.SizeBytes || got.BlockSize != before.BlockSize ||
		got.KEKID != before.KEKID ||
		got.DEKKeyID != before.DEKKeyID || string(got.DEKWrapped) != string(before.DEKWrapped) {
		t.Fatalf("rebuilt volume = %+v, original = %+v", got, before)
	}
	// Placement is not in any object, so it does not come back. An operator re-places;
	// a rebuild that invented a host would claim someone is writing when nobody is.
	if got.PrimaryHostID != "" {
		t.Errorf("the rebuild invented a primary host: %q", got.PrimaryHostID)
	}
	// Nothing above zero can be proved from any object now: the manifest that stated a
	// volume's published sequence is withdrawn. Zero is the safe direction for both of
	// the Agent's floors — `published > 0` arms on any positive number and the durability
	// floor compares with `<` — so a rebuild that under-claims never refuses a volume
	// that is fine, while one that guessed high would refuse one that is.
	if got.PublishedSequence != 0 || got.DurableSequence != 0 || got.LocalSequence != 0 {
		t.Errorf("a rebuild claimed sequences it has no object for: %d/%d/%d",
			got.LocalSequence, got.DurableSequence, got.PublishedSequence)
	}

	// The snapshot does not come back, and its absence is the assertion. Its existence
	// was read out of a manifest under the volume's prefix; with that object gone there
	// is nothing in the bucket that states a snapshot exists, and inventing the row would
	// point a clone at a parent nothing can serve.
	if _, err := fresh.GetSnapshot(ctx, snap); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("the rebuild recorded a snapshot it cannot have read: %v", err)
	}

	gotClone, err := fresh.GetVolume(ctx, clone)
	if err != nil {
		t.Fatalf("the clone was not rebuilt: %v", err)
	}
	// And the clone comes back with no parent link, for the same reason and one step on:
	// `volumes.parent_snapshot_id` references a snapshots row, so writing the link the
	// descriptor still carries would fail the foreign key and abort the whole rebuild.
	// The clone's data is untouched — the link is a catalog fact, and the descriptor is
	// still where whoever re-establishes it reads the parent id from.
	if gotClone.ParentSnapshotID != "" {
		t.Fatalf("the rebuilt clone claims a parent snapshot no row exists for: %+v", gotClone)
	}
	if gotClone.ChainDepth != 1 {
		t.Errorf("chain depth = %d, want 1", gotClone.ChainDepth)
	}
}

// Two operators run it at once, or one runs it twice after a partial failure. Both must
// finish: a rebuild that aborts half-way has written an arbitrary prefix of the catalog
// and left the operator with no way to know which.
func TestRebuildMetadataIsIdempotent(t *testing.T) {
	ctx := t.Context()
	md, store, term := cpStore(t)
	vol, _, _ := bucketWithAVolumeAndASnapshot(t, md, store, term)

	fresh, _, freshTerm := cpStore(t)
	first, err := controlplane.RebuildMetadata(ctx, fresh, store, testKMS(t), freshTerm)
	if err != nil {
		t.Fatal(err)
	}
	second, err := controlplane.RebuildMetadata(ctx, fresh, store, testKMS(t), freshTerm)
	if err != nil {
		t.Fatalf("the second rebuild failed: %v", err)
	}
	if first != second {
		t.Fatalf("a repeated rebuild found %+v then %+v", first, second)
	}
	// And it did not regress the row it re-recorded.
	if got, err := fresh.GetVolume(ctx, vol); err != nil || got.VolumeID != vol {
		t.Fatalf("volume after the second rebuild: %+v (err %v)", got, err)
	}
}

// A descriptor under one volume's prefix that names another is what a bucket copied with
// the wrong prefix looks like. Acting on it would record a volume under an id whose data
// lives somewhere else, so the rebuild stops rather than recording part of the truth.
func TestRebuildMetadataRefusesAMisplacedDescriptor(t *testing.T) {
	ctx := t.Context()
	md, store, term := cpStore(t)
	_, _, _ = bucketWithAVolumeAndASnapshot(t, md, store, term)

	stranger := ids.New().String()
	body, err := store.Get(ctx, descriptor.Key(mustOneDescriptorKey(t, store)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, "volumes/"+stranger+"/descriptor.json", body, objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	fresh, _, freshTerm := cpStore(t)
	if _, err := controlplane.RebuildMetadata(ctx, fresh, store, testKMS(t), freshTerm); err == nil {
		t.Fatal("a descriptor under the wrong prefix was accepted")
	}
	if _, err := fresh.GetVolume(ctx, stranger); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("the stranger's id was recorded anyway: %v", err)
	}
}

// mustOneDescriptorKey returns the volume id of the first descriptor in the bucket.
func mustOneDescriptorKey(t *testing.T, store objectstore.Store) string {
	t.Helper()
	objs, err := store.List(t.Context(), "volumes/")
	if err != nil || len(objs) == 0 {
		t.Fatalf("no descriptors in the bucket (err %v)", err)
	}
	id := objs[0].Key[len("volumes/") : len(objs[0].Key)-len("/descriptor.json")]
	return id
}

// "You passed the wrong -kek-file" and "somebody forged this descriptor" are the same
// failed unwrap and must not be the same message.
//
// A rebuild is run when the catalog is already gone, which is the worst moment to tell
// an operator their bucket was tampered with because they typed the wrong path. The KEK
// id is compared first for exactly that, the way publisher.encryption already does it.
func TestRebuildSeparatesAWrongKEKFromAForgedDescriptor(t *testing.T) {
	ctx := t.Context()
	md, store, term := cpStore(t)
	vol, _, _ := bucketWithAVolumeAndASnapshot(t, md, store, term)

	var other [crypto.DEKSize]byte
	for i := range other {
		other[i] = byte(255 - i)
	}
	fresh, _, freshTerm := cpStore(t)
	_, err := controlplane.RebuildMetadata(ctx, fresh, store, crypto.NewDevKMS(other, "kek-other"), freshTerm)
	if err == nil {
		t.Fatal("a rebuild under a KEK that wrapped nothing in this bucket succeeded")
	}
	if errors.Is(err, crypto.ErrUnwrap) {
		t.Fatalf("the wrong -kek-file was reported as a key that would not open, which reads as a forgery: %v", err)
	}
	if !strings.Contains(err.Error(), "-kek-file") {
		t.Fatalf("the refusal does not name the flag an operator has to fix: %v", err)
	}
	if _, err := fresh.GetVolume(ctx, vol); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("the rebuild recorded a volume before refusing: %v", err)
	}
}

// The forgery half: a descriptor naming the right KEK whose wrapped DEK was sealed for
// another volume. Nothing structural can see it — the object's digest is its own, and it
// names the volume it is filed under — so this is the assertion that the KEK is being
// used as the witness.
func TestRebuildRefusesAWrappedDEKSealedForAnotherVolume(t *testing.T) {
	ctx := t.Context()
	md, store, term := cpStore(t)
	kms := testKMS(t)
	victim, other, _ := bucketWithAVolumeAndASnapshot(t, md, store, term)

	d, err := descriptor.Read(ctx, store, victim)
	if err != nil {
		t.Fatal(err)
	}
	od, err := descriptor.Read(ctx, store, other)
	if err != nil {
		t.Fatal(err)
	}
	d.DEKWrapped, d.DEKKeyID = od.DEKWrapped, od.DEKKeyID
	if err := descriptor.Write(ctx, store, d); err != nil {
		t.Fatal(err)
	}

	fresh, _, freshTerm := cpStore(t)
	if _, err := controlplane.RebuildMetadata(ctx, fresh, store, kms, freshTerm); !errors.Is(err, crypto.ErrUnwrap) {
		t.Fatalf("a rebuild recorded a wrapped DEK sealed for another volume: %v", err)
	}
	if _, err := fresh.GetVolume(ctx, victim); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("the poisoned volume was recorded anyway: %v", err)
	}
}
