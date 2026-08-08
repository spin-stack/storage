package controlplane_test

import (
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/placement"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// bucketWithAVolumeAndASnapshot builds the S3 side of a small fleet the way production
// writes it — a provisioned volume, a published image, a published snapshot, and a clone
// of that snapshot — and returns the two volume ids.
//
// It writes through the real producers (`Provisioner`, `Clone`, `image.PublishSnapshot`)
// rather than hand-rolling objects, because a rebuild that only works against fixtures
// somebody wrote for it is exactly the shape of test this project keeps finding.
func bucketWithAVolumeAndASnapshot(t *testing.T, md metadata.Store, store objectstore.Store, term int64) (vol, clone, snap string) {
	t.Helper()
	ctx := t.Context()
	addHost(t, md, term, cloneHostA, lifecycle.HostActive, 1<<40)

	p := controlplane.NewProvisioner(md, store, testKMS(t), &ramp{})
	v, err := p.Provision(ctx, term, controlplane.VolumeSpec{
		SizeBytes: 1 << 30, BlockSize: 4096, HostID: cloneHostA,
	})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}

	u, err := ids.Parse(v.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	view := cow.NewIntervalMap()
	view.Overwrite(0, []byte("the guest's bytes"))
	snap = ids.New().String()
	if _, err := image.PublishSnapshot(ctx, store, &ramp{}, nil, image.OwnLineage([16]byte(u)), view, 9, snap); err != nil {
		t.Fatalf("PublishSnapshot: %v", err)
	}
	if err := md.CreateSnapshot(ctx, term, metadata.Snapshot{
		SnapshotID: snap, VolumeID: v.VolumeID, Epoch: 1, TargetSequence: 9,
		State: lifecycle.SnapshotPublished, RequestID: ids.New().String(),
	}); err != nil {
		t.Fatal(err)
	}

	c, err := controlplane.Clone(ctx, md, store, placement.Policy{}, term, snap, ids.New().String())
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
	sum, err := controlplane.RebuildMetadata(ctx, fresh, store, freshTerm)
	if err != nil {
		t.Fatalf("RebuildMetadata: %v", err)
	}
	if sum.Volumes != 2 || sum.Snapshots != 1 {
		t.Fatalf("rebuilt %+v, want 2 volumes and 1 snapshot", sum)
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

	gotSnap, err := fresh.GetSnapshot(ctx, snap)
	if err != nil {
		t.Fatalf("the snapshot was not rebuilt: %v", err)
	}
	// A manifest exists, is create-only and never changes (INV-16), so its existence is
	// PUBLISHED — and its sequence is the point it froze, which is what a clone descends
	// from.
	if gotSnap.State != lifecycle.SnapshotPublished || gotSnap.TargetSequence != 9 || gotSnap.VolumeID != vol {
		t.Fatalf("rebuilt snapshot = %+v", gotSnap)
	}

	// The chain, which is the part a two-pass rebuild would get wrong: the clone's row
	// references a snapshot row that references the clone's parent volume.
	gotClone, err := fresh.GetVolume(ctx, clone)
	if err != nil {
		t.Fatalf("the clone was not rebuilt: %v", err)
	}
	if gotClone.ParentSnapshotID != snap {
		t.Fatalf("the rebuilt clone lost its parent link: %+v", gotClone)
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
	first, err := controlplane.RebuildMetadata(ctx, fresh, store, freshTerm)
	if err != nil {
		t.Fatal(err)
	}
	second, err := controlplane.RebuildMetadata(ctx, fresh, store, freshTerm)
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
	if _, err := controlplane.RebuildMetadata(ctx, fresh, store, freshTerm); err == nil {
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
