package main

import (
	"bytes"
	"crypto/rand"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/placement"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/real"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// TestADeletedVolumeCannotBeStartedClonedOrRebuilt drives -delete-volume the way an
// operator does and asserts on what the outside can see afterwards: the volume's image
// does not load, `controlplane.Clone` cannot make a VM from its snapshot, and
// `-rebuild-metadata` — the one path that resurrects a volume from the bucket — does not
// bring it back.
//
// Then the other half of the owner's sentence, which is the part no code here provides:
// the bucket still holds every one of those versions, so restoring them and running the
// rebuild again produces the volume, its snapshot and a loadable image. That is what "a
// couple of days to allow recovery" is, and it is a bucket policy — the assertion here
// is only that this repository left the door it depends on unlocked.
func TestADeletedVolumeCannotBeStartedClonedOrRebuilt(t *testing.T) {
	ctx := t.Context()
	w := newDeleteWorld(t)

	// Placed: the delete is refused, and it costs nothing. This is the first thing an
	// operator does wrong, and the failure it prevents is taking the objects out from
	// under a guest that is still writing.
	before := listBucket(t, w.store)
	if err := deleteVolume(ctx, w.md, w.store, w.kekFile, w.volumeID, w.term); err == nil {
		t.Fatal("a placed volume was deleted")
	}
	if after := listBucket(t, w.store); !bytes.Equal(after, before) {
		t.Fatalf("a refused delete changed the bucket:\nbefore:\n%s\nafter:\n%s", before, after)
	}

	if err := w.md.SetVolumePrimaryHost(ctx, w.term, w.volumeID, ""); err != nil {
		t.Fatalf("detaching: %v", err)
	}
	keys := listKeys(t, w.store)
	if err := deleteVolume(ctx, w.md, w.store, w.kekFile, w.volumeID, w.term); err != nil {
		t.Fatalf("-delete-volume: %v", err)
	}

	// No VM can be started from it: the image a host would attach is not there.
	if _, _, _, err := image.Load(ctx, w.store, w.enc, image.OwnLineage(w.volumeU), nil); !errors.Is(err, image.ErrNotPublished) {
		t.Errorf("the deleted volume's image still loads: %v", err)
	}
	// And none can be created from it: Clone starts at the snapshot row, which went
	// with the volume.
	_, err := controlplane.Clone(ctx, w.md, w.store, w.policy, nil, w.term, w.snapshotID, ids.New().String())
	if !errors.Is(err, metadata.ErrNotFound) {
		t.Errorf("cloning the deleted volume's snapshot: %v, want ErrNotFound", err)
	}

	// The rebuild is the one path that brings a volume back from the bucket, so a
	// delete that did not stop it would not be a delete. It reads volumes/ for
	// descriptors, which is why the descriptor is the first object marked.
	sum, err := controlplane.RebuildMetadata(ctx, w.md, w.store, w.term)
	if err != nil {
		t.Fatalf("rebuilding after the delete: %v", err)
	}
	if sum.Volumes != 0 || sum.Snapshots != 0 {
		t.Errorf("a rebuild after the delete recorded %d volumes and %d snapshots", sum.Volumes, sum.Snapshots)
	}
	if _, err := w.md.GetVolume(ctx, w.volumeID); !errors.Is(err, metadata.ErrNotFound) {
		t.Errorf("the deleted volume is back in the catalog: %v", err)
	}

	// The window. Every key the volume owned is still a restorable version, in the
	// inverse of the delete order, so that the volume becomes visible to the rebuild
	// only once its bytes are back.
	for i := len(keys) - 1; i >= 0; i-- {
		if err := w.store.Restore(ctx, keys[i]); err != nil {
			t.Fatalf("restoring %s: %v", keys[i], err)
		}
	}
	sum, err = controlplane.RebuildMetadata(ctx, w.md, w.store, w.term)
	if err != nil {
		t.Fatalf("rebuilding after the restore: %v", err)
	}
	if sum.Volumes != 1 || sum.Snapshots != 1 {
		t.Fatalf("the rebuild recovered %d volumes and %d snapshots, want 1 and 1", sum.Volumes, sum.Snapshots)
	}
	view, _, _, err := image.Load(ctx, w.store, w.enc, image.OwnLineage(w.volumeU), nil)
	if err != nil {
		t.Fatalf("loading the recovered volume's image: %v", err)
	}
	got := make([]byte, 4096)
	view.Read(0, got)
	if !bytes.Equal(got, bytes.Repeat([]byte{0xA1}, 4096)) {
		t.Fatalf("the recovered volume reads %#x..., want 0xa1 repeated", got[:8])
	}
}

// TestDeletingAVolumeFlattensWhatDescendsFromIt is answer B through the command: the
// clone is made self-contained first, and the assertion is that it still reads its
// parent's bytes after its parent's objects are gone.
//
// The refusal before the flatten is asserted too, because "it flattened them" and "it
// ignored them" are the same exit code otherwise.
func TestDeletingAVolumeFlattensWhatDescendsFromIt(t *testing.T) {
	ctx := t.Context()
	w := newDeleteWorld(t)
	clone := w.cloneOfTheSnapshot(t)

	// The parent detached; the clone still placed. The flatten cannot run against a
	// volume a host is serving, so the delete stops with the clone's own precondition
	// rather than pretending it can flatten it.
	if err := w.md.SetVolumePrimaryHost(ctx, w.term, w.volumeID, ""); err != nil {
		t.Fatal(err)
	}
	if err := deleteVolume(ctx, w.md, w.store, w.kekFile, w.volumeID, w.term); err == nil {
		t.Fatal("the parent was deleted while its clone was still being served")
	}
	if _, err := descriptor.Read(ctx, w.store, w.volumeID); err != nil {
		t.Fatalf("the refused delete already marked the parent's descriptor: %v", err)
	}

	if err := w.md.SetVolumePrimaryHost(ctx, w.term, clone.id, ""); err != nil {
		t.Fatal(err)
	}
	if err := deleteVolume(ctx, w.md, w.store, w.kekFile, w.volumeID, w.term); err != nil {
		t.Fatalf("-delete-volume with a detached clone under it: %v", err)
	}

	// The clone reads its own image, alone, and it holds the byte only its parent ever
	// wrote. Without the flatten this is a chunk under a lineage root that no longer
	// exists.
	view, _, _, err := image.Load(ctx, w.store, clone.enc, image.OwnLineage(clone.u), nil)
	if err != nil {
		t.Fatalf("loading the flattened clone after its parent was deleted: %v", err)
	}
	got := make([]byte, 4096)
	view.Read(0, got)
	if !bytes.Equal(got, bytes.Repeat([]byte{0xA1}, 4096)) {
		t.Fatalf("the clone reads %#x... at a range only its parent wrote, want 0xa1 repeated", got[:8])
	}
	// And the catalog agrees the lineage ended, which is what stopped the delete's own
	// snapshot rows from being un-removable.
	v, err := w.md.GetVolume(ctx, clone.id)
	if err != nil {
		t.Fatal(err)
	}
	if v.ParentSnapshotID != "" || v.ChainDepth != 0 {
		t.Errorf("the flattened clone's row still says parent=%q depth=%d", v.ParentSnapshotID, v.ChainDepth)
	}
}

// --- fixture ----------------------------------------------------------------

// deleteWorld is one provisioned, published, snapshotted volume on one host, with the
// key file the command reads. It is built through the production paths — Provisioner,
// image.Publish, image.PublishSnapshot — so that what the delete removes is what a real
// deployment would have written.
type deleteWorld struct {
	md         *metasim.Store
	store      *sim.ObjectStore
	term       int64
	hostID     string
	volumeID   string
	volumeU    [16]byte
	snapshotID string
	enc        *wal.Encryption
	kekFile    string
	policy     placement.Policy
}

type clonedVolume struct {
	id  string
	u   [16]byte
	enc *wal.Encryption
}

func newDeleteWorld(t *testing.T) *deleteWorld {
	t.Helper()
	ctx := t.Context()
	now := time.Unix(1_700_000_000, 0).UTC()
	w := &deleteWorld{
		md:     metasim.New(func() time.Time { return now }),
		store:  sim.NewObjectStore(),
		hostID: ids.New().String(),
		policy: placement.Policy{MaxOversubscription: 4, MaxUsedRatio: 0.9},
	}

	term, err := w.md.AcquireLeadership(ctx, "cp-a")
	if err != nil {
		t.Fatal(err)
	}
	w.term = term
	if err := w.md.UpsertHost(ctx, term, metadata.Host{
		HostID: w.hostID, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
	}); err != nil {
		t.Fatal(err)
	}

	// The KEK on disk, because that is how both binaries are given one and the flatten
	// this delete may run reads it through crypto.LoadKEK.
	//
	// It is written through simio/disk rather than os, which INV-01 forbids everywhere
	// outside internal/simio — including here, where the temptation is that a test is
	// not production. The rule has no test exemption for ordinary unit tests and this is
	// one.
	kek := bytes.Repeat([]byte{7}, crypto.DEKSize)
	dir := t.TempDir()
	w.kekFile = filepath.Join(dir, "kek")
	d, err := real.NewDisk(dir)
	if err != nil {
		t.Fatal(err)
	}
	f, err := d.Create("kek")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Append(kek); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	kms := crypto.NewDevKMS([crypto.DEKSize]byte(kek), crypto.KEKID([crypto.DEKSize]byte(kek)))

	prov, err := controlplane.NewProvisioner(w.md, w.store, kms, rand.Reader).Provision(ctx, term,
		controlplane.VolumeSpec{SizeBytes: 1 << 20, BlockSize: 4096, HostID: w.hostID})
	if err != nil {
		t.Fatalf("provisioning: %v", err)
	}
	w.volumeID = prov.VolumeID
	u, err := ids.Parse(w.volumeID)
	if err != nil {
		t.Fatal(err)
	}
	w.volumeU = [16]byte(u)
	w.enc = w.encryptionFor(t, w.volumeU)

	// One session's worth of bytes, published the way an Agent's teardown publishes
	// them, and a snapshot of the same view.
	view := cow.NewIntervalMap()
	view.Overwrite(0, bytes.Repeat([]byte{0xA1}, 4096))
	if _, err := image.Publish(ctx, w.store, rand.Reader, w.enc, image.OwnLineage(w.volumeU), view, nil, 5, ""); err != nil {
		t.Fatalf("publishing the volume's image: %v", err)
	}
	w.snapshotID = ids.New().String()
	key, err := image.PublishSnapshot(ctx, w.store, rand.Reader, w.enc,
		image.OwnLineage(w.volumeU), view, nil, 5, w.snapshotID)
	if err != nil {
		t.Fatalf("publishing the snapshot: %v", err)
	}
	if err := w.md.CreateSnapshot(ctx, term, metadata.Snapshot{
		SnapshotID: w.snapshotID, VolumeID: w.volumeID, Epoch: 1, TargetSequence: 5,
		RootDigest: "d", State: lifecycle.SnapshotCreating, RequestID: ids.New().String(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.md.PublishSnapshot(ctx, term, w.snapshotID, 5, w.hostID, key); err != nil {
		t.Fatal(err)
	}
	return w
}

// cloneOfTheSnapshot creates a clone through controlplane.Clone, which is what writes
// the descriptor whose parent link the delete's descendant scan reads.
func (w *deleteWorld) cloneOfTheSnapshot(t *testing.T) clonedVolume {
	t.Helper()
	vol, err := controlplane.Clone(t.Context(), w.md, w.store, w.policy, nil, w.term, w.snapshotID, ids.New().String())
	if err != nil {
		t.Fatalf("cloning: %v", err)
	}
	u, err := ids.Parse(vol.VolumeID)
	if err != nil {
		t.Fatal(err)
	}
	return clonedVolume{id: vol.VolumeID, u: [16]byte(u), enc: w.encryptionFor(t, [16]byte(u))}
}

func (w *deleteWorld) encryptionFor(t *testing.T, u [16]byte) *wal.Encryption {
	t.Helper()
	v, err := w.md.GetVolume(t.Context(), w.volumeID)
	if err != nil {
		t.Fatal(err)
	}
	kek := bytes.Repeat([]byte{7}, crypto.DEKSize)
	kms := crypto.NewDevKMS([crypto.DEKSize]byte(kek), crypto.KEKID([crypto.DEKSize]byte(kek)))
	dek, err := kms.UnwrapDEK(v.DEKWrapped, v.DEKKeyID)
	if err != nil {
		t.Fatal(err)
	}
	enc, err := wal.NewEncryption(dek, u)
	if err != nil {
		t.Fatal(err)
	}
	return enc
}

func listKeys(t *testing.T, store objectstore.Store) []string {
	t.Helper()
	objs, err := store.List(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(objs))
	for _, o := range objs {
		keys = append(keys, o.Key)
	}
	return keys
}

func listBucket(t *testing.T, store objectstore.Store) []byte {
	t.Helper()
	var b bytes.Buffer
	for _, k := range listKeys(t, store) {
		b.WriteString(k)
		b.WriteByte('\n')
	}
	return b.Bytes()
}
