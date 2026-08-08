package agent_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// TestACloneReadsThroughThreeAncestors is the walk, asserted where a walk that stops
// early is a **wrong byte at a known offset** rather than an error.
//
// Each of the three ancestors wrote one range that nothing above it ever touched, so the
// three offsets say which link failed: stop after one and the great-grandparent's and the
// grandparent's ranges come back as zeros, stop after two and only the
// great-grandparent's does. A fourth offset — written by the oldest ancestor and
// overwritten by the nearest — says the layers went on in the right order, which the
// other three cannot: a walk that composed the chain upside down passes them all.
//
// # Why the ancestors' snapshots are published from unlayered views
//
// They are published as **deltas** — each manifest naming only the ranges that volume
// itself wrote — because that is the state this walk exists for and the state a real
// lineage does not hold yet. Publishing still flattens (`image.uploadChunks` walks
// `view.Ranges()`, which merges the base), so a lineage built by driving three Agents
// would leave every manifest self-contained, the nearest ancestor alone would answer
// every read, and a plant that stopped the walk after one link would not change a byte.
// The test would run and prove nothing.
//
// So the ancestors' snapshots are written by the real publisher (`image.PublishSnapshot`,
// the same call `VolumeManager.Snapshot` makes) from views holding only that volume's own
// writes. That is exactly what step 3 of CHUNK-ADDRESSING-SPEC makes the publisher
// produce, which is why it is worth asserting against now: this walk is what has to be
// true *before* that step lands.
//
// The lineage is encrypted for a second reason, and it is the question step 2 inherits:
// a chunk opens under the DEK the whole lineage shares plus the volume id passed to the
// load, and there are three different volume ids here. Unencrypted, every one of them
// could be wrong and every assertion would still pass.
func TestACloneReadsThroughThreeAncestors(t *testing.T) {
	const (
		oldestOnly = int64(0)                  // written by the great-grandparent, never touched again
		middleOnly = int64(4 * testBlockSize)  // written by the grandparent
		nearestOly = int64(8 * testBlockSize)  // written by the parent
		shared     = int64(12 * testBlockSize) // written by the great-grandparent, overwritten by the parent
	)
	store := sim.NewObjectStore()
	kms, dek, wrapped := lineageKeys(t)

	// Three ancestors, oldest first, each contributing one range of its own. The link is
	// what the walk follows: the root has none, and each of the others names the snapshot
	// above it — in its descriptor, which is the only place the bucket states a lineage.
	oldest := publishAncestor(t, store, dek, ancestorSpec{
		writes: map[int64]byte{oldestOnly: 0xA1, shared: 0xA1},
	})
	middle := publishAncestor(t, store, dek, ancestorSpec{
		parent: oldest,
		writes: map[int64]byte{middleOnly: 0xB2},
	})
	nearest := publishAncestor(t, store, dek, ancestorSpec{
		parent: middle,
		writes: map[int64]byte{nearestOly: 0xC3, shared: 0xC3},
	})

	m := lineageManager(t, store, kms, wrapped, dek.KeyID)
	v := desiredVolume(t, 1)
	v.ParentSnapshotId, v.ParentVolumeId = nearest.snapshot, nearest.volume
	dev := serveClone(t, m, v)

	readBlock(t, dev, oldestOnly, 0xA1, "the range only the great-grandparent wrote: three links up")
	readBlock(t, dev, middleOnly, 0xB2, "the range only the grandparent wrote: two links up")
	readBlock(t, dev, nearestOly, 0xC3, "the range only the parent wrote: one link up")
	readBlock(t, dev, shared, 0xC3, "a range the oldest ancestor wrote and the nearest overwrote: the nearest wins")
}

// TestALineageThatCannotBeWalkedIsRefused is the other half of the walk, and the half
// that decides whether a defect is loud or silent. Every case here would otherwise end
// the same way: a base holding however much of the lineage the walk managed to read, and
// a guest reading zeros for the rest with nothing to say why (DEV-0007).
//
// The assertion is on the guest's read, not on a returned error, because the two are not
// the same statement: fetchBase logs and calls FailBase, and what an operator's guest
// actually experiences is a read that is refused. A read that *succeeds* is the failure
// this test is about, whatever it returns — so zeros are checked for explicitly rather
// than assumed to be impossible.
func TestALineageThatCannotBeWalkedIsRefused(t *testing.T) {
	tests := []struct {
		name string
		// build returns the link the volume under test descends from, having already
		// broken the lineage above it in whatever way the case is about.
		build func(t *testing.T, store objectstore.Store, dek crypto.DEK) lineageLink
	}{
		{
			name: "a descriptor in the middle of the lineage is gone",
			build: func(t *testing.T, store objectstore.Store, dek crypto.DEK) lineageLink {
				oldest := publishAncestor(t, store, dek, ancestorSpec{writes: map[int64]byte{0: 0xA1}})
				middle := publishAncestor(t, store, dek, ancestorSpec{parent: oldest, writes: map[int64]byte{testBlockSize: 0xB2}})
				// The hole: without this object the walk cannot tell the top of a chain
				// from a link it lost, and "the top" would serve the oldest ancestor's
				// range as zeros.
				if err := store.Delete(t.Context(), descriptor.Key(middle.volume)); err != nil {
					t.Fatalf("removing the middle descriptor: %v", err)
				}
				return middle
			},
		},
		{
			name: "the lineage links back on itself",
			build: func(t *testing.T, store objectstore.Store, dek crypto.DEK) lineageLink {
				oldest := publishAncestor(t, store, dek, ancestorSpec{writes: map[int64]byte{0: 0xA1}})
				nearest := publishAncestor(t, store, dek, ancestorSpec{parent: oldest, writes: map[int64]byte{testBlockSize: 0xB2}})
				// The cycle: the root now claims to descend from its own descendant.
				// Nothing in this repository writes that, which is the point — a walk
				// with no cycle check is a hang, and a hang at attach is indistinguishable
				// from a slow object store.
				writeDescriptor(t, store, oldest.volume, nearest)
				return nearest
			},
		},
		{
			name: "the snapshot an ancestor names was never published",
			build: func(t *testing.T, store objectstore.Store, dek crypto.DEK) lineageLink {
				oldest := publishAncestor(t, store, dek, ancestorSpec{writes: map[int64]byte{0: 0xA1}})
				nearest := publishAncestor(t, store, dek, ancestorSpec{parent: oldest, writes: map[int64]byte{testBlockSize: 0xB2}})
				if err := store.Delete(t.Context(), image.SnapshotKey(uuidOf(t, oldest.volume), oldest.snapshot)); err != nil {
					t.Fatalf("removing the oldest snapshot manifest: %v", err)
				}
				return nearest
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := sim.NewObjectStore()
			kms, dek, wrapped := lineageKeys(t)
			link := tc.build(t, store, dek)

			m := lineageManager(t, store, kms, wrapped, dek.KeyID)
			v := desiredVolume(t, 1)
			v.ParentSnapshotId, v.ParentVolumeId = link.snapshot, link.volume
			if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{v}); err != nil {
				t.Fatalf("Apply: %v", err)
			}
			dev, ok := m.Device(v.GetVolumeId())
			if !ok {
				t.Fatalf("no device for volume %s", v.GetVolumeId())
			}
			got := make([]byte, testBlockSize)
			if _, err := dev.ReadAt(got, 0); err == nil {
				t.Fatalf("the read was answered with %#x, want a refusal: a partial view is a wrong answer a guest cannot detect", got[:8])
			}
		})
	}
}

// lineageLink is one published ancestor: the volume, the snapshot below it descends
// from, and the root of the lineage they both belong to. It is what the next clone in a
// test's chain is pointed at.
//
// The root is carried because the chunks of every volume in a chain live under it
// (image.Ident), so a test that builds a lineage by hand has to thread it down the chain
// exactly as controlplane.Clone threads the DEK. A link whose root is its own volume is a
// root volume — what publishAncestor produces for an ancestorSpec with no parent.
type lineageLink struct {
	volume   string
	snapshot string
	root     [16]byte
}

type ancestorSpec struct {
	// parent is the link this ancestor itself descends from; the zero value is a root.
	parent lineageLink
	// writes is offset -> the byte repeated across one block.
	writes map[int64]byte
}

// publishAncestor writes one ancestor into the bucket exactly as it will exist once
// publishing stops flattening: a snapshot manifest naming only the ranges this volume
// wrote, and a descriptor carrying its own link upward.
//
// The manifest goes through image.PublishSnapshot — the same call the Agent's Snapshot
// makes — so the objects are sealed, keyed and framed by the production writer rather
// than by the test. What the test supplies is the *view*, and it supplies an unlayered
// one, which is the whole difference between a delta and today's flattened manifest.
func publishAncestor(t *testing.T, store objectstore.Store, dek crypto.DEK, spec ancestorSpec) lineageLink {
	t.Helper()
	volumeID := ids.New().String()
	u := uuidOf(t, volumeID)

	view := cow.NewIntervalMap()
	for off, b := range spec.writes {
		view.Overwrite(uint64(off), bytes.Repeat([]byte{b}, testBlockSize))
	}
	enc, err := wal.NewEncryption(dek, u)
	if err != nil {
		t.Fatalf("binding the DEK to %s: %v", volumeID, err)
	}
	snapshotID := ids.New().String()
	// The chunks go under the lineage's root, not this volume's id: that is where the
	// Agent's walk will look for them, and a root threaded wrongly here would leave the
	// bytes in a prefix nothing reads — which the assertions would report as zeros.
	root := u
	if spec.parent.volume != "" {
		root = spec.parent.root
	}
	if _, err := image.PublishSnapshot(t.Context(), store, rand.Reader, enc, image.Ident{Volume: u, Lineage: root}, view, 1, snapshotID); err != nil {
		t.Fatalf("publishing the snapshot of %s: %v", volumeID, err)
	}
	writeDescriptor(t, store, volumeID, spec.parent)
	return lineageLink{volume: volumeID, snapshot: snapshotID, root: root}
}

// writeDescriptor writes the object controlplane.Provision and controlplane.Clone write,
// and every clone test in this package needs it: the walk reads an ancestor's descriptor
// to find the link above it and fails closed when it is not there. Hand-written rather
// than driven through the Control Plane for the reason the DST rebuild scenario gives —
// the production provisioner allocates ids from the wall clock — and the fields that are
// not the link are the geometry a provisioner would have recorded.
func writeDescriptor(t *testing.T, store objectstore.Store, volumeID string, parent lineageLink) {
	t.Helper()
	if err := descriptor.Write(t.Context(), store, descriptor.Descriptor{
		VolumeID:         volumeID,
		SizeBytes:        testVolumeSize,
		BlockSize:        testBlockSize,
		CurrentEpoch:     1,
		KEKID:            "kek-lineage",
		DEKKeyID:         1,
		ParentSnapshotID: parent.snapshot,
		ParentVolumeID:   parent.volume,
	}); err != nil {
		t.Fatalf("writing the descriptor of %s: %v", volumeID, err)
	}
}

func uuidOf(t *testing.T, volumeID string) [16]byte {
	t.Helper()
	u, err := ids.Parse(volumeID)
	if err != nil {
		t.Fatalf("volume %q is not a uuid: %v", volumeID, err)
	}
	return [16]byte(u)
}

// lineageKeys is the one DEK a whole lineage shares, because controlplane.Clone hands a
// clone its parent's — which is what makes an ancestor's chunks openable at all.
func lineageKeys(t *testing.T) (crypto.KMS, crypto.DEK, []byte) {
	t.Helper()
	var kek [crypto.DEKSize]byte
	if _, err := rand.Read(kek[:]); err != nil {
		t.Fatal(err)
	}
	kms := crypto.NewDevKMS(kek, "kek-lineage")
	dek, err := crypto.GenerateDEK(rand.Reader, 7)
	if err != nil {
		t.Fatal(err)
	}
	wrapped, err := kms.WrapDEK(rand.Reader, dek)
	if err != nil {
		t.Fatal(err)
	}
	return kms, dek, wrapped
}

func lineageManager(t *testing.T, store objectstore.Store, kms crypto.KMS, wrapped []byte, keyID uint32) *agent.VolumeManager {
	t.Helper()
	f := newListenerFactory()
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin-lineage", SocketDir: "/run/spin", Budget: testBudget(),
	}, agent.VolumeManagerDeps{
		Clock:   sim.NewClock(time.Unix(1_700_000_000, 0).UTC()),
		Disk:    sim.NewDisk(),
		Listen:  f.listen,
		Mapper:  unusedMapper{},
		EventFD: unusedEventFD,
		Store:   store,
		KMS:     kms,
		Rand:    rand.Reader,
		Keys: func(_ context.Context, volumeID string) (agent.VolumeKeys, error) {
			return agent.VolumeKeys{VolumeID: volumeID, DEKWrapped: wrapped, KEKID: "kek-lineage", DEKKeyID: keyID}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewVolumeManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close(context.Background()) }) //nolint:usetesting // a cancelled context abandons the publish
	return m
}
