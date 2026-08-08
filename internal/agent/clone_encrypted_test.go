package agent_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// An encrypted clone is the one shape of clone nothing covered. Every clone test in the
// tree — the two DST scenarios and the e2e lane — runs unencrypted, so a clone had never
// once opened a chunk its parent sealed. The seam is real: a clone inherits its parent's
// DEK (controlplane.Clone copies it deliberately, so the chain stays readable) and reads
// objects sealed under the *parent's* id, and getting either half wrong fails at the
// guest's first read, a long way from the code that got it wrong.
//
// Writing it corrected a false claim in the code. parentEncryption's comment said the
// chunk AAD binds the volume id so the key must be re-bound, and that handing the clone
// its own Encryption would fail every chunk. Planting exactly that changes nothing:
// image.chunkAAD takes the volume id as a parameter, so what opens a chunk is the DEK
// plus LoadSnapshot's argument. The plant that does fail is passing the clone's own id
// to LoadSnapshot — which is what this test is pointed at.
//
// The assertion is the guest's bytes, not a nil check on an Encryption: a wrongly bound
// key produces a value of exactly the right type.
func TestAnEncryptedCloneReadsItsParentsChunks(t *testing.T) {
	var kek [crypto.DEKSize]byte
	if _, err := rand.Read(kek[:]); err != nil {
		t.Fatal(err)
	}
	kms := crypto.NewDevKMS(kek, "kek-clone")
	dek, err := crypto.GenerateDEK(rand.Reader, 9)
	if err != nil {
		t.Fatal(err)
	}
	wrapped, err := kms.WrapDEK(rand.Reader, dek)
	if err != nil {
		t.Fatal(err)
	}

	store := sim.NewObjectStore()
	newManager := func(dataDir string) *agent.VolumeManager {
		t.Helper()
		f := newListenerFactory()
		m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
			DataDir: dataDir, SocketDir: "/run/spin", Budget: testBudget(),
		}, agent.VolumeManagerDeps{
			Clock:   sim.NewClock(time.Unix(1_700_000_000, 0).UTC()),
			Disk:    sim.NewDisk(),
			Listen:  f.listen,
			Mapper:  unusedMapper{},
			EventFD: unusedEventFD,
			Store:   store,
			KMS:     kms,
			Rand:    rand.Reader,
			// Both volumes share the DEK, which is what controlplane.Clone does and what
			// makes the chain readable at all.
			Keys: func(_ context.Context, volumeID string) (agent.VolumeKeys, error) {
				return agent.VolumeKeys{
					VolumeID: volumeID, DEKWrapped: wrapped, KEKID: "kek-clone", DEKKeyID: dek.KeyID,
				}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return m
	}

	// The parent: write, then snapshot. The snapshot's chunks are sealed under the
	// parent's id, which is the whole difficulty.
	parent := newManager("/var/lib/spin-parent")
	defer func() { _ = parent.Close(t.Context()) }()
	p := desiredVolume(t, 1)
	// The descriptor a provisioner would have written: the clone's Agent reads it to find
	// whether the parent descends from anything itself (agent.parentChain).
	writeDescriptor(t, store, p.GetVolumeId(), lineageLink{})
	if err := parent.Apply(t.Context(), []*storagev1.DesiredVolume{p}); err != nil {
		t.Fatalf("Apply(parent): %v", err)
	}
	pattern := bytes.Repeat([]byte{0x6B}, testBlockSize)
	dev, ok := parent.Device(p.GetVolumeId())
	if !ok {
		t.Fatal("the parent is not being served")
	}
	if _, err := dev.ReadAt(make([]byte, 512), 0); err != nil {
		t.Fatalf("waiting for the parent's read view: %v", err)
	}
	if _, err := dev.WriteAt(pattern, 0); err != nil {
		t.Fatalf("the parent's write: %v", err)
	}
	snapID := ids.New().String()
	if _, err := parent.Snapshot(t.Context(), p.GetVolumeId(), snapID); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Nothing the clone reads may be cleartext (§5.10/INV-15) — asserted here because a
	// clone that could read its parent's chunks *because they were never sealed* would
	// satisfy every other assertion in this test.
	assertNoPlaintext(t, store, pattern)

	// The clone: its own id, its own data directory, its own empty WAL, and a desired
	// state naming what it descends from.
	clone := newManager("/var/lib/spin-clone")
	defer func() { _ = clone.Close(t.Context()) }()
	c := desiredVolume(t, 1)
	c.ParentSnapshotId, c.ParentVolumeId = snapID, p.GetVolumeId()
	if err := clone.Apply(t.Context(), []*storagev1.DesiredVolume{c}); err != nil {
		t.Fatalf("Apply(clone): %v", err)
	}
	cdev, ok := clone.Device(c.GetVolumeId())
	if !ok {
		t.Fatal("the clone is not being served")
	}
	got := make([]byte, len(pattern))
	if _, err := cdev.ReadAt(got, 0); err != nil {
		t.Fatalf("the clone's read: %v", err)
	}
	if !bytes.Equal(got, pattern) {
		t.Fatalf("the clone read %x, its parent wrote %x", got[:8], pattern[:8])
	}
}

// assertNoPlaintext fails if the guest's bytes appear anywhere in the store.
func assertNoPlaintext(t *testing.T, store objectstore.Store, pattern []byte) {
	t.Helper()
	// Both prefixes: the manifests under image/, and the chunks — which carry the guest's
	// bytes and which live under chunks/<lineage>/ since the chunk store moved. A check
	// that listed image/ alone would now walk manifests only and pass over every object
	// that has ever held plaintext.
	objs, err := store.List(t.Context(), "image/")
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := store.List(t.Context(), "chunks/")
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) == 0 {
		t.Fatal("no chunk objects were published, so the plaintext check would pass vacuously")
	}
	objs = append(objs, chunks...)
	for _, o := range objs {
		body, err := store.Get(t.Context(), o.Key)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(body, pattern) {
			t.Fatalf("%s carries the guest's plaintext (§5.10/INV-15)", o.Key)
		}
	}
}
