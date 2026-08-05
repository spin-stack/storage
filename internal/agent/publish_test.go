package agent_test

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// publishRig is a manager with an object store, which is the only configuration in which
// a volume has anywhere to publish to.
type publishRig struct {
	m     *agent.VolumeManager
	store *sim.ObjectStore
}

// newPublishRig takes the concrete *sim.ObjectStore rather than the objectstore.Store
// interface, and that is the whole fix for an assertion that could not fail. A rig exists
// to hand a test back the very store the manager wrote through, because every check in
// this file is about what is *in the bucket*. The earlier signature took the interface and
// kept the concrete store only if a type assertion happened to succeed (`sto, _ :=`), so a
// caller passing a double got a nil one — and the test below quietly listed a *freshly
// constructed* store instead: an empty bucket, and a pass whatever the code did.
// Narrowing the parameter makes that unrepresentable rather than merely corrected.
//
// A test that needs a double takes newPublishManager and asserts through the double.
func newPublishRig(t *testing.T, store *sim.ObjectStore) *publishRig {
	t.Helper()
	return &publishRig{m: newPublishManager(t, store), store: store}
}

// newPublishManager is the manager on its own, for a test whose evidence is not the
// bucket's contents — it wraps the store in something that counts or fails calls, and
// asserts on that. It returns no rig deliberately: there is no store such a test could
// honestly read back.
func newPublishManager(t *testing.T, store objectstore.Store) *agent.VolumeManager {
	t.Helper()
	f := newListenerFactory()
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin", SocketDir: "/run/spin",
		Budget: testBudget(),
	}, agent.VolumeManagerDeps{
		Clock:   sim.NewClock(time.Unix(1_700_000_000, 0).UTC()),
		Disk:    sim.NewDisk(),
		Listen:  f.listen,
		Mapper:  unusedMapper{},
		EventFD: unusedEventFD,
		Store:   store,
		Rand:    rand.Reader,
	})
	if err != nil {
		t.Fatalf("NewVolumeManager: %v", err)
	}
	return m
}

// Stopping a volume is V1's entire durability contract (ADR-0026): nothing else leaves
// the host, so a stop that publishes nothing is a session's writes lost with no error.
//
// The assertion is on the object store, not on a return value — publish() returns
// nothing and logs its failures, which is deliberate (a failed publish must not block a
// teardown) and is exactly why the outward artefact is the only honest check.
func TestStoppingAVolumePublishesItsImage(t *testing.T) {
	r := newPublishRig(t, sim.NewObjectStore())
	v := desiredVolume(t, 1)
	if err := r.m.Apply(t.Context(), []*storagev1.DesiredVolume{v}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	writeOneBlock(t, r.m, v.GetVolumeId())

	if err := r.m.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	objs, err := r.store.List(t.Context(), "image/")
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) == 0 {
		t.Fatal("stopping the volume published nothing: the session's writes exist only on a host that has released them")
	}
}

// A volume whose read view never resolved must not publish: its view is not a subset of
// the truth, it is a different thing, and writing it down makes it the truth.
//
// The fixture is a **clone whose parent snapshot was never published**, and that choice is
// the test. It is the shape of the failure that nothing else refuses: the clone has no
// image of its own, so `image.Publish` CASes with an empty ETag — create-only — and it
// *succeeds*. What lands is a manifest claiming to be this volume's whole state while
// naming only the chunks this session happened to write; the next boot resolves it with no
// error anywhere and serves a volume missing everything it inherited.
//
// The obvious alternative fixture — an object store that answers nothing, so the volume's
// *own* manifest cannot be read — was rejected twice over. It cannot fail: such a volume
// has a manifest in the bucket, so the create-only Put loses the compare-and-set and the
// bucket is identical with or without the guard; and a store that fails every read also
// fails `uploadChunks`'s Head, so the publish would die of the double rather than of the
// rule under test.
//
// The volume writes a block before it stops for the same reason: with an empty view a
// wrong publish has nothing to carry, and an assertion about a bucket nobody could have
// written to proves nothing about the guard. What is asserted is the manifest — its
// absence, and on failure the chunk count it named, which is the fact that separates
// "published a partial view" from "correctly published nothing".
func TestAVolumeWhoseBaseFailedDoesNotPublish(t *testing.T) {
	r := newPublishRig(t, sim.NewObjectStore())
	v := desiredVolume(t, 1)
	// Cloned (§20) from a snapshot that is not in this bucket: fetchBase cannot
	// materialize the parent, so it fails the read view rather than layering an empty one
	// underneath — which would read as zeros for the parent's whole extent.
	v.ParentSnapshotId, v.ParentVolumeId = ids.New().String(), ids.New().String()
	if err := r.m.Apply(t.Context(), []*storagev1.DesiredVolume{v}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	// The read is what waits for the base, and it must fail: the parent resolves to nothing.
	dev, ok := r.m.Device(v.GetVolumeId())
	if !ok {
		t.Fatal("no device")
	}
	if _, err := dev.ReadAt(make([]byte, testBlockSize), 0); err == nil {
		t.Fatal("a read was answered with no recoverable base")
	}
	// The bytes a wrong publish would put in the bucket. Writes do not wait on the base —
	// only reads do — so this volume takes them and has a view to publish.
	if _, err := dev.WriteAt(bytes.Repeat([]byte{0x5C}, testBlockSize), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	// The refusal is *returned* now, and it is the specific one: since C5 the teardown
	// tells its caller which sessions did not reach the object store, and ErrNoReadView is
	// the flavour that must never be retried — the fetch that would have completed the
	// image is over, so holding the data directory for it would be a wait with no event
	// that could end it. A store that is merely unreachable is the retried kind.
	err := r.m.Close(t.Context())
	if !errors.Is(err, agent.ErrNoReadView) {
		t.Fatalf("Close reported %v; a volume whose base never resolved must be reported as one this Agent will not publish", err)
	}

	uu, err := ids.Parse(v.GetVolumeId())
	if err != nil {
		t.Fatal(err)
	}
	key := image.ManifestKey([16]byte(uu))
	body, err := r.store.Get(t.Context(), key)
	switch {
	case err == nil:
		var man image.Manifest
		if perr := json.Unmarshal(body, &man); perr != nil {
			t.Fatalf("a volume with no base published %s, and it does not parse: %v", key, perr)
		}
		t.Fatalf("a volume whose base never resolved published %s naming %d chunk(s) at sequence %d: that manifest is now the volume's entire state, and everything it inherited from snapshot %s is unreachable",
			key, len(man.Chunks), man.Sequence, v.GetParentSnapshotId())
	case !errors.Is(err, objectstore.ErrNotFound):
		t.Fatalf("reading %s: %v", key, err)
	}
}

// A published image is loadable, and what it holds is what the guest wrote. Without this
// the test above would pass on an image that is present and empty.
func TestThePublishedImageHoldsWhatTheGuestWrote(t *testing.T) {
	store := sim.NewObjectStore()
	r := newPublishRig(t, store)
	v := desiredVolume(t, 1)
	if err := r.m.Apply(t.Context(), []*storagev1.DesiredVolume{v}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	dev, ok := r.m.Device(v.GetVolumeId())
	if !ok {
		t.Fatal("no device")
	}
	pattern := bytes.Repeat([]byte{0x6B}, testBlockSize)
	if _, err := dev.WriteAt(pattern, 0); err != nil {
		t.Fatal(err)
	}
	if err := r.m.Close(t.Context()); err != nil {
		t.Fatal(err)
	}

	uu, err := ids.Parse(v.GetVolumeId())
	if err != nil {
		t.Fatal(err)
	}
	u := [16]byte(uu)
	view, _, _, err := image.Load(t.Context(), store, nil, u)
	if err != nil {
		t.Fatalf("the published image does not load: %v", err)
	}
	got := make([]byte, len(pattern))
	view.Read(0, got)
	if !bytes.Equal(got, pattern) {
		t.Fatalf("the image holds %x…, the guest wrote %x…", got[:8], pattern[:8])
	}
}
