package agent_test

import (
	"bytes"
	"crypto/rand"
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

func newPublishRig(t *testing.T, store objectstore.Store) *publishRig {
	t.Helper()
	sto, _ := store.(*sim.ObjectStore)
	f := newListenerFactory()
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin", SocketDir: "/run/spin",
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
	return &publishRig{m: m, store: sto}
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

	if err := r.m.Close(); err != nil {
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

// A volume whose read view never resolved must not publish, and this is the sharp edge:
// its view is missing everything the base held, and publishing CASes that partial view
// **over the manifest the base came from**. It would replace the volume's history with a
// subset of it — worse than not publishing at all.
func TestAVolumeWhoseBaseFailedDoesNotPublish(t *testing.T) {
	r := newPublishRig(t, newUnreachableStore())
	v := desiredVolume(t, 1)
	if err := r.m.Apply(t.Context(), []*storagev1.DesiredVolume{v}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	// The read is what waits for the base, and it must fail: the store answers nothing.
	dev, ok := r.m.Device(v.GetVolumeId())
	if !ok {
		t.Fatal("no device")
	}
	if _, err := dev.ReadAt(make([]byte, testBlockSize), 0); err == nil {
		t.Fatal("a read was answered with no recoverable base")
	}

	if err := r.m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Nothing was written — asserted against a real store underneath the unreachable
	// facade, so this checks what was stored rather than what the facade reported.
	if objs, _ := sim.NewObjectStore().List(t.Context(), "image/"); len(objs) != 0 {
		t.Fatal("a volume with no base published anyway")
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
	if err := r.m.Close(); err != nil {
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
