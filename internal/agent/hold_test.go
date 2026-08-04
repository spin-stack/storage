package agent_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"path"
	"sync"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// What a shutdown owes a session, when the object store will not take it.
//
// The e2e lane proves the outward half — a process that stays alive, keeps its flock and
// says so on a schedule. These are the parts that need a store which fails on demand: how
// many times the Agent tries, which failures it refuses to try again, and what is still
// on disk afterwards.

// holdRig is a manager with a store that can be made to fail, and a clock whose sleeps
// pass instantly.
type holdRig struct {
	m     *agent.VolumeManager
	store *failingStore
	disk  *sim.Disk
	clk   *steppingClock
	root  string
}

const holdDataDir = "/var/lib/spin"

func newHoldRig(t *testing.T, store *failingStore, onSleep func(n int)) *holdRig {
	t.Helper()
	d := sim.NewDisk()
	f := newListenerFactory()
	clk := &steppingClock{Clock: sim.NewClock(time.Unix(1_700_000_000, 0).UTC()), onSleep: onSleep}
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: holdDataDir, SocketDir: "/run/spin",
	}, agent.VolumeManagerDeps{
		Clock:   clk,
		Disk:    d,
		Listen:  f.listen,
		Mapper:  unusedMapper{},
		EventFD: unusedEventFD,
		Store:   store,
		Rand:    rand.Reader,
	})
	if err != nil {
		t.Fatalf("NewVolumeManager: %v", err)
	}
	return &holdRig{m: m, store: store, disk: d, clk: clk, root: path.Join(holdDataDir, "wal")}
}

// serve starts one volume, waits for its read view, and writes a block through the device
// a guest would be served from — so there is a session in the WAL worth publishing and an
// image worth landing.
func (r *holdRig) serve(t *testing.T) *storagev1.DesiredVolume {
	t.Helper()
	v := desiredVolume(t, 1)
	if err := r.m.Apply(t.Context(), []*storagev1.DesiredVolume{v}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	dev, ok := r.m.Device(v.GetVolumeId())
	if !ok {
		t.Fatal("no device")
	}
	// The read is what waits for the base. Driving the rest before it lands would make
	// the outcome depend on a goroutine's timing rather than on the store's answers.
	if _, err := dev.ReadAt(make([]byte, testBlockSize), 0); err != nil {
		t.Fatalf("waiting for the read view: %v", err)
	}
	if _, err := dev.WriteAt(bytes.Repeat([]byte{0x7E}, testBlockSize), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	return v
}

func (r *holdRig) manifestKey(t *testing.T, volumeID string) string {
	t.Helper()
	u, err := ids.Parse(volumeID)
	if err != nil {
		t.Fatal(err)
	}
	return image.ManifestKey([16]byte(u))
}

// A store that is down when the Agent stops must not cost the session. The Agent keeps
// trying, and the evidence is the object that eventually exists — an assertion on the
// returned error would pass just as happily against a teardown that gave up quietly and
// returned nil.
//
// Four failures and not one: a single failure cannot tell "it retried" from "it swallowed
// the error", and the point of the design is that the number of attempts is unbounded.
func TestAFailedPublishIsRetriedUntilTheStoreTakesIt(t *testing.T) {
	store := &failingStore{Store: sim.NewObjectStore(), failPuts: 4}
	r := newHoldRig(t, store, nil)
	v := r.serve(t)

	if err := r.m.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := store.Get(t.Context(), r.manifestKey(t, v.GetVolumeId())); err != nil {
		t.Fatalf("after a teardown that survived %d refused writes the bucket still holds no manifest for %s: %v",
			store.failed(), v.GetVolumeId(), err)
	}
}

// The escape hatch, and the property that makes it safe to have one: abandoning names
// what it abandoned, and the records are still on disk for the next incarnation.
//
// SHUTDOWN-PUBLISH-SPEC §6 wanted this pinned rather than left as a property nobody
// stated, because every "restart and it republishes" sentence in that document is false
// without it.
func TestAnAbandonedPublishNamesTheVolumeAndKeepsItsWAL(t *testing.T) {
	store := &failingStore{Store: sim.NewObjectStore(), failPuts: -1} // never takes anything
	// The second signal, expressed as the thing it does: the teardown's context is
	// cancelled while it is between attempts.
	ctx, abandon := context.WithCancel(t.Context())
	r := newHoldRig(t, store, func(n int) {
		if n == 3 {
			abandon()
		}
	})
	v := r.serve(t)

	err := r.m.Close(ctx)
	if !errors.Is(err, agent.ErrPublishAbandoned) {
		t.Fatalf("Close returned %v; an operator who abandons the publish must be told which sessions were left behind", err)
	}
	u, perr := ids.Parse(v.GetVolumeId())
	if perr != nil {
		t.Fatal(perr)
	}
	// The whole reason abandoning is allowed: the session is still here. Asserting on the
	// segment files rather than on a flag, because "the WAL is intact" is a statement
	// about the disk and nothing else can make it true.
	dir := wal.SegmentDir(r.root, [16]byte(u), uint64(v.GetEpoch()))
	segs, lerr := r.disk.List(dir)
	if lerr != nil {
		t.Fatalf("listing %s: %v", dir, lerr)
	}
	if len(segs) == 0 {
		t.Fatalf("%s is empty after an abandoned publish: the session was neither published nor kept, so it is gone", dir)
	}
}

// ErrSuperseded is the one failure that must not be retried, and the observable is the
// other writer's manifest: retrying would replace a newer image with an older one, which
// is the single thing INV-10 exists to prevent.
//
// The fixture is a manifest that appears *after* this volume resolved its (absent) read
// view, which is what a second incarnation publishing while this one ran looks like from
// here: this volume holds no ETag, so its own publish is create-only and loses.
func TestASupersededPublishIsNotRetried(t *testing.T) {
	store := &failingStore{Store: sim.NewObjectStore()}
	// "It did not retry" is asserted by making a retry *observable* rather than by
	// counting: the teardown is abandoned the moment it waits between attempts, so a
	// version that retried this failure returns ErrPublishAbandoned instead. Waiting for
	// a count to stay at one would hang here, and a test that fails by timing out says
	// nothing about which rule was broken.
	ctx, abandon := context.WithCancel(t.Context())
	r := newHoldRig(t, store, func(int) { abandon() })
	v := r.serve(t)

	key := r.manifestKey(t, v.GetVolumeId())
	winner := []byte(`{"volume_id":"someone else's","chunks":[],"sequence":99}`)
	if _, err := store.Put(t.Context(), key, winner, objectstore.PutOptions{IfNoneMatch: true}); err != nil {
		t.Fatalf("seeding the other writer's manifest: %v", err)
	}

	err := r.m.Close(ctx)
	if errors.Is(err, agent.ErrPublishAbandoned) {
		t.Fatalf("the teardown waited to retry a superseded publish: %v — retrying it would overwrite a newer image with an older one (INV-10)", err)
	}
	if !errors.Is(err, image.ErrSuperseded) {
		t.Fatalf("Close returned %v; a host whose image was published by another writer must say so, so its supervisor does not restart it", err)
	}
	got, gerr := store.Get(t.Context(), key)
	if gerr != nil {
		t.Fatalf("reading %s: %v", key, gerr)
	}
	if !bytes.Equal(got, winner) {
		t.Fatalf("%s now holds %s: the losing incarnation overwrote the image of the one that won", key, got)
	}
}

// steppingClock is a simulated clock whose Sleep advances time instead of parking until a
// harness advances it. The retry loop's waits are what a test would otherwise have to
// spend real seconds on, and a real sleep in a test is the flake CLAUDE.md calls a stop
// signal.
//
// onSleep is how a test acts *between* attempts — cancelling the teardown's context is
// what a second signal does, and doing it from here makes the moment deterministic
// instead of racing the retry loop with a timer.
type steppingClock struct {
	*sim.Clock
	onSleep func(n int)

	mu     sync.Mutex
	sleeps int
}

func (c *steppingClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.sleeps++
	n := c.sleeps
	c.mu.Unlock()
	if c.onSleep != nil {
		c.onSleep(n)
	}
	c.Advance(d)
	return ctx.Err()
}

var _ clock.Clock = (*steppingClock)(nil)

// failingStore refuses the first failPuts writes (or every one, when failPuts is
// negative) and passes everything else through to a real simulated store.
//
// Only Put is overridden, and that is the shape of the outage this models: the Agent's
// reads all happened at attach, and what a stopping host needs from the store is the
// ability to write. A double that failed reads too would fail the base fetch instead, and
// the volume would be refused for a different reason entirely (ErrNoReadView).
type failingStore struct {
	objectstore.Store

	mu       sync.Mutex
	failPuts int
	failures int
}

var errStoreRefused = errors.New("the object store refused the write")

func (s *failingStore) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	s.mu.Lock()
	fail := s.failPuts != 0
	if fail {
		if s.failPuts > 0 {
			s.failPuts--
		}
		s.failures++
	}
	s.mu.Unlock()
	if fail {
		return objectstore.PutResult{}, errStoreRefused
	}
	return s.Store.Put(ctx, key, data, opts)
}

func (s *failingStore) failed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failures
}
