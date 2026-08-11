package agent_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/vhost"
)

// The attach window, at the seam it is reachable from.
//
// `start` binds the socket and puts the volume under supervision, and only then does
// `fetchBase` go to the object store. Between those two the volume has a device and its
// sequence space is not settled: the image's sequence is unknown, and the records this host
// still holds under an earlier epoch have not been taken up. One guest write inside that
// window used to make `CarryForward` refuse for ever — which is the bricked volume of
// carry_forward_test.go, reached with no operator mistake and no crash, by a guest that
// boots faster than one object-store round trip.
//
// The window is held open by a store that answers nothing until the test says so, which is
// what the task of widening it means here: the defect is not a narrow race, it is an
// ordering the Agent had no rule about. `synctest.Wait` is what turns the ordering into an
// assertion — it returns only when every other goroutine is durably blocked or done, so at
// that line the guest's write has either reached the WAL (the defect) or is parked on the
// base (the rule), with nothing in between and no interval to tune.
//
// **Two rules, and they are not the same rule twice.** The last test in this file says no
// front-end is *accepted* until the volume can serve, which is what stops a real guest from
// ever being in the window. The first two write through `VolumeManager.Device` — the
// accessor a DST scenario and this package use, which the accept rule does not cover — and
// say what happens if something does get in there anyway: the volume still starts, this time
// and every time after. The first rule keeps the guest out; the second is why being in there
// is survivable, and it lives in the log, which is the only place that knows its own
// sequence space is unsettled.

// heldStore answers no read until it is released. Everything else is the store underneath,
// so a publish after the release behaves exactly as it does in every other test.
//
// Releasing it is registered as a cleanup as well as called by the test. A synctest bubble
// ends only when every goroutine in it has exited, so a failed assertion that skipped the
// release would replace the assertion's message with a deadlock report.
type heldStore struct {
	objectstore.Store
	once sync.Once
	open chan struct{}
}

func newHeldStore(t *testing.T, s objectstore.Store) *heldStore {
	t.Helper()
	h := &heldStore{Store: s, open: make(chan struct{})}
	t.Cleanup(h.release)
	return h
}

func (h *heldStore) release() { h.once.Do(func() { close(h.open) }) }

func (h *heldStore) Get(ctx context.Context, key string) ([]byte, error) {
	<-h.open
	return h.Store.Get(ctx, key)
}

func (h *heldStore) Head(ctx context.Context, key string) (objectstore.ObjectInfo, error) {
	<-h.open
	return h.Store.Head(ctx, key)
}

// attachSession is resumeSession with a teardown. Same reason as heldStore's: a manager
// still supervising a volume when the bubble ends is a goroutine that never exits, and
// Close is idempotent, so the tests below can also stop it where they mean to.
func attachSession(t *testing.T, d *sim.Disk, store objectstore.Store) *agent.VolumeManager {
	t.Helper()
	m := resumeSession(t, d, store)
	//nolint:usetesting // Close must be able to publish, and t.Context is cancelled by now
	t.Cleanup(func() { _ = m.Close(context.Background()) })
	return m
}

// guestWriteDuringAttach applies v to m, issues one guest write while the base is still
// being fetched, and returns once the fetch has been let through and the write has settled.
//
// The write goes through the device the manager hands out, which is the same *blockdev.Device
// the vhost front-end serves a guest from — the only write path this volume has.
func guestWriteDuringAttach(t *testing.T, m *agent.VolumeManager, held *heldStore, v *storagev1.DesiredVolume, at int64, b byte) {
	t.Helper()
	if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{v}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	dev, ok := m.Device(v.GetVolumeId())
	if !ok {
		t.Fatalf("no device for volume %s", v.GetVolumeId())
	}

	done := make(chan error, 1)
	go func() {
		_, err := dev.WriteAt(bytes.Repeat([]byte{b}, resumeBlock), at)
		done <- err
	}()

	// The base fetch is parked inside the store and the guest's write has either landed
	// or parked. Neither outcome is timing-dependent from here.
	synctest.Wait()
	held.release()
	// And now the fetch has run to its end — installed or refused — and the write has
	// settled either way. Everything a caller asserts after this is about the disk.
	synctest.Wait()

	if err := <-done; err != nil {
		t.Fatalf("the write a guest issued during the attach window failed: %v", err)
	}
}

// TestAGuestWriteDuringAttachDoesNotStrandTheSessionTheHostStillHolds is the reproduction
// and the rule.
//
// A session is ACKed and never published, so its records sit under epoch 1 on this host's
// device and nowhere else. The volume is attached again at epoch 2 — every placement grants
// a fresh epoch — and a guest writes before the base has arrived. The stranded session must
// still be taken up, and the guest's own write must survive: the assertion is the read a
// guest gets for both.
func TestAGuestWriteDuringAttachDoesNotStrandTheSessionTheHostStillHolds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, store := sim.NewDisk(), sim.NewObjectStore()

		first := desiredVolume(t, 1)
		acked, offsets := unpublishedSession(t, resumeSession(t, d, store), store, first, 32)

		regranted := lostDataVolume(t, 2, 0, acked)
		regranted.VolumeId = first.GetVolumeId()

		held := newHeldStore(t, store)
		second := attachSession(t, d, held)
		guestOffset := int64(len(offsets)) * resumeBlock
		guestWriteDuringAttach(t, second, held, regranted, guestOffset, 0x5A)

		dev, ok := second.Device(regranted.GetVolumeId())
		if !ok {
			t.Fatalf("no device for volume %s", regranted.GetVolumeId())
		}
		readBack(t, dev, offsets, 0xC1,
			fmt.Sprintf("the volume was ACKed up to sequence %d under epoch 1 and re-attached at epoch 2 with a guest "+
				"writing during the fetch; those records are on this host's disk and nothing else has them", acked))
		readBack(t, dev, []int64{guestOffset}, 0x5A,
			"the write the guest made during the attach window was accepted, so it has to be readable")

		// And the fleet is told the volume is being served. A refusal here is the volume
		// an operator cannot start, which is the whole failure.
		vols, err := second.Volumes(t.Context())
		if err != nil {
			t.Fatalf("Volumes: %v", err)
		}
		for _, s := range vols {
			if s.VolumeID == regranted.GetVolumeId() && s.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
				t.Fatalf("the volume refuses to serve after a guest write during its attach: %v — %s", s.Refusal, s.RefusalDetail)
			}
		}
		//nolint:usetesting // t.Context is cancelled before cleanups run and Close must publish
		if err := second.Close(context.Background()); err != nil {
			t.Fatalf("publishing the session that took the guest's write: %v", err)
		}
	})
}

// TestAGuestWriteDuringAttachDoesNotMakeTheVolumeUnstartable is the half that makes the
// defect permanent rather than merely bad.
//
// The write that lands inside the window is filed under the granted epoch at a sequence the
// stranded epoch already uses. From then on this host holds two directories whose sequence
// spaces overlap, and every later attach — with no guest anywhere near it — finds a hole and
// refuses. Another attach is another epoch, and there is nothing an operator can run that
// reaches those bytes.
//
// So this attaches a third time with no guest at all, and asserts the volume comes back.
func TestAGuestWriteDuringAttachDoesNotMakeTheVolumeUnstartable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, store := sim.NewDisk(), sim.NewObjectStore()

		first := desiredVolume(t, 1)
		acked, offsets := unpublishedSession(t, resumeSession(t, d, store), store, first, 32)

		regranted := lostDataVolume(t, 2, 0, acked)
		regranted.VolumeId = first.GetVolumeId()

		held := newHeldStore(t, store)
		second := attachSession(t, d, held)
		guestOffset := int64(len(offsets)) * resumeBlock
		guestWriteDuringAttach(t, second, held, regranted, guestOffset, 0x5A)

		// The second session is abandoned too: the guest's write is now part of what only
		// this host holds. Whether Close reports the abandoned publish or a refused read
		// view does not matter and is deliberately not asserted — the premise either way is
		// that nothing reached the bucket, which the third attach then proves by reading.
		store.InjectThrottle(1 << 20)
		abandoned, cancel := context.WithCancel(t.Context())
		cancel()
		_ = second.Close(abandoned)
		store.InjectThrottle(0)

		// The third attach. No guest, no window, nothing unusual — and before the rule it
		// refused for ever, because epoch 2 held a record under a sequence epoch 1 also used.
		third := lostDataVolume(t, 3, 0, acked)
		third.VolumeId = first.GetVolumeId()
		last := attachSession(t, d, store)
		dev := serveVolume(t, last, third)
		readBack(t, dev, offsets, 0xC1,
			"a guest write during an earlier attach must not make the volume unstartable: these records are on "+
				"this host's disk and no bucket has them")
		readBack(t, dev, []int64{guestOffset}, 0x5A,
			"the write the guest made during the attach window was accepted and never published, so this host still owes it")

		//nolint:usetesting // t.Context is cancelled before cleanups run and Close must publish
		if err := last.Close(context.Background()); err != nil {
			t.Fatalf("publishing the attach that had to recover from the window: %v", err)
		}
	})
}

// countingListener is a vhost.Listener that records how many times the Agent asked it for a
// connection. That count is the whole observable of the rule below: a front-end reaches the
// device by being accepted, and nothing else in this process can tell whether the socket a
// guest connected to is a disk or a promise.
type countingListener struct {
	mu       sync.Mutex
	accepts  int
	closes   int
	closedCh chan struct{}
}

func newCountingListener() *countingListener {
	return &countingListener{closedCh: make(chan struct{})}
}

func (l *countingListener) Accept() (vhost.Conn, error) {
	l.mu.Lock()
	l.accepts++
	l.mu.Unlock()
	<-l.closedCh
	return nil, net.ErrClosed
}

func (l *countingListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closes++
	select {
	case <-l.closedCh:
	default:
		close(l.closedCh)
	}
	return nil
}

func (l *countingListener) counts() (accepts, closes int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.accepts, l.closes
}

// attachRig is one Agent whose object store answers nothing until the test releases it and
// whose listener counts what the supervisor asked of it.
type attachRig struct {
	m    *agent.VolumeManager
	held *heldStore
	ln   *countingListener
}

func newAttachRig(t *testing.T, d *sim.Disk, store objectstore.Store) *attachRig {
	t.Helper()
	rig := &attachRig{held: newHeldStore(t, store), ln: newCountingListener()}
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir:   holdDataDir,
		SocketDir: "/run/spin",
		Budget:    testBudget(),
	}, agent.VolumeManagerDeps{
		Clock:   sim.NewClock(time.Unix(1_700_000_000, 0).UTC()),
		Disk:    d,
		Listen:  func(string) (vhost.Listener, error) { return rig.ln, nil },
		Mapper:  unusedMapper{},
		EventFD: unusedEventFD,
		Store:   rig.held,
		Rand:    rand.Reader,
	})
	if err != nil {
		t.Fatalf("NewVolumeManager: %v", err)
	}
	rig.m = m
	return rig
}

// TestNoFrontEndIsAcceptedUntilTheVolumeCanServe is the promise the socket makes.
//
// The listener is bound in `start` — deliberately, so a socket that cannot be bound fails
// Apply instead of disappearing into a goroutine — but being bound is not being a disk. A
// front-end accepted before the base has resolved negotiates a size, a serial and a set of
// features for a device whose reads park and which may be about to be taken away entirely:
// a guest that boots into that gets an ordinary /dev/vda and then a hard I/O error on every
// sector, which is the least actionable signal this system can emit.
//
// Two cases, one rule, and they are the same assertion counted twice: the Agent must not
// accept a connection while the base is pending, and must never accept one at all if the
// base resolves into a refusal.
func TestNoFrontEndIsAcceptedUntilTheVolumeCanServe(t *testing.T) {
	tests := []struct {
		name string
		// published is what the catalog says this volume last wrote to the object store.
		// A non-zero value the bucket cannot account for is ErrImageMissing — the refusal
		// a guest must never be attached across. Everything else about the two arms, and
		// the whole of the window, is identical.
		published int64
		serves    bool
	}{
		{name: "the base arrives", serves: true},
		{name: "the volume is refused", published: 7},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d, store := sim.NewDisk(), sim.NewObjectStore()
				rig := newAttachRig(t, d, store)

				v := desiredVolume(t, 1)
				v.PublishedSequence = tc.published
				if err := rig.m.Apply(t.Context(), []*storagev1.DesiredVolume{v}); err != nil {
					t.Fatalf("Apply: %v", err)
				}

				// The base fetch is parked in the store. If the supervisor has asked for a
				// connection by now, a guest can be holding a device this Agent cannot yet
				// promise anything about.
				synctest.Wait()
				if accepts, _ := rig.ln.counts(); accepts != 0 {
					t.Fatalf("the Agent accepted %d front-end connection(s) while the volume's read view was still being fetched: "+
						"a guest attaching here gets a device whose reads park and which may be withdrawn entirely", accepts)
				}

				rig.held.release()
				synctest.Wait()

				accepts, closes := rig.ln.counts()
				if tc.serves && accepts == 0 {
					t.Fatalf("the volume resolved its read view and the Agent never accepted a front-end: nothing can attach to it")
				}
				if !tc.serves {
					if accepts != 0 {
						t.Fatalf("the Agent accepted %d front-end connection(s) for a volume it refused to serve", accepts)
					}
					if closes == 0 {
						t.Fatal("a refused volume left its listener open, so its socket stays on the filesystem promising a device that does not exist")
					}
				}

				//nolint:usetesting // t.Context is cancelled before cleanups run and Close must publish
				if err := rig.m.Close(context.Background()); err != nil && tc.serves {
					t.Fatalf("closing the Agent: %v", err)
				}
			})
		})
	}
}
