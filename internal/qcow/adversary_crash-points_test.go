package qcow_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/qcow"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// These are adversarial tests. Each one puts the crash exactly where the production
// comment says the window is, and then asks what the *next* Agent does with what it
// finds — which is the question a window's width does not answer. A window that is
// microseconds wide and that nothing converges out of is not narrower than one that is
// seconds wide and self-healing; it is worse.

// crashPaths is fakePaths with one write under the test's control. Failing the write of
// `state.json` is exactly what a SIGKILL between the QMP snapshot returning and
// Manager.recordPending landing leaves behind, as seen by the process that starts next:
// a rotated chain on disk and no record of what was sealed.
type crashPaths struct {
	*fakePaths
	failState error
	// failStateAfter skips that many successful writes before failState starts biting,
	// so a test can name a kill point *inside* a cycle that writes the record more than
	// once — a rotation records the new layer before it moves the pointer, and the sealed
	// one after QEMU has switched.
	failStateAfter int
}

func (p *crashPaths) WriteAtomic(path string, data []byte) error {
	if p.failState != nil && path == qcow.StateFile(root, vol) {
		if p.failStateAfter > 0 {
			p.failStateAfter--
		} else {
			return p.failState
		}
	}
	return p.fakePaths.WriteAtomic(path, data)
}

// flakyDialer is a QMP socket that is momentarily not there. Every dial error becomes
// qmp.ErrNoEndpoint, which the Manager reads as "no VM is attached" — so this models the
// one thing an Agent cannot tell apart from a guest that has gone away.
type flakyDialer struct {
	*fakeDialer
	failNext int
}

func (d *flakyDialer) Dial(ctx context.Context, path string) (io.ReadWriteCloser, error) {
	if d.failNext > 0 {
		d.failNext--
		return nil, errors.New("dial unix: resource temporarily unavailable")
	}
	return d.fakeDialer.Dial(ctx, path)
}

// adversary is a Manager built directly rather than through newHarness, because these
// tests replace collaborators the harness fixes.
type adversary struct {
	m      *qcow.Manager
	runner *fakeRunner
	paths  *crashPaths
	dialer *flakyDialer
	rec    *fakeRecovery
	pub    *recordingPublisher
}

func newAdversary(t *testing.T, rotateAt int64, pub *recordingPublisher) *adversary {
	t.Helper()
	a := &adversary{
		runner: &fakeRunner{info: infoJSON("qcow2", size, false), version: "qemu-img version 11.0.2"},
		paths:  &crashPaths{fakePaths: newPaths()},
		dialer: &flakyDialer{fakeDialer: &fakeDialer{scripts: map[string][]string{}}},
		rec:    bornEmpty(),
		pub:    pub,
	}
	a.start(t, rotateAt, pub)
	return a
}

// start builds a Manager over whatever this adversary already holds on disk.
func (a *adversary) start(t *testing.T, rotateAt int64, pub *recordingPublisher) {
	t.Helper()
	deps := qcow.Deps{
		Clock: sim.NewClock(time.Unix(0, 0)), Disk: sim.NewDisk(),
		Runner: a.runner, Paths: a.paths, Dialer: a.dialer, Recovery: a.rec,
	}
	// A nil *recordingPublisher in an interface field is not a nil interface, and the
	// Manager branches on `m.pub == nil`. Assigned only when there is one.
	if pub != nil {
		deps.Publisher = pub
	}
	m, err := qcow.New(t.Context(), qcow.Config{
		Root: root, QemuImg: "/qemu-img", ProbeTimeout: time.Second, RotateAtBytes: rotateAt,
	}, deps)
	if err != nil {
		t.Fatalf("building a manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	a.runner.reset()
	a.m = m
}

// crash is the next Agent on the same data directory: nothing this process knew survives,
// everything on disk does. The lock is a fresh simulated disk because the kernel drops an
// flock when a process dies.
func (a *adversary) crash(t *testing.T, rotateAt int64, pub *recordingPublisher) {
	t.Helper()
	if err := a.m.Close(); err != nil {
		t.Fatalf("closing the Agent that is being killed: %v", err)
	}
	a.pub = pub
	a.start(t, rotateAt, pub)
}

func (a *adversary) apply(t *testing.T, epoch int64) error {
	t.Helper()
	return a.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, epoch)})
}

func (a *adversary) tip(t *testing.T) string {
	t.Helper()
	body, err := a.paths.ReadFile(qcow.ActivePointer(root, vol))
	if err != nil {
		t.Fatalf("reading the pointer: %v", err)
	}
	return string(body)
}

// guestWriting attaches a VM to `image` and says how large that layer has grown.
func (a *adversary) guestWriting(image string, bytes int64) {
	a.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(image)
	a.paths.sizes[image] = bytes
	// What `qemu-img info` will say about the overlay a rotation is about to create.
	a.runner.info = overlayJSON(size, image)
	a.runner.reset()
}

// volumes runs Volumes and returns it keyed by id.
func (a *adversary) volumes(t *testing.T) map[string]agent.VolumeStatus {
	t.Helper()
	got, err := a.m.Volumes(t.Context())
	if err != nil {
		t.Fatalf("reading the served volumes: %v", err)
	}
	out := make(map[string]agent.VolumeStatus, len(got))
	for _, v := range got {
		out[v.VolumeID] = v
	}
	return out
}

// offered is every layer id the publisher was ever handed.
func (a *adversary) offered() map[string]bool {
	out := map[string]bool{}
	for _, l := range a.pub.got {
		out[l.LayerID] = true
	}
	return out
}

// TestAdversaryASealedLayerNobodyRecordedIsDroppedFromTheHistory.
//
// qcow.rotate seals the tip through QMP and *then* writes the record of what it owes.
// The comment on qcow.PendingCommit calls the gap between those two "microseconds wide"
// and prices it at "a duplicate entry in a history". It is not a duplicate entry. The
// restarted Agent opens the chain at whatever QEMU has open, reads a state file that says
// it owes nothing, and never looks in the layers directory — so the sealed layer is not
// republished under a second commit id, it is never published at all. The next rotation
// then commits the layer *above* it with a parent read from HEAD, which splices a hole
// into the published chain: a recovery on any other host rebuilds a history in which
// everything the guest wrote into the forgotten layer never happened, and nothing
// anywhere reports it.
func TestAdversaryASealedLayerNobodyRecordedIsDroppedFromTheHistory(t *testing.T) {
	t.Parallel()
	a := newAdversary(t, 8<<20, &recordingPublisher{})

	if err := a.apply(t, 1); err != nil {
		t.Fatalf("preparing the volume: %v", err)
	}
	first := a.tip(t)
	a.guestWriting(first, 9<<20)

	// The cycle that rotates. A rotation writes the record twice: the new layer before
	// the pointer names it, and the sealed one once QEMU has switched. The kill point is
	// the second — the QMP snapshot has taken effect, the guest is writing to the new
	// layer, and the process dies before anything records what the old one became.
	a.paths.failStateAfter = 1
	a.paths.failState = errors.New("SIGKILL after blockdev-snapshot-sync and before the sealed layer is recorded")
	if err := a.apply(t, 1); err == nil {
		t.Fatal("the rotation whose record never landed was reported as a success")
	}
	second := a.tip(t)
	if second == first {
		t.Fatalf("no rotation happened: the pointer still names %q", first)
	}
	if len(a.pub.got) != 0 {
		t.Fatalf("the sealed layer was published before the crash: %v", a.pub.got)
	}

	// The next Agent. The disk is healthy again; only what was lost is lost.
	a.crash(t, 8<<20, &recordingPublisher{})
	a.paths.failState, a.paths.failStateAfter = nil, 0

	// Three cycles of a guest that keeps writing, enough for the layer above to cross
	// the threshold and be committed.
	for i := range 3 {
		a.guestWriting(second, int64(20+i)<<20)
		_ = a.apply(t, 1)
		second = a.tip(t)
	}

	// The control. Everything sealed *after* the crash is published normally, so a red
	// assertion below is about the forgotten layer and not about a harness that publishes
	// nothing.
	if len(a.pub.got) == 0 {
		t.Fatal("nothing at all was published after the restart; this test is not exercising the publish path")
	}
	sealed := qcow.LayerIDOfImage(first)
	if !a.offered()[sealed] {
		t.Errorf("layer %s was sealed with the guest's writes in it and was never offered for publishing;"+
			" the layers that were: %v", sealed, a.offered())
	}
	// And the shape of the damage: the history that *was* published starts above the
	// hole, so HEAD claims a chain whose oldest commit is not the volume's first layer.
	for _, l := range a.pub.got {
		if l.LayerID == sealed {
			continue
		}
		t.Logf("published %s, whose local backing %s is in no commit", l.LayerID, sealed)
	}
}

// TestAdversaryALayerSealedWithNoPublisherIsNeverPublishedWhenOneArrives is the same hole
// with no crash in it at all. qcow.rotate returns before it records anything when
// Deps.Publisher is nil, and STATUS says an Agent with no object store configured is the
// default. Turn publishing on later — the operator's obvious move — and every layer
// sealed in the meantime is already invisible: the first commit ever published names a
// layer whose backing files exist only on this host.
func TestAdversaryALayerSealedWithNoPublisherIsNeverPublishedWhenOneArrives(t *testing.T) {
	t.Parallel()
	a := newAdversary(t, 8<<20, nil)

	if err := a.apply(t, 1); err != nil {
		t.Fatalf("preparing the volume: %v", err)
	}
	first := a.tip(t)
	a.guestWriting(first, 9<<20)
	if err := a.apply(t, 1); err != nil {
		t.Fatalf("the cycle that rotates without a publisher: %v", err)
	}
	second := a.tip(t)
	if second == first {
		t.Fatal("no rotation happened")
	}

	// The operator configures an object store and restarts the Agent.
	a.crash(t, 8<<20, &recordingPublisher{})
	for i := range 3 {
		a.guestWriting(second, int64(20+i)<<20)
		_ = a.apply(t, 1)
		second = a.tip(t)
	}

	if len(a.pub.got) == 0 {
		t.Fatal("nothing at all was published after the restart; this test is not exercising the publish path")
	}
	sealed := qcow.LayerIDOfImage(first)
	if !a.offered()[sealed] {
		t.Errorf("layer %s was sealed before publishing was configured and was never offered afterwards;"+
			" the layers that were: %v", sealed, a.offered())
	}
}

// TestAdversaryAStaleLocalChainIsServedWithoutAskingTheBucket.
//
// The recovery guard is asked exactly one question — may this volume be born empty? — and
// only on the branch where there is no local chain. The question it does not ask is
// whether the local chain that *is* there is still this volume's history. A host that had
// the volume, lost it, and is given it back after somebody else published keeps a pointer,
// so `born` never runs and nothing consults the object store: the guest is handed a chain
// missing every commit made while this host was not the writer. It is the blank-disk
// defect with plausible-looking bytes instead of zeros, and it gets worse on the next
// rotation, which publishes this host's stale overlay with the *other* host's HEAD as its
// parent — a commit chain that no longer reconstructs anything.
func TestAdversaryAStaleLocalChainIsServedWithoutAskingTheBucket(t *testing.T) {
	t.Parallel()
	a := newAdversary(t, 8<<20, &recordingPublisher{})

	if err := a.apply(t, 1); err != nil {
		t.Fatalf("preparing the volume: %v", err)
	}
	first := a.tip(t)
	a.guestWriting(first, 9<<20)
	if err := a.apply(t, 1); err != nil {
		t.Fatalf("the cycle that rotates and publishes: %v", err)
	}
	stale := a.tip(t)

	// The fleet takes the volume away: it is placed on another host, which recovers it
	// from the bucket, serves it, and publishes two commits of its own.
	if err := a.m.Apply(t.Context(), nil); err != nil {
		t.Fatalf("releasing the volume: %v", err)
	}
	// ...and then it comes back here, at a higher epoch, with the guest not yet launched.
	delete(a.dialer.scripts, qcow.QMPSocket(root, vol))
	a.runner.info = infoJSON("qcow2", size, false)
	a.rec.calls = nil

	if err := a.apply(t, 9); err != nil {
		t.Fatalf("taking the volume back: %v", err)
	}

	if got := a.tip(t); got != stale {
		t.Fatalf("the pointer names %q, want the stale local tip %q", got, stale)
	}
	if v := a.volumes(t)[vol]; v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Logf("the volume was refused: %v %q", v.Refusal, v.RefusalDetail)
	}
	if len(a.rec.calls) == 0 {
		t.Errorf("this host took the volume back and served %q without once asking the object store"+
			" whether its local chain is still the head of this volume's history", stale)
	}
}

// TestAdversaryOneMissedQMPDialLatchesTheVolumeForever.
//
// qmp.Dial turns *every* dial failure into ErrNoEndpoint, which Manager.probe reads as
// "no VM is attached". That is the right reading for a volume whose guest has not been
// launched and the wrong one for a socket that was momentarily unavailable — and the two
// cannot be told apart from here. On the cycle after a restart under a running guest (the
// case `task demo:stage1` exists to prove), one missed dial sends Open down the offline
// branch, where `qemu-img info --backing-chain` hits QEMU's write lock and fails. That is
// classified ATTACH_FAILED, which retryable() says does not clear on its own, so the
// volume is refused for as long as this process lives even though the very next cycle can
// reach QEMU perfectly. The Control Plane is told this host cannot serve a volume whose
// guest is, at that moment, writing to it.
func TestAdversaryOneMissedQMPDialLatchesTheVolumeForever(t *testing.T) {
	t.Parallel()
	a := newAdversary(t, 0, nil)

	if err := a.apply(t, 1); err != nil {
		t.Fatalf("preparing the volume: %v", err)
	}
	live := a.tip(t)

	// The Agent is killed and comes back with the guest still running.
	a.crash(t, 0, nil)
	a.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(live)
	// One dial is missed, and qemu-img meets the lock QEMU holds on the live image.
	a.dialer.failNext = 1
	a.runner.chainErr = errors.New(`qemu-img: Failed to get shared "write" lock`)

	if err := a.apply(t, 1); err == nil {
		t.Fatal("the cycle that met the write lock was reported as a success")
	}
	if v := a.volumes(t)[vol]; v.Refusal == storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Fatal("the volume was not refused, so this test is not exercising the latch")
	}

	// Everything is fine now: the socket answers and QEMU says which layer it has open.
	a.runner.chainErr = nil
	for range 3 {
		_ = a.apply(t, 1)
	}
	if v := a.volumes(t)[vol]; v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Errorf("three cycles after the socket came back the volume is still refused as %v (%q),"+
			" while its guest is writing to %s", v.Refusal, v.RefusalDetail, live)
	}
	if !strings.HasPrefix(live, qcow.LayersDir(root)) {
		t.Fatalf("the live image %q is not a layer of this volume", live)
	}
	// The control, and the proof of what is holding the refusal on: the identical cycle
	// at a higher epoch walks straight past the latch in ensure and the volume is ready
	// again. Nothing about the host changed between these two cycles except the number
	// the Control Plane sent, so the refusal was never about the host.
	if err := a.apply(t, 2); err != nil {
		t.Fatalf("the same cycle at a higher epoch: %v", err)
	}
	if v := a.volumes(t)[vol]; v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Fatalf("even a higher epoch did not clear the refusal (%v); this test is not exercising the latch", v.Refusal)
	}
}
