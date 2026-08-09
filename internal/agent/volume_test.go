package agent_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/vhost"
	"github.com/spin-stack/storage/internal/wal"
)

// The keystone's contract, stated once: a VolumeManager turns the desired state into
// live per-volume runtimes, each owning a wal.Log, a blockdev.Device and a vhost
// server on its own socket, and reports what those runtimes actually observe.
//
// Everything here runs on sim.Disk, sim.Clock and an in-memory listener (INV-01).
// The socket is the one thing a unit test cannot exercise for real, so what is
// asserted is the *lifecycle* around it — opened per volume, closed on stop, and a
// serve loop that is restarted rather than lost.

const (
	testVolumeSize = 1 << 20 // 1 MiB, a whole number of 512-byte sectors
	testBlockSize  = 512
)

// fakeListener is a vhost.Listener that never yields a connection. A unit test has no
// front-end; what it needs from the listener is that Accept blocks until Close, which
// is precisely the contract Server.Serve relies on for cancellation.
type fakeListener struct {
	mu     sync.Mutex
	closed chan struct{}
	closes int
}

func newFakeListener() *fakeListener { return &fakeListener{closed: make(chan struct{})} }

func (l *fakeListener) Accept() (vhost.Conn, error) {
	<-l.closed
	return nil, net.ErrClosed
}

func (l *fakeListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closes++
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}

func (l *fakeListener) closeCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closes
}

// listenerFactory records every socket path it was asked for, so a test can assert
// the naming convention without a filesystem.
type listenerFactory struct {
	mu    sync.Mutex
	paths []string
	made  map[string]*fakeListener
	err   error
	// listened signals every successful open. A test waits on it rather than polling:
	// the supervisor re-listens from a goroutine, and a wall-clock poll loop is both
	// flaky and a direct INV-01 violation.
	listened chan string
}

func newListenerFactory() *listenerFactory {
	return &listenerFactory{made: map[string]*fakeListener{}, listened: make(chan string, 16)}
}

func (f *listenerFactory) listen(socket string) (vhost.Listener, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	f.paths = append(f.paths, socket)
	ln := newFakeListener()
	f.made[socket] = ln
	select {
	case f.listened <- socket:
	default: // a test that is not watching must not block the manager
	}
	return ln, nil
}

func (f *listenerFactory) socketPaths() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.paths...)
}

func (f *listenerFactory) listenerFor(socket string) *fakeListener {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.made[socket]
}

// unusedMapper and unusedEventFD stand where the kernel objects go. vhost requires both
// to build a server, and neither is ever called without a front-end — so they fail loudly
// rather than pretending, which is what would happen if a session ever did reach them.
type unusedMapper struct{}

func (unusedMapper) Map(*os.File, uint64, uint64) ([]byte, error) {
	return nil, errors.New("no front-end in a unit test")
}
func (unusedMapper) Unmap([]byte) error { return nil }

func unusedEventFD(*os.File) (vhost.EventFD, error) {
	return nil, errors.New("no front-end in a unit test")
}

// testBudget is the device budget a unit test's manager runs on (ADR-0013 §1).
//
// Every manager needs one now — NewVolumeManager refuses a zero budget, because an
// Agent with no budget has no write-path bound at all — and no test here is about the
// bound: 1 MiB per volume is orders of magnitude more than any of them writes, so
// none of them meets backpressure by accident. The proof that the bound holds is in
// the DST harness (device-budget-holds-across-volumes), where a simulated device can
// be filled and the assertion can be made against what the device reports.
func testBudget() agent.Budget {
	return agent.Budget{DeviceBytes: 8 << 20, ReserveBytes: 1 << 20, GuestBytes: 4 << 20, MaxVolumes: 4}
}

func newTestManager(t *testing.T) (*agent.VolumeManager, *listenerFactory, *sim.Disk) {
	t.Helper()
	d := sim.NewDisk()
	f := newListenerFactory()
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir:   "/var/lib/spin",
		SocketDir: "/run/spin",
		Budget:    testBudget(),
	}, agent.VolumeManagerDeps{
		Clock:   sim.NewClock(time.Unix(1_700_000_000, 0).UTC()),
		Disk:    d,
		Listen:  f.listen,
		Mapper:  unusedMapper{},
		EventFD: unusedEventFD,
	})
	if err != nil {
		t.Fatalf("NewVolumeManager: %v", err)
	}
	// context.Background and not t.Context: the test context is cancelled just
	// before cleanups run, and a cancelled context is how an operator says "abandon
	// the publish". A cleanup that abandons would make every test's teardown the
	// interesting path rather than the ordinary one.
	t.Cleanup(func() { _ = m.Close(context.Background()) }) //nolint:usetesting // see above
	return m, f, d
}

func desiredVolume(t *testing.T, epoch int64) *storagev1.DesiredVolume {
	t.Helper()
	return &storagev1.DesiredVolume{
		VolumeId:  ids.New().String(),
		SizeBytes: testVolumeSize,
		BlockSize: testBlockSize,
		Epoch:     epoch,
		State:     storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}
}

// writeOneBlock writes through the device a guest would be served from — the only way
// to make the WAL exist on disk, and the only write path worth asserting about.
func writeOneBlock(t *testing.T, m *agent.VolumeManager, volumeID string) {
	t.Helper()
	dev, ok := m.Device(volumeID)
	if !ok {
		t.Fatalf("no device for volume %s", volumeID)
	}
	if _, err := dev.WriteAt(make([]byte, testBlockSize), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
}

// TestFencedVolumesStopBeingServed is DEV-0012. A report the Control Plane refuses
// means this host is not the writer any more, and until now that was recorded and
// acted on by nothing.
func TestFencedVolumesStopBeingServed(t *testing.T) {
	t.Parallel()
	m, f, _ := newTestManager(t)
	ctx := t.Context()

	v := desiredVolume(t, 1)
	if err := m.Apply(ctx, []*storagev1.DesiredVolume{v}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	socket := f.socketPaths()[0]

	if err := m.Fence(ctx, []string{v.GetVolumeId()}, storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED, ""); err != nil {
		t.Fatalf("Fence: %v", err)
	}

	vols, err := m.Volumes(ctx)
	if err != nil {
		t.Fatalf("Volumes: %v", err)
	}
	if len(vols) != 0 {
		t.Fatalf("a fenced volume is still being served: %+v", vols)
	}
	if f.listenerFor(socket).closeCount() == 0 {
		t.Error("the socket outlived the fencing: a guest could still attach")
	}
	if _, ok := m.Device(v.GetVolumeId()); ok {
		t.Error("the device survived the fencing; reads would still be answered")
	}
}

// TestAFencedVolumeDoesNotComeBackAtTheSameEpoch is the half that is easy to miss. The
// Control Plane refuses the *report* while GetDesiredState may keep listing the volume
// for this host, so without a memory of the fencing the very next Apply would find no
// runtime and start one — serving a volume this host has just been told it lost.
func TestAFencedVolumeDoesNotComeBackAtTheSameEpoch(t *testing.T) {
	t.Parallel()
	m, f, _ := newTestManager(t)
	ctx := t.Context()

	v := desiredVolume(t, 1)
	desired := []*storagev1.DesiredVolume{v}
	if err := m.Apply(ctx, desired); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := m.Fence(ctx, []string{v.GetVolumeId()}, storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED, ""); err != nil {
		t.Fatalf("Fence: %v", err)
	}

	// The desired state has not changed; the Control Plane simply has not caught up.
	for range 3 {
		if err := m.Apply(ctx, desired); err != nil {
			t.Fatalf("Apply after fencing: %v", err)
		}
	}
	vols, err := m.Volumes(ctx)
	if err != nil {
		t.Fatalf("Volumes: %v", err)
	}
	if len(vols) != 0 {
		t.Fatalf("a fenced volume was restarted by the next Apply: %+v", vols)
	}
	if got := len(f.socketPaths()); got != 1 {
		t.Errorf("opened %d sockets, want 1 — the fenced volume was re-served", got)
	}

	// A higher epoch is the Control Plane granting the volume again, and it is the one
	// thing that clears the fencing.
	regranted := &storagev1.DesiredVolume{
		VolumeId: v.GetVolumeId(), SizeBytes: v.GetSizeBytes(),
		BlockSize: v.GetBlockSize(), Epoch: 2,
		State: v.GetState(),
	}
	if err := m.Apply(ctx, []*storagev1.DesiredVolume{regranted}); err != nil {
		t.Fatalf("Apply at the new epoch: %v", err)
	}
	vols, err = m.Volumes(ctx)
	if err != nil {
		t.Fatalf("Volumes: %v", err)
	}
	if len(vols) != 1 || vols[0].Epoch != 2 {
		t.Fatalf("a re-granted volume was not served again: %+v", vols)
	}
}

// TestAFencedVolumeIsForgottenOnceItLeavesTheDesiredState is the debt C8's guard took
// on, paid where it was taken on.
//
// Before the guard a detach reached this host as a shrinking desired state and was
// carried out by Apply, which sets no fencing memory. Now the empty half of that message
// stops nothing, so the detach arrives through the report the Control Plane refuses, and
// `Loop.fence` → `Fence` *does* record the epoch this host was fenced out of.
// `controlplane.Place` re-places a volume without bumping its epoch, so a
// `-detach-volume X` followed by an `-attach-volume X` naming this same host again would
// arrive at exactly the remembered epoch and be skipped by the check in Apply — for as
// long as the process lives, with no error printed and a guest whose device never comes
// back.
//
// The memory only ever needed to outlast a desired state that *still lists* the volume
// (DEV-0012, and TestAFencedVolumeDoesNotComeBackAtTheSameEpoch is that case); once the
// Control Plane has stopped listing it there is nothing left for it to outlast, because
// the Control Plane cannot list the volume for this host again without having made this
// host its writer again.
//
// The middle desired state here names another volume rather than nothing, so this proves
// the forgetting on its own — a version that only forgot on the empty list would pass an
// assertion written against the empty one and still strand every real re-attach.
func TestAFencedVolumeIsForgottenOnceItLeavesTheDesiredState(t *testing.T) {
	t.Parallel()
	m, f, _ := newTestManager(t)
	ctx := t.Context()

	v, other := desiredVolume(t, 1), desiredVolume(t, 1)
	if err := m.Apply(ctx, []*storagev1.DesiredVolume{v, other}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := m.Fence(ctx, []string{v.GetVolumeId()}, storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED, ""); err != nil {
		t.Fatalf("Fence: %v", err)
	}

	// The detach lands in the catalog: the Control Plane stops listing the volume for
	// this host, while still listing the other one, so this is not the empty-list path.
	if err := m.Apply(ctx, []*storagev1.DesiredVolume{other}); err != nil {
		t.Fatalf("Apply without the detached volume: %v", err)
	}

	// And the operator attaches it back here. Place does not bump the epoch, so this is
	// the same number the fencing remembered.
	if err := m.Apply(ctx, []*storagev1.DesiredVolume{v, other}); err != nil {
		t.Fatalf("Apply re-attaching the volume: %v", err)
	}

	if _, ok := m.Device(v.GetVolumeId()); !ok {
		t.Fatalf("volume %s has no device after being detached and attached back to this host: the fencing memory outlived the placement that caused it, and this guest's device never comes back",
			v.GetVolumeId())
	}
	// And the socket a guest reconnects to was opened a second time. The listener the
	// fencing closed is gone; asserting on its close count would pass on a manager that
	// never re-opened anything, which is the state this test exists to catch.
	socket := socketFor(t, f, v.GetVolumeId())
	if got := countPaths(f, socket); got != 2 {
		t.Errorf("%s was opened %d time(s), want 2 — once before the fencing and once for the re-attached volume", socket, got)
	}
	if got := f.listenerFor(socket).closeCount(); got != 0 {
		t.Errorf("the re-attached volume's listener is already closed (%d time(s)): nothing is listening for the guest", got)
	}
}

// countPaths is how many times a socket was opened. socketPaths() is an append-only
// record of every listen, so a path that appears twice was served, torn down and served
// again — which is the difference between "still running from before" and "re-attached".
func countPaths(f *listenerFactory, socket string) int {
	n := 0
	for _, p := range f.socketPaths() {
		if p == socket {
			n++
		}
	}
	return n
}

// TestReconcileServesWhatTheControlPlaneAsksFor is the keystone's point, end to end
// through the loop: readDesiredState used to assign a field nothing read. One cycle
// against a Control Plane that lists a volume must leave that volume actually served —
// and the report that goes back must carry it, not the empty set the binary reported
// forever.
func TestReconcileServesWhatTheControlPlaneAsksFor(t *testing.T) {
	t.Parallel()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	cp := newFakeCP(clk)
	m, f, _ := newTestManager(t)

	v := desiredVolume(t, 4)
	cp.setDesired([]*storagev1.DesiredVolume{v})

	loop, err := agent.New(testConfig(), agent.Deps{
		Clock:        clk,
		ControlPlane: cp,
		Device:       fakeDevice{},
		Volumes:      m,
	})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	if err := loop.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got := f.socketPaths(); len(got) != 1 {
		t.Fatalf("the cycle opened %d sockets, want 1: the desired state was recorded, not served", len(got))
	}
	reports := cp.lastReport(t).GetVolumes()
	if len(reports) != 1 || reports[0].GetVolumeId() != v.GetVolumeId() {
		t.Fatalf("reported %v, want the volume that was just started", reports)
	}
	if reports[0].GetEpoch() != 4 {
		t.Errorf("reported epoch %d, want 4 — every watermark is qualified by it (§12.3)", reports[0].GetEpoch())
	}
}

// TestApplyStartsARuntimePerVolume is the keystone's first claim: something now holds
// per-volume state, and it came from the desired state rather than from a test fixture.
func TestApplyStartsARuntimePerVolume(t *testing.T) {
	t.Parallel()
	m, f, _ := newTestManager(t)
	ctx := t.Context()

	a, b := desiredVolume(t, 1), desiredVolume(t, 1)
	if err := m.Apply(ctx, []*storagev1.DesiredVolume{a, b}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	vols, err := m.Volumes(ctx)
	if err != nil {
		t.Fatalf("Volumes: %v", err)
	}
	if len(vols) != 2 {
		t.Fatalf("got %d volumes, want 2", len(vols))
	}
	// Volumes is a VolumeSource and must be deterministic (INV-02).
	if vols[0].VolumeID > vols[1].VolumeID {
		t.Errorf("volumes are not ordered by id: %q then %q", vols[0].VolumeID, vols[1].VolumeID)
	}
	for _, v := range vols {
		if v.Epoch != 1 {
			t.Errorf("volume %s reports epoch %d, want 1", v.VolumeID, v.Epoch)
		}
	}
	if got := len(f.socketPaths()); got != 2 {
		t.Errorf("opened %d sockets, want one per volume", got)
	}
}

// TestSocketAndWALPathsArePerVolumeAndEpoch pins the two on-disk conventions. The WAL
// root carries the epoch because a promoted writer must not append into the previous
// epoch's segments; the socket does not, because it is the guest's attachment point and
// survives a promotion.
func TestSocketAndWALPathsArePerVolumeAndEpoch(t *testing.T) {
	t.Parallel()
	m, f, d := newTestManager(t)
	ctx := t.Context()

	v := desiredVolume(t, 7)
	if err := m.Apply(ctx, []*storagev1.DesiredVolume{v}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	wantSocket := path.Join("/run/spin", v.GetVolumeId()+".sock")
	if got := f.socketPaths(); len(got) != 1 || got[0] != wantSocket {
		t.Errorf("socket paths = %v, want [%s]", got, wantSocket)
	}

	// The WAL root has to be visible on the disk, not merely computed: a runtime that
	// picked the right path and never opened a log would pass a string comparison.
	// A log creates nothing until something is written to it, so this writes first —
	// which also means the assertion is about the path a guest's bytes really land in.
	//
	// The segment's *directory* is compared exactly. An earlier version of this test
	// only checked that something existed under the prefix, and List matches by prefix
	// — so it passed happily while every segment was landing in a doubled
	// .../wal/<id>/<epoch>/<id>/<epoch>, which is what wal.SegmentDir does to a root
	// that is already namespaced.
	writeOneBlock(t, m, v.GetVolumeId())
	u, err := ids.Parse(v.GetVolumeId())
	if err != nil {
		t.Fatal(err)
	}
	wantDir := wal.SegmentDir(path.Join("/var/lib/spin", "wal"), [16]byte(u), 7)
	names, err := d.List(path.Join("/var/lib/spin", "wal"))
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(names) == 0 {
		t.Fatalf("no WAL segment anywhere under /var/lib/spin/wal")
	}
	for _, n := range names {
		if got := path.Dir(n); got != wantDir {
			t.Errorf("segment %s sits in %s, want exactly %s", n, got, wantDir)
		}
	}
}

// TestApplyIsIdempotent: the desired state is re-read every few seconds, and a runtime
// that restarted on each cycle would tear down a guest's device for no reason.
func TestApplyIsIdempotent(t *testing.T) {
	t.Parallel()
	m, f, _ := newTestManager(t)
	ctx := t.Context()

	v := desiredVolume(t, 1)
	desired := []*storagev1.DesiredVolume{v}
	if err := m.Apply(ctx, desired); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	for range 3 {
		if err := m.Apply(ctx, desired); err != nil {
			t.Fatalf("repeated Apply: %v", err)
		}
	}
	if got := len(f.socketPaths()); got != 1 {
		t.Errorf("opened %d sockets across 4 applies, want 1 — the runtime was restarted", got)
	}
}

// TestVolumeLeavingTheDesiredStateIsStopped is the other half of the diff. A volume the
// Control Plane no longer lists for this host has been promoted away, detached or
// fenced; in every case this host must stop serving it and let go of its socket.
//
// The second desired state is *not* empty, and the difference is the whole of C8: an
// empty list is what a Control Plane sends when it has lost its catalog as well as when
// it has taken every volume away, so Apply stops nothing on it. A list that still names
// another volume is proof the Control Plane is deciding volume by volume, and the
// absence in it is then a decision about the volume that is missing.
func TestVolumeLeavingTheDesiredStateIsStopped(t *testing.T) {
	t.Parallel()
	m, f, _ := newTestManager(t)
	ctx := t.Context()

	leaving, staying := desiredVolume(t, 1), desiredVolume(t, 1)
	if err := m.Apply(ctx, []*storagev1.DesiredVolume{leaving, staying}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	socket := socketFor(t, f, leaving.GetVolumeId())

	if err := m.Apply(ctx, []*storagev1.DesiredVolume{staying}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	vols, err := m.Volumes(ctx)
	if err != nil {
		t.Fatalf("Volumes: %v", err)
	}
	if len(vols) != 1 || vols[0].VolumeID != staying.GetVolumeId() {
		t.Fatalf("serving %+v after %s left the desired state, want only %s",
			vols, leaving.GetVolumeId(), staying.GetVolumeId())
	}
	if got := f.listenerFor(socket).closeCount(); got == 0 {
		t.Error("the listener was never closed: the socket outlives the volume")
	}
}

// TestAnEmptyDesiredStateStopsNothing is C8's unit arm. `Apply` used to stop every
// volume the desired state did not list, which is right for a volume missing from a
// list that names others and catastrophic for a list that names none: a Control Plane
// restarted against an empty database, a GetDesiredState that returns no rows because
// of a bug, or an Agent whose host id stopped matching after a config change all send
// exactly that, and under ADR-0026 stopping a volume is what publishes it — so the host
// would upload every session it holds and take every guest's device away, on the
// strength of a message that names nothing.
//
// The e2e arm is where this is proven against the binaries
// (integration/e2e/desired_test.go); this one pins the rule at the type, including the
// case that makes the rule a rule rather than a special case for nil.
func TestAnEmptyDesiredStateStopsNothing(t *testing.T) {
	tests := []struct {
		name    string
		desired func(t *testing.T) []*storagev1.DesiredVolume
	}{
		{
			name:    "nil",
			desired: func(*testing.T) []*storagev1.DesiredVolume { return nil },
		},
		{
			name:    "empty",
			desired: func(*testing.T) []*storagev1.DesiredVolume { return []*storagev1.DesiredVolume{} },
		},
		{
			// A list whose every entry this host refused named nothing usable either,
			// and treating it as an inventory would stop the volumes on the strength of
			// what Apply had just rejected.
			name: "only entries this host cannot parse",
			desired: func(*testing.T) []*storagev1.DesiredVolume {
				return []*storagev1.DesiredVolume{{VolumeId: "not-a-uuid", SizeBytes: testVolumeSize, BlockSize: testBlockSize}}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m, f, _ := newTestManager(t)
			ctx := t.Context()

			v := desiredVolume(t, 1)
			if err := m.Apply(ctx, []*storagev1.DesiredVolume{v}); err != nil {
				t.Fatalf("Apply: %v", err)
			}
			socket := socketFor(t, f, v.GetVolumeId())

			// The error is deliberately not asserted on: an unparseable entry is
			// reported as one, and a gate that returns the right error while doing the
			// wrong thing satisfies any assertion on err. What is asserted is the
			// device the guest is served from.
			_ = m.Apply(ctx, tc.desired(t))

			vols, err := m.Volumes(ctx)
			if err != nil {
				t.Fatalf("Volumes: %v", err)
			}
			if len(vols) != 1 || vols[0].VolumeID != v.GetVolumeId() {
				t.Fatalf("serving %+v after a desired state that named nothing, want volume %s still served",
					vols, v.GetVolumeId())
			}
			if _, ok := m.Device(v.GetVolumeId()); !ok {
				t.Error("the guest's device is gone: an empty desired state was obeyed as a detach order")
			}
			if got := f.listenerFor(socket).closeCount(); got != 0 {
				t.Errorf("the socket was closed %d time(s): the guest lost its device to a message that named no volume", got)
			}
		})
	}
}

// socketFor is the socket a volume was opened on. Tests that start more than one volume
// cannot index socketPaths() — Apply walks the desired state in order, but a test that
// later reorders it would silently assert about the wrong volume.
func socketFor(t *testing.T, f *listenerFactory, volumeID string) string {
	t.Helper()
	for _, p := range f.socketPaths() {
		if path.Base(p) == volumeID+".sock" {
			return p
		}
	}
	t.Fatalf("no socket was opened for volume %s; opened %v", volumeID, f.socketPaths())
	return ""
}

// TestEpochChangeReplacesTheRuntime. An epoch bump means this host was granted the
// volume again after a fencing round, and the WAL root it must append to changed with
// it. Reusing the runtime would keep writing into the previous epoch's segments, which
// is the one thing §12.3's qualification of every report exists to prevent.
func TestEpochChangeReplacesTheRuntime(t *testing.T) {
	t.Parallel()
	m, f, d := newTestManager(t)
	ctx := t.Context()

	v := desiredVolume(t, 1)
	if err := m.Apply(ctx, []*storagev1.DesiredVolume{v}); err != nil {
		t.Fatalf("Apply(epoch 1): %v", err)
	}

	promoted := &storagev1.DesiredVolume{
		VolumeId: v.GetVolumeId(), SizeBytes: v.GetSizeBytes(),
		BlockSize: v.GetBlockSize(), Epoch: 2,
		State: v.GetState(),
	}
	if err := m.Apply(ctx, []*storagev1.DesiredVolume{promoted}); err != nil {
		t.Fatalf("Apply(epoch 2): %v", err)
	}

	vols, err := m.Volumes(ctx)
	if err != nil {
		t.Fatalf("Volumes: %v", err)
	}
	if len(vols) != 1 || vols[0].Epoch != 2 {
		t.Fatalf("after promotion the manager reports %+v, want one volume at epoch 2", vols)
	}
	if got := len(f.socketPaths()); got != 2 {
		t.Errorf("opened %d sockets, want 2 — the runtime was not replaced", got)
	}
	writeOneBlock(t, m, v.GetVolumeId())
	u, err := ids.Parse(v.GetVolumeId())
	if err != nil {
		t.Fatal(err)
	}
	root := wal.SegmentDir(path.Join("/var/lib/spin", "wal"), [16]byte(u), 2)
	names, err := d.List(root)
	if err != nil || len(names) == 0 {
		t.Errorf("no WAL under the new epoch's directory %s (err=%v)", root, err)
	}
}

// TestApplyReportsAFailureAndLeavesNothingHalfStarted. A socket that cannot be opened
// is an ordinary operational failure (a stale file, a missing directory), and the loop
// retries. What must not happen is a half-built runtime surviving it: a log open on a
// volume nothing serves would hold the WAL and be invisible to Volumes.
func TestApplyReportsAFailureAndLeavesNothingHalfStarted(t *testing.T) {
	t.Parallel()
	m, f, _ := newTestManager(t)
	ctx := t.Context()

	f.err = errors.New("address already in use")
	v := desiredVolume(t, 1)
	err := m.Apply(ctx, []*storagev1.DesiredVolume{v})
	if err == nil {
		t.Fatal("Apply succeeded with a listener that cannot be opened")
	}
	if !errors.Is(err, f.err) {
		t.Errorf("error %v does not carry the listener's failure", err)
	}

	if _, ok := m.Device(v.GetVolumeId()); ok {
		t.Error("a half-started runtime survived the failure: the volume still has a device")
	}

	// It is reported, and this is the second half of the same rule. Nothing must be
	// *running*, but the volume is still in the desired state, so it must still be on
	// the wire — with a refusal on it. Until it was, a volume that could not start
	// disappeared from the report entirely, and the fleet cannot tell an absent volume
	// from one it never placed here: the catalog kept the watermarks of whatever last
	// worked, and -fleet-status printed a healthy row for a volume with no runtime.
	vols, verr := m.Volumes(ctx)
	if verr != nil {
		t.Fatalf("Volumes: %v", verr)
	}
	if len(vols) != 1 || vols[0].VolumeID != v.GetVolumeId() {
		t.Fatalf("a volume that could not start is reported as %+v; the fleet has to be told about it", vols)
	}
	if got := vols[0].Refusal; got != storagev1.VolumeRefusal_VOLUME_REFUSAL_ATTACH_FAILED {
		t.Errorf("refusal = %s, want ATTACH_FAILED", got)
	}
	if got := vols[0].Epoch; got != v.GetEpoch() {
		t.Errorf("refusal reported under epoch %d, want %d — the Control Plane refuses any other", got, v.GetEpoch())
	}
	if !strings.Contains(vols[0].RefusalDetail, f.err.Error()) {
		t.Errorf("refusal detail = %q, which does not say what went wrong", vols[0].RefusalDetail)
	}
	if vols[0].LocalSequence != 0 || vols[0].DurableSequence != 0 || vols[0].PublishedSequence != 0 {
		t.Errorf("a volume that never opened reports watermarks %+v; it has observed nothing", vols[0])
	}
}

// TestServeIsSupervised. vhost.Server.Serve returns on any session error, and the
// device must come back — a guest reconnects to the socket. Without a supervisor the
// first protocol error ends the volume's service for the lifetime of the process, and
// nothing reports it.
func TestServeIsSupervised(t *testing.T) {
	t.Parallel()
	m, f, _ := newTestManager(t)
	ctx := t.Context()

	v := desiredVolume(t, 1)
	if err := m.Apply(ctx, []*storagev1.DesiredVolume{v}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	socket := f.socketPaths()[0]
	<-f.listened // the open Apply did

	// Closing the listener under the server is what a session error looks like from
	// Serve's side: Accept returns and Serve gives up. The supervisor must open a new
	// listener rather than leave the volume unserved.
	_ = f.listenerFor(socket).Close()

	// No deadline of its own: t.Context() is cancelled when the test ends, so a
	// supervisor that never re-listens fails at the suite's -timeout with a stack that
	// says where it is stuck, and nothing here reads a wall clock (INV-01).
	select {
	case <-f.listened:
	case <-t.Context().Done():
		t.Fatal("the serve loop died and was never restarted")
	}
}

// TestCloseStopsEverything: a shutting-down Agent must not leave a WAL open or a socket
// bound, or the next start finds its own leftovers.
func TestCloseStopsEverything(t *testing.T) {
	t.Parallel()
	m, f, _ := newTestManager(t)
	ctx := t.Context()

	if err := m.Apply(ctx, []*storagev1.DesiredVolume{desiredVolume(t, 1), desiredVolume(t, 1)}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := m.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, s := range f.socketPaths() {
		if f.listenerFor(s).closeCount() == 0 {
			t.Errorf("listener %s was left open", s)
		}
	}
	vols, err := m.Volumes(ctx)
	if err != nil {
		t.Fatalf("Volumes after Close: %v", err)
	}
	if len(vols) != 0 {
		t.Errorf("Close left %d volumes behind", len(vols))
	}
}

// TestWatermarksComeFromTheLog is the piece that makes every heartbeat honest. The
// binary reported an empty VolumeSet forever: remote_backlog=0 and zero reports, which
// the Control Plane cannot distinguish from a host with nothing to say.
func TestWatermarksComeFromTheLog(t *testing.T) {
	t.Parallel()
	m, _, _ := newTestManager(t)
	ctx := t.Context()

	v := desiredVolume(t, 3)
	if err := m.Apply(ctx, []*storagev1.DesiredVolume{v}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Write through the device the guest would be served from, so the watermark that
	// moves is the one a real WRITE moves.
	dev, ok := m.Device(v.GetVolumeId())
	if !ok {
		t.Fatal("no device for a volume that was just started")
	}
	if _, err := dev.WriteAt(make([]byte, testBlockSize), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	vols, err := m.Volumes(ctx)
	if err != nil {
		t.Fatalf("Volumes: %v", err)
	}
	if len(vols) != 1 {
		t.Fatalf("got %d volumes, want 1", len(vols))
	}
	if vols[0].LocalSequence == 0 {
		t.Error("local sequence is still 0 after a WRITE: the status is not the log's")
	}
	if vols[0].DurableSequence != 0 {
		t.Errorf("durable sequence is %d with no FLUSH: a WRITE must make no durability claim (§5.3)",
			vols[0].DurableSequence)
	}
}

// TestUnusableVolumeIsRefused. Size and block size come from the Control Plane, and a
// value the device cannot express must fail on the volume rather than at the first
// guest request — by which time the guest has a device it cannot use.
func TestUnusableVolumeIsRefused(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		mut  func(*storagev1.DesiredVolume)
	}{
		{"no capacity", func(v *storagev1.DesiredVolume) { v.SizeBytes = 0 }},
		{"a partial trailing sector", func(v *storagev1.DesiredVolume) { v.SizeBytes = testVolumeSize + 1 }},
		{"no volume id", func(v *storagev1.DesiredVolume) { v.VolumeId = "" }},
		{"a volume id that is not a UUID", func(v *storagev1.DesiredVolume) { v.VolumeId = "volume-1" }},
		{"a negative epoch", func(v *storagev1.DesiredVolume) { v.Epoch = -1 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m, _, _ := newTestManager(t)
			v := desiredVolume(t, 1)
			tc.mut(v)
			if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{v}); err == nil {
				t.Fatal("Apply accepted a volume it cannot serve")
			}
		})
	}
}

// TestARestartedVolumeReadsBackWhatWasFlushed is the whole point of resuming, measured
// where an operator would feel it. Before this, an Agent restart served zeros for data
// that had been written, FLUSHed and verified in the object store — silently, because
// the manager built a fresh wal.NewLog over a directory it never looked at.
func TestARestartedVolumeReadsBackWhatWasFlushed(t *testing.T) {
	t.Parallel()
	d, store := sim.NewDisk(), sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	v := desiredVolume(t, 1)
	desired := []*storagev1.DesiredVolume{v}

	newManager := func() *agent.VolumeManager {
		f := newListenerFactory()
		m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
			DataDir: "/var/lib/spin", SocketDir: "/run/spin",
			Budget: testBudget(),
		}, agent.VolumeManagerDeps{
			Clock: clk, Disk: d, Listen: f.listen,
			Mapper: unusedMapper{}, EventFD: unusedEventFD,
			Store: store,
		})
		if err != nil {
			t.Fatalf("NewVolumeManager: %v", err)
		}
		return m
	}

	payload := bytes.Repeat([]byte{0xAB}, testBlockSize)

	first := newManager()
	if err := first.Apply(t.Context(), desired); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	dev, ok := first.Device(v.GetVolumeId())
	if !ok {
		t.Fatal("no device")
	}
	if _, err := dev.WriteAt(payload, 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := dev.Flush(t.Context()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := first.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The restart: a new manager, the same disk and store, the same desired state.
	second := newManager()
	t.Cleanup(func() { _ = second.Close(context.Background()) }) //nolint:usetesting // a cancelled context abandons the publish; see newTestManager
	if err := second.Apply(t.Context(), desired); err != nil {
		t.Fatalf("Apply after restart: %v", err)
	}
	dev2, ok := second.Device(v.GetVolumeId())
	if !ok {
		t.Fatal("no device after restart")
	}

	got := make([]byte, len(payload))
	if _, err := dev2.ReadAt(got, 0); err != nil {
		t.Fatalf("read after restart: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("read %x after restart, want %x — flushed, verified data was lost", got[:8], payload[:8])
	}

	// And it can be written to. Before this the next append refused outright, because
	// a fresh log will not open over segments it did not replay.
	if _, err := dev2.WriteAt(bytes.Repeat([]byte{0xCD}, testBlockSize), testBlockSize); err != nil {
		t.Fatalf("write after restart: %v", err)
	}
}

// TestARestartedVolumeRefusesToReadWhenTheStoreIsGone is decision 3 where it lands: the
// base cannot be built, so the volume refuses rather than answering with the zeros it
// happens to hold.
func TestARestartedVolumeRefusesToReadWhenTheStoreIsGone(t *testing.T) {
	t.Parallel()
	d := sim.NewDisk()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	v := desiredVolume(t, 1)
	desired := []*storagev1.DesiredVolume{v}

	f := newListenerFactory()
	first, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin", SocketDir: "/run/spin",
		Budget: testBudget(),
	}, agent.VolumeManagerDeps{
		Clock: clk, Disk: d, Listen: f.listen,
		Mapper: unusedMapper{}, EventFD: unusedEventFD,
	})
	if err != nil {
		t.Fatalf("NewVolumeManager: %v", err)
	}
	if err := first.Apply(t.Context(), desired); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	writeOneBlock(t, first, v.GetVolumeId())
	if err := first.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The restart, with a store that fails every request.
	f2 := newListenerFactory()
	second, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin", SocketDir: "/run/spin",
		Budget: testBudget(),
	}, agent.VolumeManagerDeps{
		Clock: clk, Disk: d, Listen: f2.listen,
		Mapper: unusedMapper{}, EventFD: unusedEventFD,
		Store: newUnreachableStore(),
	})
	if err != nil {
		t.Fatalf("NewVolumeManager: %v", err)
	}
	t.Cleanup(func() { _ = second.Close(context.Background()) }) //nolint:usetesting // a cancelled context abandons the publish; see newTestManager
	if err := second.Apply(t.Context(), desired); err != nil {
		t.Fatalf("Apply after restart: %v", err)
	}
	dev, ok := second.Device(v.GetVolumeId())
	if !ok {
		t.Fatal("no device after restart")
	}
	if _, err := dev.ReadAt(make([]byte, testBlockSize), 0); err == nil {
		t.Fatal("a read was answered with no recoverable base: the volume served zeros")
	}
}

// unreachableStore is an object store that answers nothing, which is what a base that
// cannot be loaded looks like from the Agent's side. It is backed by a real one so every
// method it does not override still exists — a nil embedded interface would panic instead
// of failing, and a panic is not the behaviour under test.
//
// **Every read method is overridden, and that is the point.** It used to override only
// List and Get, the two the recovery path called. When the boot path became image.Load —
// which asks Head first — the double silently stopped modelling anything: Head fell
// through to the real store, answered ErrNotFound, and the Agent read that as "this
// volume has no image yet", installed an empty base and served the guest zeros. This test
// went red and is the only reason it was noticed, which makes it the fifth assertion in
// this repository that proved nothing until something moved underneath it.
type unreachableStore struct{ objectstore.Store }

func newUnreachableStore() unreachableStore { return unreachableStore{Store: sim.NewObjectStore()} }

var errStoreUnreachable = errors.New("the object store is unreachable")

func (unreachableStore) List(context.Context, string) ([]objectstore.ObjectInfo, error) {
	return nil, errStoreUnreachable
}

func (unreachableStore) Get(context.Context, string) ([]byte, error) {
	return nil, errStoreUnreachable
}

func (unreachableStore) Head(context.Context, string) (objectstore.ObjectInfo, error) {
	return objectstore.ObjectInfo{}, errStoreUnreachable
}
