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

func newTestManager(t *testing.T) (*agent.VolumeManager, *listenerFactory, *sim.Disk) {
	t.Helper()
	d := sim.NewDisk()
	f := newListenerFactory()
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir:   "/var/lib/spin",
		SocketDir: "/run/spin",
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
	t.Cleanup(func() { _ = m.Close() })
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

// remoteManager is a manager in remote mode: a real object store and a lease whose
// answer the test controls, so a FLUSH goes down the §14.4 path.
func remoteManager(t *testing.T, lease func() bool) (*agent.VolumeManager, *listenerFactory) {
	t.Helper()
	f := newListenerFactory()
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir:   "/var/lib/spin",
		SocketDir: "/run/spin",
	}, agent.VolumeManagerDeps{
		Clock:   sim.NewClock(time.Unix(1_700_000_000, 0).UTC()),
		Disk:    sim.NewDisk(),
		Listen:  f.listen,
		Mapper:  unusedMapper{},
		EventFD: unusedEventFD,
		Store:   sim.NewObjectStore(),
		Lease:   lease,
	})
	if err != nil {
		t.Fatalf("NewVolumeManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m, f
}

// TestAStoreWithoutALeaseIsRefused. wal.EnableRemote accepts a nil lease without
// complaining and the failure surfaces at the first FLUSH, inside the guest's I/O path,
// as ErrNoLease. A writer with an uploader and nothing fencing it is not a
// configuration worth starting.
func TestAStoreWithoutALeaseIsRefused(t *testing.T) {
	t.Parallel()
	f := newListenerFactory()
	_, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin", SocketDir: "/run/spin",
	}, agent.VolumeManagerDeps{
		Clock:   sim.NewClock(time.Unix(1_700_000_000, 0).UTC()),
		Disk:    sim.NewDisk(),
		Listen:  f.listen,
		Mapper:  unusedMapper{},
		EventFD: unusedEventFD,
		Store:   sim.NewObjectStore(),
	})
	if err == nil {
		t.Fatal("a manager was built with an object store and no lease to gate its ACKs")
	}
}

// TestTheLeaseIsResolvedOnEveryAck is the trap this adapter exists for, stated as a
// test: the answer must be re-read, never captured. Loop.applyLease allocates a *new*
// lease.Manager whenever the Control Plane changes the TTL, so a Log holding the old
// object would be gated by one nobody renews — invalid at the old TTL, never valid
// again, self-fencing a host that is perfectly healthy.
//
// Flipping the answer between two FLUSHes is what a captured lease could not survive.
func TestTheLeaseIsResolvedOnEveryAck(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	valid := true
	m, _ := remoteManager(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return valid
	})

	v := desiredVolume(t, 1)
	if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{v}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	dev, ok := m.Device(v.GetVolumeId())
	if !ok {
		t.Fatal("no device")
	}
	if _, err := dev.WriteAt(make([]byte, testBlockSize), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := dev.Flush(t.Context()); err != nil {
		t.Fatalf("the first FLUSH, with a valid lease: %v", err)
	}

	mu.Lock()
	valid = false
	mu.Unlock()

	if _, err := dev.WriteAt(make([]byte, testBlockSize), testBlockSize); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	err := dev.Flush(t.Context())
	if err == nil {
		t.Fatal("a FLUSH was ACKed after the lease stopped being valid (INV-06)")
	}
	if !strings.Contains(err.Error(), "authority") {
		t.Errorf("the refusal does not read as a fencing one: %v", err)
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

	if err := m.Fence(ctx, []string{v.GetVolumeId()}); err != nil {
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
	if err := m.Fence(ctx, []string{v.GetVolumeId()}); err != nil {
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
		BlockSize: v.GetBlockSize(), Epoch: 2, State: v.GetState(),
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
func TestVolumeLeavingTheDesiredStateIsStopped(t *testing.T) {
	t.Parallel()
	m, f, _ := newTestManager(t)
	ctx := t.Context()

	v := desiredVolume(t, 1)
	if err := m.Apply(ctx, []*storagev1.DesiredVolume{v}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	socket := f.socketPaths()[0]

	if err := m.Apply(ctx, nil); err != nil {
		t.Fatalf("Apply(nil): %v", err)
	}

	vols, err := m.Volumes(ctx)
	if err != nil {
		t.Fatalf("Volumes: %v", err)
	}
	if len(vols) != 0 {
		t.Fatalf("still serving %d volumes after they left the desired state", len(vols))
	}
	if got := f.listenerFor(socket).closeCount(); got == 0 {
		t.Error("the listener was never closed: the socket outlives the volume")
	}
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
		BlockSize: v.GetBlockSize(), Epoch: 2, State: v.GetState(),
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

	vols, verr := m.Volumes(ctx)
	if verr != nil {
		t.Fatalf("Volumes: %v", verr)
	}
	if len(vols) != 0 {
		t.Errorf("a half-started runtime survived the failure: %+v", vols)
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
	if err := m.Close(); err != nil {
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
		}, agent.VolumeManagerDeps{
			Clock: clk, Disk: d, Listen: f.listen,
			Mapper: unusedMapper{}, EventFD: unusedEventFD,
			Store: store, Lease: func() bool { return true },
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
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The restart: a new manager, the same disk and store, the same desired state.
	second := newManager()
	t.Cleanup(func() { _ = second.Close() })
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
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The restart, with a store that fails every request.
	f2 := newListenerFactory()
	second, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin", SocketDir: "/run/spin",
	}, agent.VolumeManagerDeps{
		Clock: clk, Disk: d, Listen: f2.listen,
		Mapper: unusedMapper{}, EventFD: unusedEventFD,
		Store: newUnreachableStore(), Lease: func() bool { return true },
	})
	if err != nil {
		t.Fatalf("NewVolumeManager: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
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
// cannot be recovered looks like from the Agent's side. It is backed by a real one so
// every method it does not override still exists — a nil embedded interface would panic
// instead of failing, and a panic is not the behaviour under test.
type unreachableStore struct{ objectstore.Store }

func newUnreachableStore() unreachableStore { return unreachableStore{Store: sim.NewObjectStore()} }

var errStoreUnreachable = errors.New("the object store is unreachable")

func (unreachableStore) List(context.Context, string) ([]objectstore.ObjectInfo, error) {
	return nil, errStoreUnreachable
}

func (unreachableStore) Get(context.Context, string) ([]byte, error) {
	return nil, errStoreUnreachable
}
