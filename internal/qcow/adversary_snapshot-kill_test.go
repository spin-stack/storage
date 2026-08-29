package qcow_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/qcow"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// The rotation seam, killed at the two points either side of the QMP command.
//
// qcow.rotate does four things in an order that cannot be rearranged: create the overlay,
// move `active/current` onto it, tell QEMU to switch, record what was sealed. The pointer
// is written *before* the switch on purpose, so every kill in this window leaves a pointer
// that ran ahead of the guest — and the only thing that can say whether it ran ahead by a
// layer QEMU reached or by one it never did is QEMU itself.
//
// The third kill in the list — after blockdev-snapshot-sync returned, before anything was
// recorded — is already covered by
// TestAdversaryASealedLayerNobodyRecordedIsDroppedFromTheHistory, which fails the write of
// state.json at exactly that instant. These two are the kills before and during.

// preSnapshotDialer is a QMP endpoint that answers everything up to the rotation and then
// loses the connection at the moment the snapshot command is *written* — the bytes never
// leave this host, which is what a kill before the command is issued looks like from
// QEMU's side. `arm` is what turns it on, so the same dialer serves the cycles either side.
type preSnapshotDialer struct {
	script []string
	arm    bool
	sent   strings.Builder
}

func (d *preSnapshotDialer) Dial(context.Context, string) (io.ReadWriteCloser, error) {
	if len(d.script) == 0 {
		return nil, errors.New("connect: no such file or directory")
	}
	return &preSnapshotConn{d: d, r: strings.NewReader(strings.Join(d.script, "\n") + "\n")}, nil
}

type preSnapshotConn struct {
	d *preSnapshotDialer
	r io.Reader
}

func (c *preSnapshotConn) Read(p []byte) (int, error) { return c.r.Read(p) }

func (c *preSnapshotConn) Write(p []byte) (int, error) {
	if c.d.arm && bytes.Contains(p, []byte("blockdev-snapshot-sync")) {
		return 0, errors.New("write unix @qmp: broken pipe")
	}
	return c.d.sent.Write(p)
}

func (*preSnapshotConn) Close() error { return nil }

// preSnapshotAgent is an Agent whose QMP endpoint is the dialer above. It is built here
// rather than from the shared harness because that one fixes the dialer, and this boundary
// is about what QEMU was and was not told.
type preSnapshotAgent struct {
	m      *qcow.Manager
	runner *fakeRunner
	paths  *fakePaths
	dialer *preSnapshotDialer
	pub    *recordingPublisher
}

func newPreSnapshotAgent(t *testing.T, rotateAt int64) *preSnapshotAgent {
	t.Helper()
	a := &preSnapshotAgent{
		runner: &fakeRunner{info: infoJSON("qcow2", size, false), version: "qemu-img version 11.1.1"},
		paths:  newPaths(),
		dialer: &preSnapshotDialer{},
		pub:    &recordingPublisher{},
	}
	a.start(t, rotateAt)
	return a
}

func (a *preSnapshotAgent) start(t *testing.T, rotateAt int64) {
	t.Helper()
	m, err := qcow.New(t.Context(), qcow.Config{
		Root: root, QemuImg: "/qemu-img", ProbeTimeout: time.Second, RotateAtBytes: rotateAt,
	}, qcow.Deps{
		Clock: sim.NewClock(time.Unix(0, 0)), Disk: sim.NewDisk(),
		Runner: a.runner, Paths: a.paths, Dialer: a.dialer, Recovery: bornEmpty(),
		Publisher: a.pub,
	})
	if err != nil {
		t.Fatalf("building a manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	a.runner.reset()
	a.m = m
}

// kill is the next Agent on the same data directory: this process's memory is gone, the
// filesystem is what it left behind, and the publisher is fresh so that what is offered
// after the kill can be told from what was offered before it.
func (a *preSnapshotAgent) kill(t *testing.T, rotateAt int64) {
	t.Helper()
	if err := a.m.Close(); err != nil {
		t.Fatalf("closing the Agent that is being killed: %v", err)
	}
	a.pub = &recordingPublisher{}
	a.start(t, rotateAt)
}

func (a *preSnapshotAgent) apply(t *testing.T) error {
	t.Helper()
	return a.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)})
}

func (a *preSnapshotAgent) tip(t *testing.T) string {
	t.Helper()
	body, err := a.paths.ReadFile(qcow.ActivePointer(root, vol))
	if err != nil {
		t.Fatalf("reading the pointer: %v", err)
	}
	return string(body)
}

// guestOn attaches a VM to `image`, says how large that layer has grown, and tells the
// fake qemu-img what the overlay a rotation is about to create will look like.
func (a *preSnapshotAgent) guestOn(image string, grown int64) {
	a.dialer.script = attachedTo(image)
	a.paths.sizes[image] = grown
	a.runner.info = overlayJSON(size, image)
	a.runner.reset()
}

func (a *preSnapshotAgent) offered() []string {
	out := make([]string, 0, len(a.pub.got))
	for _, l := range a.pub.got {
		out = append(out, l.LayerID)
	}
	return out
}

// TestAdversaryAKillBeforeTheSnapshotCommandLeavesAPointerNoGuestFollowed.
//
// The first rotation kill: the Agent dies with the overlay created and `active/current`
// already moved onto it, and blockdev-snapshot-sync never reaching QEMU. The five
// questions:
//
//   - Which commit is visible: the one before the rotation. Nothing was sealed, so nothing
//     was promised.
//   - What local state is left: a pointer naming a layer the guest never reached, an empty
//     overlay file under it, and a state.json that mentions neither.
//   - What remote objects are left: none. The publisher was never handed anything.
//   - Does it recover by itself: yes, and this is the assertion that matters — the next
//     Agent asks QEMU first, finds it still writing to the layer *under* the orphan, and
//     repairs the pointer back onto it. The rotation is then retried from the real tip, so
//     the overlay it builds is backed by the layer the guest actually wrote into.
//   - Could a confirmed commit be lost: no, and the way it could is what the last assertion
//     is for. Publishing the orphan instead of the guest's layer would land a commit whose
//     bytes are an empty overlay, silently dropping everything written since the previous
//     commit.
func TestAdversaryAKillBeforeTheSnapshotCommandLeavesAPointerNoGuestFollowed(t *testing.T) {
	t.Parallel()
	a := newPreSnapshotAgent(t, 8<<20)

	if err := a.apply(t); err != nil {
		t.Fatalf("preparing the volume: %v", err)
	}
	written := a.tip(t)
	a.guestOn(written, 9<<20)

	// The cycle that dies on the way to QEMU.
	a.dialer.arm = true
	if err := a.apply(t); err == nil {
		t.Fatal("a rotation whose snapshot command never left the host was reported as a success")
	}
	orphan := a.tip(t)
	if orphan == written {
		t.Fatalf("the pointer never moved, so this test is not standing in the window it is about: %q", orphan)
	}
	if got := a.dialer.sent.String(); strings.Contains(got, "blockdev-snapshot-sync") {
		t.Fatalf("QEMU was told to switch after all; this is not the boundary being tested:\n%s", got)
	}
	if len(a.pub.got) != 0 {
		t.Fatalf("something was published although nothing was sealed: %v", a.offered())
	}

	// The next Agent, over the same disk. QEMU never switched, so the guest is still
	// writing to the layer under the orphan.
	a.kill(t, 8<<20)
	a.dialer.arm = false
	a.guestOn(written, 9<<20)
	if err := a.apply(t); err != nil {
		t.Fatalf("the Agent after the kill could not serve the volume: %v", err)
	}

	if got := a.tip(t); got == orphan {
		t.Errorf("active/current still names %q, the layer the guest never reached", orphan)
	}
	// The retry is built over what the guest wrote, not over the layer it never reached.
	var created string
	for _, c := range a.runner.commands() {
		if strings.HasPrefix(c, "/qemu-img create ") {
			created = c
		}
	}
	if created == "" {
		t.Fatal("the rotation was not retried after the kill, so the tip goes on growing for ever")
	}
	if !strings.Contains(created, " -b "+written+" ") {
		t.Errorf("the retried rotation built its overlay over the wrong layer: %s", created)
	}
	// And the layer with the guest's bytes in it — not the orphan — is what became a commit.
	if got := a.offered(); len(got) != 1 || got[0] != qcow.LayerIDOfImage(written) {
		t.Errorf("published %v, want exactly the layer the guest wrote, %s",
			got, qcow.LayerIDOfImage(written))
	}
}

// TestAdversaryAKillDuringTheSnapshotStillPublishesTheLayerItSealed.
//
// The second rotation kill: blockdev-snapshot-sync was sent and the answer never arrived.
// The Agent cannot tell whether QEMU performed the switch, and this test takes the case
// where it did — the guest is writing to the new layer and the Agent that asked for it is
// dead. The five questions:
//
//   - Which commit is visible: the one before the rotation. `Commit() → SUCCESS` was never
//     said about the layer that was just sealed.
//   - What local state is left: a complete, immutable layer full of the guest's writes, a
//     pointer that happens to be right, and a state.json that records neither the seal nor
//     any promise about it.
//   - What remote objects are left: none.
//   - Does it recover by itself: yes. The next Agent adopts what QEMU has open, derives
//     "sealed" as everything under that tip that is in no commit, and publishes the layer
//     nobody recorded. That derivation is the whole point — a rotation whose only record
//     was the write that died would otherwise be chained past, splicing a hole into the
//     published history.
//   - Could a confirmed commit be lost: no. The layer is published once, under a fresh
//     commit id, because the id it was promised under died with the process that minted it.
func TestAdversaryAKillDuringTheSnapshotStillPublishesTheLayerItSealed(t *testing.T) {
	t.Parallel()
	a := newAdversary(t, 8<<20, &recordingPublisher{})

	if err := a.apply(t, 1); err != nil {
		t.Fatalf("preparing the volume: %v", err)
	}
	first := a.tip(t)
	a.guestWriting(first, 9<<20)
	// The answer to blockdev-snapshot-sync never comes: the script ends after query-block,
	// so the read for the reply meets a connection with nothing left in it.
	full := attachedTo(first)
	a.dialer.scripts[qcow.QMPSocket(root, vol)] = full[:len(full)-1]

	if err := a.apply(t, 1); err == nil {
		t.Fatal("a rotation whose snapshot was never answered was reported as a success")
	}
	second := a.tip(t)
	if second == first {
		t.Fatalf("the pointer never moved, so no rotation was attempted: %q", first)
	}
	if !strings.Contains(a.dialer.sent(), "blockdev-snapshot-sync") {
		t.Fatal("the snapshot command never reached QEMU; this is the kill before, not during")
	}
	if len(a.pub.got) != 0 {
		t.Fatalf("the sealed layer was published before the kill: %v", a.pub.got)
	}

	// QEMU did perform the switch. The next Agent meets a guest writing to the layer the
	// dead one asked for, with the sealed layer underneath it recorded nowhere.
	a.crash(t, 8<<20, &recordingPublisher{})
	a.guestWriting(second, 1<<20)
	if err := a.apply(t, 1); err != nil {
		t.Fatalf("the Agent after the kill could not serve the volume: %v", err)
	}

	if got := a.tip(t); got != second {
		t.Errorf("active/current names %q, want the layer QEMU has open, %q", got, second)
	}
	sealed := qcow.LayerIDOfImage(first)
	if !a.offered()[sealed] {
		t.Fatalf("layer %s was sealed with the guest's writes in it and was never published;"+
			" the layers that were: %v", sealed, a.offered())
	}
	if len(a.pub.got) != 1 {
		t.Errorf("the recovered rotation published %d layers, want exactly the one it sealed: %v",
			len(a.pub.got), a.pub.got)
	}
}
