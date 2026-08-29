package qcow_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/qcow"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// scriptConn is a QMP endpoint whose answers were decided in advance. The exchange is
// strictly request-then-answer, so a canned reader is the whole server.
type scriptConn struct {
	r    io.Reader
	sent strings.Builder
}

func newConn(lines ...string) *scriptConn {
	return &scriptConn{r: strings.NewReader(strings.Join(lines, "\n") + "\n")}
}

func (c *scriptConn) Read(p []byte) (int, error)  { return c.r.Read(p) }
func (c *scriptConn) Write(p []byte) (int, error) { return c.sent.Write(p) }
func (*scriptConn) Close() error                  { return nil }

// fakeDialer is the QMP endpoint side of a test. A path with no script is a socket
// nothing is listening at, which is the ordinary state of a prepared volume whose VM
// has not been launched.
type fakeDialer struct {
	scripts map[string][]string
	dialed  []string
	conns   []*scriptConn
}

func (d *fakeDialer) Dial(_ context.Context, path string) (io.ReadWriteCloser, error) {
	d.dialed = append(d.dialed, path)
	lines, ok := d.scripts[path]
	if !ok {
		return nil, errors.New("connect: no such file or directory")
	}
	c := newConn(lines...)
	d.conns = append(d.conns, c)
	return c, nil
}

// reset forgets the connections made so far, so an assertion can be about one cycle
// rather than about every cycle since the harness was built.
func (d *fakeDialer) reset() { d.conns, d.dialed = nil, nil }

// sent is everything every connection carried to QEMU. Rotation is asserted on this and
// not on the files: a Manager that moved the pointer and never told QEMU to switch would
// satisfy every filesystem assertion and leave the guest writing to a sealed layer.
func (d *fakeDialer) sent() string {
	var b strings.Builder
	for _, c := range d.conns {
		b.WriteString(c.sent.String())
	}
	return b.String()
}

// attachedTo scripts a QEMU that has one image open. The trailing answers are for a
// rotation: this connection is used once per Manager cycle, and a cycle that rotates
// asks query-block and then issues the snapshot on the same one.
func attachedTo(image string) []string {
	return []string{
		`{"QMP": {"version": {}, "capabilities": []}}`,
		`{"return": {}}`,
		`{"return": [{"device": "virtio0", "inserted": {"file": "` + image + `", "drv": "qcow2"}}]}`,
		`{"return": {}}`,
	}
}

// attachedByNode scripts a QEMU launched the modern way — `-blockdev node-name=vol` — so
// the disk has a real node and no drive id at all. The fourth answer is
// query-named-block-nodes, which a rotation asks in order to pick an overlay name nothing
// is using.
func attachedByNode(image string) []string {
	return []string{
		`{"QMP": {"version": {}, "capabilities": []}}`,
		`{"return": {}}`,
		`{"return": [{"device": "", "inserted": {"file": "` + image + `", "drv": "qcow2", "node-name": "vol"}}]}`,
		`{"return": [{"node-name": "vol"}, {"node-name": "vol-file"}, {"node-name": "spin1"}]}`,
		`{"return": {}}`,
	}
}

// tip is the layer `active/current` names, which is the only way a test learns the path
// the Manager chose: layer ids are v7 UUIDs minted per layer, so nothing outside can
// predict one.
func (h *harness) tip(t *testing.T) string {
	t.Helper()
	body, err := h.paths.ReadFile(qcow.ActivePointer(root, vol))
	if err != nil {
		t.Fatalf("reading the pointer: %v", err)
	}
	return string(body)
}

// harness is a Manager and everything a test needs to see what it did.
type harness struct {
	m      *qcow.Manager
	runner *fakeRunner
	paths  *fakePaths
	dialer *fakeDialer
	disk   *sim.Disk
	rec    *fakeRecovery
	clk    *sim.Clock
	// witness is the object store's answer to "who holds this volume now". Nil is an
	// Agent with no object store, which reclaims nothing.
	witness qcow.Witness
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	return newHarnessOn(t, sim.NewDisk())
}

func newHarnessRotatingAt(t *testing.T, at int64) *harness {
	t.Helper()
	return newHarnessWith(t, sim.NewDisk(), at, nil)
}

// newHarnessFull is a Manager that also publishes what it seals.
func newHarnessFull(t *testing.T, at int64, pub qcow.Publisher) *harness {
	t.Helper()
	return newHarnessWith(t, sim.NewDisk(), at, pub)
}

// newHarnessWitnessing is a Manager that can ask the object store who holds a volume now,
// which is the whole of what makes a released volume's disk reclaimable.
func newHarnessWitnessing(t *testing.T, wit qcow.Witness) *harness {
	t.Helper()
	h := &harness{
		runner:  &fakeRunner{info: infoJSON("qcow2", size, false), version: "qemu-img version 11.1.1"},
		paths:   newPaths(),
		dialer:  &fakeDialer{scripts: map[string][]string{}},
		disk:    sim.NewDisk(),
		rec:     bornEmpty(),
		witness: wit,
	}
	h.start(t, 0, nil)
	return h
}

func newHarnessOn(t *testing.T, d *sim.Disk) *harness {
	t.Helper()
	return newHarnessWith(t, d, 0, nil)
}

func newHarnessWith(t *testing.T, d *sim.Disk, rotateAt int64, pub qcow.Publisher) *harness {
	t.Helper()
	h := &harness{
		runner: &fakeRunner{info: infoJSON("qcow2", size, false), version: "qemu-img version 11.1.1"},
		paths:  newPaths(),
		dialer: &fakeDialer{scripts: map[string][]string{}},
		disk:   d,
		rec:    bornEmpty(),
	}
	h.start(t, rotateAt, pub)
	return h
}

// start builds the Manager over whatever this harness already holds.
func (h *harness) start(t *testing.T, rotateAt int64, pub qcow.Publisher) {
	t.Helper()
	if h.clk == nil {
		h.clk = sim.NewClock(time.Unix(1_700_000_000, 0))
	}
	m, err := qcow.New(t.Context(), qcow.Config{
		Root: root, QemuImg: "/qemu-img", ProbeTimeout: time.Second, RotateAtBytes: rotateAt,
	}, qcow.Deps{
		Clock:     h.clk,
		Disk:      h.disk,
		Runner:    h.runner,
		Paths:     h.paths,
		Dialer:    h.dialer,
		Publisher: pub,
		Recovery:  h.rec,
		Witness:   h.witness,
	})
	if err != nil {
		t.Fatalf("building a manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	// The start-up `--version` run is not part of what any test below is about.
	h.runner.reset()
	h.m = m
}

// restart is a second Agent over the same filesystem: everything this process knew is
// gone, and everything on disk is what it has. The data directory's lock is a fresh
// simulated disk because the kernel drops an flock when a process dies, so a restarted
// Agent meets an unlocked directory and not its own predecessor's claim.
func (h *harness) restart(t *testing.T, rotateAt int64, pub qcow.Publisher) *harness {
	t.Helper()
	if err := h.m.Close(); err != nil {
		t.Fatalf("closing the Agent that is being restarted: %v", err)
	}
	next := &harness{runner: h.runner, paths: h.paths, dialer: h.dialer, disk: sim.NewDisk(), rec: h.rec, witness: h.witness}
	next.start(t, rotateAt, pub)
	return next
}

func desired(id string, epoch int64, state storagev1.VolumeState) *storagev1.DesiredVolume {
	return &storagev1.DesiredVolume{VolumeId: id, SizeBytes: size, Epoch: epoch, State: state}
}

func active(id string, epoch int64) *storagev1.DesiredVolume {
	return desired(id, epoch, storagev1.VolumeState_VOLUME_STATE_ACTIVE)
}

// volumes runs Volumes and returns it keyed by id.
func (h *harness) volumes(t *testing.T) map[string]agent.VolumeStatus {
	t.Helper()
	got, err := h.m.Volumes(t.Context())
	if err != nil {
		t.Fatalf("reading the served volumes: %v", err)
	}
	out := make(map[string]agent.VolumeStatus, len(got))
	for _, v := range got {
		out[v.VolumeID] = v
	}
	return out
}

func TestApplyPreparesAChainAndReportsIt(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 7)}); err != nil {
		t.Fatalf("applying: %v", err)
	}

	image := h.tip(t)
	if !strings.HasPrefix(image, qcow.LayersDir(root)+"/") {
		t.Fatalf("the pointer names %q, which is not a layer of this volume", image)
	}
	if cmds := h.runner.commands(); len(cmds) != 1 || !strings.HasPrefix(cmds[0], "/qemu-img create -f qcow2 "+image) {
		t.Fatalf("qemu-img was run as %v", cmds)
	}
	if len(h.dialer.dialed) != 1 || h.dialer.dialed[0] != qcow.QMPSocket(root, vol) {
		t.Errorf("QMP was asked at %v, want %q", h.dialer.dialed, qcow.QMPSocket(root, vol))
	}

	v, ok := h.volumes(t)[vol]
	if !ok {
		t.Fatal("the prepared volume is not reported")
	}
	switch {
	case v.Epoch != 7:
		t.Errorf("reported epoch %d, want 7", v.Epoch)
	case v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED:
		t.Errorf("a healthy volume reported a refusal: %v %q", v.Refusal, v.RefusalDetail)
	}
}

func TestApplyIsIdempotent(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	for range 3 {
		if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
			t.Fatalf("applying: %v", err)
		}
	}
	// Once, on the first cycle. A chain that is re-inspected every heartbeat is a chain
	// being handed to an offline tool while QEMU may have it open (v6 §5).
	if cmds := h.runner.commands(); len(cmds) != 1 {
		t.Fatalf("qemu-img ran %d times across three cycles: %v", len(cmds), cmds)
	}
}

func TestAVolumeThatLeavesTheDesiredStateIsReleasedAndItsImageKept(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("applying: %v", err)
	}
	if err := h.m.Apply(t.Context(), nil); err != nil {
		t.Fatalf("applying an empty desired state: %v", err)
	}

	if _, ok := h.volumes(t)[vol]; ok {
		t.Error("a released volume is still reported")
	}
	// Nothing was deleted. Local persistence is what Stage 1 is, and a volume leaves
	// the desired state for reasons that reverse.
	for _, cmd := range h.runner.commands() {
		if strings.Contains(cmd, "rm") || strings.Contains(cmd, "delete") {
			t.Errorf("releasing a volume touched its image: %q", cmd)
		}
	}
	if len(h.runner.commands()) != 1 {
		t.Errorf("releasing a volume ran qemu-img: %v", h.runner.commands())
	}
}

// TestOnlyAnActiveVolumeGetsAChain has two halves and the second is the load-bearing
// one. A volume the fleet is taking away must not have a chain prepared for it — that is
// this host getting ready to write a volume it is losing — and it must not be *reported*
// either: an unset refusal is this host saying "I am serving this", a FENCING_WAIT
// volume still satisfies the host-and-epoch predicate the report is accepted on, and the
// fleet would read the outgoing writer as healthy for the whole wait.
func TestOnlyAnActiveVolumeGetsAChain(t *testing.T) {
	t.Parallel()
	for _, state := range []storagev1.VolumeState{
		storagev1.VolumeState_VOLUME_STATE_FENCING_WAIT,
		storagev1.VolumeState_VOLUME_STATE_DETACHED,
		storagev1.VolumeState_VOLUME_STATE_RECOVERY_REQUIRED,
		storagev1.VolumeState_VOLUME_STATE_UNSPECIFIED,
	} {
		t.Run(state.String(), func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			// Serving first, so what is asserted is the *transition* out of ACTIVE and
			// not merely that an unknown volume was ignored.
			if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
				t.Fatalf("applying: %v", err)
			}
			h.runner.reset()

			if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{desired(vol, 1, state)}); err != nil {
				t.Fatalf("applying %s: %v", state, err)
			}
			if cmds := h.runner.commands(); len(cmds) != 0 {
				t.Fatalf("a %s volume was prepared: %v", state, cmds)
			}
			if v, ok := h.volumes(t)[vol]; ok {
				t.Fatalf("a %s volume is still reported as served (refusal=%v)", state, v.Refusal)
			}
		})
	}
}

// TestARestartUnderARunningGuestDoesNotTouchTheImage is the seam this whole design
// turns on: the Agent does not run QEMU, so it must be able to come back while a guest
// is still writing. The observable is that no offline tool was run — which is what
// stops the Agent refusing a live volume on QEMU's write lock.
func TestARestartUnderARunningGuestDoesNotTouchTheImage(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	image := qcow.LayerImage(root, layerID)
	h.guestHas(vol, image)
	// Any offline run at all would fail the way a real one does, on QEMU's lock.
	h.runner.err = errors.New(`Failed to get shared "write" lock`)

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 3)}); err != nil {
		t.Fatalf("re-attaching to a volume a guest is using: %v", err)
	}
	if cmds := h.runner.commands(); len(cmds) != 0 {
		t.Fatalf("an image a guest is writing was handed to qemu-img: %v", cmds)
	}
	if v := h.volumes(t)[vol]; v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Errorf("a live volume was refused: %v %q", v.Refusal, v.RefusalDetail)
	}
}

func TestAQEMUWithSomebodyElsesImageIsRefused(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo("/somewhere/else/disk.qcow2")

	err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)})
	if !errors.Is(err, qcow.ErrForeignImage) {
		t.Fatalf("want ErrForeignImage, got %v", err)
	}
	v := h.volumes(t)[vol]
	if v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_ATTACH_FAILED {
		t.Fatalf("refusal = %v, want ATTACH_FAILED", v.Refusal)
	}
	// The detail is what an operator acts on, and acting starts with knowing which
	// image the other VM has.
	if !strings.Contains(v.RefusalDetail, "/somewhere/else/disk.qcow2") {
		t.Errorf("the refusal does not name the foreign image: %q", v.RefusalDetail)
	}
	if cmds := h.runner.commands(); len(cmds) != 0 {
		t.Errorf("a volume with a foreign VM on its socket was still prepared: %v", cmds)
	}
}

func TestAChainThatCannotBePreparedIsRefusedAndReported(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.runner.err = errors.New("Formatting failed: No space left on device")
	other := "0198c0de-0000-7000-8000-0000000000ff"

	err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1), {VolumeId: "", Epoch: 1}, active(other, 1)})
	if err == nil {
		t.Fatal("a chain that could not be created was reported as success")
	}
	// Both volumes were attempted. Stopping at the first would silence every volume
	// behind it, and the report is the only thing that carries a refusal off this host.
	got := h.volumes(t)
	for _, id := range []string{vol, other} {
		v, ok := got[id]
		if !ok {
			t.Fatalf("volume %s was never attempted", id)
		}
		if v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_ATTACH_FAILED {
			t.Errorf("volume %s: refusal = %v, want ATTACH_FAILED", id, v.Refusal)
		}
		if !strings.Contains(v.RefusalDetail, "No space left") {
			t.Errorf("volume %s: the refusal does not carry qemu-img's words: %q", id, v.RefusalDetail)
		}
	}
	if !strings.Contains(err.Error(), "no id") {
		t.Errorf("a desired volume with no id was accepted: %v", err)
	}
}

func TestFence(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		why  storagev1.VolumeRefusal
		// reported says whether the volume is still in the next report.
		reported bool
	}{
		{
			// The Control Plane refused the report; telling it back would be telling it
			// what it just told us, and the report would be refused again.
			name: "a refused report says nothing",
			why:  storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED,
		},
		{
			// Nothing outside this process knows the lease lapsed. Silence there is a
			// guest with no disk and a fleet that reads healthy.
			name:     "a lapsed lease is news",
			why:      storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST,
			reported: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarness(t)
			if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 4)}); err != nil {
				t.Fatalf("applying: %v", err)
			}
			if err := h.m.Fence(t.Context(), []string{vol, "never-heard-of-it"}, tt.why, "the detail"); err != nil {
				t.Fatalf("fencing: %v", err)
			}

			v, ok := h.volumes(t)[vol]
			if ok != tt.reported {
				t.Fatalf("reported = %v, want %v", ok, tt.reported)
			}
			if !tt.reported {
				return
			}
			if v.Refusal != tt.why || v.RefusalDetail != "the detail" {
				t.Errorf("reported %v %q, want %v %q", v.Refusal, v.RefusalDetail, tt.why, "the detail")
			}
		})
	}
}

// TestAGivenUpVolumeComesBackOnlyAtAHigherEpoch is the rule the loop's own teardown is
// written against: a host that gave a volume up must not resume on the strength of a
// desired state read before a promotion it never heard about. The Control Plane
// granting the volume again is what raises the epoch.
func TestAGivenUpVolumeComesBackOnlyAtAHigherEpoch(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 4)}); err != nil {
		t.Fatalf("applying: %v", err)
	}
	if err := h.m.Fence(t.Context(), []string{vol},
		storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST, "the lease expired"); err != nil {
		t.Fatalf("fencing: %v", err)
	}

	// The same epoch: still given up.
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 4)}); err != nil {
		t.Fatalf("re-applying at the same epoch: %v", err)
	}
	if v := h.volumes(t)[vol]; v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST {
		t.Fatalf("a volume given up at epoch 4 resumed at epoch 4: %v", v.Refusal)
	}

	// A higher one: the fleet has said something about it.
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 5)}); err != nil {
		t.Fatalf("re-applying at a higher epoch: %v", err)
	}
	v := h.volumes(t)[vol]
	if v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Fatalf("a volume granted at a higher epoch stayed refused: %v %q", v.Refusal, v.RefusalDetail)
	}
	if v.Epoch != 5 {
		t.Errorf("reported epoch %d, want 5", v.Epoch)
	}
}

func TestOneAgentPerHost(t *testing.T) {
	t.Parallel()
	d := sim.NewDisk()
	newHarnessOn(t, d)

	_, err := qcow.New(t.Context(), qcow.Config{Root: root, QemuImg: "/qemu-img", ProbeTimeout: time.Second}, qcow.Deps{
		Clock: sim.NewClock(time.Unix(0, 0)), Disk: d,
		Runner: &fakeRunner{version: "qemu-img version 11.1.1"}, Paths: newPaths(), Dialer: &fakeDialer{},
		Recovery: bornEmpty(),
	})
	if err == nil {
		t.Fatal("a second Agent took a data directory a live one is holding")
	}
	if !strings.Contains(err.Error(), "already using") {
		t.Errorf("the refusal does not say what is wrong: %v", err)
	}
}

func TestNewRefusesIncompleteWiring(t *testing.T) {
	t.Parallel()
	full := func() (qcow.Config, qcow.Deps) {
		return qcow.Config{Root: root, QemuImg: "/qemu-img", ProbeTimeout: time.Second},
			qcow.Deps{Clock: sim.NewClock(time.Unix(0, 0)), Disk: sim.NewDisk(),
				Runner: &fakeRunner{version: "qemu-img version 11.1.1"}, Paths: newPaths(), Dialer: &fakeDialer{},
				Recovery: bornEmpty()}
	}
	tests := []struct {
		name string
		mut  func(*qcow.Config, *qcow.Deps)
		want string
	}{
		{"no data directory", func(c *qcow.Config, _ *qcow.Deps) { c.Root = "" }, "data directory is required"},
		{"a relative data directory", func(c *qcow.Config, _ *qcow.Deps) { c.Root = "data" }, "must be absolute"},
		{"no qemu-img", func(c *qcow.Config, _ *qcow.Deps) { c.QemuImg = "" }, "qemu-img binary is required"},
		{"no timeout", func(c *qcow.Config, _ *qcow.Deps) { c.ProbeTimeout = 0 }, "timeout must be positive"},
		{"no clock", func(_ *qcow.Config, d *qcow.Deps) { d.Clock = nil }, "clock must be injected"},
		{"no disk", func(_ *qcow.Config, d *qcow.Deps) { d.Disk = nil }, "disk must be injected"},
		{"no runner", func(_ *qcow.Config, d *qcow.Deps) { d.Runner = nil }, "runner must be injected"},
		{"no paths", func(_ *qcow.Config, d *qcow.Deps) { d.Paths = nil }, "path accessor must be injected"},
		{"no dialer", func(_ *qcow.Config, d *qcow.Deps) { d.Dialer = nil }, "dialer must be injected"},
		{"no recovery", func(_ *qcow.Config, d *qcow.Deps) { d.Recovery = nil }, "recovery must be injected"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg, deps := full()
			tt.mut(&cfg, &deps)
			m, err := qcow.New(t.Context(), cfg, deps)
			if err == nil {
				_ = m.Close()
				t.Fatal("incomplete wiring was accepted")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %v, want it to say %q", err, tt.want)
			}
		})
	}
}

// TestAQEMUThatCannotBeQuestionedIsRefused: a socket that answers but is not QMP, or a
// QEMU that dies mid-handshake, leaves the Agent unable to tell whether the image is in
// use — and the one thing it must not do then is hand that image to an offline tool.
func TestAQEMUThatCannotBeQuestionedIsRefused(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = []string{`{"hello": "not qemu"}`}

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err == nil {
		t.Fatal("a socket that is not QMP was treated as an absent one")
	}
	if v := h.volumes(t)[vol]; v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_ATTACH_FAILED {
		t.Errorf("refusal = %v, want ATTACH_FAILED", v.Refusal)
	}
	if cmds := h.runner.commands(); len(cmds) != 0 {
		t.Fatalf("an image that may be in use was handed to qemu-img: %v", cmds)
	}
}

// TestAQEMUWithNoBlockDevicesIsNotAForeignOne. A QEMU can be up before its disk is
// attached; refusing over it would make a volume's readiness depend on the order two
// other processes happened to do things in.
func TestAQEMUWithNoBlockDevicesIsNotAForeignOne(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = []string{
		`{"QMP": {"version": {}, "capabilities": []}}`, `{"return": {}}`, `{"return": []}`,
	}
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("applying against a QEMU with no disks: %v", err)
	}
	if v := h.volumes(t)[vol]; v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Errorf("refused a QEMU that has no disk yet: %v %q", v.Refusal, v.RefusalDetail)
	}
}

// TestAnAgentWhoseQemuImgDoesNotWorkRefusesToStart. The failure this catches is an
// operator's — a path that is not there, not executable, or not qemu-img — and an Agent
// that discovered it when the first volume arrived would already have registered as a
// healthy host and would then refuse a volume for a reason nobody was told at start-up.
func TestAnAgentWhoseQemuImgDoesNotWorkRefusesToStart(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		runner *fakeRunner
		want   string
	}{
		{
			name:   "it cannot be run",
			runner: &fakeRunner{err: errors.New("fork/exec /qemu-img: no such file or directory")},
			want:   "cannot be run",
		},
		{
			// Something ran and said nothing. A binary that is not qemu-img satisfies
			// "the file is executable", which is why the check is a run and not a stat.
			name:   "it is not qemu-img",
			runner: &fakeRunner{},
			want:   "it is not qemu-img",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := sim.NewDisk()
			m, err := qcow.New(t.Context(),
				qcow.Config{Root: root, QemuImg: "/qemu-img", ProbeTimeout: time.Second},
				qcow.Deps{Clock: sim.NewClock(time.Unix(0, 0)), Disk: d,
					Runner: tt.runner, Paths: newPaths(), Dialer: &fakeDialer{}, Recovery: bornEmpty()})
			if err == nil {
				_ = m.Close()
				t.Fatal("an Agent that cannot run qemu-img started anyway")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %v, want it to say %q", err, tt.want)
			}
			// The data directory is given back. A refusal that kept the lock would stop
			// the *fixed* Agent from starting until somebody found the stale process.
			if _, lerr := d.Lock("agent.lock"); lerr != nil {
				t.Errorf("a refused Agent kept the data directory locked: %v", lerr)
			}
		})
	}
}

// TestAVMComingAndGoingDoesNotChangeWhoServesTheVolume. Attachment is an observation of
// another process, refreshed every cycle, and it must not become a claim of ours: a
// volume whose guest was shut down is still this host's to serve, and one whose guest
// arrives has not changed hands. The Agent does not launch QEMU, so both events happen
// entirely outside it.
func TestAVMComingAndGoingDoesNotChangeWhoServesTheVolume(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	socket := qcow.QMPSocket(root, vol)
	desired := []*storagev1.DesiredVolume{active(vol, 1)}

	// No VM yet: the chain is prepared and nothing is listening.
	if err := h.m.Apply(t.Context(), desired); err != nil {
		t.Fatalf("preparing: %v", err)
	}
	// A VM arrives.
	h.dialer.scripts[socket] = attachedTo(h.tip(t))
	if err := h.m.Apply(t.Context(), desired); err != nil {
		t.Fatalf("with a VM attached: %v", err)
	}
	if v := h.volumes(t)[vol]; v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Fatalf("a volume whose guest arrived was refused: %v %q", v.Refusal, v.RefusalDetail)
	}
	// And goes away again.
	delete(h.dialer.scripts, socket)
	if err := h.m.Apply(t.Context(), desired); err != nil {
		t.Fatalf("after the VM went away: %v", err)
	}
	v, ok := h.volumes(t)[vol]
	if !ok {
		t.Fatal("the volume stopped being reported when its guest shut down")
	}
	if v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Errorf("a volume with no guest was refused: %v %q", v.Refusal, v.RefusalDetail)
	}
	// The chain was opened once, on the first cycle, and never re-inspected — including
	// on the cycle after the guest let go of it.
	if cmds := h.runner.commands(); len(cmds) != 1 {
		t.Errorf("qemu-img ran %d times across three cycles: %v", len(cmds), cmds)
	}
}

// guestHas declares that a VM at this volume's socket has `image` open, and records the
// layer the way production would have: a layer is written into the record before anything
// points at it (recordThenPoint), so a fixture that declared only the QMP answer would be
// building a world this Agent refuses — and refuses for the right reason, since with one
// layers directory per host the record is the only thing that says which volume a file
// belongs to.
func (h *harness) guestHas(volumeID, image string) {
	h.dialer.scripts[qcow.QMPSocket(root, volumeID)] = attachedTo(image)
	h.paths.present[image] = true
	h.paths.recordTip(volumeID, image)
}

// rotating prepares a volume, attaches a VM to it, and says how large its tip has grown.
func (h *harness) rotating(t *testing.T, tipBytes int64) string {
	t.Helper()
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("preparing: %v", err)
	}
	tip := h.tip(t)
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(tip)
	h.paths.sizes[tip] = tipBytes
	// What `qemu-img info` says about the layer the rotation is about to create.
	h.runner.info = overlayJSON(size, tip)
	h.runner.reset()
	return tip
}

// TestATipThatCrossesTheThresholdIsSealedUnderTheRunningGuest is v6 §23.2 end to end
// inside this package: the size trigger fires, a layer is created over the tip, the
// pointer moves and QEMU is told to switch — with the VM attached the whole time.
func TestATipThatCrossesTheThresholdIsSealedUnderTheRunningGuest(t *testing.T) {
	t.Parallel()
	h := newHarnessRotatingAt(t, 8<<20)
	tip := h.rotating(t, 9<<20)

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("the cycle that should rotate: %v", err)
	}

	next := h.tip(t)
	if next == tip {
		t.Fatalf("the pointer still names %q; the tip was not rotated", tip)
	}
	if !strings.HasPrefix(next, qcow.LayersDir(root)+"/") {
		t.Fatalf("the new tip %q is not a layer of this volume", next)
	}
	// The overlay is created over the old tip and QEMU is never asked to open its
	// backing (-u): it holds the write lock on it.
	create := "/qemu-img create -f qcow2 -b " + tip + " -F qcow2 -u " + next
	if cmds := h.runner.commands(); len(cmds) != 2 || !strings.HasPrefix(cmds[0], create) {
		t.Fatalf("qemu-img was run as %v, want %q first", cmds, create)
	}
	// And the guest was actually moved. Without this the volume looks rotated from the
	// filesystem and the guest is still writing into a layer we have called sealed.
	sent := h.dialer.sent()
	if !strings.Contains(sent, "blockdev-snapshot-sync") || !strings.Contains(sent, next) {
		t.Errorf("QEMU was never told to switch: %s", sent)
	}
	if !strings.Contains(sent, `"mode":"existing"`) {
		t.Errorf("the snapshot did not use the overlay this Agent checked: %s", sent)
	}
}

// TestAVolumeNobodyWritesIsNeverRotated is v6 §11's first invariant. Without it an idle
// volume seals an empty layer every cycle for ever, and the RPO number stops meaning
// what it says: a volume that wrote nothing is *inside* its target, not behind it.
func TestAVolumeNobodyWritesIsNeverRotated(t *testing.T) {
	t.Parallel()
	h := newHarnessRotatingAt(t, 8<<20)
	tip := h.rotating(t, 1<<20)

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("applying: %v", err)
	}
	if got := h.tip(t); got != tip {
		t.Errorf("a tip under the threshold was rotated to %q", got)
	}
	if cmds := h.runner.commands(); len(cmds) != 0 {
		t.Errorf("an idle volume ran %v", cmds)
	}
}

// TestATipWithNoVMIsNotRotated: rotation runs through the QEMU that is writing. There is
// no offline path and there must not be one — sealing a layer with qemu-img while a
// guest could still be attached is exactly what v6 §5 forbids.
func TestATipWithNoVMIsNotRotated(t *testing.T) {
	t.Parallel()
	h := newHarnessRotatingAt(t, 8<<20)
	tip := h.rotating(t, 99<<20)
	delete(h.dialer.scripts, qcow.QMPSocket(root, vol))

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("applying: %v", err)
	}
	if got := h.tip(t); got != tip {
		t.Errorf("a volume with no VM was rotated to %q", got)
	}
}

// TestARotationThatFailsKeepsServingTheVolume: a layer that could not be sealed is a
// volume that keeps working and keeps growing. Refusing it would turn "we did not manage
// to bound this layer" into an outage for a guest that is doing nothing wrong.
func TestARotationThatFailsKeepsServingTheVolume(t *testing.T) {
	t.Parallel()
	h := newHarnessRotatingAt(t, 8<<20)
	tip := h.rotating(t, 9<<20)
	h.runner.err = errors.New("Formatting failed: No space left on device")

	err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)})
	if err == nil {
		t.Fatal("a rotation that failed was reported as a success")
	}
	v, ok := h.volumes(t)[vol]
	if !ok {
		t.Fatal("the volume disappeared because a rotation failed")
	}
	if v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Errorf("the volume was refused over a rotation: %v %q", v.Refusal, v.RefusalDetail)
	}
	if got := h.tip(t); got != tip {
		t.Errorf("the pointer moved to %q although no layer was created", got)
	}
}

// TestAPointerThatRanAheadIsRepairedFromTheGuest closes the window Rotate leaves open on
// purpose: the pointer is moved before QEMU is told to switch, so a snapshot that did
// not happen leaves it naming a layer the guest never reached. Left there, the next boot
// gets a layer with none of the guest's writes in it — and nothing looks wrong until the
// VM stops.
func TestAPointerThatRanAheadIsRepairedFromTheGuest(t *testing.T) {
	t.Parallel()
	h := newHarnessRotatingAt(t, 0)
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("preparing: %v", err)
	}
	live := h.tip(t)
	// The pointer ran ahead, and then everything stopped — recorded first, which is the
	// order a rotation writes them in and the order that lets a pointer be believed.
	ahead := qcow.LayerImage(root, nextID)
	h.paths.recordTip(vol, ahead)
	if err := h.paths.WriteAtomic(qcow.ActivePointer(root, vol), []byte(ahead)); err != nil {
		t.Fatalf("moving the pointer: %v", err)
	}
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(live)

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("coming back under a running guest: %v", err)
	}
	if got := h.tip(t); got != live {
		t.Errorf("active/current names %q, want what QEMU is writing, %q", got, live)
	}
}

func TestARotationThresholdBelowAnEmptyImageIsRefused(t *testing.T) {
	t.Parallel()
	_, err := qcow.New(t.Context(), qcow.Config{
		Root: root, QemuImg: "/qemu-img", ProbeTimeout: time.Second, RotateAtBytes: 4096,
	}, qcow.Deps{
		Clock: sim.NewClock(time.Unix(0, 0)), Disk: sim.NewDisk(),
		Runner: &fakeRunner{}, Paths: newPaths(), Dialer: &fakeDialer{scripts: map[string][]string{}},
	})
	if err == nil {
		t.Fatal("a threshold an empty qcow2 already exceeds was accepted")
	}
}

// recordingPublisher is the object store, as far as the Manager is concerned.
type recordingPublisher struct {
	got []qcow.SealedLayer
	err error
}

func (p *recordingPublisher) Publish(_ context.Context, l qcow.SealedLayer) error {
	p.got = append(p.got, l)
	return p.err
}

// TestASealedLayerIsHandedOverForPublishing: rotation produces a layer and the commit
// protocol takes it from there. The assertions are on *which* layer — the one that was
// sealed, not the empty one the guest moved on to — because sealing the wrong file
// produces an object that uploads perfectly and cannot be opened by whoever needs it.
func TestASealedLayerIsHandedOverForPublishing(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{}
	h := newHarnessFull(t, 8<<20, pub)
	tip := h.rotating(t, 9<<20)
	h.paths.sizes[tip] = 9 << 20

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("the cycle that should rotate and publish: %v", err)
	}
	if len(pub.got) != 1 {
		t.Fatalf("%d layers were handed over, want 1", len(pub.got))
	}
	l := pub.got[0]
	switch {
	case l.Path != tip:
		t.Errorf("the layer published is %q, want the one that was sealed, %q", l.Path, tip)
	case l.LayerID != qcow.LayerIDOfImage(tip):
		t.Errorf("the layer id is %q, want the sealed file's own, %q", l.LayerID, qcow.LayerIDOfImage(tip))
	case l.VolumeID != vol:
		t.Errorf("the volume is %q", l.VolumeID)
	case l.Epoch != 1:
		t.Errorf("the epoch is %d, want the one this host held", l.Epoch)
	case l.VirtualSize != size:
		t.Errorf("the virtual size is %d, want %d", l.VirtualSize, size)
	case l.CommitID == "":
		t.Error("no commit id was minted, so a retry could not be recognised as one")
	}
}

// TestNothingRotatesWhileASealedLayerIsUnpublished is v6 §11's second invariant, and the
// whole of what it buys is visible here: with the object store down, the tip goes on
// growing — one file the guest was going to fill anyway — instead of the chain gaining a
// small layer per cycle, each one a commit that never landed.
func TestNothingRotatesWhileASealedLayerIsUnpublished(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{err: errors.New("dial tcp: connection refused")}
	h := newHarnessFull(t, 8<<20, pub)
	tip := h.rotating(t, 9<<20)
	h.paths.sizes[tip] = 9 << 20

	// The cycle that rotates, and whose publish fails.
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err == nil {
		t.Fatal("a publish that failed was reported as a success")
	}
	sealedOnce := h.tip(t)

	// Three more cycles with the tip well over the threshold. Not one of them rotates.
	//
	// Asserted as "no rotation was attempted" and not as "the pointer did not move",
	// which is the same trap this file warns about elsewhere: with the guard removed, the
	// second rotation *fails* anyway — the fake qemu-img still describes the first tip —
	// so the pointer stays put and a test watching the pointer stays green while the
	// invariant is gone. What the guard promises is that nothing is tried.
	h.runner.reset()
	for i := range 3 {
		h.paths.sizes[sealedOnce] = int64(20+i) << 20
		h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(sealedOnce)
		_ = h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)})
		for _, cmd := range h.runner.commands() {
			if strings.Contains(cmd, "create -f qcow2 -b ") {
				t.Fatalf("cycle %d started a rotation while a sealed layer was unpublished: %s", i, cmd)
			}
		}
		if got := h.tip(t); got != sealedOnce {
			t.Fatalf("cycle %d rotated to %q while a sealed layer was unpublished", i, got)
		}
	}
	// And every attempt was the same commit, which is what makes the retries idempotent
	// rather than a queue of near-identical commits waiting to be published.
	if len(pub.got) < 2 {
		t.Fatalf("the sealed layer was offered %d times; it must keep being retried", len(pub.got))
	}
	for _, l := range pub.got[1:] {
		if l.CommitID != pub.got[0].CommitID {
			t.Fatalf("a retry used a fresh commit id: %q then %q", pub.got[0].CommitID, l.CommitID)
		}
	}
}

// TestAHeadThatMovedStopsTheVolume. Every other publishing failure leaves the guest
// alone, because a layer that could not be uploaded costs nothing yet. This one is
// different in kind: another host published for this volume, so this host is not its
// writer, and going on holding the disk would mean two hosts writing one volume — which
// is the failure the whole design is arranged against.
func TestAHeadThatMovedStopsTheVolume(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{err: fmt.Errorf("publishing: %w", commit.ErrHeadMoved)}
	h := newHarnessFull(t, 8<<20, pub)
	tip := h.rotating(t, 9<<20)
	h.paths.sizes[tip] = 9 << 20

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err == nil {
		t.Fatal("a volume whose HEAD moved was served without complaint")
	}
	v, ok := h.volumes(t)[vol]
	if !ok {
		t.Fatal("the volume is not reported at all, so the fleet cannot see why it stopped")
	}
	if v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_PUBLISH_FENCED {
		t.Errorf("the refusal is %v, want PUBLISH_FENCED — an operator looks at ownership for this one, not at this host", v.Refusal)
	}
}

// TestAPublishThatFailsKeepsServingTheVolume: the guest is doing nothing wrong, and
// taking its disk away because an upload did not go through would turn "we did not
// manage to bound this layer" into an outage.
func TestAPublishThatFailsKeepsServingTheVolume(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{err: errors.New("503 Service Unavailable")}
	h := newHarnessFull(t, 8<<20, pub)
	tip := h.rotating(t, 9<<20)
	h.paths.sizes[tip] = 9 << 20

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err == nil {
		t.Fatal("a publish that failed was reported as a success")
	}
	v := h.volumes(t)[vol]
	if v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Errorf("the volume was refused over an upload: %v %q", v.Refusal, v.RefusalDetail)
	}
}

// state is what this host has written down about the volume: which commits' layers it
// holds, and which sealed layer it still owes the object store.
func (h *harness) state(t *testing.T) qcow.State {
	t.Helper()
	body, err := h.paths.ReadFile(qcow.StateFile(root, vol))
	if err != nil {
		t.Fatalf("reading state.json: %v", err)
	}
	st, err := qcow.UnmarshalState(vol, body)
	if err != nil {
		t.Fatalf("state.json cannot be believed: %v", err)
	}
	return st
}

// TestAVolumeWithCommitsIsRecoveredRatherThanCreatedEmpty is the defect this stage
// exists to close, seen from the Agent: a volume that has published commits, placed on a
// host that holds none of them, used to be handed to its guest as an empty qcow2 with no
// error anywhere. The bucket is asked, the chain is rebuilt, and the tip is an overlay
// over it.
func TestAVolumeWithCommitsIsRecoveredRatherThanCreatedEmpty(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	base := qcow.LayerImage(root, baseID)
	h.rec.err, h.rec.res = nil, qcow.Restored{Base: base, VirtualSize: size, HeadCommitID: headCommit}
	h.runner.info = overlayJSON(size, base)
	h.paths.present[base] = true

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 2)}); err != nil {
		t.Fatalf("applying a volume whose chain is in the object store: %v", err)
	}

	tip := h.tip(t)
	if tip == base || !strings.HasPrefix(tip, qcow.LayersDir(root)+"/") {
		t.Fatalf("the pointer names %q, want a new layer of this volume over %q", tip, base)
	}
	create := fmt.Sprintf("/qemu-img create -f qcow2 -b %s -F qcow2 -u %s %d", base, tip, size)
	cmds := h.runner.commands()
	if len(cmds) != 2 || cmds[0] != create {
		t.Fatalf("qemu-img was run as %v, want %q first", cmds, create)
	}
	if v := h.volumes(t)[vol]; v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Errorf("a recovered volume reported a refusal: %v %q", v.Refusal, v.RefusalDetail)
	}
}

// TestAnUnreachableObjectStoreRefusesTheVolumeInsteadOfHandingOutABlankDisk is the
// assertion that matters most in this file. "I could not reach the bucket" and "this
// volume is new" lead to opposite decisions about somebody's data, and the proof that
// they are not collapsed is the *absence* of an image: an error value alone would be
// satisfied by a build that refused and created the file anyway.
func TestAnUnreachableObjectStoreRefusesTheVolumeInsteadOfHandingOutABlankDisk(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.rec.err = errors.New("dial tcp 10.0.0.7:443: connect: connection refused")

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 2)}); err == nil {
		t.Fatal("a volume nobody could ask the bucket about was prepared without complaint")
	}
	if cmds := h.runner.commands(); len(cmds) != 0 {
		t.Fatalf("an image was made for a volume whose history is unknown: %v", cmds)
	}
	if got := h.paths.pointer(); got != "" {
		t.Fatalf("active/current names %q, so a VM would be launched against it", got)
	}
	v, ok := h.volumes(t)[vol]
	if !ok {
		t.Fatal("the volume is not reported at all, so the fleet cannot see why it stopped")
	}
	// IMAGE_MISSING and not the ATTACH_FAILED catch-all: its proto comment is "the
	// object store holds none... Look at the bucket", which is the operator's next move.
	if v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_IMAGE_MISSING {
		t.Errorf("the refusal is %v, want IMAGE_MISSING", v.Refusal)
	}
	if !strings.Contains(v.RefusalDetail, "connection refused") {
		t.Errorf("the refusal does not carry what went wrong: %q", v.RefusalDetail)
	}
}

// TestAVolumeWithNoCommitsIsCreatedEmptyAndAskedOnce: the ordinary case must stay
// cheap. One question per volume per host, in the branch that was about to fork a
// process anyway; from the moment the pointer exists no cycle pays anything.
func TestAVolumeWithNoCommitsIsCreatedEmptyAndAskedOnce(t *testing.T) {
	t.Parallel()
	h := newHarness(t)

	for i := range 3 {
		if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
			t.Fatalf("cycle %d: %v", i, err)
		}
	}

	tip := h.tip(t)
	want := fmt.Sprintf("/qemu-img create -f qcow2 %s %d", tip, size)
	if cmds := h.runner.commands(); len(cmds) != 1 || cmds[0] != want {
		t.Fatalf("qemu-img was run as %v, want exactly [%q]", cmds, want)
	}
	if len(h.rec.calls) != 1 {
		t.Errorf("the object store was asked %d times over three cycles: %v", len(h.rec.calls), h.rec.calls)
	}
}

// TestAVolumeRefusedOverAnUnreachableBucketRecoversWhenItComesBack is why IMAGE_MISSING
// is exempt from the refusal latch. Every other refusal is a statement about ownership
// and must wait for a higher epoch; this one is "I could not look", which the bucket
// coming back fixes with nobody in the fleet doing anything — and latching it would turn
// a thirty-second S3 blip into a volume permanently dead on this host.
func TestAVolumeRefusedOverAnUnreachableBucketRecoversWhenItComesBack(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.rec.err = errors.New("dial tcp 10.0.0.7:443: connect: connection refused")
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 3)}); err == nil {
		t.Fatal("an unreachable bucket was not a refusal")
	}

	// The bucket is back, and the fleet has said nothing: the same epoch, on purpose.
	h.rec.err = fmt.Errorf("recovery: volume %s: %w", vol, commit.ErrNoHead)
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 3)}); err != nil {
		t.Fatalf("the cycle after the bucket came back: %v", err)
	}
	v := h.volumes(t)[vol]
	if v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Fatalf("the volume is still refused after the bucket came back: %v %q", v.Refusal, v.RefusalDetail)
	}
	if h.paths.pointer() == "" {
		t.Error("no chain was prepared, so nothing can be launched against this volume")
	}
}

// TestARestartPublishesTheSealedLayerUnderTheCommitItWasSealedWith closes the gap
// STATUS.md names: the commit id was minted in memory, so an Agent that restarted
// between sealing and publishing published the same layer a second time under a second
// id — a duplicate entry in a history that nothing could collapse afterwards. With the
// sealed layer recorded on disk the restarted Agent re-publishes under the same id,
// which commit.Publish recognises as its own retry.
func TestARestartPublishesTheSealedLayerUnderTheCommitItWasSealedWith(t *testing.T) {
	t.Parallel()
	down := &recordingPublisher{err: errors.New("503 Service Unavailable")}
	h := newHarnessFull(t, 8<<20, down)
	sealed := h.rotating(t, 9<<20)
	h.paths.sizes[sealed] = 9 << 20

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err == nil {
		t.Fatal("a publish that failed was reported as a success")
	}
	if len(down.got) != 1 {
		t.Fatalf("%d layers were offered before the restart, want 1", len(down.got))
	}
	minted := down.got[0].CommitID
	st := h.state(t)
	if st.Pending == nil {
		t.Fatal("nothing on disk says this host owes the object store a layer; a restart would mint a second commit id for it")
	}
	switch {
	case st.Pending.CommitID != minted:
		t.Errorf("the record names commit %q and the layer was sealed under %q", st.Pending.CommitID, minted)
	case st.Pending.LayerID != qcow.LayerIDOfImage(sealed):
		t.Errorf("the record names layer %q, want the sealed file's own %q", st.Pending.LayerID, qcow.LayerIDOfImage(sealed))
	}

	// The Agent is killed and started again under the guest, which kept writing to the
	// tip the rotation gave it.
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(h.tip(t))
	up := &recordingPublisher{}
	next := h.restart(t, 8<<20, up)

	if err := next.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("the first cycle after the restart: %v", err)
	}
	if len(up.got) != 1 {
		t.Fatalf("%d layers were offered after the restart, want the one this host owed", len(up.got))
	}
	l := up.got[0]
	switch {
	case l.CommitID != minted:
		t.Errorf("the restarted Agent published under commit %q; the layer was sealed under %q, so the history now has two entries for one layer", l.CommitID, minted)
	case l.Path != sealed:
		t.Errorf("the layer published is %q, want the sealed one %q", l.Path, sealed)
	case l.Epoch != 1:
		t.Errorf("the epoch is %d, want the one this host held when it sealed the layer", l.Epoch)
	}
	// And the debt is discharged: nothing is owed, and the commit is recorded as one
	// whose layer this host holds — which is what lets a later recovery skip its
	// download, because a repointed layer no longer hashes to the object it came from.
	after := next.state(t)
	if after.Pending != nil {
		t.Errorf("the layer is published and the record still owes it: %+v", after.Pending)
	}
	if len(after.Commits) != 1 || after.Commits[0].CommitID != minted ||
		after.Commits[0].LayerID != qcow.LayerIDOfImage(sealed) {
		t.Errorf("the commits this host holds are %+v, want the one it just published", after.Commits)
	}
}

// TestFencingStopsTheGuest is the decision that made fencing mean something: a host told
// it is not this volume's writer, with a VM still writing into the chain, was taking the
// chain out of a map and leaving QEMU to it.
//
// Read-only would be better and was measured to be unavailable — a live guest's virtio-blk
// holds the node read-write and nothing on the host can take that away (qmp.Stop carries
// the two QEMU error messages). Pausing is the smallest true thing an Agent that does not
// own the VM's lifetime can do.
func TestFencingStopsTheGuest(t *testing.T) {
	t.Parallel()
	h := newHarnessRotatingAt(t, 0)
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("preparing: %v", err)
	}
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(h.tip(t))
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("with a guest attached: %v", err)
	}

	if err := h.m.Fence(t.Context(), []string{vol},
		storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST, "the lease expired"); err != nil {
		t.Fatalf("fencing: %v", err)
	}
	if !strings.Contains(h.dialer.sent(), `"execute":"stop"`) {
		t.Errorf("the guest was left writing to a volume this host no longer owns: %s", h.dialer.sent())
	}
	// And the refusal is on disk, so a restart does not forget it. A host that came back
	// and resumed would be the second writer this whole design is arranged against.
	st, err := qcow.ReadState(h.paths, root, vol)
	if err != nil {
		t.Fatalf("reading state.json: %v", err)
	}
	if st.Fenced == nil {
		t.Fatal("the fence was recorded only in memory, which a SIGKILL takes with it")
	}
	if st.Fenced.Epoch != 1 {
		t.Errorf("the fence records epoch %d, want the one this host held", st.Fenced.Epoch)
	}
}

// TestFencingAVolumeWithNoGuestIsNotAFailure: no socket means no guest, and a QEMU that
// has already gone means the writing has already stopped. Neither is a reason for a fence
// to fail — what must not happen is a fence that does not happen because a socket was slow.
func TestFencingAVolumeWithNoGuestIsNotAFailure(t *testing.T) {
	t.Parallel()
	h := newHarnessRotatingAt(t, 0)
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("preparing: %v", err)
	}
	if err := h.m.Fence(t.Context(), []string{vol},
		storagev1.VolumeRefusal_VOLUME_REFUSAL_PUBLISH_FENCED, "HEAD moved"); err != nil {
		t.Fatalf("fencing a volume nothing is attached to: %v", err)
	}
	if v := h.volumes(t)[vol]; v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_PUBLISH_FENCED {
		t.Errorf("the refusal is %v", v.Refusal)
	}
}

// TestRotationNamesTheDiskTheWayThisVMAllowsIt asserts on the bytes that reach QEMU,
// because that is the whole subject: a snapshot has to name the node it acts on, and the
// two kinds of VM leave two different names to do it with.
//
// The `-blockdev` row is the one that was broken. A disk launched that way has no drive
// id, rotation refused it outright, and that is the shape any libvirt-derived runner
// produces — so the launcher we expect to meet was the launcher we could not rotate.
func TestRotationNamesTheDiskTheWayThisVMAllowsIt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		script  func(string) []string
		wants   []string
		unwants []string
	}{
		{
			name:    "launched with -drive: named by its generated drive id",
			script:  attachedTo,
			wants:   []string{`"device":"virtio0"`, `"mode":"existing"`},
			unwants: []string{`"node-name"`, `"snapshot-node-name"`},
		},
		{
			name:   "launched with -blockdev: named by its node, and the overlay named too",
			script: attachedByNode,
			// QEMU refuses the command without the second one — "New overlay node-name
			// missing" — and spin2 rather than spin1 because the graph already holds a
			// spin1 and a name in use is no name at all.
			wants:   []string{`"node-name":"vol"`, `"snapshot-node-name":"spin2"`, `"mode":"existing"`},
			unwants: []string{`"device"`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newHarnessRotatingAt(t, 8<<20)
			if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
				t.Fatalf("preparing: %v", err)
			}
			tip := h.tip(t)
			h.dialer.scripts[qcow.QMPSocket(root, vol)] = tt.script(tip)
			h.paths.sizes[tip] = 9 << 20
			h.runner.info = overlayJSON(size, tip)

			if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
				t.Fatalf("the cycle that should rotate: %v", err)
			}
			if h.tip(t) == tip {
				t.Fatal("the tip did not rotate")
			}
			sent := h.dialer.sent()
			for _, want := range tt.wants {
				if !strings.Contains(sent, want) {
					t.Errorf("the snapshot did not carry %s: %s", want, sent)
				}
			}
			for _, unwant := range tt.unwants {
				if strings.Contains(sent, unwant) {
					t.Errorf("the snapshot carried %s, which this VM cannot be named by: %s", unwant, sent)
				}
			}
		})
	}
}

// withRPO is `active` plus v6 §11's age trigger.
func withRPO(id string, epoch int64, rpo time.Duration) *storagev1.DesiredVolume {
	d := active(id, epoch)
	d.RpoTargetSeconds = int64(rpo.Seconds())
	return d
}

// TestATipOlderThanItsRPOIsCommitted is the age trigger.
//
// The size trigger alone cannot keep an RPO: a volume whose guest writes a megabyte an
// hour never reaches a threshold sized for upload cost, and everything it wrote sits on
// one host until it does. Nothing reports that — the volume is healthy, the Agent is
// doing what it was told — and the promise is broken silently, which is the only way this
// class of promise ever breaks.
func TestATipOlderThanItsRPOIsCommitted(t *testing.T) {
	t.Parallel()
	h := newHarnessRotatingAt(t, 8<<20)
	// A tip well under the size threshold and over the "has been written to" floor.
	tip := h.rotating(t, 2<<20)

	// Inside the RPO: nothing happens, and that is as much of the rule as the trigger.
	h.clk.Advance(4 * time.Minute)
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{withRPO(vol, 1, 5*time.Minute)}); err != nil {
		t.Fatalf("applying: %v", err)
	}
	if got := h.tip(t); got != tip {
		t.Fatalf("a tip inside its RPO was rotated to %q", got)
	}

	// Past it.
	h.clk.Advance(2 * time.Minute)
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{withRPO(vol, 1, 5*time.Minute)}); err != nil {
		t.Fatalf("applying: %v", err)
	}
	if got := h.tip(t); got == tip {
		t.Fatalf("a tip older than its RPO was not rotated: still %q", got)
	}
}

// TestAnIdleVolumeIsNotCommittedByAge is §11's first invariant against the new trigger.
//
// Time passes for a volume nobody writes to, so the age arm needs a condition the size
// arm gets for free. Without it an idle volume seals an empty layer every RPO for ever,
// and the number stops meaning what it says: a volume that wrote nothing is inside its
// target, not behind it.
func TestAnIdleVolumeIsNotCommittedByAge(t *testing.T) {
	t.Parallel()
	h := newHarnessRotatingAt(t, 8<<20)
	// An untouched tip: a freshly created qcow2 is ~193 KiB of header and tables before a
	// guest writes anything.
	tip := h.rotating(t, 200<<10)

	h.clk.Advance(2 * time.Hour)
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{withRPO(vol, 1, time.Minute)}); err != nil {
		t.Fatalf("applying: %v", err)
	}
	if got := h.tip(t); got != tip {
		t.Fatalf("an idle volume was rotated to %q after two hours of doing nothing", got)
	}
	if cmds := h.runner.commands(); len(cmds) != 0 {
		t.Fatalf("an idle volume ran %v", cmds)
	}
}

// TestAVolumeWithNoRPOIsNotCommittedByAge: the age trigger is a per-volume promise, and a
// volume that carries none must behave exactly as it did before the trigger existed.
func TestAVolumeWithNoRPOIsNotCommittedByAge(t *testing.T) {
	t.Parallel()
	h := newHarnessRotatingAt(t, 8<<20)
	tip := h.rotating(t, 2<<20)

	h.clk.Advance(24 * time.Hour)
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("applying: %v", err)
	}
	if got := h.tip(t); got != tip {
		t.Fatalf("a volume with no RPO was rotated by age to %q", got)
	}
}

// TestADetachStopsTheGuestStillWritingToTheVolume.
//
// A volume leaving the desired state is this host being told it is not the volume's
// writer any more — a detach, a promotion, a fleet decision. Releasing it in memory and
// leaving QEMU attached is the same half-measure fencing had before Fence learned to stop
// the guest: every byte written from here lands in a chain nothing will ever publish, and
// the guest is told each one succeeded.
//
// It is asserted on QMP and not on the map, because the map is the part that was already
// right. The whole defect is a volume correctly forgotten by an Agent whose QEMU is still
// writing to it.
func TestADetachStopsTheGuestStillWritingToTheVolume(t *testing.T) {
	t.Parallel()
	h := newHarnessRotatingAt(t, 8<<20)
	h.rotating(t, 1<<20) // served, with a VM attached at its socket
	h.dialer.reset()

	// The volume leaves the desired state entirely, which is what a detach looks like to
	// an Agent: it is simply not listed any more.
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{}); err != nil {
		t.Fatalf("applying an empty desired state: %v", err)
	}

	if _, held := h.volumes(t)[vol]; held {
		t.Fatalf("the volume is still being served after it left the desired state")
	}
	if sent := h.dialer.sent(); !strings.Contains(sent, `"execute":"stop"`) {
		t.Fatalf("the guest was left writing to a volume this host stopped serving; QMP saw: %s", sent)
	}
}

// TestAChainWhoseDirectoryVanishedIsRefusedAndNotRebuiltUnderTheGuest.
//
// Somebody rm -rf'd the volume's layers while a guest was writing to them: an operator
// reclaiming space, a stray cleanup, a filesystem that came back empty after a crash. The
// files are gone and QEMU still holds open descriptors to them, so the guest goes on
// reading and writing bytes that have no name any more.
//
// The wrong answer is to notice the absence and prepare a fresh chain, which is what
// "make the desired state true" reads like from inside a reconciler. That hands the same
// volume id a second, empty chain while the first one is still being written to through
// the open descriptors, and whichever is published is missing the other's writes with no
// error anywhere. The volume is refused instead, loudly, and the refusal names the bucket
// rather than the code: it is IMAGE_MISSING, whose own proto comment tells an operator to
// go and look there.
func TestAChainWhoseDirectoryVanishedIsRefusedAndNotRebuiltUnderTheGuest(t *testing.T) {
	t.Parallel()
	h := newHarnessRotatingAt(t, 8<<20)
	tip := h.rotating(t, 1<<20)

	// The layers go, and so does the pointer that names the tip — that is what removing
	// the directory does. The QMP script stays: QEMU is unaffected by a file being
	// unlinked under it and answers exactly as before.
	h.paths.remove(tip)
	h.paths.remove(qcow.ActivePointer(root, vol))

	// The Agent restarts, because that is what makes this the interesting case: an Agent
	// that still holds the chain in memory is not being asked the question.
	h2 := h.restart(t, 8<<20, nil)
	_ = h2.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)})

	got := h2.volumes(t)[vol]
	if got.Refusal == storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Fatalf("a volume whose layers vanished is being served as if nothing happened: %+v", got)
	}
	if got.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_IMAGE_MISSING {
		t.Fatalf("refusal = %s, want IMAGE_MISSING — the operator's next move is to look in the bucket", got.Refusal)
	}
	if strings.Contains(strings.Join(h2.runner.commands(), " "), "create") {
		t.Fatalf("a fresh chain was created under a guest still writing to the old one: %v", h2.runner.commands())
	}
}

// TestAVanishedTipIsNoticedWithNoRotationConfigured is the same disappearance on an Agent
// that rotates nothing — `-rotate-at-bytes 0` and no RPO, which is what `demo:stage1`
// runs and what an Agent with no object store has.
//
// It is a separate case because the two checks that can notice sit on different paths:
// one measures the tip when deciding whether to rotate, and never runs here, and the
// other stats what QEMU reports having open. Without this, a configuration that is
// nobody's edge case — the default one — kept serving a volume whose layers were gone,
// and the test above passed the whole time.
func TestAVanishedTipIsNoticedWithNoRotationConfigured(t *testing.T) {
	t.Parallel()
	h := newHarness(t) // rotateAt 0: nothing here ever measures the tip
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("preparing: %v", err)
	}
	tip := h.tip(t)
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(tip)
	h.paths.remove(tip)

	_ = h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)})

	if got := h.volumes(t)[vol]; got.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_IMAGE_MISSING {
		t.Fatalf("refusal = %s, want IMAGE_MISSING: an Agent that rotates nothing never measures the tip, so nothing else here can notice its layers are gone", got.Refusal)
	}
}

// withSnapshot is `active` plus a §19 request.
func withSnapshot(id string, epoch int64, snapshotID string) *storagev1.DesiredVolume {
	d := active(id, epoch)
	d.PendingSnapshotId = snapshotID
	return d
}

// TestASnapshotNamesTheCommitCarryingTheLayerSealedForIt.
//
// A snapshot under v6 is a name for a commit, and the only commit that may answer a
// request is the one carrying the layer sealed *because of* it. The obvious
// implementation — name the newest commit — passes every test that does not have another
// trigger running, and is wrong exactly when one is: a size-triggered rotation that began
// before the request arrived produces a commit that is missing everything written since,
// and reporting it says nothing about the difference.
//
// `demo:stage5` is where that showed up, one second after the request, against a guest
// writing hard enough to cross the size threshold every cycle.
func TestASnapshotNamesTheCommitCarryingTheLayerSealedForIt(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{}
	h := newHarnessFull(t, 8<<20, pub)
	// A tip well under the size threshold: nothing but the request can seal this.
	tip := h.rotating(t, 2<<20)
	h.paths.sizes[tip] = 2 << 20

	snapID := ids.New().String()
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{withSnapshot(vol, 1, snapID)}); err != nil {
		t.Fatalf("the cycle that should seal and publish for the snapshot: %v", err)
	}

	if len(pub.got) != 1 {
		t.Fatalf("%d layers were published, want the one sealed for the snapshot", len(pub.got))
	}
	sealed := pub.got[0]
	if sealed.LayerID != qcow.LayerIDOfImage(tip) {
		t.Fatalf("the layer published is %q, want the tip that was current when the request arrived, %q",
			sealed.LayerID, qcow.LayerIDOfImage(tip))
	}
	got := h.volumes(t)[vol]
	if got.SnapshotID != snapID {
		t.Fatalf("the report answers snapshot %q, want %q", got.SnapshotID, snapID)
	}
	if got.SnapshotError != "" {
		t.Fatalf("the snapshot failed: %s", got.SnapshotError)
	}
	if got.SnapshotCommitID != sealed.CommitID {
		t.Fatalf("the snapshot names commit %q; the layer sealed for it went into %q",
			got.SnapshotCommitID, sealed.CommitID)
	}
}

// TestASnapshotIsNotAnsweredByACommitThatPredatesIt is the same rule seen from the side
// that a "name the newest commit" implementation gets wrong.
//
// The volume already has a published history when the request arrives. Answering with the
// commit at the end of it would be instant, would look right, and would name a point
// before everything the guest wrote since — which is the whole of what a snapshot is for.
func TestASnapshotIsNotAnsweredByACommitThatPredatesIt(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{}
	h := newHarnessFull(t, 8<<20, pub)
	tip := h.rotating(t, 9<<20)
	h.paths.sizes[tip] = 9 << 20
	// One cycle with no request: the size trigger seals and publishes, so the volume now
	// has a history and the newest commit predates anything below.
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("the first cycle: %v", err)
	}
	if len(pub.got) != 1 {
		t.Fatalf("precondition: %d commits, want one before the request", len(pub.got))
	}
	before := pub.got[0].CommitID

	// The guest follows the rotation, and the fake has to be told: the QMP script is what
	// the Manager reads the live tip from, and left pointing at the sealed layer it
	// re-adopts that file every cycle — so the tip looks 9 MiB for ever, the size trigger
	// fires for ever, and a test about *not* rotating is exercising a volume that rotates
	// on its own. Both of this file's snapshot tests were written that way and neither
	// could fail; the plants said so.
	next := h.tip(t)
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(next)
	// A few hundred kilobytes: more than the ~193 KiB an empty qcow2 already occupies, so
	// the guest really did write; less than minRotateAtBytes, which is what the automatic
	// triggers call "has been written to". This is the size that discriminates — 2 MiB
	// sits above that floor and would let a floored snapshot arm pass this test.
	h.paths.sizes[next] = 300 << 10
	// And what `qemu-img info` says about the overlay the next rotation creates, which
	// is checked against the tip it was supposed to be built on.
	h.runner.info = overlayJSON(size, next)

	snapID := ids.New().String()
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{withSnapshot(vol, 1, snapID)}); err != nil {
		t.Fatalf("the cycle carrying the request: %v", err)
	}
	got := h.volumes(t)[vol]
	if got.SnapshotCommitID == before {
		t.Fatalf("the snapshot was answered by commit %s, which was published before the request arrived", before)
	}
	if got.SnapshotCommitID == "" {
		t.Fatal("the snapshot was not answered at all")
	}
}

// TestASnapshotOnAHostWithNoObjectStoreIsRefusedRatherThanAwaited.
//
// Nothing can be published, so no commit can ever exist for the request to name. Waiting
// is not a neutral choice here: the request keeps arriving, the rotation arm fires on the
// request alone, and the volume is ground into one layer per cycle for as long as the
// Control Plane keeps asking.
func TestASnapshotOnAHostWithNoObjectStoreIsRefusedRatherThanAwaited(t *testing.T) {
	t.Parallel()
	h := newHarnessRotatingAt(t, 8<<20) // no publisher
	tip := h.rotating(t, 2<<20)
	h.paths.sizes[tip] = 2 << 20

	snapID := ids.New().String()
	for range 3 {
		if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{withSnapshot(vol, 1, snapID)}); err != nil {
			t.Fatalf("applying: %v", err)
		}
	}
	got := h.volumes(t)[vol]
	if got.SnapshotID != snapID || got.SnapshotError == "" {
		t.Fatalf("a snapshot with nowhere to publish was not refused: %+v", got)
	}
	if got.SnapshotCommitID != "" {
		t.Fatalf("a refused snapshot names commit %q", got.SnapshotCommitID)
	}
	// And the tip was left alone. Three cycles of sealing would be three layers.
	if h.tip(t) != tip {
		t.Fatalf("the tip moved to %q: the request sealed a layer nothing can publish", h.tip(t))
	}
}

// TestAForkedChainStopsTheGuestWritingIntoIt.
//
// The object store's history has moved to a commit this host never wrote, and a guest is
// still writing into the local chain. Every byte from here lands in a history nobody will
// publish, and the guest is told each one succeeded — the same statement fencing makes,
// arrived at from the object store instead of from the Control Plane.
//
// Refusing the volume is not enough and asserting on the refusal does not see the
// difference: the chain cannot be replaced while QEMU holds the file, so without the stop
// the next cycle finds the same live image, refuses again, and the volume is stuck for as
// long as the guest runs — the rebuild is only reachable once the guest is gone. A
// mutation sweep deleted the stop call and nothing went red.
func TestAForkedChainStopsTheGuestWritingIntoIt(t *testing.T) {
	t.Parallel()
	h := newHarnessRotatingAt(t, 8<<20)
	tip := h.rotating(t, 1<<20)

	// A fresh Agent over the same filesystem: this is the cycle that opens the chain, and
	// the guest is still attached at the socket.
	h2 := h.restart(t, 8<<20, nil)
	// The bucket says the volume's newest commit is one this host has no record of.
	h2.rec.head = ids.New().String()
	h2.dialer.reset()

	_ = h2.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)})

	if sent := h2.dialer.sent(); !strings.Contains(sent, `"execute":"stop"`) {
		t.Fatalf("the guest is still writing into a chain the published history has moved past; QMP saw: %s", sent)
	}
	if got := h2.volumes(t)[vol]; got.Refusal == storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Fatalf("a volume on a forked chain is still being served: %+v", got)
	}
	if h2.tip(t) != tip {
		t.Fatalf("the chain was replaced under a guest that still had it open: tip is now %q", h2.tip(t))
	}
}

// TestASnapshotWaitsForItsOwnCommitToLand.
//
// The rotation this request asked for has happened and the publish has not. Answering now
// would name the commit at the end of the history — the one that landed *before* this
// request — and a restore of that snapshot silently yields a point missing everything the
// guest wrote since it was asked for.
//
// This is the guard in settleSnapshot, and a mutation sweep found nothing asserted it: no
// existing test reaches a cycle where a layer is sealed for the request and still
// unpublished, because the recording publisher always succeeds.
func TestASnapshotWaitsForItsOwnCommitToLand(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{}
	h := newHarnessFull(t, 8<<20, pub)
	tip := h.rotating(t, 9<<20)
	h.paths.sizes[tip] = 9 << 20
	// One cycle with no request, so the volume has a history the wrong answer could name.
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("the first cycle: %v", err)
	}
	before := pub.got[0].CommitID

	// The guest follows the rotation, and the object store goes down.
	next := h.tip(t)
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(next)
	h.paths.sizes[next] = 300 << 10
	h.runner.info = overlayJSON(size, next)
	pub.err = errors.New("dial tcp: connection refused")

	snapID := ids.New().String()
	_ = h.m.Apply(t.Context(), []*storagev1.DesiredVolume{withSnapshot(vol, 1, snapID)})

	got := h.volumes(t)[vol]
	if got.SnapshotCommitID != "" {
		t.Fatalf("the snapshot was answered with commit %q while the layer sealed for it was still unpublished (the history ended at %q)",
			got.SnapshotCommitID, before)
	}

	// And once the store comes back it is answered, with the new commit and not the old.
	pub.err = nil
	_ = h.m.Apply(t.Context(), []*storagev1.DesiredVolume{withSnapshot(vol, 1, snapID)})
	got = h.volumes(t)[vol]
	switch got.SnapshotCommitID {
	case "":
		t.Fatal("the snapshot was never answered once the object store came back")
	case before:
		t.Fatalf("the snapshot names %q, the commit that landed before it was asked for", before)
	}
}

// TestTheReportCarriesHowFarBehindTheBucketAVolumeIs.
//
// §28 calls last_successful_commit_age the most important measurement here, and the reason
// is that it IS the RPO: lose the host now and this is what the tenant loses. Until it was
// reported, a volume that had not committed for an hour looked on the wire exactly like
// one that committed a second ago — and the bytes waiting on the next commit, which is
// what those seconds cost, were not on the wire at all.
func TestTheReportCarriesHowFarBehindTheBucketAVolumeIs(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{}
	h := newHarnessFull(t, 8<<20, pub)
	tip := h.rotating(t, 9<<20)
	h.paths.sizes[tip] = 9 << 20

	// Before anything is published: no age to report, and the tip is already local bytes
	// that would be lost with the host.
	if got := h.volumes(t)[vol]; got.LastCommitAge != 0 {
		t.Fatalf("a volume that has never committed reports an age of %s", got.LastCommitAge)
	} else if got.UnpublishedLocalBytes == 0 {
		t.Fatal("the tip holds the guest's writes and the report says nothing is unpublished")
	}

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("the cycle that rotates and publishes: %v", err)
	}
	next := h.tip(t)
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(next)
	h.paths.sizes[next] = 3 << 20
	h.runner.info = overlayJSON(size, next)

	h.clk.Advance(7 * time.Minute)
	got := h.volumes(t)[vol]
	if got.LastCommitAge != 7*time.Minute {
		t.Fatalf("last commit age = %s, want the 7 minutes since the commit landed", got.LastCommitAge)
	}
	// The new tip, and nothing else: the layer that was published is not owed any more.
	if got.UnpublishedLocalBytes != 3<<20 {
		t.Fatalf("unpublished = %d bytes, want the %d the tip holds", got.UnpublishedLocalBytes, 3<<20)
	}
}

// TestTheReportedRPOIsWhatSection11Defines, which is not "how long since the last commit".
//
// §11's definition is the age of the newest commit that *covers everything written*, and
// the difference is not pedantry — it is both directions of a wrong answer, and the whole
// point of the number is that an operator can trust it:
//
//   - a volume nobody writes to is inside its RPO, not falling further behind for ever.
//     Reported the other way, every idle volume in a fleet eventually reads as a volume
//     about to lose data, and the alert that fires on all of them gets turned off;
//   - a volume whose guest has written since the last commit is exposed for as long as
//     that has been true. Reported as "0s just after a commit", the number goes to zero
//     at the exact moment the exposure starts growing again.
//
// The condition is the same one the age trigger uses to decide a tip has been written to,
// deliberately: a volume can then never report itself past a target the trigger considers
// it idle for, which is the pair of numbers contradicting each other in the catalog.
func TestTheReportedRPOIsWhatSection11Defines(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		tipBytes int64
		want     time.Duration
	}{
		// A freshly created qcow2 is ~193 KiB of header and tables before a guest writes
		// anything, which is why "untouched" is not "zero bytes".
		{"a volume nobody has written to is inside its RPO", 200 << 10, 0},
		{"a volume written to since its last commit is exposed for as long as that", 2 << 20, time.Hour},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// With a publisher, so a commit actually lands and the volume has an anchor
			// to be measured from. Without one nothing is ever published, and both cases
			// report zero for the honest reason that this host has never committed.
			h := newHarnessFull(t, 8<<20, &recordingPublisher{})
			h.rotating(t, 9<<20)
			if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
				t.Fatalf("the cycle that commits: %v", err)
			}
			// The guest follows the rotation onto the new tip, and grows it by the amount
			// under test.
			tip := h.tip(t)
			h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(tip)
			h.paths.sizes[tip] = tc.tipBytes
			h.clk.Advance(time.Hour)
			if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
				t.Fatalf("applying: %v", err)
			}

			got := h.volumes(t)[vol].LastCommitAge
			if got != tc.want {
				t.Fatalf("reported RPO %s, want %s", got, tc.want)
			}
		})
	}
}

// TestAChainAtTheCeilingStopsRotatingInsteadOfBecomingUnrecoverable.
//
// Rotation is what makes a chain deeper, and past qcow.MaxLayers a chain cannot be rebuilt
// on another host — so a volume that rotates past it is a volume that has quietly stopped
// being recoverable, with no error anywhere and the guest none the wiser. Compaction is
// what is supposed to keep the ceiling out of reach, and it cannot run while a guest holds
// the files, so a busy volume that never detaches walks towards it.
//
// The trade when it arrives: the tip grows instead. That degrades the RPO — the writes
// past the last commit are the ones the host still owes — and it shows up in the pair §11
// asks be watched, `commit_age` and `unpublished_local_bytes`, both of which keep climbing.
// The other side of the trade is a volume nothing can restore, so it is not close.
func TestAChainAtTheCeilingStopsRotatingInsteadOfBecomingUnrecoverable(t *testing.T) {
	t.Parallel()
	h := newHarnessFull(t, 8<<20, &recordingPublisher{})
	tip := h.rotating(t, 9<<20)

	// A record whose chain is already as deep as anything can rebuild: a published commit
	// per layer under the tip, which is what a volume that has been rotating for a long
	// time actually looks like.
	st, err := qcow.ReadState(h.paths, root, vol)
	if err != nil {
		t.Fatal(err)
	}
	st.Commits = nil
	for range qcow.MaxLayers - 1 {
		st.Commits = append(st.Commits, qcow.CommitLayer{CommitID: ids.New().String(), LayerID: ids.New().String()})
	}
	if err := qcow.WriteState(h.paths, root, vol, st); err != nil {
		t.Fatal(err)
	}

	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("applying: %v", err)
	}
	if got := h.tip(t); got != tip {
		t.Fatalf("a chain at the ceiling rotated to %q, so it can no longer be rebuilt anywhere", got)
	}
	// And the volume is still served: refusing it would take a guest's disk away over
	// bookkeeping, and every byte on it is still readable and still local.
	v, ok := h.volumes(t)[vol]
	if !ok {
		t.Fatal("the volume stopped being reported")
	}
	if v.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Fatalf("a volume at the depth ceiling was refused: %v %q", v.Refusal, v.RefusalDetail)
	}
}

// A restart between sealing a layer and publishing it does not mint a second commit id
// for it.
//
// It used to, and STATUS carried the cost: the id a sealed layer was promised under lived
// in memory, and a process that died before writing it down had nothing to reuse. Two
// things closed it, at different ends. The record is written the moment a layer is sealed
// (recordPending), so a restart finds the id rather than inventing one; and a publish no
// longer clears the pending layer until the note of the commit has landed, so a host that
// cannot write that note republishes under the id it already used, which commit.Publish
// answers from the manifest that is already there.
//
// Asserted on the ids that reached the object store, because that is where a duplicate
// would be: an extra commit for bytes already published, on a chain a recovery walks.
func TestARestartBetweenSealingAndPublishingMintsNoSecondCommitID(t *testing.T) {
	t.Parallel()
	pub := &recordingPublisher{}
	h := newHarnessFull(t, 8<<20, pub)
	h.rotating(t, 9<<20)

	// The cycle that seals. The publisher refuses, so the layer stays owed with its id
	// written down and nothing in the bucket yet.
	pub.err = errors.New("503 Service Unavailable")
	if err := h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err == nil {
		t.Fatal("the cycle whose publish was refused reported success")
	}
	sealed := h.state(t).Pending
	if sealed == nil {
		t.Fatal("the sealed layer this test is about was not recorded as owed")
	}

	// The process dies and comes back: everything in memory is gone and the record is all
	// there is.
	next := h.restart(t, 8<<20, pub)
	next.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(next.tip(t))
	pub.err = nil
	for range 3 {
		if err := next.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
			t.Fatalf("a cycle after the restart: %v", err)
		}
	}

	ids := map[string]bool{}
	for _, l := range pub.got {
		if l.LayerID == sealed.LayerID {
			ids[l.CommitID] = true
		}
	}
	switch {
	case len(ids) == 0:
		t.Fatalf("the layer sealed before the restart (%s) never reached the object store: %+v", sealed.LayerID, pub.got)
	case len(ids) > 1:
		t.Fatalf("layer %s was published under %d commit ids across a restart: %v", sealed.LayerID, len(ids), ids)
	}
	if !ids[sealed.CommitID] {
		t.Fatalf("the layer was published under an id the record did not promise: %v, recorded %s", ids, sealed.CommitID)
	}
}
