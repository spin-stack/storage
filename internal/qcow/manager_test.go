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

func newHarnessOn(t *testing.T, d *sim.Disk) *harness {
	t.Helper()
	return newHarnessWith(t, d, 0, nil)
}

func newHarnessWith(t *testing.T, d *sim.Disk, rotateAt int64, pub qcow.Publisher) *harness {
	t.Helper()
	h := &harness{
		runner: &fakeRunner{info: infoJSON("qcow2", size, false), version: "qemu-img version 11.0.2"},
		paths:  newPaths(),
		dialer: &fakeDialer{scripts: map[string][]string{}},
		disk:   d,
	}
	m, err := qcow.New(t.Context(), qcow.Config{
		Root: root, QemuImg: "/qemu-img", ProbeTimeout: time.Second, RotateAtBytes: rotateAt,
	}, qcow.Deps{
		Clock:     sim.NewClock(time.Unix(0, 0)),
		Disk:      d,
		Runner:    h.runner,
		Paths:     h.paths,
		Dialer:    h.dialer,
		Publisher: pub,
	})
	if err != nil {
		t.Fatalf("building a manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	// The start-up `--version` run is not part of what any test below is about.
	h.runner.reset()
	h.m = m
	return h
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
	if !strings.HasPrefix(image, qcow.LayersDir(root, vol)+"/") {
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
	// The watermarks are zero and mean nothing yet: they were the write-ahead log's
	// counters and get their meaning back from the commit protocol. Asserted, because a
	// number invented here is a fencing decision made on a fiction.
	if v.LocalSequence|v.DurableSequence|v.PublishedSequence != 0 {
		t.Errorf("a watermark was invented: local=%d durable=%d published=%d",
			v.LocalSequence, v.DurableSequence, v.PublishedSequence)
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
	image := qcow.LayerImage(root, vol, layerID)
	h.dialer.scripts[qcow.QMPSocket(root, vol)] = attachedTo(image)
	h.paths.present[image] = true
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
		Runner: &fakeRunner{version: "qemu-img version 11.0.2"}, Paths: newPaths(), Dialer: &fakeDialer{},
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
				Runner: &fakeRunner{version: "qemu-img version 11.0.2"}, Paths: newPaths(), Dialer: &fakeDialer{}}
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
					Runner: tt.runner, Paths: newPaths(), Dialer: &fakeDialer{}})
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
	if !strings.HasPrefix(next, qcow.LayersDir(root, vol)+"/") {
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
	// The pointer ran ahead, and then everything stopped.
	ahead := qcow.LayerImage(root, vol, nextID)
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
