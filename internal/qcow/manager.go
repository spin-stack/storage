package qcow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/qmp"
	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/disk"
)

// lockFile is this Manager's claim on the data directory. The name is part of the
// operator's world — it is what a human looks for to find out whether an Agent is
// holding a directory — so it is a constant here rather than a path built at a call
// site.
const lockFile = "agent.lock"

// Config is what a Manager needs to be told.
type Config struct {
	// Root is the Agent's data directory, as an absolute path. It is the same
	// directory Deps.Disk is rooted at, and both are needed for a reason worth stating:
	// the Disk is a namespace this process reads and writes inside, while Root is what
	// gets handed to *other* processes — `qemu-img` on the command line, and whoever
	// launches QEMU. A rooted namespace cannot produce that string, by design.
	Root string
	// QemuImg is the path to the `qemu-img` binary. Configured rather than resolved
	// from PATH: v6 pins QEMU to one version for CI and production, and a chain created
	// by whichever qemu-img a login shell happened to find is a chain nobody pinned.
	QemuImg string
	// ProbeTimeout bounds one exchange with QEMU over QMP, and one run of `qemu-img`.
	// Both are another process answering, and neither is allowed to park the Agent's
	// reconciliation cycle: a heartbeat that does not go out is a lease that lapses.
	ProbeTimeout time.Duration
}

// Validate reports what is missing.
func (c Config) Validate() error {
	switch {
	case c.Root == "":
		return errors.New("qcow: a data directory is required")
	case !filepath.IsAbs(c.Root):
		// Relative paths are refused rather than resolved: this string is handed to
		// other processes, whose working directory is not ours, and a chain that
		// resolves differently for QEMU than for the Agent is the `--data-dir applied
		// twice` defect with a second process added to it.
		return fmt.Errorf("qcow: the data directory must be absolute, got %q", c.Root)
	case c.QemuImg == "":
		return errors.New("qcow: a qemu-img binary is required")
	case c.ProbeTimeout <= 0:
		return errors.New("qcow: the probe timeout must be positive")
	}
	return nil
}

// Deps are the Manager's injected collaborators (INV-01).
type Deps struct {
	Clock  clock.Clock
	Disk   disk.Disk
	Runner Runner
	Paths  Paths
	Dialer qmp.Dialer
}

// Manager owns this host's local qcow2 chains. It is the implementation of
// agent.VolumeReconciler: the loop hands it the desired state, it makes each volume's
// chain exist and be usable, and it reports what it observed.
//
// It is a self-contained type that the loop *uses*, never a method on the loop
// (ADR-0021): spin's runner takes this without the heartbeat, the lease or the Control
// Plane client. That is also why it takes the data directory's lock itself — see New.
type Manager struct {
	cfg    Config
	clk    clock.Clock
	run    Runner
	paths  Paths
	dialer qmp.Dialer
	unlock io.Closer

	mu   sync.Mutex
	vols map[string]*volume
}

// volume is one volume this host holds.
type volume struct {
	id    string
	epoch int64
	// chain is nil while the volume is not being served — it was refused, or given up.
	chain *Chain
	// attached is what QEMU last said: a VM at this volume's QMP socket has the active
	// image open. It is an observation of another process and never a claim of ours,
	// which is why it is refreshed every cycle rather than latched at attach.
	attached bool
	refusal  storagev1.VolumeRefusal
	detail   string
}

// New validates the wiring, claims the data directory, and returns a Manager.
//
// # It takes the lock, and cmd/volume-agent no longer does
//
// v6 §10 opens with one Agent per host, and two live Agents on one data directory is
// not a hypothetical: both incarnations would prepare chains under the same paths and
// hand the same qcow2 file to two different QEMUs, which is the one way this design
// loses a guest's data locally. The lock was taken in `main` while nothing else owned
// the directory's layout; this type does now, so it takes it here.
//
// The move is the point rather than a tidy-up. ADR-0021 promises spin's runner can take
// this type without the loop around it, and a claim on the directory that lives in a
// `main` is a step the runner would have to know to repeat — which is exactly the shape
// of every defect in CLAUDE.md's table: one line in a `main`, invisible to every test
// that builds the type itself.
func New(ctx context.Context, cfg Config, deps Deps) (*Manager, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	switch {
	case deps.Clock == nil:
		return nil, errors.New("qcow: a clock must be injected (INV-01)")
	case deps.Disk == nil:
		return nil, errors.New("qcow: a disk must be injected (INV-01)")
	case deps.Runner == nil:
		return nil, errors.New("qcow: a process runner must be injected (INV-01)")
	case deps.Paths == nil:
		return nil, errors.New("qcow: a path accessor must be injected (INV-01)")
	case deps.Dialer == nil:
		return nil, errors.New("qcow: a QMP dialer must be injected (INV-01)")
	}
	unlock, err := deps.Disk.Lock(lockFile)
	if err != nil {
		if errors.Is(err, disk.ErrLocked) {
			return nil, fmt.Errorf("another Volume Agent is already using %s (v6 §10: one Agent per host): %w", cfg.Root, err)
		}
		return nil, fmt.Errorf("claiming %s: %w", cfg.Root, err)
	}
	m := &Manager{
		cfg: cfg, clk: deps.Clock, run: deps.Runner,
		paths: deps.Paths, dialer: deps.Dialer, unlock: unlock,
		vols: map[string]*volume{},
	}

	// qemu-img is run once, here, and the answer is logged.
	//
	// It is a start-up refusal rather than a per-volume one because the failure it
	// catches is an operator's — a path that is not there, not executable, or not
	// qemu-img — and an Agent that discovers that when the first volume arrives has
	// already registered as a healthy host and refuses a volume for a reason nobody was
	// told about at start-up. This repository has taken the same decision once before,
	// for a socket directory that could never be bound.
	//
	// The version is printed rather than compared. v6 pins one QEMU for CI and
	// production, and asserting the pin here would put a version string in a binary that
	// then has to be edited in lock-step with the Taskfile; what an operator needs is to
	// be able to *see* which qemu-img this Agent will hand its chains to.
	version, err := m.qemuImgVersion(ctx)
	if err != nil {
		return nil, errors.Join(err, unlock.Close())
	}
	slog.Info("qemu-img is usable: every qcow2 chain on this host is created and inspected by it",
		"qemu_img", cfg.QemuImg, "version", version)
	return m, nil
}

// qemuImgVersion runs `qemu-img --version` and returns its first line.
func (m *Manager) qemuImgVersion(ctx context.Context) (string, error) {
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()

	out, err := m.run.Run(ctx, m.cfg.QemuImg, "--version")
	if err != nil {
		return "", fmt.Errorf("qcow: %s cannot be run, and every chain on this host needs it: %w", m.cfg.QemuImg, err)
	}
	line, _, _ := strings.Cut(strings.TrimSpace(string(out)), "\n")
	if line == "" {
		return "", fmt.Errorf("qcow: %s answered --version with nothing; it is not qemu-img", m.cfg.QemuImg)
	}
	return line, nil
}

// Close releases the data directory.
//
// The kernel drops an flock when the process dies, so this is for the paths that return
// rather than for a crash — nothing has to clean up after one, which is what makes the
// lock usable for a data directory at all.
func (m *Manager) Close() error { return m.unlock.Close() }

// Apply converges this host on the desired state: every volume in it has a chain that
// exists and is usable, and every volume that left it is released.
//
// A volume that cannot be prepared does not end the pass. Its refusal is recorded and
// the rest are attempted, because the loop reports what this Manager holds *after*
// Apply returns — so a volume that failed here is precisely the one with something to
// say, and stopping at the first would silence every volume behind it.
func (m *Manager) Apply(ctx context.Context, desired []*storagev1.DesiredVolume) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// The mutex is held across `qemu-img` and QMP, and that is safe rather than
	// careless: the loop drives Apply and Volumes from one goroutine, so nothing is
	// waiting on this lock. It exists so that a future concurrent reader sees a whole
	// desired state rather than half of one.
	live := make(map[string]bool, len(desired))
	var failures []error
	for _, d := range desired {
		id := d.GetVolumeId()
		if id == "" {
			failures = append(failures, errors.New("qcow: the desired state carries a volume with no id"))
			continue
		}
		// Only an ACTIVE volume is served, and a volume in any other state is left out
		// of `live` so the loop below stops it by the one path that stops anything. The
		// desired state lists every volume whose primary is this host, including the
		// ones the fleet is in the middle of taking away — FENCING_WAIT is a volume
		// being moved to somebody else — and preparing a chain for one of those is this
		// host getting ready to write a volume it is losing.
		//
		// It also stops *reporting* them, which is the half that matters more: an
		// unset refusal is this host saying "I am serving this", the report is accepted
		// on a host-and-epoch predicate that a FENCING_WAIT volume still satisfies, and
		// the fleet would read the outgoing writer as healthy for the whole wait.
		if d.GetState() != storagev1.VolumeState_VOLUME_STATE_ACTIVE {
			continue
		}
		live[id] = true
		if err := m.ensure(ctx, d); err != nil {
			failures = append(failures, err)
		}
	}
	for id, v := range m.vols {
		if live[id] {
			continue
		}
		// Released, not deleted. The files stay: local persistence is what Stage 1 is,
		// a volume leaves the desired state for reasons that reverse (a promotion, a
		// detach, a fleet decision made a second ago), and this Agent is not the
		// authority on whether a guest's disk should stop existing. Reclaiming the
		// space is a verb the Control Plane will ask for.
		delete(m.vols, id)
		slog.Info("released a volume: this host is no longer serving it, and its local image is kept",
			"volume_id", id, "epoch", v.epoch, "image", ActiveImage(m.cfg.Root, id))
	}
	return errors.Join(failures...)
}

// ensure makes one desired volume's chain exist and be usable. Callers hold m.mu.
func (m *Manager) ensure(ctx context.Context, d *storagev1.DesiredVolume) error {
	id := d.GetVolumeId()
	v, held := m.vols[id]
	if !held {
		v = &volume{id: id}
		m.vols[id] = v
	}

	// A volume this host gave up comes back only at a *higher* epoch, which is the rule
	// the loop's own teardown is written against: a lease that lapsed is given up, and
	// resuming on the strength of a desired state read before a promotion nobody heard
	// about is the failure that rule exists to prevent. The Control Plane granting the
	// volume again is what raises the epoch.
	if v.refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED && d.GetEpoch() <= v.epoch {
		return nil
	}

	v.epoch = d.GetEpoch()
	image := ActiveImage(m.cfg.Root, id)

	// QEMU is asked first, and the order is load-bearing. If a VM already has this
	// image open — the Agent restarted while the guest kept running — then no offline
	// tool may touch the file (v6 §5), and `qemu-img info` would in fact fail on
	// QEMU's write lock. QEMU having opened it is a stronger statement about the image
	// than any check made from here.
	attached, foreign, err := m.probe(ctx, id, image)
	if err != nil {
		return m.refuse(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_ATTACH_FAILED, err)
	}
	if foreign != "" {
		return m.refuse(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_ATTACH_FAILED,
			fmt.Errorf("%w: it has %q open, this volume's image is %q", ErrForeignImage, foreign, image))
	}

	if v.chain != nil {
		// Already serving. The chain is not re-checked: the file may be open, and the
		// answer would not change if it were not.
		m.setAttached(v, attached)
		return nil
	}

	// Bounded like the QMP exchange above, and for the same reason: `qemu-img` is
	// another process, and a cycle that does not finish is a lease that does not get
	// renewed.
	openCtx, cancel := m.withTimeout(ctx)
	defer cancel()
	chain, err := Open(openCtx, m.run, m.paths, m.cfg.QemuImg, m.cfg.Root, id, d.GetSizeBytes(), attached)
	if err != nil {
		return m.refuse(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_ATTACH_FAILED, err)
	}
	v.chain = chain
	v.refusal, v.detail = storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED, ""
	slog.Info("volume ready: the chain is prepared and this is where the VM attaches to it",
		"volume_id", id, "epoch", v.epoch, "image", chain.Active,
		"size_bytes", chain.SizeBytes, "qmp_socket", QMPSocket(m.cfg.Root, id),
		"attached", attached)
	v.attached = attached
	return nil
}

// refuse records why a volume is not being served and returns the error for the caller
// to collect. The refusal is what reaches the Control Plane; the error is what reaches
// the log and the cycle's backoff.
func (m *Manager) refuse(v *volume, why storagev1.VolumeRefusal, err error) error {
	v.chain, v.attached = nil, false
	v.refusal, v.detail = why, err.Error()
	slog.Error("refusing a volume", "volume_id", v.id, "epoch", v.epoch, "error", err)
	return fmt.Errorf("volume %s: %w", v.id, err)
}

// setAttached records what QEMU said and logs the transitions, not the steady state. A
// line per volume per heartbeat would bury the two moments that matter — a guest
// arriving, and a guest going away — in a stream nobody reads.
func (m *Manager) setAttached(v *volume, attached bool) {
	if v.attached == attached {
		return
	}
	v.attached = attached
	if attached {
		slog.Info("a VM has attached to this volume: QEMU reports the active image open",
			"volume_id", v.id, "image", ActiveImage(m.cfg.Root, v.id))
		return
	}
	slog.Info("no VM is attached to this volume any more: nothing answers at its QMP socket",
		"volume_id", v.id, "qmp_socket", QMPSocket(m.cfg.Root, v.id))
}

// probe asks the QEMU at this volume's QMP socket what it has open.
//
// Three answers, and they are different things: attached (a VM has our image open), not
// attached with no error (nothing is listening — the ordinary state of a prepared
// volume whose VM has not been launched), and a foreign file (something is running at
// this volume's socket against an image we did not prepare, which is the one case worth
// refusing over).
func (m *Manager) probe(ctx context.Context, volumeID, image string) (attached bool, foreign string, err error) {
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()

	client, err := qmp.Dial(ctx, m.dialer, QMPSocket(m.cfg.Root, volumeID))
	if err != nil {
		if errors.Is(err, qmp.ErrNoEndpoint) {
			return false, "", nil
		}
		return false, "", err
	}
	defer func() { _ = client.Close() }()

	devices, err := client.BlockDevices()
	if err != nil {
		return false, "", err
	}
	want := filepath.Clean(image)
	for _, d := range devices {
		if filepath.Clean(d.File) == want {
			return true, "", nil
		}
	}
	for _, d := range devices {
		// The first file that is not ours. Reported rather than counted, because what
		// an operator does about this starts with knowing which image it is.
		if d.File != "" {
			return false, d.File, nil
		}
	}
	// A QEMU with no block devices at all. It is not this volume's VM and it is not
	// running anything else's image either, so there is nothing to refuse over.
	return false, "", nil
}

// withTimeout bounds one exchange with another process, using the injected clock so the
// wait is simulable like every other wait in this tree (INV-01).
func (m *Manager) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	timer := m.clk.NewTimer(m.cfg.ProbeTimeout)
	done := make(chan struct{})
	go func() {
		select {
		case <-timer.C():
			cancel()
		case <-done:
			timer.Stop()
		}
	}()
	return ctx, func() {
		close(done)
		cancel()
	}
}

// Volumes reports what this host is holding, ordered by volume id (deterministic).
//
// # What the watermarks say, and what they do not
//
// LocalSequence, DurableSequence and PublishedSequence are reported as zero, and that
// is the honest answer rather than a gap. They were the write-ahead log's counters —
// what a guest had written, what an fdatasync had made durable, what an object covered
// — and that log is withdrawn. In this design a guest's write is durable locally the
// moment QEMU's own fdatasync returns, which is QEMU's business and has no sequence for
// us to count; and durability past this host is what a published commit means, which
// Stage 3 builds. The catalog's columns are still there and get their meaning back
// then, from the commit protocol.
//
// Zero and saying so beats a number that looks like durability. A watermark is what the
// Control Plane would compare across hosts to decide who has the newest state, and a
// fabricated one is a fencing decision made on a fiction.
func (m *Manager) Volumes(context.Context) ([]agent.VolumeStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]agent.VolumeStatus, 0, len(m.vols))
	for _, v := range m.vols {
		out = append(out, agent.VolumeStatus{
			VolumeID: v.id,
			Epoch:    v.epoch,
			Refusal:  v.refusal,
			// Empty when there is no refusal, which is what the wire's healthy value is.
			RefusalDetail: v.detail,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].VolumeID < out[j].VolumeID })
	return out, nil
}

// Fence stops serving the given volumes: this host is not their writer any more.
//
// The two callers want opposite things and the difference is whether the Control Plane
// already knows. A fence that follows a refused report is the Control Plane's own
// decision coming back — telling it would be telling it what it just told us, and the
// report would be refused again on the same predicate — so the volume is dropped
// outright and says nothing. A lease that lapsed is the other case: nothing outside
// this process knows, so the volume is kept with the refusal on it, and the next report
// is what carries the news off this host.
func (m *Manager) Fence(_ context.Context, volumeIDs []string, why storagev1.VolumeRefusal, detail string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, id := range volumeIDs {
		v, held := m.vols[id]
		if !held {
			continue
		}
		if why == storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
			delete(m.vols, id)
			slog.Warn("fenced: the control plane refused this volume's report, so this host stops serving it",
				"volume_id", id, "epoch", v.epoch)
			continue
		}
		v.chain, v.attached = nil, false
		v.refusal, v.detail = why, detail
		slog.Warn("fenced: this host stops serving the volume and will report why",
			"volume_id", id, "epoch", v.epoch, "refusal", why.String(), "detail", detail)
	}
	return nil
}
