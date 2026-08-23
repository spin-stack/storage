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
	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/qmp"
	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/disk"
)

// lockFile is this Manager's claim on the data directory. The name is part of the
// operator's world — it is what a human looks for to find out whether an Agent is
// holding a directory — so it is a constant here rather than a path built at a call
// site.
const lockFile = "agent.lock"

// minRotateAtBytes is the floor under Config.RotateAtBytes. A freshly created qcow2 is
// already ~193 KiB of header, L1 table and refcount blocks before a guest writes a byte;
// 1 MiB is the nearest round number above that with room for the metadata a few writes
// allocate.
const minRotateAtBytes = 1 << 20

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
	// RotateAtBytes is how large a tip may get before it is sealed and a new one is
	// started on top of it (v6 §11's size trigger). Zero disables rotation.
	//
	// Size and not age, for the one trigger this stage has. Age is the RPO promise and
	// belongs to the Control Plane, which sets it per volume; size is what bounds the
	// two things a host can lose control of on its own — how much a single layer costs
	// to upload, and how long a recovery that downloads this chain takes. It also gives
	// §11's "an idle volume does not commit" for free: a tip nobody writes to does not
	// grow, so it never crosses the threshold and no empty layer is ever produced.
	//
	// # It is a floor, not a bound, and the gap is the reconcile interval
	//
	// The tip is measured once per cycle, so a layer is sealed at roughly the threshold
	// *plus whatever the guest wrote since the last look*. Measured by `task demo:stage2`
	// with a 4 MiB threshold, a 300 ms cycle and a guest writing about a gigabyte a
	// second: the sealed layers came out at 32 MiB, eight times the number configured.
	//
	// That is not a defect to tune away here. With QEMU in the data path a guest's write
	// cannot be refused (v6 §11), so nothing can hold a layer to a size — the only knobs
	// are how often the tip is looked at and how fast the guest is, and the second is the
	// tenant's. What the number is good for is the shape it gives: sealing happens *at
	// least* this often by volume written, and an operator sizing uploads should plan for
	// the threshold plus one cycle of the fastest guest they will host.
	RotateAtBytes int64
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
	case c.RotateAtBytes != 0 && c.RotateAtBytes < minRotateAtBytes:
		// A threshold under an empty qcow2's own overhead would rotate a volume that
		// has never been written to, once per cycle, for ever — which is exactly the
		// "an idle volume does not commit" invariant inverted, and it would be found
		// by an operator watching a disk fill rather than by anything here.
		return fmt.Errorf("qcow: a rotation threshold of %d bytes is below the %d an empty image already occupies",
			c.RotateAtBytes, minRotateAtBytes)
	}
	return nil
}

// SealedLayer is a layer this host has finished writing and has not yet published. It
// is what rotation produces and what a commit consumes.
type SealedLayer struct {
	VolumeID string
	// LayerID names the file; it is in the nonce of every frame the layer is sealed
	// with, so it is part of the object's identity and not a label.
	LayerID string
	// CommitID is minted once, when the layer is sealed, and reused by every attempt to
	// publish it. That is what makes a retry idempotent instead of a second commit —
	// see commit.Request, which carries the reasoning.
	CommitID string
	// Path is the file on this host.
	Path string
	// Epoch is the fencing token this host held when it sealed the layer.
	Epoch int64
	// PlainBytes is the file's length; VirtualSize is the guest-visible size of the
	// volume the commit reconstructs.
	PlainBytes  int64
	VirtualSize int64
}

// Publisher publishes a sealed layer as a commit. It is injected rather than done here
// because publishing needs a Control Plane (for the volume's key) and an object store,
// and a local chain needs neither — which is the whole of ADR-0021's promise that spin's
// runner can take this type on its own.
//
// A nil Publisher is a host that keeps its layers locally and publishes nothing, which
// is every lane before v6 §23.3 and is not an error.
type Publisher interface {
	Publish(ctx context.Context, layer SealedLayer) error
}

// Deps are the Manager's injected collaborators (INV-01).
type Deps struct {
	Clock  clock.Clock
	Disk   disk.Disk
	Runner Runner
	Paths  Paths
	Dialer qmp.Dialer
	// Publisher is optional; without one, nothing this host seals ever leaves it.
	Publisher Publisher
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
	pub    Publisher
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
	// pending is the sealed layer this volume owes the object store, nil when it owes
	// none. At most one, ever: v6 §11 forbids rotating while a sealed layer is
	// unpublished, so a host that cannot reach S3 grows one tip and holds one sealed
	// layer rather than a chain of small ones that are each a commit that never landed.
	pending *SealedLayer
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
		paths: deps.Paths, dialer: deps.Dialer, pub: deps.Publisher, unlock: unlock,
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
		slog.Info("released a volume: this host is no longer serving it, and its local layers are kept",
			"volume_id", id, "epoch", v.epoch, "layers", LayersDir(m.cfg.Root, id))
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

	// QEMU is asked first, and the order is load-bearing. If a VM already has a layer of
	// this volume open — the Agent restarted while the guest kept running — then no
	// offline tool may touch the file (v6 §5), and `qemu-img info` would in fact fail on
	// QEMU's write lock. QEMU having opened it is a stronger statement about the image
	// than any check made from here, and since rotation moves the tip while a VM runs,
	// it is also the only thing that knows *which* layer is current.
	open, err := m.probe(ctx, id)
	if err != nil {
		return m.refuse(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_ATTACH_FAILED, err)
	}
	live := ""
	if open != "" {
		// Ours if it is a layer of this volume, whichever layer it is. The check is the
		// directory and not one path, because after a rotation the tip is a file this
		// Agent may never have named — a restarted Agent learns it here.
		if filepath.Dir(filepath.Clean(open)) != filepath.Clean(LayersDir(m.cfg.Root, id)) {
			return m.refuse(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_ATTACH_FAILED,
				fmt.Errorf("%w: it has %q open, which is not a layer of volume %s", ErrForeignImage, open, id))
		}
		live = open
	}
	attached := live != ""

	if v.chain == nil {
		// Bounded like the QMP exchange above, and for the same reason: `qemu-img` is
		// another process, and a cycle that does not finish is a lease that does not get
		// renewed.
		openCtx, cancel := m.withTimeout(ctx)
		defer cancel()
		chain, err := Open(openCtx, m.run, m.paths, m.cfg.QemuImg, OpenRequest{
			Root: m.cfg.Root, VolumeID: id, SizeBytes: d.GetSizeBytes(),
			LiveImage: live, NewLayerID: ids.New().String(),
		})
		if err != nil {
			return m.refuse(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_ATTACH_FAILED, err)
		}
		v.chain = chain
		v.refusal, v.detail = storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED, ""
		v.attached = attached
		slog.Info("volume ready: the chain is prepared and this is where the VM attaches to it",
			"volume_id", id, "epoch", v.epoch, "image", chain.Active,
			"pointer", ActivePointer(m.cfg.Root, id),
			"size_bytes", chain.SizeBytes, "qmp_socket", QMPSocket(m.cfg.Root, id),
			"attached", attached)
	} else {
		// Already serving. The chain is not re-checked — the file may be open, and the
		// answer would not change if it were not — but a live image still outranks what
		// this process remembers: a rotation this Agent did not perform, or one it
		// performed and crashed in the middle of, shows up here as QEMU holding a
		// different layer than the one in hand.
		if live != "" {
			if live != v.chain.Active {
				slog.Info("the tip moved under this Agent: adopting what QEMU has open",
					"volume_id", id, "was", v.chain.Active, "now", live)
				v.chain.Active = live
			}
			if err := SyncPointer(m.paths, m.cfg.Root, id, live); err != nil {
				return m.refuse(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_ATTACH_FAILED, err)
			}
		}
		m.setAttached(v, attached)
	}

	if !attached {
		return nil
	}
	// Publishing before rotating, and never a refusal for either. A tip that could not
	// be sealed, or a layer that could not be published, is a volume that keeps working
	// and keeps growing; taking it away from a guest that is using it would turn "we did
	// not manage to bound this layer" into an outage. The one exception is a HEAD that
	// moved, which is not this host's volume any more — see publish.
	// Three steps, in this order, each a no-op when there is nothing to do: publish what
	// this volume already owes, rotate if the tip has grown past the threshold, publish
	// what that rotation just sealed. Rotation does not publish from inside itself —
	// that nested a refusal (which clears the chain) underneath a caller still reading
	// it, and the caller found out by dereferencing nil.
	pubErr := m.publish(ctx, v)
	if v.chain == nil {
		// Refused — the only publishing failure that stops the volume is a HEAD that
		// moved, and after it there is no chain left to rotate.
		return pubErr
	}
	// A publish that merely failed does *not* return here, and that is deliberate. The
	// layer stays pending, and it is maybeRotate's own guard that declines to rotate
	// over it — v6 §11's second invariant, enforced where it can be read rather than as
	// a side effect of this function giving up early. Planting the guard's removal
	// turned nothing red while this returned, which is what a redundant guard looks
	// like from the outside.
	tip := v.chain.Active
	if err := m.maybeRotate(ctx, v); err != nil {
		slog.Error("could not rotate this volume's tip; it keeps serving and keeps growing",
			"volume_id", id, "image", tip, "error", err)
		return errors.Join(pubErr, fmt.Errorf("volume %s: %w", id, err))
	}
	return errors.Join(pubErr, m.publish(ctx, v))
}

// publish sends this volume's sealed layer to the object store, if it owes one.
//
// It runs before the rotation trigger is even looked at, which is v6 §11's second
// invariant: nothing rotates while a sealed layer is unpublished. Without it, an object
// store that is down turns into a chain of small layers, each one a commit that never
// landed, and the local depth grows for the whole outage. With it, the tip grows instead
// — one file, which the guest was going to fill anyway — and at most one sealed layer
// waits. It is also what §15 wants at restart: if there is a sealed layer, publish *it*,
// do not rotate again.
func (m *Manager) publish(ctx context.Context, v *volume) error {
	if v.pending == nil || m.pub == nil {
		return nil
	}
	// Not bounded by ProbeTimeout: that number is what one `qemu-img` run or one QMP
	// exchange may take, and a transfer of a whole layer is neither. The cycle's own
	// context is the bound, and a publish that outlives it is retried next cycle from
	// the same SealedLayer — which is idempotent, so a duplicated attempt costs a HEAD
	// request and nothing else.
	layer := *v.pending
	if err := m.pub.Publish(ctx, layer); err != nil {
		if errors.Is(err, commit.ErrHeadMoved) {
			// Another host published for this volume. This one is not its writer, and
			// the worst thing it could do now is carry on holding the guest's disk: it
			// would keep accepting writes that can never be published, and the fleet
			// would have two hosts believing they own one volume. That is the failure
			// the whole design is arranged against, so it is the one publishing failure
			// that stops the volume.
			return m.refuse(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_PUBLISH_FENCED, err)
		}
		slog.Error("could not publish this volume's sealed layer; it stays on this host and nothing rotates until it lands",
			"volume_id", v.id, "layer", layer.Path, "commit_id", layer.CommitID, "error", err)
		return fmt.Errorf("volume %s: %w", v.id, err)
	}
	v.pending = nil
	slog.Info("committed: the sealed layer is in the object store and HEAD names it",
		"volume_id", v.id, "epoch", layer.Epoch, "commit_id", layer.CommitID,
		"layer_id", layer.LayerID, "bytes", layer.PlainBytes)
	return nil
}

// maybeRotate applies v6 §11's size trigger.
func (m *Manager) maybeRotate(ctx context.Context, v *volume) error {
	if m.cfg.RotateAtBytes <= 0 || v.pending != nil {
		return nil
	}
	size, err := m.paths.Size(v.chain.Active)
	if err != nil {
		return fmt.Errorf("qcow: measuring the tip %s: %w", v.chain.Active, err)
	}
	if size < m.cfg.RotateAtBytes {
		return nil
	}
	return m.rotate(ctx, v, size)
}

// rotate seals the tip through the QEMU that is writing to it.
//
// The pause is measured around the QMP command and nothing else, because that is the
// only part the guest experiences: creating and checking the next layer happens while the
// VM writes normally, and it is only `blockdev-snapshot-sync` that drains the device and
// swaps it. Fixing v6 §11's defaults is what these numbers are for (v6 §23.2), so they
// are logged per rotation rather than averaged into a gauge.
func (m *Manager) rotate(ctx context.Context, v *volume, tipBytes int64) error {
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()

	client, err := qmp.Dial(ctx, m.dialer, QMPSocket(m.cfg.Root, v.id))
	if err != nil {
		return fmt.Errorf("qcow: reaching the VM writing volume %s: %w", v.id, err)
	}
	defer func() { _ = client.Close() }()

	device, err := deviceFor(client, v.chain.Active)
	if err != nil {
		return err
	}

	var pause time.Duration
	layerID := ids.New().String()
	sealed, err := v.chain.Rotate(ctx, m.run, m.paths, m.cfg.QemuImg, m.cfg.Root, v.id,
		layerID, func(next string) error {
			at := m.clk.Now()
			if err := client.Snapshot(device, next); err != nil {
				return err
			}
			pause = time.Duration(m.clk.Now() - at)
			return nil
		})
	if err != nil {
		return err
	}
	slog.Info("rotated: the tip is sealed and the guest is writing to a new layer",
		"volume_id", v.id, "epoch", v.epoch, "sealed", sealed, "sealed_bytes", tipBytes,
		"tip", v.chain.Active, "pause_ms", float64(pause.Microseconds())/1000)

	if m.pub == nil {
		return nil
	}
	// The commit id is minted here, once, and every attempt to publish this layer reuses
	// it. A fresh id per attempt would publish the same layer twice and read its own
	// success as somebody else's conflict — commit.Request carries the whole reasoning.
	//
	// It lives in memory only. An Agent that restarts between sealing and publishing
	// mints a new one and publishes the same layer under a second commit id, which is a
	// duplicate entry in a history rather than a loss; closing it needs the sealed layer
	// to be recorded on disk, which is v6 §5's state.json and arrives with recovery.
	// LayerIDOfImage(sealed), not the id just minted: Rotate returns the layer it
	// *sealed*, and layerID names the empty one the guest has moved on to. Publishing
	// the sealed bytes under the new tip's id would seal every frame with the wrong
	// nonce, and the layer would come back from the object store refusing to open —
	// which is the sort of thing that is found on the day it is needed.
	//
	// Its size is measured again rather than reused from the trigger: the drain that
	// the snapshot performs writes whatever QEMU still held, so the file is a little
	// larger than it was when the threshold was crossed.
	sealedBytes, err := m.paths.Size(sealed)
	if err != nil {
		return fmt.Errorf("qcow: measuring the sealed layer %s: %w", sealed, err)
	}
	v.pending = &SealedLayer{
		VolumeID: v.id, LayerID: LayerIDOfImage(sealed), CommitID: ids.New().String(),
		Path: sealed, Epoch: v.epoch, PlainBytes: sealedBytes, VirtualSize: v.chain.SizeBytes,
	}
	return nil
}

// deviceFor finds the drive id QEMU knows the tip by, which is what a snapshot names.
//
// The drive id and not the node name. Node names are what QMP documentation reaches for
// first, but a drive QEMU created for itself — `-drive file=...,if=virtio`, which is how
// a VM is launched here — has an anonymous one (`#block126`), and QMP refuses those as
// input. Measured against the pinned QEMU; the drive id was `virtio0`.
func deviceFor(c *qmp.Client, image string) (string, error) {
	devices, err := c.BlockDevices()
	if err != nil {
		return "", err
	}
	want := filepath.Clean(image)
	for _, d := range devices {
		if filepath.Clean(d.File) != want {
			continue
		}
		if d.Device == "" {
			return "", fmt.Errorf("qcow: the VM has %s open under no drive id, so it cannot be named in a snapshot; launch it with -drive ...,if=virtio or an explicit id=", image)
		}
		return d.Device, nil
	}
	return "", fmt.Errorf("qcow: the VM at this volume's socket no longer has %s open", image)
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
		slog.Info("a VM has attached to this volume: QEMU reports one of its layers open",
			"volume_id", v.id, "image", v.chain.Active)
		return
	}
	slog.Info("no VM is attached to this volume any more: nothing answers at its QMP socket",
		"volume_id", v.id, "qmp_socket", QMPSocket(m.cfg.Root, v.id))
}

// probe asks the QEMU at this volume's QMP socket what it has open, and returns the
// first file it names.
//
// It is deliberately not told what to expect. Rotation means the tip is a path this
// Agent may not know — a restart lands mid-chain, and the file QEMU holds is the answer
// rather than the thing to check against — so classifying what comes back is the
// caller's job and this only reports it.
//
// An empty string is "nothing is listening", the ordinary state of a prepared volume
// whose VM has not been launched, and it is not an error.
func (m *Manager) probe(ctx context.Context, volumeID string) (string, error) {
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()

	client, err := qmp.Dial(ctx, m.dialer, QMPSocket(m.cfg.Root, volumeID))
	if err != nil {
		if errors.Is(err, qmp.ErrNoEndpoint) {
			return "", nil
		}
		return "", err
	}
	defer func() { _ = client.Close() }()

	devices, err := client.BlockDevices()
	if err != nil {
		return "", err
	}
	for _, d := range devices {
		// The first file, reported rather than counted: what an operator does about a
		// VM running the wrong image starts with knowing which image it is.
		if d.File != "" {
			return d.File, nil
		}
	}
	// A QEMU with no block devices at all. It is not this volume's VM and it is not
	// running anything else's image either, so there is nothing to refuse over.
	return "", nil
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
