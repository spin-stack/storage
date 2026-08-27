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
	// second: the sealed layers came out at 32 MiB, eight times the number configured —
	// the same on QEMU 11.0.2 and 11.1.1, because what decides it is the cycle and the
	// guest rather than anything QEMU does.
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
	// Recovery is not optional. A host that cannot ask whether a volume has published
	// commits cannot tell "this volume is new" from "this volume's data is elsewhere",
	// and the only answer it can give in that state is a blank disk.
	Recovery Recovery
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
	rec    Recovery
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
	// rpo is this volume's age trigger, from the desired state. Zero is a volume with
	// no RPO promise, which commits on size alone.
	rpo time.Duration
	// openedAt is when this process opened the chain, in milliseconds on the injected
	// clock. It is the age trigger's anchor for a volume that has never committed, and
	// it is deliberately not durable: a volume with no commits has nothing to be late
	// against, and the first commit replaces it with the durable one.
	openedAt int64
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
	case deps.Recovery == nil:
		return nil, errors.New("qcow: a recovery must be injected: without it this host cannot tell a new volume from one whose data is in the object store")
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
		paths: deps.Paths, dialer: deps.Dialer, pub: deps.Publisher, rec: deps.Recovery, unlock: unlock,
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

// latched says whether a refusal may only be lifted by the fleet, at a higher epoch.
//
// Exactly two are, and they are the two that are statements about *ownership*:
// PUBLISH_FENCED (another host moved HEAD) and LEASE_LOST. Resuming on either without a
// higher epoch is two hosts writing one volume, which is the failure the whole design is
// arranged against.
//
// Everything else is a statement about *reachability* — I could not reach the bucket, the
// QMP socket did not answer just now, qemu-img met the write lock a running guest holds —
// and none of those is about who owns the volume. Listing the retryable ones instead was
// the same defect in the other direction: it named IMAGE_MISSING and stopped, so one
// missed QMP dial refused a volume for the life of the process while its guest went on
// writing to it, and the only thing that cleared it was the Control Plane raising an
// epoch for reasons that had nothing to do with the socket. A retry costs one probe, and
// one HEAD read for the volumes that need it, per refused volume per cycle.
func latched(why storagev1.VolumeRefusal) bool {
	return why == storagev1.VolumeRefusal_VOLUME_REFUSAL_PUBLISH_FENCED ||
		why == storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST
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
	if v.refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED && d.GetEpoch() <= v.epoch && latched(v.refusal) {
		return nil
	}

	// Never backwards. The epoch is this host's fencing token, and a desired state
	// carrying a number below the one it already holds is either a Control Plane that has
	// been rolled back or a message that overtook a newer one — and adopting it hands
	// this host a token the fleet has already moved past, which is the whole failure the
	// epoch exists to prevent. The higher number is kept and the volume goes on being
	// served under it.
	// The RPO is re-read every cycle rather than latched at attach: it is a promise the
	// Control Plane can change under a running volume, and a host that only read it once
	// would keep a tenant on the target they bought last month.
	v.rpo = time.Duration(d.GetRpoTargetSeconds()) * time.Second
	if e := d.GetEpoch(); e > v.epoch {
		v.epoch = e
	} else if e < v.epoch {
		slog.Warn("a desired state arrived carrying an epoch below the one this host holds; keeping the higher one",
			"volume_id", id, "held", v.epoch, "offered", e)
	}

	// The same guard, from disk, because the one above is only as durable as this
	// process. A host that was fenced and then restarted — OOM, a deploy, the SIGKILL
	// `task demo:stage1` performs under a running guest — reads the same desired state it
	// read before, at the same epoch, with an empty map: it re-attached to the volume,
	// found the layer it still owed in state.json, and published it, all of it while
	// another host was the volume's writer. Nothing in the fleet corrects that; the
	// refusal is a column no reconciler reads.
	//
	// Only while there is no chain, which is every cycle a refused volume has and no
	// cycle a served one has: a volume this Agent is serving was already let past this
	// point, and re-reading the file per heartbeat would buy nothing.
	// The fence is checked whether or not this process already holds a chain, and whether
	// or not a guest is attached. It was checked only for a volume with no chain, which
	// left the case a fence is *for*: a host that was fenced with its guest still writing,
	// coming back to find the guest still there. Open's live branch then returned that
	// image before any guard ran, and the host resumed its fork.
	{
		if err := m.checkFenced(v, d.GetEpoch()); err != nil {
			return err
		}
	}

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
			LiveImage: live, NewLayerID: ids.New().String(), Recovery: m.rec,
		})
		if err != nil {
			if errors.Is(err, ErrStaleChain) && live != "" {
				// The chain this guest is writing to is a fork of the published history,
				// and it cannot be replaced while the guest holds the file. So the guest
				// is stopped, and the next cycle — which finds no live image — is the one
				// that rebuilds from the object store.
				//
				// Stopping is not a fallback for a rebuild that failed: it is the same
				// rule as fencing. Every byte this guest writes from here lands in a
				// history nobody will ever publish, and it is being told they succeeded.
				m.stopGuest(id)
			}
			// Classified rather than blanket ATTACH_FAILED, because the operator's next
			// move differs: IMAGE_MISSING's own proto comment reads "the object store
			// holds none... Look at the bucket", which is not guessable from a catch-all.
			why := storagev1.VolumeRefusal_VOLUME_REFUSAL_ATTACH_FAILED
			if errors.Is(err, ErrChainMissing) {
				why = storagev1.VolumeRefusal_VOLUME_REFUSAL_IMAGE_MISSING
			}
			return m.refuse(v, why, err)
		}
		if chain.Forked {
			// The chain this host held was a fork of the published history, and it has
			// been replaced by the object store's copy. Whatever is still in memory about
			// the old one describes a history this volume does not have — and the sealed
			// layer among it would otherwise be published onto the rebuilt chain, as a
			// commit whose bytes the served chain does not contain. clearFork drops it on
			// disk; this is the same drop in memory, and without it only a restart made
			// the Agent stop.
			v.pending = nil
		}
		v.chain = chain
		if v.openedAt == 0 {
			v.openedAt = m.clk.Wall().UnixMilli()
		}
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

	// What this host holds and what it owes, against the record on disk. It runs on
	// every cycle and for an unattached volume too, because both of the facts it
	// maintains are about files and not about a guest.
	//
	// A failure here does not refuse the volume: it is a note about work already done,
	// and taking a guest's disk away over an fsync would turn bookkeeping into an outage.
	// It is returned, so the cycle reports it and the loop backs off.
	stateErr := m.reconcile(v)
	if !attached {
		return stateErr
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
		return errors.Join(stateErr, pubErr)
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
		return errors.Join(stateErr, pubErr, fmt.Errorf("volume %s: %w", id, err))
	}
	return errors.Join(stateErr, pubErr, m.publish(ctx, v))
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
			//
			// Written down before it is refused, because this is the statement that must
			// outlive the process: a refusal that lives only in a map is undone by the
			// next SIGKILL, and the Agent that starts after it re-attaches to the guest's
			// disk and publishes again. The write failing does not soften the refusal —
			// both errors go back, and the volume stops either way.
			return errors.Join(
				m.recordFenced(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_PUBLISH_FENCED, err.Error()),
				m.refuse(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_PUBLISH_FENCED, err))
		}
		slog.Error("could not publish this volume's sealed layer; it stays on this host and nothing rotates until it lands",
			"volume_id", v.id, "layer", layer.Path, "commit_id", layer.CommitID, "error", err)
		return fmt.Errorf("volume %s: %w", v.id, err)
	}
	v.pending = nil
	slog.Info("committed: the sealed layer is in the object store and HEAD names it",
		"volume_id", v.id, "epoch", layer.Epoch, "commit_id", layer.CommitID,
		"layer_id", layer.LayerID, "bytes", layer.PlainBytes)
	if err := m.recordCommit(v, layer); err != nil {
		// The commit is in the bucket and HEAD names it; only this host's note of that
		// failed. The consequence is bounded and self-correcting: a restart re-publishes
		// the same commit id, which commit.Publish recognises as its own retry. It is
		// reported rather than swallowed because the note is also what lets a later
		// recovery skip re-downloading this layer.
		return fmt.Errorf("volume %s: %w", v.id, err)
	}
	return nil
}

// checkFenced refuses a volume this host has recorded that it is not the writer of.
//
// The record is cleared by one thing and one thing only: a *higher* epoch, which is the
// fleet granting the volume to this host again. Not a restart, not the object store
// answering again, not the guest coming back — every one of those is this host deciding
// on its own that it owns a volume it was told it does not, which is the two-writer
// failure with a plausible story in front of it.
func (m *Manager) checkFenced(v *volume, epoch int64) error {
	st, err := ReadState(m.paths, m.cfg.Root, v.id)
	if err != nil {
		// Not guessed at, for the reason ReadState carries: a state file that cannot be
		// believed is a host that does not know whether it is this volume's writer.
		return m.refuse(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_ATTACH_FAILED, err)
	}
	if st.Fenced == nil {
		return nil
	}
	if epoch > st.Fenced.Epoch {
		// A higher epoch is the fleet saying this host owns the volume again, so the
		// refusal lifts. Whether the *record* is cleared here depends on whether there is
		// a chain to re-grant, and the two cases are not the same:
		//
		//   - a host with a local chain leaves it: regrant needs the record to know this
		//     is a re-grant rather than an ordinary open, and it clears the record, the
		//     layer list and any pending commit together, once it has asked the object
		//     store what the published history is. Clearing it here took that signal away
		//     and left the host serving its stale fork.
		//   - a host with no chain has nothing for regrant to run on, and nothing else
		//     would ever clear the record — so it was refused at every later epoch for
		//     ever, with the fleet granting it the volume again and again.
		if has, err := m.paths.Exists(ActivePointer(m.cfg.Root, v.id)); err != nil {
			return m.refuse(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_ATTACH_FAILED, err)
		} else if !has {
			at := st.Fenced.Epoch
			st.Fenced = nil
			if err := WriteState(m.paths, m.cfg.Root, v.id, st); err != nil {
				return m.refuse(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_ATTACH_FAILED, err)
			}
			slog.Info("the fleet granted this volume back at a higher epoch and this host holds no chain; the fencing record is cleared",
				"volume_id", v.id, "fenced_at", at, "now", epoch)
		}
		return nil
	}
	why := storagev1.VolumeRefusal(st.Fenced.Refusal)
	return m.refuse(v, why, fmt.Errorf("this host stopped being the writer at epoch %d (%s: %s) and the fleet has not granted the volume back — it is still at epoch %d",
		st.Fenced.Epoch, why, st.Fenced.Detail, epoch))
}

// stopGuest pauses whatever VM is at this volume's QMP socket.
//
// Every failure is logged and none is returned. There is nothing a caller could do with
// one — the volume is being fenced either way — and the two ordinary failures are not
// failures at all: no socket means no guest, and a QEMU that has already gone means the
// writing has already stopped. What must not happen is a fence that does not happen
// because a socket was slow.
func (m *Manager) stopGuest(volumeID string) {
	ctx, cancel := m.withTimeout(context.Background())
	defer cancel()

	client, err := qmp.Dial(ctx, m.dialer, QMPSocket(m.cfg.Root, volumeID))
	if err != nil {
		if !errors.Is(err, qmp.ErrNoEndpoint) {
			slog.Warn("could not reach the VM to stop it while fencing this volume; it may still be writing",
				"volume_id", volumeID, "error", err)
		}
		return
	}
	defer func() { _ = client.Close() }()
	if err := client.Stop(); err != nil {
		slog.Error("the VM holding this volume would not stop while it was being fenced; it is still writing to a volume this host does not own",
			"volume_id", volumeID, "error", err)
		return
	}
	slog.Warn("stopped the guest: this host is not this volume's writer any more, and a paused VM is the strongest thing an Agent that does not own its lifetime can do",
		"volume_id", volumeID)
}

// recordFenced writes down that this host is not this volume's writer, before the
// in-memory refusal that a process death would take with it.
func (m *Manager) recordFenced(v *volume, why storagev1.VolumeRefusal, detail string) error {
	st, err := ReadState(m.paths, m.cfg.Root, v.id)
	if err != nil {
		return fmt.Errorf("volume %s: %w", v.id, err)
	}
	st.Fenced = &Fencing{Epoch: v.epoch, Refusal: int32(why), Detail: detail}
	if err := WriteState(m.paths, m.cfg.Root, v.id, st); err != nil {
		return fmt.Errorf("volume %s: %w", v.id, err)
	}
	return nil
}

// reconcile brings this host's record of a volume in line with what it can observe: which
// layer is the tip, and which sealed layers are still owed to the object store.
//
// # The tip is written down, and the sealed layer is derived from it
//
// A layer is sealed exactly when it stops being the tip. That is a fact about files, and
// the tip is *observed* — from the QEMU that has it open, or failing that from
// `active/current` — so "sealed and not published" needs no record of its own to survive a
// crash: it is (the layers this host has seen as tips) − (the tip) − (the published ones).
//
// This is what closes the window rotate opens. Rotate seals through QMP and records what
// it owes afterwards, and a crash in between used to leave a complete layer, full of the
// guest's writes, that nothing would ever publish — the next commit chained past it and
// spliced a hole into the published history that no host could see. The alternative,
// recording before the switch, is worse and was rejected where it is described: at that
// moment the layer is still live, so the record would name a file QEMU is writing into.
//
// The Pending record stays, demoted to what it always was underneath: the commit id a
// sealed layer was already promised under, so that a retry is the same commit and not a
// second one. Losing it now costs a duplicate id, which commit.Publish would catch, and
// never a layer.
func (m *Manager) reconcile(v *volume) error {
	st, err := ReadState(m.paths, m.cfg.Root, v.id)
	if err != nil {
		return fmt.Errorf("volume %s: %w", v.id, err)
	}
	tip := LayerIDOfImage(v.chain.Active)
	dirty := st.recordTip(tip)
	// Only with a publisher, and only when nothing is already in hand. A host with no
	// object store configured has nothing to owe: giving it a pending layer would stop it
	// rotating for ever (v6 §11), which is the one thing rotate's own no-publisher branch
	// is written to avoid.
	if v.pending == nil && m.pub != nil {
		if err := m.adopt(v, &st, tip, &dirty); err != nil {
			return err
		}
	}
	if !dirty {
		return nil
	}
	if err := WriteState(m.paths, m.cfg.Root, v.id, st); err != nil {
		return fmt.Errorf("volume %s: %w", v.id, err)
	}
	return nil
}

// adopt takes on the oldest layer this host owes the object store, from the record when
// there is one and from the chain when there is not.
func (m *Manager) adopt(v *volume, st *State, tip string, dirty *bool) error {
	// The chain decides *which* layer is owed next; the record only decides what to call
	// it. It was the other way round — a recorded Pending was published immediately —
	// which put the newest sealed layer into the history while an older one was still
	// owed underneath it, and a commit whose parent is not in the history yet is a hole
	// spliced into the chain. Oldest first, always, and the record is consulted only once
	// the oldest is known.
	sealed := st.sealedBelow(tip)
	if len(sealed) == 0 {
		return nil
	}
	layerID := sealed[0]
	if st.Pending != nil && st.Pending.LayerID == layerID {
		v.pending = &SealedLayer{
			VolumeID: v.id, LayerID: st.Pending.LayerID, CommitID: st.Pending.CommitID,
			Path:  LayerImage(m.cfg.Root, v.id, st.Pending.LayerID),
			Epoch: st.Pending.Epoch, PlainBytes: st.Pending.PlainBytes, VirtualSize: st.Pending.VirtualSize,
		}
		slog.Info("this host owes the object store a sealed layer it recorded earlier; it will be published under the commit id it was sealed with",
			"volume_id", v.id, "commit_id", st.Pending.CommitID, "layer_id", st.Pending.LayerID)
		return nil
	}
	path := LayerImage(m.cfg.Root, v.id, layerID)
	bytes, err := m.paths.Size(path)
	if err != nil {
		return fmt.Errorf("qcow: measuring the sealed layer %s of volume %s: %w", path, v.id, err)
	}
	// A fresh commit id, because the one this layer was promised under died with the
	// process that minted it. That is the cost of the lost record and it is the small
	// half: a duplicate id in a history is something a human can read, and a layer nobody
	// publishes is a hole nobody can see.
	//
	// The epoch is this host's current one and not the one the layer was sealed under,
	// which is also lost. It is the honest value — an epoch is a claim this host makes
	// now, and it holds this volume at this epoch or it would not be here.
	v.pending = &SealedLayer{
		VolumeID: v.id, LayerID: layerID, CommitID: ids.New().String(), Path: path,
		Epoch: v.epoch, PlainBytes: bytes, VirtualSize: v.chain.SizeBytes,
	}
	st.Pending = &PendingCommit{
		CommitID: v.pending.CommitID, LayerID: layerID, Epoch: v.epoch,
		PlainBytes: bytes, VirtualSize: v.chain.SizeBytes,
	}
	*dirty = true
	slog.Warn("found a sealed layer nothing had recorded: it is under this volume's tip, it is in no commit, and it would have been chained past",
		"volume_id", v.id, "layer_id", layerID, "layer", path, "tip", v.chain.Active,
		"commit_id", v.pending.CommitID, "also_unpublished", len(sealed)-1)
	return nil
}

// recordCommit writes down that this host holds the layer of a commit that has landed.
//
// Two facts, one file, and both are things only this host knows: which commit a local
// layer came from — after `qemu-img rebase -u` a layer no longer hashes to the object it
// came from, so nothing else can vouch for the file — and which commit id a sealed layer
// was promised under.
//
// state.json has two writers, this one and rotate, and both run under Manager.mu: Apply
// holds it across ensure, which is what calls both. The file is read back the same way.
func (m *Manager) recordCommit(v *volume, layer SealedLayer) error {
	st, err := ReadState(m.paths, m.cfg.Root, v.id)
	if err != nil {
		return err
	}
	st.Pending = nil
	st.Commits = append(st.Commits, CommitLayer{CommitID: layer.CommitID, LayerID: layer.LayerID})
	st.LastCommitAt = m.clk.Wall().UnixMilli()
	// Everything under a published layer is published with it, so nothing older is ever
	// a question again. Without this the list would grow by one entry per rotation for
	// the life of the volume, for a fact that is only ever asked about the top of it.
	st.trimLayers(layer.LayerID)
	return WriteState(m.paths, m.cfg.Root, v.id, st)
}

// maybeRotate applies v6 §11's two triggers: size, which the host sets, and age, which
// the volume carries from the Control Plane as its RPO.
//
// Either fires a rotation, and they answer different questions. Size bounds what a host
// can lose control of on its own — how much one layer costs to upload, how long a
// recovery that downloads this chain takes — and applies to every volume this Agent
// serves. Age is a promise to one tenant about how far behind the bucket their volume may
// fall, and a volume with no promise has no age trigger.
//
// § 11's first invariant survives both: an idle volume does not commit. The size arm gets
// it for free — a tip nobody writes to does not grow. The age arm needs the extra
// condition below, because time passes for an idle volume too, and without it a volume
// that wrote nothing would seal an empty layer every RPO for ever and the number would
// stop meaning what it says: a volume that wrote nothing is *inside* its target, not
// behind it.
func (m *Manager) maybeRotate(ctx context.Context, v *volume) error {
	if v.pending != nil {
		return nil
	}
	if m.cfg.RotateAtBytes <= 0 && v.rpo <= 0 {
		return nil
	}
	size, err := m.paths.Size(v.chain.Active)
	if err != nil {
		return fmt.Errorf("qcow: measuring the tip %s: %w", v.chain.Active, err)
	}
	if m.cfg.RotateAtBytes > 0 && size >= m.cfg.RotateAtBytes {
		return m.rotate(ctx, v, size)
	}
	if v.rpo <= 0 {
		return nil
	}
	// minRotateAtBytes is what "has been written to" means here, and it is the same
	// constant the size trigger is floored by for the same reason: a freshly created
	// qcow2 is already ~193 KiB of header, L1 table and refcount blocks before a guest
	// writes a byte, so "larger than zero" is true of every tip that ever existed.
	if size < minRotateAtBytes {
		return nil
	}
	age, err := m.tipAge(v)
	if err != nil || age < v.rpo {
		return err
	}
	slog.Info("committing on age: this volume's tip has been unpublished for longer than its RPO",
		"volume_id", v.id, "age_s", age.Seconds(), "rpo_s", v.rpo.Seconds(), "tip_bytes", size)
	return m.rotate(ctx, v, size)
}

// tipAge is how long it has been since this host published a commit for the volume, or
// since the chain was opened when it never has.
//
// Anchoring an uncommitted volume at the chain's opening rather than at zero is what
// stops a volume that has never committed from being infinitely late: measured from the
// epoch it would rotate on its first cycle, which is an empty-ish layer published for a
// volume whose guest may not have booted yet.
func (m *Manager) tipAge(v *volume) (time.Duration, error) {
	st, err := ReadState(m.paths, m.cfg.Root, v.id)
	if err != nil {
		return 0, err
	}
	since := st.LastCommitAt
	if since == 0 {
		since = v.openedAt
	}
	return time.Duration(m.clk.Wall().UnixMilli()-since) * time.Millisecond, nil
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

	target, overlayNode, err := targetFor(client, v.chain.Active)
	if err != nil {
		return err
	}

	var pause time.Duration
	layerID := ids.New().String()
	sealed, err := v.chain.Rotate(ctx, m.run, m.paths, m.cfg.QemuImg, m.cfg.Root, v.id,
		layerID, func(next string) error {
			at := m.clk.Now()
			if err := client.Snapshot(target, overlayNode, next); err != nil {
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
		"tip", v.chain.Active, "pause_ms", float64(pause.Microseconds())/1000,
		// How the disk was named, because there are two ways and which one applied is
		// the first thing worth knowing when a rotation fails on the naming: a drive id
		// means the VM was launched with -drive, a node means -blockdev.
		"named_by", target.String())

	// A host with no publisher still records what it sealed, and does not treat it as
	// something to wait for. Those are two different facts and they were one: rotate
	// returned here, so a layer sealed while no object store was configured left no
	// trace at all, and an operator who added the store afterwards published everything
	// from that point on and nothing from before it — the layers were on disk, complete,
	// and invisible.
	//
	// It is not `pending` because pending is what stops the next rotation (v6 §11), and
	// with no publisher there is nothing to wait for: a host configured this way would
	// rotate once and then never again.
	sealedBytes, err := m.paths.Size(sealed)
	if err != nil {
		return fmt.Errorf("qcow: measuring the sealed layer %s: %w", sealed, err)
	}
	if m.pub == nil {
		return m.recordSealed(v, sealed, sealedBytes)
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
	v.pending = &SealedLayer{
		VolumeID: v.id, LayerID: LayerIDOfImage(sealed), CommitID: ids.New().String(),
		Path: sealed, Epoch: v.epoch, PlainBytes: sealedBytes, VirtualSize: v.chain.SizeBytes,
	}
	// Written after the layer is sealed and in memory, never before. There is a window
	// here — between blockdev-snapshot-sync returning and this landing — in which a
	// crash still mints a second commit id for the same layer. Writing the record
	// *before* the QMP switch would close it and open a worse one: the layer is still
	// live at that moment, so a restart in that window would publish a file QEMU is
	// writing into, which is a corrupt commit rather than a duplicate entry. The window
	// stays on purpose; it is microseconds, and its cost is the duplicate entry the gap
	// already had.
	//
	// In memory first, then on disk, for the same asymmetry: a write that fails leaves
	// the layer pending here, so it is still published and nothing rotates over it. The
	// error is returned rather than swallowed — until it succeeds this volume is back to
	// the restart gap — and the volume keeps serving, because ensure treats a rotation
	// failure as a volume that goes on growing rather than an outage.
	return m.recordPending(v)
}

// recordSealed writes down a layer this host sealed while it had no publisher.
//
// It goes in the same Pending slot, because it is the same fact — a sealed layer that has
// not been published — and a second field for "sealed but nobody was listening" would be
// two spellings of one state that a later reader has to keep in step. What differs is
// only what the Manager does about it in memory: nothing, until a publisher exists.
func (m *Manager) recordSealed(v *volume, sealed string, sealedBytes int64) error {
	st, err := ReadState(m.paths, m.cfg.Root, v.id)
	if err != nil {
		return err
	}
	st.Pending = &PendingCommit{
		CommitID: ids.New().String(), LayerID: LayerIDOfImage(sealed), Epoch: v.epoch,
		PlainBytes: sealedBytes, VirtualSize: v.chain.SizeBytes,
	}
	return WriteState(m.paths, m.cfg.Root, v.id, st)
}

// recordPending writes down the layer this host owes the object store.
func (m *Manager) recordPending(v *volume) error {
	st, err := ReadState(m.paths, m.cfg.Root, v.id)
	if err != nil {
		return err
	}
	st.Pending = &PendingCommit{
		CommitID: v.pending.CommitID, LayerID: v.pending.LayerID, Epoch: v.pending.Epoch,
		PlainBytes: v.pending.PlainBytes, VirtualSize: v.pending.VirtualSize,
	}
	return WriteState(m.paths, m.cfg.Root, v.id, st)
}

// targetFor finds how QMP can name the node holding the tip, and picks a free name for
// the overlay a rotation will put over it.
//
// There is no single way to name a disk, because this Agent does not launch the VM
// (ADR-0021) and so does not choose its shape. A `-drive file=...,if=virtio` disk has a
// generated drive id (`virtio0`) and an *anonymous* node QMP refuses as input; a
// `-blockdev node-name=vol` disk has a real node and no drive id at all. This used to
// take the first and refuse the second, which is a VM shaped the way any libvirt-derived
// runner shapes one — so rotation broke on exactly the launcher we expect to meet.
//
// The overlay name is only needed on the node path: addressed by node-name,
// blockdev-snapshot-sync answers "New overlay node-name missing" without one. It cannot
// be the layer's id — QEMU caps a node name at 31 characters and a v7 UUID is 36, which
// is measured and is why the names are `spinN` rather than anything meaningful. What
// matters about them is only that they are free, so they are checked against the whole
// graph and not just the guest's devices: a name a *backing* node holds is as unusable
// as one a tip holds.
func targetFor(c *qmp.Client, image string) (qmp.Target, string, error) {
	devices, err := c.BlockDevices()
	if err != nil {
		return qmp.Target{}, "", err
	}
	want := filepath.Clean(image)
	for _, d := range devices {
		if filepath.Clean(d.File) != want {
			continue
		}
		t := qmp.Target{Device: d.Device, NodeName: d.NodeName}
		if !t.Named() {
			return qmp.Target{}, "", fmt.Errorf("qcow: the VM has %s open under neither a drive id nor a node name, so nothing can name it in a snapshot; launch it with `-drive ...,if=virtio` or `-blockdev node-name=<name>`", image)
		}
		if t.Device != "" {
			return t, "", nil
		}
		overlay, err := freeNodeName(c)
		if err != nil {
			return qmp.Target{}, "", err
		}
		return t, overlay, nil
	}
	return qmp.Target{}, "", fmt.Errorf("qcow: the VM at this volume's socket no longer has %s open", image)
}

// freeNodeName returns a node name nothing in the block graph is using.
func freeNodeName(c *qmp.Client) (string, error) {
	taken, err := c.NamedNodes()
	if err != nil {
		return "", err
	}
	used := make(map[string]bool, len(taken))
	for _, n := range taken {
		used[n] = true
	}
	// Bounded rather than `for {}`: the loop's exit depends on what another process
	// reports, and a QEMU answering with a graph this Agent cannot find a gap in is a
	// bug to report, not a cycle to spin in.
	for i := 1; i <= 1024; i++ {
		name := fmt.Sprintf("spin%d", i)
		if !used[name] {
			return name, nil
		}
	}
	return "", errors.New("qcow: this VM's block graph has a thousand nodes named spinN and no free one; something is not cleaning up after itself")
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
// Fence stops serving what this host is no longer the writer of, and stops its guest.
//
// All three ways in are treated the same here — a lease that lapsed on this host's own
// clock, a Control Plane that refused the report, a compare-and-set that lost — and
// whether they *should* be is an open question recorded in docs/plan/STATUS.md. The short
// version: only the last two prove somebody else has the volume, and stopping a guest
// because a network was slow costs a tenant their VM for a partition nobody else acted on.
//
// Read-only would be better than stopping and is not available. Measured against the
// pinned QEMU: with the drive QEMU creates for itself the node is anonymous and QMP
// refuses it as input, and with named nodes blockdev-reopen still refuses — "Read-only
// block node cannot support read-write users" — because the guest's virtio driver holds
// it read-write and nothing on the host can take that away. qmp.Stop carries both.
//
// Pausing does not kill the VM, whose lifetime belongs to whoever launched it (ADR-0021).
func (m *Manager) Fence(_ context.Context, volumeIDs []string, why storagev1.VolumeRefusal, detail string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var failures []error
	for _, id := range volumeIDs {
		v, held := m.vols[id]
		if !held {
			continue
		}
		if why == storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
			m.stopGuest(id)
			delete(m.vols, id)
			slog.Warn("fenced: the control plane refused this volume's report, so this host stops serving it",
				"volume_id", id, "epoch", v.epoch)
			continue
		}
		// The guest is stopped, and this is the half that was missing: fencing took the
		// chain out of a map and left QEMU writing. A host that has been *shown* it is not
		// this volume's writer, with a guest still writing to it, is accumulating bytes
		// that no commit can ever carry.
		//
		// Read-only would be better and is not available: measured against the pinned
		// QEMU, a live guest's virtio-blk holds the node read-write and nothing on the
		// host can take that away. qmp.Stop carries the two error messages. Pausing does
		// not kill the VM — its lifetime belongs to whoever launched it (ADR-0021).
		m.stopGuest(v.id)
		v.chain, v.attached = nil, false
		v.refusal, v.detail = why, detail
		// On disk as well as in memory. A lease that lapsed is the same statement as a
		// HEAD that moved — this host is not the volume's writer — and it has to survive
		// a restart for the same reason, and to be the thing that tells the chain this
		// host keeps that it is a fork the next time the fleet hands the volume back.
		if err := m.recordFenced(v, why, detail); err != nil {
			failures = append(failures, err)
		}
		slog.Warn("fenced: this host stops serving the volume and will report why",
			"volume_id", id, "epoch", v.epoch, "refusal", why.String(), "detail", detail)
	}
	return errors.Join(failures...)
}
