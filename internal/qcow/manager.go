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

// DefaultRotateAtBytes is v6 §11's size trigger, fixed by measuring rather than choosing
// — which is §23's exit criterion for Stage 2 and the reason this was 0 for so long.
//
// `task measure:publish` measures what §9's steps 9 to 13 cost through the real writer,
// and the fit gives two numbers: F, what a commit costs before a byte of payload (the
// manifest PUT, the HEAD compare-and-set, and the read of the commit being built on), and
// R, the rate the payload moves at over both passes. A commit of S bytes costs F + S/R, so
// the threshold at which the fixed cost is a tenth of the commit is S = 9FR.
//
// Against the pinned RustFS on loopback: F = 12 ms, R = 392 MiB/s, S = 43 MiB.
//
// **And that number barely moves with the backend**, which is what makes it a default at
// all rather than a measurement of this laptop. F and R move in opposite directions — a
// slower store costs more per round trip *and* less per byte — so their product is the
// bandwidth-delay product, which is far more stable than either term. Working it through:
// a real S3 at F = 50 ms and R = 100 MiB/s gives 45 MiB; a fast local store at F = 5 ms and
// R = 1 GiB/s gives 45 MiB; a slow remote one at F = 200 ms and R = 20 MiB/s gives 36 MiB.
// Two orders of magnitude of backend, and a band of 36 to 45.
//
// 32 MiB is the round number under that band, and under is the safe side: too small costs
// overhead, too large costs RPO, and the second is the promise. A deployment that wants
// its own number runs `task measure:publish` against its own store.
//
// It is a *floor* on the layer, never a bound (§11): the tip is measured once a cycle, so
// a layer weighs the threshold plus whatever the guest wrote since the last look — measured
// at 8x under a hard writer. Nothing can bound it while QEMU takes the writes.
const DefaultRotateAtBytes = 32 << 20

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
	// Size and not age, for the one trigger this stage has: age is the RPO promise and
	// belongs to the Control Plane, which sets it per volume. Size also gives §11's "an
	// idle volume does not commit" for free — a tip nobody writes to never crosses it.
	//
	// It is a floor, not a bound, and the gap is the reconcile interval. Measured by
	// `task demo:stage2` with a 4 MiB threshold, a 300 ms cycle and a guest writing about
	// a gigabyte a second, the sealed layers came out at 32 MiB — eight times the number
	// configured, the same on QEMU 11.0.2 and 11.1.1. With QEMU in the data path a guest's
	// write cannot be refused (v6 §11), so nothing can hold a layer to a size; an operator
	// sizing uploads should plan for the threshold plus one cycle of the fastest guest.
	RotateAtBytes int64
	// Compaction is when a chain has grown deep enough to be worth collapsing (v6 §19).
	// The zero value is no policy at all, which is what every caller has until somebody
	// measures one — see CompactionPolicy.
	Compaction CompactionPolicy
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
	return c.Compaction.Validate()
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
	// ReplacesCommitID, when set, publishes this layer as a root — a whole image rather
	// than a delta, so the commit carries no parent and a recovery stops at it instead of
	// walking the history it flattens. Only a chain collapse sets it (v6 §19), and it
	// names the commit the flattened bytes reconstruct.
	ReplacesCommitID string
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
	// stalled says the last attempt to publish `pending` failed. It is not a refusal —
	// the volume goes on being served, which is what §15 promises when the object store
	// is unreachable — and it is not derived from `pending` either: owing a layer is the
	// ordinary state between a rotation and a publish, while having *tried and failed* is
	// the one the fleet has to act on. Cleared by the publish that succeeds.
	stalled bool
	// rpo is this volume's age trigger, from the desired state. Zero is a volume with
	// no RPO promise, which commits on size alone.
	rpo time.Duration
	// wantSnapshot is the snapshot id the Control Plane is asking this host to take, from
	// the desired state; empty when it is asking for nothing. It is a *request* and the
	// three fields below are the answer to it — kept apart because the request keeps
	// arriving until the Control Plane sees the answer, so the two must not be one field
	// that clearing would re-arm.
	wantSnapshot string
	// snapshotID is the request this host has finished acting on, with snapshotCommit or
	// snapshotErr saying how. Reported until the request stops arriving.
	snapshotID     string
	snapshotCommit string
	snapshotErr    string
	// awaitingRebase says this volume's collapsed root is published and the chain has not
	// been repointed at it yet, so that a wait which lasts until the guest detaches is
	// reported once rather than once a heartbeat.
	awaitingRebase bool
	// openedAt is when this process opened the chain, in milliseconds on the injected
	// clock. It is the age trigger's anchor for a volume that has never committed, and
	// it is deliberately not durable: a volume with no commits has nothing to be late
	// against, and the first commit replaces it with the durable one.
	openedAt int64
}

// New validates the wiring, claims the data directory, and returns a Manager.
//
// The lock is taken here and not in cmd/volume-agent: two live Agents on one data
// directory would hand the same qcow2 file to two QEMUs (v6 §10, one Agent per host),
// and a claim that lives in a `main` is a step ADR-0021's other caller — spin's runner —
// would have to know to repeat.
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

	// A start-up refusal rather than a per-volume one: the failure it catches is an
	// operator's — a path that is not there, not executable, or not qemu-img — and an Agent
	// that discovers it when the first volume arrives has already registered as healthy.
	//
	// The version is printed rather than compared: asserting the pin here would put a
	// version string in a binary that has to be edited in lock-step with the Taskfile.
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

// Close releases the data directory. The kernel drops an flock when the process dies, so
// this is for the paths that return rather than for a crash.
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
		// Only an ACTIVE volume is served; anything else is left out of `live` so the loop
		// below stops it by the one path that stops anything. FENCING_WAIT is a volume being
		// moved to somebody else, and preparing a chain for one is getting ready to write a
		// volume this host is losing. It also stops *reporting* them: an unset refusal is
		// this host saying "I am serving this", and the fleet would read the outgoing writer
		// as healthy for the whole wait.
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
		// The guest goes first. A volume leaving the desired state is this host being told
		// it is not the writer any more, and forgetting it in memory while QEMU stays
		// attached means every byte written from here lands in a chain nothing will publish,
		// with the guest told each one succeeded. A volume whose VM is already gone has no
		// QEMU at its socket and stopGuest returns silently.
		m.stopGuest(id)
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
// higher epoch is two hosts writing one volume.
//
// Everything else is a statement about *reachability* — the bucket, the QMP socket, a
// write lock a running guest holds — and is retried, at one probe per refused volume per
// cycle. Listing the retryable ones instead was the same defect inverted: one missed QMP
// dial refused a volume for the life of the process.
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
	v.wantSnapshot = d.GetPendingSnapshotId()
	if e := d.GetEpoch(); e > v.epoch {
		v.epoch = e
	} else if e < v.epoch {
		slog.Warn("a desired state arrived carrying an epoch below the one this host holds; keeping the higher one",
			"volume_id", id, "held", v.epoch, "offered", e)
	}

	// The same guard, from disk, because the one above is only as durable as this
	// process: a host that was fenced and then restarted reads the same desired state at
	// the same epoch with an empty map, and would re-attach and publish the layer it still
	// owed while another host was the writer. Checked whether or not this process holds a
	// chain and whether or not a guest is attached — Open's live branch returns the image
	// before any guard runs, so the case a fence is *for* was the one it missed.
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
	open, corrupt, err := m.probe(ctx, id)
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
		// And it still has a name. QEMU reports the path it opened, not whether that path
		// still resolves: unlink a running guest's layer directory and the guest keeps
		// reading and writing the open inode while nothing on disk carries its bytes.
		// Everything downstream reads `live` as "the tip exists and QEMU holds it", and
		// Open's live branch would hand back a chain of files that are not there.
		//
		// A stat is not "touching" in §5's sense — it takes no lock and opens nothing — and
		// without it a second, empty chain is prepared under the same id while the first is
		// still being written to. The guest is deliberately *not* stopped: nobody else owns
		// this volume, so this is not supersession.
		if _, err := m.paths.Size(open); err != nil {
			return m.refuse(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_IMAGE_MISSING,
				fmt.Errorf("the VM has %s open and that path no longer exists, so this volume's layers were removed under a running guest; nothing here can name its bytes and no new chain will be prepared under it: %w", open, err))
		}
		// And QEMU does not say it is corrupt. This is the one moment a guest corrupting
		// its own image can be *detected* — an offline tool is refused on the write lock
		// (v6 §5 forbids it anyway), so the running QEMU is the only reader — and until
		// now the consequence was closed at both ends and the fact itself was learned at
		// the next open, which can be days later and on another host.
		//
		// The guest is not stopped: nobody else owns this volume, so this is not
		// supersession, and stopping it would take away the operator's only copy of a disk
		// whose newest bytes are on this host. It is refused instead, which is what stops
		// the chain being rotated and published under a commit no host could restore.
		if corrupt {
			return m.refuse(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_DURABILITY_LOST,
				fmt.Errorf("%w: the QEMU writing %s reports the qcow2 corrupt flag set on it, so nothing sealed from this chain can be published and the newest restorable point is the last commit",
					ErrChainMismatch, open))
		}
		live = open
	}
	attached := live != ""

	if v.chain == nil {
		openCtx, cancel := m.withTimeout(ctx)
		defer cancel()
		chain, err := Open(openCtx, m.run, m.paths, m.cfg.QemuImg, OpenRequest{
			Root: m.cfg.Root,
			// The lineage comes from the desired state every cycle, because the Agent
			// cannot look an ancestor up (ADR-0021) and the Control Plane is the only
			// party that can read the rows the chain is spelled out in.
			Lineage:   Lineage{VolumeID: id, Ancestry: ancestry(d)},
			SizeBytes: d.GetSizeBytes(),
			// What the catalog says this volume has published, which is the only thing
			// that separates a new volume from one whose HEAD is gone on a host that has
			// never held it (see chain.Open's born branch).
			HeadCommitID: d.GetHeadCommitId(),
			LiveImage:    live, NewLayerID: ids.New().String(), Recovery: m.rec,
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

	// What this host holds and what it owes, against the record on disk — both facts are
	// about files, so it runs for an unattached volume too. A failure here is a note about
	// work already done and does not refuse the volume; it is returned so the loop backs off.
	stateErr := m.reconcile(ctx, v)
	if !attached {
		return stateErr
	}
	// Three steps, in this order, each a no-op when there is nothing to do: publish what
	// this volume owes, rotate if the tip has grown past the threshold, publish what that
	// rotation sealed. Neither failure refuses the volume — a layer that could not be
	// sealed or published keeps serving and keeps growing — except a HEAD that moved (see
	// publish). Rotation does not publish from inside itself: that nested a refusal, which
	// clears the chain, under a caller still dereferencing it.
	pubErr := m.publish(ctx, v)
	if v.chain == nil {
		// Refused — the only publishing failure that stops the volume is a HEAD that
		// moved, and after it there is no chain left to rotate.
		return errors.Join(stateErr, pubErr)
	}
	// A publish that merely failed does *not* return here: the layer stays pending and
	// maybeRotate's own guard declines to rotate over it (v6 §11), enforced where it can be
	// read rather than as a side effect of this function giving up early.
	tip := v.chain.Active
	if err := m.maybeRotate(ctx, v); err != nil {
		slog.Error("could not rotate this volume's tip; it keeps serving and keeps growing",
			"volume_id", id, "image", tip, "error", err)
		return errors.Join(stateErr, pubErr, fmt.Errorf("volume %s: %w", id, err))
	}
	pubErr = errors.Join(pubErr, m.publish(ctx, v))
	return errors.Join(stateErr, pubErr, m.settleSnapshot(v))
}

// snapshotPending says a snapshot has been asked for and not yet answered. The request
// keeps arriving until the Control Plane has seen the answer, so "asked for" alone would
// re-seal the tip every cycle for as long as the report took to land.
func (v *volume) snapshotPending() bool {
	return v.wantSnapshot != "" && v.wantSnapshot != v.snapshotID
}

// settleSnapshot answers a snapshot request, once the published history contains
// everything the volume held when the request arrived.
//
// A snapshot is a *name for a commit*, so answering it is naming one: nothing is copied
// and nothing is written. A commit id exists only after `Commit() → SUCCESS`, so a
// snapshot that is reported at all can be restored. (v5 wrote a manifest, which could
// name data that was not there.)
//
// It runs after the second publish because that is the first moment the layer this
// snapshot needs can be in the history; a pending layer means the next cycle answers.
func (m *Manager) settleSnapshot(v *volume) error {
	if !v.snapshotPending() || v.chain == nil {
		return nil
	}
	// Answered, not waited on, when this host has nowhere to publish. Without this the
	// request is a loop with no exit: nothing can ever land, so the answer never comes,
	// so the Control Plane keeps asking — and the rotation arm, which fires on the
	// request alone, seals the tip *every cycle*. An Agent with no object store would
	// grind a volume into one layer per cycle for as long as the snapshot was wanted.
	if m.pub == nil {
		v.snapshotID, v.snapshotCommit = v.wantSnapshot, ""
		v.snapshotErr = "this host has no object store, so it cannot publish a commit for a snapshot to name"
		slog.Warn("cannot take a snapshot: this host has no object store",
			"volume_id", v.id, "snapshot_id", v.wantSnapshot)
		return nil
	}
	if v.pending != nil {
		return nil
	}
	st, err := ReadState(m.paths, m.cfg.Root, v.id)
	if err != nil {
		return fmt.Errorf("volume %s: %w", v.id, err)
	}
	if len(st.Commits) == 0 {
		// Sealed and not published yet. Saying nothing is what lets a later cycle answer;
		// the failure §19 has is a snapshot left CREATING for ever, and that is now only
		// reachable through a publish that keeps failing, which is loud on its own.
		return nil
	}
	// The newest commit, and it is the right one because maybeRotate has already sealed the
	// tip for this request, unconditionally, and this runs after the publish that lands it.
	// Without that floor the end of the history can be a rotation that began before the
	// request — a real point, missing everything written since. demo:stage5 caught exactly
	// that, one second after the request.
	head := st.Commits[len(st.Commits)-1]
	v.snapshotID, v.snapshotCommit, v.snapshotErr = v.wantSnapshot, head.CommitID, ""
	slog.Info("snapshot taken: it names the commit carrying the layer sealed for it",
		"volume_id", v.id, "snapshot_id", v.wantSnapshot,
		"commit_id", head.CommitID, "layer", head.LayerID)
	return nil
}

// publish sends this volume's sealed layer to the object store, if it owes one.
//
// It runs before the rotation trigger is looked at (v6 §11: nothing rotates while a
// sealed layer is unpublished). Without it, an object store that is down turns into a
// chain of small layers each of which is a commit that never landed; with it the tip
// grows instead and at most one sealed layer waits. §15 wants the same at restart:
// publish the sealed layer, do not rotate again.
// checkSealed refuses a layer whose qcow2 header says QEMU already found an inconsistency
// it could not resolve. Nothing else in the publish path opens the image: the digest is
// over the bytes as stored and says only that they are the bytes that were written.
func (m *Manager) checkSealed(ctx context.Context, path string) error {
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()
	info, err := inspect(ctx, m.run, m.cfg.QemuImg, path)
	if err != nil {
		return fmt.Errorf("qcow: inspecting the sealed layer %s before publishing it: %w", path, err)
	}
	if info.Specific.Data.Corrupt {
		return fmt.Errorf("%w: the sealed layer %s has the qcow2 corrupt flag set, so a commit naming it could never be restored", ErrChainMismatch, path)
	}
	return nil
}

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
	// The sealed layer is inspected before it becomes a commit, and this is the only
	// place that can. A guest can corrupt its own image while it runs; QEMU sets the
	// qcow2 corrupt bit inside the file, so the bit rides through the object store —
	// the sealed bytes hash to what the manifest says and every integrity check passes.
	// Both readers refuse such a chain (Open's local branch and recovery's rebuild), so
	// publishing it produces a commit that returned SUCCESS and that no host can ever
	// restore: the one sentence §32 is built on.
	//
	// Doing it here rather than at the rotation is what makes it possible at all. The
	// tip QEMU holds is refused on the write lock, and after the snapshot the sealed
	// layer is QEMU's read-only backing — measured against the pinned 11.1.1, `qemu-img
	// info` reads it while the guest runs and is refused on the live tip with `Failed to
	// get shared "write" lock`. §7 allows exactly this: qemu-img on a sealed layer.
	if err := m.checkSealed(ctx, layer.Path); err != nil {
		return m.refuse(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_DURABILITY_LOST, err)
	}
	if err := m.pub.Publish(ctx, layer); err != nil {
		if errors.Is(err, commit.ErrHeadMoved) {
			// Another host published for this volume, so this one is not its writer: it
			// would keep accepting writes that can never be published. That is the one
			// publishing failure that stops the volume.
			//
			// Written down before it is refused, because a refusal that lives only in a map
			// is undone by the next SIGKILL and the Agent that starts after it publishes
			// again. Both errors go back; the volume stops either way.
			return errors.Join(
				m.recordFenced(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_PUBLISH_FENCED, err.Error()),
				m.refuse(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_PUBLISH_FENCED, err))
		}
		// Written down, not just logged. The volume goes on being served and the guest
		// goes on writing — §15's "S3 no disponible: la VM sigue" — so every other number
		// on this row reads normal and the only symptom is a backlog nobody is looking at.
		// §11 asks that new volumes stop being placed here long before the disk fills, and
		// this is the fact the fleet takes that decision on.
		v.stalled = true
		slog.Error("could not publish this volume's sealed layer; it stays on this host, nothing rotates until it lands, and this host stops taking new volumes",
			"volume_id", v.id, "layer", layer.Path, "commit_id", layer.CommitID, "error", err)
		return fmt.Errorf("volume %s: %w", v.id, err)
	}
	v.pending, v.stalled = nil, false
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

// checkFenced refuses a volume this host has recorded that it is not the writer of. The
// record is cleared by one thing only: a *higher* epoch, the fleet granting the volume
// back. Not a restart, not the object store answering, not the guest coming back.
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
		// A higher epoch is the fleet saying this host owns the volume again, so the refusal
		// lifts. The record is cleared here only when there is no chain: with one, regrant
		// needs the record to know this is a re-grant, and clears it together with the layer
		// list and any pending commit once it has asked the object store what the published
		// history is. Clearing it here left the host serving its stale fork; not clearing it
		// for a host with no chain refused the volume at every later epoch for ever.
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

// stopGuest pauses whatever VM is at this volume's QMP socket. Every failure is logged
// and none returned: the volume is being fenced either way, no socket means no guest, and
// a QEMU that has gone has already stopped writing. What must not happen is a fence that
// does not happen because a socket was slow.
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
// A layer is sealed exactly when it stops being the tip, and the tip is *observed* — from
// the QEMU that has it open, or failing that from `active/current` — so "sealed and not
// published" is derived: (the layers this host has seen as tips) − (the tip) − (the
// published ones). That closes the window rotate opens: a crash between the QMP switch
// and the record used to leave a complete layer nothing would publish, and the next
// commit chained past it, splicing a hole into the published history. Recording before
// the switch is worse and is rejected where it is described.
//
// Pending stays as what it always was: the commit id a sealed layer was promised under,
// so a retry is the same commit. Losing it costs a duplicate id, never a layer.
func (m *Manager) reconcile(ctx context.Context, v *volume) error {
	st, err := ReadState(m.paths, m.cfg.Root, v.id)
	if err != nil {
		return fmt.Errorf("volume %s: %w", v.id, err)
	}
	tip := LayerIDOfImage(v.chain.Active)
	dirty := st.ObserveTip(tip)
	// Only with a publisher, and only when nothing is already in hand. A host with no
	// object store configured has nothing to owe: giving it a pending layer would stop it
	// rotating for ever (v6 §11), which is the one thing rotate's own no-publisher branch
	// is written to avoid.
	if v.pending == nil && m.pub != nil {
		if err := m.adopt(v, &st, tip, &dirty); err != nil {
			return err
		}
	}
	if dirty {
		if err := WriteState(m.paths, m.cfg.Root, v.id, st); err != nil {
			return fmt.Errorf("volume %s: %w", v.id, err)
		}
	}
	// Nothing else reclaims local disk, so this runs every cycle over a directory only
	// this host writes to. A failure is reported and never refuses the volume: what it
	// costs is space, and the guest is being served.
	removed, err := sweep(m.paths, m.cfg.Root, v.id, v.chain.Active, st, v.pending)
	for _, path := range removed {
		slog.Warn("swept a layer file no chain reads through: it sits above the tip QEMU has open and no record names it, which is what a rotation interrupted between creating the overlay and switching to it leaves behind",
			"volume_id", v.id, "layer", path, "tip", v.chain.Active)
	}
	if err != nil {
		return fmt.Errorf("volume %s: %w", v.id, err)
	}
	// After the sweep, so the numbers a plan is made of are the ones still on disk. A
	// compaction touches only published files, so it runs whether or not a guest is
	// attached — and its last step *needs* the volume unattached. A failure is returned
	// and does not refuse the volume, because the chain it would have collapsed is exactly
	// the chain that goes on being served.
	return m.compact(ctx, v, st)
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
	sealed := st.SealedBelow(tip)
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
	// process that minted it: a duplicate id in a history is something a human can read,
	// and a layer nobody publishes is a hole nobody can see. The epoch is this host's
	// current one, which is the honest value — it holds this volume now or it would not be
	// here.
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

// recordCommit writes down that this host holds the layer of a commit that has landed:
// which commit a local layer came from — after `qemu-img rebase -u` the file no longer
// hashes to the object it came from, so nothing else can vouch for it — and which commit
// id a sealed layer was promised under.
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
// the volume carries from the Control Plane as its RPO. Either fires a rotation.
//
// §11's first invariant survives both: an idle volume does not commit. The size arm gets
// it for free — a tip nobody writes to does not grow — and the age arm needs the extra
// size condition below, because a volume that wrote nothing is *inside* its target, not
// behind it.
func (m *Manager) maybeRotate(ctx context.Context, v *volume) error {
	if v.pending != nil {
		return nil
	}
	// A snapshot request is a third trigger and the only one that is *asked* for. It
	// fires whatever the thresholds are, including on a host that rotates nothing —
	// which is the ordinary configuration, so leaving it out would make a snapshot a
	// thing that only works when something else is already configured.
	if m.cfg.RotateAtBytes <= 0 && v.rpo <= 0 && !v.snapshotPending() {
		return nil
	}
	size, err := m.paths.Size(v.chain.Active)
	if err != nil {
		// A tip that cannot be stat'd is not a measurement that failed, it is a layer that
		// is not there: somebody unlinked this volume's layers while a guest was writing to
		// them, and QEMU keeps the open inode. IMAGE_MISSING and not a bare error, because
		// that refusal's proto comment sends an operator to the bucket, which is the only
		// place the bytes can come back from.
		return m.refuse(v, storagev1.VolumeRefusal_VOLUME_REFUSAL_IMAGE_MISSING,
			fmt.Errorf("this volume's tip %s cannot be stat'd, so its layers were removed under it; a guest holding the open file goes on writing bytes nothing here can name: %w", v.chain.Active, err))
	}
	// The snapshot arm has no size condition, and that is the difference between the
	// triggers that fire on their own and the one a human asks for. The other two decline
	// to seal a tip under minRotateAtBytes; applying that here would let a snapshot taken
	// after half a megabyte of writes silently name the commit *before* them — a real
	// point in the history, not the one that was asked for. §11's "an idle volume does not
	// commit" is about the automatic triggers, and being wrong this way costs one small
	// layer.
	// Nothing rotates a chain that is already as deep as anything can rebuild — the
	// snapshot arm included, because a snapshot naming a commit on a chain no host can
	// restore is a snapshot that cannot be cloned or recovered from, which is all a
	// snapshot is for.
	//
	// The trade is that the tip grows instead. That degrades the RPO, and it does so
	// visibly: the writes past the last commit are exactly what commit_age and
	// unpublished_local_bytes report, and both climb from here. The other side is a
	// volume nothing can restore, so it is not close. The volume is not refused either:
	// every byte of it is still readable and still local, and taking a guest's disk away
	// over bookkeeping would be the larger harm.
	//
	// What is meant to keep this unreachable is §19's collapse (DefaultCompaction), and
	// what makes it reachable anyway is that a collapse waits for the guest to let go of
	// the files. A volume that is written hard and never detached walks here.
	if depth, err := m.depth(v); err == nil && depth >= MaxLayers {
		slog.Warn("this volume's chain is as deep as a rebuild can go, so its tip grows instead of rotating; its RPO degrades from here and only a compaction can undo it, which needs the guest to detach",
			"volume_id", v.id, "chain_depth", depth, "max_layers", MaxLayers, "tip_bytes", size)
		return nil
	}
	if v.snapshotPending() {
		slog.Info("sealing the tip for a snapshot: everything written before the request has to be in the history it names",
			"volume_id", v.id, "snapshot_id", v.wantSnapshot,
			"layer", LayerIDOfImage(v.chain.Active), "tip_bytes", size)
		return m.rotate(ctx, v, size)
	}
	if m.cfg.RotateAtBytes > 0 && size >= m.cfg.RotateAtBytes {
		return m.rotate(ctx, v, size)
	}
	if v.rpo <= 0 {
		return nil
	}
	// minRotateAtBytes is what "has been written to" means here (see the constant).
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

// depth is how many layers a guest reads through for this volume.
func (m *Manager) depth(v *volume) (int, error) {
	st, err := ReadState(m.paths, m.cfg.Root, v.id)
	if err != nil {
		return 0, err
	}
	return chainDepth(st, v.chain.Active), nil
}

// tipAge is how long it has been since this host published a commit for the volume, or
// since the chain was opened when it never has. Anchoring at the opening rather than at
// zero stops a volume that has never committed from being infinitely late and rotating an
// empty-ish layer on its first cycle.
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

	// A host with no publisher still records what it sealed but does not treat it as
	// something to wait for: without the record, layers sealed before an object store was
	// configured stay on disk, complete and invisible. It is not `pending` because pending
	// is what stops the next rotation (v6 §11), so such a host would rotate once and never
	// again.
	sealedBytes, err := m.paths.Size(sealed)
	if err != nil {
		return fmt.Errorf("qcow: measuring the sealed layer %s: %w", sealed, err)
	}
	if m.pub == nil {
		return m.recordSealed(v, sealed, sealedBytes)
	}
	// The commit id is minted here, once, and every attempt to publish this layer reuses
	// it; a fresh id per attempt would publish the same layer twice and read its own
	// success as somebody else's conflict (commit.Request carries the reasoning).
	//
	// LayerIDOfImage(sealed), not the id just minted: Rotate returns the layer it *sealed*
	// and layerID names the empty one the guest moved on to, and sealing frames under the
	// wrong nonce yields a layer that comes back from the object store refusing to open.
	// The size is measured again rather than reused from the trigger — the snapshot's drain
	// writes whatever QEMU still held.
	v.pending = &SealedLayer{
		VolumeID: v.id, LayerID: LayerIDOfImage(sealed), CommitID: ids.New().String(),
		Path: sealed, Epoch: v.epoch, PlainBytes: sealedBytes, VirtualSize: v.chain.SizeBytes,
	}
	// Written after the layer is sealed and in memory, never before: writing the record
	// before the QMP switch would publish a file QEMU is writing into (PendingCommit
	// carries the window and why it stays). In memory first, then on disk, so a write that
	// fails still leaves the layer pending here and nothing rotates over it.
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
// (ADR-0021). A `-drive file=...,if=virtio` disk has a generated drive id (`virtio0`) and
// an *anonymous* node QMP refuses as input; a `-blockdev node-name=vol` disk has a real
// node and no drive id. Taking only the first broke rotation on exactly the shape any
// libvirt-derived runner produces.
//
// The overlay name is needed only on the node path — blockdev-snapshot-sync answers "New
// overlay node-name missing" without one. It cannot be the layer's id: QEMU caps a node
// name at 31 characters and a v7 UUID is 36, which is why the names are `spinN`. They are
// checked against the whole graph, because a name a *backing* node holds is as unusable
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

// ancestry copies the generations the Control Plane sent, in the order it sent them. The
// order is the whole content of the field — a chain is rebuilt oldest first, each layer
// repointed at the one below it — so it is never sorted or deduplicated here: a desired
// state that named them in the wrong order is a Control Plane defect, and reordering it
// would build a plausible chain out of somebody else's bytes.
func ancestry(d *storagev1.DesiredVolume) []Ancestor {
	out := make([]Ancestor, 0, len(d.GetAncestry()))
	for _, a := range d.GetAncestry() {
		out = append(out, Ancestor{VolumeID: a.GetVolumeId(), CommitID: a.GetCommitId()})
	}
	return out
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
func (m *Manager) probe(ctx context.Context, volumeID string) (image string, corrupt bool, err error) {
	ctx, cancel := m.withTimeout(ctx)
	defer cancel()

	client, err := qmp.Dial(ctx, m.dialer, QMPSocket(m.cfg.Root, volumeID))
	if err != nil {
		if errors.Is(err, qmp.ErrNoEndpoint) {
			return "", false, nil
		}
		return "", false, err
	}
	defer func() { _ = client.Close() }()

	devices, err := client.BlockDevices()
	if err != nil {
		return "", false, err
	}
	for _, d := range devices {
		// The first file, reported rather than counted: what an operator does about a
		// VM running the wrong image starts with knowing which image it is.
		if d.File != "" {
			// The corrupt flag comes back with it rather than in a question of its own:
			// it is a field of the answer this cycle already asks for, so noticing costs
			// nothing, and a second exchange would be a second chance to miss it.
			return d.File, d.Corrupt, nil
		}
	}
	// A QEMU with no block devices at all. It is not this volume's VM and it is not
	// running anything else's image either, so there is nothing to refuse over.
	return "", false, nil
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

// gap is where this volume stands: how far behind the object store it is, and what it
// costs this host to be there. They are §28's four per-volume numbers.
//
// Measured on every report rather than kept in counters. A counter would be a fourth
// writer of facts the disk and state.json already hold.
//
// A volume with no chain — refused, or given up — reports zeros rather than its last known
// numbers: the refusal is what an operator needs, and stale numbers beside it invite the
// reading that the volume is still making progress.
func (m *Manager) gap(v *volume) volumeGap {
	if v.chain == nil {
		return volumeGap{}
	}
	g := volumeGap{}
	if n, err := m.paths.Size(v.chain.Active); err == nil {
		g.unpublishedLocalBytes = n
	}
	if v.pending != nil {
		if n, err := m.paths.Size(v.pending.Path); err == nil {
			g.unpublishedLocalBytes += n
		}
	}
	// The disk's number and not the record's, which is why it is a listing: an orphan
	// overlay a rotation left behind occupies space no record names. A directory that
	// cannot be listed leaves it at zero rather than failing the report — every other
	// number here is still true, and a host that stops reporting is a host that looks
	// gone.
	if n, err := layerBytes(m.paths, m.cfg.Root, v.id); err == nil {
		g.localDiskBytes = n
	}
	st, err := ReadState(m.paths, m.cfg.Root, v.id)
	if err != nil {
		// The record is unreadable; the tip on disk is still one layer a guest reads
		// through, and claiming zero depth for a chain that exists would be worse.
		g.chainDepth = 1
		return g
	}
	g.chainDepth = chainDepth(st, v.chain.Active)
	// The newest commit this host published, which the catalog keeps so it can tell a
	// *later* host that this volume has a history — see metadata.Volume.HeadCommitID. Read
	// from the record rather than tracked in memory, for the reason every other number
	// here is: a restarted Agent that reported nothing would look like a volume that has
	// never published, and that is the exact claim the guard turns on.
	if n := len(st.Commits); n > 0 {
		g.publishedCommitID = st.Commits[n-1].CommitID
	}
	if st.LastCommitAt == 0 {
		// Never committed here. Zero is the honest answer: an age measured from an anchor
		// this host invented would read as an RPO somebody could rely on.
		return g
	}
	// §11's definition, which is not "how long since the last commit": *the age of the
	// newest commit that covers everything written*. A tip the guest has not written to
	// is covered by the commit below it, so the volume is inside its RPO however long ago
	// that was — and the other reading has both errors in it. An idle fleet drifts into
	// looking like a fleet about to lose data, until the alert that fires on all of it
	// gets turned off; and a volume whose guest is writing hard reads 0 at the instant a
	// commit lands, which is the instant its exposure starts growing again.
	//
	// The condition is the trigger's own (see maybeRotate), so the reported number and
	// the decision to commit can never contradict each other: a volume cannot report
	// itself past a target the trigger is treating it as idle for.
	if tip, err := m.paths.Size(v.chain.Active); err == nil && tip < minRotateAtBytes {
		return g
	}
	g.lastCommitAge = time.Duration(m.clk.Wall().UnixMilli()-st.LastCommitAt) * time.Millisecond
	return g
}

// volumeGap is what one report says about one volume beyond its identity.
type volumeGap struct {
	lastCommitAge         time.Duration
	unpublishedLocalBytes int64
	chainDepth            int
	localDiskBytes        int64
	publishedCommitID     string
}

// Volumes reports what this host is holding, ordered by volume id (deterministic).
func (m *Manager) Volumes(context.Context) ([]agent.VolumeStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]agent.VolumeStatus, 0, len(m.vols))
	for _, v := range m.vols {
		g := m.gap(v)
		out = append(out, agent.VolumeStatus{
			VolumeID:         v.id,
			Epoch:            v.epoch,
			SnapshotID:       v.snapshotID,
			SnapshotCommitID: v.snapshotCommit,
			SnapshotError:    v.snapshotErr,
			// Measured here rather than tracked, because a tracked number is one more
			// thing that can be wrong: the tip's size and the sealed layer's are on the
			// disk, and the last commit's time is in state.json.
			LastCommitAge:         g.lastCommitAge,
			UnpublishedLocalBytes: g.unpublishedLocalBytes,
			ChainDepth:            g.chainDepth,
			LocalDiskBytes:        g.localDiskBytes,
			PublishedCommitID:     g.publishedCommitID,
			PublishStalled:        v.stalled,
			Refusal:               v.refusal,
			// Empty when there is no refusal, which is what the wire's healthy value is.
			RefusalDetail: v.detail,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].VolumeID < out[j].VolumeID })
	return out, nil
}

// Fence stops serving what this host is no longer the writer of, and stops its guest.
//
// The two callers want opposite things and the difference is whether the Control Plane
// already knows. A fence that follows a refused report is its own decision coming back,
// so the volume is dropped outright and says nothing; a lease that lapsed is kept with
// the refusal on it, and the next report is what carries the news off this host.
//
// All three ways in are treated the same — a lapsed lease, a refused report, a lost
// compare-and-set — and whether they should be is an open question in docs/plan/STATUS.md.
//
// Read-only would be better than stopping and is not available. Measured against the
// pinned QEMU: the drive QEMU creates for itself has an anonymous node QMP refuses as
// input, and with named nodes blockdev-reopen still refuses — "Read-only block node
// cannot support read-write users" — because the guest's virtio driver holds it
// read-write. Pausing does not kill the VM, whose lifetime belongs to whoever launched
// it (ADR-0021).
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
