package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"sort"
	"sync"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/blockdev"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/ioclass"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/vhost"
	"github.com/spin-stack/storage/internal/wal"
)

// Volume is one volume's data path on this host: the WAL it appends to, the block
// device a guest is served from, and the vhost-user server behind its socket. It owns
// them, starts them together and stops them together.
//
// It is deliberately not a method-set on Loop, and nothing here knows what a heartbeat
// or a Control Plane is (ADR-0021). spin's runner already owns the QEMU process and the
// host's lifecycle; when storage lands there it must be able to take this type and the
// manager below without taking the reconciliation loop with them.
//
// With an object store and a lease it runs in remote mode: a FLUSH is the §14.4 ACK
// path, and it returns only once every covering object is verified and the lease is
// still valid on the monotonic clock (INV-06, INV-07). Without a store it is local-only
// — writes are taken, reads are served, and a FLUSH ACKs on fdatasync alone (§14.8)
// rather than claiming a durability nothing backs.
type Volume struct {
	id    string
	epoch int64
	// root is what wal.SegmentDir is given; the segments themselves land in
	// <root>/<volume-id>/<epoch>, and the epoch is in that path because a promoted
	// writer must never append into the segments of the epoch it replaced.
	root   string
	socket string

	log *wal.Log
	dev *blockdev.Device

	cancel context.CancelFunc
	done   chan struct{}
}

// ID is the volume this runtime serves.
func (v *Volume) ID() string { return v.id }

// Epoch is the epoch it was started under.
func (v *Volume) Epoch() int64 { return v.epoch }

// Device is the block device a guest is served from. It is what a test drives when
// there is no QEMU to drive it.
func (v *Volume) Device() *blockdev.Device { return v.dev }

// Status is what the Agent reports about this volume: the watermarks the log actually
// holds, qualified by the epoch they were produced under (§12.3).
func (v *Volume) Status() VolumeStatus {
	w := v.log.Watermarks()
	return VolumeStatus{
		VolumeID:          v.id,
		Epoch:             v.epoch,
		LocalSequence:     int64(w.Local),
		DurableSequence:   int64(w.Durable),
		PublishedSequence: int64(w.Published),
		RemoteGapBytes:    v.log.RemoteGapBytes(),
	}
}

// stop tears the runtime down and waits for the serve loop to leave. Closing the log
// last is the ordering that matters: the server must stop answering before the thing it
// answers from goes away.
func (v *Volume) stop() error {
	v.cancel()
	<-v.done
	return v.log.Close()
}

// ListenFunc opens the vhost-user socket for one volume. It is injected because a Unix
// socket is a kernel object (INV-01): production passes hostio.Listen, and a test passes
// something it can close.
type ListenFunc func(socket string) (vhost.Listener, error)

// KeysFunc fetches one volume's wrapped key material. Production passes
// Loop.VolumeKeys, which asks the Control Plane once and caches the answer; it is a
// function rather than the Loop for the same reason Lease is (see leaseFunc), and
// because ADR-0021 keeps this type from knowing what a Control Plane is.
type KeysFunc func(ctx context.Context, volumeID string) (VolumeKeys, error)

// encryptionFor unwraps this volume's DEK and binds it to the volume (§15.1). It
// returns nil, nil for an Agent with no KMS — the dev/local mode — and an error for
// every other failure, because the alternative to encrypting is not "encrypt later",
// it is writing this guest's data into the bucket in the clear.
func (m *VolumeManager) encryptionFor(ctx context.Context, id string, vol [16]byte) (*wal.Encryption, error) {
	if m.deps.KMS == nil {
		return nil, nil //nolint:nilnil // no KMS is a mode, not a failure: see VolumeManagerDeps.KMS
	}
	keys, err := m.deps.Keys(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("agent: volume %s: reading key material: %w", id, err)
	}
	if keys.KEKID != m.deps.KMS.KEKID() {
		// Not a §15.1 rotation — that is the *DEK* rotating under one KEK. This is the
		// volume having been wrapped by a KEK this host does not hold, and unwrapping
		// would fail on the AEAD anyway. Saying which key is missing turns an opaque
		// authentication failure into an operational instruction.
		return nil, fmt.Errorf("agent: volume %s is wrapped under KEK %q; this host holds %q",
			id, keys.KEKID, m.deps.KMS.KEKID())
	}
	dek, err := m.deps.KMS.UnwrapDEK(keys.DEKWrapped, keys.DEKKeyID)
	if err != nil {
		return nil, fmt.Errorf("agent: volume %s: unwrapping the DEK (version %d): %w", id, keys.DEKKeyID, err)
	}
	enc, err := wal.NewEncryption(dek, vol)
	if err != nil {
		return nil, fmt.Errorf("agent: volume %s: %w", id, err)
	}
	return enc, nil
}

// encKeyID is the version the batcher stamps on objects: the DEK's, or 0 when there is
// no encryption — which is what 0 means everywhere else on this path.
func encKeyID(e *wal.Encryption) uint32 {
	if e == nil {
		return 0
	}
	return e.DEK.KeyID
}

// leaseFunc adapts the Agent's lease question into the wal.LeaseChecker the Log gates
// its durable ACK on (§12.2, INV-06).
//
// It is a *function*, resolved on every call, and that is the whole point. Loop's
// applyLease allocates a new lease.Manager whenever the Control Plane changes the TTL,
// so a Log holding a captured *lease.Manager would be gated by an object nobody renews:
// it would go invalid at the old TTL and never recover, and the volume would self-fence
// while the host is perfectly healthy. Calling through Loop.LeaseValid resolves the
// current manager every time.
type leaseFunc func() bool

func (f leaseFunc) Valid() bool { return f() }

// VolumeManagerConfig is where this host keeps things.
type VolumeManagerConfig struct {
	// DataDir holds the WALs: <data-dir>/wal/<volume-id>/<epoch>.
	DataDir string
	// SocketDir holds one vhost-user socket per volume: <socket-dir>/<volume-id>.sock.
	SocketDir string
	// Limits bound the local WAL (§5.7). The zero value is legal and unbounded.
	Limits wal.Limits
	// UploadAttempts is how many times an object PUT is retried before the FLUSH that
	// needed it fails. Zero means 3.
	UploadAttempts int
	// HostID is this host's fleet identity. A checkpoint names the host publishing it,
	// which is what stops a second host publishing into an epoch it merely knows the
	// number of (§12.3–12.4). Without it no durability scheduler runs.
	HostID string
	// CheckpointBytes and CheckpointInterval are the two triggers of §21.1: 256 MiB of
	// local WAL, or two minutes, whichever comes first. Zero means the design's value.
	CheckpointBytes    int64
	CheckpointInterval time.Duration
	// CheckpointPoll is how often the byte trigger is examined. Not a design number —
	// see durability.go.
	CheckpointPoll time.Duration
}

// VolumeManagerDeps are the injected collaborators (INV-01).
type VolumeManagerDeps struct {
	Clock  clock.Clock
	Disk   disk.Disk
	Listen ListenFunc
	// Mapper turns the front-end's memory-region descriptors into host memory, and
	// EventFD adapts the kick/call descriptors it sends. Both are kernel objects, and
	// both live behind the ADR-0020 exemption in internal/vhost/hostio. Neither is
	// touched until a front-end connects, but both are required here: vhost.NewServer
	// refuses a Config without them, and a manager that discovers that in its serve
	// goroutine has already told Apply the volume started.
	Mapper  vhost.Mapper
	EventFD vhost.EventFDFunc
	// Store is where FLUSH makes a write durable (§14.4). Nil is local-only mode: the
	// device serves and takes writes, and no FLUSH ever claims remote durability.
	Store objectstore.Store
	// Lease answers "does this host still hold its lease, on the monotonic clock?".
	// It is required whenever Store is set, and it must be a call through to the
	// current lease — see leaseFunc.
	Lease func() bool
	// KMS unwraps a volume's DEK, and Keys is where the wrapped one comes from. Both
	// or neither: an Agent with no KMS runs unencrypted, which is the dev/local mode
	// (§6.2) the DST harness and the QEMU lane use and which claims nothing it does
	// not do. An Agent *with* a KMS encrypts every volume it serves or serves none of
	// them — see start. §15.1 puts the unwrap at attach and nowhere else: one KMS call
	// outside the data path, and the DEK lives in memory only.
	KMS  crypto.KMS
	Keys KeysFunc
	// IOClass arbitrates the Agent's I/O between classes (INV-17, §11). One per Agent,
	// not one per volume: the budget it hands out is a share of the host's NVMe and NIC
	// (§10, `background_nvme_budget: 30% de IOPS/BW`). Nil disables the gate, which is
	// what a unit test with no contention wants.
	IOClass *ioclass.Scheduler
}

// VolumeManager owns the live runtimes and is the Agent's VolumeSource. Apply is the
// whole of the reconciliation: it diffs the desired state against what is running.
type VolumeManager struct {
	cfg  VolumeManagerConfig
	deps VolumeManagerDeps

	mu      sync.Mutex
	volumes map[string]*Volume
	// fencedEpoch is the highest epoch this host has been fenced out of, per volume.
	// Without it a fenced volume would come straight back: the Control Plane refuses
	// the *report* while GetDesiredState may still list the volume for this host, so
	// the next Apply would find no runtime and start one — serving a volume this host
	// has just been told it does not own. Only a higher epoch clears it, because a
	// higher epoch is the Control Plane granting the volume again.
	fencedEpoch map[string]int64
	closed      bool
}

// NewVolumeManager validates the wiring and returns a manager with nothing running.
func NewVolumeManager(cfg VolumeManagerConfig, deps VolumeManagerDeps) (*VolumeManager, error) {
	switch {
	case cfg.DataDir == "":
		return nil, errors.New("agent: a data directory is required to hold the WALs")
	case cfg.SocketDir == "":
		return nil, errors.New("agent: a socket directory is required to serve volumes")
	case deps.Clock == nil:
		return nil, errors.New("agent: a clock must be injected (INV-01)")
	case deps.Disk == nil:
		return nil, errors.New("agent: a disk must be injected (INV-01)")
	case deps.Listen == nil:
		return nil, errors.New("agent: a listen function must be injected (INV-01)")
	case deps.Mapper == nil:
		return nil, errors.New("agent: a memory mapper must be injected (ADR-0020)")
	case deps.EventFD == nil:
		return nil, errors.New("agent: an EventFD adapter must be injected (ADR-0020)")
	case deps.Store != nil && deps.Lease == nil:
		// wal.EnableRemote accepts a nil lease without complaining and the failure
		// surfaces much later, as ErrNoLease inside durableStep — at the first FLUSH,
		// in the guest's I/O path. A store with nothing fencing the writer is not a
		// configuration worth starting.
		return nil, errors.New("agent: an object store needs a lease to gate its durable ACKs (§12.2, INV-06)")
	case deps.KMS != nil && deps.Keys == nil:
		// A KMS with nowhere to get wrapped keys from would unwrap nothing and every
		// volume would fail to start — at attach, one at a time, looking like a
		// Control Plane problem. It is a wiring problem, and it is visible here.
		return nil, errors.New("agent: a KMS needs a source of wrapped volume keys (§15.1)")
	}
	return &VolumeManager{
		cfg: cfg, deps: deps,
		volumes:     map[string]*Volume{},
		fencedEpoch: map[string]int64{},
	}, nil
}

// Apply makes the running set match desired: start what is new, stop what left, and
// replace what was promoted to a new epoch.
//
// Failures are collected rather than returned at the first one. A volume whose socket
// is taken must not stop the others from being served — the loop retries the whole
// desired state on its next cycle, and Apply is idempotent for everything that already
// started.
func (m *VolumeManager) Apply(ctx context.Context, desired []*storagev1.DesiredVolume) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return errors.New("agent: the volume manager is closed")
	}
	m.mu.Unlock()

	live := make(map[string]bool, len(desired))
	var errs []error

	for _, d := range desired {
		id := d.GetVolumeId()
		if err := validateDesired(d); err != nil {
			errs = append(errs, err)
			continue
		}
		live[id] = true

		m.mu.Lock()
		existing, running := m.volumes[id]
		fencedAt, wasFenced := m.fencedEpoch[id]
		m.mu.Unlock()

		if wasFenced && d.GetEpoch() <= fencedAt {
			// Fenced out of this epoch and the Control Plane has not granted a newer
			// one. Silently, because the desired state repeats every few seconds and
			// this is the steady state until the volume is either re-granted or
			// dropped from the list.
			continue
		}

		if running {
			if existing.epoch == d.GetEpoch() {
				continue // already serving exactly this
			}
			// Promoted. The old runtime is torn down before the new one opens, because
			// both would otherwise want the same socket.
			if err := m.remove(id); err != nil {
				errs = append(errs, fmt.Errorf("agent: replacing volume %s at epoch %d: %w", id, d.GetEpoch(), err))
				continue
			}
		}

		v, err := m.start(ctx, d)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		m.mu.Lock()
		m.volumes[id] = v
		m.mu.Unlock()
	}

	// Whatever the Control Plane no longer lists for this host: promoted away,
	// detached, or fenced. In every case this host has stopped being its writer.
	m.mu.Lock()
	var gone []string
	for id := range m.volumes {
		if !live[id] {
			gone = append(gone, id)
		}
	}
	m.mu.Unlock()
	for _, id := range gone {
		if err := m.remove(id); err != nil {
			errs = append(errs, fmt.Errorf("agent: stopping volume %s: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

// validateDesired refuses what this host cannot serve, on the volume rather than at the
// first guest request — by which time a guest has a device it cannot use.
func validateDesired(d *storagev1.DesiredVolume) error {
	id := d.GetVolumeId()
	if id == "" {
		return errors.New("agent: the Control Plane listed a volume with no id")
	}
	if _, err := ids.Parse(id); err != nil {
		// The WAL carries the volume id as 16 raw bytes and the socket path is built
		// from it; a value that is not a UUID has neither spelling.
		return fmt.Errorf("agent: volume id %q is not a UUID (INV-22): %w", id, err)
	}
	if d.GetEpoch() < 0 {
		return fmt.Errorf("agent: volume %s was listed at epoch %d", id, d.GetEpoch())
	}
	return nil
}

// start builds one volume's runtime and puts its serve loop under supervision. It
// unwinds everything it opened if any step fails: a log left open on a volume nothing
// serves would hold the WAL and be invisible to Volumes.
func (m *VolumeManager) start(ctx context.Context, d *storagev1.DesiredVolume) (*Volume, error) {
	id := d.GetVolumeId()
	u, err := ids.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("agent: volume id %q: %w", id, err)
	}

	// The root is <data-dir>/wal and nothing more: wal.SegmentDir appends the volume
	// and the epoch itself, so passing an already-namespaced path produced
	// <data-dir>/wal/<id>/<epoch>/<id>/<epoch>. It went unnoticed because sim.Disk.List
	// matches by prefix, so the test asserting the convention passed on the doubled
	// path — see TestSocketAndWALPathsArePerVolumeAndEpoch, which now asserts the
	// directory exactly.
	root := path.Join(m.cfg.DataDir, "wal")

	// Resume, or start fresh. Which one is decided by the disk, not by configuration:
	// a directory that already holds segments belongs to a previous run of this Agent,
	// and building a fresh log over it would leave every one of those records unread —
	// including ones a guest was told were durable. wal refuses that outright, so the
	// volume would be unusable rather than wrong, but unusable is not the goal.
	dir := wal.SegmentDir(root, [16]byte(u), uint64(d.GetEpoch()))
	existing, err := m.deps.Disk.List(dir)
	if err != nil {
		return nil, fmt.Errorf("agent: volume %s: looking for an existing WAL in %s: %w", id, dir, err)
	}

	// §15: every payload of guest data is sealed with the volume's DEK before any PUT.
	// The unwrap happens here, at attach, and nowhere else (§15.1) — one KMS call
	// outside the data path, and what it returns never leaves memory.
	//
	// It fails the volume rather than degrading it. An Agent that fell back to
	// plaintext would write cleartext into a bucket under a name that says otherwise,
	// and §15.3's crypto-shredding guarantee cannot survive that: the objects would
	// still be readable after the DEK was destroyed.
	enc, err := m.encryptionFor(ctx, id, [16]byte(u))
	if err != nil {
		return nil, err
	}

	var log *wal.Log
	resuming := len(existing) > 0
	if resuming {
		// The durable point is not passed here: it lives in the object store, and
		// fetching it now would mean a round trip before the volume could be served.
		// It arrives with the base (see baseFetch below), which is the only moment it
		// is known. Until then the log reports durable = 0 — an understatement, which
		// is the safe direction for every rule that reads it.
		log, err = wal.ResumeAwaitingBase(m.deps.Disk, root, m.deps.Clock,
			[16]byte(u), uint64(d.GetEpoch()), m.cfg.Limits, enc)
		if err != nil {
			return nil, fmt.Errorf("agent: volume %s: resuming the WAL in %s: %w", id, dir, err)
		}
	} else {
		log = wal.NewLog(m.deps.Disk, root, m.deps.Clock, [16]byte(u), uint64(d.GetEpoch()), m.cfg.Limits)
		if enc != nil {
			log.EnableEncryption(enc)
		}
	}

	// Remote mode, and with it the uploader and the §14.4 ACK path. Without a store
	// the Log stays local: it takes writes and serves reads, and a FLUSH ACKs on
	// fdatasync alone (§14.8) rather than claiming a durability it cannot back.
	if m.deps.Store != nil {
		attempts := m.cfg.UploadAttempts
		if attempts <= 0 {
			attempts = 3
		}
		log.EnableRemote(
			// The batcher stamps the object's key version, so it must be the same one
			// the records carry: a mismatch names an object after a key that did not
			// seal it. Zero is right exactly when there is no encryption.
			wal.NewBatcher(m.deps.Clock, [16]byte(u), uint64(d.GetEpoch()), encKeyID(enc), wal.DefaultBatchConfig()),
			wal.NewUploader(m.deps.Store, attempts),
			leaseFunc(m.deps.Lease),
		)
	}

	dev, err := blockdev.New(log, d.GetSizeBytes())
	if err != nil {
		_ = log.Close()
		return nil, fmt.Errorf("agent: volume %s: %w", id, err)
	}

	socket := path.Join(m.cfg.SocketDir, id+".sock")
	v := &Volume{
		id: id, epoch: d.GetEpoch(), root: root, socket: socket,
		log: log, dev: dev,
		done: make(chan struct{}),
	}

	// One listener is opened here so a socket that cannot be bound fails Apply rather
	// than disappearing into a goroutine. The supervisor opens the later ones.
	ln, err := m.deps.Listen(socket)
	if err != nil {
		_ = log.Close()
		return nil, fmt.Errorf("agent: volume %s: opening %s: %w", id, socket, err)
	}

	serveCtx, cancel := context.WithCancel(ctx)
	v.cancel = cancel
	go m.supervise(serveCtx, v, ln, d.GetBlockSize())
	if resuming {
		go m.fetchBase(serveCtx, v, [16]byte(u), uint64(d.GetEpoch()))
	}
	if err := m.checkpointsEnabled(); err != nil {
		// Said once, at start, rather than every poll: a volume that will never reclaim
		// a byte is worth one line explaining why.
		slog.Info("no durability scheduler for this volume; local WAL will not be reclaimed",
			"volume_id", id, "reason", err)
	} else {
		go m.checkpointLoop(serveCtx, v, [16]byte(u), uint64(d.GetEpoch()))
	}
	return v, nil
}

// fetchBase rebuilds the read view from the object store and hands it to the log. This
// is the lazy half of BUILD-INVENTORY increment 5: the volume is already being served,
// and only its *reads* are waiting on this.
//
// It owes the log exactly one InstallBase or FailBase on every path, which is why there
// is no early return that skips both. A log that gets neither parks every read for the
// life of the process.
func (m *VolumeManager) fetchBase(ctx context.Context, v *Volume, volumeID [16]byte, epoch uint64) {
	if m.deps.Store == nil {
		// Local-only mode has no object store to recover from, so the local segments
		// are all there is and they have already been replayed. Nothing to wait for.
		v.log.FailBase(errors.New("this Agent has no object store: the read view is whatever the local WAL holds"))
		return
	}

	base, durable, err := recovery.Recover(ctx, m.deps.Store, nil, volumeID, epoch)
	if err != nil {
		// Refuse loudly rather than serve zeros. An empty view where data belongs is
		// indistinguishable from a fresh volume, which is the failure this whole
		// increment exists to make impossible.
		slog.Error("the volume's read view could not be recovered; its reads will fail",
			"volume_id", v.id, "epoch", v.epoch, "error", err)
		v.log.FailBase(err)
		return
	}
	if err := v.log.InstallBase(base, durable); err != nil {
		slog.Error("the recovered read view could not be installed",
			"volume_id", v.id, "epoch", v.epoch, "error", err)
		v.log.FailBase(err)
		return
	}
	slog.Info("read view recovered from the object store",
		"volume_id", v.id, "epoch", v.epoch, "durable_sequence", durable)
}

// supervise runs the vhost server and restarts it when it returns.
//
// vhost.Server.Serve returns on any session error, and a guest reconnects to the socket
// — so without this the first protocol error would end the volume's service for the
// lifetime of the process, silently. Each attempt gets a fresh listener because the
// previous one is closed by whatever ended the session (including Serve's own
// cancellation path, which closes the listener to unblock Accept).
func (m *VolumeManager) supervise(ctx context.Context, v *Volume, first vhost.Listener, blockSize int32) {
	defer close(v.done)

	ln := first
	for {
		if ctx.Err() != nil {
			_ = ln.Close()
			return
		}

		srv, err := vhost.NewServer(ln, vhost.Config{
			Backend:   v.dev,
			Mapper:    m.deps.Mapper,
			Serial:    v.id,
			BlockSize: uint32(blockSize),
		}, m.deps.EventFD)
		if err != nil {
			// A configuration the device refuses will be refused again identically;
			// retrying it is a busy loop that logs forever.
			slog.Error("volume cannot be served", "volume_id", v.id, "epoch", v.epoch, "error", err)
			_ = ln.Close()
			return
		}

		err = srv.Serve(ctx)
		_ = ln.Close()
		if ctx.Err() != nil {
			return
		}
		slog.Warn("vhost session ended; re-listening",
			"volume_id", v.id, "epoch", v.epoch, "socket", v.socket, "error", err)

		ln, err = m.deps.Listen(v.socket)
		if err != nil {
			// Nothing left to serve on. The volume stays in the desired state, so the
			// next Apply that finds no runtime for it starts one again.
			slog.Error("cannot re-open the volume's socket",
				"volume_id", v.id, "socket", v.socket, "error", err)
			return
		}
	}
}

// Fence stops serving the named volumes, because the Control Plane has refused their
// reports: this host is not their writer anymore — the epoch moved on, the primary
// changed, or the volume is unknown to the fleet (§12.3, §16). Resolves DEV-0012.
//
// **It stops reads as well as writes, and it takes the socket with it.** That is the
// safe side of a choice with no comfortable option. A read of already-written bytes
// breaks no durability rule, but it is a stale read handed to a guest whose volume now
// has a different writer somewhere else, and the guest has no way to tell. The cost is
// paid by that guest: QEMU reconnects on its own, finds nothing listening, and its I/O
// stalls rather than being answered by a host with no authority to answer it.
//
// A volume re-granted to this host at a higher epoch starts a fresh runtime on the next
// Apply, under the new epoch's WAL root, and the guest's pending reconnect succeeds.
func (m *VolumeManager) Fence(_ context.Context, volumeIDs []string) error {
	var errs []error
	for _, id := range volumeIDs {
		m.mu.Lock()
		v, running := m.volumes[id]
		if running && v.epoch > m.fencedEpoch[id] {
			m.fencedEpoch[id] = v.epoch
		}
		m.mu.Unlock()
		if !running {
			continue
		}
		slog.Warn("volume fenced; tearing its runtime down",
			"volume_id", id, "epoch", v.epoch)
		if err := m.remove(id); err != nil {
			errs = append(errs, fmt.Errorf("agent: fencing volume %s: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

// remove stops one runtime and drops it. It is safe to call for a volume that is not
// running.
func (m *VolumeManager) remove(id string) error {
	m.mu.Lock()
	v, ok := m.volumes[id]
	if ok {
		delete(m.volumes, id)
	}
	m.mu.Unlock()
	if !ok {
		return nil
	}
	return v.stop()
}

// Volumes implements VolumeSource over the live runtimes, ordered by volume id
// (INV-02). This is what makes a heartbeat honest: before it, the binary reported an
// empty set forever — every heartbeat said remote_backlog=0 and carried zero reports.
func (m *VolumeManager) Volumes(context.Context) ([]VolumeStatus, error) {
	m.mu.Lock()
	out := make([]VolumeStatus, 0, len(m.volumes))
	for _, v := range m.volumes {
		out = append(out, v.Status())
	}
	m.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].VolumeID < out[j].VolumeID })
	return out, nil
}

// Device returns the block device serving one volume, if it is running.
func (m *VolumeManager) Device(volumeID string) (*blockdev.Device, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.volumes[volumeID]
	if !ok {
		return nil, false
	}
	return v.dev, true
}

// Close stops every runtime. It is idempotent.
func (m *VolumeManager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	ids := make([]string, 0, len(m.volumes))
	for id := range m.volumes {
		ids = append(ids, id)
	}
	m.mu.Unlock()

	var errs []error
	for _, id := range ids {
		if err := m.remove(id); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

var _ VolumeSource = (*VolumeManager)(nil)
