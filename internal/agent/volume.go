package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"sort"
	"sync"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/blockdev"
	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
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
	// enc is this volume's DEK bound to its id, or nil when the Agent has no KMS.
	// The Log already holds it for the write path; it is kept here because the *read*
	// path needs it too and rebuilding the base happens on a goroutine that has no
	// other way to reach it. Passing a literal nil there is the bug this field exists
	// to make hard to write — see fetchBase.
	enc *wal.Encryption
	// vol is the volume id as the object store keys use it.
	vol [16]byte
	// imageETag is the manifest this volume booted from, and what its own publish CASes
	// against (ADR-0026). Empty means there was none — a first boot — and publishing
	// with an empty ETag is create-only, so a first boot racing another still produces
	// one image and one refusal.
	imageETag string
	// store and rnd are kept here because publishing happens as the runtime tears down,
	// which is after the manager has stopped tracking it.
	store objectstore.Store
	rnd   io.Reader

	cancel context.CancelFunc
	done   chan struct{}
	// baseDone is closed once fetchBase has resolved the read view, whether it installed
	// a base or failed one. stop() waits on it, and the reason is not tidiness: the base
	// is everything the volume held before this session, so publishing before it lands
	// writes an image with that data missing — and the publish CASes over the previous
	// manifest, so it would replace the volume's own history with a partial view of it.
	// nil when this Agent has no object store and there is no base to wait for.
	baseDone chan struct{}
	// baseFailed records that fetchBase could not resolve the view. A volume that never
	// got its base must not publish at all: its view is not a subset of the truth, it is
	// a different thing.
	baseFailed bool
}

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
	}
}

// stop tears the runtime down and waits for the serve loop to leave. Closing the log
// last is the ordering that matters: the server must stop answering before the thing it
// answers from goes away.
func (v *Volume) stop() error {
	v.cancel()
	<-v.done
	v.publish()
	return v.log.Close()
}

// publish writes the volume's state to the object store. It is the whole durability
// contract of V1 (ADR-0026): nothing else leaves the host, and what this writes is what
// the next boot — or a clone — reads.
//
// It runs after the serve loop has gone, because that is the first moment nothing can
// append, which is what makes ViewAtRest's precondition true and the image a point rather
// than a smear.
//
// A failure is loud and does not stop the teardown. Refusing to close would leave a
// volume neither serving nor released, and the data is still in the local WAL either way;
// what an operator needs is to know this session did not reach the object store.
func (v *Volume) publish() {
	if v.store == nil {
		return // local-only Agent: the local WAL is all there is, by design
	}
	if v.baseDone != nil {
		// The fetch runs on its own goroutine and is not covered by v.done, which waits
		// for the serve loop. Publishing without waiting is how a race becomes data loss:
		// the view would be missing everything the base holds, and the CAS would install
		// that over the manifest the base came from.
		<-v.baseDone
	}
	if v.baseFailed {
		slog.Warn("not publishing this volume's image: its read view never resolved, so the image would be missing everything it held before this session",
			"volume_id", v.id)
		return
	}
	view, seq := v.log.ViewAtRest()
	// Not the serve context: that one is already cancelled by the time this runs, and a
	// cancelled publish is exactly the silent data loss this function exists to prevent.
	etag, err := image.Publish(context.Background(), v.store, v.rnd, v.enc, v.vol, view, seq, v.imageETag)
	switch {
	case errors.Is(err, image.ErrSuperseded):
		slog.Error("this volume's image was published by another writer; this session's writes were NOT saved",
			"volume_id", v.id, "sequence", seq)
	case err != nil:
		slog.Error("this volume's image could not be published; this session's writes were NOT saved",
			"volume_id", v.id, "sequence", seq, "error", err)
	default:
		v.imageETag = etag
		slog.Info("volume image published", "volume_id", v.id, "sequence", seq)
	}
}

// ListenFunc opens the vhost-user socket for one volume. It is injected because a Unix
// socket is a kernel object (INV-01): production passes hostio.Listen, and a test passes
// something it can close.
type ListenFunc func(socket string) (vhost.Listener, error)

// KeysFunc fetches one volume's wrapped key material. Production passes
// Loop.VolumeKeys, which asks the Control Plane once and caches the answer; it is a
// function rather than the Loop because ADR-0021 keeps this type from knowing what a
// Control Plane is, and
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

// VolumeManagerConfig is where this host keeps things.
type VolumeManagerConfig struct {
	// DataDir holds the WALs: <data-dir>/wal/<volume-id>/<epoch>.
	//
	// It is a path *inside the injected Disk's namespace*, not a host path, and the
	// difference has bitten once already: production roots its real.Disk at the
	// operator's --data-dir, so passing the same absolute path here produced
	// <data-dir>/<data-dir>/wal/... — every byte the Agent wrote was one level below
	// where its operator was told to look. A rooted Disk wants "." here; a Disk
	// spanning a whole filesystem (every test, and the DST harness) wants the path.
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
	// KMS unwraps a volume's DEK, and Keys is where the wrapped one comes from. Both
	// or neither: an Agent with no KMS runs unencrypted, which is the dev/local mode
	// (§6.2) the DST harness and the QEMU lane use and which claims nothing it does
	// not do. An Agent *with* a KMS encrypts every volume it serves or serves none of
	// them — see start. §15.1 puts the unwrap at attach and nowhere else: one KMS call
	// outside the data path, and the DEK lives in memory only.
	KMS  crypto.KMS
	Keys KeysFunc
	// Rand is where the image's chunk nonces come from (§15, image.Publish). It is
	// injected rather than reached for because INV-01 keeps randomness out of the data
	// path's dependencies and because DST needs the same seed to produce the same
	// ciphertext (INV-02). Production passes crypto/rand.Reader; nil means the volume
	// cannot publish an encrypted image, which is refused at construction rather than
	// discovered at the first stop.
	Rand io.Reader
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
	// lock is this host's claim on DataDir (DEV-0014). Held for the manager's life
	// and released by Close.
	lock io.Closer
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
	case deps.Store != nil && deps.KMS != nil && deps.Rand == nil:
		// An Agent that can encrypt and cannot draw a nonce would seal every image chunk
		// with whatever a nil reader gives — which is nothing, so Publish would fail at
		// the first stop, in the teardown path, where the data is already unreachable.
		// Refused here instead (§15, image.Publish).
		return nil, errors.New("agent: a random source must be injected when a KMS is (§15: image chunk nonces)")
	case deps.KMS != nil && deps.Keys == nil:
		// A KMS with nowhere to get wrapped keys from would unwrap nothing and every
		// volume would fail to start — at attach, one at a time, looking like a
		// Control Plane problem. It is a wiring problem, and it is visible here.
		return nil, errors.New("agent: a KMS needs a source of wrapped volume keys (§15.1)")
	}
	// §10 opens with "un proceso por host", and until now nothing enforced it. Two
	// Agents against one data directory both resume the same segment files and both
	// append to them, and `hostio.Listen` unlinks a stale socket before binding — so
	// the second silently steals the guest from the first rather than failing to bind.
	// The lock is taken here rather than in `main` because this type owns DataDir, and
	// because a step left to `main` is a step spin's runner will not inherit (ADR-0021)
	// — which is exactly how HostID went missing until an e2e lane read the log line
	// about it.
	lock, err := deps.Disk.Lock(path.Join(cfg.DataDir, lockFile))
	if err != nil {
		if errors.Is(err, disk.ErrLocked) {
			return nil, fmt.Errorf("agent: another Volume Agent is already using %s (§10: one Agent per host): %w",
				cfg.DataDir, err)
		}
		return nil, fmt.Errorf("agent: claiming %s: %w", cfg.DataDir, err)
	}
	return &VolumeManager{
		cfg: cfg, deps: deps, lock: lock,
		volumes:     map[string]*Volume{},
		fencedEpoch: map[string]int64{},
	}, nil
}

// lockFile is what this Agent claims inside its data directory. Its *contents* are
// never read: a pid in it would be a liveness check the kernel already performs, with
// the classic race (read pid, process dies, pid is reused) this deliberately avoids.
const lockFile = "agent.lock"

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
	// Every volume with an object store behind it needs its read view built from that
	// store. There is no case where the local segments are the whole truth:
	//
	//   - *resuming* — its own objects hold what truncation reclaimed;
	//   - a *clone* — its parent's objects hold everything it has not written itself;
	//   - and the one that cost the most to find: a volume **promoted to this host**
	//     (§12.3, a drain or a failover) has no local segments and no parent, and
	//     everything it owns was written by a previous epoch on another machine.
	//     Keying this on "resuming, or a clone" made a promoted destination serve
	//     **zeros for its predecessor's whole volume** — no error, no complaint, and
	//     INV-09's guarantee intact in the object store the Agent never asked.
	//
	// A genuinely new volume recovers an empty view, which is the right answer for it,
	// and costs one LIST. That is the price of not having to decide which of the four
	// cases this is from the outside.
	resuming := len(existing) > 0
	needsBase := m.deps.Store != nil
	if needsBase {
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

	// Nothing enables a remote path any more (ADR-0026 increment 4.5). A FLUSH is
	// fdatasync and an ACK; the volume reaches the object store when it stops, as one
	// image, and that is the whole of what leaves the host.

	dev, err := blockdev.New(log, d.GetSizeBytes())
	if err != nil {
		_ = log.Close()
		return nil, fmt.Errorf("agent: volume %s: %w", id, err)
	}

	socket := path.Join(m.cfg.SocketDir, id+".sock")
	v := &Volume{
		id: id, epoch: d.GetEpoch(), root: root, socket: socket,
		log: log, dev: dev, enc: enc,
		vol: [16]byte(u), store: m.deps.Store, rnd: m.deps.Rand,
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
	if needsBase {
		v.baseDone = make(chan struct{})
		go func() {
			defer close(v.baseDone)
			m.fetchBase(serveCtx, v, [16]byte(u), uint64(d.GetEpoch()), d)
		}()
	}
	// One line per volume this host starts serving. Everything else the manager logs
	// is an exception, so an Agent that came up correctly said nothing at all about
	// the volumes it opened — which is the state an operator most needs confirmed, and
	// the only evidence available to anything watching from outside the process.
	slog.Info("serving volume",
		"volume_id", id, "epoch", d.GetEpoch(), "socket", socket,
		"resumed", resuming, "encrypted", enc != nil)

	return v, nil
}

// fetchBase rebuilds the read view from the object store and hands it to the log. This
// is the lazy half of BUILD-INVENTORY increment 5: the volume is already being served,
// and only its *reads* are waiting on this.
//
// It owes the log exactly one InstallBase or FailBase on every path, which is why there
// is no early return that skips both. A log that gets neither parks every read for the
// life of the process.
func (m *VolumeManager) fetchBase(ctx context.Context, v *Volume, volumeID [16]byte, epoch uint64, d *storagev1.DesiredVolume) {
	if m.deps.Store == nil {
		// Local-only mode has no object store to recover from, so the local segments
		// are all there is and they have already been replayed. Nothing to wait for.
		v.baseFailed = true
		v.log.FailBase(errors.New("this Agent has no object store: the read view is whatever the local WAL holds"))
		return
	}

	// A clone reads *through* its parent (§20): its own objects are only what it has
	// written since, and everything else lives under the parent's volume id. The
	// parent's view goes underneath as the base layer, which is exactly what
	// increment 5 built cow.IntervalMap's base for — the clone's own extents shadow
	// it, and a DISCARD in the clone reads as zeros rather than falling through.
	parent, err := m.parentView(ctx, v, d)
	if err != nil {
		// Fail closed, the same rule as a base that cannot be recovered and for the
		// same reason: an empty view where data belongs is a wrong answer a guest
		// cannot detect.
		slog.Error("the clone's parent snapshot could not be materialized; its reads will fail",
			"volume_id", v.id, "parent_snapshot_id", d.GetParentSnapshotId(), "error", err)
		v.baseFailed = true
		v.log.FailBase(err)
		return
	}

	// The volume's own state is one image, published when it last stopped (ADR-0026).
	// This replaced replaying a chain of WAL objects: there is no chain, and no
	// contiguous prefix to establish — the manifest resolves or it does not.
	//
	// v.enc, not nil: the chunks are sealed under this volume's DEK, and loading them
	// without it would fold ciphertext into the read view (DEV-0019).
	base, man, etag, err := image.Load(ctx, m.deps.Store, v.enc, volumeID)
	switch {
	case errors.Is(err, image.ErrNotPublished):
		// A volume that has never stopped cleanly has no image, which is the first boot
		// and must work. Its base is whatever its parent gives it, or nothing.
		base, man = parent, image.Manifest{}
		if base == nil {
			base = cow.NewIntervalMap()
		}
	case err != nil:
		// Refuse loudly rather than serve zeros. An empty view where data belongs is
		// indistinguishable from a fresh volume, and a guest cannot tell them apart.
		slog.Error("the volume's image could not be loaded; its reads will fail",
			"volume_id", v.id, "epoch", v.epoch, "error", err)
		v.baseFailed = true
		v.log.FailBase(err)
		return
	default:
		if parent != nil { // a clone reads through its parent, underneath its own image (§20)
			if err := base.SetBase(parent); err != nil {
				slog.Error("the clone's parent could not be layered under its image; its reads will fail",
					"volume_id", v.id, "error", err)
				v.baseFailed = true
				v.log.FailBase(err)
				return
			}
		}
	}
	// The ETag this volume CASes against when it publishes in turn. Carrying it is what
	// makes the fence work: a host that never loaded the manifest publishes with an empty
	// ETag, which is create-only, and loses to the one that did.
	v.imageETag = etag
	durable := man.Sequence
	if err := v.log.InstallBase(base, durable); err != nil {
		slog.Error("the recovered read view could not be installed",
			"volume_id", v.id, "epoch", v.epoch, "error", err)
		v.baseFailed = true
		v.log.FailBase(err)
		return
	}
	slog.Info("read view recovered from the object store",
		"volume_id", v.id, "epoch", v.epoch, "durable_sequence", durable,
		"cloned_from", d.GetParentSnapshotId())
}

// parentView loads the snapshot this volume was cloned from, or returns nil for a volume
// that was created rather than cloned (§20).
//
// It used to materialize that snapshot from a checkpoint plus the WAL objects after it.
// Under ADR-0026 a snapshot is one manifest naming chunks the parent already wrote, and
// reading it is the same operation a boot performs — no replay, and no data copied.
//
// The ids come from the desired state rather than a lookup: ADR-0021 keeps this type from
// knowing what a Control Plane is, so the Control Plane is what tells it.
func (m *VolumeManager) parentView(ctx context.Context, v *Volume, d *storagev1.DesiredVolume) (*cow.IntervalMap, error) {
	snapID, parentVol := d.GetParentSnapshotId(), d.GetParentVolumeId()
	if snapID == "" {
		return nil, nil //nolint:nilnil // no parent is a shape, not a failure
	}
	if parentVol == "" {
		// Half a link is worse than none: it would load nothing and install a base that
		// silently reads as zeros for the parent's whole extent.
		return nil, fmt.Errorf("agent: volume %s names parent snapshot %s with no parent volume", v.id, snapID)
	}
	u, err := ids.Parse(parentVol)
	if err != nil {
		return nil, fmt.Errorf("agent: volume %s names parent volume %q, which is not a uuid: %w", v.id, parentVol, err)
	}
	// The parent's chunks are sealed under the parent's id, and a clone inherits the
	// parent's DEK and its version (controlplane.Clone) precisely so the chain stays
	// readable — but the AAD binds the volume id, so the key has to be re-bound. Handing
	// this volume's own Encryption would fail to open every chunk the parent wrote.
	penc, err := m.parentEncryption(v, parentVol)
	if err != nil {
		return nil, err
	}
	// The *snapshot*, not the parent's live image. A parent that is still running has
	// written past the point this clone descends from, and its image carries those
	// writes — so loading it would hand the clone a state its snapshot never described.
	// §19 is what makes the distinction cheap: the snapshot is a frozen view at a
	// sequence, sharing the parent's chunks.
	view, _, err := image.LoadSnapshot(ctx, m.deps.Store, penc, [16]byte(u), snapID)
	if errors.Is(err, image.ErrNotPublished) {
		// Not this volume's failure to hide: a clone whose parent snapshot was never
		// published would read zeros for everything the parent wrote — DEV-0007's shape.
		return nil, fmt.Errorf("agent: volume %s clones snapshot %s of %s, which was never published",
			v.id, snapID, parentVol)
	}
	if err != nil {
		return nil, fmt.Errorf("agent: loading snapshot %s of parent %s for volume %s: %w", snapID, parentVol, v.id, err)
	}
	return view, nil
}

// parentEncryption re-binds this volume's DEK to its parent's id, which is what opens the
// chunks the parent wrote. Returns nil for an unencrypted volume.
func (m *VolumeManager) parentEncryption(v *Volume, parentVol string) (*wal.Encryption, error) {
	if v.enc == nil {
		return nil, nil //nolint:nilnil // no encryption is a mode, not a failure — see encryptionFor
	}
	u, err := ids.Parse(parentVol)
	if err != nil {
		return nil, fmt.Errorf("agent: volume %s names parent volume %q, which is not a uuid: %w", v.id, parentVol, err)
	}
	penc, err := wal.NewEncryption(v.enc.DEK, [16]byte(u))
	if err != nil {
		return nil, fmt.Errorf("agent: volume %s: binding the DEK to parent %s: %w", v.id, parentVol, err)
	}
	return penc, nil
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
// changed, or the volume is unknown to the fleet (§12.3, §16). This is the trigger that
// stops a guest's I/O — the Control Plane's view, not this host's lease clock; the log's
// own self-fencing stops only the durable path, deliberately (see wal.Log's `fenced`).
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
	// Released last, after every runtime is down: while any of them is still writing,
	// this Agent still owns the directory.
	if m.lock != nil {
		if err := m.lock.Close(); err != nil {
			errs = append(errs, fmt.Errorf("agent: releasing %s: %w", m.cfg.DataDir, err))
		}
	}
	return errors.Join(errs...)
}

var _ VolumeSource = (*VolumeManager)(nil)

// Snapshot freezes a running volume under a name and publishes the frozen copy.
//
// It is §19 end to end and it does not stop the guest: Freeze captures the sequence and
// swaps the read view under the volume's lock, the guest carries on writing into a fresh
// layer, and the upload happens afterwards against a map nothing can mutate. §2's "pausa
// de I/O por snapshot ~0" is that swap.
//
// The snapshot does *not* become the volume's image. They are different things with
// different lives: the image is where this volume resumes, the snapshot is a named point
// others descend from. Coupling them would make taking a snapshot change what a restart
// reads, which is not something anybody asked for.
func (m *VolumeManager) Snapshot(ctx context.Context, volumeID, snapshotID string) error {
	m.mu.Lock()
	v, ok := m.volumes[volumeID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("agent: volume %s is not being served here", volumeID)
	}
	if m.deps.Store == nil {
		return fmt.Errorf("agent: volume %s has no object store to snapshot into", volumeID)
	}
	if v.log.BasePending() {
		// Snapshotting before the base lands would freeze a view missing everything the
		// volume held before this session — the same hazard Volume.publish waits for,
		// and here it would be written down under a name others clone from.
		return fmt.Errorf("agent: volume %s is still loading its read view", volumeID)
	}

	frozen, seq, err := v.log.Freeze()
	if err != nil {
		return fmt.Errorf("agent: volume %s: freezing at a sequence: %w", volumeID, err)
	}
	if _, err := image.PublishSnapshot(ctx, m.deps.Store, v.rnd, v.enc, v.vol, frozen, seq, snapshotID); err != nil {
		return fmt.Errorf("agent: volume %s: publishing snapshot %s: %w", volumeID, snapshotID, err)
	}
	slog.Info("snapshot published", "volume_id", volumeID, "snapshot_id", snapshotID, "sequence", seq)
	return nil
}
