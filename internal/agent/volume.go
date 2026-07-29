package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"sort"
	"strconv"
	"sync"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/blockdev"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/disk"
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
// What it does *not* do yet is the remote half. wal.Log.EnableRemote needs a
// LeaseChecker, and passing one that nobody renews would give the volume a durability
// claim it cannot honour — so remote mode, and with it the uploader and the §14.4 ACK
// path, arrives with the fencing increment. Until then a volume serves reads and takes
// writes locally, and its durable watermark stays where a WRITE leaves it: 0.
type Volume struct {
	id    string
	epoch int64
	// root is <data-dir>/wal/<volume-id>/<epoch>. The epoch is in the path because a
	// promoted writer must never append into the segments of the epoch it replaced.
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

// VolumeManagerConfig is where this host keeps things.
type VolumeManagerConfig struct {
	// DataDir holds the WALs: <data-dir>/wal/<volume-id>/<epoch>.
	DataDir string
	// SocketDir holds one vhost-user socket per volume: <socket-dir>/<volume-id>.sock.
	SocketDir string
	// Limits bound the local WAL (§5.7). The zero value is legal and unbounded.
	Limits wal.Limits
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
}

// VolumeManager owns the live runtimes and is the Agent's VolumeSource. Apply is the
// whole of the reconciliation: it diffs the desired state against what is running.
type VolumeManager struct {
	cfg  VolumeManagerConfig
	deps VolumeManagerDeps

	mu      sync.Mutex
	volumes map[string]*Volume
	closed  bool
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
	}
	return &VolumeManager{cfg: cfg, deps: deps, volumes: map[string]*Volume{}}, nil
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
		m.mu.Unlock()

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

	root := path.Join(m.cfg.DataDir, "wal", id, strconv.FormatInt(d.GetEpoch(), 10))
	log := wal.NewLog(m.deps.Disk, root, m.deps.Clock, [16]byte(u), uint64(d.GetEpoch()), m.cfg.Limits)

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
	return v, nil
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
