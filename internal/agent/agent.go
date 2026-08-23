// Package agent is the Volume Agent's control loop: the process-side half of
// ADR-0018. The Agent *pulls* — it heartbeats, asks what it should be serving, and
// reports what it observed — and never accepts a command from the Control Plane.
// A partitioned Agent simply stops learning, its lease lapses on its own monotonic
// clock (§12.2), and §12 fencing does the rest.
//
// Everything the loop touches is injected (INV-01): the clock, the RPC client, the
// device, and the set of volumes this host is serving. cmd/volume-agent is the only
// place the real implementations are constructed.
//
// **There is no volume manager behind the VolumeSource right now.** The one that was
// here served each volume from a local write-ahead log through a vhost-user-blk socket,
// and it went with that engine: QEMU owns the local copy-on-write format from here on,
// and this system's job narrows to immutable commits, publication and recovery. What is
// left is exactly the half that talks to the Control Plane — heartbeat, lease, desired
// state, the report — and it is deliberately unchanged, because it is the half the new
// design keeps. VolumeReconciler is the seam the qcow2 manager plugs into, and until it
// exists the loop learns what this host should be serving and can act on none of it,
// which is what VolumeSet stands in for and what the Agent says on the way up.
package agent

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/disk"
)

// Device reports the local NVMe device's usage (ADR-0013). It is an interface, and
// it carries a context the disk's own statfs does not need, because the device an
// Agent owns will not always be a filesystem under its feet: a future one is a block
// device queried over a socket, and a caller that cannot cancel that is a caller
// whose heartbeat can hang.
//
// It reports disk.Usage unchanged rather than a shape of its own. The Agent used to
// have one, and the translation was where the honest numbers were lost.
type Device interface {
	Usage(ctx context.Context) (disk.Usage, error)
}

// VolumeStatus is what the Agent knows about one volume it is serving. The epoch is
// the load-bearing field: every watermark reported to the Control Plane is qualified
// by the epoch it was produced under, so a fenced writer's report can be refused
// rather than applied (§12.3).
type VolumeStatus struct {
	VolumeID          string
	Epoch             int64
	LocalSequence     int64
	DurableSequence   int64
	PublishedSequence int64
	// SnapshotID is the snapshot this host was asked to take and has finished acting
	// on, empty while there is nothing to say — including while an upload is still in
	// flight, because a half-taken snapshot is not a fact the catalog can hold. The
	// two fields below are its outcome: a sequence, or an error.
	SnapshotID string
	// SnapshotSequence is the §19 sequence the copy was frozen at.
	SnapshotSequence int64
	// SnapshotError is why it could not be taken. A snapshot that fails silently stays
	// CREATING in the catalog forever.
	SnapshotError string

	// Refusal says this host is **not serving** this volume, and RefusalDetail is the
	// sentence behind it. Unset is this host saying it is serving.
	//
	// It is the one field here that is not a measurement. Everything above is a number
	// the volume's own storage holds; this is a decision the Agent made — the image the catalog promised
	// is not in the bucket, the volume came back under the durability floor, the read
	// view never resolved, there is no key for it, the lease lapsed. Every one of those
	// was already correct and every one was invisible: a volume that fails closed keeps
	// reporting the watermarks its last healthy session left behind, and one that never
	// started reported nothing at all — it stopped appearing on the wire, and an absence
	// is not a signal.
	//
	// It is the proto's enum rather than a vocabulary of the Agent's own, for the reason
	// VolumeReconciler.Apply takes proto types: this is the wire's word, the Agent is
	// the only thing that produces it, and a third spelling between here and the message
	// would be a mapping that can be wrong in one direction only.
	Refusal storagev1.VolumeRefusal
	// RefusalDetail is free text an operator reads and nothing branches on: which
	// sequence, which KEK. Empty when there is no refusal.
	RefusalDetail string
}

// VolumeKeys is what a host needs to seal and open one volume's payloads (§15.1).
// The DEK arrives wrapped and stays wrapped here: unwrapping is the KMS's, with a
// KEK this Agent already holds and the Control Plane never sees.
type VolumeKeys struct {
	VolumeID string
	// DEKWrapped is the volume's data-encryption key sealed under the KEK.
	DEKWrapped []byte
	// KEKID names the key that wraps it, for a host holding more than one.
	KEKID string
	// DEKKeyID is the DEK's version (§15.1). It is not decoration: crypto.DevKMS binds
	// it as GCM additional authenticated
	// data, so unwrapping with the wrong version fails outright rather than yielding
	// a key that decrypts nothing. Never 0 — see VolumeKeys on the Loop.
	DEKKeyID uint32
}

// VolumeSource is the set of volumes this host is serving right now. VolumeSet is the
// only implementation today, and it is a stand-in: see the package doc.
type VolumeSource interface {
	Volumes(ctx context.Context) ([]VolumeStatus, error)
}

// VolumeReconciler is a VolumeSource that can also be told what this host *should* be
// serving. The Loop uses it when its VolumeSource happens to be one; a plain source
// leaves the desired state recorded and unacted-on, which is what a test driving
// VolumeSet wants — and, until the qcow2 volume manager lands, what every real Agent
// does. Nothing implements it in this tree; it is the seam, kept because the loop on
// this side of it is not being rewritten.
//
// It is deliberately the same object as the source. What is reported and what is served
// must come from one place: two would drift, and the report is what the Control Plane
// makes fencing decisions from.
type VolumeReconciler interface {
	VolumeSource
	Apply(ctx context.Context, desired []*storagev1.DesiredVolume) error
	// Fence stops serving the given volumes. This host is not their writer any more,
	// and the data path is where that has to take effect (§16) — recording it was all
	// the loop could ever do on its own.
	//
	// why and detail are what the *next* report says about them, and
	// VOLUME_REFUSAL_UNSPECIFIED means "say nothing". The two callers want opposite
	// things and the difference is whether the Control Plane already knows: a fence
	// that follows a refused report is the Control Plane's own decision coming back,
	// and reporting it would be telling it what it just told us — and the report would
	// be refused again anyway, on the same host-and-epoch predicate. A lease that
	// lapsed is the other case entirely: nothing outside this process knows the device
	// is gone, the volume stays this host's at this epoch until somebody moves it, and
	// silence there is a guest with no disk and a fleet that reads healthy.
	Fence(ctx context.Context, volumeIDs []string, why storagev1.VolumeRefusal, detail string) error
}

// VolumeSet is an in-memory VolumeSource. It is what the Agent runs against until there
// is a data path to ask — which is every Agent today — and it is what tests drive.
type VolumeSet struct {
	mu   sync.Mutex
	vols map[string]VolumeStatus
}

// NewVolumeSet returns an empty set.
func NewVolumeSet() *VolumeSet { return &VolumeSet{vols: map[string]VolumeStatus{}} }

// Set records (or replaces) one volume's status.
func (s *VolumeSet) Set(v VolumeStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vols[v.VolumeID] = v
}

// Remove drops a volume from the set.
func (s *VolumeSet) Remove(volumeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.vols, volumeID)
}

// Volumes returns the set ordered by volume id (deterministic, INV-02). The slice
// is the caller's own copy.
func (s *VolumeSet) Volumes(context.Context) ([]VolumeStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]VolumeStatus, 0, len(s.vols))
	for _, v := range s.vols {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].VolumeID < out[j].VolumeID })
	return out, nil
}

// DiskUsage is the Device backed by this Agent's data disk: it asks the device itself
// (a statfs in production), rather than estimating from the files it happens to know
// about.
//
// The difference is what the Agent cannot reclaim. A sum of our own files says
// nothing about the space another tenant of the same filesystem occupies, and nothing
// this Agent can do will ever free it — so a threshold evaluated on the sum fires
// after the device is already full, which is the one moment it needed to have fired
// earlier (ADR-0013 §3).
type DiskUsage struct {
	disk disk.Disk
}

// NewDiskUsage returns a Device reporting the device behind d.
func NewDiskUsage(d disk.Disk) *DiskUsage { return &DiskUsage{disk: d} }

// Usage reports the device. A failure is returned, never smoothed into a zero: an
// unreadable device that looks empty is the worst answer available here, because
// every ADR-0013 threshold would read it as headroom.
func (u *DiskUsage) Usage(context.Context) (disk.Usage, error) {
	usage, err := u.disk.Usage()
	if err != nil {
		return disk.Usage{}, fmt.Errorf("agent: measuring the data disk: %w", err)
	}
	return usage, nil
}

// Config is the Agent's configuration. Every field is required: an Agent that
// cannot name its host, its version, or its device is one whose reports the Control
// Plane would act on anyway.
type Config struct {
	// HostID is this host's fleet identity.
	HostID string
	// AgentVersion is the build being run (§27, fleet-mixed — INV-19).
	AgentVersion string
	// MaxFormatVersion is the highest on-disk/on-S3 format this build can read and
	// write. The Control Plane will not place a volume this Agent cannot read.
	MaxFormatVersion int32
	// HeartbeatInterval is the reconciliation cadence.
	HeartbeatInterval time.Duration
	// RetryBackoff is the delay after the first failed cycle. It doubles per
	// consecutive failure and is capped at HeartbeatInterval.
	RetryBackoff time.Duration
	// LeaseTTL is the TTL the Agent expects its host lease to be granted with. The
	// Control Plane's answer wins when it differs — a shorter one must shorten the
	// Agent's window (§12.2).
	LeaseTTL time.Duration
	// The device's capacity is deliberately absent. It used to be configured here,
	// and configuration is the wrong authority for it: the number ADR-0013 divides
	// by has to be the device's own answer, not the one an operator typed on a host
	// whose disk was later replaced. It comes from Device.Usage.
}

// Validate reports what is missing or contradictory.
func (c Config) Validate() error {
	switch {
	case c.HostID == "":
		return errors.New("agent: host id is required")
	case !isHostID(c.HostID):
		// hosts.host_id is the `uuidv7` domain (INV-22) and the pg adapter parses the
		// string before it reaches SQL, so anything else writes zero rows on every
		// heartbeat — forever, since Run retries. Refusing it here is the difference
		// between a startup error and a process that looks alive while the fleet never
		// learns the host exists. Use internal/ids to mint one.
		return fmt.Errorf("agent: host id %q is not a UUIDv7 (INV-22): every heartbeat would match no row", c.HostID)
	case c.AgentVersion == "":
		return errors.New("agent: agent version is required")
	case c.MaxFormatVersion <= 0:
		return errors.New("agent: max format version must be positive")
	case c.HeartbeatInterval <= 0:
		return errors.New("agent: heartbeat interval must be positive")
	case c.RetryBackoff <= 0:
		return errors.New("agent: retry backoff must be positive")
	case c.RetryBackoff > c.HeartbeatInterval:
		return errors.New("agent: retry backoff must not exceed the heartbeat interval")
	case c.LeaseTTL <= 0:
		return errors.New("agent: lease TTL must be positive")
	case c.LeaseTTL <= c.HeartbeatInterval:
		// A lease that expires within one heartbeat interval is a lease that lapses
		// during normal operation, which would fence a perfectly healthy host.
		return errors.New("agent: lease TTL must exceed the heartbeat interval")
	}
	return nil
}

// isHostID reports whether s is exactly a UUIDv7 in canonical form. It is deliberately
// stricter than ids.Parse, which accepts the braced and urn: spellings and would let a
// value through that the database's own comparison then misses.
func isHostID(s string) bool {
	if len(s) != 36 {
		return false
	}
	u, err := ids.Parse(s)
	return err == nil && ids.IsV7(u) && u.String() == s
}
