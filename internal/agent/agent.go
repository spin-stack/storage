// Package agent is the Volume Agent's control loop: the process-side half of
// ADR-0018. The Agent *pulls* — it heartbeats, asks what it should be serving, and
// reports what it observed — and never accepts a command from the Control Plane.
// A partitioned Agent simply stops learning, its lease lapses on its own monotonic
// clock (§12.2), and §12 fencing does the rest.
//
// Everything the loop touches is injected (INV-01): the clock, the RPC client, the
// device, and the set of volumes this host is serving. cmd/volume-agent is the only
// place the real implementations are constructed. There is no data path here yet;
// the vhost-user increment supplies the VolumeSource that a WAL actually backs.
package agent

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/spin-stack/storage/internal/simio/disk"
)

// DeviceUsage is the byte accounting of the local NVMe device (ADR-0013). Total is
// the device's capacity; Used is what this Agent occupies on it.
type DeviceUsage struct {
	TotalBytes int64
	UsedBytes  int64
}

// Device reports the local device's usage. It is an interface because the honest
// implementation is a statfs, which lives behind internal/simio — see DiskUsage for
// what this increment can measure without one.
type Device interface {
	Usage(ctx context.Context) (DeviceUsage, error)
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
	// RemoteGapBytes is this volume's contribution to the device's remote backlog:
	// bytes no verified object covers yet, which no local truncation can reclaim
	// (INV-13).
	RemoteGapBytes int64
}

// VolumeSource is the set of volumes this host is serving right now. The data path
// will implement it over the live WAL; until then VolumeSet stands in.
type VolumeSource interface {
	Volumes(ctx context.Context) ([]VolumeStatus, error)
}

// VolumeSet is an in-memory VolumeSource. It is what the Agent runs against until
// there is a data path to ask, and it is what tests drive.
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

// DiskUsage measures the Agent's own footprint by summing the files under its data
// prefix, and takes the device's capacity from configuration.
//
// It is deliberately not a statfs. internal/simio/disk has no notion of a device's
// free space, and adding one is a change to an interface this increment does not
// own (see the increment's report): what a statfs would add is the space *other*
// tenants of the filesystem occupy. Until then the number reported is the one the
// Agent is responsible for and the one ADR-0013's thresholds act on.
type DiskUsage struct {
	disk       disk.Disk
	prefix     string
	totalBytes int64
}

// NewDiskUsage returns a Device that sums the files under prefix on d and reports
// totalBytes as the device's capacity.
func NewDiskUsage(d disk.Disk, prefix string, totalBytes int64) *DiskUsage {
	return &DiskUsage{disk: d, prefix: prefix, totalBytes: totalBytes}
}

// Usage sums the sizes of every file under the prefix.
func (u *DiskUsage) Usage(context.Context) (DeviceUsage, error) {
	names, err := u.disk.List(u.prefix)
	if err != nil {
		return DeviceUsage{}, fmt.Errorf("agent: listing %q: %w", u.prefix, err)
	}
	var used int64
	for _, name := range names {
		size, err := u.fileSize(name)
		if err != nil {
			// A file that vanished between the listing and the open is not an error:
			// a truncation or a GC ran, and the next cycle will see the new picture.
			if errors.Is(err, disk.ErrNotExist) {
				continue
			}
			return DeviceUsage{}, err
		}
		used += size
	}
	return DeviceUsage{TotalBytes: u.totalBytes, UsedBytes: used}, nil
}

func (u *DiskUsage) fileSize(name string) (int64, error) {
	f, err := u.disk.Open(name)
	if err != nil {
		return 0, fmt.Errorf("agent: opening %q: %w", name, err)
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		return 0, fmt.Errorf("agent: sizing %q: %w", name, err)
	}
	return size, nil
}

// Config is the Agent's configuration. Every field is required: an Agent that
// cannot name its host, its version, or its device is one whose reports the Control
// Plane would act on anyway.
type Config struct {
	// HostID is this host's fleet identity.
	HostID string
	// AgentVersion is the build being run (§13.4, fleet-mixed).
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
	// DeviceTotalBytes is the capacity of the NVMe device this Agent owns.
	DeviceTotalBytes int64
}

// Validate reports what is missing or contradictory.
func (c Config) Validate() error {
	switch {
	case c.HostID == "":
		return errors.New("agent: host id is required")
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
	case c.DeviceTotalBytes <= 0:
		return errors.New("agent: device total bytes must be positive")
	}
	return nil
}
