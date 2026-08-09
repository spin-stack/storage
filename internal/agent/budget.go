package agent

import (
	"fmt"

	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/wal"
)

// The ADR-0013 §1–§2 division of a device, as fractions of what statfs reported.
//
// GuestRatio is the share of the device this Agent's guests may fill between them. It
// is placement.DefaultMaxUsedRatio — the fill ceiling above which the fleet stops
// giving this host new volumes — deliberately spelled as the same number: the Agent's
// local bound and the Control Plane's admission ceiling describe one device, and two
// numbers that mean "full" drift apart the first time one of them is tuned. It is not
// imported from internal/placement because that package is the Control Plane's view of
// a fleet and this one is a host looking at its own disk; the coupling is the number,
// not the package.
//
// ReserveRatio is §2's headroom, and this is the amendment's re-aim: its consumers
// were the checkpoint, the recovery point and the summary object, all deleted by
// ADR-0026, and what has to stay possible on a full device now is the **image publish
// at stop** — a volume that cannot publish loses its whole session.
//
// The publish itself writes **no local byte**: image.Publish reads the volume's view
// and PUTs chunks and a manifest, and that is all it does. So the reserve is not a pool
// anything withdraws from, and this Agent has no machinery to let one writer spend what
// another may not — a privileged-writer path would be exactly the "reserve nobody can
// spend" the amendment warns against. What the reserve is instead is subtraction: the
// guest write path is bounded strictly below the device, so a device that is full *for
// guests* still has free blocks. That is what the publish actually needs, because the
// one local operation the stop depends on is the fdatasync of segments already
// appended, and on a filesystem with delayed allocation that is where ENOSPC is
// reported (ADR-0013 gap 6, recorded in wal/degraded.go — the simulated disk charges
// allocation at append time, so this is the one part of the reserve DST cannot model).
// It also leaves room for the filesystem's own metadata, which no accounting of ours
// covers and which a device at exactly 100% denies.
//
// The ADR's "floor 1 GiB" is dropped, and that is a deliberate departure. The floor was
// sized for objects the ADR expected to write locally; nothing writes them any more, so
// what the reserve must cover scales with the WAL that has to be synced, not with a
// constant. Keeping it would also make every device under 20 GiB have a budget of zero
// — a rule that refuses to start on the small devices a test rig and a dev host use,
// in exchange for headroom nothing has a use for.
const (
	GuestRatio   = 0.85
	ReserveRatio = 0.05
)

// DefaultMaxVolumes is the fan-out a host's device is divided by when nobody says
// otherwise. It is what -max-volumes defaults to.
//
// 16 is a choice about failure, not about capacity: it is the number of volumes whose
// backlogs a device is sized to hold *simultaneously and at their worst*, which under
// ADR-0026 means "for a whole session each". On the 1 TiB NVMe this runs on, that is a
// ~54 GiB share per volume, which is more WAL than a session of a normal guest writes;
// on a host that runs fewer, larger volumes the operator lowers it and every volume's
// share grows.
const DefaultMaxVolumes = 16

// Budget is one device divided into what this host's guests may fill, what they may
// not, and the share each volume gets (ADR-0013 §1).
//
// It exists because a per-volume limit is not a bound on a device: N volumes each
// inside their own limit exhaust the device between them and nothing sums them. Under
// ADR-0026 that is not a corner case — a session's whole WAL stays local until the
// volume stops, with no mid-session reclaim at all — so the device holds everything
// every attached volume has written, and the only thing standing between a busy host
// and ENOSPC is this division.
//
// The share is **static**, computed once from the measured device and the configured
// fan-out, rather than divided among the volumes actually attached. The dynamic version
// is the obvious one and it does not work: a share that shrinks when a volume attaches
// would put logs that are already over their new share into backpressure retroactively
// — a guest that was writing happily starts failing WRITEs because a *different* volume
// arrived on the host — and a share that grows when one detaches would have to be
// pushed into every running log, which is a second authority over a number wal.Limits
// documents as fixed for a log's life. Static and slightly wasteful beats dynamic and
// retroactive: the volumes that never arrive cost this host unused headroom, which is
// the failure mode an operator can see and lower -max-volumes for.
//
// Rejected too: deriving the share from the volume's declared size. The volume quota is
// **soft** on purpose, and the backlog is driven by how long the session
// runs, not by how big the volume was provisioned — a 10 GiB volume writing its blocks
// over and over holds more WAL than a 1 TiB volume that was touched once.
type Budget struct {
	// DeviceBytes is the device as it measured itself (statfs), including whatever
	// other tenants of that filesystem occupy. See DiskUsage for why the measurement
	// and not a configured capacity.
	DeviceBytes int64
	// ReserveBytes is the headroom the guest write path may never allocate.
	ReserveBytes int64
	// GuestBytes is what every volume on this host may hold locally, together.
	GuestBytes int64
	// MaxVolumes is the fan-out GuestBytes is divided by, and the number of volumes
	// this Agent will serve at once.
	MaxVolumes int
}

// NewBudget divides a measured device. It is the production derivation, and the only
// one: a test that wants a particular share builds a Budget literal, and `main` calls
// this with what DiskUsage reported.
//
// A device that reports nothing is an error rather than an unbounded Agent. That is
// the whole point of the type — an Agent with no budget has no write-path bound at
// all, which is what every Agent this repository has ever run had, because nothing set
// wal.Limits.
func NewBudget(u disk.Usage, maxVolumes int) (Budget, error) {
	if u.TotalBytes <= 0 {
		return Budget{}, fmt.Errorf("agent: a device budget needs a measured device; statfs reported %d bytes", u.TotalBytes)
	}
	if maxVolumes <= 0 {
		return Budget{}, fmt.Errorf("agent: a device budget needs a volume fan-out to divide by; got %d", maxVolumes)
	}
	b := Budget{
		DeviceBytes:  u.TotalBytes,
		ReserveBytes: int64(ReserveRatio * float64(u.TotalBytes)),
		MaxVolumes:   maxVolumes,
	}
	b.GuestBytes = int64(GuestRatio*float64(u.TotalBytes)) - b.ReserveBytes
	if b.Share() <= 0 {
		return Budget{}, fmt.Errorf(
			"agent: a %d-byte device leaves %d bytes for %d volumes: no volume can be served on it",
			u.TotalBytes, b.GuestBytes, maxVolumes)
	}
	return b, nil
}

// Share is one volume's slice of the device: what its WAL may hold locally.
func (b Budget) Share() int64 {
	if b.MaxVolumes <= 0 {
		return 0
	}
	return b.GuestBytes / int64(b.MaxVolumes)
}

// Limits is the bound handed to one volume's Log. It is where wal.Limits comes from in
// production — before this, `grep -rn Limits cmd/` returned nothing and no real Agent
// had a write-path bound of any kind, while eight unit tests proved backpressure
// against limits they set themselves.
//
// MaxUnflushedBytes is deliberately not set. It bounds what fdatasync has not seen, and
// Sync clears it on every guest FLUSH — so on any workload that fsyncs it reads zero
// while the segments grow, which makes it a bound on a burst and no bound at all on a
// session. What measures a volume's footprint on the device is the retained segments,
// which is MaxLocalBytes (and what Log.LocalBytes reports).
//
// SegmentBytes is derived from the share because the segment is the unit of
// reclamation: one segment is the least a truncation can give back, so a log whose
// segment is its whole share can never return anything while it is running. An eighth
// of the share is ADR-0013's ratio. The ADR's 8 MiB floor is not applied — it binds
// only when the share is under 64 MiB, i.e. on a device small enough that 8 MiB
// segments would be the entire share, which is the case it would break rather than
// protect. The 32 MiB cap is wal.SegmentBytes, the default this replaces: past it
// segments stop buying anything and each one is a bigger crash-tail to re-scan.
func (b Budget) Limits() wal.Limits {
	share := b.Share()
	seg := share / 8
	if seg > wal.SegmentBytes {
		seg = wal.SegmentBytes
	}
	return wal.Limits{MaxLocalBytes: share, SegmentBytes: seg}
}
