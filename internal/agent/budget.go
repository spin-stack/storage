package agent

import (
	"fmt"

	"github.com/spin-stack/storage/internal/cow"
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

// The same division applied to memory, for the read view (wal.Limits.MaxViewBytes).
//
// ViewRatio is the share of this Agent's memory that may sit in read views across every
// volume it serves. It is a quarter and not GuestRatio's 0.85, because a device is
// almost entirely what the guests put on it while memory is mostly *not*: the same
// process holds a WAL segment buffer per volume, the vhost-user mappings of every
// guest's rings, whatever a publish is streaming through image.Publish, and the Go
// runtime around all of it. And the failure modes are not symmetric — a device that
// fills says ENOSPC on the volume that filled it, while memory that runs out is the OOM
// killer taking the Agent and every other tenant's session with it. The asymmetry is
// paid for in the ratio.
//
// ViewRSSFactor is measured, not chosen: cow.ExtentOverheadBytes records process RSS at
// 2.07 and 2.04 times cow.Cost.Memory across an eight-fold change in extent density,
// because a guest writing distinct blocks churns the extent slice on every write and the
// heap sits at its GOGC goal of twice the live heap. The bound compares against
// Cost.Memory, so what a volume actually costs the host is twice its bound, and dividing
// by two here is what makes the sum of the bounds a statement about RSS.
//
// The two together reproduce the constant they replace on the machine that constant was
// chosen for: 32 GiB × 0.25 ÷ 16 volumes ÷ 2 = 256 MiB, which was wal.DefaultMaxViewBytes.
// That is the point — the judgement is unchanged, and it is now a division the machine
// participates in rather than a number that is only right on a 32 GiB host. On an 8 GiB
// host the old constant entitled 16 volumes to 8.5 GiB of read view; this gives them
// 64 MiB each.
const (
	ViewRatio     = 0.25
	ViewRSSFactor = 2
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
// Both shares — the device's and the read view's — are **static**, computed once from
// the measured machine and the configured fan-out, rather than divided among the volumes
// actually attached. The dynamic version
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
	// MemoryBytes is the memory this Agent may use before something kills it, as it
	// measured it: the machine's RAM, or the cgroup limit under it (real.MeasureMemory).
	// The cgroup is the case that matters — an Agent in a 2 GiB container on a 256 GiB
	// host that sized itself from the host would set a per-volume read-view bound 128x
	// too large, and in a container the OOM killer takes the Agent, not the guest that
	// caused it.
	MemoryBytes int64
	// MaxVolumes is the fan-out GuestBytes and MemoryBytes are divided by, and the
	// number of volumes this Agent will serve at once.
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
func NewBudget(u disk.Usage, memBytes int64, maxVolumes int) (Budget, error) {
	if u.TotalBytes <= 0 {
		return Budget{}, fmt.Errorf("agent: a device budget needs a measured device; statfs reported %d bytes", u.TotalBytes)
	}
	// A machine that could not be measured is refused for the same reason as a device
	// that could not: there is no honest default for how much memory a host has. A
	// constant here would be exactly the 256 MiB this derivation exists to remove.
	if memBytes <= 0 {
		return Budget{}, fmt.Errorf("agent: a budget needs a measured machine; memory was reported as %d bytes", memBytes)
	}
	if maxVolumes <= 0 {
		return Budget{}, fmt.Errorf("agent: a device budget needs a volume fan-out to divide by; got %d", maxVolumes)
	}
	b := Budget{
		DeviceBytes:  u.TotalBytes,
		ReserveBytes: int64(ReserveRatio * float64(u.TotalBytes)),
		MemoryBytes:  memBytes,
		MaxVolumes:   maxVolumes,
	}
	b.GuestBytes = int64(GuestRatio*float64(u.TotalBytes)) - b.ReserveBytes
	// The device this divides may already be somebody else's. Until this line the
	// derivation read TotalBytes and nothing else, so an Agent brought up on a
	// filesystem another tenant had filled to 95% handed each of its volumes the same
	// share it would hand them on an empty one, and kept a "reserve" that was a fraction
	// of a capacity it does not have. The guests met the difference as ENOSPC — a partial
	// append, after which every write on that volume fails — and the publish at stop met
	// it as an fdatasync that could not allocate, which is the whole session.
	//
	// **What is checked is the reserve, and that is the strongest sound statement
	// available here.** statfs reports one number for what is occupied and cannot say
	// whose bytes those are; at this exact moment the largest holder is usually *this
	// Agent*. A session's WAL stays on the device for the whole session and comes back at
	// the next attach, when the published image is installed as the log's base
	// (wal.InstallBase, the reclaimed_local_bytes on the attach line), so a restarting
	// Agent is looking at a device its own last session filled, minutes before that space
	// returns. What the bound above promises is a *footprint*, and this Agent's own
	// footprint is inside it by construction — so the one thing that must be true whoever
	// those bytes belong to is that the reserve fits behind them.
	//
	// Rejected: dividing what is free instead of what exists. It is the obvious shape and
	// it is wrong in the same way the dynamic share is (see Budget). MaxLocalBytes bounds
	// a log's whole footprint rather than its new writes, so a restart that divided the
	// free space would hand a resumed volume a share smaller than the log it is resuming:
	// every guest on the host takes an I/O error on a device that is 80% free the moment
	// the bases install. And it ratchets — each restart divides what the last one left —
	// on the dedicated filesystem that is the normal case.
	//
	// Rejected: refusing a device that is "materially occupied" by anything at all. That
	// is the same measurement read the other way, and its false positive is the whole
	// host: it fires on every restart of a busy Agent, and the volumes it refuses to serve
	// are the ones whose unpublished sessions are on that disk waiting for exactly this
	// process to publish them.
	//
	// **The residual, said out loud:** a genuinely shared filesystem between empty and
	// full is still divided as if the Agent owned it, so the shares can sum past what is
	// free and the guests still find out by ENOSPC. Closing that needs a number statfs
	// does not carry — how much of UsedBytes is this Agent's own — which only the caller
	// can produce, by measuring --data-dir before any volume attaches and passing it in.
	// It is left open rather than guessed: every guess here is either the ratchet above or
	// the refusal above, and both are worse than the gap.
	if u.AvailBytes < b.ReserveBytes {
		return Budget{}, fmt.Errorf(
			"agent: this device has %d bytes free of %d (%d occupied) and the budget it would hand %d volumes keeps %d bytes free for the image publish at stop: the reserve alone does not fit. Give this Agent a filesystem of its own, or free %d bytes on this one",
			u.AvailBytes, u.TotalBytes, u.UsedBytes, maxVolumes, b.ReserveBytes, b.ReserveBytes-u.AvailBytes)
	}
	if b.Share() <= 0 {
		return Budget{}, fmt.Errorf(
			"agent: a %d-byte device leaves %d bytes for %d volumes: no volume can be served on it",
			u.TotalBytes, b.GuestBytes, maxVolumes)
	}
	// Refused rather than floored, and the threshold is derived rather than picked.
	//
	// **Not floored**, because a floor is only safe where exceeding it is safe. Rounding
	// a share up would hand every volume a bound the machine cannot honour: the number
	// would stop being one volume's share of anything, the sum across the fan-out would
	// exceed the memory this process has, and the failure the floor was there to avoid —
	// the OOM killer, silent, in another process, to volumes that did nothing wrong — is
	// exactly the one it would then produce. The device side refuses for the same reason
	// (Share() above, and NewVolumeManager).
	//
	// **The threshold is one extent, and no larger**, which is the only line here that
	// is not a matter of taste: the bound is compared against cow.Cost.Memory, which
	// charges cow.ExtentOverheadBytes for the structure of every extent, so a share
	// under that admits no write at all — the first WRITE of the session crosses the
	// bound, and only DISCARD shrinks a view, so there is nothing to discard and the
	// volume never serves.
	//
	// Above it, a small share is served rather than refused, and that is the deliberate
	// half. A 2 GiB container serving sixteen volumes gives each a 16 MiB view; that is
	// a guest that meets backpressure early, which is an I/O error on the volume that
	// caused it and a line in the log naming the share. Refusing it instead would need a
	// threshold — "16 MiB is too small, 64 MiB is fine" — and that number would be
	// exactly the constant somebody chose that this whole derivation exists to remove.
	// The operator's knob is real and the error below names it: the share is memory ÷
	// fan-out, so a host that cannot serve sixteen volumes can serve two.
	if b.ViewShare() < cow.ExtentOverheadBytes {
		return Budget{}, fmt.Errorf(
			"agent: %d bytes of memory across %d volumes leaves each %d bytes of read view, less than the %d one extent costs: no volume can be served on it. Lower -max-volumes, or give this Agent more memory",
			memBytes, maxVolumes, b.ViewShare(), cow.ExtentOverheadBytes)
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

// ViewShare is one volume's slice of this Agent's memory: what its read view may cost
// (cow.Cost.Memory), which is half of what it costs the host in RSS.
//
// Zero when the machine was never measured — a Budget literal in a test — and a Log given
// a zero bound falls back to wal.DefaultMaxViewBytes, which is a bound and not
// "unbounded". Production never takes that path: NewBudget refuses a budget it cannot
// derive this from.
func (b Budget) ViewShare() int64 {
	if b.MaxVolumes <= 0 || b.MemoryBytes <= 0 {
		return 0
	}
	return int64(ViewRatio*float64(b.MemoryBytes)) / int64(b.MaxVolumes) / ViewRSSFactor
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
//
// MaxViewBytes is the other half of the same idea, and the reason ViewShare exists: the
// device bound is one volume's share of a device this host measured, and the read view's
// counterpart is one volume's share of the memory this host measured. It was a constant
// until this derivation replaced it — 256 MiB, on every machine, whatever the machine
// was — and 256 MiB × 16 volumes × the measured 2x RSS factor is 8.5 GiB of read view on
// a host that may have 8 GiB.
func (b Budget) Limits() wal.Limits {
	share := b.Share()
	seg := share / 8
	if seg > wal.SegmentBytes {
		seg = wal.SegmentBytes
	}
	return wal.Limits{MaxLocalBytes: share, SegmentBytes: seg, MaxViewBytes: b.ViewShare()}
}
