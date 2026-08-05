// Package placement decides which host a volume lands on. It is the §20 placement
// order — source host with capacity, then a host that already has the data cached
// (snapshot cache or warm standby), then any host with capacity — bounded by the
// declared NVMe oversubscription policy of §28.2, by the measured fill ceiling of
// ADR-0013 §3, and by the fleet states of §28.1 (a CORDONED, DRAINING, or DEAD host
// never receives new work).
//
// The policy is pure: no clock, no I/O, and no dependence on the order of its input,
// so the DST harness replays a placement decision identically (INV-02).
package placement

import (
	"errors"

	"github.com/spin-stack/storage/internal/metadata"
)

// ErrNoCapacity means no host can hold the request under the policy.
var ErrNoCapacity = errors.New("placement: no host with capacity")

// DefaultMaxUsedRatio is the fill ceiling a Policy uses when it names none: the
// ≥85% row of ADR-0013 §3, the point at which a device stops taking new work. It is
// the zero value's meaning rather than "no ceiling" on purpose — a Policy written
// before this field existed is a Policy whose author never decided that a full
// device may keep receiving volumes, and defaulting to "unbounded" would reinstate
// exactly the gap this closes for every caller that did not edit its literal.
const DefaultMaxUsedRatio = 0.85

// Policy is the declared capacity rule, in two parts that answer two questions.
//
// MaxOversubscription bounds what a host has been *promised*: committed/total may
// not exceed it after the placement (§28.2). The zero value means no
// oversubscription (committed <= total) — the safe default, never "unbounded".
//
// MaxUsedRatio bounds what the host is *measured to be using*: the fraction of the
// device that may already be occupied for it to take new work (ADR-0013 §3). Zero
// means DefaultMaxUsedRatio; 1 or more means the operator has said a device may fill
// completely, which has to be typed rather than inherited.
//
// The two are separate ceilings and not one number, because the whole point of
// oversubscription is that promises exceed bytes: volumes are thin, so a fleet
// deliberately promises 1.5× what its devices hold, while no device is ever 1.5×
// full. Collapsing them (one ceiling over max(committed, used)) makes the lower —
// physical — ceiling subsume the higher one and MaxOversubscription stops doing
// anything at all.
type Policy struct {
	MaxOversubscription float64
	MaxUsedRatio        float64
}

// Request describes what is being placed. SourceHostID is the host that already
// holds the data (same-host clone, §20 step 1); CachedHostIDs are hosts with the
// snapshot cached or acting as warm standby (§20 step 2, §22.3).
type Request struct {
	SizeBytes     int64
	SourceHostID  string
	CachedHostIDs []string
}

func (p Policy) maxRatio() float64 {
	if p.MaxOversubscription <= 0 {
		return 1
	}
	return p.MaxOversubscription
}

func (p Policy) maxUsedRatio() float64 {
	if p.MaxUsedRatio <= 0 {
		return DefaultMaxUsedRatio
	}
	return p.MaxUsedRatio
}

// Admits reports whether h can take sizeBytes more without breaking the policy,
// evaluated against the host as it is *now*: the §28.2 bound on what it has been
// promised, and the ADR-0013 fill ceiling on what it is measured to be using. Both
// have to hold, because a host can be well inside its promises and out of device.
//
// The measurement arm charges the request nothing. It is a gate ("this device is
// already filling, so it takes no new work"), not an accounting, and the two
// alternatives are worse:
//
//   - `used + sizeBytes <= UsedLimit` assumes a volume occupies its declared size
//     the moment it is placed, which is the assumption oversubscription exists to
//     deny. A 1 TiB volume would be unplaceable on a half-empty 2 TiB device
//     although it will write a few GiB, so the arm that measures would kill thin
//     provisioning outright.
//   - a single occupancy number, `max(committed, used) + sizeBytes <= one ceiling`,
//     puts the physical ceiling (below 1) above the promise ceiling (usually above
//     1), so it subsumes it and MaxOversubscription stops meaning anything.
//
// It also cannot be an accounting, because the catalog cannot predict what a volume
// adds physically: under ADR-0026 a session's whole WAL stays on the device until
// the volume stops, so what lands there is what the guest writes, not what it
// declared. What is knowable is what is already there — including the bytes of
// other tenants of that filesystem, which is deliberate (agent.DiskUsage): no
// truncation of ours frees them, so a threshold that ignores them fires too late.
//
// A host that has never measured its device is refused by the NVMeTotalBytes guard
// above, and that is the whole of the "no measurement yet" case: total and used come
// from one statfs in one heartbeat, and agent.DiskUsage.Usage returns an error
// rather than a zero when that statfs fails, so there is no state where the total is
// known and the usage is not. Reading used == 0 as "unknown, refuse" instead would
// refuse the empty host — the one we most want to place on.
//
// Choose is advisory. It is pure, so two operations that read the fleet before
// either has reserved anything — a drain and a clone, or two drains — both get the
// same destination and both commit, and the destination ends up past the declared
// bound with neither caller having made a mistake. The bound therefore has to be
// re-evaluated by whatever performs the reservation, and it has to be the same rule:
// two copies of it is how a host ends up holding what placement believed it refused.
//
// This is the check; the enforcement is the same two numbers handed to the write
// that adds the bytes (Bound below, ADR-0017), so the bound is a predicate of that
// statement rather than a step before it. Re-checking here and committing afterwards
// would leave the read and the write two steps apart, and the race would survive —
// just narrower.
func (p Policy) Admits(h metadata.Host, sizeBytes int64) bool {
	if !h.State.AcceptsPlacement() || h.NVMeTotalBytes <= 0 {
		return false
	}
	return h.NVMeCommittedBytes+sizeBytes <= p.Limit(h) && h.NVMeUsedBytes <= p.UsedLimit(h)
}

// Limit is the highest committed-bytes value the policy allows on h (§28.2).
func (p Policy) Limit(h metadata.Host) int64 {
	return int64(p.maxRatio() * float64(h.NVMeTotalBytes))
}

// UsedLimit is the highest measured used-bytes value the policy lets h show and
// still receive work (ADR-0013 §3).
func (p Policy) UsedLimit(h metadata.Host) int64 {
	return int64(p.maxUsedRatio() * float64(h.NVMeTotalBytes))
}

// Bound states the same rule for the write that places the bytes: the ceilings the
// destination must respect, computed from the host Choose admitted (ADR-0017). It
// exists so the two numbers are derived in one place — a caller that computed one of
// them itself would be the second copy of the rule this whole arrangement removes,
// and a caller that forgot the fill ceiling would hand the write a zero, which the
// stores read as "the device must be measured empty" and refuse.
func (p Policy) Bound(h metadata.Host, sizeBytes int64) *metadata.CapacityBound {
	return &metadata.CapacityBound{
		HostID:    h.HostID,
		AddBytes:  sizeBytes,
		Limit:     p.Limit(h),
		UsedLimit: p.UsedLimit(h),
	}
}

// Choose returns the host for req, following the §20 order. It never mutates hosts.
func (p Policy) Choose(hosts []metadata.Host, req Request) (string, error) {
	// 1. The source host: no download at all if it still has room.
	if req.SourceHostID != "" {
		for _, h := range hosts {
			if h.HostID == req.SourceHostID && p.Admits(h, req.SizeBytes) {
				return h.HostID, nil
			}
		}
	}

	// 2. A host that already has the data cached (snapshot cache / warm standby):
	//    a shorter materialization than a cold one.
	if id, ok := p.best(hosts, req.SizeBytes, req.CachedHostIDs); ok {
		return id, nil
	}

	// 3. Any host with capacity: a full cold materialization (§22.3).
	if id, ok := p.best(hosts, req.SizeBytes, nil); ok {
		return id, nil
	}
	return "", ErrNoCapacity
}

// best returns the fitting host with the lowest post-placement committed ratio,
// breaking ties on host id so the choice is independent of input order. When only
// is non-nil, candidates are restricted to those ids.
//
// It ranks on the promise and not on the measurement, although Admits reads both.
// The measurement lags placement by a heartbeat and by however long the guest takes
// to write: a host handed ten volumes still measures empty, so ranking on used bytes
// would keep choosing it until the first of them filled — the feedback loop arrives
// after the damage. Committed moves with every placement, including the ones still
// in flight, which is exactly what a ranking needs.
func (p Policy) best(hosts []metadata.Host, sizeBytes int64, only []string) (string, bool) {
	var (
		bestID    string
		bestRatio float64
		found     bool
	)
	for _, h := range hosts {
		if only != nil && !contains(only, h.HostID) {
			continue
		}
		if !p.Admits(h, sizeBytes) {
			continue
		}
		ratio := float64(h.NVMeCommittedBytes+sizeBytes) / float64(h.NVMeTotalBytes)
		if !found || ratio < bestRatio || (ratio == bestRatio && h.HostID < bestID) {
			bestID, bestRatio, found = h.HostID, ratio, true
		}
	}
	return bestID, found
}

func contains(ids []string, id string) bool {
	for _, s := range ids {
		if s == id {
			return true
		}
	}
	return false
}
