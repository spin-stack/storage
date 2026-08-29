// Package placement decides which host a volume lands on: the §20 order (source host, then a
// host that already holds the data, then any host with capacity), bounded by §28.2's
// oversubscription policy, ADR-0013 §3's measured fill ceiling, and §28.1's fleet states — a
// CORDONED, DRAINING or DEAD host never receives new work. The policy is pure (no clock, no
// I/O, no dependence on input order), so DST replays a decision identically (INV-02).
package placement

import (
	"errors"

	"github.com/spin-stack/storage/internal/metadata"
)

// ErrNoCapacity means no host can hold the request under the policy.
var ErrNoCapacity = errors.New("placement: no host with capacity")

// DefaultMaxUsedRatio is the fill ceiling a Policy uses when it names none: the ≥85% row of
// ADR-0013 §3, at which a device stops taking new work. The zero value means this rather than
// "no ceiling" — an unedited Policy literal must not be one that fills a device.
const DefaultMaxUsedRatio = 0.85

// Policy is the declared capacity rule, in two ceilings.
//
// MaxOversubscription bounds what a host has been *promised*: committed/total may not exceed
// it after the placement (§28.2). Zero means no oversubscription, never unbounded.
// MaxUsedRatio bounds what the host is *measured* to be using (ADR-0013 §3); zero means
// DefaultMaxUsedRatio, and 1 or more has to be typed rather than inherited.
//
// Not one number: volumes are thin, so a fleet deliberately promises 1.5× what its devices
// hold while no device is ever 1.5× full — collapsing them lets the lower, physical ceiling
// subsume MaxOversubscription entirely.
type Policy struct {
	MaxOversubscription float64
	MaxUsedRatio        float64
}

// Request describes what is being placed. SourceHostID is the host that already
// holds the data (same-host clone, §20 step 1); CachedHostIDs are hosts that already
// hold the snapshot's chunks, so placing there is a shorter download (§20 step 2,
// §22.3). Both production callers pass it empty — controlplane/place.go says why,
// and clone.go passes SourceHostID instead — so step 2 selects nothing in the fleet
// today and step 1 is the whole of the locality placement buys.
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

// Admits reports whether h can take sizeBytes more: §28.2's bound on what it has been
// promised, and ADR-0013's fill ceiling on what it is measured to be using. Both have
// to hold — a host can be well inside its promises and out of device.
//
// The measurement arm charges the request nothing. It is a gate, not an accounting:
// `used + sizeBytes <= UsedLimit` would assume a volume occupies its declared size the
// moment it is placed, which is the assumption oversubscription exists to deny, and a
// single `max(committed, used)` ceiling would subsume MaxOversubscription. The catalog
// cannot predict what a volume adds physically anyway — a guest's writes stay on the
// device until a commit takes them — so what is knowable is what is already
// there, including other tenants of that filesystem (agent.DiskUsage), which no
// truncation of ours frees.
//
// used == 0 is read as an empty device, not as "unmeasured": total and used come from one
// statfs in one heartbeat, and a host that never measured is refused by the
// NVMeTotalBytes guard.
//
// This check is advisory — it is pure, so two callers reading the same fleet both get the
// same destination — which is why the same two numbers go to the write that adds the
// bytes (Bound, ADR-0017) rather than being re-checked a step before it.
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

	// 2. A host that already holds the snapshot's chunks: a shorter download than a
	//    cold one. Empty from both production callers — see Request.
	if id, ok := p.best(hosts, req.SizeBytes, req.CachedHostIDs); ok {
		return id, nil
	}

	// 3. Any host with capacity: a full cold materialization (§22.3).
	if id, ok := p.best(hosts, req.SizeBytes, nil); ok {
		return id, nil
	}
	return "", ErrNoCapacity
}

// best returns the fitting host with the lowest post-placement committed ratio, ties broken on
// host id so the choice is independent of input order. A non-nil `only` restricts candidates
// to those ids.
//
// It ranks on the promise, not the measurement: used bytes lag placement by a heartbeat and by
// however long the guest takes to write, so a host handed ten volumes still measures empty and
// would keep being chosen.
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
