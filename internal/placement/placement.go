// Package placement decides which host a volume lands on. It is the §20 placement
// order — source host with capacity, then a host that already has the data cached
// (snapshot cache or warm standby), then any host with capacity — bounded by the
// declared NVMe oversubscription policy of §28.2 and by the fleet states of §28.1
// (a CORDONED, DRAINING, or DEAD host never receives new work).
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

// Policy is the declared oversubscription rule: committed/total may not exceed
// MaxOversubscription after the placement. The zero value means no oversubscription
// (committed <= total) — the safe default, never "unbounded".
type Policy struct {
	MaxOversubscription float64
}

// Request describes what is being placed. SourceHostID is the host that already
// holds the data (same-host clone, §20 step 1); CachedHostIDs are hosts with the
// snapshot cached or acting as warm standby (§20 step 2, §22.3).
type Request struct {
	SizeBytes     int64
	SourceHostID  string
	CachedHostIDs []string
}

// CommittedRatio is committed/total, the value behind the host_nvme_committed_ratio
// alert (§28.2). A host that reports no NVMe has ratio 0 (and is never chosen).
func CommittedRatio(h metadata.Host) float64 {
	if h.NVMeTotalBytes <= 0 {
		return 0
	}
	return float64(h.NVMeCommittedBytes) / float64(h.NVMeTotalBytes)
}

func (p Policy) maxRatio() float64 {
	if p.MaxOversubscription <= 0 {
		return 1
	}
	return p.MaxOversubscription
}

// Admits reports whether h can take sizeBytes more without breaking the policy: the
// §28.2 bound, evaluated against the host as it is *now*.
//
// Choose is advisory. It is pure, so two operations that read the fleet before
// either has reserved anything — a drain and a clone, or two drains — both get the
// same destination and both commit, and the destination ends up past the declared
// bound with neither caller having made a mistake. The bound therefore has to be
// re-evaluated by whatever performs the reservation, and it has to be the same rule:
// two copies of it is how a host ends up holding what placement believed it refused.
//
// This is the check, not the enforcement. Enforcement belongs inside the write that
// adds the bytes — otherwise the read and the write are still two steps and the race
// survives, just narrower.
func (p Policy) Admits(h metadata.Host, sizeBytes int64) bool {
	if !h.State.AcceptsPlacement() || h.NVMeTotalBytes <= 0 {
		return false
	}
	return h.NVMeCommittedBytes+sizeBytes <= p.Limit(h)
}

// Limit is the highest committed-bytes value the policy allows on h (§28.2).
func (p Policy) Limit(h metadata.Host) int64 {
	return int64(p.maxRatio() * float64(h.NVMeTotalBytes))
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
