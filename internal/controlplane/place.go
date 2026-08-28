package controlplane

import (
	"context"
	"fmt"

	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/placement"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// Placement is what an attach produced: where the volume went and under which epoch. The
// epoch is returned rather than left in the row because it is the one thing an operator
// cannot see from outside afterwards — the directory name the Agent will use, and the number
// that says this attach is not resuming the last one.
type Placement struct {
	// Host is the host the volume was placed on, with the fleet state it had when the
	// decision was taken: an operator naming a CORDONED host is honoured, and has to be
	// able to see that they were.
	Host metadata.Host
	// Epoch is the epoch the volume is served under from now on.
	Epoch int64
}

// Place gives a volume a primary host, and is the counterpart of the bare
// SetVolumePrimaryHost(…, "") that releases one: one term-guarded write plus the decision
// of where.
//
// An empty hostID asks placement.Choose over the fleet — the default, because
// -rebuild-metadata restores no placement at all (no object records a primary host), and
// making an operator name a host per volume there is making them invent one.
//
// A named host is honoured *without* an admission check: an operator naming a host is
// overriding the bound on information the catalog does not have — a volume restored to the
// machine whose device still holds its bytes goes to a host cordoned for exactly that
// fill. The override must not be silent, so the host is returned with its state.
//
// Known gap: the bound is not carried into the write. placement.Choose is advisory and
// SetVolumePrimaryHost takes no bound, so two operators attaching different volumes to one
// host at the same instant both evaluate the ceiling against a fleet the other is not in
// yet, and both commit. Left open because closing it is a signature change to a store
// method, and this path is one human running one command (ADR-0021). STATUS.md records it
// as owed.
//
// A volume that already has a primary is refused with ErrAlreadyPlaced: a host learns it
// has lost a volume only on its next poll, so a straight hand-over would have two Agents
// serving one volume.
//
// **An attach grants a fresh epoch, and that is what makes a returning host safe.** The
// Agent's WAL lives at <data-dir>/wal/<volume-id>/<epoch>, so a volume that goes A -> B and
// comes back to A at the epoch A already used opens A's *previous* session's directory and
// lays those records over the image B published — older bytes on top of newer ones, no
// error anywhere. What it costs, said out loud: a session whose teardown publish failed is
// abandoned, its records left in the old epoch's directory for an operator to recover by
// hand. Keeping the epoch is only correct when nobody else has served the volume since,
// which is exactly what this code cannot know.
//
// Three placements deliberately do **not** move it:
//
//   - a re-run of the same attach, which has to stay the no-op the store made it or it
//     tears down a running guest's device;
//   - a detach, which grants the volume to nobody: burning a fencing token no host holds
//     names the next attach's WAL root after a writer that never existed;
//   - an Agent restart, which is not a placement at all (ADR-0024): the row is untouched
//     and the Agent re-attaches to its own WAL and republishes.
//
// Provisioning does not bump either: a fresh v7 id has no WAL directory on any host to
// collide with.
func Place(ctx context.Context, md metadata.Store, store objectstore.Store, policy placement.Policy, term int64, volumeID, hostID string) (Placement, error) {
	// The volume first: it is where the size the policy admits against comes from, and
	// reading it means a mistyped volume id is ErrNotFound here rather than a placement
	// failure that names a host the operator never mentioned. Its epoch is also what the
	// grant below compares against.
	vol, err := md.GetVolume(ctx, volumeID)
	if err != nil {
		return Placement{}, err
	}

	if hostID == "" {
		hosts, lerr := md.ListHosts(ctx)
		if lerr != nil {
			return Placement{}, lerr
		}
		// SourceHostID and CachedHostIDs are deliberately empty, so this is §20's third
		// rule — any host with capacity. Nothing in the catalog records where a cleared
		// volume's bytes are, and a stale id would not be a hint but an override: rule 1
		// returns the source host if it merely admits.
		hostID, err = policy.Choose(hosts, placement.Request{SizeBytes: vol.SizeBytes})
		if err != nil {
			return Placement{}, fmt.Errorf("controlplane: placing volume %s: %w", volumeID, err)
		}
	}

	// Read the host even on the automatic path, where Choose already returned it from
	// this same listing: it is the caller's proof of what it placed onto (an operator
	// naming a CORDONED host has to see that in the output), and on the named path it
	// turns a typo into ErrNotFound naming the host instead of a driver-level 23503
	// naming a constraint.
	host, err := md.GetHost(ctx, hostID)
	if err != nil {
		return Placement{}, fmt.Errorf("controlplane: host %s: %w", hostID, err)
	}

	// The hand-over refusal, restated here because the epoch grant below would not make it:
	// BumpVolumeEpoch writes primary_host_id whatever it holds, so reaching it with a volume
	// placed elsewhere would move the volume. It is a read-then-write and therefore not an
	// exclusion; what makes it safe is that the grant underneath *is* a compare-and-set, so
	// two operators racing to attach the same unplaced volume produce one winner.
	epoch := vol.CurrentEpoch
	if vol.PrimaryHostID != "" && vol.PrimaryHostID != hostID {
		return Placement{}, fmt.Errorf("%w: volume %s is placed on %s, detach it before placing it on %s",
			metadata.ErrAlreadyPlaced, volumeID, vol.PrimaryHostID, hostID)
	}

	// Order: the epoch first, the §7 state second, and it is not interchangeable. The other
	// order puts the volume in the host's desired state at the epoch it used before — the
	// stale WAL root this grant exists to make unreachable — for as long as the second write
	// takes, and for ever if the process dies between them. This order's interruption leaves
	// the volume placed at a fresh epoch and still DETACHED, which the operator fixes by
	// re-running the attach.
	if vol.PrimaryHostID == "" {
		// Recorded in the bucket before the catalog moves, because the catalog is the
		// thing rebuild-metadata exists because you can lose. Before, not after: a crash
		// in between leaves the bucket holding a number the catalog never reached, and a
		// rebuild that restores too high can only over-fence; the other order leaves the
		// bucket behind and hands a predecessor a live token.
		if err := descriptor.WriteEpoch(ctx, store, volumeID, vol.CurrentEpoch+1); err != nil {
			return Placement{}, err
		}
		epoch, err = md.BumpVolumeEpoch(ctx, term, volumeID, hostID, vol.CurrentEpoch)
		if err != nil {
			return Placement{}, fmt.Errorf("controlplane: granting volume %s an epoch on %s: %w", volumeID, hostID, err)
		}
	}
	if err := md.SetVolumePrimaryHost(ctx, term, volumeID, hostID); err != nil {
		return Placement{}, err
	}
	return Placement{Host: host, Epoch: epoch}, nil
}
