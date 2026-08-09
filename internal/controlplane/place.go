package controlplane

import (
	"context"
	"fmt"

	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/placement"
)

// Placement is what an attach produced: where the volume went and under which epoch.
//
// The epoch is returned rather than left in the row because it is the one thing an
// operator cannot see from outside afterwards — it is the directory name the Agent will
// use, and the number that says this attach is not resuming the last one. The command
// that placed the volume prints it, so "the volume moved" and "the old session's WAL is
// out of reach" are one line in a log rather than a psql query nobody runs.
type Placement struct {
	// Host is the host the volume was placed on, with the fleet state it had when the
	// decision was taken: an operator naming a CORDONED host is honoured, and has to be
	// able to see that they were.
	Host metadata.Host
	// Epoch is the epoch the volume is served under from now on.
	Epoch int64
}

// Place gives a volume a primary host, and is the counterpart of the bare
// SetVolumePrimaryHost(…, "") that releases one. It is one term-guarded write plus the
// decision of where — which is the whole reason it is a function and not a store call.
//
// **Who chooses the host: both, and the default is placement.** hostID names one; an
// empty hostID asks placement.Choose over the fleet, the same §20 order and the same
// ADR-0013/§28.2 ceilings Clone already places against.
//
// The default is the automatic one because of the case that needs this most.
// -rebuild-metadata restores the catalog from the bucket with no placement at all — no
// object records a primary host, so *every* volume comes back unplaced — and an
// operator putting a fleet back together after losing PostgreSQL has no list of where
// things were, only a list of volumes. Making them name a host per volume there is
// making them invent one, and inventing it is exactly what the placement order exists
// to do better: it knows which hosts are DRAINING, which are over their fill ceiling,
// and which have the least committed.
//
// A named host is still honoured, and honoured *without* an admission check. The bound
// says where the system may put new work; an operator naming a host is overriding that
// on information the catalog does not have — a volume being restored to the machine
// whose device still holds its bytes is going to a host that is cordoned for exactly
// that fill, and refusing it would make the override useless the one time it is
// needed. What must not happen is the override being silent, so the host is returned
// with its state and the caller reports it.
//
// **The bound is not carried into the write, and that is a known gap rather than a
// decision that this is safe.** placement.Choose is advisory (its doc says so): the
// enforcement ADR-0017 asks for is the same two numbers handed to the statement that
// places the bytes, as CreateVolume takes them, and SetVolumePrimaryHost takes no
// bound. So two operators attaching different volumes to the same host at the same
// instant both evaluate the ceiling against a fleet the other is not in yet and both
// commit. It is left because closing it is a signature change to a store method whose
// two implementations and contract were written one increment ago, and because this
// path is one human running one command from a shell (ADR-0021: these binaries are
// test harnesses, not a deployed admin API). It is recorded in STATUS.md as owed, not
// as done.
//
// A volume that already has a primary is refused by the store with ErrAlreadyPlaced:
// a host learns it has lost a volume only on its next poll, so a straight hand-over
// would have two Agents serving one volume. The caller detaches, observes the release,
// and places.
//
// **An attach grants a fresh epoch, and that is what makes a returning host safe.** The
// Agent's WAL lives at <data-dir>/wal/<volume-id>/<epoch>, so a volume that goes A -> B
// and comes back to A at the epoch A already used opens the directory A's *previous*
// session left behind, and lays those records over the image B published in the
// meantime: older bytes on top of newer ones, no error anywhere, on a path a pilot with
// two hosts reaches by moving one volume. A number that is new to the volume is a
// directory that is empty on every host, so the session starts from the published image
// — which is the authority (§5.8) — instead of from whatever is on the local device.
//
// What it costs, said out loud: a session whose teardown publish failed is abandoned.
// The Agent logs that failure ("this session's writes are only in this host's local
// WAL") and deletes nothing, so the records are still in the old epoch's directory for
// an operator to recover by hand. The alternative — keeping the epoch so the local WAL
// is resumed — is only correct when nobody else has served the volume since, which is
// exactly the thing this code cannot know: an unrecoverable publish is loud and rare,
// wrong bytes are silent and follow every move.
//
// Three placements deliberately do **not** move it:
//
//   - a re-run of the same attach (the volume is already on this host). It has to stay
//     the no-op the store made it — an operator re-running the command after a timeout
//     would otherwise tear down a running guest's device;
//   - a detach, which grants the volume to nobody. An epoch is a fencing token, and
//     burning one that no host holds means the next attach's WAL root is named after a
//     writer that never existed;
//   - an Agent restart, which is not a placement at all: the row is untouched, the
//     desired state repeats the epoch, and the Agent re-attaches to its own WAL and
//     republishes (ADR-0024). That case is what the resume path is for.
//
// Provisioning does not bump either, and cannot: `Provision` writes epoch 1 into the row
// and into descriptor.json in one act, and a fresh volume id has no WAL directory on any
// host to collide with.
func Place(ctx context.Context, md metadata.Store, policy placement.Policy, term int64, volumeID, hostID string) (Placement, error) {
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
		// rule — any host with capacity — rather than the first two. Those two are about
		// where the bytes already are, and once a volume's placement is cleared nothing
		// in the catalog records that: the image is in the object store, the host that
		// last served it is the field this write is about to set, and a parent
		// snapshot's source host is where its *parent's* chunks were cached at the
		// moment it was taken, which nothing keeps true. Passing a stale id as
		// SourceHostID would not be a hint, it would be an override that skips the
		// ranking entirely (rule 1 returns the source host if it merely admits).
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

	// The hand-over refusal, restated here because the epoch grant below would not make
	// it: BumpVolumeEpoch writes primary_host_id whatever it holds, so reaching it with
	// a volume that is placed elsewhere would move the volume in the one write
	// SetVolumePrimaryHost exists to refuse (metadata.ErrAlreadyPlaced carries why). It
	// is a read-then-write and therefore not the exclusion the store's predicate is —
	// what makes it safe is that the epoch grant underneath *is* a compare-and-set, so
	// two operators racing to attach the same unplaced volume produce one winner and one
	// ErrEpochConflict rather than two placements.
	epoch := vol.CurrentEpoch
	if vol.PrimaryHostID != "" && vol.PrimaryHostID != hostID {
		return Placement{}, fmt.Errorf("%w: volume %s is placed on %s, detach it before placing it on %s",
			metadata.ErrAlreadyPlaced, volumeID, vol.PrimaryHostID, hostID)
	}

	// Order: the epoch first, the §7 state second, and it is not interchangeable. The
	// other order puts the volume in the host's desired state at the epoch it used
	// before, which is precisely the stale WAL root this grant exists to make
	// unreachable — for as long as it takes the second write to land, and for ever if
	// the process dies in between. This order's interruption leaves the volume placed at
	// a fresh epoch with the state still DETACHED: the operator re-runs the attach, which
	// is a no-op on the ownership and moves only the state, and until they do the volume
	// carries a state that says nobody is writing rather than an epoch that says the
	// wrong writer may.
	if vol.PrimaryHostID == "" {
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
