package controlplane

import (
	"context"
	"fmt"

	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/placement"
)

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
func Place(ctx context.Context, md metadata.Store, policy placement.Policy, term int64, volumeID, hostID string) (metadata.Host, error) {
	// The volume first: it is where the size the policy admits against comes from, and
	// reading it means a mistyped volume id is ErrNotFound here rather than a placement
	// failure that names a host the operator never mentioned.
	vol, err := md.GetVolume(ctx, volumeID)
	if err != nil {
		return metadata.Host{}, err
	}

	if hostID == "" {
		hosts, lerr := md.ListHosts(ctx)
		if lerr != nil {
			return metadata.Host{}, lerr
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
			return metadata.Host{}, fmt.Errorf("controlplane: placing volume %s: %w", volumeID, err)
		}
	}

	// Read the host even on the automatic path, where Choose already returned it from
	// this same listing: it is the caller's proof of what it placed onto (an operator
	// naming a CORDONED host has to see that in the output), and on the named path it
	// turns a typo into ErrNotFound naming the host instead of a driver-level 23503
	// naming a constraint.
	host, err := md.GetHost(ctx, hostID)
	if err != nil {
		return metadata.Host{}, fmt.Errorf("controlplane: host %s: %w", hostID, err)
	}
	if err := md.SetVolumePrimaryHost(ctx, term, volumeID, hostID); err != nil {
		return metadata.Host{}, err
	}
	return host, nil
}
