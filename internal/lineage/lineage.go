// Package lineage is a volume's ancestry: how to walk it, how to compose it into a read
// view, and — with Flatten — how to leave it.
//
// It exists because two components need the same walk and must not have two of them. The
// Agent walks a lineage on every attach of a clone (agent.fetchBase), and the operator's
// FLATTEN walks the same chain to write down what it composes. This repository has already
// paid for the alternative once: the two binaries parsed the KEK file differently, because
// nothing had ever run both against one file, and the fix was to put the rule in one place
// (crypto.LoadKEK). A lineage walk has more corners than a key file — a cycle, half a
// link, a descriptor that is not there, a snapshot that was never published — and each one
// is a silent wrong answer if the two implementations disagree about it.
//
// # The bucket states a lineage, and it is the only thing that can
//
// Every link comes from an object: `volumes/<vol>/descriptor.json` carries
// ParentSnapshotID and ParentVolumeID, and the walk follows them upward. Nothing here
// asks a Control Plane anything (ADR-0021), and that is not merely allowed, it is
// required: `-rebuild-metadata` exists because the catalog is the component whose loss the
// bucket must survive (§22.5, INV-20), so a lineage the catalog alone could state would be
// a lineage a restore could not.
//
// The catalog cannot be the authority for a second reason, and it is the one that made
// Flatten possible at all: `volumes.parent_snapshot_id` is **write-once by construction**
// — CreateVolume's conflict path is
// `parent_snapshot_id = COALESCE(volumes.parent_snapshot_id, EXCLUDED.parent_snapshot_id)`
// — so no write path in the catalog can ever say "this volume no longer descends from
// anything". Only the bucket can. DELETION-AND-RECLAIM-SPEC found that hole and made it
// FLATTEN's job to fill.
//
// # Every failure is closed
//
// A walk that stopped early on anything — a missing descriptor, a half link, a cycle —
// would compose a view holding however much of the lineage it managed to read, and a
// guest cannot tell a partial view from a complete one (DEV-0007). So each of them is
// refused by name rather than treated as the top of the chain.
package lineage

import (
	"context"
	"errors"
	"fmt"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/wal"
)

// Ancestor is one link of a lineage: a snapshot, and the volume it lives under. Both
// halves are needed to name an object at all — image.SnapshotKey takes the volume — which
// is why descriptor.json carries both.
type Ancestor struct {
	// Volume is the ancestor's id as the catalog and the descriptor keys spell it.
	Volume string
	// ID is that same id as the object keys carry it.
	ID [16]byte
	// Snapshot is the frozen point the volume below descends from.
	Snapshot string
}

// maxWalk is what stops a malformed lineage from becoming an attach that never returns.
// It is not the depth limit.
//
// The depth limit is a refusal at *create* time — controlplane.Clone refuses past the
// design document's ceiling of 5 (§20.1) — and that is where it belongs, because refusing
// here would turn a volume the fleet created successfully into one nothing can read. So
// this sits far above that ceiling deliberately: it is a termination guard for a chain
// some future bucket edit or wrong rebuild made absurd, not a policy anybody should hit.
// The cycle case is caught by name below and does not depend on this number.
const maxWalk = 64

// Walk returns volumeID's ancestry, nearest ancestor first, or nil for a volume that
// descends from nothing.
//
// **It starts at the volume's own descriptor**, not at a link the caller supplies, and
// that is the change FLATTEN forced. The Agent used to take the first link from the
// desired state and every link above it from an ancestor's descriptor — two authorities
// for one fact, which was harmless only while nothing could ever dissolve a lineage.
// Something can now: after a flatten the catalog still names a parent (its column is
// write-once, see the package doc) and the bucket does not, and a reader that believed the
// catalog would walk to an ancestry the volume no longer reads through — and look for its
// own chunks under a lineage root that is no longer its own.
//
// So the desired state's link says only *whether to look*; what is found is the bucket's
// answer. ADR-0021 is untouched: this is a read of an object, not a lookup against a
// Control Plane.
func Walk(ctx context.Context, store objectstore.Store, volumeID string) ([]Ancestor, error) {
	// The volume being walked is in the set from the start: a lineage that links back to
	// it is a cycle like any other.
	seen := map[string]bool{volumeID: true}
	from := volumeID

	self, err := descriptor.Read(ctx, store, volumeID)
	if err != nil {
		return nil, fmt.Errorf("lineage: volume %s: reading %s, which is where its own parent link lives: %w",
			volumeID, descriptor.Key(volumeID), err)
	}
	snapID, volID := self.ParentSnapshotID, self.ParentVolumeID

	var chain []Ancestor
	for snapID != "" {
		if volID == "" {
			// Half a link is worse than none. image.SnapshotKey needs the volume, so a
			// snapshot id on its own addresses nothing: the load would find no manifest
			// and the base would read as zeros for that ancestor's whole extent.
			return nil, fmt.Errorf("lineage: volume %s: the descriptor of %s names parent snapshot %s with no parent volume",
				volumeID, from, snapID)
		}
		if seen[volID] {
			return nil, fmt.Errorf("lineage: volume %s: its lineage revisits volume %s, which makes it a cycle rather than a chain",
				volumeID, volID)
		}
		seen[volID] = true
		u, err := ids.Parse(volID)
		if err != nil {
			return nil, fmt.Errorf("lineage: volume %s: the descriptor of %s names parent volume %q, which is not a uuid: %w",
				volumeID, from, volID, err)
		}
		chain = append(chain, Ancestor{Volume: volID, ID: [16]byte(u), Snapshot: snapID})
		if len(chain) > maxWalk {
			return nil, fmt.Errorf("lineage: volume %s: its lineage is over %d links deep, which is not a chain this process will walk",
				volumeID, maxWalk)
		}

		// A descriptor that is not there is *not* "the lineage ends here". This walk
		// cannot tell the top of a chain from a hole in one, and answering "the top"
		// would serve every range above the hole as zeros — so it says so instead.
		anc, err := descriptor.Read(ctx, store, volID)
		if err != nil {
			return nil, fmt.Errorf("lineage: volume %s: reading %s, the descriptor of ancestor %s, which is where its own parent link lives: %w",
				volumeID, descriptor.Key(volID), volID, err)
		}
		from, snapID, volID = volID, anc.ParentSnapshotID, anc.ParentVolumeID
	}
	return chain, nil
}

// Root is the lineage root of a volume with this chain: the volume that descends from
// nothing. It names the chunk store the whole chain shares and it is what the chunk AAD
// binds (image.Ident), because it is the scope of the DEK — every clone inherits its
// parent's (controlplane.Clone).
//
// A volume with an empty chain is its own root, which is nearly every volume.
func Root(volumeID [16]byte, chain []Ancestor) [16]byte {
	if len(chain) == 0 {
		return volumeID
	}
	// Walk returns nearest-first, so the last link is the one that descends from nothing.
	return chain[len(chain)-1].ID
}

// Compose builds the read view a volume inherits: every snapshot in chain, layered oldest
// first over bottom — or bottom itself for a volume that descends from nothing.
//
// # The order is what makes it a view rather than a pile
//
// Oldest at the bottom, so the nearest ancestor — the snapshot this volume was actually
// cloned from — ends on top, where its bytes win over everything it inherited. cow needed
// nothing for this: NewIntervalMapOver nests arbitrarily and both Ranges and Read recurse
// through m.base.
//
// # The erasures are what this composition cannot do without
//
// A range an intermediate ancestor discarded is a range where the layers below it still
// hold bytes, and absence in a layered chain means "ask the layer below". So the snapshot
// manifests loaded here carry their erasures explicitly (image.Manifest.Discarded, replayed
// as a tombstone by the loader) and this reproduces §14.6 rather than uncovering the
// grandparent's older bytes.
//
// # bottom
//
// nil for a reader — the bottom of a chain is a link like any other, laid over nothing.
// Flatten passes an **empty map** instead, and the difference is the whole of what makes a
// flattened manifest flattened: cow.DeltaOver reports what sits above the map it is given,
// so a delta taken over nil is this volume's own layers and a delta taken over an empty
// map at the foot of the chain is everything the chain answers with.
//
// The snapshot, never an ancestor's live image: an ancestor that is still running has
// written past the point this lineage descends from, and its image carries those writes.
// §19 makes the distinction cheap — a snapshot is a frozen view at a sequence, naming
// chunks the lineage already holds.
func Compose(ctx context.Context, store objectstore.Store, enc *wal.Encryption, root [16]byte,
	chain []Ancestor, bottom *cow.IntervalMap,
) (*cow.IntervalMap, error) {
	view := bottom
	for i := len(chain) - 1; i >= 0; i-- {
		a := chain[i]
		aenc, err := bind(enc, a)
		if err != nil {
			return nil, err
		}
		// **The two ids are different things here, which is why image takes them as one
		// named pair.** The ancestor's own id says which manifest to read — a snapshot is
		// addressed under the volume it belongs to. The lineage root says where that
		// manifest's chunks are and what their AAD binds, and it is the *walking volume's*
		// root, which is the same root as every ancestor's precisely because they are one
		// chain. Getting the first wrong reads the wrong manifest; getting the second
		// wrong finds no chunk at all, or one that will not open.
		next, _, err := image.LoadSnapshotOver(ctx, store, aenc,
			image.Ident{Volume: a.ID, Lineage: root}, a.Snapshot, view)
		switch {
		case err == nil:
			view = next
		case errors.Is(err, image.ErrNotPublished):
			// Not a failure to hide: a lineage whose snapshot was never published would
			// read zeros for everything that ancestor wrote.
			return nil, fmt.Errorf("lineage: snapshot %s of %s was never published, so the chain below it cannot be read",
				a.Snapshot, a.Volume)
		default:
			return nil, fmt.Errorf("lineage: loading snapshot %s of ancestor %s: %w", a.Snapshot, a.Volume, err)
		}
	}
	return view, nil
}

// bind re-binds a DEK to an ancestor's id, and returns nil for an unencrypted lineage.
//
// **The re-binding is inert for image chunks, and it is inert twice over.** A comment in
// the Agent used to claim that handing an ancestor's snapshot the *walking* volume's
// Encryption would fail to open every chunk that ancestor wrote; it would not, because
// image.open takes the id its AAD binds as a *parameter* rather than reading it off the
// Encryption, and the DEK is the same one either way — each clone inherits its parent's
// (controlplane.Clone), so a whole lineage shares the root's. Since the chunk AAD binds
// the **lineage root** rather than a volume, the argument LoadSnapshotOver is given is the
// same for every ancestor as for the volume itself, so even the parameter no longer
// distinguishes them. Proven by planting it: binding to the walking volume's id changes
// nothing.
//
// Kept, with the claim corrected rather than the call deleted, because an Encryption bound
// to the wrong volume is the wrong object to be holding on a path whose whole subject is
// another volume's data — and because wal.Encryption *does* bind the id for WAL records,
// so a future reader of an ancestor's records would need exactly this.
func bind(enc *wal.Encryption, a Ancestor) (*wal.Encryption, error) {
	if enc == nil {
		return nil, nil //nolint:nilnil // no encryption is a mode, not a failure — see agent.encryptionFor
	}
	aenc, err := wal.NewEncryption(enc.DEK, a.ID)
	if err != nil {
		return nil, fmt.Errorf("lineage: binding the DEK to ancestor %s: %w", a.Volume, err)
	}
	return aenc, nil
}
