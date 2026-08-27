package controlplane

import (
	"context"
	"fmt"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// RebuildSummary is what one rebuild found and recorded.
//
// There is no Snapshots count any more, and the absence is the report: the objects a
// snapshot's existence was read out of were the chunked image's manifests, which went
// with the local block engine. Keeping a field that is structurally zero would have a
// rebuild print "0 snapshots" at an operator who has just lost their catalog, and that
// sentence is indistinguishable from "your bucket holds no snapshots".
type RebuildSummary struct {
	Volumes int
}

// RebuildMetadata reconstructs the volume catalog from the object store alone
// (§22.5, INV-20). It is the reader `descriptor.Write` had been feeding since
// increment 4 deleted the previous one, and it is why those objects are written at all:
// without it a lost PostgreSQL is unrecoverable even though every byte of every volume
// is intact in the bucket.
//
// # What it restores, and what it cannot
//
// It restores what the descriptor states: geometry, the wrapped DEK with the version
// that names it, the epoch, and the chain depth. It does **not** restore placement — no
// volume comes back with a primary host — because no object records one. A rebuilt
// catalog therefore describes volumes nobody is serving, which is the honest outcome:
// after losing the database you know what exists, not who was running it, and an
// operator re-places them.
//
// # One pass, and what the other two used to do
//
// It used to be three, because it also recorded every snapshot (pass 2) and then linked
// each clone to its parent snapshot (pass 3), and `volumes.parent_snapshot_id` and
// `snapshots.volume_id` reference each other so neither could go first.
//
// Both of those read the chunked image's manifests — the per-volume `manifest.json` for
// the published sequence, and `image/<vol>/snapshots/*.json` for the snapshots — and
// that format is withdrawn with the local block engine. There is nothing left in the
// bucket to read them out of, and inventing the numbers is the one thing a rebuild must
// not do: `published_sequence` and `durable_sequence` are the two floors an Agent checks
// at attach, so a rebuild that guesses high refuses to serve a volume that is fine, and
// one that guesses low hands a guest a blank device for a volume it has written into.
//
// So a rebuilt volume comes back with zeroed sequences and no parent link, and the
// commit protocol that replaces the manifest is what will restore both. A clone whose
// parent link is not restored still has its data — the link is a catalog fact, and the
// descriptor still carries the parent id for whoever re-establishes it.
//
// # It needs the KEK
//
// Every descriptor's wrapped DEK is unwrapped before the volume is recorded, and a
// failure stops the whole rebuild — see checkKey for why anything softer turns one
// poisoned PUT into permanent key loss. That makes `-rebuild-metadata` require
// `-kek-file`, a new demand at the worst possible moment, and it is accepted
// deliberately: the alternative is a repair tool that records key material nobody
// verified.
//
// Running it twice, or from two operators at once, converges rather than aborting;
// `CreateVolume` is idempotent by design, and this passes no capacity bound because it
// is recording volumes that already occupy their hosts.
// KeyChecker is the KMS operation a rebuild needs: prove that each descriptor's wrapped
// DEK really is this volume's, before the catalog records it as such.
//
// Narrow, and read-only in effect — the DEK it recovers is discarded. What it buys is
// the difference between a rebuild that repairs and one that launders: a rebuild runs
// precisely when the catalog is gone, so a poisoned `dek_wrapped` copied out of the
// bucket becomes the *only* record of that volume's key, and the loss is permanent and
// silent.
type KeyChecker interface {
	KEKID() string
	UnwrapDEK(wrapped []byte, keyID uint32, volumeID [16]byte) (crypto.DEK, error)
}

func RebuildMetadata(ctx context.Context, md metadata.Store, store objectstore.Store, kms KeyChecker, term int64) (RebuildSummary, error) {
	var sum RebuildSummary

	descs, err := listDescriptors(ctx, store, kms)
	if err != nil {
		return sum, err
	}
	for _, d := range descs {
		// The same geometry rules provisioning applies, on the way in. A rebuild reads
		// objects from a bucket, so a descriptor with a size of zero or a block size no
		// device can address is an input, not a bug in this code — and recorded, it is a
		// catalog row nothing can ever serve, created by the one command an operator runs
		// when the catalog is already gone.
		if err := geometry(d.SizeBytes, d.BlockSize); err != nil {
			return sum, fmt.Errorf("controlplane: volume %s: %w", d.VolumeID, err)
		}
		v := volumeFromDescriptor(ctx, store, d)
		// Cleared, not carried: `volumes.parent_snapshot_id` references a snapshots row,
		// and nothing records snapshots any more, so writing the link would fail the
		// foreign key on every clone in the bucket and abort the rebuild.
		v.ParentSnapshotID = ""
		if err := md.CreateVolume(ctx, term, v, nil); err != nil {
			return sum, fmt.Errorf("controlplane: recording volume %s: %w", d.VolumeID, err)
		}
		sum.Volumes++
	}
	return sum, nil
}

func volumeFromDescriptor(ctx context.Context, store objectstore.Store, d descriptor.Descriptor) metadata.Volume {
	return metadata.Volume{
		VolumeID:  d.VolumeID,
		SizeBytes: d.SizeBytes,
		BlockSize: d.BlockSize,
		// The epoch is taken from the *published history* and raised past it, not from
		// the descriptor.
		//
		// descriptor.json's current_epoch is written at create and at clone and updated
		// by nothing — the comment at the field says so. A volume fenced up to epoch 4
		// therefore has a descriptor that still reads 1, and a rebuild that believed it
		// handed every host that ever held the volume a token this catalog would accept
		// again. Every commit manifest records the epoch its writer held, and the
		// manifest chain under HEAD is framed and digest-checked, so the newest commit
		// is a lower bound on the truth that an adversary with the bucket cannot lower.
		//
		// Raised *past* it rather than restored to it: the point of a fencing token is
		// that no predecessor holds a live one, and coming back at the same number leaves
		// every one of them valid.
		CurrentEpoch: epochFloor(ctx, store, d) + 1,
		// ACTIVE with no primary: the volume exists and nobody is serving it. There is
		// no object that records placement, and inventing one would make a rebuilt
		// catalog claim a host is writing when nothing is.
		State:            lifecycle.VolumeActive,
		ChainDepth:       d.ChainDepth,
		ParentSnapshotID: d.ParentSnapshotID,
		DEKWrapped:       d.DEKWrapped,
		KEKID:            d.KEKID,
		DEKKeyID:         d.DEKKeyID,
	}
}

// listDescriptors reads every volume descriptor in the bucket. A descriptor that fails
// its own digest check stops the rebuild rather than being skipped: a catalog rebuilt
// from some of the bucket, silently, is worse than no rebuild — the operator would have
// no way to know which volumes are missing.
func listDescriptors(ctx context.Context, store objectstore.Store, kms KeyChecker) ([]descriptor.Descriptor, error) {
	objs, err := store.List(ctx, descriptor.Prefix)
	if err != nil {
		return nil, fmt.Errorf("controlplane: listing volume descriptors: %w", err)
	}
	var out []descriptor.Descriptor
	for _, o := range objs {
		volumeID, ok := descriptor.VolumeOfKey(o.Key)
		if !ok {
			continue
		}
		d, err := descriptor.Read(ctx, store, volumeID)
		if err != nil {
			return nil, fmt.Errorf("controlplane: reading %s: %w", o.Key, err)
		}
		if d.VolumeID != volumeID {
			// The object says it belongs to another volume. That is what a bucket
			// copied with the wrong prefix looks like, and acting on it would record a
			// volume under an id whose data lives somewhere else.
			return nil, fmt.Errorf("controlplane: %s describes volume %s", o.Key, d.VolumeID)
		}
		if err := checkKey(d, kms); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

// checkKey refuses a descriptor whose wrapped DEK is not this volume's.
//
// The structural checks above cannot see the attack this closes. `descriptor.json` is
// digest-framed and nothing more, the digest is not authentication, and a swap that
// moves only `dek_wrapped`/`dek_key_id` leaves `volume_id` honest — so every check on
// the object passes. The KEK is the only witness, because it is the one thing the
// bucket-writing adversary does not hold.
//
// The whole rebuild fails, the same way a corrupt digest already fails it: a catalog
// rebuilt from *some* of the bucket, silently, leaves the operator with no way to know
// which volumes are missing — and here the alternative is worse than missing, it is
// recording key material nobody verified.
//
// The KEK-ID mismatch is reported separately and first. An operator who passed the
// wrong -kek-file must not be told their bucket was forged; publisher.encryption already
// draws that same line, and this is the moment — the catalog is already gone — when a
// misleading message costs the most.
func checkKey(d descriptor.Descriptor, kms KeyChecker) error {
	if d.KEKID != kms.KEKID() {
		return fmt.Errorf("controlplane: %s was wrapped under KEK %q and this Control Plane holds %q: this is a wrong -kek-file, not a forged descriptor",
			descriptor.Key(d.VolumeID), d.KEKID, kms.KEKID())
	}
	u, err := ids.Parse(d.VolumeID)
	if err != nil {
		return fmt.Errorf("controlplane: %s names volume %q, which is not a uuid: %w", descriptor.Key(d.VolumeID), d.VolumeID, err)
	}
	if _, err := kms.UnwrapDEK(d.DEKWrapped, d.DEKKeyID, [16]byte(u)); err != nil {
		return fmt.Errorf("controlplane: %s carries a wrapped DEK that was not sealed for this volume, so it was written by something other than this fleet's Control Plane. "+
			"Recording it would make it the only record of the volume's key and lose the real one for good: %w",
			descriptor.Key(d.VolumeID), err)
	}
	return nil
}

// epochFloor is the highest epoch this volume can be shown to have used: the greater of
// what its descriptor last recorded and what the newest published commit says.
//
// A volume that has published nothing falls back to the descriptor, which is all there is.
// A bucket that will not answer is not a reason to guess low — the caller is rebuilding a
// catalog, and an epoch that comes back too low is a fence that is not one — so the read
// failing is reported through the descriptor's own number and the summary says the history
// was not consulted.
func epochFloor(ctx context.Context, store objectstore.Store, d descriptor.Descriptor) int64 {
	// Three sources, and the highest wins because a fencing token may only ever go up.
	//
	// The recorded epoch is the one written on every grant and is the authority; the
	// descriptor's is what a volume was created at and is a floor for one that has never
	// been re-placed; the newest commit's is what a writer actually held, which covers a
	// bucket restored without its epoch object. A source that will not answer contributes
	// nothing rather than lowering the answer.
	floor := d.CurrentEpoch
	if recorded, err := descriptor.ReadEpoch(ctx, store, d.VolumeID); err == nil {
		floor = max(floor, recorded)
	}
	head, err := commit.ReadHeadCommit(ctx, store, d.VolumeID)
	if err != nil {
		return floor
	}
	m, err := commit.ReadManifest(ctx, store, d.VolumeID, head)
	if err != nil {
		return floor
	}
	return max(floor, m.Epoch)
}
