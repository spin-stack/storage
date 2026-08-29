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

// RebuildSummary is what one rebuild found and recorded. There is no Snapshots count: the
// objects a snapshot's existence was read out of went with the chunked image, and printing
// "0 snapshots" at an operator who has just lost their catalog is indistinguishable from
// "your bucket holds no snapshots".
type RebuildSummary struct {
	Volumes int
}

// RebuildMetadata reconstructs the volume catalog from the object store alone (§22.5,
// INV-20). It is why descriptors are written at all: without it a lost PostgreSQL is
// unrecoverable even though every byte of every volume is intact in the bucket.
//
// It restores what the descriptor states: geometry, the wrapped DEK with the version that
// names it, the epoch, and the chain depth. It does **not** restore placement — no object
// records one — so a rebuilt catalog describes volumes nobody is serving, and an operator
// re-places them.
//
// A rebuilt volume also comes back with zeroed sequences and no parent link. The objects
// those were read out of (the chunked image's manifests) are withdrawn, and inventing the
// numbers is the one thing a rebuild must not do: `published_sequence` and
// `durable_sequence` are the two floors an Agent checks at attach, so a rebuild that
// guesses high refuses to serve a volume that is fine and one that guesses low hands a
// guest a blank device for a volume it has written into. A clone's data is untouched — the
// link is a catalog fact, and the descriptor still carries the parent id.
//
// Every descriptor's wrapped DEK is unwrapped before its volume is recorded, and a failure
// stops the whole rebuild (see checkKey). That makes `-rebuild-metadata` require
// `-kek-file` at the worst possible moment, accepted deliberately: the alternative is a
// repair tool that records key material nobody verified.
//
// Running it twice, or from two operators at once, converges rather than aborting;
// CreateVolume is idempotent, and no capacity bound is passed because these volumes already
// occupy their hosts.
//
// KeyChecker is the KMS operation a rebuild needs: prove that each descriptor's wrapped DEK
// really is this volume's before the catalog records it. Read-only in effect — the DEK it
// recovers is discarded.
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
		// The same geometry rules provisioning applies: a descriptor is an input read out
		// of a bucket, and a bad one recorded is a row nothing can ever serve.
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
		// The epoch is taken from the published history and raised past it, not from the
		// descriptor: descriptor.json's current_epoch is written at create and at clone and
		// updated by nothing, so a volume fenced up to epoch 4 has a descriptor that still
		// reads 1, and a rebuild that believed it would hand every host that ever held the
		// volume a token this catalog accepts. Raised *past* the floor rather than restored to
		// it, because coming back at the same number leaves every predecessor's token live.
		CurrentEpoch: epochFloor(ctx, store, d) + 1,
		// And what the bucket's HEAD names, so the blank-disk refusal is armed the moment
		// the catalog comes back rather than after each volume's next publish. A HEAD that
		// does not answer leaves it empty, which under-claims: the rebuild cannot tell a
		// volume that never published from one whose HEAD is the object that went missing,
		// and refusing on that guess would strand a fleet on the day it lost its catalog.
		HeadCommitID: headCommit(ctx, store, d.VolumeID),
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
// The structural checks cannot see this: descriptor.json is digest-framed and a digest is
// not authentication, and a swap that moves only dek_wrapped/dek_key_id leaves volume_id
// honest. The KEK is the only witness the bucket-writing adversary does not hold.
//
// The whole rebuild fails rather than skipping the volume: a catalog rebuilt from *some* of
// the bucket leaves the operator unable to say which volumes are missing, and here the
// alternative is worse than missing — recording key material nobody verified.
//
// The KEK-ID mismatch is reported separately and first, because an operator who passed the
// wrong -kek-file must not be told their bucket was forged.
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

// headCommit is what the bucket's HEAD names, or empty when it will not answer.
func headCommit(ctx context.Context, store objectstore.Store, volumeID string) string {
	head, err := commit.ReadHeadCommit(ctx, store, volumeID)
	if err != nil {
		return ""
	}
	return head
}

// epochFloor is the highest epoch this volume can be shown to have used: the greater of
// what its descriptor last recorded and what the newest published commit says.
func epochFloor(ctx context.Context, store objectstore.Store, d descriptor.Descriptor) int64 {
	// Three sources and the highest wins, because a fencing token may only go up: the
	// recorded epoch (the authority, written on every grant), the descriptor's create-time
	// number, and the newest commit's, which covers a bucket restored without its epoch
	// object. A source that will not answer contributes nothing rather than lowering the
	// answer.
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
