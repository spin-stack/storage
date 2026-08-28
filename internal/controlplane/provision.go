package controlplane

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// sectorSize is the unit a guest addresses the device in. A capacity that is not a
// whole number of them is one blockdev.New refuses, so it is refused here instead —
// where the operator who typed the number is still watching.
const sectorSize = 512

// VolumeSpec is what an operator asks for.
type VolumeSpec struct {
	// SizeBytes is the capacity the guest sees. A whole number of 512-byte sectors.
	SizeBytes int64
	// BlockSize is the logical block size reported to the guest.
	BlockSize int32
	// RPOTargetSeconds is how far behind the object store this volume may fall before
	// its host commits on age rather than size (v6 §11). Zero means no age trigger, and
	// it is the default deliberately: §11 forbids choosing a target instead of measuring
	// one, and zero is the only value that cannot silently under-deliver a promise.
	RPOTargetSeconds int32
	// HostID is the host that will serve it. Placement is deliberately explicit: the
	// placement package chooses when there is a fleet to choose from, and a spec that
	// names no host is one no Agent will ever see (GetDesiredState filters by
	// primary_host_id).
	HostID string
}

// ProvisionedVolume is what provisioning produced.
type ProvisionedVolume struct {
	VolumeID string
	// KeyID is the DEK's version. It is returned because it is the one thing the
	// caller cannot recover from the row: unwrapping binds the version as GCM AAD, so
	// a holder of the wrapped bytes who does not know the version cannot open them.
	KeyID uint32
}

// KeyWrapper is the KMS operation provisioning needs: seal a fresh DEK under a KEK the
// Control Plane holds and the host will hold too. Narrow on purpose — provisioning has
// no business unwrapping anything.
type KeyWrapper interface {
	KEKID() string
	WrapDEK(r io.Reader, dek crypto.DEK, volumeID [16]byte) ([]byte, error)
}

// Provisioner creates volumes.
type Provisioner struct {
	md    metadata.Store
	store objectstore.Store
	kms   KeyWrapper
	rand  io.Reader
}

// NewProvisioner returns a Provisioner. rand is the source of key material and volume
// ids; it is injected so a DST run is reproducible from its seed (INV-02) and so the
// production caller passes crypto/rand explicitly rather than by default.
func NewProvisioner(md metadata.Store, store objectstore.Store, kms KeyWrapper, rand io.Reader) *Provisioner {
	return &Provisioner{md: md, store: store, kms: kms, rand: rand}
}

// Provision creates a volume: a fresh DEK wrapped under the deployment's KEK, a
// term-guarded row naming the host that will serve it, and a descriptor in the object
// store.
//
// The row first, then the descriptor. The row is the term-guarded step (§7), so a stale
// Control Plane is refused before anything durable is written anywhere; if the descriptor
// write then fails, what is left is a volume the fleet knows about with no object-store
// anchor — visible, repairable, refused by the Agent. The reverse order leaves an orphan
// descriptor under a volume id no row mentions: invisible to every query, and a root a
// reachability sweep would walk from forever.
//
// Epoch **1**, not 0: epoch 0 is the absence of an epoch and a writer cannot address a WAL
// namespace under it. Creating a volume does not grant a fresh epoch and does not need to —
// a fresh v7 id has no WAL directory on any host at any epoch — and the 1 written here is
// the same 1 the descriptor carries, which is what -rebuild-metadata restores the row from.
func (p *Provisioner) Provision(ctx context.Context, term int64, spec VolumeSpec) (ProvisionedVolume, error) {
	if err := spec.validate(); err != nil {
		return ProvisionedVolume{}, err
	}

	// A v7 id (INV-22). The uuid itself is kept, not just its string: the wrap below binds
	// the volume's 16 raw bytes.
	u := ids.New()
	volumeID := u.String()

	// KeyID 1 is the first version. Zero is reserved: a WAL record header carrying
	// KeyID 0 means "this payload is plaintext" (§15.2), so a volume provisioned with
	// it would announce ciphertext as clear text.
	dek, err := crypto.GenerateDEK(p.rand, 1)
	if err != nil {
		return ProvisionedVolume{}, fmt.Errorf("generating the volume DEK: %w", err)
	}
	// Bound to this volume, so that a wrapped DEK PUT into another volume's descriptor
	// fails to unwrap instead of silently re-keying it (crypto.wrapAAD).
	wrapped, err := p.kms.WrapDEK(p.rand, dek, [16]byte(u))
	if err != nil {
		return ProvisionedVolume{}, fmt.Errorf("wrapping the volume DEK: %w", err)
	}

	vol := metadata.Volume{
		VolumeID:         volumeID,
		SizeBytes:        spec.SizeBytes,
		BlockSize:        spec.BlockSize,
		RPOTargetSeconds: spec.RPOTargetSeconds,
		CurrentEpoch:     1,
		State:            lifecycle.VolumeActive,
		PrimaryHostID:    spec.HostID,
		DEKWrapped:       wrapped,
		KEKID:            p.kms.KEKID(),
		DEKKeyID:         dek.KeyID,
	}
	if err := p.md.CreateVolume(ctx, term, vol, nil); err != nil {
		return ProvisionedVolume{}, fmt.Errorf("creating the volume row: %w", err)
	}

	if err := descriptor.Write(ctx, p.store, descriptor.Descriptor{
		VolumeID:     volumeID,
		SizeBytes:    spec.SizeBytes,
		BlockSize:    spec.BlockSize,
		CurrentEpoch: 1,
		KEKID:        p.kms.KEKID(),
		DEKWrapped:   wrapped,
		DEKKeyID:     dek.KeyID,
	}); err != nil {
		// Reported, not rolled back. Deleting the row here would need a term-guarded
		// delete that does not exist, and would turn one repairable inconsistency into
		// two writes that can each fail. The descriptor is idempotent: the fix is to
		// run provisioning's repair, or simply to write it again.
		return ProvisionedVolume{VolumeID: volumeID, KeyID: dek.KeyID},
			fmt.Errorf("writing the descriptor for %s (the row exists; rebuild-metadata cannot see it until this succeeds): %w", volumeID, err)
	}

	return ProvisionedVolume{VolumeID: volumeID, KeyID: dek.KeyID}, nil
}

func (s VolumeSpec) validate() error {
	if s.HostID == "" {
		return errors.New("controlplane: a volume needs a host: GetDesiredState filters on primary_host_id, so an unplaced volume is one no Agent is ever told about")
	}
	// A negative RPO is refused here rather than clamped. The column has the same CHECK,
	// and the difference between the two is which caller learns: a clamp turns "minus one
	// hour" into a volume with no age trigger at all, silently, and the operator who
	// typed it goes on believing they bought one.
	if s.RPOTargetSeconds < 0 {
		return fmt.Errorf("controlplane: an RPO target of %ds is not a duration", s.RPOTargetSeconds)
	}
	return geometry(s.SizeBytes, s.BlockSize)
}

// geometry is the rule about a volume's shape, split out of validate so that
// rebuild-metadata — which reads descriptors out of a bucket and so takes this geometry as
// *input* — applies it too. It used to record whatever it found: a zero-byte volume or an
// unaddressable block size is a catalog row nothing can serve, created by the one command an
// operator runs when the catalog is already gone.
func geometry(sizeBytes int64, blockSize int32) error {
	switch {
	case sizeBytes <= 0:
		return fmt.Errorf("controlplane: size must be positive, got %d", sizeBytes)
	case sizeBytes%sectorSize != 0:
		return fmt.Errorf("controlplane: size %d is not a whole number of %d-byte sectors: no Agent can serve it", sizeBytes, sectorSize)
	case blockSize <= 0 || int64(blockSize)%sectorSize != 0:
		return fmt.Errorf("controlplane: block size %d is not a multiple of %d", blockSize, sectorSize)
	}
	return nil
}
