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
	// HostID is the host that will serve it. Placement is deliberately explicit: the
	// placement package chooses when there is a fleet to choose from, and a spec that
	// names no host is one no Agent will ever see (GetDesiredState filters by
	// primary_host_id).
	HostID string
	// Durability selects the FLUSH ACK contract (§14.8).
	Durability lifecycle.Durability
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
	WrapDEK(r io.Reader, dek crypto.DEK) ([]byte, error)
}

// Provisioner creates volumes: the act that has never existed in this tree, which is
// why GetDesiredState has always returned an empty list.
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
// Order is the row first, then the descriptor, and it matters. The row is the
// term-guarded step (§7), so a stale Control Plane is refused before anything durable
// is written anywhere. If the descriptor write then fails, what is left is a volume the
// fleet knows about whose object-store anchor is missing — visible, repairable, and
// refused by the Agent when it cannot read the descriptor. The reverse order leaves an
// orphan descriptor under a volume id no row mentions: invisible to every query, and a
// root `gc.Reachable` would walk from forever.
//
// The volume starts at **epoch 1**, not 0. Epoch 0 is the absence of an epoch, and a
// writer cannot address a WAL namespace under it.
//
// It deliberately does not write the epoch object (§12.4). `recovery.VerifyPublisher`
// treats an absent epoch object as "nothing has claimed this volume yet" and permits
// the publisher; an object initialised to 0 while the row says 1 makes every checkpoint
// fail with ErrEpochChanged instead. The epoch object belongs to the first promotion,
// which is what §12.4 says it is for.
func (p *Provisioner) Provision(ctx context.Context, term int64, spec VolumeSpec) (ProvisionedVolume, error) {
	if err := spec.validate(); err != nil {
		return ProvisionedVolume{}, err
	}

	// A v7 id (INV-22), and its timestamp prefix means volumes sort by creation.
	volumeID := ids.New().String()

	// KeyID 1 is the first version. Zero is reserved: a WAL record header carrying
	// KeyID 0 means "this payload is plaintext" (§15.2), so a volume provisioned with
	// it would announce ciphertext as clear text.
	dek, err := crypto.GenerateDEK(p.rand, 1)
	if err != nil {
		return ProvisionedVolume{}, fmt.Errorf("generating the volume DEK: %w", err)
	}
	wrapped, err := p.kms.WrapDEK(p.rand, dek)
	if err != nil {
		return ProvisionedVolume{}, fmt.Errorf("wrapping the volume DEK: %w", err)
	}

	vol := metadata.Volume{
		VolumeID:      volumeID,
		SizeBytes:     spec.SizeBytes,
		BlockSize:     spec.BlockSize,
		Durability:    spec.Durability,
		CurrentEpoch:  1,
		State:         lifecycle.VolumeActive,
		PrimaryHostID: spec.HostID,
		DEKWrapped:    wrapped,
		KEKID:         p.kms.KEKID(),
	}
	if err := p.md.CreateVolume(ctx, term, vol, nil); err != nil {
		return ProvisionedVolume{}, fmt.Errorf("creating the volume row: %w", err)
	}

	if err := descriptor.Write(ctx, p.store, descriptor.Descriptor{
		VolumeID:     volumeID,
		SizeBytes:    spec.SizeBytes,
		BlockSize:    spec.BlockSize,
		Durability:   spec.Durability,
		CurrentEpoch: 1,
		KEKID:        p.kms.KEKID(),
		DEKWrapped:   wrapped,
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
	switch {
	case s.HostID == "":
		return errors.New("controlplane: a volume needs a host: GetDesiredState filters on primary_host_id, so an unplaced volume is one no Agent is ever told about")
	case s.SizeBytes <= 0:
		return fmt.Errorf("controlplane: size must be positive, got %d", s.SizeBytes)
	case s.SizeBytes%sectorSize != 0:
		return fmt.Errorf("controlplane: size %d is not a whole number of %d-byte sectors: no Agent can serve it", s.SizeBytes, sectorSize)
	case s.BlockSize <= 0 || int64(s.BlockSize)%sectorSize != 0:
		return fmt.Errorf("controlplane: block size %d is not a multiple of %d", s.BlockSize, sectorSize)
	case !s.Durability.Valid():
		return fmt.Errorf("controlplane: durability %q is not one of the §14.8 modes: it decides what a FLUSH ACK means", s.Durability)
	}
	return nil
}
