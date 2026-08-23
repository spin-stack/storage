// Package publisher turns a sealed layer on this host's disk into a published commit.
//
// It exists as its own component because the two halves answer different questions and
// fail for different reasons. internal/qcow owns a volume's local chain and decides
// *when* a layer is ready — that is where v6 §11's triggers and invariants live, and it
// is what spin's runner takes without a Control Plane anywhere near it. This is *what*
// happens then: fetch the volume's key, read the file, and run v6 §9's publish protocol.
// It needs a Control Plane (for the key) and an object store; the chain needs neither.
package publisher

import (
	"context"
	"fmt"
	"io"

	"github.com/google/uuid"

	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/qcow"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// Keys hands over a volume's wrapped key material. It is agent.Loop in production: the
// Control Plane is the only thing that knows a volume's DEK, and it hands it over only
// to the host that owns the volume — so a key that cannot be fetched is this host having
// been fenced, arriving through a different door.
type Keys interface {
	VolumeKeys(ctx context.Context, volumeID string) (agent.VolumeKeys, error)
}

// Files opens a sealed layer for reading. It is the one filesystem verb this needs, and
// it is injected for INV-01 like every other one.
type Files interface {
	Open(path string) (io.ReadCloser, error)
}

// Publisher publishes sealed layers.
type Publisher struct {
	store objectstore.Store
	kms   crypto.KMS
	keys  Keys
	files Files
}

// New returns a Publisher.
func New(store objectstore.Store, kms crypto.KMS, keys Keys, files Files) *Publisher {
	return &Publisher{store: store, kms: kms, keys: keys, files: files}
}

// Publish is qcow's Publisher: it seals the layer with the volume's DEK and runs v6 §9's
// ordering. It is idempotent on the same SealedLayer, because every step under it is.
func (p *Publisher) Publish(ctx context.Context, l qcow.SealedLayer) error {
	enc, err := p.encryption(ctx, l.VolumeID)
	if err != nil {
		return err
	}
	f, err := p.files.Open(l.Path)
	if err != nil {
		return fmt.Errorf("publisher: opening the sealed layer %s: %w", l.Path, err)
	}
	defer func() { _ = f.Close() }()

	m, err := commit.Publish(ctx, p.store, enc, f, commit.Request{
		VolumeID: l.VolumeID, CommitID: l.CommitID, LayerID: l.LayerID,
		Epoch: l.Epoch, VirtualSize: l.VirtualSize, PlainBytes: l.PlainBytes,
	})
	if err != nil {
		return err
	}
	_ = m
	return nil
}

// encryption fetches the volume's key, unwraps it, and binds it to the volume.
//
// Fetched per publish rather than held: agent.Loop caches the wrapped material, and the
// unwrapped key is deliberately not kept anywhere it would outlive the operation that
// needed it. The cost is one unwrap per commit, which is a few microseconds against a
// transfer.
func (p *Publisher) encryption(ctx context.Context, volumeID string) (*crypto.Encryption, error) {
	id, err := uuid.Parse(volumeID)
	if err != nil {
		return nil, fmt.Errorf("publisher: volume id %q: %w", volumeID, err)
	}
	keys, err := p.keys.VolumeKeys(ctx, volumeID)
	if err != nil {
		return nil, fmt.Errorf("publisher: volume %s: %w", volumeID, err)
	}
	dek, err := p.kms.UnwrapDEK(keys.DEKWrapped, keys.DEKKeyID)
	if err != nil {
		return nil, fmt.Errorf("publisher: unwrapping the DEK of volume %s: %w", volumeID, err)
	}
	return crypto.NewEncryption(dek, id)
}
