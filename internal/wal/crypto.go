package wal

import (
	"errors"
	"fmt"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/wal/format"
)

// ErrPlaintextCRC is returned when a decrypted payload fails its plaintext CRC32C
// (§14.1: the CRC is over the cleartext, verified after decryption).
var ErrPlaintextCRC = errors.New("wal: decrypted payload CRC mismatch")

// ErrUnversionedKey is returned when a DEK carries KeyID 0. KeyID 0 is not a key
// version: it is the on-disk marker for "this payload is cleartext" (§14.1), which
// DecodeRecord and Decrypt both act on. A DEK with KeyID 0 would seal the payload
// and then label it plaintext — the record's CRC would be over the cleartext while
// its bytes are ciphertext, so the local WAL would stop replaying and every object
// built from it would fail recovery's integrity check, all after the FLUSH was ACKed
// as durable. Refused at the write instead.
var ErrUnversionedKey = errors.New("wal: KeyID 0 is reserved for plaintext records, it is not a DEK version")

// ErrUnknownKeyID is returned when a replayed record was sealed with a key version
// this volume does not hold. It is deliberately distinct from a GCM authentication
// failure: rotation (§15.1) leaves history sealed under older versions, and "fetch
// key version 3" is a recoverable answer where "tamper detected" aborts the recovery
// of every remaining epoch.
var ErrUnknownKeyID = errors.New("wal: record sealed with a key version this volume does not hold")

// Encryption binds a volume's DEK to the WAL so the write path can seal payloads
// (§15). When a Log has no Encryption, payloads are written in the clear (Phase 04
// behavior / dev without KMS).
type Encryption struct {
	DEK      crypto.DEK
	VolumeID [16]byte
}

// NewEncryption binds a versioned DEK to a volume. It is the checked way to build an
// Encryption: KeyID 0 is refused here rather than at recovery time.
func NewEncryption(dek crypto.DEK, volumeID [16]byte) (*Encryption, error) {
	if dek.KeyID == 0 {
		return nil, fmt.Errorf("%w: volume %s", ErrUnversionedKey, format.UUIDString(volumeID))
	}
	return &Encryption{DEK: dek, VolumeID: volumeID}, nil
}

// encodeWrite builds the on-disk bytes for a WRITE. With encryption the payload is
// sealed (ciphertext on disk), the plaintext CRC and GCM tag go in the header, and
// KeyID records the DEK version (§14.1, §15.1).
func (e *Encryption) encodeWrite(epoch, seq, offset uint64, flags uint32, plaintext []byte) ([]byte, error) {
	// A struct literal can carry an unversioned DEK past NewEncryption; this is the
	// last point before the bytes exist.
	if e.DEK.KeyID == 0 {
		return nil, fmt.Errorf("%w: sequence %d", ErrUnversionedKey, seq)
	}
	ct, tag, err := e.DEK.Seal(e.VolumeID, epoch, seq, plaintext)
	if err != nil {
		return nil, err
	}
	h := format.RecordHeader{
		RecordType:    format.RecordWrite,
		VolumeID:      e.VolumeID,
		Epoch:         epoch,
		Sequence:      seq,
		Offset:        offset,
		Length:        uint32(len(plaintext)),
		Flags:         flags,
		KeyID:         e.DEK.KeyID,
		PayloadCRC32C: format.PayloadCRC(plaintext),
		AuthTag:       tag,
	}
	return format.EncodeRecordRaw(h, ct)
}

// Decrypt returns the plaintext of a replayed record. Plaintext records (KeyID==0)
// are returned as-is; encrypted records are GCM-opened and their plaintext CRC is
// verified. Any tamper fails closed.
//
// A record sealed with a key version this volume does not hold is reported as such
// rather than handed to Open, whose failure is indistinguishable from a bit flip. The
// difference matters at recovery: "fetch DEK version 3" is actionable, while "tamper
// detected" aborts the volume.
func (e *Encryption) Decrypt(rec Record) ([]byte, error) {
	if rec.KeyID == 0 {
		return rec.Payload, nil
	}
	if rec.KeyID != e.DEK.KeyID {
		return nil, fmt.Errorf("%w: sequence %d is sealed with key version %d, this volume holds %d",
			ErrUnknownKeyID, rec.Sequence, rec.KeyID, e.DEK.KeyID)
	}
	pt, err := e.DEK.Open(e.VolumeID, rec.Epoch, rec.Sequence, rec.Payload, rec.AuthTag)
	if err != nil {
		return nil, err
	}
	if format.PayloadCRC(pt) != rec.PayloadCRC {
		return nil, ErrPlaintextCRC
	}
	return pt, nil
}
