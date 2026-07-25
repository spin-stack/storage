package wal

import (
	"errors"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/wal/format"
)

// ErrPlaintextCRC is returned when a decrypted payload fails its plaintext CRC32C
// (§14.1: the CRC is over the cleartext, verified after decryption).
var ErrPlaintextCRC = errors.New("wal: decrypted payload CRC mismatch")

// Encryption binds a volume's DEK to the WAL so the write path can seal payloads
// (§15). When a Log has no Encryption, payloads are written in the clear (Phase 04
// behavior / dev without KMS).
type Encryption struct {
	DEK      crypto.DEK
	VolumeID [16]byte
}

// encodeWrite builds the on-disk bytes for a WRITE. With encryption the payload is
// sealed (ciphertext on disk), the plaintext CRC and GCM tag go in the header, and
// KeyID records the DEK version (§14.1, §15.1).
func (e *Encryption) encodeWrite(epoch, seq, offset uint64, flags uint32, plaintext []byte) ([]byte, error) {
	ct, tag, err := e.DEK.Seal(e.VolumeID, epoch, seq, plaintext)
	if err != nil {
		return nil, err
	}
	h := format.RecordHeader{
		RecordType:    format.RecordWrite,
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
func (e *Encryption) Decrypt(rec Record) ([]byte, error) {
	if rec.KeyID == 0 {
		return rec.Payload, nil
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
