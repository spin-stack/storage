package format

import (
	"errors"
	"fmt"
)

// ErrPayloadCRC is returned when a decoded WRITE payload fails its CRC32C check.
var ErrPayloadCRC = errors.New("wal/format: payload CRC mismatch")

// EncodeRecord serializes a record (header + payload) for the local WAL. For
// WRITE, Length and PayloadCRC32C are derived from payload. For DISCARD and
// WRITE_ZEROES the on-disk record is header-only (no payload); Length carries the
// extent length and must be set by the caller (§14.1).
func EncodeRecord(h RecordHeader, payload []byte) ([]byte, error) {
	switch h.RecordType {
	case RecordWrite:
		h.Length = uint32(len(payload))
		h.PayloadCRC32C = PayloadCRC(payload)
	case RecordDiscard, RecordWriteZeroes:
		if len(payload) != 0 {
			return nil, fmt.Errorf("wal/format: %s records carry no payload", h.RecordType)
		}
		h.PayloadCRC32C = 0
	default:
		return nil, fmt.Errorf("wal/format: unknown record type %d", uint8(h.RecordType))
	}
	hb, err := h.MarshalBinary()
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 {
		return hb, nil
	}
	return append(hb, payload...), nil
}

// EncodeRecordRaw marshals h (trusting its Length, PayloadCRC32C, KeyID, and
// AuthTag as already set) followed by payload, without recomputing anything. The
// encrypted write path uses this: h carries the plaintext CRC and the GCM tag while
// payload is the ciphertext (§14.1).
func EncodeRecordRaw(h RecordHeader, payload []byte) ([]byte, error) {
	hb, err := h.MarshalBinary()
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 {
		return hb, nil
	}
	return append(hb, payload...), nil
}

// DecodeRecord decodes one record from the front of b, returning the header, the
// payload (nil for header-only records), and the total bytes consumed. It is used
// by the replayer to stream over a WAL file; a truncated tail returns ErrShortBuf
// so the replayer can stop cleanly at the last intact record.
//
// Plaintext records (KeyID == 0) have their payload CRC verified here. Encrypted
// records (KeyID != 0) carry ciphertext whose integrity is verified by the GCM tag
// plus the post-decrypt plaintext CRC in the crypto layer, so DecodeRecord returns
// their ciphertext unverified.
func DecodeRecord(b []byte) (h RecordHeader, payload []byte, n int, err error) {
	h, err = UnmarshalRecordHeader(b)
	if err != nil {
		return RecordHeader{}, nil, 0, err
	}
	payloadLen := 0
	if h.RecordType == RecordWrite {
		payloadLen = int(h.Length)
	}
	total := RecordHeaderSize + payloadLen
	if len(b) < total {
		return RecordHeader{}, nil, 0, ErrShortBuf
	}
	if payloadLen > 0 {
		payload = b[RecordHeaderSize:total]
		if h.KeyID == 0 && PayloadCRC(payload) != h.PayloadCRC32C {
			return RecordHeader{}, nil, 0, ErrPayloadCRC
		}
	}
	return h, payload, total, nil
}
