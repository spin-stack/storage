// Package format defines the on-disk / on-S3 WAL formats (doc §14.1 record header,
// §14.2 object header). It is a data-loss zone: the layout is locked by golden-bytes
// tests and versioned with a magic + version so a future v3 can never silently
// co-mingle with v2 (§27). Byte order is little-endian; integrity is CRC32C
// (Castagnoli) over the pre-CRC header bytes. Crypto fields (KeyID, AuthTag) are
// present but zero in Phase 04; Phase 05 turns them on without a format change.
//
// Header sizes are 104 bytes (not the "96" in the doc — see ADR-0005 / DEV-0001).
package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// FormatVersion is the current WAL format major (magic-tagged, §27).
const FormatVersion uint16 = 2

// Header sizes are 104 bytes (ADR-0005 / DEV-0001; the doc's "96" is an erratum).
const (
	RecordHeaderSize int = 104
	ObjectHeaderSize int = 104
)

// Magic tags per §14.1/§14.2.
var (
	MagicRecord = [4]byte{'V', 'W', '0', '2'}
	MagicObject = [4]byte{'W', 'B', '0', '2'}
)

// RecordType classifies a WAL record.
type RecordType uint8

const (
	RecordWrite       RecordType = 0
	RecordDiscard     RecordType = 1
	RecordWriteZeroes RecordType = 2
)

// Flags bits (§14.1).
const (
	FlagFUA         uint32 = 1 << 0
	FlagPartOfFlush uint32 = 1 << 1
)

// Decode errors.
var (
	ErrBadMagic     = errors.New("wal/format: bad magic")
	ErrBadVersion   = errors.New("wal/format: unsupported version")
	ErrBadHeaderLen = errors.New("wal/format: unexpected header length")
	ErrHeaderCRC    = errors.New("wal/format: header CRC mismatch")
	ErrShortBuf     = errors.New("wal/format: buffer too short")
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// RecordHeader is the fixed 104-byte WAL record header (§14.1).
type RecordHeader struct {
	RecordType    RecordType
	VolumeID      [16]byte
	Epoch         uint64
	Sequence      uint64
	OperationID   [16]byte
	Offset        uint64
	Length        uint32
	KeyID         uint32
	PayloadCRC32C uint32
	Flags         uint32
	AuthTag       [16]byte
}

// MarshalBinary encodes the header into exactly RecordHeaderSize bytes, computing
// HeaderCRC32C over bytes [0,100).
func (h RecordHeader) MarshalBinary() ([]byte, error) {
	b := make([]byte, RecordHeaderSize)
	copy(b[0:4], MagicRecord[:])
	binary.LittleEndian.PutUint16(b[4:6], FormatVersion)
	binary.LittleEndian.PutUint16(b[6:8], uint16(RecordHeaderSize))
	b[8] = byte(h.RecordType)
	// b[9:12] Reserved0 stays zero
	copy(b[12:28], h.VolumeID[:])
	binary.LittleEndian.PutUint64(b[28:36], h.Epoch)
	binary.LittleEndian.PutUint64(b[36:44], h.Sequence)
	copy(b[44:60], h.OperationID[:])
	binary.LittleEndian.PutUint64(b[60:68], h.Offset)
	binary.LittleEndian.PutUint32(b[68:72], h.Length)
	binary.LittleEndian.PutUint32(b[72:76], h.KeyID)
	binary.LittleEndian.PutUint32(b[76:80], h.PayloadCRC32C)
	binary.LittleEndian.PutUint32(b[80:84], h.Flags)
	copy(b[84:100], h.AuthTag[:])
	binary.LittleEndian.PutUint32(b[100:104], crc32.Checksum(b[0:100], crcTable))
	return b, nil
}

// UnmarshalRecordHeader decodes and validates a record header from b.
func UnmarshalRecordHeader(b []byte) (RecordHeader, error) {
	var h RecordHeader
	if len(b) < RecordHeaderSize {
		return h, ErrShortBuf
	}
	if [4]byte(b[0:4]) != MagicRecord {
		return h, ErrBadMagic
	}
	if binary.LittleEndian.Uint16(b[4:6]) != FormatVersion {
		return h, ErrBadVersion
	}
	if binary.LittleEndian.Uint16(b[6:8]) != uint16(RecordHeaderSize) {
		return h, ErrBadHeaderLen
	}
	if got := binary.LittleEndian.Uint32(b[100:104]); got != crc32.Checksum(b[0:100], crcTable) {
		return h, ErrHeaderCRC
	}
	h.RecordType = RecordType(b[8])
	copy(h.VolumeID[:], b[12:28])
	h.Epoch = binary.LittleEndian.Uint64(b[28:36])
	h.Sequence = binary.LittleEndian.Uint64(b[36:44])
	copy(h.OperationID[:], b[44:60])
	h.Offset = binary.LittleEndian.Uint64(b[60:68])
	h.Length = binary.LittleEndian.Uint32(b[68:72])
	h.KeyID = binary.LittleEndian.Uint32(b[72:76])
	h.PayloadCRC32C = binary.LittleEndian.Uint32(b[76:80])
	h.Flags = binary.LittleEndian.Uint32(b[80:84])
	copy(h.AuthTag[:], b[84:100])
	return h, nil
}

// ObjectHeader is the fixed 104-byte WAL object header (§14.2).
type ObjectHeader struct {
	VolumeID      [16]byte
	Epoch         uint64
	FirstSequence uint64
	LastSequence  uint64
	RecordCount   uint32
	KeyID         uint32
	PayloadLength uint64
	PayloadSHA256 [32]byte
}

// MarshalBinary encodes the object header into ObjectHeaderSize bytes, with
// HeaderCRC32C over bytes [0,96).
func (h ObjectHeader) MarshalBinary() ([]byte, error) {
	b := make([]byte, ObjectHeaderSize)
	copy(b[0:4], MagicObject[:])
	binary.LittleEndian.PutUint16(b[4:6], FormatVersion)
	binary.LittleEndian.PutUint16(b[6:8], uint16(ObjectHeaderSize))
	copy(b[8:24], h.VolumeID[:])
	binary.LittleEndian.PutUint64(b[24:32], h.Epoch)
	binary.LittleEndian.PutUint64(b[32:40], h.FirstSequence)
	binary.LittleEndian.PutUint64(b[40:48], h.LastSequence)
	binary.LittleEndian.PutUint32(b[48:52], h.RecordCount)
	binary.LittleEndian.PutUint32(b[52:56], h.KeyID)
	binary.LittleEndian.PutUint64(b[56:64], h.PayloadLength)
	copy(b[64:96], h.PayloadSHA256[:])
	binary.LittleEndian.PutUint32(b[96:100], crc32.Checksum(b[0:96], crcTable))
	// b[100:104] Reserved stays zero (not CRC-covered)
	return b, nil
}

// UnmarshalObjectHeader decodes and validates an object header from b.
func UnmarshalObjectHeader(b []byte) (ObjectHeader, error) {
	var h ObjectHeader
	if len(b) < ObjectHeaderSize {
		return h, ErrShortBuf
	}
	if [4]byte(b[0:4]) != MagicObject {
		return h, ErrBadMagic
	}
	if binary.LittleEndian.Uint16(b[4:6]) != FormatVersion {
		return h, ErrBadVersion
	}
	if binary.LittleEndian.Uint16(b[6:8]) != uint16(ObjectHeaderSize) {
		return h, ErrBadHeaderLen
	}
	if got := binary.LittleEndian.Uint32(b[96:100]); got != crc32.Checksum(b[0:96], crcTable) {
		return h, ErrHeaderCRC
	}
	copy(h.VolumeID[:], b[8:24])
	h.Epoch = binary.LittleEndian.Uint64(b[24:32])
	h.FirstSequence = binary.LittleEndian.Uint64(b[32:40])
	h.LastSequence = binary.LittleEndian.Uint64(b[40:48])
	h.RecordCount = binary.LittleEndian.Uint32(b[48:52])
	h.KeyID = binary.LittleEndian.Uint32(b[52:56])
	h.PayloadLength = binary.LittleEndian.Uint64(b[56:64])
	copy(h.PayloadSHA256[:], b[64:96])
	return h, nil
}

// PayloadCRC computes the CRC32C used in RecordHeader.PayloadCRC32C (over the
// plaintext payload, §14.1).
func PayloadCRC(payload []byte) uint32 { return crc32.Checksum(payload, crcTable) }

// String aids debugging.
func (t RecordType) String() string {
	switch t {
	case RecordWrite:
		return "WRITE"
	case RecordDiscard:
		return "DISCARD"
	case RecordWriteZeroes:
		return "WRITE_ZEROES"
	default:
		return fmt.Sprintf("RecordType(%d)", uint8(t))
	}
}
