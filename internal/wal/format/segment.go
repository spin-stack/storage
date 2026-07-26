package format

import (
	"encoding/binary"
	"hash/crc32"
)

// SegmentHeaderSize is the fixed size of a WAL segment header, in bytes.
const SegmentHeaderSize int = 64

// SegmentVersion is the segment header's format major. It is deliberately its own
// number rather than FormatVersion: the segment file is a container for records, and
// the two evolve independently — a record layout change does not move the container,
// and the magic tags them apart anyway ("WS01" vs "VW02").
const SegmentVersion uint16 = 1

// magicSegment tags a WAL segment file (unexported: internal to the codec).
var magicSegment = [4]byte{'W', 'S', '0', '1'}

// SegmentHeader is the fixed 64-byte header every WAL segment file opens with.
//
// LastSequence and RecordCount are deliberately absent. Both would have to be written
// back into the header when the segment is sealed, which turns an otherwise immutable
// file into a mutable one and adds a torn-write case for no gain: replay establishes
// both, and the authority for "what is durable" is the object store, not this file
// (§5.8).
//
// VolumeID and Epoch are not redundant with the per-record binding (§14.1). A
// directory restored under the wrong volume or the wrong epoch is caught here, before
// a single record is decoded — and the path (wal/<vol>/<epoch>/) can disagree with the
// header, which is exactly the case a restored backup produces.
type SegmentHeader struct {
	// VolumeID binds the file to the volume that created it.
	VolumeID [16]byte
	// Epoch binds it to the writer generation that created it.
	Epoch uint64
	// FirstSequence is the sequence of the first record the segment carries. It is
	// also the file's name (zero-padded), so the directory listing is the index.
	FirstSequence uint64
	// CreatedAtMs is the injected clock's wall time at creation, in milliseconds. It
	// is diagnostic only — nothing branches on it, because wall time may be skewed
	// (§12.1).
	CreatedAtMs uint64
}

// MarshalBinary encodes the header into exactly SegmentHeaderSize bytes, computing
// HeaderCRC32C over bytes [0,60).
func (h SegmentHeader) MarshalBinary() ([]byte, error) {
	b := make([]byte, SegmentHeaderSize)
	copy(b[0:4], magicSegment[:])
	binary.LittleEndian.PutUint16(b[4:6], SegmentVersion)
	binary.LittleEndian.PutUint16(b[6:8], uint16(SegmentHeaderSize))
	copy(b[8:24], h.VolumeID[:])
	binary.LittleEndian.PutUint64(b[24:32], h.Epoch)
	binary.LittleEndian.PutUint64(b[32:40], h.FirstSequence)
	binary.LittleEndian.PutUint64(b[40:48], h.CreatedAtMs)
	// b[48:60] Reserved stays zero (CRC-covered, so a future field cannot be
	// introduced silently).
	binary.LittleEndian.PutUint32(b[60:64], crc32.Checksum(b[0:60], crcTable))
	return b, nil
}

// UnmarshalSegmentHeader decodes and validates a segment header from b.
func UnmarshalSegmentHeader(b []byte) (SegmentHeader, error) {
	var h SegmentHeader
	if len(b) < SegmentHeaderSize {
		return h, ErrShortBuf
	}
	if [4]byte(b[0:4]) != magicSegment {
		return h, ErrBadMagic
	}
	if binary.LittleEndian.Uint16(b[4:6]) != SegmentVersion {
		return h, ErrBadVersion
	}
	if binary.LittleEndian.Uint16(b[6:8]) != uint16(SegmentHeaderSize) {
		return h, ErrBadHeaderLen
	}
	if got := binary.LittleEndian.Uint32(b[60:64]); got != crc32.Checksum(b[0:60], crcTable) {
		return h, ErrHeaderCRC
	}
	copy(h.VolumeID[:], b[8:24])
	h.Epoch = binary.LittleEndian.Uint64(b[24:32])
	h.FirstSequence = binary.LittleEndian.Uint64(b[32:40])
	h.CreatedAtMs = binary.LittleEndian.Uint64(b[40:48])
	return h, nil
}
