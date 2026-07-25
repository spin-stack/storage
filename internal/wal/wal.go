// Package wal implements the local write-ahead log: serializing records to an
// append-only file and replaying them back to reconstruct volume state. Its
// correctness is the correctness of the data path (§14). Replay is total and safe
// (INV-05, §25.2): a torn tail (crash mid-append) yields the intact prefix cleanly,
// and any bit corruption is caught by CRC — never applied silently.
package wal

import (
	"errors"

	"github.com/spin-stack/storage/internal/wal/format"
)

// Record is a decoded WAL record. For WRITE, Payload holds the data (ciphertext if
// KeyID != 0) and Length == len(plaintext). For DISCARD/WRITE_ZEROES, Payload is nil
// and Length is the extent length (§14.1). KeyID/PayloadCRC/AuthTag mirror the
// header so an encrypted record can be decrypted and verified after replay.
type Record struct {
	Type       format.RecordType
	Epoch      uint64
	Sequence   uint64
	Offset     uint64
	Length     uint32
	Flags      uint32
	KeyID      uint32
	PayloadCRC uint32
	AuthTag    [16]byte
	Payload    []byte
}

// Encode serializes a single record.
func (r Record) Encode() ([]byte, error) {
	h := format.RecordHeader{
		RecordType: r.Type,
		Epoch:      r.Epoch,
		Sequence:   r.Sequence,
		Offset:     r.Offset,
		Length:     r.Length,
		Flags:      r.Flags,
	}
	if r.Type == format.RecordWrite {
		return format.EncodeRecord(h, r.Payload)
	}
	// DISCARD / WRITE_ZEROES: header-only; Length carries the extent.
	return format.EncodeRecord(h, nil)
}

// Serialize concatenates the encoding of every record, as written to the local WAL.
func Serialize(records []Record) ([]byte, error) {
	var out []byte
	for _, r := range records {
		b, err := r.Encode()
		if err != nil {
			return nil, err
		}
		out = append(out, b...)
	}
	return out, nil
}

// Replay decodes records from b in order. It returns the intact prefix and:
//   - nil error for a clean end or a torn tail (truncation mid-record), which is
//     the normal crash-during-append case;
//   - a decode error (CRC/magic/version) the moment corruption is detected — the
//     returned records are the verified prefix before it.
//
// It never returns a record whose content differs from what was written without an
// error (INV-05).
func Replay(b []byte) ([]Record, error) {
	var records []Record
	for off := 0; off < len(b); {
		h, payload, n, err := format.DecodeRecord(b[off:])
		if err != nil {
			if errors.Is(err, format.ErrShortBuf) {
				// Torn tail: the last record was not fully written. Clean stop.
				return records, nil
			}
			// CRC / magic / version mismatch: corruption. Surface it.
			return records, err
		}
		rec := Record{
			Type:       h.RecordType,
			Epoch:      h.Epoch,
			Sequence:   h.Sequence,
			Offset:     h.Offset,
			Length:     h.Length,
			Flags:      h.Flags,
			KeyID:      h.KeyID,
			PayloadCRC: h.PayloadCRC32C,
			AuthTag:    h.AuthTag,
		}
		if len(payload) > 0 {
			rec.Payload = append([]byte(nil), payload...)
		}
		records = append(records, rec)
		off += n
	}
	return records, nil
}

// State is a sparse byte-addressed model of a volume, used to check that replay
// reconstructs the same logical state (a read of an unwritten/discarded byte is 0,
// §14.6).
type State struct {
	data map[uint64]byte
}

// NewState returns an empty state.
func NewState() *State { return &State{data: map[uint64]byte{}} }

// Apply mutates the state by one record.
func (s *State) Apply(r Record) {
	switch r.Type {
	case format.RecordWrite:
		for i, b := range r.Payload {
			off := r.Offset + uint64(i)
			if b == 0 {
				delete(s.data, off) // 0 is the default; keep the map sparse
			} else {
				s.data[off] = b
			}
		}
	case format.RecordDiscard, format.RecordWriteZeroes:
		for i := uint64(0); i < uint64(r.Length); i++ {
			delete(s.data, r.Offset+i) // reads as zero
		}
	}
}

// ApplyAll folds a record sequence into a fresh state.
func ApplyAll(records []Record) *State {
	s := NewState()
	for _, r := range records {
		s.Apply(r)
	}
	return s
}

// Equal reports whether two states have identical logical content (missing == 0).
func (s *State) Equal(other *State) bool {
	if len(s.data) != len(other.data) {
		return false
	}
	for off, b := range s.data {
		if other.data[off] != b {
			return false
		}
	}
	return true
}
