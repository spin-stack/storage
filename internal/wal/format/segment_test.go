package format_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/wal/format"
)

func sampleSegmentHeader() format.SegmentHeader {
	var vol [16]byte
	for i := range vol {
		vol[i] = byte(i + 1)
	}
	return format.SegmentHeader{
		VolumeID:      vol,
		Epoch:         7,
		FirstSequence: 42,
		CreatedAtMs:   1_700_000_000_000,
	}
}

func TestSegmentHeaderSizeIs64(t *testing.T) {
	if format.SegmentHeaderSize != 64 {
		t.Fatalf("segment header size is %d, want 64", format.SegmentHeaderSize)
	}
	b, err := sampleSegmentHeader().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != 64 {
		t.Fatalf("marshaled segment header is %d bytes, want 64", len(b))
	}
}

func TestSegmentHeaderRoundTrip(t *testing.T) {
	h := sampleSegmentHeader()
	b, err := h.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	got, err := format.UnmarshalSegmentHeader(b)
	if err != nil {
		t.Fatal(err)
	}
	if got != h {
		t.Fatalf("round trip changed the header:\n got %+v\nwant %+v", got, h)
	}
}

// TestSegmentGoldenBytes locks the exact wire layout of the CRC-covered prefix
// (bytes [0,60)). The trailing HeaderCRC32C is a pure function of those bytes, so
// locking the prefix locks the format. If this fails, the on-disk format changed —
// that requires a version bump and an ADR, never a silent edit.
func TestSegmentGoldenBytes(t *testing.T) {
	b, err := sampleSegmentHeader().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	want := "" +
		"57533031" + // Magic "WS01"
		"0100" + // Version=1
		"4000" + // HeaderLen=64
		"0102030405060708090a0b0c0d0e0f10" + // VolumeID = 1..16
		"0700000000000000" + // Epoch=7
		"2a00000000000000" + // FirstSequence=42
		"0068e5cf8b010000" + // CreatedAtMs=1700000000000
		"000000000000000000000000" // Reserved (zero, CRC-covered)

	if len(want)/2 != 60 {
		t.Fatalf("golden prefix definition is %d bytes, want 60", len(want)/2)
	}
	if got := hex.EncodeToString(b[:60]); got != want {
		t.Fatalf("golden bytes changed (format drift!):\n got %s\nwant %s", got, want)
	}
	if bytes.Equal(b[60:64], []byte{0, 0, 0, 0}) {
		t.Fatal("header CRC should be non-zero")
	}
	if _, err := format.UnmarshalSegmentHeader(b); err != nil {
		t.Fatalf("golden bytes must decode: %v", err)
	}
}

// TestSegmentHeaderRejects is the decoder's half of the format lock: every field that
// identifies the container has to be checked, or a file from another format, another
// version, or a bad sector is read as a segment.
func TestSegmentHeaderRejects(t *testing.T) {
	good, err := sampleSegmentHeader().MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		mut  func(b []byte) []byte
		want error
	}{
		{"a buffer shorter than a header", func(b []byte) []byte { return b[:63] }, format.ErrShortBuf},
		{"another format's magic", func(b []byte) []byte { b[0] = 'X'; return b }, format.ErrBadMagic},
		{"a version this build does not know", func(b []byte) []byte { b[4] = 9; return b }, format.ErrBadVersion},
		{"a header length that is not 64", func(b []byte) []byte { b[6] = 0x20; return b }, format.ErrBadHeaderLen},
		{"a bit flipped in the volume id", func(b []byte) []byte { b[8] ^= 1; return b }, format.ErrHeaderCRC},
		{"a bit flipped in the reserved space", func(b []byte) []byte { b[50] ^= 1; return b }, format.ErrHeaderCRC},
		{"a bit flipped in the CRC itself", func(b []byte) []byte { b[60] ^= 1; return b }, format.ErrHeaderCRC},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := tc.mut(append([]byte(nil), good...))
			if _, err := format.UnmarshalSegmentHeader(b); !errors.Is(err, tc.want) {
				t.Fatalf("decode returned %v, want %v", err, tc.want)
			}
		})
	}
}
