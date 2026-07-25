package format_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/wal/format"
)

func sampleRecordHeader() format.RecordHeader {
	var vol, op, tag [16]byte
	for i := range vol {
		vol[i] = byte(i + 1)
		op[i] = byte(i + 100)
	}
	return format.RecordHeader{
		RecordType:    format.RecordWrite,
		VolumeID:      vol,
		Epoch:         7,
		Sequence:      42,
		OperationID:   op,
		Offset:        4096,
		Length:        16,
		KeyID:         3,
		PayloadCRC32C: 0xDEADBEEF,
		Flags:         format.FlagFUA | format.FlagPartOfFlush,
		AuthTag:       tag,
	}
}

func TestRecordHeaderSizeIs104(t *testing.T) {
	if format.RecordHeaderSize != 104 || format.ObjectHeaderSize != 104 {
		t.Fatalf("header sizes must be 104 (ADR-0005): rec=%d obj=%d",
			format.RecordHeaderSize, format.ObjectHeaderSize)
	}
	b, _ := sampleRecordHeader().MarshalBinary()
	if len(b) != 104 {
		t.Fatalf("marshaled record header is %d bytes, want 104", len(b))
	}
}

func TestRecordHeaderRoundTrip(t *testing.T) {
	h := sampleRecordHeader()
	b, err := h.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	got, err := format.UnmarshalRecordHeader(b)
	if err != nil {
		t.Fatal(err)
	}
	// PayloadCRC32C etc. must survive; compare the whole struct.
	if got != h {
		t.Fatalf("round-trip mismatch:\n got %+v\nwant %+v", got, h)
	}
}

func TestObjectHeaderRoundTrip(t *testing.T) {
	var vol [16]byte
	var sha [32]byte
	for i := range vol {
		vol[i] = byte(i)
	}
	for i := range sha {
		sha[i] = byte(255 - i)
	}
	h := format.ObjectHeader{
		VolumeID: vol, Epoch: 9, FirstSequence: 100, LastSequence: 200,
		RecordCount: 12, KeyID: 1, PayloadLength: 8 << 20, PayloadSHA256: sha,
	}
	b, err := h.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != 104 {
		t.Fatalf("object header %d bytes, want 104", len(b))
	}
	got, err := format.UnmarshalObjectHeader(b)
	if err != nil {
		t.Fatal(err)
	}
	if got != h {
		t.Fatalf("object round-trip mismatch:\n got %+v\nwant %+v", got, h)
	}
}

func TestHeaderCorruptionDetected(t *testing.T) {
	b, _ := sampleRecordHeader().MarshalBinary()
	// Flip a byte in the middle (VolumeID region) — CRC must catch it.
	corrupt := append([]byte(nil), b...)
	corrupt[20] ^= 0xFF
	if _, err := format.UnmarshalRecordHeader(corrupt); !errors.Is(err, format.ErrHeaderCRC) {
		t.Fatalf("want ErrHeaderCRC on corruption, got %v", err)
	}
}

func TestBadMagicAndVersion(t *testing.T) {
	b, _ := sampleRecordHeader().MarshalBinary()

	badMagic := append([]byte(nil), b...)
	badMagic[0] = 'X'
	if _, err := format.UnmarshalRecordHeader(badMagic); !errors.Is(err, format.ErrBadMagic) {
		t.Fatalf("want ErrBadMagic, got %v", err)
	}

	badVer := append([]byte(nil), b...)
	badVer[4] = 0xFF // bump version, then repair CRC so version is what fails
	// Recompute nothing: version check happens before CRC, so this is fine.
	if _, err := format.UnmarshalRecordHeader(badVer); !errors.Is(err, format.ErrBadVersion) {
		t.Fatalf("want ErrBadVersion, got %v", err)
	}
}

func TestShortBuffer(t *testing.T) {
	if _, err := format.UnmarshalRecordHeader(make([]byte, 10)); !errors.Is(err, format.ErrShortBuf) {
		t.Fatalf("want ErrShortBuf, got %v", err)
	}
}

// TestGoldenBytes locks the exact wire layout of the CRC-covered header prefix
// (bytes [0,100)). The trailing HeaderCRC32C is a pure function of these bytes, so
// locking the prefix locks the format. If this fails, the on-disk format changed —
// that requires a version bump and an ADR, never a silent edit.
func TestGoldenBytes(t *testing.T) {
	b, _ := sampleRecordHeader().MarshalBinary()
	gotPrefix := hex.EncodeToString(b[:100])

	want := "" +
		"56573032" + // Magic "VW02"
		"0200" + // Version=2
		"6800" + // HeaderLen=104
		"00" + // RecordType=WRITE
		"000000" + // Reserved0
		"0102030405060708090a0b0c0d0e0f10" + // VolumeID = 1..16
		"0700000000000000" + // Epoch=7
		"2a00000000000000" + // Sequence=42
		"6465666768696a6b6c6d6e6f70717273" + // OperationID = 100..115
		"0010000000000000" + // Offset=4096
		"10000000" + // Length=16
		"03000000" + // KeyID=3
		"efbeadde" + // PayloadCRC32C=0xDEADBEEF (little-endian)
		"03000000" + // Flags = FUA|FLUSH = 3
		"00000000000000000000000000000000" // AuthTag (zero, reserved)

	if len(want)/2 != 100 {
		t.Fatalf("golden prefix definition is %d bytes, want 100", len(want)/2)
	}
	if gotPrefix != want {
		t.Fatalf("golden bytes changed (format drift!):\n got %s\nwant %s", gotPrefix, want)
	}
	// The CRC over the prefix must be non-zero and the whole header must decode.
	if bytes.Equal(b[100:104], []byte{0, 0, 0, 0}) {
		t.Fatal("header CRC should be non-zero")
	}
	if _, err := format.UnmarshalRecordHeader(b); err != nil {
		t.Fatalf("golden bytes must decode: %v", err)
	}
}
