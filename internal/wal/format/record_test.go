package format_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/wal/format"
)

func TestEncodeDecodeWriteRecord(t *testing.T) {
	payload := []byte("guest-extent-bytes-4k...")
	h := format.RecordHeader{RecordType: format.RecordWrite, Epoch: 1, Sequence: 5, Offset: 512}

	enc, err := format.EncodeRecord(h, payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(enc) != format.RecordHeaderSize+len(payload) {
		t.Fatalf("encoded size %d, want %d", len(enc), format.RecordHeaderSize+len(payload))
	}

	gotH, gotPayload, n, err := format.DecodeRecord(enc)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(enc) {
		t.Fatalf("consumed %d, want %d", n, len(enc))
	}
	if gotH.Length != uint32(len(payload)) {
		t.Fatalf("Length=%d, want %d", gotH.Length, len(payload))
	}
	if !bytes.Equal(gotPayload, payload) {
		t.Fatalf("payload mismatch: %q", gotPayload)
	}
}

func TestEncodeDiscardIsHeaderOnly(t *testing.T) {
	// DISCARD carries the extent length in Length but no payload (§14.1).
	h := format.RecordHeader{RecordType: format.RecordDiscard, Epoch: 1, Sequence: 6, Offset: 0, Length: 65536}
	enc, err := format.EncodeRecord(h, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(enc) != format.RecordHeaderSize {
		t.Fatalf("discard record should be header-only, got %d bytes", len(enc))
	}
	gotH, payload, n, err := format.DecodeRecord(enc)
	if err != nil {
		t.Fatal(err)
	}
	if payload != nil || n != format.RecordHeaderSize {
		t.Fatalf("discard decode: payload=%v n=%d", payload, n)
	}
	if gotH.Length != 65536 {
		t.Fatalf("discard extent length lost: %d", gotH.Length)
	}
}

func TestDiscardWithPayloadRejected(t *testing.T) {
	h := format.RecordHeader{RecordType: format.RecordDiscard}
	if _, err := format.EncodeRecord(h, []byte("x")); err == nil {
		t.Fatal("discard record with a payload must be rejected")
	}
}

func TestDecodePayloadCorruptionDetected(t *testing.T) {
	payload := []byte("important data")
	enc, _ := format.EncodeRecord(format.RecordHeader{RecordType: format.RecordWrite}, payload)
	// Corrupt a payload byte; PayloadCRC32C must catch it.
	enc[format.RecordHeaderSize+2] ^= 0xFF
	if _, _, _, err := format.DecodeRecord(enc); !errors.Is(err, format.ErrPayloadCRC) {
		t.Fatalf("want ErrPayloadCRC, got %v", err)
	}
}

func TestDecodeTruncatedPayloadIsShortBuf(t *testing.T) {
	payload := []byte("0123456789abcdef")
	enc, _ := format.EncodeRecord(format.RecordHeader{RecordType: format.RecordWrite}, payload)
	// Chop the last few payload bytes: a torn tail.
	if _, _, _, err := format.DecodeRecord(enc[:len(enc)-4]); !errors.Is(err, format.ErrShortBuf) {
		t.Fatalf("want ErrShortBuf on truncated payload, got %v", err)
	}
}

func TestWALObjectKeyDeterministic(t *testing.T) {
	var vol [16]byte
	for i := range vol {
		vol[i] = byte(i)
	}
	var sha [32]byte
	sha[0], sha[1], sha[2], sha[3] = 0xAB, 0xCD, 0xEF, 0x01

	key := format.WALObjectKey(vol, 3, 100, 250, sha)
	want := "wal/00010203-0405-0607-0809-0a0b0c0d0e0f/3/100-250-abcdef01.wal"
	if key != want {
		t.Fatalf("key = %q, want %q", key, want)
	}
	// Determinism: same inputs, same key.
	if format.WALObjectKey(vol, 3, 100, 250, sha) != key {
		t.Fatal("key builder is not deterministic")
	}
}
