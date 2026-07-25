package wal_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

type ramp struct{ b byte }

func (r *ramp) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.b
		r.b++
	}
	return len(p), nil
}

func readAll(t *testing.T, f disk.File) []byte {
	t.Helper()
	sz, _ := f.Size()
	buf := make([]byte, sz)
	if sz > 0 {
		if _, err := f.ReadAt(buf, 0); err != nil {
			// io.EOF at the exact end is fine.
			if int64(len(buf)) != sz {
				t.Fatalf("readAll: %v", err)
			}
		}
	}
	return buf
}

func encryptedLog(t *testing.T) (*wal.Log, disk.File, *wal.Encryption) {
	t.Helper()
	dek, err := crypto.GenerateDEK(&ramp{b: 1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	enc := &wal.Encryption{DEK: dek, VolumeID: [16]byte{9, 9, 9}}
	d := sim.NewDisk()
	f, _ := d.Create("wal/active.wal")
	l := wal.NewLog(f, sim.NewClock(time.Unix(1_700_000_000, 0).UTC()), enc.VolumeID, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableEncryption(enc)
	return l, f, enc
}

// TestEncryptedWALIsCiphertextButReadsPlaintext is INV-15: the on-disk WAL bytes
// (which later leave the host as S3 objects) must not contain the plaintext, while
// live reads through the log return plaintext (it stays in host memory).
func TestEncryptedWALIsCiphertextButReadsPlaintext(t *testing.T) {
	l, f, _ := encryptedLog(t)
	canary := []byte("TOP-SECRET-GUEST-PAYLOAD")

	if _, err := l.Write(0, canary, 0); err != nil {
		t.Fatal(err)
	}

	// On disk: ciphertext, no plaintext canary.
	raw := readAll(t, f)
	if bytes.Contains(raw, canary) {
		t.Fatal("plaintext canary found in the on-disk WAL — INV-15 violation")
	}

	// Live read through the log: plaintext.
	buf := make([]byte, len(canary))
	l.Read(0, buf)
	if !bytes.Equal(buf, canary) {
		t.Fatalf("live read should be plaintext, got %q", buf)
	}
}

// TestEncryptedReplayDecrypts verifies recovery: replay the ciphertext WAL and
// decrypt back to the original plaintext.
func TestEncryptedReplayDecrypts(t *testing.T) {
	l, f, enc := encryptedLog(t)
	payloads := [][]byte{[]byte("alpha"), []byte("bravo-payload"), []byte("charlie")}
	for i, p := range payloads {
		if _, err := l.Write(uint64(i*64), p, 0); err != nil {
			t.Fatal(err)
		}
	}

	recs, err := wal.Replay(readAll(t, f))
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != len(payloads) {
		t.Fatalf("replayed %d records, want %d", len(recs), len(payloads))
	}
	for i, rec := range recs {
		pt, err := enc.Decrypt(rec)
		if err != nil {
			t.Fatalf("decrypt record %d: %v", i, err)
		}
		if !bytes.Equal(pt, payloads[i]) {
			t.Fatalf("record %d: got %q want %q", i, pt, payloads[i])
		}
	}
}

// TestOnDiskTamperFailsClosed corrupts a ciphertext byte on disk; decryption must
// fail (GCM), never return silently-wrong plaintext.
func TestOnDiskTamperFailsClosed(t *testing.T) {
	l, f, enc := encryptedLog(t)
	if _, err := l.Write(0, []byte("important guest bytes"), 0); err != nil {
		t.Fatal(err)
	}
	raw := readAll(t, f)
	// Corrupt a byte inside the ciphertext payload (past the 104-byte header).
	raw[len(raw)-3] ^= 0xFF

	recs, err := wal.Replay(raw)
	if err != nil {
		t.Fatalf("header still intact, replay should succeed: %v", err)
	}
	if _, err := enc.Decrypt(recs[0]); err == nil {
		t.Fatal("tampered ciphertext must fail decryption, not decode silently")
	}
}

// TestCryptoShred models §15.3: without the DEK, remnants are undecryptable.
func TestCryptoShred(t *testing.T) {
	l, f, _ := encryptedLog(t)
	_, _ = l.Write(0, []byte("shred me"), 0)
	recs, _ := wal.Replay(readAll(t, f))

	// A different DEK (the original "destroyed") cannot read the remnants.
	otherDEK, _ := crypto.GenerateDEK(&ramp{b: 200}, 1)
	other := &wal.Encryption{DEK: otherDEK, VolumeID: [16]byte{9, 9, 9}}
	if _, err := other.Decrypt(recs[0]); err == nil {
		t.Fatal("remnants must be unreadable without the original DEK")
	}
}

func TestDiscardAccounting(t *testing.T) {
	d := sim.NewDisk()
	f, _ := d.Create("wal/active.wal")
	l := wal.NewLog(f, sim.NewClock(time.Unix(1_700_000_000, 0).UTC()), [16]byte{}, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	_, _ = l.Write(0, []byte("data"), 0)
	_, _ = l.Discard(0, 4096)
	_, _ = l.WriteZeroes(8192, 2048)
	if got := l.DiscardedBytes(); got != 4096+2048 {
		t.Fatalf("discarded_bytes = %d, want %d", got, 4096+2048)
	}
}
