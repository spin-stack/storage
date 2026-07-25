package wal_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
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

// TestUnversionedDEKIsRefusedAtWriteTime: KeyID 0 is the on-disk marker for "this
// payload is cleartext" (§14.1 — DecodeRecord verifies the payload CRC for KeyID 0,
// and Decrypt hands the payload back untouched). A DEK whose KeyID is 0 therefore
// seals a payload and then labels it plaintext: the record's CRC is over the
// cleartext while its payload is ciphertext, so the local WAL no longer replays and
// every object built from it fails recovery's integrity check — after the FLUSH was
// ACKed as remotely durable. The write must fail at the write, not at recovery.
func TestUnversionedDEKIsRefusedAtWriteTime(t *testing.T) {
	dek, err := crypto.GenerateDEK(&ramp{b: 1}, 0) // KeyID 0
	if err != nil {
		t.Fatal(err)
	}
	d := sim.NewDisk()
	f, _ := d.Create("wal/active.wal")
	vol := [16]byte{9, 9, 9}
	l := wal.NewLog(f, sim.NewClock(time.Unix(1_700_000_000, 0).UTC()), vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableEncryption(&wal.Encryption{DEK: dek, VolumeID: vol})

	if _, err := l.Write(0, []byte("guest bytes"), 0); err == nil {
		t.Fatal("a WRITE sealed with a KeyID-0 DEK was accepted; it can never be replayed")
	}
	if sz, _ := f.Size(); sz != 0 {
		t.Fatalf("the refused write left %d bytes in the WAL", sz)
	}
}

// TestEncryptedObjectHeaderCarriesTheDEKKeyID: the object header's KeyID is what a
// recoverer reads to pick the DEK *version* (§15.1 rotation). It is taken from the
// batcher, which is constructed independently of the Encryption context, so the two
// can disagree — and then the object announces a key version that did not seal it.
// The DEK is the only source of truth for that field.
func TestEncryptedObjectHeaderCarriesTheDEKKeyID(t *testing.T) {
	ctx := context.Background()
	dek, err := crypto.GenerateDEK(&ramp{b: 3}, 7)
	if err != nil {
		t.Fatal(err)
	}
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	f, _ := d.Create("wal/active.wal")
	vol := [16]byte{9, 9, 9}
	l := wal.NewLog(f, clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	// The batcher is told KeyID 0 — the value every call site in the tree passes.
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), leaseOK{})
	l.EnableEncryption(&wal.Encryption{DEK: dek, VolumeID: vol})

	if _, err := l.Write(0, []byte("guest bytes"), 0); err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	objs, _ := store.List(ctx, "wal/")
	if len(objs) != 1 {
		t.Fatalf("expected 1 object, got %d", len(objs))
	}
	body, _ := store.Get(ctx, objs[0].Key)
	h, err := format.UnmarshalObjectHeader(body[:format.ObjectHeaderSize])
	if err != nil {
		t.Fatal(err)
	}
	if h.KeyID != dek.KeyID {
		t.Fatalf("object header claims KeyID %d, the records were sealed with %d", h.KeyID, dek.KeyID)
	}
}

// TestEncryptedWriteSurvivesTheS3RoundTrip is the positive control for the two above:
// with a versioned DEK the ACKed FLUSH is genuinely reproducible from the bucket.
func TestEncryptedWriteSurvivesTheS3RoundTrip(t *testing.T) {
	ctx := context.Background()
	dek, err := crypto.GenerateDEK(&ramp{b: 5}, 7)
	if err != nil {
		t.Fatal(err)
	}
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	f, _ := d.Create("wal/active.wal")
	vol := [16]byte{9, 9, 9}
	enc := &wal.Encryption{DEK: dek, VolumeID: vol}
	l := wal.NewLog(f, clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, dek.KeyID, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), leaseOK{})
	l.EnableEncryption(enc)

	payload := []byte("guest bytes that must come back")
	if _, err := l.Write(0, payload, 0); err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	prefix, err := recovery.DurablePrefix(ctx, store, vol, 1)
	if err != nil {
		t.Fatal(err)
	}
	if prefix != l.Watermarks().Durable {
		t.Fatalf("S3 reproduces up to %d, the FLUSH ACKed %d", prefix, l.Watermarks().Durable)
	}
	view, _, err := recovery.Recover(ctx, store, enc, vol, 1)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len(payload))
	view.Read(0, buf)
	if !bytes.Equal(buf, payload) {
		t.Fatalf("recovered %q, want %q", buf, payload)
	}
}

// TestDecryptRejectsAKeyVersionItDoesNotHold: rotation (§15.1) leaves history sealed
// under older DEK versions, so a record whose KeyID this volume does not hold is a
// *missing key*, not a tamper. Reporting it as a GCM authentication failure aborts
// the recovery of every remaining epoch instead of naming the version to fetch — and
// it hides a genuinely mixed-key epoch behind the same error a bit flip produces.
func TestDecryptRejectsAKeyVersionItDoesNotHold(t *testing.T) {
	dek, err := crypto.GenerateDEK(&ramp{b: 2}, 7)
	if err != nil {
		t.Fatal(err)
	}
	enc := &wal.Encryption{DEK: dek, VolumeID: [16]byte{9, 9, 9}}

	sealed := wal.Record{Type: format.RecordWrite, Epoch: 1, Sequence: 1, KeyID: 9, Payload: []byte("ciphertext")}
	_, err = enc.Decrypt(sealed)
	if !errors.Is(err, wal.ErrUnknownKeyID) {
		t.Fatalf("want ErrUnknownKeyID, got %v", err)
	}
	if errors.Is(err, crypto.ErrOpen) {
		t.Fatal("a key version we do not hold must not be reported as tamper")
	}
}

// TestNewEncryptionRefusesAnUnversionedDEK guards the checked constructor: KeyID 0 is
// the plaintext marker, so it can never be a DEK version.
func TestNewEncryptionRefusesAnUnversionedDEK(t *testing.T) {
	dek, err := crypto.GenerateDEK(&ramp{b: 4}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.NewEncryption(dek, [16]byte{9}); !errors.Is(err, wal.ErrUnversionedKey) {
		t.Fatalf("want ErrUnversionedKey, got %v", err)
	}
	versioned, err := crypto.GenerateDEK(&ramp{b: 4}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.NewEncryption(versioned, [16]byte{9}); err != nil {
		t.Fatalf("a versioned DEK must be accepted: %v", err)
	}
}

// TestRecordsCarryTheLogsEpoch: every record type must be stamped with the epoch of
// the log that wrote it — it is the only identity a replayed record carries (INV-08:
// a record from another epoch must never be applied as ours).
func TestRecordsCarryTheLogsEpoch(t *testing.T) {
	d := sim.NewDisk()
	f, _ := d.Create("wal/active.wal")
	l := wal.NewLog(f, sim.NewClock(time.Unix(1_700_000_000, 0).UTC()), [16]byte{1}, 5, wal.Limits{MaxUnflushedBytes: 1 << 20})
	if _, err := l.Write(0, []byte("w"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Discard(64, 8); err != nil {
		t.Fatal(err)
	}
	if _, err := l.WriteZeroes(128, 8); err != nil {
		t.Fatal(err)
	}
	recs, err := wal.Replay(readAll(t, f))
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("replayed %d records, want 3", len(recs))
	}
	for i, r := range recs {
		if r.Epoch != 5 {
			t.Fatalf("record %d (%v) carries epoch %d, the log is at 5", i, r.Type, r.Epoch)
		}
	}
}

// TestRecordsCarryTheLogsVolumeID: the record header has a VolumeID field (§14.1)
// and the write path leaves it zero, so the local WAL has no volume binding at all.
// A file replayed against the wrong volume — a path bug, a restored backup, a reused
// volume directory, two volumes' records concatenated — applies to another guest's
// extents with nothing to detect it. Encryption's AAD catches it for sealed payloads
// only, which is not the layer INV-05/INV-08 are claimed at, and DISCARD records
// carry no payload at all.
func TestRecordsCarryTheLogsVolumeID(t *testing.T) {
	vol := [16]byte{0xA1, 0xB2, 0xC3}
	d := sim.NewDisk()
	f, _ := d.Create("wal/active.wal")
	l := wal.NewLog(f, sim.NewClock(time.Unix(1_700_000_000, 0).UTC()), vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	if _, err := l.Write(0, []byte("w"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Discard(64, 8); err != nil {
		t.Fatal(err)
	}
	if _, err := l.WriteZeroes(128, 8); err != nil {
		t.Fatal(err)
	}
	recs, err := wal.Replay(readAll(t, f))
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range recs {
		if r.VolumeID != vol {
			t.Fatalf("record %d (%v) carries volume %x, the log belongs to %x", i, r.Type, r.VolumeID, vol)
		}
	}
}

// TestEncryptedRecordsCarryTheVolumeID: the encrypted path builds its header
// separately, so it needs its own assertion.
func TestEncryptedRecordsCarryTheVolumeID(t *testing.T) {
	l, f, _ := encryptedLog(t)
	if _, err := l.Write(0, []byte("sealed"), 0); err != nil {
		t.Fatal(err)
	}
	recs, err := wal.Replay(readAll(t, f))
	if err != nil {
		t.Fatal(err)
	}
	if recs[0].VolumeID != ([16]byte{9, 9, 9}) {
		t.Fatalf("encrypted record carries volume %x, want %x", recs[0].VolumeID, [16]byte{9, 9, 9})
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
