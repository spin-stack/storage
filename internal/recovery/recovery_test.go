package recovery_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

func v7Vol() [16]byte {
	var v [16]byte
	v[6], v[8] = 0x70, 0x80 // v7 shape
	return v
}

// putBatch uploads a WAL object covering [first,last] for the volume/epoch.
func putBatch(t *testing.T, store *sim.ObjectStore, volID [16]byte, epoch, first, last uint64) {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	b := wal.NewBatcher(clk, volID, epoch, 0, wal.DefaultBatchConfig())
	for seq := first; seq <= last; seq++ {
		enc, _ := wal.Record{Sequence: seq, Epoch: epoch, Payload: []byte("x")}.Encode()
		b.Append(seq, enc, false)
	}
	b.Flush()
	if _, err := wal.NewUploader(store, 3).Upload(t.Context(), b.Pending()[0]); err != nil {
		t.Fatal(err)
	}
}

func TestDurablePrefix(t *testing.T) {
	tests := []struct {
		name   string
		spans  [][2]uint64 // uploaded [first,last] object spans
		expect uint64
	}{
		{"contiguous", [][2]uint64{{1, 2}, {3, 4}}, 4},
		{"stops at gap", [][2]uint64{{1, 2}, {5, 6}}, 2}, // 3-4 missing
		{"empty", nil, 0},
		{"single", [][2]uint64{{1, 3}}, 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := sim.NewObjectStore()
			vol := v7Vol()
			for _, s := range tc.spans {
				putBatch(t, store, vol, 1, s[0], s[1])
			}
			last, err := recovery.DurablePrefix(t.Context(), store, vol, 1)
			if err != nil {
				t.Fatal(err)
			}
			if last != tc.expect {
				t.Fatalf("durable prefix = %d, want %d", last, tc.expect)
			}
		})
	}
}

// TestRecoverReconstructsState writes a volume through a Log and recovers its read
// view from S3 alone.
func TestRecoverReconstructsState(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := v7Vol()

	d := sim.NewDisk()
	l := wal.NewLog(d, "wal", clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 3), leaseOK{})

	_, _ = l.Write(0, []byte("hello"), 0)
	_, _ = l.Write(8, []byte("world"), 0)
	_, _ = l.Discard(0, 1) // zero the first byte
	if err := l.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	view, durable, err := recovery.Recover(ctx, store, nil, vol, 1)
	if err != nil {
		t.Fatal(err)
	}
	if durable != 3 {
		t.Fatalf("recovered durable = %d, want 3", durable)
	}
	buf := make([]byte, 13)
	view.Read(0, buf)
	if !bytes.Equal(buf, []byte("\x00ello\x00\x00\x00world")) {
		t.Fatalf("recovered read view mismatch: %q", buf)
	}
}

// TestRecoverEncrypted recovers a volume whose WAL objects are ciphertext,
// decrypting each record with the volume DEK.
func TestRecoverEncrypted(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := v7Vol()

	dek, _ := crypto.GenerateDEK(&ramp{1}, 1)
	enc := &wal.Encryption{DEK: dek, VolumeID: vol}

	d := sim.NewDisk()
	l := wal.NewLog(d, "wal", clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableEncryption(enc)
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, dek.KeyID, wal.DefaultBatchConfig()), wal.NewUploader(store, 3), leaseOK{})

	secret := []byte("SECRET-PAYLOAD")
	_, _ = l.Write(0, secret, 0)
	if err := l.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// On-S3 object is ciphertext.
	objs, _ := store.List(ctx, "wal/")
	body, _ := store.Get(ctx, objs[0].Key)
	if bytes.Contains(body, secret) {
		t.Fatal("recovered object should be ciphertext")
	}

	view, _, err := recovery.Recover(ctx, store, enc, vol, 1)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len(secret))
	view.Read(0, buf)
	if !bytes.Equal(buf, secret) {
		t.Fatalf("decrypted recover = %q, want %q", buf, secret)
	}
}

type ramp struct{ b byte }

func (r *ramp) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.b
		r.b++
	}
	return len(p), nil
}

// TestDurablePointRejectsLyingSummary is the §22.1 cross-check: a summary that
// claims more than the contiguous prefix provides is rejected.
func TestDurablePointRejectsLyingSummary(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := v7Vol()

	// Only seq 1-2 are actually durable.
	putBatch(t, store, vol, 1, 1, 2)
	if last, _ := recovery.DurablePoint(ctx, store, vol, 1); last != 2 {
		t.Fatalf("honest durable point = %d, want 2", last)
	}

	// Forge a summary claiming durable=5, which the objects do not back.
	body, _ := json.Marshal(wal.Summary{VolumeID: format.UUIDString(vol), Epoch: 1, DurableSequence: 5})
	if _, err := store.Put(ctx, wal.SummaryKey(vol, 1), body, objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := recovery.DurablePoint(ctx, store, vol, 1); err == nil {
		t.Fatal("a summary claiming more than the contiguous prefix must be rejected")
	}
}

// TestRecoverExcludesLatePutBeyondGap is INV-12: a late PUT beyond a gap is not
// part of the recovered prefix, and the recovery-point records the true boundary.
func TestRecoverExcludesLatePutBeyondGap(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := v7Vol()

	putBatch(t, store, vol, 1, 1, 2) // durable
	putBatch(t, store, vol, 1, 4, 4) // late/orphan PUT after a gap at 3

	_, durable, err := recovery.Recover(ctx, store, nil, vol, 1)
	if err != nil {
		t.Fatal(err)
	}
	if durable != 2 {
		t.Fatalf("recovered durable = %d, want 2 (late PUT at 4 excluded)", durable)
	}
	// W2 fixes the boundary; it must equal the recovered prefix, bounding the late PUT.
	if err := recovery.WriteRecoveryPoint(ctx, store, vol, 2, 1, durable); err != nil {
		t.Fatal(err)
	}
	rp, _ := recovery.ReadRecoveryPoint(ctx, store, vol, 2)
	if rp.RecoveredUpTo != 2 {
		t.Fatalf("recovery-point recovered_up_to = %d, want 2", rp.RecoveredUpTo)
	}
}

func TestRecoveryPointRoundTrip(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := v7Vol()
	if err := recovery.WriteRecoveryPoint(ctx, store, vol, 2, 1, 42); err != nil {
		t.Fatal(err)
	}
	rp, err := recovery.ReadRecoveryPoint(ctx, store, vol, 2)
	if err != nil {
		t.Fatal(err)
	}
	if rp.PrevEpoch != 1 || rp.RecoveredUpTo != 42 {
		t.Fatalf("recovery point = %+v", rp)
	}
}

// leaseOK is the fence for tests that are not about fencing: remote durability
// requires a lease checker (DEV-0004), and these hold a valid one.
type leaseOK struct{}

func (leaseOK) Valid() bool { return true }
