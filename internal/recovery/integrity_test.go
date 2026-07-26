package recovery_test

import (
	"crypto/sha256"
	"testing"

	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// These tests are DEV-0003: the durable point is the one number the whole durability
// argument rests on (INV-08, INV-09), and it was being derived from object *headers*
// that nobody validated. An object may raise the durable point only if its bytes
// match its header and its records actually cover the range it claims.

func vol7() [16]byte {
	var v [16]byte
	v[6], v[8] = 0x70, 0x80
	return v
}

// craftObject builds a WAL object the way the uploader does: header + serialized
// records, with the header's payload digest/length/count/span describing the payload.
// mut lets a test lie in the header or damage the payload after the fact.
func craftObject(t *testing.T, volumeID [16]byte, epoch, first, last uint64, mut func(h *format.ObjectHeader, payload *[]byte)) (string, []byte) {
	t.Helper()

	var payload []byte
	for seq := first; seq <= last; seq++ {
		rec, err := format.EncodeRecord(format.RecordHeader{
			RecordType: format.RecordWrite,
			VolumeID:   volumeID,
			Epoch:      epoch,
			Sequence:   seq,
			Offset:     seq * 512,
		}, []byte("payload"))
		if err != nil {
			t.Fatal(err)
		}
		payload = append(payload, rec...)
	}

	sum := sha256.Sum256(payload)
	h := format.ObjectHeader{
		VolumeID:      volumeID,
		Epoch:         epoch,
		FirstSequence: first,
		LastSequence:  last,
		RecordCount:   uint32(last - first + 1),
		PayloadLength: uint64(len(payload)),
		PayloadSHA256: sum,
	}
	if mut != nil {
		mut(&h, &payload)
	}
	head, err := h.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	key := format.WALObjectKey(volumeID, epoch, h.FirstSequence, h.LastSequence, h.PayloadSHA256)
	return key, append(head, payload...)
}

func putObject(t *testing.T, store *sim.ObjectStore, key string, body []byte) {
	t.Helper()
	if _, err := store.Put(t.Context(), key, body, objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
}

// TestTruncatedObjectDoesNotRaiseTheDurablePoint: a WAL object whose payload was cut
// short still carries a header claiming its full range. Trusting that header
// overstates durability — exactly the failure INV-08 must exclude.
func TestTruncatedObjectDoesNotRaiseTheDurablePoint(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	k1, b1 := craftObject(t, vol, 1, 1, 2, nil)
	putObject(t, store, k1, b1)
	// Sequences 3..4 exist as an object, but the upload was cut in the middle.
	k2, b2 := craftObject(t, vol, 1, 3, 4, nil)
	putObject(t, store, k2, b2[:len(b2)-9])

	durable, err := recovery.DurablePrefix(ctx, store, vol, 1)
	if err != nil {
		t.Fatalf("DurablePrefix: %v", err)
	}
	if durable != 2 {
		t.Fatalf("durable point = %d, want 2 — a truncated object must not count as durable", durable)
	}
}

// TestHeaderLyingAboutItsRangeIsRejected: the payload digest can be perfectly valid
// while the header claims a longer sequence span than the records it holds. The
// durable point must follow the records, not the claim.
func TestHeaderLyingAboutItsRangeIsRejected(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	k1, b1 := craftObject(t, vol, 1, 1, 2, nil)
	putObject(t, store, k1, b1)
	// Holds records 3..4 but says it covers 3..99 (and its digest is honest).
	k2, b2 := craftObject(t, vol, 1, 3, 4, func(h *format.ObjectHeader, _ *[]byte) {
		h.LastSequence = 99
		h.RecordCount = 97
	})
	putObject(t, store, k2, b2)

	durable, err := recovery.DurablePrefix(ctx, store, vol, 1)
	if err != nil {
		t.Fatalf("DurablePrefix: %v", err)
	}
	if durable != 2 {
		t.Fatalf("durable point = %d, want 2 — a header must not be able to claim sequences it does not carry", durable)
	}
}

// TestObjectFromAnotherVolumeOrEpochIsIgnored: a stray object under the prefix (a
// mis-keyed upload, a bug, a restored bucket) must not contribute.
func TestObjectFromAnotherVolumeOrEpochIsIgnored(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()
	other := vol
	other[15] = 0xEE

	k1, b1 := craftObject(t, vol, 1, 1, 2, nil)
	putObject(t, store, k1, b1)

	// Same key prefix, foreign volume id in the header.
	_, foreign := craftObject(t, other, 1, 3, 4, nil)
	putObject(t, store, format.WALObjectKey(vol, 1, 3, 4, sha256.Sum256([]byte("x"))), foreign)

	// Same volume, wrong epoch in the header.
	_, wrongEpoch := craftObject(t, vol, 9, 5, 6, nil)
	putObject(t, store, format.WALObjectKey(vol, 1, 5, 6, sha256.Sum256([]byte("y"))), wrongEpoch)

	durable, err := recovery.DurablePrefix(ctx, store, vol, 1)
	if err != nil {
		t.Fatalf("DurablePrefix: %v", err)
	}
	if durable != 2 {
		t.Fatalf("durable point = %d, want 2 — foreign objects must not contribute", durable)
	}
}

// TestPrefixRequiresItsFloor: the contiguous prefix has to start somewhere. Without a
// floor, a bucket whose first objects are missing (a partial restore, an aborted GC)
// reads as a healthy prefix starting at whatever survived.
func TestPrefixRequiresItsFloor(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	// Epoch 1 with no recovery point: sequences must start at 1. These start at 5.
	k, b := craftObject(t, vol, 1, 5, 6, nil)
	putObject(t, store, k, b)

	durable, err := recovery.DurablePrefix(ctx, store, vol, 1)
	if err != nil {
		t.Fatalf("DurablePrefix: %v", err)
	}
	if durable != 0 {
		t.Fatalf("durable point = %d, want 0 — the prefix does not reach the floor (sequence 1)", durable)
	}
}

// TestPrefixFloorComesFromTheRecoveryPoint: in epoch N+1 the WAL legitimately starts
// after the previous epoch's recovered point (§12.5), so that is the floor.
func TestPrefixFloorComesFromTheRecoveryPoint(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	if err := recovery.WriteRecoveryPoint(ctx, store, vol, 2, 1, 7); err != nil {
		t.Fatal(err)
	}
	k, b := craftObject(t, vol, 2, 8, 9, nil)
	putObject(t, store, k, b)

	durable, err := recovery.DurablePoint(ctx, store, vol, 2)
	if err != nil {
		t.Fatalf("DurablePoint: %v", err)
	}
	if durable != 9 {
		t.Fatalf("durable point = %d, want 9 — the epoch's prefix starts at recovered_up_to+1", durable)
	}

	// And an epoch whose objects do not reach that floor recovers nothing.
	store2 := sim.NewObjectStore()
	if err := recovery.WriteRecoveryPoint(ctx, store2, vol, 2, 1, 7); err != nil {
		t.Fatal(err)
	}
	k2, b2 := craftObject(t, vol, 2, 12, 13, nil)
	putObject(t, store2, k2, b2)
	durable, err = recovery.DurablePoint(ctx, store2, vol, 2)
	if err != nil {
		t.Fatalf("DurablePoint: %v", err)
	}
	if durable != 7 {
		t.Fatalf("durable point = %d, want 7 — a gap right after the floor stops the prefix", durable)
	}
}

// TestRecoverStopsAtTheValidatedPrefix: Recover must apply exactly the records the
// durable point covers, so a corrupt tail cannot leak into the rebuilt view.
func TestRecoverStopsAtTheValidatedPrefix(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	k1, b1 := craftObject(t, vol, 1, 1, 2, nil)
	putObject(t, store, k1, b1)
	k2, b2 := craftObject(t, vol, 1, 3, 3, nil)
	putObject(t, store, k2, b2[:len(b2)-5]) // truncated

	view, durable, err := recovery.Recover(ctx, store, nil, vol, 1)
	if err != nil {
		t.Fatalf("Recover must tolerate a corrupt tail by stopping at it: %v", err)
	}
	if durable != 2 {
		t.Fatalf("recovered durable = %d, want 2", durable)
	}
	// Sequence 3 wrote at offset 3*512; nothing may be there.
	buf := make([]byte, 7)
	view.Read(3*512, buf)
	for _, b := range buf {
		if b != 0 {
			t.Fatalf("the truncated object's record leaked into the recovered view: %q", buf)
		}
	}
}

// TestWALObjectKeyIsNotTrusted: validation is on the object's own bytes, not on the
// sequence numbers someone encoded in the key.
func TestWALObjectKeyIsNotTrusted(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	k1, b1 := craftObject(t, vol, 1, 1, 2, nil)
	putObject(t, store, k1, b1)
	// Body covers 3..4; the key claims 3..50.
	_, b2 := craftObject(t, vol, 1, 3, 4, nil)
	putObject(t, store, format.WALObjectKey(vol, 1, 3, 50, sha256.Sum256(b2)), b2)

	durable, err := recovery.DurablePrefix(ctx, store, vol, 1)
	if err != nil {
		t.Fatal(err)
	}
	if durable != 4 {
		t.Fatalf("durable point = %d, want 4 — the body decides, not the key", durable)
	}
}

// TestValidObjectsStillRecover is the control: nothing above may be achieved by
// making the validator reject everything.
func TestValidObjectsStillRecover(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	for _, span := range [][2]uint64{{1, 2}, {3, 4}, {5, 5}} {
		k, b := craftObject(t, vol, 1, span[0], span[1], nil)
		putObject(t, store, k, b)
	}
	durable, err := recovery.DurablePrefix(ctx, store, vol, 1)
	if err != nil {
		t.Fatal(err)
	}
	if durable != 5 {
		t.Fatalf("durable point = %d, want 5", durable)
	}
	view, seq, err := recovery.Recover(ctx, store, nil, vol, 1)
	if err != nil || seq != 5 {
		t.Fatalf("Recover: seq=%d err=%v", seq, err)
	}
	buf := make([]byte, 7)
	view.Read(5*512, buf)
	if string(buf) != "payload" {
		t.Fatalf("record 5 missing from the recovered view: %q", buf)
	}
}

var _ = wal.Replay // the validator must agree with the replayer

// TestValidationRejectsEveryShapeOfLie exercises the remaining validate() branches
// directly: each is the difference between "this object is what it claims" and "the
// durable point is a guess".
func TestValidationRejectsEveryShapeOfLie(t *testing.T) {
	ctx := t.Context()
	vol := vol7()

	tests := []struct {
		name string
		mut  func(h *format.ObjectHeader, payload *[]byte)
	}{
		{"payload digest does not match", func(h *format.ObjectHeader, _ *[]byte) {
			h.PayloadSHA256[0] ^= 0xFF
		}},
		{"record count is inflated", func(h *format.ObjectHeader, _ *[]byte) {
			h.RecordCount += 7
		}},
		{"the payload is empty but the header claims records", func(h *format.ObjectHeader, p *[]byte) {
			*p = nil
			h.PayloadLength = 0
			h.PayloadSHA256 = sha256.Sum256(nil)
		}},
		{"first sequence does not match the records", func(h *format.ObjectHeader, _ *[]byte) {
			h.FirstSequence += 10
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := sim.NewObjectStore()
			k1, b1 := craftObject(t, vol, 1, 1, 2, nil)
			putObject(t, store, k1, b1)
			k2, b2 := craftObject(t, vol, 1, 3, 4, tc.mut)
			putObject(t, store, k2, b2)

			durable, err := recovery.DurablePrefix(ctx, store, vol, 1)
			if err != nil {
				t.Fatal(err)
			}
			if durable != 2 {
				t.Fatalf("durable = %d, want 2 — the lying object contributed", durable)
			}
		})
	}
}
