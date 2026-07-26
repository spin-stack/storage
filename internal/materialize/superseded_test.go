package materialize_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/materialize"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/snapshot"
	"github.com/spin-stack/storage/internal/wal/format"
)

// FromEpoch is the drain's and the warm standby's source (ADR-0008): it is handed an
// epoch and rebuilds the volume from S3 alone. Two properties of the object store were
// never expressed here.
//
// A superseded epoch has a ceiling. Once a promotion has written the create-only
// recovery point of epoch N+1, sequences of epoch N above it were written by a fenced
// writer and were never adopted. A late PUT that lands afterwards used to extend the
// prefix, so the same bucket materialized a volume the live one never had — and a
// drain resumed by finishMovedVolume recomputed a prog.UpTo that contradicts the
// immutable boundary already stored, which is a permanent hard error for that volume.
//
// And overlapping objects are not a gap. FromEpoch demanded that every referenced
// object start exactly where the previous one ended, so a re-batch after a restart
// turned into ErrSequenceGap while DurablePrefix answered a number — the two views of
// one bucket disagreeing about whether it is recoverable at all.

// craft builds a valid WAL object covering [first,last] whose records carry `content`.
// Header digest, length, count and span are all honest, so it passes validation; only
// the bytes differ between two objects claiming one range, which is what makes the
// key differ and both create-only PUTs succeed.
func craft(t *testing.T, vol [16]byte, epoch, first, last uint64, content string) (string, []byte) {
	t.Helper()
	var payload []byte
	for seq := first; seq <= last; seq++ {
		rec, err := format.EncodeRecord(format.RecordHeader{
			RecordType: format.RecordWrite,
			VolumeID:   vol,
			Epoch:      epoch,
			Sequence:   seq,
			Offset:     seq * 512,
		}, []byte(content))
		if err != nil {
			t.Fatal(err)
		}
		payload = append(payload, rec...)
	}
	sum := sha256.Sum256(payload)
	h := format.ObjectHeader{
		VolumeID:      vol,
		Epoch:         epoch,
		FirstSequence: first,
		LastSequence:  last,
		RecordCount:   uint32(last - first + 1),
		PayloadLength: uint64(len(payload)),
		PayloadSHA256: sum,
	}
	head, err := h.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return format.WALObjectKey(vol, epoch, first, last, sum), append(head, payload...)
}

func put(t *testing.T, store *sim.ObjectStore, key string, body []byte) {
	t.Helper()
	if _, err := store.Put(context.Background(), key, body, objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
}

// TestFromEpochStopsAtTheSuccessorsBoundary: the drain's final pass over the old epoch
// must land on exactly the number the boundary already records.
func TestFromEpochStopsAtTheSuccessorsBoundary(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	vol := v7Vol()

	for _, s := range [][2]uint64{{1, 2}, {3, 4}} {
		k, b := craft(t, vol, 1, s[0], s[1], "acked")
		put(t, store, k, b)
	}
	if err := recovery.WriteRecoveryPoint(ctx, store, vol, 2, 1, 4); err != nil {
		t.Fatal(err)
	}
	// W1's in-flight PUT lands after the boundary was fixed.
	k, b := craft(t, vol, 1, 5, 6, "never-adopted")
	put(t, store, k, b)

	view, prog, err := materialize.New(store, nil, nil).FromEpoch(ctx, vol, 1)
	if err != nil {
		t.Fatalf("FromEpoch: %v", err)
	}
	rp, err := recovery.ReadRecoveryPoint(ctx, store, vol, 2)
	if err != nil {
		t.Fatal(err)
	}
	if prog.UpTo != rp.RecoveredUpTo {
		t.Fatalf("FromEpoch(epoch 1) covered up to %d while the immutable boundary of epoch 2 "+
			"records %d — a resumed drain recomputes this number and then fails permanently "+
			"because it disagrees with the create-only object", prog.UpTo, rp.RecoveredUpTo)
	}
	if got := readAt(view, 5*512, len("never-adopted")); got == "never-adopted" {
		t.Fatal("a record the live volume never had was materialized onto the destination")
	}
}

// TestFromEpochAndDurablePrefixAgreeOnOverlappingObjects: one bucket, two readers.
// They must not disagree about whether it is recoverable.
func TestFromEpochAndDurablePrefixAgreeOnOverlappingObjects(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	vol := v7Vol()

	// A restarted writer re-batches: the second object re-sends 2..3 and adds 4..5.
	k1, b1 := craft(t, vol, 1, 1, 3, "same-bytes")
	put(t, store, k1, b1)
	k2, b2 := craft(t, vol, 1, 2, 5, "same-bytes")
	put(t, store, k2, b2)

	durable, err := recovery.DurablePrefix(ctx, store, vol, 1)
	if err != nil {
		t.Fatalf("DurablePrefix: %v", err)
	}
	_, prog, err := materialize.New(store, nil, nil).FromEpoch(ctx, vol, 1)
	if err != nil {
		t.Fatalf("FromEpoch refused a layout DurablePrefix reports as durable through %d: %v", durable, err)
	}
	if prog.UpTo != durable {
		t.Fatalf("FromEpoch covered %d, DurablePrefix says %d", prog.UpTo, durable)
	}
	if durable != 5 {
		t.Fatalf("durable = %d, want 5", durable)
	}
}

// TestFromEpochRefusesDivergentObjects is INV-21 on the materialization path: the
// destination must never boot a volume whose content was decided by sort order.
func TestFromEpochRefusesDivergentObjects(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	vol := v7Vol()

	k1, b1 := craft(t, vol, 1, 1, 3, "written-by-W1")
	put(t, store, k1, b1)
	k2, b2 := craft(t, vol, 1, 1, 3, "written-by-W2")
	put(t, store, k2, b2)

	if _, _, err := materialize.New(store, nil, nil).FromEpoch(ctx, vol, 1); !errors.Is(err, recovery.ErrAmbiguousSequence) {
		t.Fatalf("want ErrAmbiguousSequence, got %v", err)
	}
}

// TestFromSnapshotRefusesDivergentObjects: the same on the manifest path, which does
// its own listing rather than going through the durable point. The root digest cannot
// see this — it hashes key strings, and the two objects have different keys precisely
// because their contents differ.
func TestFromSnapshotRefusesDivergentObjects(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	vol := v7Vol()

	k1, b1 := craft(t, vol, 1, 1, 3, "written-by-W1")
	put(t, store, k1, b1)
	k2, b2 := craft(t, vol, 1, 1, 3, "written-by-W2")
	put(t, store, k2, b2)

	man := snapshot.Manifest{
		SnapshotID: "snap-1", VolumeID: format.UUIDString(vol), Epoch: 1,
		TargetSequence: 3, Objects: []string{k1, k2},
	}
	man.RootDigest = snapshot.Digest(man.TargetSequence, man.Objects)
	if err := snapshot.Publish(ctx, store, man); err != nil {
		t.Fatal(err)
	}

	_, _, err := materialize.New(store, nil, nil).FromSnapshot(ctx, format.UUIDString(vol), "snap-1")
	if !errors.Is(err, recovery.ErrAmbiguousSequence) {
		t.Fatalf("a manifest referencing two objects that disagree about sequences 1..3 was "+
			"materialized: the destination's content is whichever one replayed last. got %v", err)
	}
}

// TestFromSnapshotAcceptsAgreeingOverlap is the control on the manifest path: a
// duplicate that carries the same records is not a gap and not a divergence.
func TestFromSnapshotAcceptsAgreeingOverlap(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	vol := v7Vol()

	k1, b1 := craft(t, vol, 1, 1, 3, "same-bytes")
	put(t, store, k1, b1)
	k2, b2 := craft(t, vol, 1, 2, 5, "same-bytes")
	put(t, store, k2, b2)

	man := snapshot.Manifest{
		SnapshotID: "snap-1", VolumeID: format.UUIDString(vol), Epoch: 1,
		TargetSequence: 5, Objects: []string{k1, k2},
	}
	man.RootDigest = snapshot.Digest(man.TargetSequence, man.Objects)
	if err := snapshot.Publish(ctx, store, man); err != nil {
		t.Fatal(err)
	}

	view, prog, err := materialize.New(store, nil, nil).FromSnapshot(ctx, format.UUIDString(vol), "snap-1")
	if err != nil {
		t.Fatalf("FromSnapshot: %v", err)
	}
	if prog.UpTo != 5 {
		t.Fatalf("covered up to %d, want 5", prog.UpTo)
	}
	if got := readAt(view, 5*512, len("same-bytes")); got != "same-bytes" {
		t.Fatalf("sequence 5 missing from the materialized view: %q", got)
	}
}
