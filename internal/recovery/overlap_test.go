package recovery_test

import (
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal/format"
)

// The contiguity walk demanded that every object start *exactly* where the previous
// one ended. Two things fall out of that, and they pull in opposite directions.
//
// Under-reporting (§22.1): objects that overlap — which a restarted writer's re-batch
// produces, even though a correct Batcher never does — stop the walk at the first one
// that does not line up. {1-3} and {2-5} report a durable point of 3, silently, while
// sequences 4 and 5 sit in S3 inside an ACKed FLUSH. A promotion then records 3 as the
// next epoch's create-only floor and 4..5 are never replayed by anyone again.
//
// Over-reporting (INV-21, §14.5): "same range + different hash ⇒ hard fail" is
// unreachable, because the object key embeds the payload digest. Two objects claiming
// one sequence range with different bytes get different keys, so both create-only PUTs
// succeed, both validate, and both are replayed — the recovered content is decided by
// whichever SHA prefix sorts first, with no diagnostic at all. That is exactly the
// state a restarted writer or a failure of fencing produces, and exactly what INV-21
// exists to catch.
//
// One rule settles both: where two objects carry the same sequence they must carry the
// same record. Agreeing objects are folded at record level (so the run reaches 5);
// disagreeing ones are a hard, named failure.

// craftDivergent builds a valid WAL object covering [first,last] whose records carry
// `content` — so two objects can claim one range while disagreeing about its bytes.
// Everything else (digest, count, span) is honest, so both pass validation.
func craftDivergent(t *testing.T, volumeID [16]byte, epoch, first, last uint64, content string) (string, []byte) {
	t.Helper()

	var payload []byte
	for seq := first; seq <= last; seq++ {
		rec, err := format.EncodeRecord(format.RecordHeader{
			RecordType: format.RecordWrite,
			VolumeID:   volumeID,
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
		VolumeID:      volumeID,
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
	return format.WALObjectKey(volumeID, epoch, first, last, sum), append(head, payload...)
}

// TestOverlappingObjectsDoNotUnderReportTheDurablePoint: the sequences are there, in
// validated objects, under an ACKed FLUSH. Stopping short of them loses them.
func TestOverlappingObjectsDoNotUnderReportTheDurablePoint(t *testing.T) {
	tests := []struct {
		name   string
		spans  [][2]uint64
		expect uint64
	}{
		{"disjoint, the ordinary case", [][2]uint64{{1, 3}, {4, 5}}, 5},
		{"the second object re-sends the tail of the first", [][2]uint64{{1, 3}, {2, 5}}, 5},
		{"the second object re-sends the whole first one and more", [][2]uint64{{1, 3}, {1, 5}}, 5},
		{"one object is wholly contained in another", [][2]uint64{{1, 5}, {2, 3}}, 5},
		{"an overlap does not paper over a real gap", [][2]uint64{{1, 3}, {2, 4}, {6, 7}}, 4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			store := sim.NewObjectStore()
			vol := vol7()
			for _, s := range tc.spans {
				k, b := craftObject(t, vol, 1, s[0], s[1], nil)
				putObject(t, store, k, b)
			}
			got, err := recovery.DurablePrefix(ctx, store, vol, 1)
			if err != nil {
				t.Fatalf("DurablePrefix: %v", err)
			}
			if got != tc.expect {
				t.Fatalf("durable point = %d, want %d — the records covering it are in S3 "+
					"and were ACKed; a promotion is about to write this number as an immutable floor",
					got, tc.expect)
			}
		})
	}
}

// TestDivergentObjectsForOneSequenceAreAHardFailure is INV-21 / §14.5. Two writers in
// one epoch, or one writer re-batching after a restart, is a fencing failure — and it
// must be reported as one, not resolved by sort order.
func TestDivergentObjectsForOneSequenceAreAHardFailure(t *testing.T) {
	tests := []struct {
		name  string
		spans [][2]uint64
	}{
		{"the same range twice", [][2]uint64{{1, 3}, {1, 3}}},
		{"ranges that partly overlap", [][2]uint64{{1, 3}, {2, 5}}},
		{"one range contained in the other", [][2]uint64{{1, 5}, {2, 3}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			store := sim.NewObjectStore()
			vol := vol7()

			k1, b1 := craftDivergent(t, vol, 1, tc.spans[0][0], tc.spans[0][1], "written-by-W1")
			putObject(t, store, k1, b1)
			k2, b2 := craftDivergent(t, vol, 1, tc.spans[1][0], tc.spans[1][1], "written-by-W2")
			putObject(t, store, k2, b2)
			if k1 == k2 {
				t.Fatal("setup: divergent objects must land on different keys, which is the whole problem")
			}

			_, err := recovery.DurablePrefix(ctx, store, vol, 1)
			if err == nil {
				t.Fatal("two objects carrying different records for one sequence were accepted: " +
					"the recovered content is decided by whichever SHA prefix sorts first (violates INV-21/§14.5)")
			}
			if !errors.Is(err, recovery.ErrAmbiguousSequence) {
				t.Fatalf("want ErrAmbiguousSequence, got %v", err)
			}
			if _, _, err := recovery.Recover(ctx, store, nil, vol, 1); !errors.Is(err, recovery.ErrAmbiguousSequence) {
				t.Fatalf("Recover must refuse an ambiguous epoch rather than pick a side, got %v", err)
			}
		})
	}
}

// TestAgreeingObjectsAreNotAConflict is the control the rule must not swallow: a
// re-sent object that carries the same records is a duplicate, not a divergence, and
// the epoch stays recoverable.
func TestAgreeingObjectsAreNotAConflict(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	k1, b1 := craftDivergent(t, vol, 1, 1, 3, "same-bytes")
	putObject(t, store, k1, b1)
	k2, b2 := craftDivergent(t, vol, 1, 2, 5, "same-bytes")
	putObject(t, store, k2, b2)

	got, err := recovery.DurablePrefix(ctx, store, vol, 1)
	if err != nil {
		t.Fatalf("agreeing objects must not be a conflict: %v", err)
	}
	if got != 5 {
		t.Fatalf("durable point = %d, want 5", got)
	}
	view, _, err := recovery.Recover(ctx, store, nil, vol, 1)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len("same-bytes"))
	view.Read(5*512, buf)
	if string(buf) != "same-bytes" {
		t.Fatalf("sequence 5 is missing from the recovered view: %q", buf)
	}
}

// TestDivergenceOutsideThePrefixIsStillFatal: the ambiguity does not become harmless
// because it sits past a gap. An object below the durable point can be re-uploaded
// later and close the gap, and by then the choice would already have been made.
func TestDivergenceIsReportedEvenAboveAGap(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	k, b := craftObject(t, vol, 1, 1, 2, nil)
	putObject(t, store, k, b)
	k1, b1 := craftDivergent(t, vol, 1, 8, 9, "one")
	putObject(t, store, k1, b1)
	k2, b2 := craftDivergent(t, vol, 1, 8, 9, "another")
	putObject(t, store, k2, b2)

	if _, err := recovery.DurablePrefix(ctx, store, vol, 1); !errors.Is(err, recovery.ErrAmbiguousSequence) {
		t.Fatalf("an ambiguous sequence anywhere in the epoch must be reported, got %v", err)
	}
}
