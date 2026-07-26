package wal_test

import (
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// TEST-GAPS (Open, known-weaker): "The uploader detects an identical span, not a
// partial overlap ({1-3} vs {2-5}), and its check is a LIST, so a lagging listing
// blinds it."
//
// INV-21 is "one object per sequence span". `spanClaimed` compares the key minus its
// content-hash suffix, so it decides *identical* spans and nothing else: {1-3} and
// {2-5} produce two unrelated prefixes, both PUTs succeed, both objects pass
// recovery's integrity check, and sequences 2 and 3 exist twice in the bucket.
//
// The decision this file records (see the Uploader's package doc for the argument):
//
//   - an overlap this uploader itself produced is decidable with no I/O at all, from
//     what it has already published, so it is closed here. That is the failure the
//     write path can actually see: a batcher accounting bug, or a resume that
//     re-queued records an object already covers;
//   - an overlap produced by *another* writer is not decidable on the write path. The
//     only evidence is a LIST, LIST is eventually consistent (§6.1), and a listing
//     that has not caught up produces a false negative while a stricter rule would
//     produce false positives that wedge a legitimately restarted writer forever.
//     recovery.VerifyAgreement is the backstop: it reads a complete listing, at the
//     one moment the answer has to be right, and fails with ErrAmbiguousSequence.

func TestUploaderRefusesASpanOverlappingOneItPublished(t *testing.T) {
	ctx := t.Context()
	published := oneBatch(1, 3)

	tests := []struct {
		name    string
		second  *wal.ClosedBatch
		wantErr error
	}{
		{
			name:    "a partial overlap on the right",
			second:  oneBatch(2, 5),
			wantErr: wal.ErrOverlappingSpan,
		},
		{
			name:    "a partial overlap on the left",
			second:  oneBatch(0, 1),
			wantErr: wal.ErrOverlappingSpan,
		},
		{
			name:    "a span strictly inside the published one",
			second:  oneBatch(2, 2),
			wantErr: wal.ErrOverlappingSpan,
		},
		{
			name:    "a span strictly containing the published one",
			second:  oneBatch(1, 9),
			wantErr: wal.ErrOverlappingSpan,
		},
		{
			name:   "the next contiguous span",
			second: oneBatch(4, 6),
		},
		{
			name:   "the identical span again (an idempotent retry after a lost response)",
			second: oneBatch(1, 3),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			u := wal.NewUploader(sim.NewObjectStore(), 3)
			if _, err := u.Upload(ctx, published); err != nil {
				t.Fatalf("publishing the first span: %v", err)
			}
			_, err := u.Upload(ctx, tc.second)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("upload of %d-%d = %v, want success", tc.second.First, tc.second.Last, err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("upload of %d-%d = %v, want %v", tc.second.First, tc.second.Last, err, tc.wantErr)
			}
		})
	}
}

// TestOverlapIsScopedToOneVolumeAndEpoch: sequence spans belong to a volume, and an
// uploader is not promised to serve only one. Two volumes both writing sequences 1-3
// are not an overlap, and neither are two epochs — the epoch boundary is what makes
// the second one's records different records (§12.5).
func TestOverlapIsScopedToOneVolumeAndEpoch(t *testing.T) {
	ctx := t.Context()
	u := wal.NewUploader(sim.NewObjectStore(), 3)
	if _, err := u.Upload(ctx, oneBatch(1, 3)); err != nil {
		t.Fatal(err)
	}

	otherVolume := oneBatch(2, 4)
	otherVolume.VolumeID = [16]byte{99}
	if _, err := u.Upload(ctx, otherVolume); err != nil {
		t.Fatalf("another volume's overlapping span was refused: %v", err)
	}

	otherEpoch := oneBatch(2, 4)
	otherEpoch.Epoch = 2
	if _, err := u.Upload(ctx, otherEpoch); err != nil {
		t.Fatalf("another epoch's overlapping span was refused: %v", err)
	}
}

// TestAForeignWritersPartialOverlapIsNotCaughtOnTheWritePath states the other half of
// the decision as a test rather than as a comment, so that a later change which
// *does* close it fails here and has to say so.
//
// Two writers in one (volume, epoch) is already a fencing failure (§12.2, INV-06);
// this asserts what the write path does about it, which is nothing, and points at
// what does. Making the write path strict instead would mean treating "the listing
// shows an overlap" as fatal — a rule that is silent exactly when the listing lags
// (the case it exists for) and that permanently wedges a restarted writer whose
// re-batch is harmless, which is the trade-off recovery.VerifyAgreement already
// resolves the other way.
func TestAForeignWritersPartialOverlapIsNotCaughtOnTheWritePath(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()

	fenced := wal.NewUploader(store, 3)
	if _, err := fenced.Upload(ctx, oneBatch(1, 3)); err != nil {
		t.Fatal(err)
	}
	// A second host, its own uploader, the same volume and epoch.
	successor := wal.NewUploader(store, 3)
	if _, err := successor.Upload(ctx, oneBatch(2, 5)); err != nil {
		t.Fatalf("the write path is not the place this is decided, but it returned %v", err)
	}

	// Both objects are in the bucket; the two sequence spans overlap. This is what
	// recovery.VerifyAgreement is there to refuse.
	objs, err := store.List(ctx, "wal/")
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 2 {
		t.Fatalf("expected both writers' objects in the bucket, got %d", len(objs))
	}
}
