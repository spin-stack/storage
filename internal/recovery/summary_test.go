package recovery_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// The summary is an accelerator (§22.1), not an authority: it is overwritable, it is
// written by whoever holds the pen, and its own volume/epoch fields were never
// checked. Yet it could veto recovery outright — a summary naming a sequence the
// contiguous prefix cannot reach turned every recovery of that epoch into an
// undifferentiated error, on every retry, for ever. A volume in that state can be
// neither evacuated nor promoted, and a stray or mis-keyed summary object is enough
// to put it there.

func putSummary(t *testing.T, store *sim.ObjectStore, key string, s wal.Summary) {
	t.Helper()
	body, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(t.Context(), key, body, objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
}

// TestOverclaimingSummaryIsDistinguishableAndTheHonestPrefixSurvives: the
// discrepancy is real and must be reported — an object that was uploaded is gone —
// but it has to be reportable *as that*, with the prefix S3 can still prove, so an
// operator or a drain can act on it instead of retrying an opaque error for ever.
func TestOverclaimingSummaryIsDistinguishableAndTheHonestPrefixSurvives(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	for _, span := range [][2]uint64{{1, 2}, {3, 4}} {
		k, b := craftObject(t, vol, 1, span[0], span[1], nil)
		putObject(t, store, k, b)
	}
	// The summary names a sequence the surviving objects cannot reach.
	putSummary(t, store, wal.SummaryKey(vol, 1), wal.Summary{
		VolumeID: format.UUIDString(vol), Epoch: 1, DurableSequence: 9,
	})

	_, err := recovery.DurablePoint(ctx, store, vol, 1)
	if err == nil {
		t.Fatal("a summary claiming more than the prefix provides must not be trusted")
	}
	var oc *recovery.SummaryOverclaim
	if !errors.As(err, &oc) {
		t.Fatalf("the discrepancy must be actionable, got an opaque %T: %v", err, err)
	}
	if !errors.Is(err, recovery.ErrSummaryOverclaims) {
		t.Fatalf("want ErrSummaryOverclaims, got %v", err)
	}
	if oc.Claimed != 9 || oc.Contiguous != 4 {
		t.Fatalf("reported claimed=%d contiguous=%d, want 9 and 4", oc.Claimed, oc.Contiguous)
	}

	// And the honest prefix is still there to recover from.
	if got, err := recovery.DurablePrefix(ctx, store, vol, 1); err != nil || got != 4 {
		t.Fatalf("DurablePrefix = %d err=%v, want 4 — the surviving prefix is still recoverable", got, err)
	}
}

// TestSummaryFromAnotherVolumeOrEpochIsIgnored: the summary key is overwritable and
// its fields were never checked against the volume being recovered, so one stray
// object could brick a volume's recovery until a human deleted it.
func TestSummaryFromAnotherVolumeOrEpochIsIgnored(t *testing.T) {
	ctx := t.Context()
	vol := vol7()
	other := vol
	other[15] = 0xEE

	tests := []struct {
		name    string
		summary wal.Summary
	}{
		{"belongs to another volume", wal.Summary{
			VolumeID: format.UUIDString(other), Epoch: 1, DurableSequence: 999,
		}},
		{"belongs to another epoch", wal.Summary{
			VolumeID: format.UUIDString(vol), Epoch: 7, DurableSequence: 999,
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := sim.NewObjectStore()
			k, b := craftObject(t, vol, 1, 1, 3, nil)
			putObject(t, store, k, b)
			putSummary(t, store, wal.SummaryKey(vol, 1), tc.summary)

			got, err := recovery.DurablePoint(ctx, store, vol, 1)
			if err != nil {
				t.Fatalf("a summary that is not this volume/epoch's must be ignored, not obeyed: %v", err)
			}
			if got != 3 {
				t.Fatalf("DurablePoint = %d, want 3", got)
			}
		})
	}
}

// TestSummaryClaimingLessDoesNotLowerTheDurablePoint pins today's behaviour before
// the §22.1 optimisation (starting the LIST from the summary) can turn a harmless
// skip into a silent under-report. The objects are the authority; a stale summary
// that names fewer of them must not shrink what S3 can prove.
func TestSummaryClaimingLessDoesNotLowerTheDurablePoint(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	vol := vol7()

	for _, span := range [][2]uint64{{1, 2}, {3, 4}, {5, 6}} {
		k, b := craftObject(t, vol, 1, span[0], span[1], nil)
		putObject(t, store, k, b)
	}
	// A summary written before the last two objects landed.
	putSummary(t, store, wal.SummaryKey(vol, 1), wal.Summary{
		VolumeID: format.UUIDString(vol), Epoch: 1, DurableSequence: 2,
	})

	got, err := recovery.DurablePoint(ctx, store, vol, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got != 6 {
		t.Fatalf("DurablePoint = %d, want 6 — a lagging summary must not hide durable objects", got)
	}
}

// TestUnreadableSummaryIsNotSilentlyNoSummary: a store failure on the summary GET is
// not the same as there being no summary, for the same reason it is not on the
// boundary object — one is "nothing to cross-check against", the other is "we could
// not look".
func TestUnreadableSummaryIsNotSilentlyNoSummary(t *testing.T) {
	ctx := t.Context()
	base := sim.NewObjectStore()
	vol := vol7()

	k, b := craftObject(t, vol, 1, 1, 3, nil)
	putObject(t, base, k, b)
	putSummary(t, base, wal.SummaryKey(vol, 1), wal.Summary{
		VolumeID: format.UUIDString(vol), Epoch: 1, DurableSequence: 3,
	})

	store := summaryFaultStore{Store: base}
	if got, err := recovery.DurablePoint(ctx, store, vol, 1); err == nil {
		t.Fatalf("DurablePoint returned %d with no error while the summary could not be read", got)
	}
}

// summaryFaultStore fails the summary GET the way a throttled backend does (§24).
type summaryFaultStore struct {
	objectstore.Store
}

func (s summaryFaultStore) Get(ctx context.Context, key string) ([]byte, error) {
	if key == wal.SummaryKey(vol7(), 1) {
		return nil, sim.ErrThrottled
	}
	return s.Store.Get(ctx, key)
}
