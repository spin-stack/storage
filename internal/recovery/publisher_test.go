package recovery_test

import (
	"context"
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal/format"
)

// The epoch boundary is the most irreversible publication in the system: create-only,
// never rewritten, and the floor the next epoch derives from it. §12.5 says it is
// written by the promoted writer — the host the new epoch was *granted* to — but
// nothing in this package ever asked who the caller is, so any process that computed
// the right epoch number could write it.
//
// That is precisely the split wave 2 named. PostgreSQL's volumes.primary_host_id and
// the S3 epoch object are two records of one promotion (§12.3), written by two steps;
// a promoter that died between them leaves them naming different hosts at the same
// epoch. The host the stale row names then records the boundary of an epoch it was
// never granted — and it is immutable, so the host that actually holds the epoch can
// never correct it.

const (
	boundaryHolder = "00000000-0000-7000-8000-0000000000b1"
	boundaryOther  = "00000000-0000-7000-8000-0000000000b2"
)

// boundaryAuthor is the missing surface: a boundary write that records who is making
// it, so it can be checked against the grant.
type boundaryAuthor interface {
	WriteAs(ctx context.Context, store objectstore.Store, volumeID [16]byte, newEpoch uint64, hostID string) error
}

func writeBoundaryAs(t *testing.T, rp recovery.RecoveryPoint, store objectstore.Store,
	vol [16]byte, newEpoch uint64, hostID string,
) error {
	t.Helper()
	a, ok := any(rp).(boundaryAuthor)
	if !ok {
		t.Fatal("recovery cannot record who is writing an epoch boundary: no RecoveryPoint.WriteAs (§12.5). " +
			"Any caller holding the right epoch number may write the create-only boundary, " +
			"including the host a crashed promoter left named in PostgreSQL while the object " +
			"store granted the epoch to somebody else — and the object cannot be rewritten")
	}
	return a.WriteAs(context.Background(), store, vol, newEpoch, hostID)
}

// grantEpoch puts the epoch object at `ep` for the volume, granted to holder. An empty
// holder is the epoch Init writes for a volume nobody has been granted yet (§22.5).
func grantEpoch(t *testing.T, store objectstore.Store, vol [16]byte, ep uint64, holder string) {
	t.Helper()
	ctx := context.Background()
	es := epoch.NewStore(store)
	vid := format.UUIDString(vol)
	etag, err := es.Init(ctx, vid, ep-1)
	if err != nil {
		t.Fatalf("init epoch %d: %v", ep-1, err)
	}
	if _, err := es.Grant(ctx, vid, etag, ep, holder); err != nil {
		t.Fatalf("granting epoch %d to %q: %v", ep, holder, err)
	}
}

// TestOnlyTheEpochHolderMayRecordItsBoundary. The two open cases are deliberate and
// are the reason this is not "VerifyHolder everywhere": an epoch nobody was granted is
// legitimate (Init, §22.5), and a volume with no epoch object at all is safe by §12.2
// plus the §7 term — §12.4 makes the object defence in depth, not a precondition.
func TestOnlyTheEpochHolderMayRecordItsBoundary(t *testing.T) {
	tests := []struct {
		name    string
		arrange func(t *testing.T, store objectstore.Store, vol [16]byte)
		author  string
		want    error
	}{
		{
			name:    "the host epoch 2 was granted to",
			arrange: func(t *testing.T, s objectstore.Store, v [16]byte) { grantEpoch(t, s, v, 2, boundaryHolder) },
			author:  boundaryHolder,
		},
		{
			name:    "the host a stale PostgreSQL row names, at the very same epoch",
			arrange: func(t *testing.T, s objectstore.Store, v [16]byte) { grantEpoch(t, s, v, 2, boundaryHolder) },
			author:  boundaryOther,
			want:    epoch.ErrNotHolder,
		},
		{
			name:    "an author that will not name itself",
			arrange: func(t *testing.T, s objectstore.Store, v [16]byte) { grantEpoch(t, s, v, 2, boundaryHolder) },
			author:  "",
			want:    epoch.ErrNotHolder,
		},
		{
			name:    "the holder, after a third promotion overtook it",
			arrange: func(t *testing.T, s objectstore.Store, v [16]byte) { grantEpoch(t, s, v, 3, boundaryHolder) },
			author:  boundaryHolder,
			want:    epoch.ErrEpochChanged,
		},
		{
			name:    "an epoch nobody was granted (a fresh or rebuilt volume, §22.5)",
			arrange: func(t *testing.T, s objectstore.Store, v [16]byte) { grantEpoch(t, s, v, 2, "") },
			author:  boundaryHolder,
		},
		{
			name:    "no epoch object at all (§12.4: defence in depth, not a precondition)",
			arrange: func(t *testing.T, s objectstore.Store, v [16]byte) {},
			author:  boundaryHolder,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := sim.NewObjectStore()
			vol := vol7()
			tc.arrange(t, store, vol)

			rp := recovery.RecoveryPoint{PrevEpoch: 1, RecoveredUpTo: 7}
			err := writeBoundaryAs(t, rp, store, vol, 2, tc.author)
			if !errors.Is(err, tc.want) {
				t.Fatalf("WriteAs = %v, want %v", err, tc.want)
			}
			got, rerr := recovery.ReadRecoveryPoint(ctx, store, vol, 2)
			if tc.want != nil {
				// Nothing may be left behind: the object is create-only, so a boundary
				// written by the wrong host is a floor nobody can raise again.
				if !errors.Is(rerr, objectstore.ErrNotFound) {
					t.Fatalf("a refused boundary was written anyway: %+v (err %v)", got, rerr)
				}
				return
			}
			if rerr != nil {
				t.Fatalf("the accepted boundary is not readable: %v", rerr)
			}
			if got != rp {
				t.Fatalf("boundary = %+v, want %+v", got, rp)
			}
		})
	}
}

// TestTheBoundaryGuardsStillApplyToItsHolder: naming the author adds a check, it does
// not replace the ones that were already there. A holder writing a boundary below what
// its predecessor already established is still refused (§12.5, INV-12).
func TestTheBoundaryGuardsStillApplyToItsHolder(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	vol := vol7()

	if err := recovery.WriteRecoveryPoint(ctx, store, vol, 2, 1, 10); err != nil {
		t.Fatal(err)
	}
	grantEpoch(t, store, vol, 3, boundaryHolder)

	rp := recovery.RecoveryPoint{PrevEpoch: 2, RecoveredUpTo: 4}
	err := writeBoundaryAs(t, rp, store, vol, 3, boundaryHolder)
	if !errors.Is(err, recovery.ErrBoundaryRegression) {
		t.Fatalf("WriteAs = %v, want ErrBoundaryRegression", err)
	}
}
