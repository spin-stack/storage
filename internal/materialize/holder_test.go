package materialize_test

import (
	"context"
	"testing"

	"github.com/spin-stack/storage/internal/checkpoint"
	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/materialize"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/wal/format"
)

// Materialization is a *read*. It never writes a byte into wal/<vol>/<epoch>/ or into
// any other namespace the epoch fences, so the holder check that gates the publishers
// must not be added here — and this file exists to keep it out.
//
// ADR-0008 is the reason. A drain moves a volume from the durable prefix of the epoch
// the *source* still holds: the destination materializes it while the source is still
// serving, before the fence, precisely so that the evacuation works when the source is
// dead, partitioned, or simply uncooperative. A destination that had to hold the epoch
// it is reading could never run that bulk pass, and the drain would be reduced to the
// snapshot-based flow ADR-0008 rejected. The same is true of §22.5 recovery tooling
// and of a clone, which read epochs they will never own.
//
// Exclusivity is enforced where it is needed — at the publications (checkpoint.Create,
// recovery.RecoveryPoint.WriteAs) and, for the state read here, by the immutability of
// the epoch boundary: a superseded epoch has a ceiling, so what a non-holder can read
// is exactly what the promotion adopted.

const holderElsewhere = "00000000-0000-7000-8000-0000000000c1"

// grantEpochTo puts the epoch object at `ep`, granted to holder — a host that is not
// the one doing the materializing.
func grantEpochTo(t *testing.T, store objectstore.Store, vol [16]byte, ep uint64, holder string) {
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

// TestMaterializationDoesNotRequireHoldingTheEpoch pins ADR-0008's bulk pass: every
// source a destination can be handed rebuilds an epoch held by another host.
func TestMaterializationDoesNotRequireHoldingTheEpoch(t *testing.T) {
	tests := []struct {
		name string
		// build prepares a source on the volume *before* the epoch is granted away,
		// then returns the materialization the destination runs.
		run func(t *testing.T, w *world) (uint64, error)
	}{
		{
			name: "FromEpoch — the drain's bulk pass (ADR-0008 step 1)",
			run: func(t *testing.T, w *world) (uint64, error) {
				grantEpochTo(t, w.store, w.vol, 1, holderElsewhere)
				_, prog, err := materialize.New(w.store, nil, nil).FromEpoch(context.Background(), w.vol, 1)
				return prog.UpTo, err
			},
		},
		{
			name: "FromCheckpoint — warm-standby hydration (§21.1, §22.3)",
			run: func(t *testing.T, w *world) (uint64, error) {
				ctx := context.Background()
				cp, err := checkpoint.NewCheckpointer(w.store).Create(ctx, w.log, w.vol, 1)
				if err != nil {
					t.Fatalf("publishing the source checkpoint: %v", err)
				}
				grantEpochTo(t, w.store, w.vol, 1, holderElsewhere)
				_, prog, err := materialize.New(w.store, nil, nil).
					FromCheckpoint(ctx, cp.VolumeID, cp.Epoch, cp.DurableSequence)
				return prog.UpTo, err
			},
		},
		{
			name: "FromSnapshot — a cross-host clone (§20)",
			run: func(t *testing.T, w *world) (uint64, error) {
				m := w.snapshot(t, "snap-holder")
				grantEpochTo(t, w.store, w.vol, 1, holderElsewhere)
				_, prog, err := materialize.New(w.store, nil, nil).
					FromSnapshot(context.Background(), m.VolumeID, m.SnapshotID)
				return prog.UpTo, err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t, nil)
			w.writeAndFlush(t, 0, "alpha")
			w.writeAndFlush(t, 64, "beta!")

			upTo, err := tc.run(t, w)
			if err != nil {
				t.Fatalf("a destination cannot rebuild an epoch another host holds: %v — "+
					"that is ADR-0008's bulk pass, and without it a drain cannot evacuate "+
					"a host that is dead or uncooperative", err)
			}
			if upTo != 2 {
				t.Fatalf("materialized up to %d, want 2", upTo)
			}
		})
	}
}
