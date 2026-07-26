package dst

// Garbage-collection and anchor-reachability scenarios (§21) — see scenarios_drain.go
// for why the list is split by area.

import (
	"context"
	"fmt"
	"time"

	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/gc"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/snapshot"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

func gcScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "gc-keeps-a-superseded-epochs-snapshot", Run: scenarioGCKeepsSupersededEpochSnapshot},
	}
}

func gcCheckers() []Checker { return nil }

// scenarioGCKeepsSupersededEpochSnapshot is ADR-0012: a sweep whose listing has not
// caught up with a manifest must not destroy the snapshot that manifest publishes.
//
// The sequence is one an ordinary promotion produces:
//
//  1. the writer ACKs sequences 1 and 2 and publishes a snapshot targeting 2 — §21.1
//     writes the objects first and the manifest last, so the manifest is always the
//     youngest key involved;
//  2. a promotion closes the epoch and records its boundary at 1, because the listing
//     the promoted writer read had not caught up either. That object is create-only,
//     so the number is permanent (§12.5);
//  3. the GC sweeps. The epoch's durable point is clamped to the boundary, so the
//     object carrying sequence 2 sits outside every durable prefix and the only thing
//     holding it up is the manifest — which this sweep's LIST does not return.
//
// Marking that object does not merely lose bytes: it changes a snapshot that is
// already PUBLISHED, so the scenario reports it as a snapshot mutation and the
// existing immutable-snapshot checker (INV-16) is the one that fails.
func scenarioGCKeepsSupersededEpochSnapshot(s *Sim) error {
	ctx := context.Background()
	var vol [16]byte
	vol[6], vol[8] = 0x70, 0x80
	vol[15] = 0xc5
	vid := format.UUIDString(vol)
	const snapID = "00000000-0000-7000-8000-0000000000c5"

	if err := descriptor.Write(ctx, s.Store, descriptor.Descriptor{
		VolumeID: vid, SizeBytes: 1 << 20, BlockSize: 65536, CurrentEpoch: 1,
		KEKID: "k", DEKWrapped: []byte{1},
	}); err != nil {
		return err
	}

	f, err := s.Disk.Create("wal/superseded.wal")
	if err != nil {
		return err
	}
	l := wal.NewLog(f, s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(s.Store, 5), alwaysValidLease{})
	for i := range 2 {
		if _, err := l.Write(uint64(i)*4096, []byte("acked before the move"), 0); err != nil {
			return err
		}
		if err := l.Flush(ctx); err != nil {
			return err
		}
	}

	// The backend defect the scenario turns on: from here a fresh PUT is GET-visible
	// and LIST-invisible. The manifest is written under it; the WAL objects it names
	// were listed long before.
	s.Store.SetEventualList(true)
	s.Emit(Event{Kind: EventFault, Msg: "LIST no longer returns freshly written keys"})

	m, _, err := snapshot.NewSnapshotter(s.Store, s.Clock).Create(ctx, l, vol, 1, snapID, "")
	if err != nil {
		return err
	}
	if len(m.Objects) == 0 || m.TargetSequence < 2 {
		return fmt.Errorf("setup: the snapshot should anchor the ACKed objects, got %+v", m)
	}

	// The promotion closes epoch 1 below the published snapshot's target.
	if err := recovery.WriteRecoveryPoint(ctx, s.Store, vol, 2, 1, m.TargetSequence-1); err != nil {
		return err
	}
	s.Notef("epoch 1 closed at %d, below snapshot %s's target %d", m.TargetSequence-1, snapID, m.TargetSequence)

	s.Tick(48 * time.Hour) // every object is well past the grace period

	reachable, err := gc.Reachable(ctx, s.Store)
	if err != nil {
		return err
	}
	if _, err := gc.Mark(ctx, s.Store, s.Clock, reachable, time.Hour); err != nil {
		return err
	}

	// A published snapshot whose objects a sweep marked is a snapshot that changed.
	var lost []string
	for _, key := range m.Objects {
		_, err := s.Store.Get(ctx, key)
		s.Emit(Event{Kind: EventSnapshot, SnapshotMutated: err != nil, Msg: "post-sweep read of " + key})
		if err != nil {
			lost = append(lost, key)
		}
	}
	if len(lost) > 0 {
		return fmt.Errorf("the sweep marked %d object(s) of published snapshot %s: %v "+
			"(§21.3/INV-16: a listing that has not caught up is not a licence to destroy)",
			len(lost), snapID, lost)
	}

	// The manifest still describes itself, so the snapshot remains materializable.
	read, err := snapshot.Read(ctx, s.Store, vid, snapID)
	if err != nil {
		return err
	}
	if !read.DigestMatches() {
		return fmt.Errorf("the published manifest no longer matches its own contents: %+v", read)
	}
	s.Notef("a superseded epoch's published snapshot survived a sweep whose listing was behind")
	return nil
}
