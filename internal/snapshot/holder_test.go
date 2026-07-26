package snapshot_test

import (
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/snapshot"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// Taking a snapshot is a publication into a namespace exactly one host may claim.
// snapshots/<vol>/<snap>/manifest.json is create-only and the manifest is built from
// the *live* log, so the reasoning has been that only the host holding the volume can
// produce one (§19) — but that is an argument about who has the log, not a check.
// Nothing between Create and the PUT asks the object store whose epoch this is.
//
// The epoch *number* cannot answer it either. The two durable records of a promotion —
// volumes.primary_host_id in PostgreSQL and the S3 epoch object — are written by two
// different steps of §12.3, so a promoter that died between them, or one that was
// overtaken, leaves them naming different hosts at the same epoch. A host fenced that
// way still has its WAL file open, still reads "epoch 1", and publishes a manifest
// naming the *other* host's WAL objects. That manifest is immutable (INV-16) and is a
// GC root (§21.3, ADR-0012): it pins the fenced writer's view of the epoch forever, and
// a clone made from it rebuilds a state the live volume never had.
//
// Wave 3 closed the same hole in checkpoint and recovery with recovery.VerifyPublisher;
// these tests are what makes the snapshot publisher use it. The gate is expressed
// through an optional interface, so a missing HeldBy fails as a test rather than as a
// build break — the idiom internal/checkpoint/holder_test.go uses.

const (
	hostGranted = "00000000-0000-7000-8000-0000000000b1" // the host the epoch was granted to
	hostFenced  = "00000000-0000-7000-8000-0000000000b2" // the host a stale PostgreSQL row still names
)

// holderScoped is the publisher identity a snapshotter needs: which host it speaks for.
// Without it a snapshotter can only ask "is the number still mine?", which both hosts
// above answer yes to.
type holderScoped interface {
	HeldBy(hostID string) *snapshot.Snapshotter
}

func heldBy(t *testing.T, s *snapshot.Snapshotter, hostID string) *snapshot.Snapshotter {
	t.Helper()
	h, ok := any(s).(holderScoped)
	if !ok {
		t.Fatal("snapshot.Snapshotter cannot say which host it publishes for: no HeldBy (§12.4). " +
			"A snapshot is therefore gated on nothing at all, so the host a crashed promoter left " +
			"named in volumes.primary_host_id publishes an immutable manifest into the epoch the " +
			"object store granted to somebody else — pinning its own view of that epoch as a GC " +
			"root (INV-16, §21.3)")
	}
	return h.HeldBy(hostID)
}

// grantEpoch puts the epoch object at `ep`, granted to holder. An empty holder leaves
// the epoch owned by nobody, which is what Init writes for a volume that has just been
// created or rebuilt (§22.5).
func grantEpoch(t *testing.T, store objectstore.Store, volumeID string, ep uint64, holder string) {
	t.Helper()
	ctx := t.Context()
	es := epoch.NewStore(store)
	etag, err := es.Init(ctx, volumeID, ep-1)
	if err != nil {
		t.Fatalf("init epoch %d: %v", ep-1, err)
	}
	if _, err := es.Grant(ctx, volumeID, etag, ep, holder); err != nil {
		t.Fatalf("granting epoch %d to %q: %v", ep, holder, err)
	}
}

// snapshotWorld is a volume with two flushed WAL objects under epoch 1, ready to
// snapshot.
type snapshotWorld struct {
	store *sim.ObjectStore
	vol   [16]byte
	log   *wal.Log
	snap  *snapshot.Snapshotter
}

func newSnapshotWorld(t *testing.T) *snapshotWorld {
	t.Helper()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := v7Vol()
	l := remoteLog(t, store, clk, vol)
	for i := range 2 {
		if _, err := l.Write(uint64(i)*4096, []byte("payload"), 0); err != nil {
			t.Fatal(err)
		}
		if err := l.Flush(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	return &snapshotWorld{store: store, vol: vol, log: l, snap: snapshot.NewSnapshotter(store, clk)}
}

func (w *snapshotWorld) create(t *testing.T, s *snapshot.Snapshotter, id string) (snapshot.Manifest, error) {
	t.Helper()
	m, _, err := s.Create(t.Context(), w.log, w.vol, 1, id, "")
	return m, err
}

// TestAnUnnamedPublisherCannotSnapshotAHeldEpoch is the hole itself, and it needs none
// of the new surface to state: the epoch object says epoch 1 was granted to hostGranted,
// and a snapshotter that cannot prove it is that host publishes an immutable manifest
// into that epoch anyway, because nothing is ever asked.
func TestAnUnnamedPublisherCannotSnapshotAHeldEpoch(t *testing.T) {
	w := newSnapshotWorld(t)
	grantEpoch(t, w.store, format.UUIDString(w.vol), 1, hostGranted)

	_, err := w.create(t, w.snap, "00000000-0000-7000-8000-0000000000b8")
	if !errors.Is(err, epoch.ErrNotHolder) {
		t.Fatalf("Create = %v, want %v: a publisher that cannot name itself must not claim "+
			"an epoch the object store granted to %s", err, epoch.ErrNotHolder, hostGranted)
	}
	objs, lerr := w.store.List(t.Context(), "snapshots/")
	if lerr != nil {
		t.Fatal(lerr)
	}
	if len(objs) != 0 {
		t.Fatalf("a refused snapshot still published %d manifest(s): %v — and a manifest is "+
			"immutable (INV-16) and a GC root (§21.3), so it cannot be taken back", len(objs), objs)
	}
}

// TestOnlyTheEpochHolderMaySnapshot is the difference the number-only check cannot see:
// same volume, same epoch number, two hosts, one grant.
//
// Two of the cases must stay open, and they are the reason this is not simply
// "VerifyHolder everywhere". An epoch nobody was granted is legitimate — Init writes one
// for a fresh or rebuilt volume (§22.5). So is a volume with no epoch object at all:
// §12.4 makes the object defence in depth over §12.2 + the §7 term, not a precondition
// for publishing.
func TestOnlyTheEpochHolderMaySnapshot(t *testing.T) {
	vid := format.UUIDString(v7Vol())
	tests := []struct {
		name      string
		arrange   func(t *testing.T, store objectstore.Store)
		publisher string
		want      error
	}{
		{
			name:      "the host the epoch was granted to",
			arrange:   func(t *testing.T, s objectstore.Store) { grantEpoch(t, s, vid, 1, hostGranted) },
			publisher: hostGranted,
		},
		{
			name:      "the host a stale PostgreSQL row names, at the very same epoch",
			arrange:   func(t *testing.T, s objectstore.Store) { grantEpoch(t, s, vid, 1, hostGranted) },
			publisher: hostFenced,
			want:      epoch.ErrNotHolder,
		},
		{
			name:      "the holder, at an epoch that has since moved on",
			arrange:   func(t *testing.T, s objectstore.Store) { grantEpoch(t, s, vid, 2, hostGranted) },
			publisher: hostGranted,
			want:      epoch.ErrEpochChanged,
		},
		{
			name:      "an epoch nobody was granted (a fresh or rebuilt volume, §22.5)",
			arrange:   func(t *testing.T, s objectstore.Store) { grantEpoch(t, s, vid, 1, "") },
			publisher: hostGranted,
		},
		{
			name:      "no epoch object at all (§12.4: defence in depth, not a precondition)",
			arrange:   func(t *testing.T, s objectstore.Store) {},
			publisher: hostGranted,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newSnapshotWorld(t)
			tc.arrange(t, w.store)

			const snapID = "00000000-0000-7000-8000-0000000000b9"
			m, err := w.create(t, heldBy(t, w.snap, tc.publisher), snapID)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Create = %v, want %v", err, tc.want)
			}
			objs, lerr := w.store.List(t.Context(), "snapshots/")
			if lerr != nil {
				t.Fatal(lerr)
			}
			if tc.want != nil {
				if len(objs) != 0 {
					t.Fatalf("a refused snapshot still published %d manifest(s): %v", len(objs), objs)
				}
				return
			}
			if len(objs) != 1 || !m.DigestMatches() {
				t.Fatalf("the epoch's holder could not publish its own snapshot: manifests=%v m=%+v", objs, m)
			}
		})
	}
}

// TestHeldByLeavesTheReceiverAlone: scoping a snapshotter to a host must not mutate the
// one it was derived from, so a single Snapshotter can be scoped per volume without
// sharing state — and, more to the point, so scoping for one host cannot silently grant
// another host's snapshotter that identity.
func TestHeldByLeavesTheReceiverAlone(t *testing.T) {
	w := newSnapshotWorld(t)
	grantEpoch(t, w.store, format.UUIDString(w.vol), 1, hostGranted)

	granted := heldBy(t, w.snap, hostGranted)
	fenced := heldBy(t, w.snap, hostFenced)

	if _, err := w.create(t, fenced, "00000000-0000-7000-8000-0000000000ba"); !errors.Is(err, epoch.ErrNotHolder) {
		t.Fatalf("Create by the fenced host = %v, want %v", err, epoch.ErrNotHolder)
	}
	if _, err := w.create(t, granted, "00000000-0000-7000-8000-0000000000bb"); err != nil {
		t.Fatalf("scoping another snapshotter changed this one's identity: %v", err)
	}
}
