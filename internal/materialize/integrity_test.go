package materialize_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/checkpoint"
	"github.com/spin-stack/storage/internal/materialize"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/snapshot"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// DEV-0003 was fixed inside internal/recovery and never mirrored here. Recovery
// validates every WAL object against its own header before it may raise the durable
// point; materialization — the cross-host clone, the drain-driven move and the
// warm-standby hydration — parsed the same header and trusted it.
//
// The manifest cannot cover for that: snapshot.Digest / checkpoint.Digest hash the
// *key strings*, so an object rewritten, torn, or restored to a wrong version after
// publication leaves the root digest matching. wal.Replay stops cleanly at a torn
// record (that is the normal crash-during-append case), so the destination boots a
// volume missing ACKed writes while Progress.UpTo reports the header's full span.

// rewriteObject rewrites a published WAL object in place — the shape of a bucket
// that was restored to a wrong version, a torn re-upload, or a mis-keyed PUT. The
// key does not change, so every manifest and checkpoint referencing it still passes
// its digest check.
func rewriteObject(t *testing.T, store *sim.ObjectStore, key string, mut func(h *format.ObjectHeader, payload *[]byte)) {
	t.Helper()
	ctx := context.Background()
	body, err := store.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	h, err := format.UnmarshalObjectHeader(body[:format.ObjectHeaderSize])
	if err != nil {
		t.Fatal(err)
	}
	payload := append([]byte(nil), body[format.ObjectHeaderSize:]...)
	mut(&h, &payload)
	head, err := h.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(ctx, key, append(head, payload...), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
}

// epoch2Log opens a second-epoch log for w continuing the sequence space after
// `boundary` — the state a volume is in right after a promotion (§12.5).
func epoch2Log(t *testing.T, w *world, boundary uint64) *wal.Log {
	t.Helper()
	d := sim.NewDisk()
	f, err := d.Create("wal/epoch2.wal")
	if err != nil {
		t.Fatal(err)
	}
	l := wal.NewLogAfter(f, w.clk, w.vol, 2, boundary, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(w.clk, w.vol, 2, 0, wal.DefaultBatchConfig()), wal.NewUploader(w.store, 5), leaseOK{})
	return l
}

// damage is the set of ways a referenced object can stop being what its header says
// while the manifest digest keeps matching. Every one of them must stop the
// materialization: a destination that boots on any of them is missing ACKed writes.
var damage = []struct {
	name string
	mut  func(h *format.ObjectHeader, payload *[]byte)
}{
	{"payload truncated after publication", func(_ *format.ObjectHeader, p *[]byte) {
		*p = (*p)[:len(*p)-9]
	}},
	{"one payload byte flipped", func(_ *format.ObjectHeader, p *[]byte) {
		(*p)[len(*p)-1] ^= 0xFF
	}},
	{"payload replaced by a wrong version of itself", func(_ *format.ObjectHeader, p *[]byte) {
		*p = append(*p, 0x00, 0x01, 0x02)
	}},
	{"header claims sequences the object does not carry", func(h *format.ObjectHeader, _ *[]byte) {
		h.LastSequence += 5
		h.RecordCount += 5
	}},
	{"object belongs to another volume", func(h *format.ObjectHeader, _ *[]byte) {
		h.VolumeID[15] ^= 0xEE
	}},
	{"object belongs to another epoch", func(h *format.ObjectHeader, _ *[]byte) {
		h.Epoch = 9
	}},
}

// TestFromSnapshotRefusesADamagedObject: a cross-host clone must never hand back a
// view built from an object that does not match its own header.
func TestFromSnapshotRefusesADamagedObject(t *testing.T) {
	for _, tc := range damage {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			w := newWorld(t, nil)
			w.writeAndFlush(t, 0, "alpha")
			w.writeAndFlush(t, 64, "beta!")
			m := w.snapshot(t, "snap-1")
			if len(m.Objects) < 2 {
				t.Fatalf("setup: %d objects", len(m.Objects))
			}

			rewriteObject(t, w.store, m.Objects[1], tc.mut)

			view, prog, err := materialize.New(w.store, nil, nil).FromSnapshot(ctx, m.VolumeID, m.SnapshotID)
			if !errors.Is(err, recovery.ErrObjectIntegrity) {
				t.Fatalf("want ErrObjectIntegrity, got view=%v prog=%+v err=%v", view != nil, prog, err)
			}
			if view != nil {
				t.Fatal("a damaged object must not yield a view")
			}
		})
	}
}

// TestFromCheckpointRefusesADamagedObject: the same on the live-volume source —
// this is the standby-hydration and drain-move path.
func TestFromCheckpointRefusesADamagedObject(t *testing.T) {
	for _, tc := range damage {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			w := newWorld(t, nil)
			w.writeAndFlush(t, 0, "alpha")
			w.writeAndFlush(t, 64, "beta!")

			cp, err := checkpoint.NewCheckpointer(w.store).Create(ctx, w.log, w.vol, 1)
			if err != nil {
				t.Fatal(err)
			}
			if len(cp.Objects) < 2 {
				t.Fatalf("setup: %d objects", len(cp.Objects))
			}
			rewriteObject(t, w.store, cp.Objects[1], tc.mut)

			view, _, err := materialize.New(w.store, nil, nil).
				FromCheckpoint(ctx, cp.VolumeID, cp.Epoch, cp.DurableSequence)
			if !errors.Is(err, recovery.ErrObjectIntegrity) {
				t.Fatalf("want ErrObjectIntegrity, got view=%v err=%v", view != nil, err)
			}
			if view != nil {
				t.Fatal("a damaged object must not yield a view")
			}
		})
	}
}

// TestTruncatedObjectIsNotReportedAsFullCoverage is the failure mode stated
// precisely: with a torn payload the replay stops early but the header still claims
// the full span, so the destination boots missing an ACKed write while Progress says
// it covered everything.
func TestTruncatedObjectIsNotReportedAsFullCoverage(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "alpha")
	w.writeAndFlush(t, 64, "beta!")
	m := w.snapshot(t, "snap-1")

	rewriteObject(t, w.store, m.Objects[1], func(_ *format.ObjectHeader, p *[]byte) {
		*p = (*p)[:len(*p)-9]
	})

	view, prog, err := materialize.New(w.store, nil, nil).FromSnapshot(ctx, m.VolumeID, m.SnapshotID)
	if err == nil {
		t.Fatalf("materialization succeeded on a torn object: covered up to %d, "+
			"but offset 64 reads %q — an ACKed write is missing and nothing says so",
			prog.UpTo, readAt(view, 64, 5))
	}
}

// TestCrossVolumeBodyIsNeverReplayed: a plaintext volume has no DEK to fail closed
// on, so a body belonging to another volume stored at this volume's key is replayed
// straight into the view unless the header is checked.
func TestCrossVolumeBodyIsNeverReplayed(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "alpha")
	w.writeAndFlush(t, 64, "beta!")
	m := w.snapshot(t, "snap-1")

	// Another volume's second object — same sequence number as ours, so the run stays
	// contiguous and nothing but the header says it is foreign.
	other := newWorld(t, nil)
	other.writeAndFlush(t, 0, "aaaaa")
	other.writeAndFlush(t, 64, "THEIR")
	otherObjs, err := other.store.List(ctx, "wal/")
	if err != nil || len(otherObjs) < 2 {
		t.Fatalf("setup: %d objects err=%v", len(otherObjs), err)
	}
	foreign, err := other.store.Get(ctx, otherObjs[1].Key)
	if err != nil {
		t.Fatal(err)
	}
	// Give it a different volume id so it is unmistakably not ours.
	h, err := format.UnmarshalObjectHeader(foreign[:format.ObjectHeaderSize])
	if err != nil {
		t.Fatal(err)
	}
	h.VolumeID[15] ^= 0x5A
	head, err := h.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.Put(ctx, m.Objects[1],
		append(head, foreign[format.ObjectHeaderSize:]...), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	view, _, err := materialize.New(w.store, nil, nil).FromSnapshot(ctx, m.VolumeID, m.SnapshotID)
	if err == nil {
		t.Fatalf("another volume's data was materialized into this one: offset 64 = %q", readAt(view, 64, 5))
	}
	if !errors.Is(err, recovery.ErrObjectIntegrity) {
		t.Fatalf("want ErrObjectIntegrity, got %v", err)
	}
}

// TestFromSnapshotRequiresThePrefixFloor: a manifest that lost its early objects —
// an aborted GC, a partial bucket restore, a mis-scoped lifecycle rule — describes a
// volume with a hole at the front. Every object it names can be perfectly valid and
// perfectly contiguous with the next; what is missing is the run's *start*.
func TestFromSnapshotRequiresThePrefixFloor(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "first")
	w.writeAndFlush(t, 64, "secnd")
	w.writeAndFlush(t, 128, "third")
	full := w.snapshot(t, "snap-full")
	if len(full.Objects) < 3 {
		t.Fatalf("setup: %d objects", len(full.Objects))
	}

	headless := snapshot.Manifest{
		SnapshotID:     "snap-headless",
		VolumeID:       full.VolumeID,
		Epoch:          full.Epoch,
		TargetSequence: full.TargetSequence,
		Objects:        full.Objects[1:], // sequence 1 simply does not exist any more
	}
	headless.RootDigest = snapshot.Digest(headless.TargetSequence, headless.Objects)
	if err := snapshot.Publish(ctx, w.store, headless); err != nil {
		t.Fatal(err)
	}

	view, prog, err := materialize.New(w.store, nil, nil).
		FromSnapshot(ctx, headless.VolumeID, headless.SnapshotID)
	if err == nil {
		t.Fatalf("a manifest missing sequence 1 materialized as a healthy volume "+
			"(covered up to %d; offset 0 reads %q)", prog.UpTo, readAt(view, 0, 5))
	}
	if !errors.Is(err, materialize.ErrPrefixFloor) {
		t.Fatalf("want ErrPrefixFloor, got %v", err)
	}
	if view != nil {
		t.Fatal("a manifest with a hole at the front must not yield a view")
	}
}

// TestFromCheckpointRequiresThePrefixFloor: the same on the checkpoint source, which
// is what a live move and a standby hydration replay.
func TestFromCheckpointRequiresThePrefixFloor(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "first")
	w.writeAndFlush(t, 64, "secnd")
	w.writeAndFlush(t, 128, "third")

	objects, err := recovery.ObjectKeysUpTo(ctx, w.store, w.vol, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) < 3 {
		t.Fatalf("setup: %d objects", len(objects))
	}
	headless := checkpoint.Checkpoint{
		VolumeID:        format.UUIDString(w.vol),
		Epoch:           1,
		DurableSequence: 3,
		Objects:         objects[1:], // sequence 1 is gone
	}
	headless.RootDigest = checkpoint.Digest(headless.DurableSequence, headless.Objects)
	if err := checkpoint.Publish(ctx, w.store, headless); err != nil {
		t.Fatal(err)
	}

	view, _, err := materialize.New(w.store, nil, nil).
		FromCheckpoint(ctx, headless.VolumeID, headless.Epoch, headless.DurableSequence)
	if !errors.Is(err, materialize.ErrPrefixFloor) {
		t.Fatalf("want ErrPrefixFloor, got view=%v err=%v", view != nil, err)
	}
}

// TestFloorFollowsTheEpochBoundary: in epoch N+1 the WAL legitimately starts above
// sequence 1 — at recovery_point(epoch).RecoveredUpTo+1 (§12.5) — so the floor must
// come from the boundary, not from a constant. Without this the fix for the previous
// test would simply break every promoted volume.
func TestFloorFollowsTheEpochBoundary(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "before-move")
	boundary := w.log.Watermarks().Durable

	if err := recovery.WriteRecoveryPoint(ctx, w.store, w.vol, 2, 1, boundary); err != nil {
		t.Fatal(err)
	}
	l2 := epoch2Log(t, w, boundary)
	if _, err := l2.Write(4096, []byte("after-move"), 0); err != nil {
		t.Fatal(err)
	}
	if err := l2.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	keys, err := recovery.ObjectKeysUpTo(ctx, w.store, w.vol, 2, boundary+1)
	if err != nil {
		t.Fatal(err)
	}
	m := snapshot.Manifest{
		SnapshotID: "snap-e2", VolumeID: format.UUIDString(w.vol), Epoch: 2,
		TargetSequence: boundary + 1, Objects: keys,
	}
	m.RootDigest = snapshot.Digest(m.TargetSequence, m.Objects)
	if err := snapshot.Publish(ctx, w.store, m); err != nil {
		t.Fatal(err)
	}

	view, prog, err := materialize.New(w.store, nil, nil).FromSnapshot(ctx, m.VolumeID, m.SnapshotID)
	if err != nil {
		t.Fatalf("a snapshot of epoch 2 starts above sequence 1 by design: %v", err)
	}
	if got := readAt(view, 4096, 10); got != "after-move" {
		t.Fatalf("offset 4096 = %q", got)
	}
	if prog.UpTo != m.TargetSequence {
		t.Fatalf("covered up to %d, want %d", prog.UpTo, m.TargetSequence)
	}
}

// boundaryFaultStore cannot serve the epoch-boundary object, so the floor a
// materialization must check against is unknown.
type boundaryFaultStore struct {
	objectstore.Store
}

func (s boundaryFaultStore) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasSuffix(key, "recovery-point.json") {
		return nil, sim.ErrThrottled
	}
	return s.Store.Get(ctx, key)
}

// TestUnknownFloorStopsTheMaterialization: with the boundary unreadable there is no
// way to tell a complete run from one missing its head, so the destination must not
// boot on a guess.
func TestUnknownFloorStopsTheMaterialization(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "alpha")
	m := w.snapshot(t, "snap-1")
	cp, err := checkpoint.NewCheckpointer(w.store).Create(ctx, w.log, w.vol, 1)
	if err != nil {
		t.Fatal(err)
	}

	mat := materialize.New(boundaryFaultStore{Store: w.store}, nil, nil)
	if view, _, err := mat.FromSnapshot(ctx, m.VolumeID, m.SnapshotID); err == nil || view != nil {
		t.Fatalf("FromSnapshot proceeded with an unknown floor: view=%v err=%v", view != nil, err)
	}
	if view, _, err := mat.FromCheckpoint(ctx, cp.VolumeID, cp.Epoch, cp.DurableSequence); err == nil || view != nil {
		t.Fatalf("FromCheckpoint proceeded with an unknown floor: view=%v err=%v", view != nil, err)
	}
}

// TestSourceDescribingAnotherVolumeIsRefused: the document sitting at our key is not
// automatically ours. A manifest or checkpoint naming a different volume is a
// mis-keyed or restored object, and replaying what it points at would fold another
// volume's history into this one.
func TestSourceDescribingAnotherVolumeIsRefused(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "alpha")
	full := w.snapshot(t, "snap-1")
	ours := format.UUIDString(w.vol)
	theirs := format.UUIDString(v7Other())

	foreignManifest := snapshot.Manifest{
		SnapshotID: "snap-foreign", VolumeID: theirs, Epoch: 1,
		TargetSequence: full.TargetSequence, Objects: full.Objects,
	}
	foreignManifest.RootDigest = snapshot.Digest(foreignManifest.TargetSequence, foreignManifest.Objects)
	putJSON(t, w.store, snapshot.ManifestKey(ours, "snap-foreign"), foreignManifest)

	if _, _, err := materialize.New(w.store, nil, nil).FromSnapshot(ctx, ours, "snap-foreign"); !errors.Is(err, recovery.ErrObjectIntegrity) {
		t.Fatalf("want ErrObjectIntegrity, got %v", err)
	}

	foreignCP := checkpoint.Checkpoint{
		VolumeID: theirs, Epoch: 1, DurableSequence: 1, Objects: full.Objects,
	}
	foreignCP.RootDigest = checkpoint.Digest(foreignCP.DurableSequence, foreignCP.Objects)
	putJSON(t, w.store, checkpoint.Key(ours, 1, 1), foreignCP)

	if _, _, err := materialize.New(w.store, nil, nil).FromCheckpoint(ctx, ours, 1, 1); !errors.Is(err, recovery.ErrObjectIntegrity) {
		t.Fatalf("want ErrObjectIntegrity, got %v", err)
	}
}

func v7Other() [16]byte {
	v := v7Vol()
	v[15] = 0xEE
	return v
}

func putJSON(t *testing.T, store *sim.ObjectStore, key string, v any) {
	t.Helper()
	body, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), key, body, objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
}

// TestProgressMustReachTheClaimedSequence is the cheap cross-check the whole class
// of gaps fails: whatever the source claims to cover, the replayed run has to
// actually reach it. A manifest whose objects stop short is a volume that boots
// missing its most recent ACKed writes.
func TestProgressMustReachTheClaimedSequence(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "first")
	w.writeAndFlush(t, 64, "secnd")
	w.writeAndFlush(t, 128, "third")
	full := w.snapshot(t, "snap-full")

	short := snapshot.Manifest{
		SnapshotID:     "snap-short",
		VolumeID:       full.VolumeID,
		Epoch:          full.Epoch,
		TargetSequence: full.TargetSequence, // claims 3
		Objects:        full.Objects[:2],    // carries 1..2
	}
	short.RootDigest = snapshot.Digest(short.TargetSequence, short.Objects)
	if err := snapshot.Publish(ctx, w.store, short); err != nil {
		t.Fatal(err)
	}

	view, prog, err := materialize.New(w.store, nil, nil).FromSnapshot(ctx, short.VolumeID, short.SnapshotID)
	if err == nil {
		t.Fatalf("a manifest claiming sequence %d materialized from a run ending at %d",
			short.TargetSequence, prog.UpTo)
	}
	if !errors.Is(err, materialize.ErrCoverageShort) {
		t.Fatalf("want ErrCoverageShort, got %v", err)
	}
	if view != nil {
		t.Fatal("short coverage must not yield a view")
	}
}
