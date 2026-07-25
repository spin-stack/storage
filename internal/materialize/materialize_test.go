package materialize_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/checkpoint"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ioclass"
	"github.com/spin-stack/storage/internal/materialize"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/snapshot"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

func v7Vol() [16]byte {
	var v [16]byte
	v[6], v[8] = 0x70, 0x80
	return v
}

type world struct {
	store *sim.ObjectStore
	clk   *sim.Clock
	vol   [16]byte
	log   *wal.Log
}

func newWorld(t *testing.T, enc *wal.Encryption) *world {
	t.Helper()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := v7Vol()
	d := sim.NewDisk()
	f, err := d.Create("wal/active.wal")
	if err != nil {
		t.Fatal(err)
	}
	l := wal.NewLog(f, clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), leaseOK{})
	if enc != nil {
		l.EnableEncryption(enc)
	}
	return &world{store: store, clk: clk, vol: vol, log: l}
}

// writeAndFlush appends one record and closes the batch, so each call produces one
// WAL object in the store.
func (w *world) writeAndFlush(t *testing.T, offset uint64, payload string) {
	t.Helper()
	if _, err := w.log.Write(offset, []byte(payload), 0); err != nil {
		t.Fatal(err)
	}
	if err := w.log.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func (w *world) snapshot(t *testing.T, id string) snapshot.Manifest {
	t.Helper()
	m, _, err := snapshot.NewSnapshotter(w.store, w.clk).Create(context.Background(), w.log, w.vol, 1, id, "")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func readAt(m interface{ Read(uint64, []byte) }, offset uint64, n int) string {
	buf := make([]byte, n)
	m.Read(offset, buf)
	return string(buf)
}

// TestFromSnapshotRebuildsExactState is the core of §20/§22.3 cold cross-host: the
// destination reconstructs the volume from the object store alone — it is given
// nothing but a Store — and the result is the source's state at the captured
// sequence.
func TestFromSnapshotRebuildsExactState(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "alpha")
	w.writeAndFlush(t, 64, "beta!")
	m := w.snapshot(t, "snap-1")

	mat := materialize.New(w.store, nil, nil)
	view, prog, err := mat.FromSnapshot(ctx, m.VolumeID, m.SnapshotID)
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if got := readAt(view, 0, 5); got != "alpha" {
		t.Fatalf("offset 0 = %q, want alpha", got)
	}
	if got := readAt(view, 64, 5); got != "beta!" {
		t.Fatalf("offset 64 = %q, want beta!", got)
	}
	if prog.Objects != len(m.Objects) || prog.Bytes <= 0 {
		t.Fatalf("progress = %+v, want %d objects and non-zero bytes", prog, len(m.Objects))
	}
	if prog.UpTo != m.TargetSequence {
		t.Fatalf("covered up to %d, want the captured sequence %d", prog.UpTo, m.TargetSequence)
	}
}

// TestFromSnapshotDecryptsWithVolumeDEK: an encrypted volume materializes to the
// same plaintext on the destination (§15, INV-15 — the bytes in S3 stay sealed).
func TestFromSnapshotDecryptsWithVolumeDEK(t *testing.T) {
	ctx := context.Background()
	dek, err := crypto.GenerateDEK(&fixedReader{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	vol := v7Vol()
	enc := &wal.Encryption{DEK: dek, VolumeID: vol}
	w := newWorld(t, enc)
	w.writeAndFlush(t, 0, "secret-payload")
	m := w.snapshot(t, "snap-enc")

	view, _, err := materialize.New(w.store, nil, enc).FromSnapshot(ctx, m.VolumeID, m.SnapshotID)
	if err != nil {
		t.Fatalf("materialize encrypted: %v", err)
	}
	if got := readAt(view, 0, 14); got != "secret-payload" {
		t.Fatalf("decrypted state = %q", got)
	}
}

// TestFromSnapshotRefusesMissingObject: a referenced object that is gone must be a
// hard failure, never a silently partial volume (§5.8 authority, §29.4).
func TestFromSnapshotRefusesMissingObject(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "alpha")
	w.writeAndFlush(t, 64, "beta!")
	m := w.snapshot(t, "snap-1")

	if err := w.store.Delete(ctx, m.Objects[0]); err != nil {
		t.Fatal(err)
	}
	view, _, err := materialize.New(w.store, nil, nil).FromSnapshot(ctx, m.VolumeID, m.SnapshotID)
	if !errors.Is(err, materialize.ErrMissingObject) {
		t.Fatalf("want ErrMissingObject, got %v", err)
	}
	if view != nil {
		t.Fatal("a failed materialization must not return a partial view")
	}
}

// TestFromSnapshotRefusesSequenceGap: a manifest whose objects do not form a
// contiguous run is refused — the destination never boots on a hole (§22.1).
func TestFromSnapshotRefusesSequenceGap(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "one")
	w.writeAndFlush(t, 64, "two")
	w.writeAndFlush(t, 128, "three")
	full := w.snapshot(t, "snap-full")
	if len(full.Objects) < 3 {
		t.Fatalf("expected 3 WAL objects, got %d", len(full.Objects))
	}

	// A well-formed manifest (its digest matches) that skips the middle object.
	gapped := snapshot.Manifest{
		SnapshotID:     "snap-gap",
		VolumeID:       full.VolumeID,
		Epoch:          full.Epoch,
		TargetSequence: full.TargetSequence,
		Objects:        []string{full.Objects[0], full.Objects[2]},
	}
	gapped.RootDigest = snapshot.Digest(gapped.TargetSequence, gapped.Objects)
	if err := snapshot.Publish(ctx, w.store, gapped); err != nil {
		t.Fatal(err)
	}

	view, _, err := materialize.New(w.store, nil, nil).FromSnapshot(ctx, gapped.VolumeID, gapped.SnapshotID)
	if !errors.Is(err, materialize.ErrSequenceGap) {
		t.Fatalf("want ErrSequenceGap, got %v", err)
	}
	if view != nil {
		t.Fatal("a gapped manifest must not yield a view")
	}
}

// TestFromSnapshotRefusesDigestMismatch: the manifest must be self-consistent
// before a single byte is fetched (INV-16 — a published snapshot never changes, so
// a mismatch means tampering or corruption).
func TestFromSnapshotRefusesDigestMismatch(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "alpha")
	full := w.snapshot(t, "snap-1")

	tampered := full
	tampered.SnapshotID = "snap-tampered"
	tampered.RootDigest = "deadbeef"
	if err := snapshot.Publish(ctx, w.store, tampered); err != nil {
		t.Fatal(err)
	}
	if _, _, err := materialize.New(w.store, nil, nil).FromSnapshot(ctx, tampered.VolumeID, tampered.SnapshotID); !errors.Is(err, materialize.ErrDigestMismatch) {
		t.Fatalf("want ErrDigestMismatch, got %v", err)
	}
}

// TestFromCheckpointRebuildsLiveVolume: the other materialization source — the
// latest verified checkpoint of a live volume (§21.1, §22.3).
func TestFromCheckpointRebuildsLiveVolume(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "alpha")
	w.writeAndFlush(t, 64, "beta!")

	cp, err := checkpoint.NewCheckpointer(w.store).Create(ctx, w.log, w.vol, 1)
	if err != nil {
		t.Fatal(err)
	}
	view, prog, err := materialize.New(w.store, nil, nil).FromCheckpoint(ctx, format.UUIDString(w.vol), cp.Epoch, cp.DurableSequence)
	if err != nil {
		t.Fatalf("materialize from checkpoint: %v", err)
	}
	if got := readAt(view, 64, 5); got != "beta!" {
		t.Fatalf("offset 64 = %q, want beta!", got)
	}
	if prog.UpTo != cp.DurableSequence {
		t.Fatalf("covered up to %d, want %d", prog.UpTo, cp.DurableSequence)
	}
}

// TestMaterializationYieldsToForeground is INV-17 on the new background consumer:
// while a foreground/flush op is in flight the fetch is refused outright (the
// reconciler retries), and it proceeds once the data path is idle.
func TestMaterializationYieldsToForeground(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "alpha")
	m := w.snapshot(t, "snap-1")

	sched := ioclass.NewScheduler(1000)
	mat := materialize.New(w.store, sched, nil)

	sched.Begin(ioclass.Foreground)
	if _, _, err := mat.FromSnapshot(ctx, m.VolumeID, m.SnapshotID); !errors.Is(err, materialize.ErrThrottled) {
		t.Fatalf("materialization must yield to foreground, got %v", err)
	}
	sched.End(ioclass.Foreground)

	if _, _, err := mat.FromSnapshot(ctx, m.VolumeID, m.SnapshotID); err != nil {
		t.Fatalf("materialization should proceed when idle: %v", err)
	}
}

// TestMaterializationRespectsBackgroundBudget: over-budget is a yield, not an
// unbounded background burst (§5.9).
func TestMaterializationRespectsBackgroundBudget(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "one")
	w.writeAndFlush(t, 64, "two")
	m := w.snapshot(t, "snap-1")

	sched := ioclass.NewScheduler(1) // one object per window
	mat := materialize.New(w.store, sched, nil)
	if _, _, err := mat.FromSnapshot(ctx, m.VolumeID, m.SnapshotID); !errors.Is(err, materialize.ErrThrottled) {
		t.Fatalf("want ErrThrottled when the budget runs out, got %v", err)
	}
	sched.Refill()
	sched.Refill()
}

// TestMissingManifestIsAnError: nothing to materialize from is an error, not an
// empty volume.
func TestMissingManifestIsAnError(t *testing.T) {
	store := sim.NewObjectStore()
	if _, _, err := materialize.New(store, nil, nil).FromSnapshot(context.Background(), format.UUIDString(v7Vol()), "absent"); err == nil {
		t.Fatal("materializing from a missing manifest must fail")
	}
	if _, _, err := materialize.New(store, nil, nil).FromCheckpoint(context.Background(), format.UUIDString(v7Vol()), 1, 7); err == nil {
		t.Fatal("materializing from a missing checkpoint must fail")
	}
}

// TestFromEpochRebuildsDurablePrefix is the source an evacuation uses: no snapshot,
// no cooperation from the host being drained — just the durable prefix in S3.
func TestFromEpochRebuildsDurablePrefix(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "alpha")
	w.writeAndFlush(t, 64, "beta!")

	view, prog, err := materialize.New(w.store, nil, nil).FromEpoch(ctx, w.vol, 1)
	if err != nil {
		t.Fatalf("from epoch: %v", err)
	}
	if got := readAt(view, 0, 5); got != "alpha" {
		t.Fatalf("offset 0 = %q", got)
	}
	if prog.UpTo != w.log.Watermarks().Durable {
		t.Fatalf("covered up to %d, want the durable point %d", prog.UpTo, w.log.Watermarks().Durable)
	}
}

// TestFromEpochRefusesLyingSummary: the summary must never claim more than the
// contiguous prefix provides (§22.1) — materialization inherits that guard.
func TestFromEpochRefusesLyingSummary(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "alpha")

	lie, err := json.Marshal(wal.Summary{
		VolumeID: format.UUIDString(w.vol), Epoch: 1, DurableSequence: 999,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.store.Put(ctx, wal.SummaryKey(w.vol, 1), lie, objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := materialize.New(w.store, nil, nil).FromEpoch(ctx, w.vol, 1); err == nil {
		t.Fatal("a summary claiming more than the contiguous prefix must be refused")
	}
}

// TestRefusesUnparseableObject: a referenced key holding something that is not a WAL
// object fails before any state is produced.
func TestRefusesUnparseableObject(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "alpha")

	junkKey := "wal/" + format.UUIDString(w.vol) + "/1/junk.wal"
	if _, err := w.store.Put(ctx, junkKey, []byte("too short"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	m := snapshot.Manifest{
		SnapshotID: "snap-junk", VolumeID: format.UUIDString(w.vol), Epoch: 1,
		TargetSequence: 1, Objects: []string{junkKey},
	}
	m.RootDigest = snapshot.Digest(m.TargetSequence, m.Objects)
	if err := snapshot.Publish(ctx, w.store, m); err != nil {
		t.Fatal(err)
	}
	view, _, err := materialize.New(w.store, nil, nil).FromSnapshot(ctx, m.VolumeID, m.SnapshotID)
	if err == nil || view != nil {
		t.Fatalf("a short/unparseable object must fail materialization, got view=%v err=%v", view, err)
	}
}

// TestWrongDEKFailsClosed: materializing an encrypted volume with the wrong key
// fails rather than producing garbage state (§15, INV-15 fails closed).
func TestWrongDEKFailsClosed(t *testing.T) {
	ctx := context.Background()
	dek, err := crypto.GenerateDEK(&fixedReader{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	vol := v7Vol()
	w := newWorld(t, &wal.Encryption{DEK: dek, VolumeID: vol})
	w.writeAndFlush(t, 0, "secret-payload")
	m := w.snapshot(t, "snap-enc")

	otherDEK, err := crypto.GenerateDEK(&fixedReader{b: 200}, 1)
	if err != nil {
		t.Fatal(err)
	}
	view, _, err := materialize.New(w.store, nil, &wal.Encryption{DEK: otherDEK, VolumeID: vol}).
		FromSnapshot(ctx, m.VolumeID, m.SnapshotID)
	if err == nil || view != nil {
		t.Fatalf("the wrong DEK must fail closed: view=%v err=%v", view, err)
	}
}

// TestObjectStoreErrorsSurface: a backend error (throttling, §24) is reported as
// itself — it is not a missing object and not a partial volume.
func TestObjectStoreErrorsSurface(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "alpha")
	m := w.snapshot(t, "snap-1")

	mat := materialize.New(w.store, nil, nil)
	// One op for the manifest GET, the next one (the object GET) is throttled.
	w.store.InjectThrottle(2)
	view, _, err := mat.FromSnapshot(ctx, m.VolumeID, m.SnapshotID)
	if err == nil || errors.Is(err, materialize.ErrMissingObject) || view != nil {
		t.Fatalf("a throttled backend must surface as an error: view=%v err=%v", view, err)
	}

	// The same on the epoch path, where the failure happens during the LIST.
	w.store.InjectThrottle(1)
	if _, _, err := mat.FromEpoch(ctx, w.vol, 1); err == nil {
		t.Fatal("a throttled LIST must fail the materialization")
	}
}

// TestFromCheckpointRefusesDigestMismatch mirrors the snapshot guard on the other
// materialization source.
func TestFromCheckpointRefusesDigestMismatch(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	w.writeAndFlush(t, 0, "alpha")

	cp := checkpoint.Checkpoint{
		VolumeID: format.UUIDString(w.vol), Epoch: 1, DurableSequence: 42,
		Objects: []string{"wal/x"}, RootDigest: "not-the-digest",
	}
	if err := checkpoint.Publish(ctx, w.store, cp); err != nil {
		t.Fatal(err)
	}
	if _, _, err := materialize.New(w.store, nil, nil).FromCheckpoint(ctx, cp.VolumeID, 1, 42); !errors.Is(err, materialize.ErrDigestMismatch) {
		t.Fatalf("want ErrDigestMismatch, got %v", err)
	}
}

// fixedReader is a deterministic key source for the encrypted case.
type fixedReader struct{ b byte }

func (r *fixedReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.b
		r.b++
	}
	return len(p), nil
}

// leaseOK is the fence for tests that are not about fencing: remote durability
// requires a lease checker (DEV-0004), and these hold a valid one.
type leaseOK struct{}

func (leaseOK) Valid() bool { return true }
