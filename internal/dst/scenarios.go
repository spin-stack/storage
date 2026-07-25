package dst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spin-stack/storage/internal/checkpoint"
	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/gc"
	"github.com/spin-stack/storage/internal/ioclass"
	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/materialize"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/network"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/snapshot"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// deterministicReader yields seed-derived bytes for DEK material under DST.
type deterministicReader struct{ b byte }

func (r *deterministicReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.b
		r.b++
	}
	return len(p), nil
}

// MandatoryScenario is one entry in the §25.1 must-be-green-on-every-PR set. The
// data-path arms (real WAL records, real fencing) are added by later phases; here
// each scenario exercises the interface + fault machinery deterministically.
type MandatoryScenario struct {
	Name string
	Run  Scenario
}

// MandatoryScenarios is the set the `task dst` gate runs.
func MandatoryScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "lost-put-idempotent-retry", Run: scenarioLostPutIdempotent},
		{Name: "crash-around-fdatasync", Run: scenarioCrashAroundFdatasync},
		{Name: "clock-drift-beyond-skew", Run: scenarioClockDriftBeyondSkew},
		{Name: "network-partition", Run: scenarioNetworkPartition},
		{Name: "wal-write-path-no-put", Run: scenarioWALWritePathNoPut},
		{Name: "wal-backpressure", Run: scenarioWALBackpressure},
		{Name: "encrypted-wal-no-plaintext-leak", Run: scenarioEncryptedWALNoPlaintextLeak},
		{Name: "remote-flush-ordering", Run: scenarioRemoteFlushOrdering},
		{Name: "idempotent-batch-upload", Run: scenarioIdempotentBatchUpload},
		{Name: "lease-fences-durable-ack", Run: scenarioLeaseFencesDurableAck},
		{Name: "promotion-fencing-wait", Run: scenarioPromotionFencingWait},
		{Name: "fenced-writer-no-lost-ack", Run: scenarioFencedWriterNoLostAck},
		{Name: "recovery-authority-is-s3", Run: scenarioRecoveryAuthorityIsS3},
		{Name: "rebuild-metadata-from-s3", Run: scenarioRebuildMetadataFromS3},
		{Name: "snapshot-pausefree-immutable", Run: scenarioSnapshotPauseFreeImmutable},
		{Name: "same-host-clone-independent", Run: scenarioSameHostCloneIndependent},
		{Name: "checkpoint-then-truncate", Run: scenarioCheckpointThenTruncate},
		{Name: "gc-marks-orphans-not-live", Run: scenarioGCMarksOrphansNotLive},
		{Name: "background-yields-to-foreground", Run: scenarioBackgroundYields},
		{Name: "cross-host-materialization", Run: scenarioCrossHostMaterialization},
	}
}

// scenarioCrossHostMaterialization is §20 / §22.3 cold: a destination host that
// never had the volume rebuilds it from the object store alone. The source host is
// partitioned away for the whole materialization — there is no host-to-host path to
// fall back on (INV-08, §5.4) — the rebuilt state is byte-identical to the source's,
// and the work yields to foreground I/O (INV-17). A missing referenced object is a
// hard failure, never a half-materialized volume.
func scenarioCrossHostMaterialization(s *Sim) error {
	ctx := context.Background()
	var vol [16]byte
	vol[6], vol[8] = 0x70, 0x80
	vol[15] = 0xc1
	vols := format.UUIDString(vol)
	const snapID = "00000000-0000-7000-8000-0000000000c2"

	// Source host writes and publishes a snapshot.
	f, err := s.Disk.Create("wal/source.wal")
	if err != nil {
		return err
	}
	src := wal.NewLog(f, s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	src.EnableRemote(wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(s.Store, 5))
	if _, err := src.Write(0, []byte("cross-host"), 0); err != nil {
		return err
	}
	if _, err := src.Write(4096, []byte("payload"), 0); err != nil {
		return err
	}
	m, _, err := snapshot.NewSnapshotter(s.Store, s.Clock).Create(ctx, src, vol, 1, snapID, "")
	if err != nil {
		return err
	}
	acked := src.Watermarks().Durable
	s.Emit(Event{Kind: EventWatermark, Local: src.Watermarks().Local, Durable: acked, Published: src.Watermarks().Published})

	// The source host is gone for the rest of the scenario.
	s.Net.Partition("host-source")
	s.Notef("source host partitioned; destination materializes from S3 alone")

	sched := ioclass.NewScheduler(64)
	mat := materialize.New(s.Store, sched, nil)

	// Under data-path contention the materialization yields (INV-17).
	sched.Begin(ioclass.Foreground)
	_, _, err = mat.FromSnapshot(ctx, vols, snapID)
	s.Emit(Event{Kind: EventIOClass, BgGranted: err == nil, HighInFlight: sched.HighActive() > 0})
	if !errors.Is(err, materialize.ErrThrottled) {
		return fmt.Errorf("materialization must yield to foreground, got %v", err)
	}
	sched.End(ioclass.Foreground)

	// Idle: it proceeds and rebuilds exactly the source's state.
	view, prog, err := mat.FromSnapshot(ctx, vols, snapID)
	s.Emit(Event{Kind: EventIOClass, BgGranted: err == nil, HighInFlight: sched.HighActive() > 0})
	if err != nil {
		return fmt.Errorf("materialize: %w", err)
	}
	want, _, err := recovery.Recover(ctx, s.Store, nil, vol, 1)
	if err != nil {
		return err
	}
	for _, off := range []uint64{0, 4096} {
		got, expect := make([]byte, 16), make([]byte, 16)
		view.Read(off, got)
		want.Read(off, expect)
		if !bytes.Equal(got, expect) {
			return fmt.Errorf("materialized state differs at offset %d: %q vs %q", off, got, expect)
		}
	}
	if prog.UpTo < acked {
		return fmt.Errorf("materialized up to %d, below the ACKed-durable %d", prog.UpTo, acked)
	}
	s.Emit(Event{Kind: EventFailover, AckedDurable: acked, Recovered: prog.UpTo})

	// A referenced object that is missing must fail hard, with no partial view.
	if err := s.Store.Delete(ctx, m.Objects[0]); err != nil {
		return err
	}
	s.Emit(Event{Kind: EventDelete, Key: m.Objects[0], Permanent: false})
	partial, _, err := mat.FromSnapshot(ctx, vols, snapID)
	if !errors.Is(err, materialize.ErrMissingObject) {
		return fmt.Errorf("missing object must fail materialization, got %v", err)
	}
	if partial != nil {
		return errors.New("a failed materialization returned a partial view")
	}
	s.Notef("cross-host materialization: S3-only, identical state, yields, no partial volume")
	return nil
}

// scenarioBackgroundYields is INV-17 (§5.9, §11): background I/O yields whenever a
// foreground or flush op is in flight, and stays within its token budget.
func scenarioBackgroundYields(s *Sim) error {
	sched := ioclass.NewScheduler(200)

	// Idle: background is granted (within budget) and no high op is in flight.
	g := sched.TryAcquire(ioclass.Background, 100)
	s.Emit(Event{Kind: EventIOClass, BgGranted: g, HighInFlight: sched.HighActive() > 0})
	if !g {
		return errors.New("idle background within budget should be granted")
	}

	// Under contention: a foreground op in flight → background must yield.
	sched.Begin(ioclass.Foreground)
	g = sched.TryAcquire(ioclass.Background, 1)
	s.Emit(Event{Kind: EventIOClass, BgGranted: g, HighInFlight: sched.HighActive() > 0})
	if g {
		return errors.New("background was granted while foreground in flight (INV-17)")
	}
	// A flush op too.
	sched.Begin(ioclass.Flush)
	g = sched.TryAcquire(ioclass.Background, 1)
	s.Emit(Event{Kind: EventIOClass, BgGranted: g, HighInFlight: sched.HighActive() > 0})
	if g {
		return errors.New("background was granted while flush in flight (INV-17)")
	}

	// After the high ops drain, background resumes.
	sched.End(ioclass.Foreground)
	sched.End(ioclass.Flush)
	sched.Refill()
	g = sched.TryAcquire(ioclass.Background, 100)
	s.Emit(Event{Kind: EventIOClass, BgGranted: g, HighInFlight: sched.HighActive() > 0})
	if !g {
		return errors.New("background should resume once high classes drain")
	}
	s.Notef("background yielded under foreground/flush contention, resumed when idle")
	return nil
}

// scenarioGCMarksOrphansNotLive is INV-14 (§21.3, §5.11): the GC marks unreachable
// objects reversibly and never permanently deletes; a live object is never marked.
func scenarioGCMarksOrphansNotLive(s *Sim) error {
	ctx := context.Background()
	const vid = "00000000-0000-7000-8000-000000000090"
	var vol [16]byte
	vol[6], vol[8] = 0x70, 0x80

	// A live WAL object anchored by a checkpoint, plus structural metadata.
	_ = descriptor.Write(ctx, s.Store, descriptor.Descriptor{VolumeID: vid, SizeBytes: 1, BlockSize: 65536, KEKID: "k", DEKWrapped: []byte{1}})
	lf, _ := s.Disk.Create("wal/active.wal")
	l := wal.NewLog(lf, s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(s.Store, 5))
	_, _ = l.Write(0, []byte("live"), 0)
	if err := l.Flush(ctx); err != nil {
		return err
	}
	if _, err := checkpoint.NewCheckpointer(s.Store).Create(ctx, l, vol, 1); err != nil {
		return err
	}
	liveObjs, _ := s.Store.List(ctx, "wal/"+format.UUIDString(vol)+"/1/")
	var live string
	for _, o := range liveObjs {
		if len(o.Key) > 4 && o.Key[len(o.Key)-4:] == ".wal" {
			live = o.Key
		}
	}

	// An orphan object referenced by nothing.
	if _, err := s.Store.Put(ctx, "wal/"+format.UUIDString(vol)+"/1/999-999-deadbeef.wal", []byte("orphan"), objectstore.PutOptions{}); err != nil {
		return err
	}

	reachable, err := gc.Reachable(ctx, s.Store)
	if err != nil {
		return err
	}
	marks, err := gc.Collect(ctx, s.Store, reachable)
	if err != nil {
		return err
	}

	// Each mark is a reversible delete-marker — emit it as a non-permanent delete so
	// the NoPermanentDeleteChecker (INV-14) validates the GC never permanent-deletes.
	liveMarked := false
	for _, m := range marks {
		s.Emit(Event{Kind: EventDelete, Key: m, Permanent: false})
		if m == live {
			liveMarked = true
		}
	}
	if liveMarked {
		return errors.New("the GC marked a live object")
	}
	if len(marks) == 0 {
		return errors.New("the GC should have marked the orphan")
	}
	// Marks are reversible: the objects still exist.
	for _, m := range marks {
		if _, err := s.Store.Head(ctx, m); err != nil {
			return fmt.Errorf("GC marks must be reversible, but %q is gone", m)
		}
	}
	s.Notef("GC marked %d orphan(s), no live object, no permanent delete", len(marks))
	return nil
}

// scenarioCheckpointThenTruncate is INV-13 (§21.1): local WAL is truncated only up to
// a verified, checkpoint-published point — never above it.
func scenarioCheckpointThenTruncate(s *Sim) error {
	ctx := context.Background()
	var vol [16]byte
	vol[6], vol[8] = 0x70, 0x80

	f, err := s.Disk.Create("wal/active.wal")
	if err != nil {
		return err
	}
	l := wal.NewLog(f, s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(s.Store, 5))

	_, _ = l.Write(0, []byte("a"), 0)
	_, _ = l.Write(8, []byte("b"), 0)
	if err := l.Flush(ctx); err != nil {
		return err
	}

	// Truncating before a checkpoint (published=0) is refused.
	if err := l.TruncateLocal(2); !errors.Is(err, wal.ErrTruncateAboveDurable) {
		return fmt.Errorf("pre-checkpoint truncate: want ErrTruncateAboveDurable, got %v", err)
	}

	if _, err := checkpoint.NewCheckpointer(s.Store).Create(ctx, l, vol, 1); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	// Now truncate up to the published point — allowed.
	if err := l.TruncateLocal(2); err != nil {
		return fmt.Errorf("post-checkpoint truncate: %w", err)
	}
	s.Emit(Event{Kind: EventTruncate, TruncatedUpTo: l.TruncatedUpTo(), Published: l.Watermarks().Published})

	// A later un-checkpointed write cannot be truncated away.
	_, _ = l.Write(16, []byte("c"), 0)
	if err := l.TruncateLocal(3); !errors.Is(err, wal.ErrTruncateAboveDurable) {
		return fmt.Errorf("truncate above published: want ErrTruncateAboveDurable, got %v", err)
	}
	s.Notef("WAL truncated to the published point (2), never above it")
	return nil
}

// scenarioSameHostCloneIndependent (§20, §5.2): a same-host clone is a new active
// child — writes to it land under its own key prefix and never touch the parent's
// durable objects or the snapshot.
func scenarioSameHostCloneIndependent(s *Sim) error {
	ctx := context.Background()
	var pv [16]byte
	pv[6], pv[8] = 0x70, 0x80
	pv[15] = 1 // parent
	cv := pv
	cv[15] = 2 // clone
	pvs, cvs := format.UUIDString(pv), format.UUIDString(cv)

	md := metasim.New(s.Clock.Wall)
	term, _ := md.AcquireLeadership(ctx, "cp")
	_ = md.CreateVolume(ctx, term, metadata.Volume{VolumeID: pvs, SizeBytes: 1 << 30, BlockSize: 65536, State: "ACTIVE", DEKWrapped: []byte{1}, KEKID: "k"})

	// Parent writes + snapshot.
	pf, _ := s.Disk.Create("wal/parent.wal")
	parent := wal.NewLog(pf, s.Clock, pv, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	parent.EnableRemote(wal.NewBatcher(s.Clock, pv, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(s.Store, 5))
	_, _ = parent.Write(0, []byte("parent"), 0)
	if err := parent.Flush(ctx); err != nil {
		return err
	}
	m, _, err := snapshot.NewSnapshotter(s.Store, s.Clock).Create(ctx, parent, pv, 1, "00000000-0000-7000-8000-000000000071", "")
	if err != nil {
		return err
	}
	_ = md.CreateSnapshot(ctx, term, metadata.Snapshot{SnapshotID: m.SnapshotID, VolumeID: pvs, Epoch: 1, TargetSequence: int64(m.TargetSequence), RootDigest: m.RootDigest, State: "PUBLISHED", RequestID: "00000000-0000-7000-8000-000000000072"})

	parentObjsBefore, _ := s.Store.List(ctx, "wal/"+pvs+"/")

	// Clone (pure metadata; no data copy) then write to the clone.
	if _, err := controlplane.Clone(ctx, md, term, m.SnapshotID, cvs, "00000000-0000-7000-8000-0000000000f1"); err != nil {
		return err
	}
	cf, _ := s.Disk.Create("wal/clone.wal")
	clone := wal.NewLog(cf, s.Clock, cv, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	clone.EnableRemote(wal.NewBatcher(s.Clock, cv, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(s.Store, 5))
	_, _ = clone.Write(0, []byte("clone-only"), 0)
	if err := clone.Flush(ctx); err != nil {
		return err
	}

	// The parent's durable objects and the snapshot manifest are untouched.
	parentObjsAfter, _ := s.Store.List(ctx, "wal/"+pvs+"/")
	if len(parentObjsAfter) != len(parentObjsBefore) {
		return fmt.Errorf("clone write changed the parent's objects: %d -> %d", len(parentObjsBefore), len(parentObjsAfter))
	}
	got, err := snapshot.Read(ctx, s.Store, pvs, m.SnapshotID)
	if err != nil || got.TargetSequence != m.TargetSequence {
		return fmt.Errorf("snapshot changed after clone write: %+v err=%v", got, err)
	}
	// The clone's data is under its own prefix.
	if cloneObjs, _ := s.Store.List(ctx, "wal/"+cvs+"/"); len(cloneObjs) == 0 {
		return errors.New("clone write did not land under the clone prefix")
	}
	s.Notef("same-host clone independent: parent + snapshot untouched")
	return nil
}

// scenarioSnapshotPauseFreeImmutable is INV-16 + §19: a snapshot captures a sequence
// with ~0 pause and, once published, never changes even as writes continue.
func scenarioSnapshotPauseFreeImmutable(s *Sim) error {
	ctx := context.Background()
	var vol [16]byte
	vol[6], vol[8] = 0x70, 0x80

	f, err := s.Disk.Create("wal/active.wal")
	if err != nil {
		return err
	}
	l := wal.NewLog(f, s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(s.Store, 5))

	_, _ = l.Write(0, []byte("a"), 0)
	_, _ = l.Write(8, []byte("b"), 0)

	m, pause, err := snapshot.NewSnapshotter(s.Store, s.Clock).Create(ctx, l, vol, 1, "snap-1", "")
	if err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	if pause != 0 {
		return fmt.Errorf("snapshot pause = %v, want ~0", pause)
	}

	// Keep writing after the snapshot.
	_, _ = l.Write(16, []byte("c"), 0)
	if err := l.Flush(ctx); err != nil {
		return err
	}

	// The published manifest is unchanged, and republishing (mutating) it fails.
	got, err := snapshot.Read(ctx, s.Store, m.VolumeID, "snap-1")
	if err != nil {
		return err
	}
	mutated := got.TargetSequence != m.TargetSequence || len(got.Objects) != len(m.Objects)
	changed := m
	changed.TargetSequence = 999
	if err := snapshot.Publish(ctx, s.Store, changed); !errors.Is(err, objectstore.ErrPreconditionFailed) {
		mutated = true // the manifest was overwritten — INV-16 violation
	}
	s.Emit(Event{Kind: EventSnapshot, SnapshotMutated: mutated, Msg: fmt.Sprintf("target=%d", m.TargetSequence)})
	if mutated {
		return errors.New("published snapshot changed")
	}
	s.Notef("snapshot at seq %d: pause ~0, immutable after later writes", m.TargetSequence)
	return nil
}

// scenarioRebuildMetadataFromS3 is INV-20 (§22.5): with PostgreSQL empty,
// rebuild-metadata reconstructs the volume from the self-describing S3 layout.
func scenarioRebuildMetadataFromS3(s *Sim) error {
	ctx := context.Background()
	const vid = "00000000-0000-7000-8000-000000000050"
	epochs := epoch.NewStore(s.Store)

	if err := descriptor.Write(ctx, s.Store, descriptor.Descriptor{
		VolumeID: vid, SizeBytes: 1 << 30, BlockSize: 65536, Durability: "remote", KEKID: "k", DEKWrapped: []byte{1},
	}); err != nil {
		return err
	}
	if _, err := epochs.Init(ctx, vid, 7); err != nil {
		return err
	}

	md := metasim.New(s.Clock.Wall)
	term, _ := md.AcquireLeadership(ctx, "cp")
	if _, err := md.GetVolume(ctx, vid); err == nil {
		return errors.New("PG should start empty")
	}

	n, err := controlplane.RebuildMetadata(ctx, s.Store, epochs, md, term)
	if err != nil || n != 1 {
		return fmt.Errorf("rebuild: n=%d err=%v", n, err)
	}
	v, err := md.GetVolume(ctx, vid)
	if err != nil {
		return err
	}
	if v.CurrentEpoch != 7 {
		return fmt.Errorf("rebuilt epoch = %d, want 7 (from the S3 epoch object)", v.CurrentEpoch)
	}
	s.Notef("rebuilt PG volume from S3 (epoch 7 from the epoch object)")
	return nil
}

// scenarioRecoveryAuthorityIsS3 is INV-08 (§5.8): the durable point is determined
// from S3 alone; a wrong PostgreSQL watermark does not affect it.
func scenarioRecoveryAuthorityIsS3(s *Sim) error {
	ctx := context.Background()
	var vol [16]byte
	vol[6], vol[8] = 0x70, 0x80

	// Write two records to S3 through a Log.
	f, err := s.Disk.Create("wal/active.wal")
	if err != nil {
		return err
	}
	l := wal.NewLog(f, s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(s.Store, 5))
	_, _ = l.Write(0, []byte("a"), 0)
	_, _ = l.Write(8, []byte("b"), 0)
	if err := l.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	// PostgreSQL holds a WRONG informative watermark.
	md := metasim.New(s.Clock.Wall)
	term, _ := md.AcquireLeadership(ctx, "cp")
	_ = md.CreateVolume(ctx, term, metadata.Volume{VolumeID: format.UUIDString(vol), State: "ACTIVE", DurableSequence: 999, DEKWrapped: []byte{1}, KEKID: "k"})

	// Recovery derives the durable point from S3, ignoring PG's 999.
	durable, err := recovery.DurablePoint(ctx, s.Store, vol, 1)
	if err != nil {
		return err
	}
	if durable != 2 {
		return fmt.Errorf("durable from S3 = %d, want 2 (PG said 999)", durable)
	}
	if v, _ := md.GetVolume(ctx, format.UUIDString(vol)); v.DurableSequence != 999 {
		return fmt.Errorf("expected PG to still hold the wrong watermark, got %d", v.DurableSequence)
	}
	s.Notef("recovery used the S3 durable point (2), not the PG watermark (999)")
	return nil
}

// failVol/failHosts are the v7-shaped ids for the full-fencing scenario.
const (
	failVolStr = "00000000-0000-7000-8000-000000000040"
	failHost1  = "00000000-0000-7000-8000-0000000000c1"
	failHost2  = "00000000-0000-7000-8000-0000000000c2"
)

// scenarioFencedWriterNoLostAck is INV-09 — the headline of v5. W1 (epoch 1) ACKs
// some FLUSHes; it is then partitioned from the CP (S3 intact), so past its lease it
// cannot ACK (INV-06); the CP waits FENCING_WAIT and promotes W2 (INV-10/11); W2
// recovers the durable prefix from S3 and it covers everything W1 ACKed. W1's late
// PUT (which landed in S3 but was never ACKed) is a harmless superset.
func scenarioFencedWriterNoLostAck(s *Sim) error {
	ctx := context.Background()
	var volID [16]byte
	volID[6], volID[8] = 0x70, 0x80 // v7 shape; UUIDString(volID) is a valid v7 id

	// Control Plane state.
	md := metasim.New(s.Clock.Wall)
	epochs := epoch.NewStore(s.Store)
	term, _ := md.AcquireLeadership(ctx, "cp")
	_ = md.UpsertHost(ctx, term, metadata.Host{HostID: failHost1, State: "ACTIVE"})
	_ = md.UpsertHost(ctx, term, metadata.Host{HostID: failHost2, State: "ACTIVE"})
	_ = md.CreateVolume(ctx, term, metadata.Volume{VolumeID: format.UUIDString(volID), CurrentEpoch: 1, State: "ACTIVE", PrimaryHostID: failHost1, DEKWrapped: []byte{1}, KEKID: "k"})
	if _, err := epochs.Init(ctx, format.UUIDString(volID), 1); err != nil {
		return err
	}

	// W1: remote leased log at epoch 1.
	lm := lease.NewManager(s.Clock, 10*time.Second)
	lm.Grant()
	f, err := s.Disk.Create("wal/active.wal")
	if err != nil {
		return err
	}
	w1 := wal.NewLog(f, s.Clock, volID, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	w1.EnableRemote(
		wal.NewBatcher(s.Clock, volID, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(s.Store, 5),
	)
	w1.SetLease(lm)

	// W1 ACKs seq 1..2 with a valid lease.
	_, _ = w1.Write(0, []byte("one"), 0)
	_, _ = w1.Write(8, []byte("two"), 0)
	if err := w1.Flush(ctx); err != nil {
		return fmt.Errorf("w1 flush (valid lease): %w", err)
	}
	ackedDurable := w1.Watermarks().Durable // = 2

	// Partition: heartbeats stop; the lease expires. W1's next FLUSH's object lands
	// in S3 (S3 is reachable) but W1 self-fences and does NOT ACK it.
	_, _ = w1.Write(16, []byte("three"), 0)
	s.Clock.Advance(11 * time.Second) // lease expires, no renewal
	if err := w1.Flush(ctx); !errors.Is(err, wal.ErrSelfFenced) {
		return fmt.Errorf("w1 partitioned flush: want ErrSelfFenced, got %v", err)
	}
	if w1.Watermarks().Durable != ackedDurable {
		return errors.New("w1 durable advanced past its ACK despite an invalid lease (INV-06)")
	}

	// CP promotes W2 after FENCING_WAIT (its lease was renewed at t0 = start).
	s.Clock.Advance(2 * time.Second) // ensure past last_renewal + ttl + skew
	newEpoch, err := controlplane.NewPromoter(md, epochs, s.Clock, 10*time.Second, 2*time.Second).
		Promote(ctx, term, format.UUIDString(volID), s.Clock.Wall().Add(-13*time.Second), failHost2)
	if err != nil {
		return fmt.Errorf("promote W2: %w", err)
	}

	// W2 recovers epoch 1's durable prefix from S3 and fixes the recovery point.
	recovered, err := recovery.DurablePrefix(ctx, s.Store, volID, 1)
	if err != nil {
		return fmt.Errorf("recover durable prefix: %w", err)
	}
	if err := recovery.WriteRecoveryPoint(ctx, s.Store, volID, newEpoch, 1, recovered); err != nil {
		return err
	}

	s.Emit(Event{Kind: EventFailover, AckedDurable: ackedDurable, Recovered: recovered})
	if recovered < ackedDurable {
		return fmt.Errorf("recovered %d < acked %d — a durable ACK was lost (INV-09)", recovered, ackedDurable)
	}
	s.Notef("failover: W1 acked=%d, W2 recovered=%d (>= acked); no ACKed write lost", ackedDurable, recovered)
	return nil
}

// promoVol/promoHosts are v7-shaped ids used by the promotion scenario.
const (
	promoVol   = "00000000-0000-7000-8000-000000000020"
	promoHost1 = "00000000-0000-7000-8000-0000000000b1"
	promoHost2 = "00000000-0000-7000-8000-0000000000b2"
)

// scenarioPromotionFencingWait exercises INV-11 (promotion waits) and INV-10 (a
// fenced writer cannot publish) end-to-end through the Control Plane promoter.
func scenarioPromotionFencingWait(s *Sim) error {
	ctx := context.Background()
	md := metasim.New(s.Clock.Wall)
	epochs := epoch.NewStore(s.Store)
	p := controlplane.NewPromoter(md, epochs, s.Clock, 10*time.Second, 2*time.Second)

	term, _ := md.AcquireLeadership(ctx, "cp")
	_ = md.UpsertHost(ctx, term, metadata.Host{HostID: promoHost1, State: "ACTIVE"})
	_ = md.UpsertHost(ctx, term, metadata.Host{HostID: promoHost2, State: "ACTIVE"})
	_ = md.CreateVolume(ctx, term, metadata.Volume{VolumeID: promoVol, State: "ACTIVE", PrimaryHostID: promoHost1, DEKWrapped: []byte{1}, KEKID: "k"})
	if _, err := epochs.Init(ctx, promoVol, 0); err != nil {
		return err
	}
	// The old writer W1 records the epoch object ETag it holds at epoch 0.
	_, w1ETag, _ := epochs.Current(ctx, promoVol)
	renewedAt := s.Clock.Wall()

	// INV-11: promotion before FENCING_WAIT is refused.
	if _, err := p.Promote(ctx, term, promoVol, renewedAt, promoHost2); !errors.Is(err, controlplane.ErrFencingWaitNotElapsed) {
		return fmt.Errorf("promote-too-early: want ErrFencingWaitNotElapsed, got %v", err)
	}

	// After FENCING_WAIT (lease_ttl 10s + skew 2s), promotion succeeds.
	s.Clock.Advance(13 * time.Second)
	deadline := p.FencingDeadline(renewedAt)
	newEpoch, err := p.Promote(ctx, term, promoVol, renewedAt, promoHost2)
	if err != nil {
		return fmt.Errorf("promote after wait: %w", err)
	}
	s.Emit(Event{Kind: EventPromotion, EarlyGrant: s.Clock.Wall().Before(deadline), Msg: fmt.Sprintf("epoch=%d", newEpoch)})
	if newEpoch != 1 {
		return fmt.Errorf("new epoch = %d, want 1", newEpoch)
	}

	// INV-10: W1 (epoch 0) is now fenced — its publish check and CAS both fail.
	fenced := errors.Is(epochs.Verify(ctx, promoVol, 0), epoch.ErrEpochChanged)
	_, casErr := epochs.CompareAndAdvance(ctx, promoVol, w1ETag, 99)
	published := !fenced || casErr == nil
	s.Emit(Event{Kind: EventStalePublsh, StalePublishOK: published})
	if published {
		return errors.New("a fenced writer was able to publish")
	}
	s.Notef("promotion respected FENCING_WAIT; old epoch fenced")
	return nil
}

// scenarioLeaseFencesDurableAck is INV-06 (§12.2): a valid lease lets a FLUSH ACK;
// once the lease expires, a FLUSH whose object still lands in S3 is NOT ACKed —
// durable does not advance and the log self-fences.
func scenarioLeaseFencesDurableAck(s *Sim) error {
	ctx := context.Background()
	vol := [16]byte{8}
	f, err := s.Disk.Create("wal/active.wal")
	if err != nil {
		return err
	}
	lm := lease.NewManager(s.Clock, 10*time.Second)
	lm.Grant()

	l := wal.NewLog(f, s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(
		wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(s.Store, 5),
	)
	l.SetLease(lm)

	// (1) With a valid lease, the FLUSH ACKs.
	if _, err := l.Write(0, []byte("acked"), 0); err != nil {
		return err
	}
	if err := l.Flush(ctx); err != nil {
		return fmt.Errorf("flush with valid lease: %w", err)
	}
	s.Emit(Event{Kind: EventDurableAck, Durable: l.Watermarks().Durable, LeaseValid: lm.Valid()})
	ackedDurable := l.Watermarks().Durable

	// (2) Lease expires; a further write + FLUSH must self-fence without ACKing,
	// even though the object reaches S3.
	if _, err := l.Write(8, []byte("not-acked"), 0); err != nil {
		return err
	}
	s.Clock.Advance(11 * time.Second) // no renewal
	err = l.Flush(ctx)
	if !errors.Is(err, wal.ErrSelfFenced) {
		return fmt.Errorf("expired-lease flush: want ErrSelfFenced, got %v", err)
	}
	// No durable-ack event is emitted here — there was no ACK. The checker verifies
	// no ACK ever escaped with an invalid lease.
	if l.Watermarks().Durable != ackedDurable {
		return fmt.Errorf("durable advanced past the last ACK despite an invalid lease (INV-06)")
	}
	if !l.Fenced() {
		return errors.New("log should have self-fenced")
	}
	if objs, _ := s.Store.List(ctx, "wal/"); len(objs) != 2 {
		return fmt.Errorf("both objects should be in S3 (PUT succeeded), got %d", len(objs))
	}
	s.Notef("lease expiry self-fenced the ACK; object in S3 but not confirmed")
	return nil
}

func remoteLog(s *Sim, vol [16]byte) (*wal.Log, error) {
	f, err := s.Disk.Create("wal/active.wal")
	if err != nil {
		return nil, err
	}
	l := wal.NewLog(f, s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(
		wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(s.Store, 5),
	)
	return l, nil
}

// scenarioRemoteFlushOrdering is INV-07 (§14.4): durable_sequence advances only
// after the covering objects are verified in S3. A failing upload must leave
// durable where it was.
func scenarioRemoteFlushOrdering(s *Sim) error {
	ctx := context.Background()
	l, err := remoteLog(s, [16]byte{5})
	if err != nil {
		return err
	}
	if _, err := l.Write(0, []byte("durable-me"), 0); err != nil {
		return err
	}

	s.Store.InjectThrottle(5) // exhaust the uploader budget, then clear
	if err := l.Flush(ctx); err == nil {
		return errors.New("flush should fail while uploads fail")
	}
	emitWatermarks(s, l)
	if l.Watermarks().Durable != 0 {
		return fmt.Errorf("durable advanced to %d despite upload failure (INV-07)", l.Watermarks().Durable)
	}
	if objs, _ := s.Store.List(ctx, "wal/"); len(objs) != 0 {
		return fmt.Errorf("no object should be durable, got %d", len(objs))
	}

	// Retry succeeds; durable now advances.
	if err := l.Flush(ctx); err != nil {
		return fmt.Errorf("retry flush: %w", err)
	}
	emitWatermarks(s, l)
	if l.Watermarks().Durable != 1 {
		return fmt.Errorf("durable should be 1 after verified upload, got %d", l.Watermarks().Durable)
	}
	s.Emit(Event{Kind: EventObject, Msg: "batch verified in S3"})
	return nil
}

// scenarioIdempotentBatchUpload is INV-21 (§14.5): a PUT that persisted but lost
// its response reconciles on retry, and re-uploads never duplicate.
func scenarioIdempotentBatchUpload(s *Sim) error {
	ctx := context.Background()
	vol := [16]byte{6}
	b := wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig())
	enc, _ := wal.Record{Type: format.RecordWrite, Epoch: 1, Sequence: 1, Payload: []byte("batch-bytes")}.Encode()
	b.Append(1, enc, false)
	b.Flush()
	cb := b.Pending()[0]

	s.Store.InjectLostResponse(cb.Object().Key)
	up := wal.NewUploader(s.Store, 5)
	if _, err := up.Upload(ctx, cb); err != nil {
		return fmt.Errorf("upload should be idempotent after lost response: %w", err)
	}
	if _, err := up.Upload(ctx, cb); err != nil {
		return fmt.Errorf("re-upload should be idempotent: %w", err)
	}
	objs, _ := s.Store.List(ctx, "wal/")
	if len(objs) != 1 {
		return fmt.Errorf("expected exactly 1 object after idempotent retries, got %d", len(objs))
	}
	s.Notef("idempotent upload: 1 object after lost response + re-upload")
	return nil
}

// scenarioEncryptedWALNoPlaintextLeak: with per-volume encryption, the bytes that
// will leave the host (the WAL file → later S3) contain no cleartext (§5.10,
// INV-15), yet replay+decrypt recovers the plaintext.
func scenarioEncryptedWALNoPlaintextLeak(s *Sim) error {
	dek, err := crypto.GenerateDEK(&deterministicReader{b: byte(s.Rand.Intn(200) + 1)}, 1)
	if err != nil {
		return err
	}
	enc := &wal.Encryption{DEK: dek, VolumeID: [16]byte{7}}

	f, err := s.Disk.Create("wal/active.wal")
	if err != nil {
		return err
	}
	l := wal.NewLog(f, s.Clock, enc.VolumeID, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableEncryption(enc)

	canary := []byte("CLEARTEXT-CANARY-DO-NOT-LEAK")
	if _, err := l.Write(0, canary, 0); err != nil {
		return err
	}

	// Inspect the bytes bound to leave the host.
	sz, _ := f.Size()
	raw := make([]byte, sz)
	_, _ = f.ReadAt(raw, 0)
	leak := bytes.Contains(raw, canary)
	s.Emit(Event{Kind: EventLeavesHost, ClearLeak: leak, Msg: "wal object bytes"})
	if leak {
		return errors.New("cleartext canary present in WAL bytes")
	}

	// Recovery still works.
	recs, err := wal.Replay(raw)
	if err != nil {
		return fmt.Errorf("replay: %w", err)
	}
	if len(recs) != 1 {
		return fmt.Errorf("expected 1 record, got %d", len(recs))
	}
	pt, err := enc.Decrypt(recs[0])
	if err != nil {
		return fmt.Errorf("decrypt: %w", err)
	}
	if !bytes.Equal(pt, canary) {
		return fmt.Errorf("decrypted plaintext mismatch: %q", pt)
	}
	s.Notef("encrypted WAL: 0 cleartext leak, replay+decrypt OK")
	return nil
}

// emitWatermarks records the log's current watermarks for the ordering checker.
func emitWatermarks(s *Sim, l *wal.Log) {
	w := l.Watermarks()
	s.Emit(Event{Kind: EventWatermark, Local: w.Local, Durable: w.Durable, Published: w.Published})
}

// scenarioWALWritePathNoPut: normal WRITEs go to the local WAL and are readable
// back, they issue no object-store PUT (§5.3, INV-18), and the watermarks stay
// ordered (§5.6, INV-03).
func scenarioWALWritePathNoPut(s *Sim) error {
	f, err := s.Disk.Create("wal/active.wal")
	if err != nil {
		return err
	}
	l := wal.NewLog(f, s.Clock, [16]byte{}, 1, wal.Limits{MaxUnflushedBytes: 1 << 20, MaxUnflushedAge: 30 * time.Second})

	n := 3 + s.Rand.Intn(6)
	written := map[uint64][]byte{}
	for i := 0; i < n; i++ {
		off := uint64(s.Rand.Intn(16)) * 8
		payload := []byte(fmt.Sprintf("rec%02d", i))
		if _, err := l.Write(off, payload, 0); err != nil {
			return fmt.Errorf("write %d: %w", i, err)
		}
		written[off] = payload
		emitWatermarks(s, l)
	}

	// Read-back matches the last write at each offset.
	for off, want := range written {
		buf := make([]byte, len(want))
		l.Read(off, buf)
		if !bytes.Equal(buf, want) {
			return fmt.Errorf("read-back at %d: got %q want %q", off, buf, want)
		}
	}

	// INV-18: no PUT happened on the write path.
	objs, err := s.Store.List(context.Background(), "")
	if err != nil {
		return err
	}
	if len(objs) != 0 {
		return fmt.Errorf("write path issued %d PUTs; a normal WRITE must not PUT (§5.3)", len(objs))
	}
	s.Notef("wrote %d records, 0 PUTs", n)
	return nil
}

// scenarioWALBackpressure: exceeding the unflushed byte limit yields an explicit
// error (§5.7, INV-04), and after Sync writes resume.
func scenarioWALBackpressure(s *Sim) error {
	f, err := s.Disk.Create("wal/active.wal")
	if err != nil {
		return err
	}
	// Room for one record (104-byte header + small payload) but not two.
	l := wal.NewLog(f, s.Clock, [16]byte{}, 1, wal.Limits{MaxUnflushedBytes: 200})

	if _, err := l.Write(0, make([]byte, 32), 0); err != nil {
		return fmt.Errorf("first write should fit: %w", err)
	}
	if _, err := l.Write(64, make([]byte, 64), 0); !errors.Is(err, wal.ErrBackpressure) {
		return fmt.Errorf("expected backpressure, got %v", err)
	}
	s.Emit(Event{Kind: EventFault, Msg: "backpressure asserted"})

	if err := l.Sync(); err != nil {
		return err
	}
	if _, err := l.Write(64, make([]byte, 64), 0); err != nil {
		return fmt.Errorf("write after sync should resume: %w", err)
	}
	emitWatermarks(s, l)
	return nil
}

// scenarioLostPutIdempotent: a PUT persists but its response is lost (§14.5). The
// idempotent retry sees the object already present (412), HEADs it, and the
// checksums match => success without duplication.
func scenarioLostPutIdempotent(s *Sim) error {
	ctx := context.Background()
	key := "wal/vol/0/1-1-hash.wal"
	data := []byte("the-encrypted-batch")

	s.Store.InjectLostResponse(key)
	s.Notef("PUT with lost response injected")
	_, err := s.Store.Put(ctx, key, data, objectstore.PutOptions{IfNoneMatch: true})
	if !errors.Is(err, sim.ErrLostResponse) {
		return fmt.Errorf("expected lost-response error, got %v", err)
	}
	s.Emit(Event{Kind: EventObject, Key: key, Msg: "put response lost"})

	// Retry: create-only now fails because it persisted.
	_, err = s.Store.Put(ctx, key, data, objectstore.PutOptions{IfNoneMatch: true})
	if !errors.Is(err, objectstore.ErrPreconditionFailed) {
		return fmt.Errorf("retry expected precondition-failed, got %v", err)
	}
	// HEAD + checksum reconcile: same size and readable content => idempotent OK.
	info, err := s.Store.Head(ctx, key)
	if err != nil {
		return fmt.Errorf("HEAD after retry: %w", err)
	}
	if info.Size != int64(len(data)) {
		return fmt.Errorf("HEAD size mismatch: got %d want %d", info.Size, len(data))
	}
	got, err := s.Store.Get(ctx, key)
	if err != nil || !bytes.Equal(got, data) {
		return fmt.Errorf("content mismatch after lost-response retry: %q err=%v", got, err)
	}
	s.Emit(Event{Kind: EventObject, Key: key, Msg: "idempotent retry reconciled"})
	return nil
}

// scenarioCrashAroundFdatasync: write to the WAL then crash at a seed-chosen point
// relative to the sync. After the crash exactly the durable prefix must remain.
func scenarioCrashAroundFdatasync(s *Sim) error {
	f, err := s.Disk.Create("wal/active.wal")
	if err != nil {
		return err
	}
	durable := []byte("committed-record")
	if _, err := f.Append(durable); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	s.Emit(Event{Kind: EventDisk, Msg: "appended+synced committed-record"})

	// A second record whose fate depends on whether we sync before crashing.
	uncommitted := []byte("-in-flight")
	if _, err := f.Append(uncommitted); err != nil {
		return err
	}

	syncBeforeCrash := s.Rand.Intn(2) == 0
	if syncBeforeCrash {
		if err := f.Sync(); err != nil {
			return err
		}
		s.Emit(Event{Kind: EventFault, Msg: "sync then crash"})
	} else {
		s.Emit(Event{Kind: EventFault, Msg: "crash before sync"})
	}
	s.Disk.Crash()
	s.Emit(Event{Kind: EventRecovery, Msg: "recovering after crash"})

	// Determine what must survive.
	want := durable
	if syncBeforeCrash {
		want = append(append([]byte(nil), durable...), uncommitted...)
	}
	size, err := f.Size()
	if err != nil {
		return err
	}
	if size != int64(len(want)) {
		return fmt.Errorf("post-crash size %d, want %d (syncBeforeCrash=%t)", size, len(want), syncBeforeCrash)
	}
	got := make([]byte, size)
	if _, err := f.ReadAt(got, 0); err != nil && size > 0 {
		// io.EOF at exact end is acceptable; only fail on short read handled by size check above.
		if !bytes.Equal(got, want) {
			return fmt.Errorf("post-crash content mismatch: %q want %q", got, want)
		}
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("post-crash content mismatch: %q want %q", got, want)
	}
	return nil
}

// scenarioClockDriftBeyondSkew: inject wall skew far beyond max_clock_skew (2 s).
// Monotonic time — the basis of writer safety (§12.1) — must be unaffected; only
// wall time moves. The MonotonicClockChecker corroborates across the run.
func scenarioClockDriftBeyondSkew(s *Sim) error {
	before := s.Clock.Now()
	wallBefore := s.Clock.Wall()

	drift := time.Duration(3+s.Rand.Intn(30)) * time.Second // always > 2 s skew bound
	s.Clock.SetSkew(drift)
	s.Emit(Event{Kind: EventFault, Msg: fmt.Sprintf("inject wall skew=%s", drift)})

	if got := s.Clock.Now(); got != before {
		return fmt.Errorf("skew perturbed monotonic time: %d -> %d", before, got)
	}
	// Advance and confirm monotonic still moves normally.
	s.Tick(time.Second)
	if s.Clock.Now() <= before {
		return fmt.Errorf("monotonic did not advance after Tick")
	}
	// Wall reflects the injected skew.
	wallAfter := s.Clock.Wall()
	if wallAfter.Sub(wallBefore) < drift {
		return fmt.Errorf("wall did not reflect injected skew: delta=%s drift=%s", wallAfter.Sub(wallBefore), drift)
	}
	return nil
}

// scenarioNetworkPartition: a Control-Plane/Agent link is partitioned; sends fail
// while partitioned and resume after heal (the §12/§23 fencing precondition).
func scenarioNetworkPartition(s *Sim) error {
	ctx := context.Background()
	addr := "agent:1"
	l, err := s.Net.Listen(addr)
	if err != nil {
		return err
	}
	defer l.Close()

	accepted := make(chan network.Conn, 1)
	accErr := make(chan error, 1)
	go func() {
		c, err := l.Accept(ctx)
		if err != nil {
			accErr <- err
			return
		}
		accepted <- c
	}()

	client, err := s.Net.Dial(ctx, addr)
	if err != nil {
		return err
	}
	defer client.Close()

	select {
	case <-accepted:
	case err := <-accErr:
		return fmt.Errorf("accept: %w", err)
	}

	s.Net.Partition(addr)
	s.Emit(Event{Kind: EventFault, Msg: "partition agent:1"})
	if err := client.Send(ctx, []byte("heartbeat")); !errors.Is(err, network.ErrPartitioned) {
		return fmt.Errorf("send during partition: want ErrPartitioned, got %v", err)
	}

	s.Net.Heal(addr)
	s.Emit(Event{Kind: EventNetwork, Msg: "heal agent:1"})
	if err := client.Send(ctx, []byte("heartbeat")); err != nil {
		return fmt.Errorf("send after heal: %w", err)
	}
	return nil
}
