// Package materialize rebuilds a volume's state on a host that does not have it —
// the cross-host path of §20 and the cold case of §22.3. The only I/O dependency is
// the object store: there is no host-to-host transport, by construction, so a volume
// can be moved while its previous host is dead or partitioned (§5.4 — locality is
// not durability; §5.8 — S3 is the authority).
//
// Materialization is background-class work (§5.9, §11, INV-17): it acquires a token
// per object and, when foreground/flush I/O is in flight or the window budget is
// spent, it yields with ErrThrottled instead of competing with the data path. The
// caller is a reconciled operation, so it simply retries later (§7).
//
// It never produces a partial volume. A source whose digest does not match its
// contents, a referenced object that is missing, or a gap in the referenced sequence
// range all fail hard before any state is handed back.
package materialize

import (
	"context"
	"errors"
	"fmt"

	"github.com/spin-stack/storage/internal/checkpoint"
	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/ioclass"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/snapshot"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// Sentinel errors. Each one means "do not boot this volume".
var (
	// ErrMissingObject means a referenced WAL object is not in the object store.
	ErrMissingObject = errors.New("materialize: referenced object missing")
	// ErrSequenceGap means the referenced objects do not form a contiguous run.
	ErrSequenceGap = errors.New("materialize: gap in the referenced WAL sequences")
	// ErrDigestMismatch means the manifest/checkpoint is not self-consistent.
	ErrDigestMismatch = errors.New("materialize: root digest mismatch")
	// ErrPrefixFloor means the referenced objects do not start at the epoch's first
	// sequence: the rebuilt volume would have a hole at the front.
	ErrPrefixFloor = errors.New("materialize: referenced objects do not reach the epoch floor")
	// ErrCoverageShort means the replayed run stops below the sequence the source
	// claims to cover.
	ErrCoverageShort = errors.New("materialize: replayed state does not reach the claimed sequence")
	// ErrThrottled means the background class yielded (foreground/flush in flight or
	// the window budget is spent). Not a failure: the reconciler retries.
	ErrThrottled = errors.New("materialize: background class yielded")
)

// Progress is what a long-running move reports (§28.1 operation progress) and what
// the measured cold RTO per GiB is computed from (§29.4).
type Progress struct {
	Objects int
	Bytes   int64
	UpTo    uint64 // last sequence covered by the materialized state
}

// Materializer rebuilds volume state from an object store. sched may be nil
// (unthrottled, e.g. an operator-driven restore); enc is nil for plaintext volumes.
type Materializer struct {
	store objectstore.Store
	sched *ioclass.Scheduler
	enc   *wal.Encryption
	rec   *obs.Recorder // nil = telemetry not wired (no-op)
}

// SetRecorder wires the §26.2 metric this path owns: bytes_downloaded_before_boot,
// which is the measured cold RTO input of §29.4 (DEV-0010).
func (m *Materializer) SetRecorder(r *obs.Recorder) { m.rec = r }

// New returns a Materializer.
func New(store objectstore.Store, sched *ioclass.Scheduler, enc *wal.Encryption) *Materializer {
	return &Materializer{store: store, sched: sched, enc: enc}
}

// FromSnapshot rebuilds the state a published snapshot describes — the source for a
// cross-host clone and for a drain-driven move (§20).
func (m *Materializer) FromSnapshot(ctx context.Context, volumeID, snapshotID string) (*cow.IntervalMap, Progress, error) {
	man, err := snapshot.Read(ctx, m.store, volumeID, snapshotID)
	if err != nil {
		return nil, Progress{}, fmt.Errorf("materialize: read manifest %s/%s: %w", volumeID, snapshotID, err)
	}
	if !man.DigestMatches() {
		return nil, Progress{}, fmt.Errorf("%w: snapshot %s", ErrDigestMismatch, snapshotID)
	}
	if man.VolumeID != volumeID {
		return nil, Progress{}, fmt.Errorf("%w: manifest at %s/%s describes volume %s",
			recovery.ErrObjectIntegrity, volumeID, snapshotID, man.VolumeID)
	}
	src, err := m.sourceFor(ctx, volumeID, man.Epoch, man.TargetSequence, man.Objects)
	if err != nil {
		return nil, Progress{}, fmt.Errorf("materialize: snapshot %s: %w", snapshotID, err)
	}
	return m.fetchAndReplay(ctx, src)
}

// FromCheckpoint rebuilds the state a verified checkpoint describes — the source
// for moving a live volume, and the warm-standby hydration point (§21.1, §22.3).
func (m *Materializer) FromCheckpoint(ctx context.Context, volumeID string, epoch, seq uint64) (*cow.IntervalMap, Progress, error) {
	cp, err := checkpoint.Read(ctx, m.store, volumeID, epoch, seq)
	if err != nil {
		return nil, Progress{}, fmt.Errorf("materialize: read checkpoint %s/%d/%d: %w", volumeID, epoch, seq, err)
	}
	if !cp.DigestMatches() {
		return nil, Progress{}, fmt.Errorf("%w: checkpoint %s/%d/%d", ErrDigestMismatch, volumeID, epoch, seq)
	}
	if cp.VolumeID != volumeID {
		return nil, Progress{}, fmt.Errorf("%w: checkpoint at %s/%d/%d describes volume %s",
			recovery.ErrObjectIntegrity, volumeID, epoch, seq, cp.VolumeID)
	}
	src, err := m.sourceFor(ctx, volumeID, epoch, cp.DurableSequence, cp.Objects)
	if err != nil {
		return nil, Progress{}, fmt.Errorf("materialize: checkpoint %s/%d/%d: %w", volumeID, epoch, seq, err)
	}
	return m.fetchAndReplay(ctx, src)
}

// sourceFor turns a manifest's or checkpoint's flat key list into a source: every key
// belongs to one epoch, the run must start at that epoch's floor (§12.5), and it must
// reach the sequence the document claims to cover.
func (m *Materializer) sourceFor(ctx context.Context, volumeID string, epoch, target uint64, keys []string) (source, error) {
	floor, err := recovery.PrefixFloor(ctx, m.store, volumeID, epoch)
	if err != nil {
		return source{}, err
	}
	refs := make([]objectRef, len(keys))
	for i, k := range keys {
		refs[i] = objectRef{key: k, epoch: epoch}
	}
	return source{volumeID: volumeID, floor: floor, target: target, refs: refs}, nil
}

// FromEpoch rebuilds everything the volume holds as of `epoch`: the durable prefix of
// that epoch plus every earlier epoch of its chain, each up to the boundary its
// successor recorded (§12.5). This is the source a host-evacuation uses — it needs no
// snapshot and no cooperation from the host being drained (§28.1).
//
// The chain is the point. A volume that has been promoted keeps its earlier writes in
// earlier epochs, so fetching only `epoch` would hand the destination a volume
// missing everything written before its last move, and report it as complete.
func (m *Materializer) FromEpoch(ctx context.Context, volumeID [16]byte, epoch uint64) (*cow.IntervalMap, Progress, error) {
	spans, err := recovery.EpochChain(ctx, m.store, volumeID, epoch)
	if err != nil {
		return nil, Progress{}, fmt.Errorf("materialize: epoch chain %s/%d: %w", format.UUIDString(volumeID), epoch, err)
	}
	durable, err := recovery.DurablePoint(ctx, m.store, volumeID, epoch)
	if err != nil {
		return nil, Progress{}, fmt.Errorf("materialize: durable point %s/%d: %w", format.UUIDString(volumeID), epoch, err)
	}

	src := source{volumeID: format.UUIDString(volumeID), floor: spans[0].From, target: durable}
	for _, span := range spans {
		upto := durable
		if span.Epoch != epoch {
			upto = span.Upto
		}
		keys, err := recovery.ObjectKeysUpTo(ctx, m.store, volumeID, span.Epoch, upto)
		if err != nil {
			return nil, Progress{}, err
		}
		for _, k := range keys {
			src.refs = append(src.refs, objectRef{key: k, epoch: span.Epoch})
		}
	}
	return m.fetchAndReplay(ctx, src)
}

// objectRef is one referenced WAL object and the epoch it must belong to. The epoch
// travels with the key because a chain spans several of them and an object may only
// be replayed as the epoch it was written in.
type objectRef struct {
	key   string
	epoch uint64
}

// source is everything a materialization must be checked against: the volume the
// objects must belong to, the sequence the run has to start at (the epoch floor,
// §12.5) and the one it has to reach (what the manifest/checkpoint claims to cover).
type source struct {
	volumeID string
	floor    uint64
	target   uint64
	refs     []objectRef
}

// grant takes a background token for one object fetch. With no scheduler the caller
// is not competing with a data path (offline restore), so it always proceeds.
func (m *Materializer) grant() bool {
	if m.sched == nil {
		return true
	}
	return m.sched.TryAcquire(ioclass.Background, 1)
}

// fetchAndReplay downloads every referenced object under the background class,
// validates each one against its own header, checks the set is a contiguous run from
// the epoch floor to the sequence the source claims, and replays it into a fresh
// view. Every failure returns a nil view: a half-materialized volume must never
// escape.
//
// The validation is not redundant with the root digest. snapshot.Digest and
// checkpoint.Digest hash the *key strings* they reference, so an object that is
// rewritten, torn, or restored to a wrong version after the document was published
// leaves the digest matching — and wal.Replay stops cleanly at a torn record, which
// is how a short view used to escape while Progress reported the header's full span.
// The contiguity walk is over sequences, not over object boundaries, and it shares
// both rules with recovery.DurablePrefix — one bucket must not be durable to one
// reader and a sequence gap to the other. Overlapping objects (a restarted writer's
// re-batch) are folded at record level, and objects that disagree about a sequence
// they both carry are a hard failure (INV-21, §14.5) rather than a race between two
// replays.
func (m *Materializer) fetchAndReplay(ctx context.Context, src source) (*cow.IntervalMap, Progress, error) {
	var prog Progress
	objs := make([]recovery.ObjectRun, 0, len(src.refs))
	for _, ref := range src.refs {
		if !m.grant() {
			return nil, Progress{}, fmt.Errorf("%w: fetching %s", ErrThrottled, ref.key)
		}
		body, err := m.store.Get(ctx, ref.key)
		switch {
		case errors.Is(err, objectstore.ErrNotFound):
			return nil, Progress{}, fmt.Errorf("%w: %s", ErrMissingObject, ref.key)
		case err != nil:
			return nil, Progress{}, fmt.Errorf("materialize: get %s: %w", ref.key, err)
		}
		span, err := recovery.VerifyObject(src.volumeID, ref.epoch, ref.key, body)
		if err != nil {
			return nil, Progress{}, err
		}
		objs = append(objs, recovery.ObjectRun{Key: ref.key, First: span.First, Last: span.Last, Body: body})
		prog.Objects++
		prog.Bytes += int64(len(body))
	}
	recovery.SortRuns(objs)
	if err := recovery.VerifyAgreement(objs); err != nil {
		return nil, Progress{}, err
	}

	// A run can be perfectly contiguous and still be missing its start: an aborted
	// GC, a partial bucket restore or a mis-scoped lifecycle rule takes the early
	// objects, and what is left rebuilds as a volume with a hole at the front.
	if len(objs) > 0 && objs[0].First != src.floor {
		return nil, Progress{}, fmt.Errorf("%w: %s starts at %d, the epoch's first sequence is %d",
			ErrPrefixFloor, objs[0].Key, objs[0].First, src.floor)
	}
	// Unlike recovery, which stops at a gap and reports the shorter prefix, a
	// materialization that is missing a sequence in the middle of what it was asked
	// for must not boot: the destination would be a volume with a hole.
	covered := recovery.ContiguousEnd(objs, src.floor)
	if n := len(objs); n > 0 && covered < objs[n-1].Last {
		return nil, Progress{}, fmt.Errorf("%w: the referenced objects stop at %d but %s carries %d..%d",
			ErrSequenceGap, covered, objs[n-1].Key, objs[n-1].First, objs[n-1].Last)
	}
	// And the cheap cross-check that catches the rest: whatever the source claims to
	// cover, the objects have to actually reach it.
	if covered != src.target {
		return nil, Progress{}, fmt.Errorf("%w: the objects cover up to %d, the source claims %d",
			ErrCoverageShort, covered, src.target)
	}
	prog.UpTo = covered

	view := cow.NewIntervalMap()
	for _, o := range objs {
		recs, err := wal.Replay(o.Body[format.ObjectHeaderSize:])
		if err != nil {
			return nil, Progress{}, fmt.Errorf("materialize: replay %s: %w", o.Key, err)
		}
		for _, rec := range recs {
			if err := recovery.ApplyRecord(view, m.enc, rec); err != nil {
				return nil, Progress{}, fmt.Errorf("materialize: apply seq %d: %w", rec.Sequence, err)
			}
		}
	}
	m.rec.Gauge(ctx, "bytes_downloaded_before_boot", float64(prog.Bytes))
	return view, prog, nil
}
