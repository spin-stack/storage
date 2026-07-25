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
	"sort"

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
	return m.fetchAndReplay(ctx, man.Objects)
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
	return m.fetchAndReplay(ctx, cp.Objects)
}

// FromEpoch rebuilds the state durable in S3 for a volume's epoch: the longest
// contiguous WAL prefix, which is the recovery authority (§5.8, §22.1). This is the
// source a host-evacuation uses — it needs no snapshot and no cooperation from the
// host being drained (§28.1).
func (m *Materializer) FromEpoch(ctx context.Context, volumeID [16]byte, epoch uint64) (*cow.IntervalMap, Progress, error) {
	durable, err := recovery.DurablePoint(ctx, m.store, volumeID, epoch)
	if err != nil {
		return nil, Progress{}, fmt.Errorf("materialize: durable point %s/%d: %w", format.UUIDString(volumeID), epoch, err)
	}
	keys, err := recovery.ObjectKeysUpTo(ctx, m.store, volumeID, epoch, durable)
	if err != nil {
		return nil, Progress{}, err
	}
	return m.fetchAndReplay(ctx, keys)
}

// object is one fetched WAL object with its parsed sequence span.
type object struct {
	key         string
	first, last uint64
	body        []byte
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
// verifies the set is a contiguous sequence run, and replays it into a fresh view.
// Every failure returns a nil view: a half-materialized volume must never escape.
func (m *Materializer) fetchAndReplay(ctx context.Context, keys []string) (*cow.IntervalMap, Progress, error) {
	var prog Progress
	objs := make([]object, 0, len(keys))
	for _, key := range keys {
		if !m.grant() {
			return nil, Progress{}, fmt.Errorf("%w: fetching %s", ErrThrottled, key)
		}
		body, err := m.store.Get(ctx, key)
		switch {
		case errors.Is(err, objectstore.ErrNotFound):
			return nil, Progress{}, fmt.Errorf("%w: %s", ErrMissingObject, key)
		case err != nil:
			return nil, Progress{}, fmt.Errorf("materialize: get %s: %w", key, err)
		}
		if len(body) < format.ObjectHeaderSize {
			return nil, Progress{}, fmt.Errorf("materialize: short WAL object %s", key)
		}
		h, err := format.UnmarshalObjectHeader(body[:format.ObjectHeaderSize])
		if err != nil {
			return nil, Progress{}, fmt.Errorf("materialize: decode %s: %w", key, err)
		}
		objs = append(objs, object{key: key, first: h.FirstSequence, last: h.LastSequence, body: body})
		prog.Objects++
		prog.Bytes += int64(len(body))
	}
	sort.Slice(objs, func(i, j int) bool { return objs[i].first < objs[j].first })

	for i := 1; i < len(objs); i++ {
		if objs[i].first != objs[i-1].last+1 {
			return nil, Progress{}, fmt.Errorf("%w: %s ends at %d, %s starts at %d",
				ErrSequenceGap, objs[i-1].key, objs[i-1].last, objs[i].key, objs[i].first)
		}
	}

	view := cow.NewIntervalMap()
	for _, o := range objs {
		recs, err := wal.Replay(o.body[format.ObjectHeaderSize:])
		if err != nil {
			return nil, Progress{}, fmt.Errorf("materialize: replay %s: %w", o.key, err)
		}
		for _, rec := range recs {
			if err := recovery.ApplyRecord(view, m.enc, rec); err != nil {
				return nil, Progress{}, fmt.Errorf("materialize: apply seq %d: %w", rec.Sequence, err)
			}
		}
	}
	if n := len(objs); n > 0 {
		prog.UpTo = objs[n-1].last
	}
	m.rec.Gauge(ctx, "bytes_downloaded_before_boot", float64(prog.Bytes))
	return view, prog, nil
}
