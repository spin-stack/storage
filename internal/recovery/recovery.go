// Package recovery reconstructs volume state from S3, the authority for recovery
// (§5.8, §22.1): the durable point is the end of the longest contiguous prefix of
// sequences under wal/<vol>/<epoch>/. Recover replays the WAL objects up to that
// point (decrypting) to rebuild the read view; the summary object accelerates the
// scan (§22.1), and the recovery-point object fixes the epoch frontier (§12.5).
//
// # What this package needs from the object store
//
// The durable point is derived from a LIST, and a LIST that has not caught up answers
// with a *smaller* prefix and no error — the one failure shape that is
// indistinguishable from the truth. So:
//
//   - LIST must be strongly consistent (read-after-write for a new key). That is the
//     backend's obligation, not something this package can check: nothing here can
//     tell "the listing is behind" from "the object is gone". A candidate backend is
//     held to it by integration/backend/conformance_test.go's TestListSeesAFreshPut,
//     which is blocking per backend (§6.1).
//   - Everything the walk cannot afford to guess is read with a GET instead: the
//     epoch's floor (PrefixFloor), its ceiling (EpochCeiling) and the previous
//     epoch's summary (boundaryFloor). A GET failure is an error, never a default —
//     a defaulted floor of 1 or an assumed absent ceiling each produce a number a
//     promotion writes into a create-only object.
//   - The number a stale listing could still bury is refused at the write:
//     WriteRecoveryPoint will not record a boundary below what the previous epoch's
//     own boundary or its summary already establishes.
package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

func walPrefix(volumeID [16]byte, epoch uint64) string {
	return fmt.Sprintf("wal/%s/%d/", format.UUIDString(volumeID), epoch)
}

// ErrObjectIntegrity means a stored WAL object does not match its own header, or
// does not belong to the volume/epoch being recovered. Such an object can never
// raise the durable point (DEV-0003): the prefix ends before it.
var ErrObjectIntegrity = errors.New("recovery: WAL object failed integrity validation")

// ErrBoundaryRegression means an epoch boundary would be recorded below one that is
// already immutable, or below what the previous epoch's writer already ACKed. The
// recovery-point object is create-only: everything under a boundary is under it for
// ever, so a boundary that goes down is unrecoverable data loss (§12.5).
var ErrBoundaryRegression = errors.New("recovery: epoch boundary would move backwards")

// ErrAmbiguousSequence means two validated WAL objects carry different records for a
// sequence they both cover. INV-21 (§14.5) calls for a hard failure on "same range,
// different hash", and this is the only place it can be enforced: the object key
// embeds the payload digest, so divergent objects land on different keys and both
// create-only PUTs succeed. Which one wins would otherwise be decided by sort order.
var ErrAmbiguousSequence = errors.New("recovery: two objects carry different records for the same sequence")

// ObjectSpan is the sequence range a validated WAL object actually carries — the
// records', not the header's claim.
type ObjectSpan struct {
	First, Last uint64
}

// ObjectRun is a validated WAL object reduced to what a prefix walk needs: the
// sequence span its records really cover and the bytes behind them. It is exported
// because the durable point is not the only prefix walk over these objects —
// cross-host materialization walks the very same runs (§20, §22.3) and must reach
// the same verdict about the same bucket.
type ObjectRun struct {
	Key         string
	First, Last uint64
	Body        []byte // the whole object: header + payload
}

// VerifyObject is the single definition of "this stored object is what it claims"
// (DEV-0003). It decodes the header, checks the object belongs to volumeID/epoch,
// and validates the payload against that header; the returned span is the one the
// records really cover.
//
// It exists as an exported function because the durable point is not the only place
// that reads a WAL object out of S3: cross-host materialization replays the very same
// objects, and a manifest or checkpoint cannot stand in for this check — the root
// digest hashes key *strings*, so an object rewritten, torn, or restored to a wrong
// version after publication leaves the digest matching.
func VerifyObject(volumeID string, epoch uint64, key string, body []byte) (ObjectSpan, error) {
	if len(body) < format.ObjectHeaderSize {
		return ObjectSpan{}, fmt.Errorf("%w: %s is %d bytes, too short to hold a header",
			ErrObjectIntegrity, key, len(body))
	}
	h, err := format.UnmarshalObjectHeader(body[:format.ObjectHeaderSize])
	if err != nil {
		// A torn header is exactly as untrustworthy as a torn payload.
		return ObjectSpan{}, fmt.Errorf("%w: %s header: %v", ErrObjectIntegrity, key, err)
	}
	if err := validate(volumeID, epoch, key, h, body[format.ObjectHeaderSize:]); err != nil {
		return ObjectSpan{}, err
	}
	return ObjectSpan{First: h.FirstSequence, Last: h.LastSequence}, nil
}

// validate checks a stored object against its own header before it is allowed to
// contribute anything to the durable point. The header is self-describing but not
// self-proving: it is CRC-protected, so a *torn* header is caught by the decoder,
// while a torn or rewritten payload — and a header that simply claims more than it
// carries — is caught only here.
//
// The checks, in the order they can fail cheaply:
//
//  1. the object belongs to this volume and epoch (a mis-keyed or restored object
//     must not be read as ours);
//  2. the payload is exactly as long as the header says (truncation);
//  3. the payload digest matches (any corruption, including a rewritten tail);
//  4. the records replay, and their count and their first/last sequences are exactly
//     what the header claims (a header cannot claim sequences it does not carry).
func validate(volumeID string, epoch uint64, key string, h format.ObjectHeader, payload []byte) error {
	if got := format.UUIDString(h.VolumeID); got != volumeID {
		return fmt.Errorf("%w: %s belongs to volume %s, not %s", ErrObjectIntegrity, key, got, volumeID)
	}
	if h.Epoch != epoch {
		return fmt.Errorf("%w: %s belongs to epoch %d, not %d", ErrObjectIntegrity, key, h.Epoch, epoch)
	}
	if uint64(len(payload)) != h.PayloadLength {
		return fmt.Errorf("%w: %s payload is %d bytes, header says %d",
			ErrObjectIntegrity, key, len(payload), h.PayloadLength)
	}
	if sha256.Sum256(payload) != h.PayloadSHA256 {
		return fmt.Errorf("%w: %s payload digest mismatch", ErrObjectIntegrity, key)
	}
	recs, err := wal.Replay(payload)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrObjectIntegrity, key, err)
	}
	if uint32(len(recs)) != h.RecordCount {
		return fmt.Errorf("%w: %s holds %d records, header says %d",
			ErrObjectIntegrity, key, len(recs), h.RecordCount)
	}
	if len(recs) == 0 {
		return fmt.Errorf("%w: %s is empty", ErrObjectIntegrity, key)
	}
	if recs[0].Sequence != h.FirstSequence || recs[len(recs)-1].Sequence != h.LastSequence {
		return fmt.Errorf("%w: %s carries sequences %d..%d, header claims %d..%d",
			ErrObjectIntegrity, key, recs[0].Sequence, recs[len(recs)-1].Sequence,
			h.FirstSequence, h.LastSequence)
	}
	for i, rec := range recs {
		if rec.Sequence != h.FirstSequence+uint64(i) {
			return fmt.Errorf("%w: %s is not a contiguous run at record %d (sequence %d)",
				ErrObjectIntegrity, key, i, rec.Sequence)
		}
	}
	return nil
}

// listObjects fetches every WAL object for a volume/epoch, validates it, and returns
// the ones that passed, sorted by first sequence. An object that fails validation is
// dropped rather than fatal: it simply cannot be part of the durable prefix, and the
// contiguity walk stops where it is missing (§22.1). summary.json /
// recovery-point.json are skipped.
// It also verifies that the surviving objects agree with each other (INV-21): the
// listing is refused outright if any two of them carry different records for one
// sequence, because from that point on no answer about the epoch is meaningful.
func listObjects(ctx context.Context, store objectstore.Store, volumeID [16]byte, epoch uint64) ([]ObjectRun, error) {
	infos, err := store.List(ctx, walPrefix(volumeID, epoch))
	if err != nil {
		return nil, err
	}
	want := format.UUIDString(volumeID)
	var objs []ObjectRun
	for _, info := range infos {
		if !strings.HasSuffix(info.Key, ".wal") {
			continue
		}
		body, err := store.Get(ctx, info.Key)
		if err != nil {
			return nil, err
		}
		span, err := VerifyObject(want, epoch, info.Key, body)
		if err != nil {
			// Skipped, not fatal: an object that is not what it claims simply cannot
			// be part of the durable prefix, and the contiguity walk stops where it is
			// missing. Failing here would make a single piece of garbage under the
			// prefix render the volume unrecoverable — and would hand anything that can
			// write to the bucket a denial of service over recovery.
			continue
		}
		objs = append(objs, ObjectRun{Key: info.Key, First: span.First, Last: span.Last, Body: body})
	}
	SortRuns(objs)
	if err := VerifyAgreement(objs); err != nil {
		return nil, err
	}
	return objs, nil
}

// SortRuns orders runs deterministically: by first sequence, then by last, then by
// key. The last two keys matter — two objects starting at the same sequence used to
// be ordered by whatever sort.Slice happened to do, which made the walk below depend
// on the order the backend listed them in.
func SortRuns(objs []ObjectRun) {
	sort.Slice(objs, func(i, j int) bool {
		switch {
		case objs[i].First != objs[j].First:
			return objs[i].First < objs[j].First
		case objs[i].Last != objs[j].Last:
			return objs[i].Last < objs[j].Last
		default:
			return objs[i].Key < objs[j].Key
		}
	})
}

// recordDigests hashes each record the run carries whose sequence is within [lo,hi].
// The digest is over the record's canonical encoding, so it compares the record, not
// the object it happens to be packed in: two writers that produced the same record
// inside differently-cut objects agree, and two that produced different bytes for one
// sequence do not.
func (o ObjectRun) recordDigests(lo, hi uint64) (map[uint64][32]byte, error) {
	recs, err := wal.Replay(o.Body[format.ObjectHeaderSize:])
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrObjectIntegrity, o.Key, err)
	}
	out := make(map[uint64][32]byte, hi-lo+1)
	for _, rec := range recs {
		if rec.Sequence < lo || rec.Sequence > hi {
			continue
		}
		enc, err := rec.Encode()
		if err != nil {
			return nil, fmt.Errorf("%w: %s: re-encoding sequence %d: %v",
				ErrObjectIntegrity, o.Key, rec.Sequence, err)
		}
		out[rec.Sequence] = sha256.Sum256(enc)
	}
	return out, nil
}

// VerifyAgreement enforces INV-21 (§14.5) where it is actually enforceable: where two
// validated objects carry the same sequence, they must carry the same record.
//
// "Same range, different hash ⇒ hard fail" cannot be enforced at the PUT, because the
// object key embeds the payload digest — divergent objects land on *different* keys,
// so both create-only PUTs succeed and the create-only guard never fires. Both then
// pass validation, both are replayed, and the recovered content is decided by
// whichever SHA prefix sorts first. That is the signature of two writers in one epoch
// or of a writer that re-batched after a restart, and it is unrecoverable ambiguity
// rather than a smaller answer, so it fails loudly.
//
// A duplicate that agrees is not a divergence: a re-batched object re-sending records
// it already sent carries the same bytes, and the run below folds it at record level.
//
// objs need not be sorted; the copy this takes is. The overlap scan costs nothing when
// there is none, which is every healthy epoch.
func VerifyAgreement(objs []ObjectRun) error {
	sorted := append([]ObjectRun(nil), objs...)
	SortRuns(sorted)

	var maxLast uint64
	for i, o := range sorted {
		if i > 0 && o.First <= maxLast {
			for j := range i {
				if sorted[j].Last < o.First || sorted[j].First > o.Last {
					continue
				}
				if err := agree(sorted[j], o); err != nil {
					return err
				}
			}
		}
		if o.Last > maxLast {
			maxLast = o.Last
		}
	}
	return nil
}

// agree compares two overlapping runs record by record over the sequences they share.
func agree(a, b ObjectRun) error {
	lo, hi := max(a.First, b.First), min(a.Last, b.Last)
	da, err := a.recordDigests(lo, hi)
	if err != nil {
		return err
	}
	db, err := b.recordDigests(lo, hi)
	if err != nil {
		return err
	}
	for seq := lo; seq <= hi; seq++ {
		if da[seq] != db[seq] {
			return fmt.Errorf("%w: %s and %s both carry sequence %d, with different records",
				ErrAmbiguousSequence, a.Key, b.Key, seq)
		}
	}
	return nil
}

// ContiguousEnd returns the last sequence of the longest run of *records* covering
// [floor, …]. objs must be sorted (SortRuns) and must have passed VerifyAgreement, so
// overlap here is redundancy rather than ambiguity.
//
// The floor matters as much as the contiguity: without it, a bucket whose first
// objects are missing (a partial restore, an aborted GC, a mis-scoped lifecycle rule)
// reads as a healthy prefix starting at whatever survived, and the durable point jumps
// forward over lost data. Objects past a gap are late/orphan (§22.1, §12.5).
//
// The walk is over sequences, not over object boundaries. Demanding that each object
// start exactly where the previous ended made overlapping objects — which a restarted
// writer's re-batch produces — stop the walk early and under-report the durable point
// with no error at all, which a promotion then writes down as an immutable floor.
func ContiguousEnd(objs []ObjectRun, floor uint64) uint64 {
	expected := floor
	last := floor - 1
	for _, o := range objs {
		if o.First > expected {
			break // a real gap: nothing carries `expected`
		}
		if o.Last >= expected {
			last = o.Last
			expected = o.Last + 1
		}
		// Otherwise the object lies entirely below the floor or inside what an earlier
		// one already covered: redundant, and it cannot extend the run.
	}
	return last
}

// PrefixFloor is the first sequence this epoch's WAL must start at: 1 for a volume's
// first epoch, or one past what the previous epoch was recovered up to, which the
// epoch boundary records (§12.5). volumeID is the canonical uuid string, because the
// callers that need a floor for a manifest or a checkpoint hold it in that form.
//
// Only a genuinely absent boundary means floor 1. A boundary that could not be read —
// a throttled GET (§24), a damaged object — is an error, and the distinction is the
// whole point: the epoch's objects legitimately start above 1, so a floor of 1 makes
// the contiguous run reach nothing and DurablePrefix answer 0 with no error. That 0
// is a number a drain writes into the next epoch's create-only recovery point, which
// buries every ACKed write below it permanently.
func PrefixFloor(ctx context.Context, store objectstore.Store, volumeID string, epoch uint64) (uint64, error) {
	body, err := store.Get(ctx, recoveryPointKeyFor(volumeID, epoch))
	switch {
	case errors.Is(err, objectstore.ErrNotFound):
		return 1, nil
	case err != nil:
		return 0, fmt.Errorf("recovery: cannot read the epoch %d boundary of %s: %w", epoch, volumeID, err)
	}
	var rp RecoveryPoint
	if err := json.Unmarshal(body, &rp); err != nil {
		return 0, fmt.Errorf("recovery: the epoch %d boundary of %s is damaged: %w", epoch, volumeID, err)
	}
	return rp.RecoveredUpTo + 1, nil
}

// ErrBrokenEpochChain means an epoch's predecessor holds data that cannot be chained
// to it: the boundary between them is missing. Rebuilding from the newest epoch alone
// would silently drop everything written before the promotion, so recovery stops.
var ErrBrokenEpochChain = errors.New("recovery: epoch chain is broken")

// EpochSpan is one link of a volume's history: an epoch and the sequences of it that
// belong to the volume's state. Upto is zero for the newest epoch, whose end is the
// durable point rather than a recorded boundary.
type EpochSpan struct {
	Epoch uint64
	From  uint64
	Upto  uint64
}

// EpochChain walks a volume's epochs back from `epoch` to its first, following the
// recovery-point objects each promotion wrote (§12.5), and returns the spans oldest
// first.
//
// A volume's sequence space is continuous across epochs, so its state is the whole
// chain: epoch N holds everything written before the promotion that opened N+1, up to
// exactly the boundary N+1 recorded. Scanning only the newest epoch hands back a
// volume containing just the writes made since its last move — which is what every
// failover, drain, or evacuation after the first one would have produced.
func EpochChain(ctx context.Context, store objectstore.Store, volumeID [16]byte, epoch uint64) ([]EpochSpan, error) {
	type link struct{ epoch, from uint64 }
	var links []link // newest first

	for e := epoch; ; {
		rp, err := ReadRecoveryPoint(ctx, store, volumeID, e)
		switch {
		case err == nil:
			if rp.PrevEpoch == 0 || rp.PrevEpoch >= e {
				return nil, fmt.Errorf("%w: epoch %d records predecessor %d", ErrBrokenEpochChain, e, rp.PrevEpoch)
			}
			from := rp.RecoveredUpTo + 1
			// Walking back, each older epoch must start no later than the one after
			// it. A boundary that is higher than its successor's means the successor
			// was recorded below a point that was already immutable: the spans would
			// run backwards, and everything between them is under a floor nobody can
			// raise again (§12.5).
			if n := len(links); n > 0 && from > links[n-1].from {
				return nil, fmt.Errorf("%w: epoch %d starts at %d, above epoch %d's start %d — the boundaries run backwards",
					ErrBrokenEpochChain, e, from, links[n-1].epoch, links[n-1].from)
			}
			links = append(links, link{epoch: e, from: from})
			e = rp.PrevEpoch
		case errors.Is(err, objectstore.ErrNotFound):
			// No boundary, so this must be the volume's first epoch. If an earlier
			// one holds objects, the boundary was lost and we must not pretend the
			// volume's history starts here.
			for earlier := uint64(1); earlier < e; earlier++ {
				objs, lerr := listObjects(ctx, store, volumeID, earlier)
				if lerr != nil {
					return nil, lerr
				}
				if len(objs) > 0 {
					return nil, fmt.Errorf("%w: epoch %d has no boundary, but epoch %d holds data",
						ErrBrokenEpochChain, e, earlier)
				}
			}
			links = append(links, link{epoch: e, from: 1})

			spans := make([]EpochSpan, 0, len(links))
			for i := len(links) - 1; i >= 0; i-- { // oldest first
				span := EpochSpan{Epoch: links[i].epoch, From: links[i].from}
				if i > 0 {
					// Its successor recorded where this epoch ends.
					span.Upto = links[i-1].from - 1
				}
				spans = append(spans, span)
			}
			return spans, nil
		default:
			return nil, err
		}
	}
}

// ObjectKeysUpTo returns the keys of the WAL objects whose last sequence is <= upTo,
// in order. Used to build a snapshot manifest (§19) covering a captured sequence.
func ObjectKeysUpTo(ctx context.Context, store objectstore.Store, volumeID [16]byte, epoch, upTo uint64) ([]string, error) {
	objs, err := listObjects(ctx, store, volumeID, epoch)
	if err != nil {
		return nil, err
	}
	var keys []string
	for _, o := range objs {
		if o.Last <= upTo {
			keys = append(keys, o.Key)
		}
	}
	return keys, nil
}

// EpochCeiling is the highest sequence of `epoch` that anything ever adopted, and the
// mirror of PrefixFloor. An epoch that has been superseded has one: the promotion that
// opened the next epoch recorded, in a create-only object, exactly how far it
// recovered (§12.5). Everything above that was written by a writer that was already
// fenced and was never part of the volume.
//
// Without it, a PUT still in flight when the promotion happened — a slow backend, an
// SDK retry — lands afterwards and extends the old epoch's prefix past the boundary.
// Nothing rewrites the boundary, so from then on the same bucket answers two different
// questions about one volume: `materialize.FromEpoch(vol, N)` rebuilds a state the
// live volume at N+1 never had, and a drain resumed by finishMovedVolume recomputes a
// number that contradicts an immutable object and fails hard for ever.
//
// The successor is `epoch+1`: epochs are consecutive here (the drain derives
// prevEpoch as newEpoch-1), and the object is read with a strongly consistent GET
// rather than found by a LIST. A boundary that records a *different* predecessor makes
// no claim about this epoch and is not treated as its ceiling. An unreadable one is an
// error: this number is as load-bearing as the floor, and guessing "no ceiling" is the
// over-reporting the whole function exists to prevent.
func EpochCeiling(ctx context.Context, store objectstore.Store, volumeID [16]byte, epoch uint64) (uint64, bool, error) {
	rp, err := ReadRecoveryPoint(ctx, store, volumeID, epoch+1)
	switch {
	case errors.Is(err, objectstore.ErrNotFound):
		return 0, false, nil // still open: no promotion has closed this epoch
	case err != nil:
		return 0, false, fmt.Errorf("recovery: cannot read the epoch %d boundary of %s: %w",
			epoch+1, format.UUIDString(volumeID), err)
	}
	if rp.PrevEpoch != epoch {
		return 0, false, nil
	}
	return rp.RecoveredUpTo, true, nil
}

// DurablePrefix returns the durable point for a volume/epoch computed from S3 alone
// (INV-08): the end of the longest contiguous run of *validated* records between the
// epoch's floor and, once a promotion has closed the epoch, its ceiling.
func DurablePrefix(ctx context.Context, store objectstore.Store, volumeID [16]byte, epoch uint64) (uint64, error) {
	adopted, err := durablePrefix(ctx, store, volumeID, epoch)
	return adopted, err
}

// durablePrefix is what the epoch's own objects establish, clamped to the ceiling a
// successor recorded.
//
// It used to return both numbers, because the unclamped one told a summary over-claim
// ("the summary is lying") apart from a superseded epoch ("a fenced writer's PUT landed
// late"). The summary went with ADR-0026 — nothing ever wrote one — so there is one
// number again and the distinction has no reader.
func durablePrefix(ctx context.Context, store objectstore.Store, volumeID [16]byte, epoch uint64) (adopted uint64, err error) {
	objs, err := listObjects(ctx, store, volumeID, epoch)
	if err != nil {
		return 0, err
	}
	floor, err := PrefixFloor(ctx, store, format.UUIDString(volumeID), epoch)
	if err != nil {
		return 0, err
	}
	proven := ContiguousEnd(objs, floor)

	ceiling, closed, err := EpochCeiling(ctx, store, volumeID, epoch)
	if err != nil {
		return 0, err
	}
	if closed && ceiling < proven {
		return ceiling, nil
	}
	return proven, nil
}

// DurablePoint is DurablePrefix with a summary cross-check (§22.1): the summary
// object must never claim a durable sequence beyond what the contiguous prefix
// actually provides. In production the summary lets recovery start its LIST near the
// end instead of scanning everything; here it is a correctness guard.
//
// An over-claim is real — an object that was uploaded is gone — so it is reported,
// not smoothed over. It is reported as *SummaryOverclaim, carrying the prefix S3 can
// still prove, so a caller can act on the discrepancy (escalate, recover the shorter
// prefix) instead of retrying an opaque error against an epoch that will never
// answer differently.
// The cross-check is against what the epoch's own objects prove, not against the
// ceiling a successor imposed on it: a writer that ACKed up to 6 and was then fenced
// at 4 wrote an honest summary, and reporting that as an over-claim would turn every
// ordinary promotion into an unrecoverable epoch.
func DurablePoint(ctx context.Context, store objectstore.Store, volumeID [16]byte, epoch uint64) (uint64, error) {
	// §22.1 also described a summary object cross-checked against this prefix. It went
	// on 2026-08-02 with ADR-0026: nothing ever wrote one, so the check was a permanent
	// no-op, and V1 has no promotion for it to become a floor for. The contiguous
	// prefix is the authority, which is what §5.8 said all along.
	return durablePrefix(ctx, store, volumeID, epoch)
}

// Recover reconstructs the read view (interval map) from S3 up to the durable point,
// decrypting each record with enc (nil for plaintext volumes). It walks the volume's
// whole epoch chain (§12.5) — a volume that has been promoted keeps its earlier
// writes in earlier epochs, and replaying only the newest one would hand back a
// volume missing everything written before its last move. It returns the view and the
// durable sequence of the requested epoch.
func Recover(ctx context.Context, store objectstore.Store, enc *wal.Encryption, volumeID [16]byte, epoch uint64) (*cow.IntervalMap, uint64, error) {
	return RecoverOver(ctx, store, enc, volumeID, epoch, nil)
}

// RecoverOver is Recover with the view layered over base — a clone's own records
// replayed on top of the parent snapshot it reads through (§20).
//
// The layering has to happen *here*, at construction, and not by handing the result to
// SetBase afterwards: an unlayered map discards its tombstones as it replays, so giving
// it a base later would uncover every range the clone was told to DISCARD. A nil base
// gives exactly the map Recover always returned.
func RecoverOver(ctx context.Context, store objectstore.Store, enc *wal.Encryption, volumeID [16]byte, epoch uint64, base *cow.IntervalMap) (*cow.IntervalMap, uint64, error) {
	spans, err := EpochChain(ctx, store, volumeID, epoch)
	if err != nil {
		return nil, 0, err
	}
	durable, err := DurablePoint(ctx, store, volumeID, epoch)
	if err != nil {
		return nil, 0, err
	}

	view := cow.NewIntervalMapOver(base)
	for _, span := range spans {
		upto := durable
		if span.Epoch != epoch {
			// An earlier epoch is covered exactly up to the boundary its successor
			// recorded; anything past that was never adopted (§12.5).
			upto = span.Upto
		}
		if err := replayEpoch(ctx, store, enc, volumeID, span.Epoch, upto, view); err != nil {
			return nil, 0, err
		}
	}
	return view, durable, nil
}

// replayEpoch applies one epoch's validated objects up to `upto` into view.
func replayEpoch(ctx context.Context, store objectstore.Store, enc *wal.Encryption, volumeID [16]byte, epoch, upto uint64, view *cow.IntervalMap) error {
	objs, err := listObjects(ctx, store, volumeID, epoch)
	if err != nil {
		return err
	}
	// Objects are sorted and have been proven to agree wherever they overlap, so
	// replaying a sequence twice is replaying the same record twice — idempotent for
	// every record type (§14.1) — and the order stays deterministic.
	for _, o := range objs {
		if o.First > upto {
			break // past what this epoch contributes (sorted by first)
		}
		recs, err := wal.Replay(o.Body[format.ObjectHeaderSize:])
		if err != nil {
			return fmt.Errorf("recovery: replay %s: %w", o.Key, err)
		}
		for _, rec := range recs {
			if rec.Sequence > upto {
				break
			}
			if err := ApplyRecord(view, enc, rec); err != nil {
				return err
			}
		}
	}
	return nil
}

// ErrSealedWithoutKey is returned when a replayed record is sealed and the replay was
// handed no Encryption to open it. It is deliberately not a decryption failure: the
// key was never offered, so there is nothing to tamper with and nothing to retry —
// the caller passed the wrong arguments.
var ErrSealedWithoutKey = errors.New("recovery: record is sealed and this replay holds no key")

// ApplyRecord folds one replayed record into the read view, decrypting WRITEs when
// the volume is encrypted. It is the single definition of "replaying a WAL record
// onto a view", shared by recovery and by cross-host materialization (§20, §22).
//
// A nil enc means "this volume is plaintext", and the record itself is what says
// whether that is true: KeyID 0 is not a key version, it is the on-disk marker for a
// cleartext payload (§14.1, and wal.ErrUnversionedKey spells out why 0 can never be a
// DEK). So a sealed record arriving with no key is a contradiction, and it fails here.
//
// This check is load-bearing rather than defensive. Without it the mistake is
// invisible in every direction: crypto.Seal returns ciphertext of exactly the
// plaintext's length with the GCM tag held separately in the header, so folding the
// undecrypted payload into the view overwrites the right extent with the right number
// of bytes and no CRC is consulted on this path. internal/agent shipped exactly that —
// a literal nil passed to RecoverOver for a volume whose DEK it had just unwrapped —
// and the guest was served its own data as ciphertext with no error anywhere. Callers
// that legitimately have no key (a plaintext volume, dev mode without a KMS) are
// unaffected, because their records carry KeyID 0.
func ApplyRecord(view *cow.IntervalMap, enc *wal.Encryption, rec wal.Record) error {
	switch rec.Type {
	case format.RecordWrite:
		payload := rec.Payload
		switch {
		case enc != nil:
			pt, err := enc.Decrypt(rec)
			if err != nil {
				return fmt.Errorf("recovery: decrypt seq %d: %w", rec.Sequence, err)
			}
			payload = pt
		case rec.KeyID != 0:
			return fmt.Errorf("%w: sequence %d is sealed with key version %d",
				ErrSealedWithoutKey, rec.Sequence, rec.KeyID)
		}
		view.Overwrite(rec.Offset, payload)
	case format.RecordDiscard, format.RecordWriteZeroes:
		view.Clear(rec.Offset, uint64(rec.Length))
	}
	return nil
}

// RecoveryPoint is the immutable epoch frontier written by a promoted writer (§12.5).
type RecoveryPoint struct {
	PrevEpoch     uint64 `json:"prev_epoch"`
	RecoveredUpTo uint64 `json:"recovered_up_to"`
}

func recoveryPointKeyFor(volumeID string, newEpoch uint64) string {
	return fmt.Sprintf("wal/%s/%d/recovery-point.json", volumeID, newEpoch)
}

func recoveryPointKey(volumeID [16]byte, newEpoch uint64) string {
	return recoveryPointKeyFor(format.UUIDString(volumeID), newEpoch)
}

// boundaryFloor is the lowest sequence a new boundary over prevEpoch may record: the
// highest point anything already established for that epoch. Two sources, both read
// with strongly consistent GETs rather than a LIST:
//
//   - prevEpoch's own boundary, which is immutable — a new one below it would put
//     sequences under a floor that can never be raised again;
//   - prevEpoch's summary, which is what its writer ACKed. A LIST that has not caught
//     up reports a shorter prefix with no error, and writing *that* number down is how
//     a stale listing loses an ACKed FLUSH for good.
func boundaryFloor(ctx context.Context, store objectstore.Store, volumeID [16]byte, prevEpoch uint64) (uint64, error) {
	var floor uint64
	switch rp, err := ReadRecoveryPoint(ctx, store, volumeID, prevEpoch); {
	case err == nil:
		floor = rp.RecoveredUpTo
	case errors.Is(err, objectstore.ErrNotFound):
		// prevEpoch is the volume's first epoch: no earlier boundary to respect.
	default:
		return 0, fmt.Errorf("recovery: cannot read the epoch %d boundary: %w", prevEpoch, err)
	}
	return floor, nil
}

// WriteAs records the boundary of newEpoch on behalf of hostID — the host newEpoch was
// granted to, which §12.5 makes the rightful author of it.
//
// It is the entry point every caller that knows which host it speaks for should use.
// The boundary is create-only and immutable, so a boundary written by a host that was
// never granted the epoch is not a mistake anybody can correct afterwards: the epoch's
// real holder inherits a floor it did not choose, and every sequence beneath it is
// unreachable for good. VerifyPublisher is what separates that from an ordinary
// promotion — see there for the cases it deliberately lets through, and why an empty
// hostID is refused for any epoch that has a holder.
//
// The check is made before the PUT and not repeated after it. Unlike a checkpoint,
// which authorises a local truncation once it is published, this function's PUT is the
// last thing it does: there is no later step a second read could still refuse, and the
// object it just wrote cannot be unwritten.
func (rp RecoveryPoint) WriteAs(ctx context.Context, store objectstore.Store, volumeID [16]byte, newEpoch uint64, hostID string) error {
	if err := VerifyPublisher(ctx, store, format.UUIDString(volumeID), newEpoch, hostID); err != nil {
		return fmt.Errorf("recovery: epoch %d boundary of %s: %w", newEpoch, format.UUIDString(volumeID), err)
	}
	return WriteRecoveryPoint(ctx, store, volumeID, newEpoch, rp.PrevEpoch, rp.RecoveredUpTo)
}

// WriteRecoveryPoint records the epoch boundary at the start of newEpoch (§12.5):
// which prior epoch was recovered and up to which sequence. Create-only.
//
// The boundary is the one number in the system nothing can walk back: the next epoch
// will never look below it, and the object cannot be rewritten. So it is refused
// outright if it would move backwards — below the previous epoch's own boundary, or
// below what that epoch's writer already ACKed. Every way of computing a
// too-low value (a stale LIST, an unreadable floor, a GC-shortened prefix) is caught
// here, at the write, instead of being discovered as missing data long afterwards.
//
// This form does not name its author, so it cannot be checked against the grant: it
// enforces the *value* of the boundary, never the right to record one. Prefer
// RecoveryPoint.WriteAs. The remaining unnamed caller is controlplane.Drainer, which
// writes the boundary immediately after Promote granted newEpoch to its destination
// and has that host id in hand; until it passes it, that write is fenced by §12.3's
// own CAS alone.
func WriteRecoveryPoint(ctx context.Context, store objectstore.Store, volumeID [16]byte, newEpoch, prevEpoch, recoveredUpTo uint64) error {
	floor, err := boundaryFloor(ctx, store, volumeID, prevEpoch)
	if err != nil {
		return err
	}
	if recoveredUpTo < floor {
		return fmt.Errorf("%w: epoch %d would record %d, below the %d already established for epoch %d",
			ErrBoundaryRegression, newEpoch, recoveredUpTo, floor, prevEpoch)
	}
	body, err := json.Marshal(RecoveryPoint{PrevEpoch: prevEpoch, RecoveredUpTo: recoveredUpTo})
	if err != nil {
		return err
	}
	_, err = store.Put(ctx, recoveryPointKeyFor(format.UUIDString(volumeID), newEpoch), body, objectstore.PutOptions{IfNoneMatch: true})
	return err
}

// ReadRecoveryPoint loads the epoch-boundary object for newEpoch.
func ReadRecoveryPoint(ctx context.Context, store objectstore.Store, volumeID [16]byte, newEpoch uint64) (RecoveryPoint, error) {
	var rp RecoveryPoint
	body, err := store.Get(ctx, recoveryPointKey(volumeID, newEpoch))
	if err != nil {
		return rp, err
	}
	if err := json.Unmarshal(body, &rp); err != nil {
		return rp, err
	}
	return rp, nil
}
