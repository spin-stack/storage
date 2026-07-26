package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/materialize"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/placement"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/wal/format"
)

// Sentinel errors a caller branches on.
var (
	// ErrOperationMismatch means the operation id names a different drain than the
	// one being asked for — another host, or another kind of operation entirely.
	ErrOperationMismatch = errors.New("controlplane: the operation id belongs to another drain")

	// ErrEpochAdvanced means the volume is no longer where this operation's own
	// promotion put it: somebody else moved it on. Everything the drain still owes
	// that volume — its epoch boundary, its capacity release — is now unauthorable,
	// because both are statements about a promotion this operation can no longer
	// prove it performed.
	ErrEpochAdvanced = errors.New("controlplane: the volume advanced past the epoch this drain granted")

	// ErrCapacityLedgerMoved means a host's committed bytes are neither what they
	// were before this move's capacity change nor what they would be after it: a
	// third party is writing the same ledger. The drain reports it instead of
	// guessing, because a wrong guess either wedges the operation (an underflow
	// every later pass repeats) or silently consumes another volume's reservation.
	ErrCapacityLedgerMoved = errors.New("controlplane: the host's committed capacity changed under the move")

	// ErrReservationNotReleased means a move that will not happen could not give its
	// destination reservation back. A reservation nobody remembers making is a host
	// placement under-uses forever, so it is surfaced rather than swallowed (§28.2).
	ErrReservationNotReleased = errors.New("controlplane: a destination reservation could not be released")

	// ErrHostAlreadyDraining means another drain operation is still live for this
	// host. Two evacuations of one host capture the same plan and promote the same
	// volumes; the one that loses each race holds a destination reservation that
	// nobody will ever release, and placement under-uses that host forever (§28.2).
	ErrHostAlreadyDraining = errors.New("controlplane: the host already has a live drain operation")

	// ErrDurableRegression means a move was about to record an epoch boundary below
	// what the volume already had durable. The boundary is immutable and is what
	// every later recovery treats as the floor, so writing one that goes backwards
	// does not lose data slowly — it loses it permanently, at the moment of writing.
	ErrDurableRegression = errors.New("controlplane: refusing an epoch boundary below the volume's durable point")
)

// Move records one volume evacuated from a host.
type Move struct {
	VolumeID string
	FromHost string
	ToHost   string
	NewEpoch uint64
	UpTo     uint64 // sequence the destination materialized through
	Bytes    int64  // bytes fetched from the object store for this move
}

// DrainResult is the outcome of one Drain pass. A drain is reconciled: an
// interrupted pass is resumed by calling Drain again with the same operation id.
// Phase is the shared reconciliation phase (§7) — a drain has no private
// vocabulary; what it is *doing* lives in the operation's current_state.
type DrainResult struct {
	Phase     lifecycle.OperationPhase
	Moved     []Move
	Remaining int
}

// plan is the operation's desired_state: the volumes this drain committed to move,
// captured once when the operation is recorded. Every later pass walks this list
// rather than asking who owns what right now — a volume that was already promoted is
// no longer listed under the source, and driving from the live listing is exactly how
// a half-finished move used to disappear (DEV-0008).
type plan struct {
	Host    string   `json:"drain_host"`
	Volumes []string `json:"volumes"`
}

// stage is how far this operation has taken one volume, recorded *before* each step
// rather than after it. That order is what makes the pass resumable: a stage says
// "this operation is the one that did (or is about to do) this", which is a fact no
// later pass can reconstruct by looking at who owns the volume now.
type stage string

const (
	// stageReserving: the destination is chosen and its reservation is about to be
	// committed.
	stageReserving stage = "RESERVING"
	// stageMoving: the reservation is held; materialization and promotion may run.
	stageMoving stage = "MOVING"
	// stagePromoted: *this operation* fenced the source and was granted NewEpoch on
	// ToHost. Only a volume in this stage may have its epoch boundary written here.
	stagePromoted stage = "PROMOTED"
	// stageReleasing: the boundary is written and the source's release is next.
	stageReleasing stage = "RELEASING"
	// stageDone: the move is complete.
	stageDone stage = "DONE"
	// stageForeign: the volume left the source under somebody else's promotion. It is
	// evacuated, but not by this operation, which therefore writes no boundary for it
	// and releases no capacity for it.
	stageForeign stage = "FOREIGN"
)

// volumeProgress is what this operation has established about one volume. The
// capacity fields are the ledger values observed immediately before a change: with
// CommitHostCapacity being a delta rather than a compare-and-set, they are the only
// proof a resumed pass has that its own change already landed.
type volumeProgress struct {
	VolumeID  string `json:"volume_id"`
	Stage     stage  `json:"stage"`
	ToHost    string `json:"to_host,omitempty"`
	PrevEpoch uint64 `json:"prev_epoch,omitempty"`
	NewEpoch  uint64 `json:"new_epoch,omitempty"`
	UpTo      uint64 `json:"recovered_up_to,omitempty"`
	Bytes     int64  `json:"bytes,omitempty"`
	DstBefore int64  `json:"dst_committed_before,omitempty"`
	SrcBefore int64  `json:"src_committed_before,omitempty"`
	Note      string `json:"note,omitempty"`
}

// progress is the operation's current_state (§28.1: progress must be visible).
type progress struct {
	Total   int              `json:"total"`
	Current string           `json:"current_volume,omitempty"`
	Volumes []volumeProgress `json:"volumes,omitempty"`
}

// volume returns the recorded progress for volumeID, or nil if this operation has
// not started it. The pointer is into the slice: callers mutate it in place and save.
func (p *progress) volume(volumeID string) *volumeProgress {
	for i := range p.Volumes {
		if p.Volumes[i].VolumeID == volumeID {
			return &p.Volumes[i]
		}
	}
	return nil
}

// begin records the intent to move volumeID to dest before anything is reserved.
func (p *progress) begin(volumeID, dest string, prevEpoch uint64, dstCommitted int64) *volumeProgress {
	p.Volumes = append(p.Volumes, volumeProgress{
		VolumeID: volumeID, Stage: stageReserving, ToHost: dest,
		PrevEpoch: prevEpoch, DstBefore: dstCommitted,
	})
	return &p.Volumes[len(p.Volumes)-1]
}

// foreign records that the volume left the source under another actor's promotion.
func (p *progress) foreign(volumeID, owner string) {
	p.Volumes = append(p.Volumes, volumeProgress{
		VolumeID: volumeID, Stage: stageForeign, ToHost: owner,
		Note: "promoted by another actor; this operation wrote no boundary and released no capacity",
	})
}

// drop forgets a volume, so a later pass places it afresh.
func (p *progress) drop(volumeID string) {
	for i := range p.Volumes {
		if p.Volumes[i].VolumeID == volumeID {
			p.Volumes = append(p.Volumes[:i], p.Volumes[i+1:]...)
			return
		}
	}
}

// settled reports whether this operation has nothing left to do for the volume.
func (p *progress) settled(volumeID string) bool {
	vp := p.volume(volumeID)
	return vp != nil && (vp.Stage == stageDone || vp.Stage == stageForeign)
}

// remaining is how many planned volumes are still unsettled.
func (p *progress) remaining() int {
	left := p.Total
	for _, vp := range p.Volumes {
		if vp.Stage == stageDone || vp.Stage == stageForeign {
			left--
		}
	}
	if left < 0 {
		return 0
	}
	return left
}

// Drainer evacuates every volume off a host (§28.1). It is the composition of the
// pieces that already carry their own invariants: placement (§20/§28.2),
// materialization from S3 (§22.3), and the promotion protocol (§12.3–12.5). It adds
// no new durability rule — in particular it never moves a volume without fencing
// its previous writer first, and never speaks for a move it did not make.
type Drainer struct {
	md       metadata.Store
	promoter *Promoter
	mat      *materialize.Materializer
	store    objectstore.Store
	policy   placement.Policy
}

// NewDrainer builds a Drainer.
func NewDrainer(md metadata.Store, promoter *Promoter, mat *materialize.Materializer,
	store objectstore.Store, policy placement.Policy,
) *Drainer {
	return &Drainer{md: md, promoter: promoter, mat: mat, store: store, policy: policy}
}

// Cancel asks a drain to stop. It is honored at the next volume boundary — never
// between promoting the destination and releasing the source — so a canceled drain
// always leaves every volume with exactly one writer.
func (d *Drainer) Cancel(ctx context.Context, term int64, operationID string) error {
	op, err := d.md.GetOperation(ctx, operationID)
	if errors.Is(err, metadata.ErrNotFound) {
		// Canceled before it started: record the intent so the drain sees it.
		_, rerr := d.md.RecordOperation(ctx, term, metadata.Operation{
			OperationID: operationID, Kind: lifecycle.OpDrain,
			DesiredState: []byte(`{}`), CurrentState: []byte(`{}`), Phase: lifecycle.OpCanceling,
		})
		return rerr
	}
	if err != nil {
		return err
	}
	// The store refuses this on a terminal operation (lifecycle.ErrInvalidTransition):
	// a finished drain cannot be un-finished.
	op.Phase = lifecycle.OpCanceling
	return d.md.UpdateOperation(ctx, term, op)
}

// Drain evacuates hostID. It cordons the host, then moves every volume in the plan
// this operation recorded. The pass is idempotent and resumable: what it already did
// is read back from the operation's own progress, never inferred from who owns a
// volume now (§7, §18, §28.1).
//
// Errors are expected and retryable — ErrFencingWaitNotElapsed until the source's
// lease can no longer ACK durability, placement.ErrNoCapacity when the fleet has no
// room, materialize.ErrThrottled while the data path is busy. The operation keeps
// its progress and the reconciler calls Drain again.
func (d *Drainer) Drain(ctx context.Context, term int64, hostID, operationID string) (DrainResult, error) {
	p, prog, op, err := d.operation(ctx, hostID, operationID)
	if err != nil {
		return DrainResult{}, err
	}
	if op != nil && op.Phase.Terminal() {
		// A duplicate of a finished request must not touch a thing — least of all the
		// host's fleet state. Re-cordoning a host an operator repaired and returned to
		// ACTIVE takes it out of placement again, silently, while the call reports
		// success. That is why the cordon lives behind this short-circuit.
		return DrainResult{Phase: op.Phase, Remaining: prog.remaining()}, nil
	}

	// One live evacuation per host, decided before anything is written.
	if err := d.exclusive(ctx, hostID, operationID); err != nil {
		return DrainResult{}, err
	}

	// Cordon first: even if this pass aborts immediately, nothing new lands here.
	if err := d.md.SetHostState(ctx, term, hostID, lifecycle.HostCordoned); err != nil {
		return DrainResult{}, err
	}
	if err := d.md.SetHostState(ctx, term, hostID, lifecycle.HostDraining); err != nil {
		return DrainResult{}, err
	}

	if p.Host == "" {
		// The plan is captured once. On a resumed pass we read back what this
		// operation committed to move, so a volume that was promoted before the crash
		// is still on the list and gets finished (DEV-0008).
		vols, err := d.md.ListVolumesByHost(ctx, hostID)
		if err != nil {
			return DrainResult{}, err
		}
		p = plan{Host: hostID}
		for _, v := range vols {
			p.Volumes = append(p.Volumes, v.VolumeID)
		}
		prog = progress{Total: len(p.Volumes)}
		if _, err := d.md.RecordOperation(ctx, term, metadata.Operation{
			OperationID: operationID, Kind: lifecycle.OpDrain, HostID: hostID,
			DesiredState: mustJSON(p), CurrentState: mustJSON(prog), Phase: lifecycle.OpPending,
		}); err != nil {
			return DrainResult{}, err
		}
	}

	res := DrainResult{Phase: lifecycle.OpRunning, Remaining: prog.remaining()}

	// A cancellation recorded before the first pass wins here, so the operation
	// never leaves PENDING for RUNNING just to be canceled a line later.
	canceled, err := d.canceled(ctx, operationID)
	if err != nil {
		return res, err
	}
	if canceled {
		res.Phase = lifecycle.OpCanceled
		return res, d.record(ctx, term, operationID, lifecycle.OpCanceled, prog, "")
	}
	// PENDING -> RUNNING (or RUNNING/FAILED -> RUNNING on a resumed pass). An empty
	// host still runs: cordoning it is work.
	if err := d.save(ctx, term, operationID, prog); err != nil {
		return res, err
	}

	for _, volumeID := range p.Volumes {
		if prog.settled(volumeID) {
			continue // finished (or evacuated by somebody else) in an earlier pass
		}
		canceled, err := d.canceled(ctx, operationID)
		if err != nil {
			return res, err
		}
		if canceled {
			res.Phase = lifecycle.OpCanceled
			return res, d.record(ctx, term, operationID, lifecycle.OpCanceled, prog, "")
		}

		prog.Current = volumeID
		if err := d.save(ctx, term, operationID, prog); err != nil {
			return res, err
		}
		v, err := d.md.GetVolume(ctx, volumeID)
		if err != nil {
			return res, err
		}
		mv, err := d.move(ctx, term, operationID, hostID, v, &prog)
		if err != nil {
			d.recordFailure(ctx, term, operationID, prog, err)
			return res, err
		}
		if mv.VolumeID != "" {
			res.Moved = append(res.Moved, mv)
		}
		res.Remaining = prog.remaining()
		prog.Current = ""
		if err := d.save(ctx, term, operationID, prog); err != nil {
			return res, err
		}
	}

	res.Phase = lifecycle.OpSucceeded
	// The host stays DRAINING: returning it to ACTIVE is an operator decision.
	return res, d.record(ctx, term, operationID, lifecycle.OpSucceeded, prog, "")
}

// operation loads the recorded drain, or reports that there is none yet. A drain is
// identified by its operation id *and* the host it evacuates: an id reused for
// another host — an operator retry with a copy-pasted id, a replayed request, a UI
// keyed on the wrong entity — would drain the recorded plan and bill this host for
// it, burning create-only boundary keys on volumes it never owned and reporting
// SUCCEEDED over fabricated moves.
func (d *Drainer) operation(ctx context.Context, hostID, operationID string) (plan, progress, *metadata.Operation, error) {
	var (
		p    plan
		prog progress
	)
	op, err := d.md.GetOperation(ctx, operationID)
	if errors.Is(err, metadata.ErrNotFound) {
		return p, prog, nil, nil
	}
	if err != nil {
		return p, prog, nil, err
	}
	if op.Kind != lifecycle.OpDrain {
		return p, prog, nil, fmt.Errorf("%w: operation %s is a %s", ErrOperationMismatch, operationID, op.Kind)
	}
	if err := json.Unmarshal(op.DesiredState, &p); err != nil {
		return p, prog, nil, fmt.Errorf("drain: decode plan: %w", err)
	}
	if err := json.Unmarshal(op.CurrentState, &prog); err != nil {
		return p, prog, nil, fmt.Errorf("drain: decode progress: %w", err)
	}
	// Either recording may be empty (a cancellation registered before the first pass
	// has neither), but neither may name a different host.
	for _, recorded := range [...]string{p.Host, op.HostID} {
		if recorded != "" && recorded != hostID {
			return p, prog, nil, fmt.Errorf("%w: operation %s was recorded for host %s, not %s",
				ErrOperationMismatch, operationID, recorded, hostID)
		}
	}
	return p, prog, &op, nil
}

// exclusive refuses to start a second evacuation of a host that already has one.
// Two drains of one host are not two halves of the same work: each captures its own
// plan, each promotes the same volumes, and whichever loses a race is left holding a
// destination reservation nobody will release, because releasing it is the losing
// operation's own next step and that step now fails forever (§28.2). The volumes are
// safe either way — the promotion protocol serializes them — but the accounting is
// not, and a host that placement believes is full is a host that stays empty.
//
// "Live" is the operation lifecycle's own answer: not Terminal. FAILED counts as
// live deliberately, because FAILED -> RUNNING is a legal move and the reconciler
// will resume it; an operator who really wants a different operation id cancels the
// first one. A drain never blocks itself, so its own later passes are unaffected.
//
// This is a read followed by a write rather than a database constraint. The
// alternative — a unique partial index over live drain operations per host — would
// make it atomic, but it puts "live" in a migration instead of in the lifecycle
// table, and it surfaces as a constraint violation through an INSERT whose ON
// CONFLICT clause already belongs to operation_id, which the caller cannot tell from
// any other integrity error. The Control Plane is single-active and every write here
// is term-guarded, so the window this leaves is two goroutines inside one leader,
// not two leaders; if that ever becomes real, the index is the answer.
func (d *Drainer) exclusive(ctx context.Context, hostID, operationID string) error {
	ops, err := d.md.ListOperationsByHost(ctx, hostID)
	if err != nil {
		return err
	}
	for _, op := range ops {
		if op.Kind != lifecycle.OpDrain || op.OperationID == operationID || op.Phase.Terminal() {
			continue
		}
		return fmt.Errorf("%w: %s is already being drained by operation %s (%s)",
			ErrHostAlreadyDraining, hostID, op.OperationID, op.Phase)
	}
	return nil
}

// move evacuates one volume: place → reserve → bulk materialize → fence → final
// materialize → epoch boundary → release the source's reservation. Every step is
// preceded by a durable note of the intent, so a resumed pass continues from a fact
// rather than from a reconstruction. A volume this operation did not move is skipped
// rather than finished: writing its boundary or releasing its capacity would be
// speaking for a promotion it never performed.
//
// A zero Move (empty VolumeID) means the volume was skipped, not moved.
func (d *Drainer) move(ctx context.Context, term int64, operationID, source string, v metadata.Volume, prog *progress) (Move, error) {
	parsed, err := ids.Parse(v.VolumeID)
	if err != nil {
		return Move{}, fmt.Errorf("drain: volume id %q: %w", v.VolumeID, err)
	}
	vol := [16]byte(parsed)

	vp := prog.volume(v.VolumeID)
	entry := stage("") // the stage this pass started from; later ones are first attempts
	if vp != nil {
		entry = vp.Stage
	}

	if vp == nil {
		if v.PrimaryHostID != source {
			prog.foreign(v.VolumeID, v.PrimaryHostID)
			return Move{}, d.save(ctx, term, operationID, *prog)
		}
		dest, err := d.choose(ctx, v)
		if err != nil {
			return Move{}, err
		}
		dh, err := d.md.GetHost(ctx, dest)
		if err != nil {
			return Move{}, err
		}
		vp = prog.begin(v.VolumeID, dest, uint64(v.CurrentEpoch), dh.NVMeCommittedBytes)
		if err := d.save(ctx, term, operationID, *prog); err != nil {
			return Move{}, err
		}
	}

	if vp.Stage == stageReserving {
		if err := d.applyCapacity(ctx, term, vp.ToHost, v.SizeBytes, vp.DstBefore, entry == stageReserving); err != nil {
			// Nothing is reserved, so there is nothing to give back: forget the
			// volume and let a later pass place it afresh.
			prog.drop(v.VolumeID)
			return Move{}, errors.Join(err, d.save(ctx, term, operationID, *prog))
		}
		vp.Stage = stageMoving
		if err := d.save(ctx, term, operationID, *prog); err != nil {
			return Move{}, d.abandon(ctx, term, operationID, prog, v, err)
		}
	}

	if vp.Stage == stageMoving {
		if err := d.stillOurs(v, source, vp); err != nil {
			if v.PrimaryHostID == vp.ToHost {
				return Move{}, err // the reservation covers what is actually there
			}
			return Move{}, d.abandon(ctx, term, operationID, prog, v, err)
		}
		// Bulk pass: rebuild on the destination from S3 while the source still serves.
		// This is where the cold RTO is spent (§22.3), overlapping the fencing wait.
		if _, _, err := d.mat.FromEpoch(ctx, vol, vp.PrevEpoch); err != nil {
			return Move{}, d.abandon(ctx, term, operationID, prog, v, err)
		}

		// Fence the source before the destination can write: promotion waits out
		// lease_ttl + max_clock_skew and CASes the epoch object (§12.3–12.4).
		// No lease row means the host never held one (or it was already revoked): the
		// zero instant puts the fencing deadline far in the past, so promotion proceeds.
		var renewedAt time.Time
		if l, lerr := d.md.GetHostLease(ctx, v.PrimaryHostID); lerr == nil {
			renewedAt = l.LastRenewal
		} else if !errors.Is(lerr, metadata.ErrNotFound) {
			return Move{}, d.abandon(ctx, term, operationID, prog, v, lerr)
		}
		newEpoch, err := d.promoter.Promote(ctx, term, v.VolumeID, renewedAt, vp.ToHost)
		if err != nil {
			return Move{}, d.abandon(ctx, term, operationID, prog, v, err)
		}
		// From here the volume is the destination's: nothing below releases the
		// reservation, whatever goes wrong.
		if newEpoch != vp.PrevEpoch+1 {
			return Move{}, fmt.Errorf("%w: %s was promoted to epoch %d, this drain planned %d",
				ErrEpochAdvanced, v.VolumeID, newEpoch, vp.PrevEpoch+1)
		}
		vp.NewEpoch = newEpoch
		vp.Stage = stagePromoted
		if err := d.save(ctx, term, operationID, *prog); err != nil {
			return Move{}, err
		}
		// The volume was read before it was fenced; the steps below reason about
		// where it is *now*.
		if v, err = d.md.GetVolume(ctx, v.VolumeID); err != nil {
			return Move{}, err
		}
	}

	if vp.Stage == stagePromoted {
		// The boundary about to be written says "epoch NewEpoch begins where PrevEpoch
		// ended". That is only this operation's to say while the volume is still where
		// its own promotion left it.
		if v.PrimaryHostID != vp.ToHost || uint64(v.CurrentEpoch) != vp.NewEpoch {
			return Move{}, fmt.Errorf("%w: %s is on %s at epoch %d; this drain granted epoch %d to %s",
				ErrEpochAdvanced, v.VolumeID, v.PrimaryHostID, v.CurrentEpoch, vp.NewEpoch, vp.ToHost)
		}
		// Final pass: the old writer is fenced, so the old epoch's durable prefix can
		// no longer grow. Whatever it ACKed is inside what we materialize here (INV-09).
		_, mp, err := d.mat.FromEpoch(ctx, vol, vp.PrevEpoch)
		if err != nil {
			return Move{}, err
		}
		if err := d.guardDurableFloor(ctx, v, vol, vp.PrevEpoch, mp.UpTo); err != nil {
			return Move{}, err
		}
		if err := d.writeRecoveryPoint(ctx, vol, vp.NewEpoch, vp.PrevEpoch, mp.UpTo); err != nil {
			return Move{}, err
		}
		src, err := d.md.GetHost(ctx, source)
		if err != nil {
			return Move{}, err
		}
		vp.UpTo, vp.Bytes, vp.SrcBefore = mp.UpTo, mp.Bytes, src.NVMeCommittedBytes
		vp.Stage = stageReleasing
		if err := d.save(ctx, term, operationID, *prog); err != nil {
			return Move{}, err
		}
	}

	if vp.Stage == stageReleasing {
		if err := d.applyCapacity(ctx, term, source, -v.SizeBytes, vp.SrcBefore, entry == stageReleasing); err != nil {
			return Move{}, err
		}
		vp.Stage = stageDone
		if err := d.save(ctx, term, operationID, *prog); err != nil {
			return Move{}, err
		}
	}

	return Move{
		VolumeID: v.VolumeID, FromHost: source, ToHost: vp.ToHost,
		NewEpoch: vp.NewEpoch, UpTo: vp.UpTo, Bytes: vp.Bytes,
	}, nil
}

// choose picks the destination for a volume (§20 step 2 / §22.3: a warm standby is
// already hydrated, so the move is short).
func (d *Drainer) choose(ctx context.Context, v metadata.Volume) (string, error) {
	hosts, err := d.md.ListHosts(ctx)
	if err != nil {
		return "", err
	}
	req := placement.Request{SizeBytes: v.SizeBytes}
	if v.StandbyHostID != "" {
		req.CachedHostIDs = []string{v.StandbyHostID}
	}
	return d.policy.Choose(hosts, req)
}

// stillOurs reports whether a reserved-but-not-yet-promoted volume is still the one
// this operation planned to move: on the source at the epoch we recorded, or already
// on our destination at the next one (our own promotion, resumed).
func (d *Drainer) stillOurs(v metadata.Volume, source string, vp *volumeProgress) error {
	epoch := uint64(v.CurrentEpoch)
	if (epoch == vp.PrevEpoch && v.PrimaryHostID == source) ||
		(epoch == vp.PrevEpoch+1 && v.PrimaryHostID == vp.ToHost) {
		return nil
	}
	return fmt.Errorf("%w: %s is on %s at epoch %d; this drain reserved %s for epoch %d",
		ErrEpochAdvanced, v.VolumeID, v.PrimaryHostID, epoch, vp.ToHost, vp.PrevEpoch+1)
}

// abandon gives back the reservation held for a move that will not happen and
// forgets the volume, so a later pass places it afresh. A release that fails is
// joined into the reported error rather than swallowed: leadership can change
// between the reservation and the promotion, and a phantom reservation nobody
// reconciles makes placement under-use a host that is actually empty.
func (d *Drainer) abandon(ctx context.Context, term int64, operationID string, prog *progress, v metadata.Volume, cause error) error {
	errs := []error{cause}
	if vp := prog.volume(v.VolumeID); vp != nil {
		// A release is never bounded: the destination may be over its ceiling by now
		// (another placement landed there), and refusing to hand the bytes back would
		// leave the reservation stranded on exactly the host that can least afford it.
		if rerr := d.md.CommitHostCapacity(ctx, term, vp.ToHost,
			metadata.CapacityChange{DeltaBytes: -v.SizeBytes}); rerr != nil {
			errs = append(errs, fmt.Errorf("%w: %s still holds %d bytes for %s: %w",
				ErrReservationNotReleased, vp.ToHost, v.SizeBytes, v.VolumeID, rerr))
		}
	}
	prog.drop(v.VolumeID)
	if serr := d.save(ctx, term, operationID, *prog); serr != nil {
		errs = append(errs, serr)
	}
	return errors.Join(errs...)
}

// applyCapacity moves a host's committed bytes by delta exactly once across resumed
// passes, under the §28.2 bound.
//
// A first attempt is an unconditional change: there is nothing to be idempotent
// about yet, and what protects it is the bound — the placement decision behind it
// was taken against a fleet read that any number of other operations shared, so the
// statement that adds the bytes is the only place the ceiling still means anything.
//
// A resumed attempt is conditional. CommitHostCapacity is a delta, not an
// idempotency key, so the only proof this pass has that its own change already
// landed is the ledger value recorded before it was attempted: `before` means it did
// not, `before+delta` means it did. Comparing those in Go leaves a window in which a
// third party's change is indistinguishable from ours, so the comparison is a
// predicate of the write itself; a ledger that moved comes back as
// ErrCapacityConflict with nothing applied, and the drain reports it rather than
// guessing. Releasing twice either wedges the drain with ErrCapacityUnderflow or
// silently consumes another volume's reservation.
func (d *Drainer) applyCapacity(ctx context.Context, term int64, hostID string, delta, before int64, resumed bool) error {
	h, err := d.md.GetHost(ctx, hostID)
	if err != nil {
		return err
	}
	change := metadata.CapacityChange{DeltaBytes: delta, Limit: d.policy.Limit(h)}
	if resumed {
		change = change.Expecting(before)
	}
	err = d.md.CommitHostCapacity(ctx, term, hostID, change)
	if !errors.Is(err, metadata.ErrCapacityConflict) {
		return err
	}
	// The ledger is not where this pass left it. An earlier pass of this same
	// operation having applied the delta is the one reading that is still ours to
	// finish; anything else is a third party writing the same books.
	h, gerr := d.md.GetHost(ctx, hostID)
	if gerr != nil {
		return gerr
	}
	if resumed && h.NVMeCommittedBytes == before+delta {
		return nil
	}
	return fmt.Errorf("%w: %s holds %d committed bytes, expected %d before the change or %d after: %w",
		ErrCapacityLedgerMoved, hostID, h.NVMeCommittedBytes, before, before+delta, err)
}

// guardDurableFloor refuses a boundary that would move the durable point backwards.
// The floor is whatever we can establish without trusting a single source: the
// previous epoch's own boundary (§12.5) and PostgreSQL's informative watermark (§5.8
// — informative, so it can only raise the floor, never lower it).
func (d *Drainer) guardDurableFloor(ctx context.Context, v metadata.Volume, vol [16]byte, prevEpoch, upTo uint64) error {
	floor := uint64(0)
	if v.DurableSequence > 0 {
		floor = uint64(v.DurableSequence)
	}
	if rp, err := recovery.ReadRecoveryPoint(ctx, d.store, vol, prevEpoch); err == nil && rp.RecoveredUpTo > floor {
		floor = rp.RecoveredUpTo
	}
	if upTo < floor {
		return fmt.Errorf("%w: epoch %d would record %d, but the volume was durable through %d",
			ErrDurableRegression, prevEpoch+1, upTo, floor)
	}

	// A floor of zero proves nothing: PostgreSQL's watermark is lazy (§5.8) and the
	// first epoch has no predecessor boundary. So ask the object store directly —
	// "the epoch holds objects but none of them validated" is a different fact from
	// "the epoch is empty", and only the second one may record a boundary of zero.
	if upTo == 0 {
		infos, err := d.store.List(ctx, fmt.Sprintf("wal/%s/%d/", format.UUIDString(vol), prevEpoch))
		if err != nil {
			return err
		}
		for _, info := range infos {
			if strings.HasSuffix(info.Key, ".wal") {
				return fmt.Errorf("%w: epoch %d holds objects but none is readable, so its durable point cannot be established",
					ErrDurableRegression, prevEpoch)
			}
		}
	}
	return nil
}

// writeRecoveryPoint records the epoch boundary (§12.5). The object is create-only,
// so a move retried after a crash between promotion and this write finds its own
// boundary already there: an identical one is accepted (§18), a different one means
// two writers claimed the same epoch and is a hard error.
func (d *Drainer) writeRecoveryPoint(ctx context.Context, vol [16]byte, newEpoch, oldEpoch, upTo uint64) error {
	err := recovery.WriteRecoveryPoint(ctx, d.store, vol, newEpoch, oldEpoch, upTo)
	if !errors.Is(err, objectstore.ErrPreconditionFailed) {
		return err
	}
	existing, rerr := recovery.ReadRecoveryPoint(ctx, d.store, vol, newEpoch)
	if rerr != nil {
		return rerr
	}
	if existing.PrevEpoch != oldEpoch || existing.RecoveredUpTo != upTo {
		return fmt.Errorf("drain: epoch %d already has a different recovery point %+v (want prev=%d up_to=%d)",
			newEpoch, existing, oldEpoch, upTo)
	}
	return nil
}

func (d *Drainer) canceled(ctx context.Context, operationID string) (bool, error) {
	op, err := d.md.GetOperation(ctx, operationID)
	if err != nil {
		return false, err
	}
	return op.Phase == lifecycle.OpCanceling || op.Phase == lifecycle.OpCanceled, nil
}

// recordFailure stores a retryable failure with its progress, so the reconciler runs
// the operation again (FAILED -> RUNNING is a legal move, §7). It leaves a
// cancellation alone: the lifecycle does allow CANCELING -> FAILED, but taking that
// edge would erase the operator's request — a FAILED drain is simply retried, and
// nothing would ever read the cancellation again.
func (d *Drainer) recordFailure(ctx context.Context, term int64, operationID string, p progress, cause error) {
	if canceling, err := d.canceled(ctx, operationID); err != nil || canceling {
		return
	}
	_ = d.record(ctx, term, operationID, lifecycle.OpFailed, p, cause.Error())
}

// save records the operation's progress while it keeps running. It is the write that
// makes every step above resumable, so its failure is never ignored.
func (d *Drainer) save(ctx context.Context, term int64, operationID string, p progress) error {
	return d.record(ctx, term, operationID, lifecycle.OpRunning, p, "")
}

func (d *Drainer) record(ctx context.Context, term int64, operationID string, phase lifecycle.OperationPhase, p progress, opErr string) error {
	return d.md.UpdateOperation(ctx, term, metadata.Operation{
		OperationID: operationID, Phase: phase, CurrentState: mustJSON(p), Error: opErr,
	})
}

// mustJSON marshals progress-shaped values; these types always marshal.
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte(`{}`)
	}
	return b
}
