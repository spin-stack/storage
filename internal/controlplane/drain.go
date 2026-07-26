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
	// promotion put it: somebody else moved it on. The epoch boundary the drain
	// still owes that volume is now unauthorable, because it is a statement about a
	// promotion this operation can no longer prove it performed.
	ErrEpochAdvanced = errors.New("controlplane: the volume advanced past the epoch this drain granted")

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
	// stageMoving: the destination is chosen and the entry that records it is the
	// destination's reservation (ADR-0017); materialization and promotion may run.
	stageMoving stage = "MOVING"
	// stagePromoted: *this operation* fenced the source and was granted NewEpoch on
	// ToHost. Only a volume in this stage may have its epoch boundary written here.
	stagePromoted stage = "PROMOTED"
	// stageDone: the move is complete.
	stageDone stage = "DONE"
	// stageForeign: the volume left the source under somebody else's promotion. It is
	// evacuated, but not by this operation, which therefore writes no boundary for it.
	stageForeign stage = "FOREIGN"
)

// The stage vocabulary is read by two things besides this file: metadata's
// PlanReservation (which decides that DONE and FOREIGN reserve nothing) and the
// host_committed_bytes view, which says the same in SQL. Adding or renaming a stage
// means editing all three, deliberately.

// volumeProgress is what this operation has established about one volume. It is
// also the destination's reservation: an entry naming ToHost charges that host for
// the volume until the volume is actually primary there (ADR-0017), which is why
// there are no ledger fields left to record — there is no ledger.
type volumeProgress struct {
	VolumeID  string `json:"volume_id"`
	Stage     stage  `json:"stage"`
	ToHost    string `json:"to_host,omitempty"`
	PrevEpoch uint64 `json:"prev_epoch,omitempty"`
	NewEpoch  uint64 `json:"new_epoch,omitempty"`
	UpTo      uint64 `json:"recovered_up_to,omitempty"`
	Bytes     int64  `json:"bytes,omitempty"`
	Note      string `json:"note,omitempty"`
}

// progress is the operation's current_state (§28.1: progress must be visible).
type progress struct {
	Total   int              `json:"total"`
	Current string           `json:"current_volume,omitempty"`
	Volumes []volumeProgress `json:"volumes,omitempty"`
	// SrcLease is the latest renewal of the source host's lease this operation ever
	// observed, recorded *before* the lease was revoked. Once the row is gone nothing
	// can read that instant again, and a promotion with no instant to measure from
	// refuses outright rather than guess (ErrSourceLeaseUnknown, §12.3), so every
	// later pass measures the same fencing wait from what this one saw.
	SrcLease time.Time `json:"src_lease_renewed_at,omitzero"`
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

// begin records the intent to move volumeID to dest. Writing this entry *is* the
// reservation: from the moment it is durable, the derived capacity of dest includes
// the volume (ADR-0017).
func (p *progress) begin(volumeID, dest string, prevEpoch uint64) *volumeProgress {
	p.Volumes = append(p.Volumes, volumeProgress{
		VolumeID: volumeID, Stage: stageMoving, ToHost: dest, PrevEpoch: prevEpoch,
	})
	return &p.Volumes[len(p.Volumes)-1]
}

// foreign records that the volume left the source under another actor's promotion.
func (p *progress) foreign(volumeID, owner string) {
	p.Volumes = append(p.Volumes, volumeProgress{
		VolumeID: volumeID, Stage: stageForeign, ToHost: owner,
		Note: "promoted by another actor; this operation wrote no boundary for it",
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
	return d.md.UpdateOperation(ctx, term, op, nil)
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
		vp = prog.begin(v.VolumeID, dest, uint64(v.CurrentEpoch))
		// This write *is* the reservation (ADR-0017): once the entry is durable the
		// destination's derived capacity includes the volume, so the §28.2 bound
		// travels with it. A destination another placement filled between
		// d.choose and here refuses the write and nothing is reserved — there is no
		// second statement that could have landed, so there is nothing to undo.
		if err := d.reserve(ctx, term, operationID, *prog, dest, v.SizeBytes, dh); err != nil {
			prog.drop(v.VolumeID)
			return Move{}, errors.Join(err, d.save(ctx, term, operationID, *prog))
		}
	}

	if vp.Stage == stageMoving {
		if err := d.stillOurs(v, source, vp); err != nil {
			switch {
			case v.PrimaryHostID == vp.ToHost:
				return Move{}, err // the reservation covers what is actually there
			case v.PrimaryHostID != source:
				// It left the source under somebody else's promotion while this
				// operation was still holding a reservation for it. It is evacuated,
				// just not by us: record that, which also releases the reservation
				// (ADR-0017: the entry was the reservation), and move on. Failing the
				// pass instead would make the drain of a host somebody else is also
				// repairing terminate on every attempt.
				prog.drop(v.VolumeID)
				prog.foreign(v.VolumeID, v.PrimaryHostID)
				return Move{}, d.save(ctx, term, operationID, *prog)
			default:
				return Move{}, d.abandon(ctx, term, operationID, prog, v, err)
			}
		}
		// Bulk pass: rebuild on the destination from S3 while the source still serves.
		// This is where the cold RTO is spent (§22.3), overlapping the fencing wait.
		if _, _, err := d.mat.FromEpoch(ctx, vol, vp.PrevEpoch); err != nil {
			return Move{}, d.abandon(ctx, term, operationID, prog, v, err)
		}

		newEpoch, err := d.fenceAndPromote(ctx, term, operationID, v, vp, prog)
		if err != nil {
			if errors.Is(err, ErrFencingWaitNotElapsed) {
				// The fence is running, not failed. The entry stays: it is the
				// destination's reservation, and handing it back on every pass would
				// let another placement take the room this move is waiting for, so
				// the drain would come back to a host that no longer fits it.
				return Move{}, err
			}
			return Move{}, d.abandon(ctx, term, operationID, prog, v, err)
		}
		// From here the volume is the destination's, and the derived accounting says
		// so on its own: the source stops being charged the moment it stops being
		// primary, whatever goes wrong below.
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
		if err := d.writeRecoveryPoint(ctx, vol, vp.NewEpoch, vp.PrevEpoch, mp.UpTo, vp.ToHost); err != nil {
			return Move{}, err
		}
		vp.UpTo, vp.Bytes = mp.UpTo, mp.Bytes
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

// fenceAndPromote fences the source and grants the volume's next epoch to the
// destination (§12.3–12.5). The revocation is skipped once the volume is already on
// the destination — this operation's own promotion, being resumed — because the
// lease it would take away then is the one promotion granted to the *new* writer.
func (d *Drainer) fenceAndPromote(ctx context.Context, term int64, operationID string,
	v metadata.Volume, vp *volumeProgress, prog *progress,
) (uint64, error) {
	if v.PrimaryHostID != vp.ToHost {
		if err := d.fenceSource(ctx, term, operationID, v.PrimaryHostID, prog); err != nil {
			return 0, err
		}
	}
	return d.promoter.Promote(ctx, term, v.VolumeID, prog.SrcLease, vp.ToHost)
}

// fenceSource is the Control Plane withdrawing its own record of hostID as a writer:
// it notes when the host's lease was last renewed and then takes the lease away
// (term-guarded). Both steps, in that order, before the promotion waits the fence
// out (§12.3).
//
// The revocation is what makes a *healthy* host evacuable. The drain refuses to
// promote a source whose lease is live, so on a host that is up and heartbeating the
// deadline keeps moving forward and the evacuation never starts. Deleting the row is
// the CP saying it will not count that host as a writer again.
//
// It shortens nothing. The Agent counts its own copy of the lease down on a
// monotonic clock (§12.2) and never learns the row is gone, so the promotion still
// waits out last_renewal + lease_ttl + max_clock_skew — which is why the instant is
// recorded, and saved, *before* the row that carries it is deleted. A later pass
// finds no lease and measures from what this one saw; a host that renewed again in
// the meantime moves that instant forward, never back.
//
// No lease row and nothing recorded means the host never held one. That is "I know
// nothing", not "it expired long ago", and the promoter refuses on it (§12.3) unless
// the fleet has recorded the host DEAD — the drain does not paper over it here.
func (d *Drainer) fenceSource(ctx context.Context, term int64, operationID, hostID string, prog *progress) error {
	l, err := d.md.GetHostLease(ctx, hostID)
	switch {
	case err == nil:
		if prog.SrcLease.Before(l.LastRenewal) {
			prog.SrcLease = l.LastRenewal
			if serr := d.save(ctx, term, operationID, *prog); serr != nil {
				return serr
			}
		}
	case errors.Is(err, metadata.ErrNotFound):
		return nil // already revoked by an earlier pass, or never held
	default:
		return err
	}
	return d.md.RevokeHostLease(ctx, term, hostID)
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

// abandon forgets a volume whose move will not happen, so a later pass places it
// afresh.
//
// Under ADR-0017 that single write is also the release: the reservation *was* the
// entry, so dropping it gives the destination's capacity back with no second
// statement that could fail on its own. What used to live here — a bounded release,
// a joined ErrReservationNotReleased, a phantom reservation nobody reconciles when
// leadership changed between the two writes — was the cost of carrying the number.
func (d *Drainer) abandon(ctx context.Context, term int64, operationID string, prog *progress, v metadata.Volume, cause error) error {
	prog.drop(v.VolumeID)
	if serr := d.save(ctx, term, operationID, *prog); serr != nil {
		return errors.Join(cause, serr)
	}
	return cause
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
// It is written as the destination host, the one Promote just granted newEpoch to:
// the boundary is the floor everything published in the new epoch is measured against,
// so an author that does not hold the epoch has no business recording it (§12.5).
func (d *Drainer) writeRecoveryPoint(ctx context.Context, vol [16]byte, newEpoch, oldEpoch, upTo uint64, hostID string) error {
	rp := recovery.RecoveryPoint{PrevEpoch: oldEpoch, RecoveredUpTo: upTo}
	err := rp.WriteAs(ctx, d.store, vol, newEpoch, hostID)
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
	}, nil)
}

// reserve is the progress write that first names a destination, carrying the §28.2
// bound the placement decision was taken under (ADR-0017). Passing the policy's own
// answer keeps one copy of the rule: placement.Choose is pure and advisory, so the
// statement that records the reservation is the only place the ceiling still means
// anything.
func (d *Drainer) reserve(ctx context.Context, term int64, operationID string, p progress,
	dest string, sizeBytes int64, h metadata.Host,
) error {
	return d.md.UpdateOperation(ctx, term, metadata.Operation{
		OperationID: operationID, Phase: lifecycle.OpRunning, CurrentState: mustJSON(p),
	}, &metadata.CapacityBound{HostID: dest, AddBytes: sizeBytes, Limit: d.policy.Limit(h)})
}

// mustJSON marshals progress-shaped values; these types always marshal.
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte(`{}`)
	}
	return b
}
