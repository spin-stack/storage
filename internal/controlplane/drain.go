package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/materialize"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/placement"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
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

// progress is the operation's current_state (§28.1: progress must be visible).
type progress struct {
	Total   int    `json:"total"`
	Moved   int    `json:"moved"`
	Current string `json:"current_volume,omitempty"`
}

// Drainer evacuates every volume off a host (§28.1). It is the composition of the
// pieces that already carry their own invariants: placement (§20/§28.2),
// materialization from S3 (§22.3), and the promotion protocol (§12.3–12.5). It adds
// no new durability rule — in particular it never moves a volume without fencing
// its previous writer first.
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

// Drain evacuates hostID. It cordons the host, then moves every volume whose
// primary it still is. The pass is idempotent and resumable: a volume that already
// moved is no longer listed under the source, so re-running the same operation
// finishes the remainder without touching what is done (§7, §18).
//
// Errors are expected and retryable — ErrFencingWaitNotElapsed until the source's
// lease can no longer ACK durability, placement.ErrNoCapacity when the fleet has no
// room, materialize.ErrThrottled while the data path is busy. The operation keeps
// its progress and the reconciler calls Drain again.
func (d *Drainer) Drain(ctx context.Context, term int64, hostID, operationID string) (DrainResult, error) {
	// Cordon first: even if this pass aborts immediately, nothing new lands here.
	if err := d.md.SetHostState(ctx, term, hostID, lifecycle.HostCordoned); err != nil {
		return DrainResult{}, err
	}
	if err := d.md.SetHostState(ctx, term, hostID, lifecycle.HostDraining); err != nil {
		return DrainResult{}, err
	}

	vols, err := d.md.ListVolumesByHost(ctx, hostID)
	if err != nil {
		return DrainResult{}, err
	}
	prog := progress{Total: len(vols)}
	recorded, err := d.md.RecordOperation(ctx, term, metadata.Operation{
		OperationID: operationID, Kind: lifecycle.OpDrain, HostID: hostID,
		DesiredState: mustJSON(map[string]string{"drain_host": hostID}),
		CurrentState: mustJSON(prog), Phase: lifecycle.OpPending,
	})
	if err != nil {
		return DrainResult{}, err
	}
	_ = recorded // a duplicate request resumes the recorded operation (§18)

	res := DrainResult{Phase: lifecycle.OpRunning, Remaining: len(vols)}

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
	if err := d.record(ctx, term, operationID, lifecycle.OpRunning, prog, ""); err != nil {
		return res, err
	}

	for _, v := range vols {
		canceled, err := d.canceled(ctx, operationID)
		if err != nil {
			return res, err
		}
		if canceled {
			res.Phase = lifecycle.OpCanceled
			return res, d.record(ctx, term, operationID, lifecycle.OpCanceled, prog, "")
		}

		prog.Current = v.VolumeID
		if err := d.record(ctx, term, operationID, lifecycle.OpRunning, prog, ""); err != nil {
			return res, err
		}
		mv, err := d.move(ctx, term, v)
		if err != nil {
			// Retryable: the operation stays FAILED with its progress, and the
			// reconciler runs it again (FAILED -> RUNNING is a legal move, §7).
			_ = d.record(ctx, term, operationID, lifecycle.OpFailed, prog, err.Error())
			return res, err
		}
		res.Moved = append(res.Moved, mv)
		res.Remaining--
		prog.Moved++
		prog.Current = ""
		if err := d.record(ctx, term, operationID, lifecycle.OpRunning, prog, ""); err != nil {
			return res, err
		}
	}

	res.Phase = lifecycle.OpSucceeded
	// The host stays DRAINING: returning it to ACTIVE is an operator decision.
	return res, d.record(ctx, term, operationID, lifecycle.OpSucceeded, prog, "")
}

// move evacuates one volume: place → reserve → bulk materialize → fence → final
// materialize → recovery point → release the source's reservation.
func (d *Drainer) move(ctx context.Context, term int64, v metadata.Volume) (Move, error) {
	parsed, err := ids.Parse(v.VolumeID)
	if err != nil {
		return Move{}, fmt.Errorf("drain: volume id %q: %w", v.VolumeID, err)
	}
	vol := [16]byte(parsed)
	hosts, err := d.md.ListHosts(ctx)
	if err != nil {
		return Move{}, err
	}
	req := placement.Request{SizeBytes: v.SizeBytes}
	if v.StandbyHostID != "" {
		req.CachedHostIDs = []string{v.StandbyHostID} // the warm standby is already hydrated (§22.3)
	}
	dest, err := d.policy.Choose(hosts, req)
	if err != nil {
		return Move{}, err
	}

	if err := d.md.CommitHostCapacity(ctx, term, dest, v.SizeBytes); err != nil {
		return Move{}, err
	}
	release := func() { _ = d.md.CommitHostCapacity(ctx, term, dest, -v.SizeBytes) }

	oldEpoch := uint64(v.CurrentEpoch)
	// Bulk pass: rebuild on the destination from S3 while the source still serves.
	// This is where the cold RTO is spent (§22.3), overlapping the fencing wait.
	if _, _, err := d.mat.FromEpoch(ctx, vol, oldEpoch); err != nil {
		release()
		return Move{}, err
	}

	// Fence the source before the destination can write: promotion waits out
	// lease_ttl + max_clock_skew and CASes the epoch object (§12.3–12.4).
	// No lease row means the host never held one (or it was already revoked): the
	// zero instant puts the fencing deadline far in the past, so promotion proceeds.
	var renewedAt time.Time
	if l, lerr := d.md.GetHostLease(ctx, v.PrimaryHostID); lerr == nil {
		renewedAt = l.LastRenewal
	} else if !errors.Is(lerr, metadata.ErrNotFound) {
		release()
		return Move{}, lerr
	}
	newEpoch, err := d.promoter.Promote(ctx, term, v.VolumeID, renewedAt, dest)
	if err != nil {
		release()
		return Move{}, err
	}

	// Final pass: the old writer is fenced, so the old epoch's durable prefix can no
	// longer grow. Whatever it ACKed is inside what we materialize here (INV-09).
	_, prog, err := d.mat.FromEpoch(ctx, vol, oldEpoch)
	if err != nil {
		return Move{}, err // the volume is already the destination's; do not release
	}
	if err := d.writeRecoveryPoint(ctx, vol, newEpoch, oldEpoch, prog.UpTo); err != nil {
		return Move{}, err
	}
	if err := d.md.CommitHostCapacity(ctx, term, v.PrimaryHostID, -v.SizeBytes); err != nil {
		return Move{}, err
	}
	return Move{
		VolumeID: v.VolumeID, FromHost: v.PrimaryHostID, ToHost: dest,
		NewEpoch: newEpoch, UpTo: prog.UpTo, Bytes: prog.Bytes,
	}, nil
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
