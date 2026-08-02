package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/spin-stack/storage/internal/checkpoint"
	"github.com/spin-stack/storage/internal/ioclass"
	"github.com/spin-stack/storage/internal/simio/clock"
)

// The durability scheduler: the thing that decides *when* to checkpoint and truncate
// (BUILD-INVENTORY increment 3, DURABILITY-SCHEDULER-SPEC.md).
//
// Nothing here decides *how*. checkpoint.Create verifies this host may publish into the
// epoch, takes the durable sequence from the object store's proof rather than the log's
// watermark, publishes create-only, and re-verifies the epoch before advancing published
// — because advancing published is what authorises discarding the last local copy.
// wal.TruncateLocal refuses anything above published (INV-13). This file only picks the
// moment, and the design already picked it: §21.1 and §10 say 256 MiB of WAL or two
// minutes, whichever comes first.
//
// Without it `published` stays 0 for the life of the process, not one byte is ever
// reclaimed, and the host's NVMe fills until writes stall — for this volume and for
// every co-tenant of the disk.

// Design defaults (§21.1 "Objectization", §10 configuration). They are named here rather
// than left as literals so a reader can see which numbers come from the doc.
const (
	defaultCheckpointBytes    = 256 << 20 // checkpoint_interval_bytes
	defaultCheckpointInterval = 2 * time.Minute
	// defaultCheckpointPoll is *not* from the design. It is how often the scheduler
	// looks at the byte trigger, and it exists because the alternative — checking on
	// every append — puts the decision in the guest's write path, which is the one
	// place §5.3 keeps clear.
	defaultCheckpointPoll = 15 * time.Second
	// backgroundCost is what one checkpoint asks of the background budget. A
	// checkpoint is a LIST plus a PUT; the unit is arbitrary and the budget is
	// configured in the same unit, so what matters is that it is not free.
	backgroundCost = 1
)

// checkpointLoop runs one volume's durability scheduler until ctx is done. It is started
// by start() and stopped with the runtime, like the serve loop beside it.
func (m *VolumeManager) checkpointLoop(ctx context.Context, v *Volume, volumeID [16]byte, epoch uint64) {
	poll := m.cfg.CheckpointPoll
	if poll <= 0 {
		poll = defaultCheckpointPoll
	}
	last := m.deps.Clock.Now()

	for {
		if err := m.deps.Clock.Sleep(ctx, poll); err != nil {
			return // ctx done
		}
		due, why := m.checkpointDue(v, last)
		if !due {
			continue
		}
		if m.checkpointOnce(ctx, v, volumeID, epoch, why) {
			last = m.deps.Clock.Now()
		}
	}
}

// checkpointDue reports whether either trigger has fired, and which — the reason travels
// to the log line, because "a checkpoint happened" without "because the WAL hit 256 MiB"
// is not something anyone can tune from.
func (m *VolumeManager) checkpointDue(v *Volume, last clock.Instant) (bool, string) {
	interval := m.cfg.CheckpointInterval
	if interval <= 0 {
		interval = defaultCheckpointInterval
	}
	if m.deps.Clock.Now().Sub(last) >= interval {
		return true, "interval"
	}

	limit := m.cfg.CheckpointBytes
	if limit <= 0 {
		limit = defaultCheckpointBytes
	}
	local, err := v.log.LocalBytes()
	if err != nil {
		// Not fatal and not silent: the byte trigger is blind until the disk answers,
		// and the interval trigger still fires.
		slog.Warn("cannot measure the local WAL; the checkpoint byte trigger is blind",
			"volume_id", v.id, "error", err)
		return false, ""
	}
	return local >= limit, "bytes"
}

// checkpointOnce publishes a checkpoint and reclaims what it covers. It reports whether
// the checkpoint was actually taken, so a skipped cycle does not reset the interval.
func (m *VolumeManager) checkpointOnce(ctx context.Context, v *Volume, volumeID [16]byte, epoch uint64, why string) bool {
	// §12.6: a SELF_FENCED Agent "deja de publicar checkpoints/manifests". The lease is
	// checked here, on top of the epoch verification inside Create, because the two fail
	// differently — the epoch object is a network read that can be served stale, the
	// lease is local and monotonic — and the cheap one is the one that would otherwise
	// not be made.
	if m.deps.Lease != nil && !m.deps.Lease() {
		return false
	}

	// INV-17: background yields. A checkpoint is a LIST and a PUT against the same
	// object store the guest's FLUSH path uses, and the guest wins. Denied means try
	// again next poll, not queue behind the guest.
	if m.deps.IOClass != nil && !m.deps.IOClass.TryAcquire(ioclass.Background, backgroundCost) {
		return false
	}

	started := m.deps.Clock.Now()
	cp, err := m.checkpointer().Create(ctx, v.log, volumeID, epoch)
	if err != nil {
		m.handleCheckpointError(ctx, v, err)
		return false
	}

	// Truncate to *published*, never to durable. They differ by exactly the window in
	// which the object store has not confirmed, and TruncateLocal would refuse the
	// higher number (INV-13) — but the refusal is the backstop, not the reason.
	published := v.log.Watermarks().Published
	if err := v.log.TruncateLocal(published); err != nil {
		slog.Error("the checkpoint published but the local WAL could not be reclaimed",
			"volume_id", v.id, "epoch", v.epoch, "published", published, "error", err)
		return true // the checkpoint itself succeeded; the space comes back next time
	}

	slog.Info("checkpoint published and local WAL reclaimed",
		"volume_id", v.id, "epoch", v.epoch, "trigger", why,
		"durable_sequence", cp.DurableSequence,
		"reclaimed_bytes", v.log.ReclaimedBytes(),
		"took", m.deps.Clock.Now().Sub(started))
	return true
}

// handleCheckpointError decides whether a failed checkpoint is something to retry or
// something to stop serving over.
func (m *VolumeManager) handleCheckpointError(ctx context.Context, v *Volume, err error) {
	// ADR-0023: the object store has just proved that another writer is in this epoch.
	// It is a better witness of that than a lease this host still holds or a heartbeat
	// that has not failed yet — it is holding an object this host did not write. The
	// volume stops serving by the same path the Control Plane's refusal takes, and the
	// epoch is recorded so Apply does not restart it at the epoch it just lost.
	if errors.Is(err, checkpoint.ErrDurablePointMismatch) || errors.Is(err, checkpoint.ErrCheckpointConflict) {
		slog.Error("another writer is in this epoch; the object store said so",
			"volume_id", v.id, "epoch", v.epoch, "witness", "object-store", "error", err)
		if ferr := m.Fence(ctx, []string{v.id}); ferr != nil {
			slog.Error("tearing the runtime down after an object-store fencing witness",
				"volume_id", v.id, "error", ferr)
		}
		return
	}

	// Everything else — an unreachable store, a transient refusal — is retried on the
	// next poll. The remote gap grows meanwhile, which is the RPO an operator reads, and
	// wal.Limits.MaxRemoteGapBytes is what bounds it.
	slog.Warn("checkpoint failed; retrying on the next cycle",
		"volume_id", v.id, "epoch", v.epoch, "error", err)
}

// checkpointer builds the Checkpointer this Agent publishes as. HeldBy is what stops a
// second host publishing into an epoch it merely knows the number of (§12.3–12.4).
func (m *VolumeManager) checkpointer() *checkpoint.Checkpointer {
	return checkpoint.NewCheckpointer(m.deps.Store).HeldBy(m.cfg.HostID)
}

// checkpointsEnabled reports whether this Agent can run a durability scheduler at all. A
// local-only Agent has no object store to publish into, so `published` stays where it is
// and nothing is ever reclaimed — which is the honest behaviour, not a degraded one.
func (m *VolumeManager) checkpointsEnabled() error {
	switch {
	case m.deps.Store == nil:
		return errors.New("no object store")
	case m.cfg.HostID == "":
		return fmt.Errorf("no host id: %w", errNoPublisherIdentity)
	}
	return nil
}

// errNoPublisherIdentity is what a checkpoint with no host id would become: an
// unattributable publication into an epoch, which §12.4's ownership check cannot verify.
var errNoPublisherIdentity = errors.New("a checkpoint must name the host publishing it")
