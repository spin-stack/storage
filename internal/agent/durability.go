package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/spin-stack/storage/internal/checkpoint"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/ioclass"
	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/wal"
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
		// §14.8 rule 3, and it must come before the checkpoint rather than after: a
		// `local` volume's FLUSH ACKs on fdatasync and returns before any PUT, so with
		// nothing draining, durable_sequence stays at 0, checkpoint.Create has no
		// durable point to publish, TruncateLocal(published) reclaims nothing, and the
		// WAL grows for the life of the volume. That is the state increment 3 removed
		// for `remote` volumes, and it applied to every `local` one the moment the Agent
		// started honouring the mode the Control Plane sets.
		//
		// A no-op for remote volumes: their durable step already uploaded.
		if v.log.Mode() == wal.ModeLocal {
			m.drainOnce(ctx, v)
		}

		due, why := m.checkpointDue(v, last)
		if !due {
			continue
		}
		if ran, _ := m.checkpointOnce(ctx, v, volumeID, epoch, why); ran {
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

// Checkpoint publishes a checkpoint for one volume now and reclaims what it covers,
// instead of waiting for a trigger. It is what a drain calls before moving a volume, and
// what a test calls instead of driving the clock.
//
// It goes through exactly the same gates as the scheduler — lease, io-class, the same
// failure classification — because a second path to publication is a second place for the
// §12.6 lease rule to be forgotten.
func (m *VolumeManager) Checkpoint(ctx context.Context, volumeID string) error {
	m.mu.Lock()
	v, ok := m.volumes[volumeID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("agent: volume %s is not being served here", volumeID)
	}
	if err := m.checkpointsEnabled(); err != nil {
		return fmt.Errorf("agent: volume %s cannot checkpoint: %w", volumeID, err)
	}
	u, err := ids.Parse(volumeID)
	if err != nil {
		return err
	}
	ran, err := m.checkpointOnce(ctx, v, [16]byte(u), uint64(v.epoch), "explicit")
	if err != nil {
		return fmt.Errorf("agent: the checkpoint for volume %s failed: %w", volumeID, err)
	}
	if !ran {
		// Declined rather than failed. Which of the three it was matters to whoever
		// called: a pending base clears on its own, a lapsed lease is fencing, and a
		// denied budget means the guest is busy.
		switch {
		case v.log.BasePending():
			return fmt.Errorf("agent: volume %s is still recovering its read view", volumeID)
		case m.deps.Lease != nil && !m.deps.Lease():
			return fmt.Errorf("agent: volume %s has no valid lease to publish under (§12.6)", volumeID)
		default:
			return fmt.Errorf("agent: volume %s yielded to the guest (INV-17)", volumeID)
		}
	}
	return nil
}

// checkpointOnce publishes a checkpoint and reclaims what it covers. It reports whether
// the checkpoint was actually taken, so a skipped cycle does not reset the interval.
func (m *VolumeManager) checkpointOnce(ctx context.Context, v *Volume, volumeID [16]byte, epoch uint64, why string) (bool, error) {
	// §12.6: a SELF_FENCED Agent "deja de publicar checkpoints/manifests". The lease is
	// checked here, on top of the epoch verification inside Create, because the two fail
	// differently — the epoch object is a network read that can be served stale, the
	// lease is local and monotonic — and the cheap one is the one that would otherwise
	// not be made.
	if m.deps.Lease != nil && !m.deps.Lease() {
		return false, nil
	}

	// A resumed log reports durable = 0 until its base arrives (see wal.BasePending).
	// Checkpointing in that window compares the store's real durable point against 0
	// and raises ErrDurablePointMismatch — which ADR-0023 reads as "another writer is
	// in this epoch" and acts on by fencing. That would fence a healthy host out of its
	// own volume on every restart, and it is exactly what the DST arm caught.
	if v.log.BasePending() {
		return false, nil
	}

	// INV-17: background yields. A checkpoint is a LIST and a PUT against the same
	// object store the guest's FLUSH path uses, and the guest wins. Denied means try
	// again next poll, not queue behind the guest.
	if m.deps.IOClass != nil && !m.deps.IOClass.TryAcquire(ioclass.Background, backgroundCost) {
		return false, nil
	}

	started := m.deps.Clock.Now()
	cp, err := m.checkpointer().Create(ctx, v.log, volumeID, epoch)
	if err != nil {
		m.handleCheckpointError(ctx, v, err)
		return false, err
	}

	// Truncate to *published*, never to durable. They differ by exactly the window in
	// which the object store has not confirmed, and TruncateLocal would refuse the
	// higher number (INV-13) — but the refusal is the backstop, not the reason.
	published := v.log.Watermarks().Published
	if err := v.log.TruncateLocal(published); err != nil {
		slog.Error("the checkpoint published but the local WAL could not be reclaimed",
			"volume_id", v.id, "epoch", v.epoch, "published", published, "error", err)
		return true, nil // the checkpoint itself succeeded; the space comes back next time
	}

	slog.Info("checkpoint published and local WAL reclaimed",
		"volume_id", v.id, "epoch", v.epoch, "trigger", why,
		"durable_sequence", cp.DurableSequence,
		"reclaimed_bytes", v.log.ReclaimedBytes(),
		"took", m.deps.Clock.Now().Sub(started))
	return true, nil
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

// drainOnce is the asynchronous half of §14.8's `local` mode: the records a FLUSH already
// ACKed on fdatasync alone are put in the object store, and durable_sequence is advanced
// to cover them.
//
// The two halves are gated differently, and that split is the decision this function
// exists to encode.
//
// **Uploading is not gated.** An object is create-only under a key derived from
// (volume, epoch, sequences, content hash), and INV-21 hard-fails a divergent PUT — so
// writing one asserts nothing about who owns the volume and takes nothing from a
// successor. A host that has lost its lease still holds the only copy of these records,
// and refusing to upload them would turn a fencing event into data loss.
//
// **Advancing durable_sequence is gated**, on the same monotonic lease check §14.4 step 5
// applies. durable_sequence is the claim: INV-03 orders it, INV-13 truncates against it,
// §12.6 governs what may be published under it, and a promoted successor reads it. §14.8
// frees the *FLUSH ACK* from the lease in local mode; it does not free the watermark, and
// reading it that way would let a fenced host move a number its replacement trusts.
//
// So a fenced local volume ends up with its records safe in the bucket and its watermark
// standing still — which is exactly the state a successor wants to find.
//
// Errors are logged rather than returned: the caller is the scheduler loop, the next poll
// retries, and a drain that failed has not made anything untrue. A drain that *cannot*
// happen at all (no uploader) is not an error either — that is a local-only Agent with no
// object store, where the local WAL is all there is by design.
func (m *VolumeManager) drainOnce(ctx context.Context, v *Volume) {
	covered, err := v.log.DrainPending(ctx)
	switch {
	case errors.Is(err, wal.ErrNoUploader), errors.Is(err, wal.ErrSelfFenced):
		return // no object store, or already fenced: neither is this loop's problem
	case err != nil:
		slog.Warn("the local-mode drain could not reach the object store; this volume's RPO is growing",
			"volume_id", v.id, "remote_gap_bytes", v.log.RemoteGapBytes(), "error", err)
		return
	case covered == 0:
		return // nothing was pending
	}

	// The claim, and the only half the lease governs (see above).
	if m.deps.Lease != nil && !m.deps.Lease() {
		slog.Warn("drained to the object store but not advancing durable: this host's lease is not valid (§12.2, INV-06)",
			"volume_id", v.id, "covered_sequence", covered)
		return
	}
	if err := v.log.AdvanceDurable(covered); err != nil {
		slog.Error("the drained records could not be marked durable",
			"volume_id", v.id, "covered_sequence", covered, "error", err)
		return
	}
	slog.Info("local-mode WAL drained to the object store",
		"volume_id", v.id, "durable_sequence", covered, "remote_gap_bytes", v.log.RemoteGapBytes())
}

// Drain performs §14.8 rule 3's asynchronous upload for one volume now, instead of
// waiting for the scheduler's next poll. It is the counterpart of Checkpoint: the same
// work the loop does, reachable by a caller that has a reason not to wait — a drain
// moving the volume, or a test that would otherwise have to drive the clock.
//
// It goes through drainOnce and therefore through the same lease split, because a second
// path to advancing durable_sequence is a second place for §12.2 to be forgotten.
func (m *VolumeManager) Drain(ctx context.Context, volumeID string) error {
	m.mu.Lock()
	v, ok := m.volumes[volumeID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("agent: volume %s is not being served here", volumeID)
	}
	m.drainOnce(ctx, v)
	return nil
}

// WatermarksOf reports one served volume's watermarks, or the zero value if this host is
// not serving it.
func (m *VolumeManager) WatermarksOf(volumeID string) wal.Watermarks {
	m.mu.Lock()
	v, ok := m.volumes[volumeID]
	m.mu.Unlock()
	if !ok {
		return wal.Watermarks{}
	}
	return v.log.Watermarks()
}
