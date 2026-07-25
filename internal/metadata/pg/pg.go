// Package pg is the production metadata.Store: a thin adapter over the sqlc-
// generated queries on pgx/v5 (ADR-0006). Identity columns are uuid (ADR-0007); the
// adapter parses string ids at the boundary. It is verified by TestContainers
// integration tests; the fencing protocol itself is proven in metadata/sim.
package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/spin-stack/storage/internal/db"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
)

// Store adapts the generated db.Queries to the metadata.Store interface.
type Store struct {
	q *db.Queries
}

// New returns a Store over any pgx DBTX (pool, conn, or tx).
func New(conn db.DBTX) *Store { return &Store{q: db.New(conn)} }

var _ metadata.Store = (*Store)(nil)

// --- id / null helpers (string boundary <-> uuid columns, ADR-0007) ---

// requireUUID parses an identifier that must be present. Empty and malformed are
// both ErrInvalidID: a row keyed on a value the caller did not mean is a row nobody
// finds again.
func requireUUID(kind, s string) (uuid.UUID, error) {
	if s == "" {
		return uuid.UUID{}, fmt.Errorf("%w: empty %s id", metadata.ErrInvalidID, kind)
	}
	u, err := uuid.Parse(s)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("%w: %s id %q: %v", metadata.ErrInvalidID, kind, s, err)
	}
	return u, nil
}

// nullUUID parses an *optional* identifier. Empty means SQL NULL, which is
// meaningful (no owner, no parent). Malformed is an error and never NULL: coercing
// a truncated host id to NULL writes a volume with primary_host_id NULL, which
// ListVolumesByHost never returns — so a drain of that host reports success without
// evacuating it — and which promotion's resume branch can never match, so every
// retry burns another epoch.
func nullUUID(kind, s string) (pgtype.UUID, error) {
	if s == "" {
		return pgtype.UUID{}, nil
	}
	u, err := uuid.Parse(s)
	if err != nil {
		return pgtype.UUID{}, fmt.Errorf("%w: %s id %q: %v", metadata.ErrInvalidID, kind, s, err)
	}
	return pgtype.UUID{Bytes: u, Valid: true}, nil
}

func fromNullUUID(u pgtype.UUID) string {
	if u.Valid {
		return uuid.UUID(u.Bytes).String()
	}
	return ""
}

func text(s string) pgtype.Text { return pgtype.Text{String: s, Valid: s != ""} }
func fromText(t pgtype.Text) string {
	if t.Valid {
		return t.String
	}
	return ""
}
func fromTS(t pgtype.Timestamptz) time.Time { return t.Time }

// wrote reports whether a term-guarded write landed, having first answered the only
// question a 0-row result always has a definite answer to: was the caller still the
// leader? Every guarded query has more than one way to affect no rows (a stale term,
// a missing row, a refused transition, a conflict), and the term must win — a zombie
// CP told "shrink not allowed" concludes it is still the leader.
//
// The re-read is on the 0-row path only, and terms are monotonic: a write that
// affected no rows cannot have had a term that becomes current afterwards.
func (s *Store) wrote(ctx context.Context, term, rows int64, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	if rows > 0 {
		return true, nil
	}
	if terr := s.currentTerm(ctx, term); terr != nil {
		return false, terr
	}
	return false, nil // 0 rows for a reason the caller must diagnose
}

// currentTerm returns ErrStaleTerm unless term is the leader's term. With no leader
// row at all — before any election — every term is stale, including 0.
func (s *Store) currentTerm(ctx context.Context, term int64) error {
	l, err := s.GetLeader(ctx)
	if errors.Is(err, metadata.ErrNotFound) {
		return metadata.ErrStaleTerm
	}
	if err != nil {
		return err
	}
	if l.Term != term {
		return metadata.ErrStaleTerm
	}
	return nil
}

// staleIfZero is `wrote` for a query whose only way to affect no rows is a stale
// term (an unconditional INSERT ... SELECT WHERE EXISTS(term match) or an upsert).
func (s *Store) staleIfZero(ctx context.Context, term, rows int64, err error) error {
	ok, err := s.wrote(ctx, term, rows, err)
	if err != nil {
		return err
	}
	if !ok {
		return metadata.ErrStaleTerm
	}
	return nil
}

func notFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return metadata.ErrNotFound
	}
	return err
}

func (s *Store) AcquireLeadership(ctx context.Context, holderID string) (int64, error) {
	return s.q.AcquireLeadership(ctx, holderID)
}

func (s *Store) GetLeader(ctx context.Context) (metadata.Leader, error) {
	row, err := s.q.GetLeader(ctx)
	if err != nil {
		return metadata.Leader{}, notFound(err)
	}
	return metadata.Leader{Term: row.Term, HolderID: row.HolderID, RenewedAt: fromTS(row.RenewedAt)}, nil
}

func (s *Store) UpsertHost(ctx context.Context, term int64, h metadata.Host) error {
	hostID, err := requireUUID("host", h.HostID)
	if err != nil {
		return err
	}
	if !h.State.Valid() {
		return fmt.Errorf("%w: host state %q", lifecycle.ErrUnknownState, h.State)
	}
	// state and nvme_committed_bytes are written only when the row is created; the
	// conflict path is a heartbeat and does not carry them (see hosts.sql).
	rows, err := s.q.UpsertHost(ctx, db.UpsertHostParams{
		HostID:             hostID,
		State:              h.State.String(),
		AgentVersion:       h.AgentVersion,
		MaxFormatVersion:   h.MaxFormatVersion,
		NvmeTotalBytes:     h.NVMeTotalBytes,
		NvmeUsedBytes:      h.NVMeUsedBytes,
		NvmeCommittedBytes: h.NVMeCommittedBytes,
		Term:               term,
	})
	return s.staleIfZero(ctx, term, rows, err)
}

func (s *Store) GetHost(ctx context.Context, hostID string) (metadata.Host, error) {
	id, err := requireUUID("host", hostID)
	if err != nil {
		return metadata.Host{}, err
	}
	h, err := s.q.GetHost(ctx, id)
	if err != nil {
		return metadata.Host{}, notFound(err)
	}
	return hostFromRow(h)
}

// hostFromRow converts a generated row to the interface type. A row whose state is
// outside the vocabulary is an error, not a silently propagated string (the DB CHECK
// makes this unreachable in practice — this is the second line of defence).
func hostFromRow(h *db.Host) (metadata.Host, error) {
	state, err := lifecycle.ParseHostState(h.State)
	if err != nil {
		return metadata.Host{}, fmt.Errorf("host %s: %w", h.HostID, err)
	}
	return metadata.Host{
		HostID: h.HostID.String(), State: state, AgentVersion: h.AgentVersion,
		MaxFormatVersion: h.MaxFormatVersion, NVMeTotalBytes: h.NvmeTotalBytes,
		NVMeUsedBytes: h.NvmeUsedBytes, NVMeCommittedBytes: h.NvmeCommittedBytes,
		LastHeartbeat: fromTS(h.LastHeartbeat),
	}, nil
}

func (s *Store) ListHosts(ctx context.Context) ([]metadata.Host, error) {
	rows, err := s.q.ListHosts(ctx)
	if err != nil {
		return nil, err
	}
	hosts := make([]metadata.Host, 0, len(rows))
	for _, row := range rows {
		h, err := hostFromRow(row)
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, h)
	}
	return hosts, nil
}

func (s *Store) SetHostState(ctx context.Context, term int64, hostID string, state lifecycle.HostState) error {
	id, err := requireUUID("host", hostID)
	if err != nil {
		return err
	}
	if !state.Valid() {
		return fmt.Errorf("%w: host state %q", lifecycle.ErrUnknownState, state)
	}
	rows, err := s.q.SetHostState(ctx, db.SetHostStateParams{
		HostID: id, State: state.String(), Term: term,
		AllowedStates: state.PredecessorNames(), // the §28.1 transition table, as a predicate
	})
	ok, err := s.wrote(ctx, term, rows, err)
	if err != nil || ok {
		return err
	}
	// Still the leader, so 0 rows means a missing host or an illegal transition.
	h, gerr := s.GetHost(ctx, hostID)
	if gerr != nil {
		return gerr
	}
	if terr := h.State.Transition(state); terr != nil {
		return terr
	}
	// The row looks legal now: it moved between the write and this read. The write
	// did not land, and saying so beats reporting success.
	return fmt.Errorf("%w: host %s changed state concurrently", lifecycle.ErrInvalidTransition, hostID)
}

func (s *Store) CommitHostCapacity(ctx context.Context, term int64, hostID string, deltaBytes int64) error {
	id, err := requireUUID("host", hostID)
	if err != nil {
		return err
	}
	rows, err := s.q.CommitHostCapacity(ctx, db.CommitHostCapacityParams{
		HostID: id, NvmeCommittedBytes: deltaBytes, Term: term,
	})
	ok, err := s.wrote(ctx, term, rows, err)
	if err != nil || ok {
		return err
	}
	// Still the leader, so 0 rows means a missing host or an over-release — the
	// non-negative guard (§28.2), which is an accounting bug, never a silent clamp.
	h, gerr := s.GetHost(ctx, hostID)
	if gerr != nil {
		return gerr
	}
	if h.NVMeCommittedBytes+deltaBytes < 0 {
		return metadata.ErrCapacityUnderflow
	}
	return fmt.Errorf("%w: host %s capacity changed concurrently", metadata.ErrCapacityUnderflow, hostID)
}

func (s *Store) RenewHostLease(ctx context.Context, term int64, hostID string, ttlSeconds int) error {
	id, err := requireUUID("host", hostID)
	if err != nil {
		return err
	}
	rows, err := s.q.RenewHostLease(ctx, db.RenewHostLeaseParams{
		HostID: id, TtlSeconds: int32(ttlSeconds), Term: term,
	})
	ok, err := s.wrote(ctx, term, rows, err)
	if err != nil || ok {
		return err
	}
	// Still the leader: the only other predicate is the host-exists one.
	return metadata.ErrNotFound
}

func (s *Store) GetHostLease(ctx context.Context, hostID string) (metadata.HostLease, error) {
	id, err := requireUUID("host", hostID)
	if err != nil {
		return metadata.HostLease{}, err
	}
	l, err := s.q.GetHostLease(ctx, id)
	if err != nil {
		return metadata.HostLease{}, notFound(err)
	}
	return metadata.HostLease{
		HostID: l.HostID.String(), GrantedAt: fromTS(l.GrantedAt),
		LastRenewal: fromTS(l.LastRenewal), TTLSeconds: l.TtlSeconds,
	}, nil
}

func (s *Store) CreateVolume(ctx context.Context, term int64, v metadata.Volume) error {
	id, err := requireUUID("volume", v.VolumeID)
	if err != nil {
		return err
	}
	primary, err := nullUUID("primary host", v.PrimaryHostID)
	if err != nil {
		return err
	}
	standby, err := nullUUID("standby host", v.StandbyHostID)
	if err != nil {
		return err
	}
	durability := v.Durability
	if durability == "" {
		durability = lifecycle.DurabilityRemote
	}
	if !durability.Valid() {
		return fmt.Errorf("%w: durability %q", lifecycle.ErrUnknownState, v.Durability)
	}
	if !v.State.Valid() {
		return fmt.Errorf("%w: volume state %q", lifecycle.ErrUnknownState, v.State)
	}
	rows, err := s.q.CreateVolume(ctx, db.CreateVolumeParams{
		VolumeID: id, SizeBytes: v.SizeBytes, Durability: durability.String(),
		BlockSize: v.BlockSize, CurrentEpoch: v.CurrentEpoch, State: v.State.String(),
		DekWrapped: v.DEKWrapped, KekID: v.KEKID,
		PrimaryHostID: primary, StandbyHostID: standby, ChainDepth: v.ChainDepth,
		LocalSequence: v.LocalSequence, DurableSequence: v.DurableSequence,
		PublishedSequence: v.PublishedSequence, Term: term,
	})
	return s.staleIfZero(ctx, term, rows, err)
}

// volumeFromRow converts a generated row to the interface type, parsing its state
// and durability rather than trusting the column.
func volumeFromRow(v *db.Volume) (metadata.Volume, error) {
	state, err := lifecycle.ParseVolumeState(v.State)
	if err != nil {
		return metadata.Volume{}, fmt.Errorf("volume %s: %w", v.VolumeID, err)
	}
	durability, err := lifecycle.ParseDurability(v.Durability)
	if err != nil {
		return metadata.Volume{}, fmt.Errorf("volume %s: %w", v.VolumeID, err)
	}
	return metadata.Volume{
		VolumeID: v.VolumeID.String(), SizeBytes: v.SizeBytes, Durability: durability,
		BlockSize: v.BlockSize, CurrentEpoch: v.CurrentEpoch, State: state,
		PrimaryHostID: fromNullUUID(v.PrimaryHostID), StandbyHostID: fromNullUUID(v.StandbyHostID),
		ChainDepth: v.ChainDepth, DEKWrapped: v.DekWrapped, KEKID: v.KekID,
		LocalSequence: v.LocalSequence, DurableSequence: v.DurableSequence,
		PublishedSequence: v.PublishedSequence,
	}, nil
}

func (s *Store) GetVolume(ctx context.Context, volumeID string) (metadata.Volume, error) {
	id, err := requireUUID("volume", volumeID)
	if err != nil {
		return metadata.Volume{}, err
	}
	v, err := s.q.GetVolume(ctx, id)
	if err != nil {
		return metadata.Volume{}, notFound(err)
	}
	return volumeFromRow(v)
}

func (s *Store) ListVolumesByHost(ctx context.Context, hostID string) ([]metadata.Volume, error) {
	id, err := requireUUID("host", hostID)
	if err != nil {
		return nil, err
	}
	rows, err := s.q.ListVolumesByHost(ctx, pgtype.UUID{Bytes: id, Valid: true})
	if err != nil {
		return nil, err
	}
	vols := make([]metadata.Volume, 0, len(rows))
	for _, row := range rows {
		v, err := volumeFromRow(row)
		if err != nil {
			return nil, err
		}
		vols = append(vols, v)
	}
	return vols, nil
}

func (s *Store) BumpVolumeEpoch(ctx context.Context, term int64, volumeID, primaryHostID string) (int64, error) {
	id, err := requireUUID("volume", volumeID)
	if err != nil {
		return 0, err
	}
	primary, err := nullUUID("primary host", primaryHostID)
	if err != nil {
		return 0, err
	}
	epoch, err := s.q.BumpVolumeEpoch(ctx, db.BumpVolumeEpochParams{
		VolumeID: id, PrimaryHostID: primary, Term: term,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		// 0 rows: a stale term or a volume that is gone (§12.3). They are not the
		// same instruction to the caller — one means step down, the other means stop
		// reconciling this volume — so they are not reported as the same error.
		if terr := s.currentTerm(ctx, term); terr != nil {
			return 0, terr
		}
		return 0, metadata.ErrNotFound
	}
	return epoch, err
}

func (s *Store) UpdateWatermarks(ctx context.Context, term int64, volumeID string, local, durable, published int64) error {
	id, err := requireUUID("volume", volumeID)
	if err != nil {
		return err
	}
	if published > durable || durable > local {
		return fmt.Errorf("%w: published=%d durable=%d local=%d",
			metadata.ErrWatermarkOrder, published, durable, local)
	}
	// The query itself is monotonic (GREATEST), so a late report is ignored rather
	// than rejected — see volumes.sql.
	rows, err := s.q.UpdateVolumeWatermarks(ctx, db.UpdateVolumeWatermarksParams{
		VolumeID: id, LocalSequence: local, DurableSequence: durable,
		PublishedSequence: published, Term: term,
	})
	ok, err := s.wrote(ctx, term, rows, err)
	if err != nil || ok {
		return err
	}
	return metadata.ErrNotFound
}

func (s *Store) ResizeVolume(ctx context.Context, term int64, volumeID string, newSizeBytes int64) error {
	id, err := requireUUID("volume", volumeID)
	if err != nil {
		return err
	}
	rows, err := s.q.ResizeVolume(ctx, db.ResizeVolumeParams{VolumeID: id, SizeBytes: newSizeBytes, Term: term})
	ok, err := s.wrote(ctx, term, rows, err)
	if err != nil || ok {
		return err
	}
	// Still the leader, so 0 rows means a missing volume or a rejected shrink.
	v, gerr := s.GetVolume(ctx, volumeID)
	if gerr != nil {
		return gerr
	}
	if newSizeBytes < v.SizeBytes {
		return metadata.ErrShrinkNotAllowed
	}
	return fmt.Errorf("%w: volume %s resized concurrently", metadata.ErrShrinkNotAllowed, volumeID)
}

// SetVolumeState moves a volume through the §7 ownership machine.
func (s *Store) SetVolumeState(ctx context.Context, term int64, volumeID string, state lifecycle.VolumeState) error {
	id, err := requireUUID("volume", volumeID)
	if err != nil {
		return err
	}
	if !state.Valid() {
		return fmt.Errorf("%w: volume state %q", lifecycle.ErrUnknownState, state)
	}
	rows, err := s.q.SetVolumeState(ctx, db.SetVolumeStateParams{
		VolumeID: id, State: state.String(), Term: term,
		AllowedStates: state.PredecessorNames(), // the §7 transition table, as a predicate
	})
	ok, err := s.wrote(ctx, term, rows, err)
	if err != nil || ok {
		return err
	}
	v, gerr := s.GetVolume(ctx, volumeID)
	if gerr != nil {
		return gerr
	}
	if terr := v.State.Transition(state); terr != nil {
		return terr
	}
	return fmt.Errorf("%w: volume %s changed state concurrently", lifecycle.ErrInvalidTransition, volumeID)
}

func (s *Store) CreateSnapshot(ctx context.Context, term int64, snap metadata.Snapshot) error {
	sid, err := requireUUID("snapshot", snap.SnapshotID)
	if err != nil {
		return err
	}
	vid, err := requireUUID("volume", snap.VolumeID)
	if err != nil {
		return err
	}
	rid, err := requireUUID("request", snap.RequestID)
	if err != nil {
		return err
	}
	parent, err := nullUUID("parent snapshot", snap.ParentSnapshotID)
	if err != nil {
		return err
	}
	source, err := nullUUID("source host", snap.SourceHostID)
	if err != nil {
		return err
	}
	if !snap.State.Valid() {
		return fmt.Errorf("%w: snapshot state %q", lifecycle.ErrUnknownState, snap.State)
	}
	rows, err := s.q.CreateSnapshot(ctx, db.CreateSnapshotParams{
		SnapshotID: sid, VolumeID: vid, ParentSnapshotID: parent,
		Epoch: snap.Epoch, TargetSequence: snap.TargetSequence, RootDigest: snap.RootDigest,
		SourceHostID: source, State: snap.State.String(),
		ManifestKey: text(snap.ManifestKey), RequestID: rid, Term: term,
	})
	// 0 rows with a current term is the ON CONFLICT DO NOTHING path: the snapshot is
	// already in the catalog and is immutable (INV-16), so this is a no-op, not an
	// error. Only a stale term is.
	_, err = s.wrote(ctx, term, rows, err)
	return err
}

func (s *Store) GetSnapshot(ctx context.Context, snapshotID string) (metadata.Snapshot, error) {
	id, err := requireUUID("snapshot", snapshotID)
	if err != nil {
		return metadata.Snapshot{}, err
	}
	snap, err := s.q.GetSnapshot(ctx, id)
	if err != nil {
		return metadata.Snapshot{}, notFound(err)
	}
	state, err := lifecycle.ParseSnapshotState(snap.State)
	if err != nil {
		return metadata.Snapshot{}, fmt.Errorf("snapshot %s: %w", snap.SnapshotID, err)
	}
	return metadata.Snapshot{
		SnapshotID: snap.SnapshotID.String(), VolumeID: snap.VolumeID.String(),
		ParentSnapshotID: fromNullUUID(snap.ParentSnapshotID), Epoch: snap.Epoch,
		TargetSequence: snap.TargetSequence, RootDigest: snap.RootDigest,
		SourceHostID: fromNullUUID(snap.SourceHostID), State: state,
		ManifestKey: fromText(snap.ManifestKey), RequestID: snap.RequestID.String(),
	}, nil
}

// SetSnapshotState moves a snapshot through the §19 lifecycle.
func (s *Store) SetSnapshotState(ctx context.Context, term int64, snapshotID string, state lifecycle.SnapshotState) error {
	id, err := requireUUID("snapshot", snapshotID)
	if err != nil {
		return err
	}
	if !state.Valid() {
		return fmt.Errorf("%w: snapshot state %q", lifecycle.ErrUnknownState, state)
	}
	rows, err := s.q.SetSnapshotState(ctx, db.SetSnapshotStateParams{
		SnapshotID: id, State: state.String(), Term: term,
		AllowedStates: state.PredecessorNames(), // the §19 lifecycle, as a predicate
	})
	ok, err := s.wrote(ctx, term, rows, err)
	if err != nil || ok {
		return err
	}
	snap, gerr := s.GetSnapshot(ctx, snapshotID)
	if gerr != nil {
		return gerr
	}
	if terr := snap.State.Transition(state); terr != nil {
		return terr
	}
	return fmt.Errorf("%w: snapshot %s changed state concurrently", lifecycle.ErrInvalidTransition, snapshotID)
}

func (s *Store) RecordOperation(ctx context.Context, term int64, op metadata.Operation) (bool, error) {
	id, err := requireUUID("operation", op.OperationID)
	if err != nil {
		return false, err
	}
	volID, err := nullUUID("volume", op.VolumeID)
	if err != nil {
		return false, err
	}
	hostID, err := nullUUID("host", op.HostID)
	if err != nil {
		return false, err
	}
	if !op.Kind.Valid() {
		return false, fmt.Errorf("%w: operation kind %q", lifecycle.ErrUnknownState, op.Kind)
	}
	if !op.Phase.Valid() {
		return false, fmt.Errorf("%w: operation phase %q", lifecycle.ErrUnknownState, op.Phase)
	}
	rows, err := s.q.RecordOperation(ctx, db.RecordOperationParams{
		OperationID: id, Kind: op.Kind.String(), VolumeID: volID, HostID: hostID,
		DesiredState: op.DesiredState, CurrentState: op.CurrentState, Phase: op.Phase.String(),
		Term: term,
	})
	// This query affects 0 rows for two very different reasons: the request is a
	// duplicate (§18 idempotency — recorded=false, no error) or the caller is not
	// the leader (§7 — ErrStaleTerm). Reporting the second as the first is what lets
	// a zombie CP's Cancel return success while the real drain keeps promoting.
	return s.wrote(ctx, term, rows, err)
}

func (s *Store) UpdateOperation(ctx context.Context, term int64, op metadata.Operation) error {
	id, err := requireUUID("operation", op.OperationID)
	if err != nil {
		return err
	}
	if !op.Phase.Valid() {
		return fmt.Errorf("%w: operation phase %q", lifecycle.ErrUnknownState, op.Phase)
	}
	rows, err := s.q.UpdateOperationPhase(ctx, db.UpdateOperationPhaseParams{
		OperationID: id, CurrentState: op.CurrentState, Phase: op.Phase.String(), Error: text(op.Error),
		AllowedPhases: op.Phase.PredecessorNames(), // the §7 lifecycle, as a predicate
		Term:          term,                        // and the §7 term guard
	})
	ok, err := s.wrote(ctx, term, rows, err)
	if err != nil || ok {
		return err
	}
	// Still the leader, so 0 rows means the operation is missing or the phase move
	// is illegal — a terminal operation is never resurrected.
	cur, gerr := s.GetOperation(ctx, op.OperationID)
	if gerr != nil {
		return gerr
	}
	if terr := cur.Phase.Transition(op.Phase); terr != nil {
		return terr
	}
	return fmt.Errorf("%w: operation %s changed phase concurrently", lifecycle.ErrInvalidTransition, op.OperationID)
}

func (s *Store) GetOperation(ctx context.Context, operationID string) (metadata.Operation, error) {
	id, err := requireUUID("operation", operationID)
	if err != nil {
		return metadata.Operation{}, err
	}
	op, err := s.q.GetOperation(ctx, id)
	if err != nil {
		return metadata.Operation{}, notFound(err)
	}
	kind, err := lifecycle.ParseOperationKind(op.Kind)
	if err != nil {
		return metadata.Operation{}, fmt.Errorf("operation %s: %w", op.OperationID, err)
	}
	phase, err := lifecycle.ParseOperationPhase(op.Phase)
	if err != nil {
		return metadata.Operation{}, fmt.Errorf("operation %s: %w", op.OperationID, err)
	}
	return metadata.Operation{
		OperationID: op.OperationID.String(), Kind: kind,
		VolumeID: fromNullUUID(op.VolumeID), HostID: fromNullUUID(op.HostID),
		DesiredState: op.DesiredState, CurrentState: op.CurrentState,
		Phase: phase, Error: fromText(op.Error),
	}, nil
}
