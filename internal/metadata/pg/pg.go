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

func nullUUID(s string) pgtype.UUID {
	if s == "" {
		return pgtype.UUID{}
	}
	u, err := uuid.Parse(s)
	if err != nil {
		return pgtype.UUID{}
	}
	return pgtype.UUID{Bytes: u, Valid: true}
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

// staleIfZero maps "0 rows affected" from a term-guarded write to ErrStaleTerm (§7).
func staleIfZero(rows int64, err error) error {
	if err != nil {
		return err
	}
	if rows == 0 {
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
	hostID, err := uuid.Parse(h.HostID)
	if err != nil {
		return err
	}
	if !h.State.Valid() {
		return fmt.Errorf("%w: host state %q", lifecycle.ErrUnknownState, h.State)
	}
	return staleIfZero(s.q.UpsertHost(ctx, db.UpsertHostParams{
		HostID:             hostID,
		State:              h.State.String(),
		AgentVersion:       h.AgentVersion,
		MaxFormatVersion:   h.MaxFormatVersion,
		NvmeTotalBytes:     h.NVMeTotalBytes,
		NvmeUsedBytes:      h.NVMeUsedBytes,
		NvmeCommittedBytes: h.NVMeCommittedBytes,
		Term:               term,
	}))
}

func (s *Store) GetHost(ctx context.Context, hostID string) (metadata.Host, error) {
	id, err := uuid.Parse(hostID)
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
	id, err := uuid.Parse(hostID)
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
	if err != nil {
		return err
	}
	if rows == 0 {
		// 0 rows: missing host, an illegal transition, or a stale term.
		h, gerr := s.GetHost(ctx, hostID)
		switch {
		case errors.Is(gerr, metadata.ErrNotFound):
			return metadata.ErrNotFound
		case gerr != nil:
			return gerr
		}
		if terr := h.State.Transition(state); terr != nil {
			return terr
		}
		return metadata.ErrStaleTerm
	}
	return nil
}

func (s *Store) CommitHostCapacity(ctx context.Context, term int64, hostID string, deltaBytes int64) error {
	id, err := uuid.Parse(hostID)
	if err != nil {
		return err
	}
	rows, err := s.q.CommitHostCapacity(ctx, db.CommitHostCapacityParams{
		HostID: id, NvmeCommittedBytes: deltaBytes, Term: term,
	})
	if err != nil {
		return err
	}
	if rows == 0 {
		// 0 rows: missing host, an over-release (the non-negative guard), or a stale
		// term — disambiguated so the caller gets an actionable error (§28.2).
		h, gerr := s.GetHost(ctx, hostID)
		switch {
		case errors.Is(gerr, metadata.ErrNotFound):
			return metadata.ErrNotFound
		case gerr != nil:
			return gerr
		case h.NVMeCommittedBytes+deltaBytes < 0:
			return metadata.ErrCapacityUnderflow
		}
		return metadata.ErrStaleTerm
	}
	return nil
}

func (s *Store) RenewHostLease(ctx context.Context, term int64, hostID string, ttlSeconds int) error {
	id, err := uuid.Parse(hostID)
	if err != nil {
		return err
	}
	return staleIfZero(s.q.RenewHostLease(ctx, db.RenewHostLeaseParams{
		HostID: id, TtlSeconds: int32(ttlSeconds), Term: term,
	}))
}

func (s *Store) GetHostLease(ctx context.Context, hostID string) (metadata.HostLease, error) {
	id, err := uuid.Parse(hostID)
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
	id, err := uuid.Parse(v.VolumeID)
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
	return staleIfZero(s.q.CreateVolume(ctx, db.CreateVolumeParams{
		VolumeID: id, SizeBytes: v.SizeBytes, Durability: durability.String(),
		BlockSize: v.BlockSize, CurrentEpoch: v.CurrentEpoch, State: v.State.String(),
		DekWrapped: v.DEKWrapped, KekID: v.KEKID,
		PrimaryHostID: nullUUID(v.PrimaryHostID), ChainDepth: v.ChainDepth, Term: term,
	}))
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
	id, err := uuid.Parse(volumeID)
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
	id, err := uuid.Parse(hostID)
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
	id, err := uuid.Parse(volumeID)
	if err != nil {
		return 0, err
	}
	epoch, err := s.q.BumpVolumeEpoch(ctx, db.BumpVolumeEpochParams{
		VolumeID: id, PrimaryHostID: nullUUID(primaryHostID), Term: term,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		// 0 rows: stale term or missing volume (§12.3). Treat as stale term.
		return 0, metadata.ErrStaleTerm
	}
	return epoch, err
}

func (s *Store) UpdateWatermarks(ctx context.Context, term int64, volumeID string, local, durable, published int64) error {
	id, err := uuid.Parse(volumeID)
	if err != nil {
		return err
	}
	return staleIfZero(s.q.UpdateVolumeWatermarks(ctx, db.UpdateVolumeWatermarksParams{
		VolumeID: id, LocalSequence: local, DurableSequence: durable,
		PublishedSequence: published, Term: term,
	}))
}

func (s *Store) ResizeVolume(ctx context.Context, term int64, volumeID string, newSizeBytes int64) error {
	id, err := uuid.Parse(volumeID)
	if err != nil {
		return err
	}
	rows, err := s.q.ResizeVolume(ctx, db.ResizeVolumeParams{VolumeID: id, SizeBytes: newSizeBytes, Term: term})
	if err != nil {
		return err
	}
	if rows == 0 {
		// 0 rows: stale term, missing volume, or a rejected shrink.
		v, gerr := s.GetVolume(ctx, volumeID)
		if gerr == nil && newSizeBytes < v.SizeBytes {
			return metadata.ErrShrinkNotAllowed
		}
		return metadata.ErrStaleTerm
	}
	return nil
}

func (s *Store) CreateSnapshot(ctx context.Context, term int64, snap metadata.Snapshot) error {
	sid, err := uuid.Parse(snap.SnapshotID)
	if err != nil {
		return err
	}
	vid, err := uuid.Parse(snap.VolumeID)
	if err != nil {
		return err
	}
	rid, err := uuid.Parse(snap.RequestID)
	if err != nil {
		return err
	}
	if !snap.State.Valid() {
		return fmt.Errorf("%w: snapshot state %q", lifecycle.ErrUnknownState, snap.State)
	}
	return staleIfZero(s.q.CreateSnapshot(ctx, db.CreateSnapshotParams{
		SnapshotID: sid, VolumeID: vid, ParentSnapshotID: nullUUID(snap.ParentSnapshotID),
		Epoch: snap.Epoch, TargetSequence: snap.TargetSequence, RootDigest: snap.RootDigest,
		SourceHostID: nullUUID(snap.SourceHostID), State: snap.State.String(),
		ManifestKey: text(snap.ManifestKey), RequestID: rid, Term: term,
	}))
}

func (s *Store) GetSnapshot(ctx context.Context, snapshotID string) (metadata.Snapshot, error) {
	id, err := uuid.Parse(snapshotID)
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

func (s *Store) RecordOperation(ctx context.Context, term int64, op metadata.Operation) (bool, error) {
	id, err := uuid.Parse(op.OperationID)
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
		OperationID: id, Kind: op.Kind.String(), VolumeID: nullUUID(op.VolumeID), HostID: nullUUID(op.HostID),
		DesiredState: op.DesiredState, CurrentState: op.CurrentState, Phase: op.Phase.String(),
		Term: term,
	})
	if err != nil {
		return false, err
	}
	return rows == 1, nil
}

func (s *Store) UpdateOperation(ctx context.Context, term int64, op metadata.Operation) error {
	id, err := uuid.Parse(op.OperationID)
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
	if err != nil {
		return err
	}
	if rows == 0 {
		// 0 rows: the operation does not exist, the phase move is illegal, or the
		// term is stale.
		cur, gerr := s.GetOperation(ctx, op.OperationID)
		if gerr != nil {
			return gerr
		}
		if terr := cur.Phase.Transition(op.Phase); terr != nil {
			return terr
		}
		return metadata.ErrStaleTerm
	}
	return nil
}

func (s *Store) GetOperation(ctx context.Context, operationID string) (metadata.Operation, error) {
	id, err := uuid.Parse(operationID)
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
