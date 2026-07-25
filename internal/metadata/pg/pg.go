// Package pg is the production metadata.Store: a thin adapter over the sqlc-
// generated queries on pgx/v5 (ADR-0006). Identity columns are uuid (ADR-0007); the
// adapter parses string ids at the boundary. It is verified by TestContainers
// integration tests; the fencing protocol itself is proven in metadata/sim.
package pg

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/spin-stack/storage/internal/db"
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
	return staleIfZero(s.q.UpsertHost(ctx, db.UpsertHostParams{
		HostID:             hostID,
		State:              h.State,
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
	return hostFromRow(h), nil
}

// hostFromRow converts a generated row to the interface type.
func hostFromRow(h *db.Host) metadata.Host {
	return metadata.Host{
		HostID: h.HostID.String(), State: h.State, AgentVersion: h.AgentVersion,
		MaxFormatVersion: h.MaxFormatVersion, NVMeTotalBytes: h.NvmeTotalBytes,
		NVMeUsedBytes: h.NvmeUsedBytes, NVMeCommittedBytes: h.NvmeCommittedBytes,
		LastHeartbeat: fromTS(h.LastHeartbeat),
	}
}

func (s *Store) ListHosts(ctx context.Context) ([]metadata.Host, error) {
	rows, err := s.q.ListHosts(ctx)
	if err != nil {
		return nil, err
	}
	hosts := make([]metadata.Host, 0, len(rows))
	for _, h := range rows {
		hosts = append(hosts, hostFromRow(h))
	}
	return hosts, nil
}

func (s *Store) SetHostState(ctx context.Context, term int64, hostID, state string) error {
	id, err := uuid.Parse(hostID)
	if err != nil {
		return err
	}
	rows, err := s.q.SetHostState(ctx, db.SetHostStateParams{HostID: id, State: state, Term: term})
	if err != nil {
		return err
	}
	if rows == 0 {
		// 0 rows: stale term or a host that does not exist.
		if _, gerr := s.GetHost(ctx, hostID); errors.Is(gerr, metadata.ErrNotFound) {
			return metadata.ErrNotFound
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
		durability = "remote"
	}
	return staleIfZero(s.q.CreateVolume(ctx, db.CreateVolumeParams{
		VolumeID: id, SizeBytes: v.SizeBytes, Durability: durability,
		BlockSize: v.BlockSize, CurrentEpoch: v.CurrentEpoch, State: v.State,
		DekWrapped: v.DEKWrapped, KekID: v.KEKID,
		PrimaryHostID: nullUUID(v.PrimaryHostID), ChainDepth: v.ChainDepth, Term: term,
	}))
}

// volumeFromRow converts a generated row to the interface type.
func volumeFromRow(v *db.Volume) metadata.Volume {
	return metadata.Volume{
		VolumeID: v.VolumeID.String(), SizeBytes: v.SizeBytes, Durability: v.Durability,
		BlockSize: v.BlockSize, CurrentEpoch: v.CurrentEpoch, State: v.State,
		PrimaryHostID: fromNullUUID(v.PrimaryHostID), StandbyHostID: fromNullUUID(v.StandbyHostID),
		ChainDepth: v.ChainDepth, DEKWrapped: v.DekWrapped, KEKID: v.KekID,
		LocalSequence: v.LocalSequence, DurableSequence: v.DurableSequence,
		PublishedSequence: v.PublishedSequence,
	}
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
	return volumeFromRow(v), nil
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
	for _, v := range rows {
		vols = append(vols, volumeFromRow(v))
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
	return staleIfZero(s.q.CreateSnapshot(ctx, db.CreateSnapshotParams{
		SnapshotID: sid, VolumeID: vid, ParentSnapshotID: nullUUID(snap.ParentSnapshotID),
		Epoch: snap.Epoch, TargetSequence: snap.TargetSequence, RootDigest: snap.RootDigest,
		SourceHostID: nullUUID(snap.SourceHostID), State: snap.State,
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
	return metadata.Snapshot{
		SnapshotID: snap.SnapshotID.String(), VolumeID: snap.VolumeID.String(),
		ParentSnapshotID: fromNullUUID(snap.ParentSnapshotID), Epoch: snap.Epoch,
		TargetSequence: snap.TargetSequence, RootDigest: snap.RootDigest,
		SourceHostID: fromNullUUID(snap.SourceHostID), State: snap.State,
		ManifestKey: fromText(snap.ManifestKey), RequestID: snap.RequestID.String(),
	}, nil
}

func (s *Store) RecordOperation(ctx context.Context, op metadata.Operation) (bool, error) {
	id, err := uuid.Parse(op.OperationID)
	if err != nil {
		return false, err
	}
	rows, err := s.q.RecordOperation(ctx, db.RecordOperationParams{
		OperationID: id, Kind: op.Kind, VolumeID: nullUUID(op.VolumeID), HostID: nullUUID(op.HostID),
		DesiredState: op.DesiredState, CurrentState: op.CurrentState, Phase: op.Phase,
	})
	if err != nil {
		return false, err
	}
	return rows == 1, nil
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
	return metadata.Operation{
		OperationID: op.OperationID.String(), Kind: op.Kind,
		VolumeID: fromNullUUID(op.VolumeID), HostID: fromNullUUID(op.HostID),
		DesiredState: op.DesiredState, CurrentState: op.CurrentState,
		Phase: op.Phase, Error: fromText(op.Error),
	}, nil
}
