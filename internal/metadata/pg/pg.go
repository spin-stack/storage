// Package pg is the production metadata.Store: a thin adapter over the sqlc-
// generated queries on pgx/v5 (ADR-0006). It is verified by TestContainers
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
	return staleIfZero(s.q.UpsertHost(ctx, db.UpsertHostParams{
		HostID:             h.HostID,
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
	h, err := s.q.GetHost(ctx, hostID)
	if err != nil {
		return metadata.Host{}, notFound(err)
	}
	return metadata.Host{
		HostID: h.HostID, State: h.State, AgentVersion: h.AgentVersion,
		MaxFormatVersion: h.MaxFormatVersion, NVMeTotalBytes: h.NvmeTotalBytes,
		NVMeUsedBytes: h.NvmeUsedBytes, NVMeCommittedBytes: h.NvmeCommittedBytes,
		LastHeartbeat: fromTS(h.LastHeartbeat),
	}, nil
}

func (s *Store) RenewHostLease(ctx context.Context, term int64, hostID string, ttlSeconds int) error {
	return staleIfZero(s.q.RenewHostLease(ctx, db.RenewHostLeaseParams{
		HostID: hostID, TtlSeconds: int32(ttlSeconds), Term: term,
	}))
}

func (s *Store) GetHostLease(ctx context.Context, hostID string) (metadata.HostLease, error) {
	l, err := s.q.GetHostLease(ctx, hostID)
	if err != nil {
		return metadata.HostLease{}, notFound(err)
	}
	return metadata.HostLease{
		HostID: l.HostID, GrantedAt: fromTS(l.GrantedAt),
		LastRenewal: fromTS(l.LastRenewal), TTLSeconds: l.TtlSeconds,
	}, nil
}

func (s *Store) CreateVolume(ctx context.Context, term int64, v metadata.Volume) error {
	durability := v.Durability
	if durability == "" {
		durability = "remote"
	}
	return staleIfZero(s.q.CreateVolume(ctx, db.CreateVolumeParams{
		VolumeID: v.VolumeID, SizeBytes: v.SizeBytes, Durability: durability,
		BlockSize: v.BlockSize, State: v.State, DekWrapped: v.DEKWrapped, KekID: v.KEKID,
		Term: term,
	}))
}

func (s *Store) GetVolume(ctx context.Context, volumeID string) (metadata.Volume, error) {
	v, err := s.q.GetVolume(ctx, volumeID)
	if err != nil {
		return metadata.Volume{}, notFound(err)
	}
	return metadata.Volume{
		VolumeID: v.VolumeID, SizeBytes: v.SizeBytes, Durability: v.Durability,
		BlockSize: v.BlockSize, CurrentEpoch: v.CurrentEpoch, State: v.State,
		PrimaryHostID: fromText(v.PrimaryHostID), StandbyHostID: fromText(v.StandbyHostID),
		ChainDepth: v.ChainDepth, DEKWrapped: v.DekWrapped, KEKID: v.KekID,
		LocalSequence: v.LocalSequence, DurableSequence: v.DurableSequence,
		PublishedSequence: v.PublishedSequence,
	}, nil
}

func (s *Store) BumpVolumeEpoch(ctx context.Context, term int64, volumeID, primaryHostID string) (int64, error) {
	epoch, err := s.q.BumpVolumeEpoch(ctx, db.BumpVolumeEpochParams{
		VolumeID: volumeID, PrimaryHostID: text(primaryHostID), Term: term,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		// 0 rows: stale term or missing volume (§12.3). Treat as stale term.
		return 0, metadata.ErrStaleTerm
	}
	return epoch, err
}

func (s *Store) UpdateWatermarks(ctx context.Context, term int64, volumeID string, local, durable, published int64) error {
	return staleIfZero(s.q.UpdateVolumeWatermarks(ctx, db.UpdateVolumeWatermarksParams{
		VolumeID: volumeID, LocalSequence: local, DurableSequence: durable,
		PublishedSequence: published, Term: term,
	}))
}

func (s *Store) RecordOperation(ctx context.Context, op metadata.Operation) (bool, error) {
	id, err := uuid.Parse(op.OperationID)
	if err != nil {
		return false, err
	}
	rows, err := s.q.RecordOperation(ctx, db.RecordOperationParams{
		OperationID: id, Kind: op.Kind, VolumeID: text(op.VolumeID), HostID: text(op.HostID),
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
		VolumeID: fromText(op.VolumeID), HostID: fromText(op.HostID),
		DesiredState: op.DesiredState, CurrentState: op.CurrentState,
		Phase: op.Phase, Error: fromText(op.Error),
	}, nil
}
