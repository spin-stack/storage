// Package pg is the production metadata.Store: a thin adapter over the sqlc-
// generated queries on pgx/v5. Identity columns are uuid (ADR-0007); the
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
	// conn is kept alongside the queries for one reason: a bounded write needs two
	// statements in one transaction (see placing). db.DBTX is sqlc's generated
	// interface and cannot open one.
	conn db.DBTX
}

// New returns a Store over any pgx DBTX (pool, conn, or tx).
func New(conn db.DBTX) *Store { return &Store{q: db.New(conn), conn: conn} }

// beginner is the part of a pool, a connection or a transaction that can open a
// (nested) transaction. Declared where it is consumed rather than added to db.DBTX,
// which sqlc generates and regenerates.
type beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// placing runs a write that carries a capacity bound: one transaction that takes the
// destination's advisory lock first, so the bound's predicate cannot be evaluated by
// two placements against a fleet neither of them is in yet.
//
// The bound being a predicate of the write (ADR-0017) is necessary and not
// sufficient here. READ COMMITTED fixes a statement's snapshot before it runs, and
// the derived committed value is an aggregate over rows the statement does not lock;
// two INSERTs that overlap in time each see a fleet without the other, both affect
// one row, and the host lands at twice its ceiling. That was measured against a real
// PostgreSQL before this existed, not inferred. The lock's own reasoning — and why
// it is a separate statement, and why not SERIALIZABLE or FOR UPDATE — is in
// hosts.sql next to the query.
//
// An unbounded write is not a placement decision and pays nothing: no transaction,
// no lock, the same single statement as before.
func (s *Store) placing(ctx context.Context, b *metadata.CapacityBound, write func(*db.Queries) (int64, error)) (int64, error) {
	if b == nil {
		return write(s.q)
	}
	hostID, err := requireUUID("bound host", b.HostID)
	if err != nil {
		return 0, err
	}
	conn, ok := s.conn.(beginner)
	if !ok {
		// Fail closed rather than silently falling back to the unserialized write:
		// the caller would get a bound that holds under test and races in production,
		// which is the failure this whole arrangement exists to remove.
		return 0, fmt.Errorf("metadata/pg: a bounded write needs a connection that can open a transaction, got %T", s.conn)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("metadata/pg: opening the placement transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // a no-op once Commit has run
	q := s.q.WithTx(tx)
	if err := q.LockHostPlacement(ctx, hostID); err != nil {
		return 0, fmt.Errorf("metadata/pg: locking placement on host %s: %w", b.HostID, err)
	}
	rows, err := write(q)
	if err != nil {
		return 0, err
	}
	// Committed even when the bound refused the write: zero rows changed nothing, and
	// the caller's diagnosis (boundRefused) reads the same rows the next placement
	// will. Rolling back would say the same thing more slowly.
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("metadata/pg: committing the placement: %w", err)
	}
	return rows, nil
}

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

// boundParams turns the optional capacity bound into the four query parameters the
// guarded writes take. A nil bound is a NULL host, which the predicate reads as
// "this write is not a placement decision".
func boundParams(b *metadata.CapacityBound) (pgtype.UUID, int64, int64, int64, error) {
	if b == nil {
		return pgtype.UUID{}, 0, 0, 0, nil
	}
	id, err := requireUUID("bound host", b.HostID)
	if err != nil {
		return pgtype.UUID{}, 0, 0, 0, err
	}
	return pgtype.UUID{Bytes: id, Valid: true}, b.AddBytes, b.Limit, b.UsedLimit, nil
}

// boundRefused diagnoses a 0-row write that carried a bound, on the 0-row path only:
// the write did not land, so there is nothing to undo and nothing racing this read
// can make it wrong about that. It returns nil when the bound is not what stopped it.
func (s *Store) boundRefused(ctx context.Context, b *metadata.CapacityBound) error {
	if b == nil {
		return nil
	}
	h, err := s.GetHost(ctx, b.HostID)
	if err != nil {
		return err
	}
	if after := h.NVMeCommittedBytes + b.AddBytes; after > b.Limit {
		return fmt.Errorf("%w: host %s would hold %d committed bytes, the policy admits %d",
			metadata.ErrCapacityExceeded, b.HostID, after, b.Limit)
	}
	// The measured arm (ADR-0013): what the host reported about its own device at its
	// last heartbeat, which is what actually runs out. Diagnosed second because a host
	// that is over both should be reported as over its promises first — that is the
	// number an operator can act on by moving volumes, while the fill is whatever the
	// guests and the other tenants of that filesystem have written.
	if h.NVMeUsedBytes > b.UsedLimit {
		return fmt.Errorf("%w: host %s measures %d used bytes, the policy takes new volumes below %d",
			metadata.ErrCapacityExceeded, b.HostID, h.NVMeUsedBytes, b.UsedLimit)
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

// RenewLeadership stamps renewed_at under the caller's own term and holder id, moving
// neither (metadata.Store carries why it is not AcquireLeadership on a timer).
//
// holder_id is TEXT, not a uuid, so the empty check is here rather than in requireUUID:
// an empty holder would otherwise match no row and be reported as a lost term, which is
// a process exiting because of an unset flag.
func (s *Store) RenewLeadership(ctx context.Context, term int64, holderID string) error {
	if holderID == "" {
		return fmt.Errorf("%w: empty holder id", metadata.ErrInvalidID)
	}
	rows, err := s.q.RenewLeadership(ctx, db.RenewLeadershipParams{Term: term, HolderID: holderID})
	// staleIfZero: the predicate is the term and the holder, and there is no third way
	// to affect no rows — the singleton row either exists and agrees, or this process is
	// not the leader any more.
	return s.staleIfZero(ctx, term, rows, err)
}

// Now is PostgreSQL's clock: the one that stamps last_renewal, and so the one every
// fencing deadline has to be measured against (§12.1).
func (s *Store) Now(ctx context.Context) (time.Time, error) {
	ts, err := s.q.DatabaseNow(ctx)
	if err != nil {
		return time.Time{}, err
	}
	return fromTS(ts), nil
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
	// state is written only when the row is created; the conflict path is a
	// heartbeat and does not carry it (see hosts.sql). Committed capacity is not a
	// column at all (ADR-0017), so h.NVMeCommittedBytes is dropped here.
	rows, err := s.q.UpsertHost(ctx, db.UpsertHostParams{
		HostID:           hostID,
		State:            h.State.String(),
		AgentVersion:     h.AgentVersion,
		MaxFormatVersion: h.MaxFormatVersion,
		NvmeTotalBytes:   h.NVMeTotalBytes,
		NvmeUsedBytes:    h.NVMeUsedBytes,
		// The backlog is the host's own report, so it travels with the rest of what
		// the host knows about itself (ADR-0013 §1).
		NvmeRemoteBacklogBytes: h.RemoteBacklogBytes,
		Term:                   term,
	})
	return s.staleIfZero(ctx, term, rows, err)
}

func (s *Store) GetHost(ctx context.Context, hostID string) (metadata.Host, error) {
	id, err := requireUUID("host", hostID)
	if err != nil {
		return metadata.Host{}, err
	}
	row, err := s.q.GetHost(ctx, id)
	if err != nil {
		return metadata.Host{}, notFound(err)
	}
	return hostFromRow(&row.Host, row.CommittedBytes)
}

// hostFromRow converts a generated row to the interface type. A row whose state is
// outside the vocabulary is an error, not a silently propagated string (the DB CHECK
// makes this unreachable in practice — this is the second line of defence).
func hostFromRow(h *db.Host, committed int64) (metadata.Host, error) {
	state, err := lifecycle.ParseHostState(h.State)
	if err != nil {
		return metadata.Host{}, fmt.Errorf("host %s: %w", h.HostID, err)
	}
	reason, err := lifecycle.ParseCordonReason(h.CordonReason)
	if err != nil {
		return metadata.Host{}, fmt.Errorf("host %s: %w", h.HostID, err)
	}
	return metadata.Host{
		HostID: h.HostID.String(), State: state, CordonReason: reason, AgentVersion: h.AgentVersion,
		MaxFormatVersion: h.MaxFormatVersion, NVMeTotalBytes: h.NvmeTotalBytes,
		NVMeUsedBytes: h.NvmeUsedBytes, RemoteBacklogBytes: h.NvmeRemoteBacklogBytes,
		NVMeCommittedBytes: committed,
		LastHeartbeat:      fromTS(h.LastHeartbeat),
	}, nil
}

func (s *Store) ListHosts(ctx context.Context) ([]metadata.Host, error) {
	rows, err := s.q.ListHosts(ctx)
	if err != nil {
		return nil, err
	}
	hosts := make([]metadata.Host, 0, len(rows))
	for _, row := range rows {
		h, err := hostFromRow(&row.Host, row.CommittedBytes)
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, h)
	}
	return hosts, nil
}

func (s *Store) SetHostState(ctx context.Context, term int64, hostID string, state lifecycle.HostState, reason lifecycle.CordonReason) error {
	id, err := requireUUID("host", hostID)
	if err != nil {
		return err
	}
	if !state.Valid() {
		return fmt.Errorf("%w: host state %q", lifecycle.ErrUnknownState, state)
	}
	if !reason.Authority() {
		return fmt.Errorf("%w: cordon reason %q is not an authority", lifecycle.ErrUnknownState, reason)
	}
	// The stored reason is derived here rather than by a CASE in the statement: the
	// rule "a reason belongs to a cordon" is already stated by the table constraint
	// and by the Go type, and a third copy in SQL is a third place it can drift.
	stored := lifecycle.CordonNone
	if state == lifecycle.HostCordoned {
		stored = reason
	}
	rows, err := s.q.SetHostState(ctx, db.SetHostStateParams{
		HostID: id, State: state.String(), Term: term,
		AllowedStates:       state.PredecessorNames(), // the §28.1 transition table, as a predicate
		CordonReason:        stored.String(),
		OverwritableReasons: reason.OverwritableNames(), // ADR-0013 §5's authority split, as a predicate
	})
	ok, err := s.wrote(ctx, term, rows, err)
	if err != nil || ok {
		return err
	}
	// Still the leader, so 0 rows means a missing host, an illegal transition, or a
	// cordon this writer does not outrank.
	h, gerr := s.GetHost(ctx, hostID)
	if gerr != nil {
		return gerr
	}
	if terr := h.State.Transition(state); terr != nil {
		return terr
	}
	if !reason.MayOverwrite(h.CordonReason) {
		return fmt.Errorf("%w: host %s is cordoned by %s, %s may not change it",
			lifecycle.ErrCordonHeld, hostID, h.CordonReason, reason)
	}
	// The row looks legal now: it moved between the write and this read. The write
	// did not land, and saying so beats reporting success.
	return fmt.Errorf("%w: host %s changed state concurrently", lifecycle.ErrInvalidTransition, hostID)
}

func (s *Store) RenewHostLease(ctx context.Context, term int64, hostID string, ttlSeconds int) error {
	id, err := requireUUID("host", hostID)
	if err != nil {
		return err
	}
	rows, err := s.q.RenewHostLease(ctx, db.RenewHostLeaseParams{
		HostID: id, TtlSeconds: int32(ttlSeconds), Term: term,
		ServingStates: lifecycle.ServingHostStateNames(), // §28.1, as a predicate
	})
	ok, err := s.wrote(ctx, term, rows, err)
	if err != nil || ok {
		return err
	}
	// Still the leader, so the row was filtered by the host predicate: there is no
	// such host, or it is one the fleet no longer counts as a writer.
	h, gerr := s.GetHost(ctx, hostID)
	if gerr != nil {
		return gerr
	}
	if !h.State.Serving() {
		return fmt.Errorf("%w: host %s is %s", metadata.ErrHostNotServing, hostID, h.State)
	}
	// The host looks eligible now: it changed state between the write and this read.
	// The write did not land, and saying so beats reporting success.
	return fmt.Errorf("%w: host %s changed state concurrently", metadata.ErrHostNotServing, hostID)
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

func (s *Store) CreateVolume(ctx context.Context, term int64, v metadata.Volume, bound *metadata.CapacityBound) error {
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
	if !v.State.Valid() {
		return fmt.Errorf("%w: volume state %q", lifecycle.ErrUnknownState, v.State)
	}
	// INV-03 before the insert, so a disordered triple is ErrWatermarkOrder rather
	// than the volumes_watermarks_ordered constraint arriving as an opaque 23514.
	if err := metadata.CheckWatermarkOrder(v.LocalSequence, v.DurableSequence, v.PublishedSequence); err != nil {
		return err
	}
	if err := metadata.CheckDEKKeyID(v.DEKKeyID); err != nil {
		return err
	}
	parentSnap, err := nullUUID("parent snapshot", v.ParentSnapshotID)
	if err != nil {
		return err
	}
	boundHost, addBytes, limit, usedLimit, err := boundParams(bound)
	if err != nil {
		return err
	}
	rows, err := s.placing(ctx, bound, func(q *db.Queries) (int64, error) {
		return q.CreateVolume(ctx, db.CreateVolumeParams{
			VolumeID: id, SizeBytes: v.SizeBytes,
			BlockSize: v.BlockSize, RpoTargetSeconds: v.RPOTargetSeconds,
			CurrentEpoch: v.CurrentEpoch, State: v.State.String(),
			DekWrapped: v.DEKWrapped, KekID: v.KEKID, DekKeyID: int64(v.DEKKeyID),
			ParentSnapshotID: parentSnap,
			PrimaryHostID:    primary, StandbyHostID: standby, ChainDepth: v.ChainDepth,
			LocalSequence: v.LocalSequence, DurableSequence: v.DurableSequence,
			PublishedSequence: v.PublishedSequence, Term: term,
			BoundHost: boundHost, BoundAddBytes: addBytes, BoundLimit: limit,
			BoundUsedLimit: usedLimit,
		})
	})
	ok, err := s.wrote(ctx, term, rows, err)
	if err != nil || ok {
		return err
	}
	// Still the leader, so the only other predicate is the §28.2 bound (ADR-0017).
	if berr := s.boundRefused(ctx, bound); berr != nil {
		return berr
	}
	return fmt.Errorf("%w: host %s capacity changed concurrently", metadata.ErrCapacityExceeded, bound.HostID)
}

// volumeFromRow converts a generated row to the interface type, parsing its state
// rather than trusting the column.
func volumeFromRow(v *db.Volume) (metadata.Volume, error) {
	state, err := lifecycle.ParseVolumeState(v.State)
	if err != nil {
		return metadata.Volume{}, fmt.Errorf("volume %s: %w", v.VolumeID, err)
	}
	refusal, err := lifecycle.ParseRefusal(v.Refusal)
	if err != nil {
		return metadata.Volume{}, fmt.Errorf("volume %s: %w", v.VolumeID, err)
	}
	return metadata.Volume{
		VolumeID: v.VolumeID.String(), SizeBytes: v.SizeBytes,
		BlockSize: v.BlockSize, RPOTargetSeconds: v.RpoTargetSeconds,
		CurrentEpoch: v.CurrentEpoch, State: state,
		PrimaryHostID: fromNullUUID(v.PrimaryHostID), StandbyHostID: fromNullUUID(v.StandbyHostID),
		ChainDepth: v.ChainDepth, ParentSnapshotID: fromNullUUID(v.ParentSnapshotID),
		DEKWrapped: v.DekWrapped, KEKID: v.KekID,
		// The column's CHECK bounds it to (0, 2^32), so the narrowing is total —
		// and it is the same 16-byte-id story as volume_id: BIGINT at the boundary,
		// the format's own width in the interface.
		DEKKeyID:      uint32(v.DekKeyID), //nolint:gosec // bounded by volumes.dek_key_id's CHECK
		LocalSequence: v.LocalSequence, DurableSequence: v.DurableSequence,
		PublishedSequence: v.PublishedSequence,
		Refusal:           refusal,
		RefusalDetail:     v.RefusalDetail,
		FencingStartedAt:  fromTS(v.FencingStartedAt),
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

// ListVolumes returns every volume, placed or not, in volume-id order.
func (s *Store) ListVolumes(ctx context.Context) ([]metadata.Volume, error) {
	rows, err := s.q.ListVolumes(ctx)
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

func (s *Store) BumpVolumeEpoch(ctx context.Context, term int64, volumeID, primaryHostID string, expectedEpoch int64) (int64, error) {
	id, err := requireUUID("volume", volumeID)
	if err != nil {
		return 0, err
	}
	primary, err := nullUUID("primary host", primaryHostID)
	if err != nil {
		return 0, err
	}
	epoch, err := s.q.BumpVolumeEpoch(ctx, db.BumpVolumeEpochParams{
		VolumeID: id, PrimaryHostID: primary, Term: term, ExpectedEpoch: expectedEpoch,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		// 0 rows: a stale term, a volume that is gone, or an epoch that moved on
		// (§12.3). They are three different instructions to the caller — step down,
		// stop reconciling this volume, re-read and decide again — so they are not
		// reported as the same error.
		if terr := s.currentTerm(ctx, term); terr != nil {
			return 0, terr
		}
		v, gerr := s.GetVolume(ctx, volumeID)
		if gerr != nil {
			return 0, gerr
		}
		return 0, fmt.Errorf("%w: volume %s is at %d, expected %d",
			metadata.ErrEpochConflict, volumeID, v.CurrentEpoch, expectedEpoch)
	}
	return epoch, err
}

// SetVolumePrimaryHost places a volume on a host or clears its placement, with the §7
// state that goes with it (metadata.PlacedState) in the same statement.
//
// The 0-row path is where the work is. The query has four ways to affect nothing and
// they are four different instructions to the caller, so it re-reads to tell them
// apart — on that path only, where the write is known not to have landed:
//
//	stale term      -> step down (`wrote` answers this first, and it must win)
//	no such volume  -> stop reconciling this volume
//	placed on B     -> detach it first (ErrAlreadyPlaced)
//	illegal move    -> the §7 machine says no (lifecycle.ErrInvalidTransition)
func (s *Store) SetVolumePrimaryHost(ctx context.Context, term int64, volumeID, primaryHostID string) error {
	id, err := requireUUID("volume", volumeID)
	if err != nil {
		return err
	}
	// nullUUID, not requireUUID: empty is the clear, and it is the whole point of this
	// method. A malformed id is still refused rather than coerced to NULL — that
	// coercion would silently detach the volume the operator meant to place.
	host, err := nullUUID("primary host", primaryHostID)
	if err != nil {
		return err
	}
	state := metadata.PlacedState(primaryHostID)
	rows, err := s.q.SetVolumePrimaryHost(ctx, db.SetVolumePrimaryHostParams{
		VolumeID: id, PrimaryHostID: host, Term: term,
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
	if v.PrimaryHostID != "" && primaryHostID != "" && v.PrimaryHostID != primaryHostID {
		return fmt.Errorf("%w: volume %s is placed on %s, detach it before placing it on %s",
			metadata.ErrAlreadyPlaced, volumeID, v.PrimaryHostID, primaryHostID)
	}
	if terr := v.State.Transition(state); terr != nil {
		return terr
	}
	// Every diagnosis above was made from a read taken after the write, so a volume
	// that has since been placed or moved leaves none of them true. Saying so is
	// better than returning nil, which would tell the caller the placement it asked
	// for is in the catalog when something else is.
	return fmt.Errorf("%w: volume %s was placed concurrently", metadata.ErrAlreadyPlaced, volumeID)
}

// ClearVolumeParent records that a volume descends from nothing any more.
// metadata.Store carries why the column could not be written before this existed;
// volumes.sql carries why chain_depth moves in the same statement.
func (s *Store) ClearVolumeParent(ctx context.Context, term int64, volumeID string) error {
	id, err := requireUUID("volume", volumeID)
	if err != nil {
		return err
	}
	rows, err := s.q.ClearVolumeParent(ctx, db.ClearVolumeParentParams{VolumeID: id, Term: term})
	ok, err := s.wrote(ctx, term, rows, err)
	if err != nil || ok {
		return err
	}
	// The statement's only predicates are the id and the term, and `wrote` has already
	// ruled the term out, so nothing else can have matched no row.
	return metadata.ErrNotFound
}

// DeleteVolume removes a volume and its snapshots. metadata.Store carries why the row
// goes rather than entering a DELETING state, and volumes.sql carries why one
// statement does both tables.
func (s *Store) DeleteVolume(ctx context.Context, term int64, volumeID string) error {
	id, err := requireUUID("volume", volumeID)
	if err != nil {
		return err
	}
	rows, err := s.q.DeleteVolume(ctx, db.DeleteVolumeParams{VolumeID: id, Term: term})
	ok, err := s.wrote(ctx, term, rows, err)
	if err != nil || ok {
		return err
	}
	// Three predicates could have matched nothing and `wrote` has ruled out the term,
	// so the diagnosis is a read: the row is gone, or something still descends from
	// its snapshots. Taken in that order because "not found" is what a re-run of a
	// completed delete must be told.
	if _, gerr := s.GetVolume(ctx, volumeID); gerr != nil {
		return gerr
	}
	return fmt.Errorf("%w: %s", metadata.ErrHasDescendants, volumeID)
}

func (s *Store) UpdateWatermarks(ctx context.Context, term int64, volumeID string, local, durable, published int64) error {
	id, err := requireUUID("volume", volumeID)
	if err != nil {
		return err
	}
	if err := metadata.CheckWatermarkOrder(local, durable, published); err != nil {
		return err
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

// SetVolumeRefusal records, or clears, why the volume's host is not serving it.
//
// The 0-row path is the one that differs from every other write here, and volumes.sql
// says why: the statement is qualified by the reporting host and epoch, so no rows means
// the volume has moved on and this report's opinion about whether it is being served is
// void. That is not an error and must not be diagnosed as one — only a volume that is
// not in the catalog at all is. So the re-read is a bare existence check rather than the
// four-way diagnosis SetVolumePrimaryHost does.
func (s *Store) SetVolumeRefusal(ctx context.Context, term int64, volumeID, hostID string, epoch int64,
	refusal lifecycle.Refusal, detail string,
) error {
	id, err := requireUUID("volume", volumeID)
	if err != nil {
		return err
	}
	host, err := requireUUID("host", hostID)
	if err != nil {
		return err
	}
	if !refusal.Valid() {
		return fmt.Errorf("%w: volume refusal %q", lifecycle.ErrUnknownState, refusal)
	}
	rows, err := s.q.SetVolumeRefusal(ctx, db.SetVolumeRefusalParams{
		VolumeID: id, HostID: host, Epoch: epoch, Term: term,
		Refusal: refusal.String(), RefusalDetail: detail,
	})
	ok, err := s.wrote(ctx, term, rows, err)
	if err != nil || ok {
		return err
	}
	if _, gerr := s.GetVolume(ctx, volumeID); gerr != nil {
		return gerr
	}
	return nil
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
	return snapshotFromRow(snap)
}

// ListPendingSnapshots returns the snapshots the host must take, oldest first.
func (s *Store) ListPendingSnapshots(ctx context.Context, hostID string) ([]metadata.Snapshot, error) {
	id, err := requireUUID("host", hostID)
	if err != nil {
		return nil, err
	}
	rows, err := s.q.ListPendingSnapshots(ctx, pgtype.UUID{Bytes: id, Valid: true})
	if err != nil {
		return nil, err
	}
	snaps := make([]metadata.Snapshot, 0, len(rows))
	for _, row := range rows {
		snap, err := snapshotFromRow(&row.Snapshot)
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, snap)
	}
	return snaps, nil
}

// ListUnfinishedSnapshots returns the snapshots nothing has closed out, fleet-wide.
// The state set is lifecycle's, handed to the query, so the §19 vocabulary is not
// copied into SQL.
func (s *Store) ListUnfinishedSnapshots(ctx context.Context) ([]metadata.Snapshot, error) {
	rows, err := s.q.ListUnfinishedSnapshots(ctx, lifecycle.UnfinishedSnapshotStateNames())
	if err != nil {
		return nil, err
	}
	snaps := make([]metadata.Snapshot, 0, len(rows))
	for _, row := range rows {
		snap, err := snapshotFromRow(row)
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, snap)
	}
	return snaps, nil
}

// PublishSnapshot records what the host that took the snapshot observed.
func (s *Store) PublishSnapshot(ctx context.Context, term int64, snapshotID string, targetSequence int64, sourceHostID, manifestKey string) error {
	id, err := requireUUID("snapshot", snapshotID)
	if err != nil {
		return err
	}
	source, err := nullUUID("source host", sourceHostID)
	if err != nil {
		return err
	}
	rows, err := s.q.PublishSnapshot(ctx, db.PublishSnapshotParams{
		SnapshotID: id, TargetSequence: targetSequence, SourceHostID: source,
		ManifestKey: text(manifestKey), Term: term,
	})
	ok, err := s.wrote(ctx, term, rows, err)
	if err != nil || ok {
		return err
	}
	// A current term that wrote nothing means the snapshot is not CREATING. The two
	// cases differ to an operator: a second report of the same publication is the
	// convergence working, and anything else is a transition nobody may make (INV-16).
	snap, gerr := s.GetSnapshot(ctx, snapshotID)
	if gerr != nil {
		return gerr
	}
	if snap.State == lifecycle.SnapshotPublished && snap.TargetSequence == targetSequence {
		return nil
	}
	return fmt.Errorf("%w: snapshot %s is %s, not CREATING", lifecycle.ErrInvalidTransition, snapshotID, snap.State)
}

func snapshotFromRow(snap *db.Snapshot) (metadata.Snapshot, error) {
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
