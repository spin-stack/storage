// Package metadata is the Control Plane's authority for leases, epochs, ownership,
// snapshots, hosts/capacity, reconciliation operations, and CP terms (§7, §8). It is
// NOT the authority for the durable point of data (that is S3, §5.8).
//
// It is reached through a Store interface with two implementations (ADR-0006):
// metadata/sim (in-memory, deterministic — for DST fencing proofs under partitions
// and clock drift) and metadata/pg (a thin adapter over sqlc-generated queries on
// pgx/v5, verified by TestContainers). Every mutating operation is guarded by the
// Control Plane term; a zombie CP affects 0 rows and gets ErrStaleTerm.
package metadata

import (
	"context"
	"errors"
	"time"
)

// Sentinel errors.
var (
	// ErrStaleTerm means the caller's CP term is not the current leader term (§7).
	ErrStaleTerm = errors.New("metadata: stale control-plane term")
	// ErrNotFound means the row does not exist.
	ErrNotFound = errors.New("metadata: not found")
	// ErrShrinkNotAllowed means a resize tried to reduce a volume's size (§3 non-goal).
	ErrShrinkNotAllowed = errors.New("metadata: volume shrink not allowed")
)

// Leader is the single-active Control Plane record (§7).
type Leader struct {
	Term      int64
	HolderID  string
	RenewedAt time.Time
}

// Host is a compute host (§8).
type Host struct {
	HostID             string
	State              string // ACTIVE | CORDONED | DRAINING | DEAD
	AgentVersion       string
	MaxFormatVersion   int32
	NVMeTotalBytes     int64
	NVMeUsedBytes      int64
	NVMeCommittedBytes int64
	LastHeartbeat      time.Time
}

// HostLease is the per-host lease (§12.6).
type HostLease struct {
	HostID      string
	GrantedAt   time.Time
	LastRenewal time.Time
	TTLSeconds  int32
}

// Volume is the durable-volume record (§8). Watermarks are informative (§5.8).
type Volume struct {
	VolumeID          string
	SizeBytes         int64
	Durability        string // remote | local (§14.8)
	BlockSize         int32
	CurrentEpoch      int64
	State             string
	PrimaryHostID     string
	StandbyHostID     string
	ChainDepth        int32
	DEKWrapped        []byte
	KEKID             string
	LocalSequence     int64
	DurableSequence   int64
	PublishedSequence int64
}

// Snapshot is a catalog entry for a published snapshot (§8, §19).
type Snapshot struct {
	SnapshotID       string
	VolumeID         string
	ParentSnapshotID string
	Epoch            int64
	TargetSequence   int64
	RootDigest       string
	SourceHostID     string
	State            string
	ManifestKey      string
	RequestID        string
}

// Operation is a reconciliation operation, idempotent by OperationID (§7, §18).
type Operation struct {
	OperationID  string
	Kind         string
	VolumeID     string
	HostID       string
	DesiredState []byte // JSON
	CurrentState []byte // JSON
	Phase        string
	Error        string
}

// Store is the Control Plane metadata authority. Mutations take the caller's CP
// term and return ErrStaleTerm if it is not current.
type Store interface {
	// AcquireLeadership takes/renews leadership, incrementing and returning the term.
	AcquireLeadership(ctx context.Context, holderID string) (int64, error)
	// GetLeader returns the current leader record.
	GetLeader(ctx context.Context) (Leader, error)

	// UpsertHost registers or updates a host (term-guarded).
	UpsertHost(ctx context.Context, term int64, h Host) error
	// GetHost returns a host.
	GetHost(ctx context.Context, hostID string) (Host, error)
	// RenewHostLease renews (or grants) a host's lease with the given TTL (term-guarded).
	RenewHostLease(ctx context.Context, term int64, hostID string, ttlSeconds int) error
	// GetHostLease returns a host's lease.
	GetHostLease(ctx context.Context, hostID string) (HostLease, error)

	// CreateVolume inserts a volume (term-guarded).
	CreateVolume(ctx context.Context, term int64, v Volume) error
	// GetVolume returns a volume.
	GetVolume(ctx context.Context, volumeID string) (Volume, error)
	// BumpVolumeEpoch increments the epoch and sets the primary host (term-guarded),
	// returning the new epoch (§12.3).
	BumpVolumeEpoch(ctx context.Context, term int64, volumeID, primaryHostID string) (int64, error)
	// UpdateWatermarks lazily updates the informative watermarks (term-guarded).
	UpdateWatermarks(ctx context.Context, term int64, volumeID string, local, durable, published int64) error
	// ResizeVolume grows size_bytes (term-guarded); shrink is rejected (§3 non-goal).
	ResizeVolume(ctx context.Context, term int64, volumeID string, newSizeBytes int64) error

	// CreateSnapshot records a published snapshot (term-guarded, §19).
	CreateSnapshot(ctx context.Context, term int64, s Snapshot) error
	// GetSnapshot returns a snapshot by id.
	GetSnapshot(ctx context.Context, snapshotID string) (Snapshot, error)

	// RecordOperation records an admin operation idempotently; recorded is false if
	// the operation_id already existed (a duplicate request, §18).
	RecordOperation(ctx context.Context, op Operation) (recorded bool, err error)
	// GetOperation returns a recorded operation.
	GetOperation(ctx context.Context, operationID string) (Operation, error)
}
