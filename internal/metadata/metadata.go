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

	"github.com/spin-stack/storage/internal/lifecycle"
)

// Sentinel errors.
var (
	// ErrStaleTerm means the caller's CP term is not the current leader term (§7).
	ErrStaleTerm = errors.New("metadata: stale control-plane term")
	// ErrNotFound means the row does not exist.
	ErrNotFound = errors.New("metadata: not found")
	// ErrShrinkNotAllowed means a resize tried to reduce a volume's size (§3 non-goal).
	ErrShrinkNotAllowed = errors.New("metadata: volume shrink not allowed")
	// ErrCapacityUnderflow means a capacity release would drive a host's committed
	// bytes below zero — an accounting bug, never silently clamped (§28.2).
	ErrCapacityUnderflow = errors.New("metadata: committed capacity would go negative")
	// ErrCapacityExceeded means a reservation would take a host past the placement
	// bound it was offered (§28.2). It is the opposite incident to
	// ErrCapacityUnderflow — bytes that were never released against bytes that were
	// never available — and callers branch differently on the two: an over-commit
	// re-places the volume somewhere else, an underflow is an accounting bug that
	// must stop the operation and be looked at.
	ErrCapacityExceeded = errors.New("metadata: committed capacity would exceed the placement bound")
	// ErrCapacityConflict means the ledger was not at the value the change was
	// conditional on: somebody else wrote the same books between the caller's read
	// and its write. Nothing was applied.
	ErrCapacityConflict = errors.New("metadata: committed capacity is not the expected value")
	// ErrWatermarkOrder means a watermark report violates
	// published ≤ durable ≤ local (INV-03).
	ErrWatermarkOrder = errors.New("metadata: watermarks out of order")
	// ErrInvalidID means an identifier is not usable as a key — empty, or (in an
	// implementation that constrains identifier syntax) malformed. It is never
	// coerced to NULL or to a row nobody can find again.
	ErrInvalidID = errors.New("metadata: invalid identifier")
	// ErrEpochConflict means a volume was not at the epoch the caller compared
	// against: another promoter got there first (§12.3). The caller must re-read and
	// decide again — it has *not* been granted an epoch.
	ErrEpochConflict = errors.New("metadata: volume is not at the expected epoch")
	// ErrHostNotServing means the operation needs a host the fleet still considers a
	// writer, and this one is DEAD (§28.1). Marking a host dead is the Control Plane
	// asserting that its writer is gone; handing it a fresh lease afterwards
	// contradicts that assertion.
	ErrHostNotServing = errors.New("metadata: host is not serving")
)

// The lifecycle vocabularies (host/volume/snapshot/operation states, §7/§19/§28.1)
// live in internal/lifecycle: they are typed, so a state from the wrong vocabulary
// does not compile, and every mutation below is guarded by their transition tables.

// Leader is the single-active Control Plane record (§7).
type Leader struct {
	Term      int64
	HolderID  string
	RenewedAt time.Time
}

// Host is a compute host (§8).
type Host struct {
	HostID             string
	State              lifecycle.HostState
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

// CapacityChange is one change to a host's committed-NVMe ledger (§28.2). It is a
// bounded compare-and-set rather than a bare delta, because both rules that govern
// the ledger used to live between a read and a write that were two statements:
//
//   - Limit is the §28.2 oversubscription bound, as the placement policy computes it
//     for this host (placement.Policy.Limit) — the highest committed value the host
//     may hold *after a reservation*. placement.Choose is pure and advisory: two
//     operations that read the fleet before either reserved anything pick the same
//     destination and both commit, and the host lands past the declared bound with
//     neither caller having made a mistake. Re-checking in Go only narrows that
//     window; the bound is a bound only when the statement that adds the bytes
//     evaluates it. The caller passes the policy's own answer so there is one copy
//     of the rule.
//
//     It deliberately does not bound a *release*: a host that is already above the
//     bound — its policy was tightened, its device came back smaller — must still be
//     able to give bytes back, and a release that bounced off the bound would wedge
//     every drain of that host.
//
//   - Expect, when set, is the ledger value the change is conditional on. A delta is
//     not an idempotency key, so the only proof a resumed operation has that its own
//     change already landed is the value it recorded before attempting it — and that
//     proof is worth nothing if a third party can write between the read and the
//     write. With the comparison inside the statement, a ledger that moved is
//     ErrCapacityConflict and nothing is applied.
type CapacityChange struct {
	// DeltaBytes is added to the host's committed bytes; negative releases.
	DeltaBytes int64
	// Limit bounds a reservation (DeltaBytes > 0) and nothing else. Zero admits no
	// reservation at all, which is the fail-closed direction: a caller that cannot
	// name a bound has not been told the host can hold anything.
	Limit int64
	// Expect, when non-nil, is the committed value the change is conditional on.
	Expect *int64
}

// Expecting returns c conditional on the host's committed bytes being committed.
func (c CapacityChange) Expecting(committed int64) CapacityChange {
	c.Expect = &committed
	return c
}

// Volume is the durable-volume record (§8). Watermarks are informative (§5.8).
type Volume struct {
	VolumeID          string
	SizeBytes         int64
	Durability        lifecycle.Durability // §14.8
	BlockSize         int32
	CurrentEpoch      int64
	State             lifecycle.VolumeState
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
	State            lifecycle.SnapshotState
	ManifestKey      string
	RequestID        string
}

// Operation is a reconciliation operation, idempotent by OperationID (§7, §18).
type Operation struct {
	OperationID  string
	Kind         lifecycle.OperationKind
	VolumeID     string
	HostID       string
	DesiredState []byte // JSON
	CurrentState []byte // JSON
	Phase        lifecycle.OperationPhase
	Error        string
}

// Store is the Control Plane metadata authority. Every implementation answers the
// same way; the shared contract lives in metadata/metadatatest and runs against
// both (sim in the unit lane, pg in the integration lane). In summary:
//
//   - Arguments are validated first: an empty identifier is ErrInvalidID, and a
//     value outside a lifecycle vocabulary is lifecycle.ErrUnknownState. Neither is
//     a question about leadership.
//   - Then the term. Every mutation validates the caller's CP term (§7); a stale
//     term — including term 0, before any election — is ErrStaleTerm, and it wins
//     over every other diagnosis. A zombie CP must learn that it is a zombie rather
//     than be told its resize was a shrink.
//   - Then existence: a mutation naming a row that is not there is ErrNotFound.
//   - Then the domain guard: lifecycle.ErrInvalidTransition, ErrShrinkNotAllowed,
//     ErrCapacityUnderflow, ErrWatermarkOrder.
type Store interface {
	// AcquireLeadership takes/renews leadership, incrementing and returning the term.
	AcquireLeadership(ctx context.Context, holderID string) (int64, error)
	// GetLeader returns the current leader record.
	GetLeader(ctx context.Context) (Leader, error)
	// Now returns the store's own clock: the one that stamps last_renewal, and so
	// the one every fencing deadline is derived from (§12.1). A Control Plane that
	// compares those stamps against its own wall clock shortens the fencing wait by
	// exactly the offset between the two — an NTP correction, a VM restored from a
	// snapshot, a bad RTC — and grants an epoch while the old writer's monotonic
	// lease is still valid. Reading the deadline's own clock removes the comparison.
	Now(ctx context.Context) (time.Time, error)

	// UpsertHost registers a host or refreshes what the host itself reports:
	// agent version, format version, NVMe totals, heartbeat. It deliberately does
	// NOT carry the fleet state or the committed-capacity ledger — those belong to
	// the Control Plane (SetHostState, CommitHostCapacity), and a routine heartbeat
	// that carried them would un-cordon a draining host and zero its ledger.
	// Term-guarded.
	UpsertHost(ctx context.Context, term int64, h Host) error
	// GetHost returns a host.
	GetHost(ctx context.Context, hostID string) (Host, error)
	// ListHosts returns every host ordered by host id (deterministic, INV-02).
	ListHosts(ctx context.Context) ([]Host, error)
	// SetHostState transitions a host's fleet state (term-guarded, §28.1). The move
	// is checked against the lifecycle table: an unknown value is
	// lifecycle.ErrUnknownState, an illegal move lifecycle.ErrInvalidTransition.
	SetHostState(ctx context.Context, term int64, hostID string, state lifecycle.HostState) error
	// CommitHostCapacity applies c to a host's committed NVMe ledger, term-guarded.
	// Every rule the ledger has is evaluated inside the write (see CapacityChange):
	// a release below zero is ErrCapacityUnderflow rather than a clamp (§28.2), a
	// reservation past c.Limit is ErrCapacityExceeded, and a change conditional on a
	// value the ledger has moved away from is ErrCapacityConflict. In all three cases
	// nothing is written.
	CommitHostCapacity(ctx context.Context, term int64, hostID string, c CapacityChange) error
	// RenewHostLease renews (or grants) a host's lease with the given TTL
	// (term-guarded). A host the fleet has recorded as DEAD is refused with
	// ErrHostNotServing: that state is the Control Plane asserting the writer is
	// gone — the same assertion promotion accepts as a reason to skip the fencing
	// wait — so a routine heartbeat must not be able to re-arm it. A CORDONED or
	// DRAINING host still renews: both are still serving the volumes they hold, and
	// stopping their ACKs mid-evacuation is the failure this would cause.
	RenewHostLease(ctx context.Context, term int64, hostID string, ttlSeconds int) error
	// RevokeHostLease drops a host's lease (term-guarded), so nothing keeps its
	// Agent-side lease alive once the Control Plane has fenced it. It is idempotent:
	// revoking a lease that is not there is the state the caller asked for. An
	// unregistered host is ErrNotFound.
	//
	// Revoking does *not* shorten a fencing wait. The Agent counts its own lease down
	// on a monotonic clock (§12.2) and never learns that the row is gone, so a
	// promotion still waits out last_renewal + lease_ttl + max_clock_skew.
	RevokeHostLease(ctx context.Context, term int64, hostID string) error
	// GetHostLease returns a host's lease.
	GetHostLease(ctx context.Context, hostID string) (HostLease, error)

	// CreateVolume inserts a volume (term-guarded). It is idempotent and never
	// destructive: for an id that already exists it converges instead of aborting —
	// two operators running rebuild-metadata at once must both finish — but it never
	// lowers current_epoch, shrinks size_bytes, rewinds a watermark, blanks an
	// owner, or rewrites the lifecycle state. Ownership and state move only through
	// BumpVolumeEpoch and SetVolumeState.
	CreateVolume(ctx context.Context, term int64, v Volume) error
	// GetVolume returns a volume.
	GetVolume(ctx context.Context, volumeID string) (Volume, error)
	// ListVolumesByHost returns the volumes whose primary is hostID, ordered by
	// volume id — what a drain iterates over (§28.1).
	ListVolumesByHost(ctx context.Context, hostID string) ([]Volume, error)
	// BumpVolumeEpoch advances the epoch to expectedEpoch+1 and sets the primary
	// host, term-guarded, returning the new epoch (§12.3). It is a compare-and-set,
	// not an increment: a promotion chooses which epoch to grant by reading the
	// volume first, and expectedEpoch is what it read. A volume that has moved on
	// since is ErrEpochConflict and nothing is written — otherwise each of n racing
	// promoters burns an epoch and the last one writes its own host into
	// primary_host_id, naming an owner that never won the S3 epoch object and never
	// got a lease.
	BumpVolumeEpoch(ctx context.Context, term int64, volumeID, primaryHostID string, expectedEpoch int64) (int64, error)
	// UpdateWatermarks lazily updates the informative watermarks (term-guarded).
	// A report violating published ≤ durable ≤ local is ErrWatermarkOrder (INV-03).
	// A report that is merely *late* — an epoch-N primary's, delivered after epoch
	// N+1 published its own — is not an error and is not applied: each watermark is
	// monotonic, because promotion does not change the CP term and this number is
	// what an operator reads during an incident.
	UpdateWatermarks(ctx context.Context, term int64, volumeID string, local, durable, published int64) error
	// ResizeVolume grows size_bytes (term-guarded); shrink is rejected (§3 non-goal).
	ResizeVolume(ctx context.Context, term int64, volumeID string, newSizeBytes int64) error
	// SetVolumeState moves a volume through the §7 ownership machine (term-guarded).
	// The move is guarded by the lifecycle table inside the write itself, so two
	// Control Planes reacting to the same suspicion cannot both win.
	SetVolumeState(ctx context.Context, term int64, volumeID string, state lifecycle.VolumeState) error

	// CreateSnapshot records a snapshot (term-guarded, §19). Like CreateVolume it is
	// idempotent, but a snapshot is immutable (INV-16): re-recording an existing id
	// is a no-op, never an overwrite.
	CreateSnapshot(ctx context.Context, term int64, s Snapshot) error
	// GetSnapshot returns a snapshot by id.
	GetSnapshot(ctx context.Context, snapshotID string) (Snapshot, error)
	// SetSnapshotState moves a snapshot through the §19 lifecycle (term-guarded,
	// transition-guarded in the write). Without it a snapshot whose publication
	// crashed stays CREATING forever and the catalog side of GC never sees it.
	SetSnapshotState(ctx context.Context, term int64, snapshotID string, state lifecycle.SnapshotState) error

	// RecordOperation records an admin operation idempotently (term-guarded, §7/§18);
	// recorded is false if the operation_id already existed (a duplicate request).
	RecordOperation(ctx context.Context, term int64, op Operation) (recorded bool, err error)
	// GetOperation returns a recorded operation.
	GetOperation(ctx context.Context, operationID string) (Operation, error)
	// UpdateOperation stores an operation's phase, current state, and error — the
	// visible progress of a long-running reconciled operation (§7, §28.1). It is
	// term-guarded, and the phase move is guarded by the lifecycle table, so a
	// terminal operation is never resurrected (lifecycle.ErrInvalidTransition).
	UpdateOperation(ctx context.Context, term int64, op Operation) error
}
