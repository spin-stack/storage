// Package metadata is the Control Plane's authority for leases, epochs, ownership,
// snapshots, hosts/capacity, reconciliation operations, and CP terms (§7, §8). It is
// NOT the authority for the durable point of data (that is S3, §5.8).
//
// It is reached through a Store interface with two implementations, and the reason is
// not symmetry:
//
//   - metadata/sim — in-memory and deterministic. The §12 fencing protocol is *proven*
//     here, under injected partitions and clock drift, and a real PostgreSQL cannot be
//     deterministic under either. That is the whole constraint: the DST harness must be
//     able to prove fencing without Docker, on any machine, reproducibly from a seed.
//   - metadata/pg — the production path: a thin adapter over sqlc-generated queries on
//     pgx/v5, verified against a real PostgreSQL 18 by TestContainers.
//
// Rejected: one real PostgreSQL serving both. It would make the fencing proof
// non-reproducible, which is the single property that proof exists to have.
//
// Every mutating operation is guarded by the Control Plane term; a zombie CP affects 0
// rows and gets ErrStaleTerm.
package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	// ErrCapacityExceeded means a write would take a host past the §28.2
	// oversubscription bound it was offered: the volume it places, plus what the
	// host already holds and what is already in flight to it, is more than the
	// policy admits. Nothing was written, and the caller re-places the volume
	// somewhere else.
	//
	// It is the only capacity sentinel left. The underflow and expected-value
	// errors it used to sit beside were properties of an incremental ledger —
	// "this delta was applied twice", "the books moved under my read" — and a
	// derived value has neither (ADR-0017).
	ErrCapacityExceeded = errors.New("metadata: committed capacity would exceed the placement bound")
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
	// ErrRenewalsBlocked means a host's lease renewals are refused for as long as a
	// bounded revocation window is open on it (§12.6, ADR-0016 stage 1). The Control
	// Plane opens one for the duration of a single volume's promotion, so that the
	// lease it revoked to fence the source cannot be re-armed by the source's next
	// heartbeat; without it the fencing wait measures from an instant that keeps
	// moving and a healthy host can never be drained.
	//
	// It is deliberately not ErrHostNotServing. The host *is* serving — the volumes
	// nobody is moving are still its, and their WAL keeps accepting writes; what
	// waits is the durable ACK, for at most one lease_ttl + max_clock_skew per volume
	// moved. A caller that reads this should retry, not conclude the host is gone.
	ErrRenewalsBlocked = errors.New("metadata: host lease renewals are blocked by a revocation window")
	// ErrDrainInProgress means the host already has a live drain operation and this
	// one was not recorded (§28.1). Two evacuations of one host each capture their
	// own plan and promote the same volumes; whichever loses a race is left holding
	// a destination reservation nobody will release, because releasing it is the
	// losing operation's own next step and that step now fails for ever (§28.2).
	//
	// The Control Plane refuses this before it writes, and that check is where the
	// useful message comes from — it names the operation that owns the host. This
	// sentinel is the store closing the window the check leaves: a read followed by
	// a write is not exclusion, and two goroutines inside one leader can both pass
	// it. A caller that sees it should reconcile the drain that already exists, not
	// retry its own.
	ErrDrainInProgress = errors.New("metadata: the host already has a live drain operation")
	// ErrHostNotServing means the operation needs a host the fleet still considers a
	// writer, and this one is DEAD (§28.1). Marking a host dead is the Control Plane
	// asserting that its writer is gone; handing it a fresh lease afterwards
	// contradicts that assertion.
	ErrHostNotServing = errors.New("metadata: host is not serving")

	// ErrUnversionedDEK is a volume written with DEKKeyID 0 — see CheckDEKKeyID.
	ErrUnversionedDEK = errors.New("metadata: a volume's DEK must carry a version")
)

// CheckWatermarkOrder returns ErrWatermarkOrder unless published ≤ durable ≤ local
// (INV-03, §5.6). It is the Go half of the CHECK constraint the volumes table
// carries: every store validates the triple before writing it, so the two
// implementations refuse the same input with the same sentinel instead of one of
// them surfacing an integrity error the caller cannot classify.
//
// Only the writes that *set* the triple need it. The ones that advance it take a
// component-wise maximum, and the max of two ordered triples is ordered.
func CheckWatermarkOrder(local, durable, published int64) error {
	if published > durable || durable > local {
		return fmt.Errorf("%w: published=%d durable=%d local=%d",
			ErrWatermarkOrder, published, durable, local)
	}
	return nil
}

// CheckDEKKeyID returns ErrUnversionedDEK unless keyID is a real DEK version. It is
// the Go half of volumes.dek_key_id's CHECK, for the same reason CheckWatermarkOrder
// exists: both stores must refuse the same input with the same sentinel.
//
// Zero is the whole rule. It is not "unset" — on the WAL path KeyID 0 means *this
// record is plaintext* (§14.1), so a volume row carrying 0 describes a key the Agent
// must refuse at attach (wal.ErrUnversionedKey), after the KMS call, with the DEK
// already in memory. Refusing it at the write refuses it while it can still be fixed.
func CheckDEKKeyID(keyID uint32) error {
	if keyID == 0 {
		return fmt.Errorf("%w: 0 is the plaintext marker, not a DEK version (§15.1)", ErrUnversionedDEK)
	}
	return nil
}

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
	HostID           string
	State            lifecycle.HostState
	AgentVersion     string
	MaxFormatVersion int32
	NVMeTotalBytes   int64
	NVMeUsedBytes    int64
	// RemoteBacklogBytes is the share of NVMeUsedBytes that no verified object
	// covers yet, summed over every volume the host holds (ADR-0013 §1). Like the
	// two fields above it, the host reports it and a heartbeat writes it.
	//
	// It is stored rather than derived — the opposite call from NVMeCommittedBytes
	// below — because nothing else here can produce it: the distance between what a
	// host has written and what S3 has acknowledged is measured in bytes on that
	// host, while the catalog holds watermarks in sequence numbers. It is also the
	// number that tells a busy host from a host whose object store has stopped
	// answering: only the second one keeps growing, because no local truncation may
	// reclaim records that exist nowhere else (INV-13).
	RemoteBacklogBytes int64
	// NVMeCommittedBytes is §28.2 committed capacity. It is *derived*, computed by
	// the store on every read, and never stored anywhere (ADR-0017):
	//
	//	committed(host) = Σ size_bytes of the volumes whose primary is host
	//	                + Σ size_bytes reserved by in-flight operation plans
	//	                  targeting host
	//
	// It is therefore ignored on the way in: UpsertHost cannot set it, and neither
	// can anything else. A number S3 or SQL can recompute is a cache, never an
	// authority, and the cheapest cache to keep honest is the one that does not
	// exist.
	NVMeCommittedBytes int64
	LastHeartbeat      time.Time
	// RenewalsBlockedUntil is the end of the bounded revocation window (ADR-0016
	// stage 1): until this instant, on the store's clock, RenewHostLease refuses.
	// Zero means the host renews normally.
	RenewalsBlockedUntil time.Time
}

// HostLease is the per-host lease (§12.6).
type HostLease struct {
	HostID      string
	GrantedAt   time.Time
	LastRenewal time.Time
	TTLSeconds  int32
}

// CapacityBound is the §28.2 oversubscription ceiling a write must respect, stated
// by the caller that took the placement decision so there is one copy of the rule.
//
// placement.Choose is pure and advisory: two operations that read the fleet before
// either reserved anything pick the same destination and both proceed, and the host
// lands past the declared bound with neither caller having made a mistake.
// Re-checking in Go only narrows that window; the bound is a bound only when the
// statement that places the bytes evaluates it. So it travels *with* the write:
//
//	committed(HostID) + AddBytes <= Limit
//
// where committed is the derived value (ADR-0017) as it stands immediately before
// the write. A write that carries no bound is not a placement decision —
// rebuild-metadata recreating volumes that already occupy their hosts, a progress
// save that reserves nothing new — and a bound is never applied to a write that
// gives capacity back: a host can be over its ceiling for reasons that have nothing
// to do with the caller (a tightened policy, a device that came back smaller), and
// refusing the write that brings it down would wedge every drain of that host.
type CapacityBound struct {
	// HostID is the host being placed on.
	HostID string
	// AddBytes is what this write places there. Zero is legitimate: a write may be
	// bounded without adding anything, which asserts the host is not already over.
	AddBytes int64
	// Limit is the highest committed value the host may hold afterwards
	// (placement.Policy.Limit). Zero admits nothing, which is the fail-closed
	// direction: a caller that cannot name a bound has not been told the host can
	// hold anything.
	Limit int64
}

// PlanReservation is one entry of the "volumes" array an operation records in its
// current_state: a volume this operation has committed to place on ToHost. It is the
// second term of ADR-0017's derived capacity — "reserved but not yet primary" — and
// it is declared here, next to CapacityBound, because three things have to agree on
// it: the drain that writes it, the sim store that sums it in Go, and the
// host_committed_bytes view that sums it in SQL. A shape that lived only in
// internal/controlplane would be a shape the accounting had to guess at.
//
// It is decoded leniently. A plan nobody can read reserves nothing, which makes the
// destination look emptier than it is (ADR-0017 says so explicitly) — but a plan
// that made every capacity read fail would take the fleet down instead.
type PlanReservation struct {
	VolumeID string `json:"volume_id"`
	ToHost   string `json:"to_host"`
	Stage    string `json:"stage"`
}

// settledStages are the stages of a plan entry that reserve nothing: the move is
// finished, or it turned out to belong to somebody else.
var settledStages = map[string]bool{"DONE": true, "FOREIGN": true}

// Reserves reports whether this entry still charges its destination.
func (r PlanReservation) Reserves() bool {
	return r.ToHost != "" && r.VolumeID != "" && !settledStages[r.Stage]
}

// PlanReservations decodes the reservation entries of an operation's current_state.
func PlanReservations(currentState []byte) []PlanReservation {
	var plan struct {
		Volumes []PlanReservation `json:"volumes"`
	}
	if err := json.Unmarshal(currentState, &plan); err != nil {
		return nil
	}
	return plan.Volumes
}

// Volume is the durable-volume record (§8). Watermarks are informative (§5.8).
type Volume struct {
	VolumeID      string
	SizeBytes     int64
	Durability    lifecycle.Durability // §14.8
	BlockSize     int32
	CurrentEpoch  int64
	State         lifecycle.VolumeState
	PrimaryHostID string
	StandbyHostID string
	ChainDepth    int32
	// ParentSnapshotID is the snapshot this volume was cloned from (§20), empty for a
	// volume that was created rather than cloned. ChainDepth says a chain exists; this
	// says what is on the other end of it, which is what the clone's Agent needs to
	// find the objects it reads through.
	ParentSnapshotID string
	// ParentVolumeID is the volume that snapshot belongs to. It is derivable — it is
	// snapshots.volume_id — and it is carried anyway, because the Agent is a thing that
	// is *told* (ADR-0021) and cannot perform the second lookup itself. It is not a
	// column and no store populates it: the one place that needs it — cpserver, when
	// it builds the desired state — reads it from the snapshot row, so it cannot
	// disagree with the snapshot it names.
	ParentVolumeID string
	DEKWrapped     []byte
	KEKID          string
	// DEKKeyID is the DEK's own version — RecordHeader.KeyID (§15.1), the field that
	// lets rotation re-key new data without re-encrypting history. It travels with
	// DEKWrapped because a wrapped key and another key's version describe a volume
	// nothing can open. Zero is not a version: it is the WAL's plaintext marker, and
	// wal.NewEncryption refuses it.
	DEKKeyID          uint32
	LocalSequence     int64
	DurableSequence   int64
	PublishedSequence int64
	// FencingStartedAt is the instant the Control Plane observed the lease of the
	// writer it is fencing, stamped by the store's own clock when the volume entered
	// FENCING_WAIT (§7, ADR-0015). It is the durable half of the promotion dwell: a
	// Control Plane that restarts mid-fence has no memory of having observed
	// anything, and without this would have to start the wait again.
	//
	// Zero means "no fence is running, or nobody recorded one", which a promoter
	// answers by starting a full dwell now. Fail slow, never short.
	FencingStartedAt time.Time
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
	// agent version, format version, NVMe totals, remote backlog, heartbeat. It
	// deliberately does NOT carry the fleet state — that belongs to the Control
	// Plane (SetHostState),
	// and a routine heartbeat that carried it would un-cordon a draining host.
	// Committed capacity is not carried either, and could not be: it is derived
	// from the volumes and plans that name the host (ADR-0017). Term-guarded.
	UpsertHost(ctx context.Context, term int64, h Host) error
	// GetHost returns a host.
	GetHost(ctx context.Context, hostID string) (Host, error)
	// ListHosts returns every host ordered by host id (deterministic, INV-02).
	ListHosts(ctx context.Context) ([]Host, error)
	// SetHostState transitions a host's fleet state (term-guarded, §28.1). The move
	// is checked against the lifecycle table: an unknown value is
	// lifecycle.ErrUnknownState, an illegal move lifecycle.ErrInvalidTransition.
	SetHostState(ctx context.Context, term int64, hostID string, state lifecycle.HostState) error
	// RenewHostLease renews (or grants) a host's lease with the given TTL
	// (term-guarded). A host the fleet has recorded as DEAD is refused with
	// ErrHostNotServing: that state is the Control Plane asserting the writer is
	// gone — the same assertion promotion accepts as a reason to skip the fencing
	// wait — so a routine heartbeat must not be able to re-arm it. A CORDONED or
	// DRAINING host still renews: both are still serving the volumes they hold, and
	// stopping their ACKs for the whole evacuation is the failure that would cause.
	//
	// It is also refused, with ErrRenewalsBlocked, while a revocation window is open
	// on the host (ADR-0016 stage 1) — the bounded version of that same refusal, for
	// the length of one volume's promotion.
	RenewHostLease(ctx context.Context, term int64, hostID string, ttlSeconds int) error
	// BlockHostRenewals opens (or re-arms) the revocation window on hostID for d
	// (term-guarded, §12.6/ADR-0016). While it is open the host's renewals are
	// ErrRenewalsBlocked, so the lease the Control Plane revoked to fence one of its
	// volumes cannot be put back by the host's next heartbeat.
	//
	// The window carries its own deadline rather than being a flag, because the
	// Control Plane that opened it may not survive to close it: a host that can never
	// renew again is worse than the bug the window fixes. d is therefore the length
	// of one promotion — one lease_ttl + max_clock_skew — and a pass that is still
	// running re-arms it rather than relying on the first call.
	BlockHostRenewals(ctx context.Context, term int64, hostID string, d time.Duration) error
	// UnblockHostRenewals closes the window (term-guarded). It is idempotent: a host
	// with no window is the state the caller asked for, which matters because this
	// runs on every exit path of a promotion, including the ones that never opened
	// one.
	UnblockHostRenewals(ctx context.Context, term int64, hostID string) error
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
	//
	// A volume with a primary host is a placement, so this is one of the two writes
	// that carry the §28.2 bound (§28.2, ADR-0017): a non-nil bound makes the
	// insert affect 0 rows — ErrCapacityExceeded, nothing written — when the
	// destination cannot hold it. rebuild-metadata passes none: it is recording
	// volumes that already occupy their hosts, and a ceiling that refused to record
	// reality would leave the catalog short of it.
	CreateVolume(ctx context.Context, term int64, v Volume, bound *CapacityBound) error
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
	//
	// A drain of a host that already has a live one is ErrDrainInProgress and is not
	// recorded (§28.1): two evacuations of one host strand a reservation nobody will
	// release. The refusal is the store's, so a Control Plane that checked first and
	// then wrote — which is not exclusion — cannot end up with two.
	RecordOperation(ctx context.Context, term int64, op Operation) (recorded bool, err error)
	// GetOperation returns a recorded operation.
	GetOperation(ctx context.Context, operationID string) (Operation, error)
	// ListLiveOperationsByHost returns the operations still under way on hostID —
	// every phase but the terminal ones — ordered by operation id (deterministic,
	// INV-02). It is how a reconciler asks what is already happening to a host
	// before starting something else: an operation id is the only handle
	// GetOperation offers, and a second drain arrives with a new one (§7, §28.1).
	//
	// Finished operations are excluded by the store, not by the caller. Nothing
	// deletes them, so the set of operations a host has ever had only grows, and a
	// listing that carried the history would make the question that runs before
	// every drain pass more expensive for the rest of the cluster's life. A caller
	// that wants a specific past operation has its id and GetOperation.
	ListLiveOperationsByHost(ctx context.Context, hostID string) ([]Operation, error)
	// UpdateOperation stores an operation's phase, current state, and error — the
	// visible progress of a long-running reconciled operation (§7, §28.1). It is
	// term-guarded, and the phase move is guarded by the lifecycle table, so a
	// terminal operation is never resurrected (lifecycle.ErrInvalidTransition).
	//
	// It is also the write that records a reservation, because an operation's
	// progress *is* its plan: a drain entry naming a destination charges that host
	// for a volume which is not primary there yet (ADR-0017). So it is the second
	// write carrying the §28.2 bound; a non-nil bound that the derived value cannot
	// admit is ErrCapacityExceeded with nothing written, including the progress.
	UpdateOperation(ctx context.Context, term int64, op Operation, bound *CapacityBound) error
}
