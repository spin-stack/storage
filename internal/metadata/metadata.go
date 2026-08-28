// Package metadata is the Control Plane's authority for leases, epochs, ownership,
// snapshots, hosts/capacity, and CP terms (§7, §8). It is NOT the authority for the
// durable point of data (that is S3, §5.8).
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
	// ErrCapacityExceeded means a write would take a host past the §28.2
	// oversubscription bound it was offered: the volume it places, plus what the
	// host already holds and what is already in flight to it, is more than the
	// policy admits. Nothing was written, and the caller re-places the volume
	// somewhere else.
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
	// ErrHostNotServing means the operation needs a host the fleet still considers a
	// writer, and this one is DEAD (§28.1). Marking a host dead is the Control Plane
	// asserting that its writer is gone; handing it a fresh lease afterwards
	// contradicts that assertion.
	ErrHostNotServing = errors.New("metadata: host is not serving")
	// ErrAlreadyPlaced means SetVolumePrimaryHost was asked to move a volume straight from
	// one host to another. It is refused because a host serves exactly the volumes
	// GetDesiredState lists for it and learns it has lost one on its *next* poll: a single
	// write naming a new owner hands the volume to the destination while the source still
	// serves it, both guests writing and both Agents publishing over the same manifest.
	// The caller detaches first and places afterwards. That is not exclusion either —
	// closing the window is the §7 state machine's job; what this refusal buys is that no
	// *single* catalog write can open it.
	ErrAlreadyPlaced = errors.New("metadata: volume is already placed on another host")

	// ErrUnversionedDEK is a volume written with DEKKeyID 0 — see CheckDEKKeyID.
	ErrUnversionedDEK = errors.New("metadata: a volume's DEK must carry a version")

	// ErrHasDescendants means a volume cannot be removed while another volume — or another
	// volume's snapshot — still descends from one of its snapshots. It is a safety net, not
	// the check an operator meets: `parent_snapshot_id` is write-once, so the catalog keeps
	// naming a parent a FLATTEN has dissolved in the bucket, and the command asks the
	// *bucket* who descends from what. Postgres would refuse the write anyway; the sentinel
	// exists because an integrity violation surfacing as a driver error is one no caller can
	// branch on, and the sim has no foreign keys at all.
	ErrHasDescendants = errors.New("metadata: a volume whose snapshots something still descends from")
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

// PlacedState is the §7 volume state that belongs with an ownership write: a volume with a
// writer is ACTIVE, a volume with none is DETACHED. Both stores must derive it identically,
// and SetVolumePrimaryHost writes state and ownership together rather than in two calls — a
// volume left ACTIVE with no host is one controlplane.RequestSnapshot accepts and no Agent
// can ever take, and one left DETACHED while a host serves it is the catalog saying nobody
// is writing while a guest is.
func PlacedState(primaryHostID string) lifecycle.VolumeState {
	if primaryHostID == "" {
		return lifecycle.VolumeDetached
	}
	return lifecycle.VolumeActive
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
	HostID string
	State  lifecycle.HostState
	// CordonReason is why the host is CORDONED, and empty in every other state
	// (ADR-0013 §3). It is what tells an operator's cordon from the one the Control
	// Plane places when the device passes 70% used, and — read back through
	// lifecycle.CordonReason.MayOverwrite — what stops the automatic loop from
	// clearing the operator's.
	CordonReason     lifecycle.CordonReason
	AgentVersion     string
	MaxFormatVersion int32
	NVMeTotalBytes   int64
	NVMeUsedBytes    int64
	// RemoteBacklogBytes is a byte count the host reports about itself (ADR-0013 §1). Every
	// Agent reports 0 and the column holds 0 fleet-wide: a FLUSH is ACKed on an fdatasync and
	// a volume reaches the object store when it stops, so there is no running distance to
	// measure. Stored rather than derived — the opposite call from NVMeCommittedBytes — because
	// the catalog holds watermarks in sequence numbers, not bytes. Nothing branches on it;
	// removing it is a change to the wire and the schema, which is why it is written down.
	RemoteBacklogBytes int64
	// NVMeCommittedBytes is §28.2 committed capacity. It is *derived*, computed by the store
	// on every read and never stored anywhere (ADR-0017):
	//
	//	committed(host) = Σ size_bytes of the volumes whose primary is host
	//
	// It is therefore ignored on the way in: UpsertHost cannot set it, and neither can
	// anything else. A number SQL can recompute is a cache, never an authority.
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

// CapacityBound is the §28.2 oversubscription ceiling a write must respect, stated by the
// caller that took the placement decision so there is one copy of the rule:
//
//	committed(HostID) + AddBytes <= Limit   and   used(HostID) <= UsedLimit
//
// It travels *with* the write because placement.Choose is pure and advisory: two
// operations that read the fleet before either reserved anything pick the same destination
// and both proceed. A bound is a bound only when the statement that places the bytes
// evaluates it. The second half is the ADR-0013 gap — promises are not what fills a
// device, and under ADR-0026 a session's WAL stays local with no reservation covering it.
// Both are the same decision, taken once by placement.Policy.Bound.
//
// A write with no bound is not a placement decision (rebuild-metadata recreating volumes
// that already occupy their hosts), and no bound is applied to a write that gives capacity
// back — refusing that would wedge every release of a host already over its ceiling.
// CreateVolume is the only write that takes one; SetVolumePrimaryHost takes none, which is
// an open gap and not a decision.
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
	// UsedLimit is the highest *measured* used value the host may already show and still take
	// the write (placement.Policy.UsedLimit). It charges AddBytes nothing: a volume does not
	// occupy its declared size the moment it is placed (placement.Policy.Admits evaluates the
	// same rule). Zero is fail-closed like Limit, and here it bites in production — every real
	// device measures something, so a hand-built bound missing this field refuses everything.
	UsedLimit int64
}

// Volume is the durable-volume record (§8). Watermarks are informative (§5.8).
type Volume struct {
	VolumeID  string
	SizeBytes int64
	BlockSize int32
	// RPOTargetSeconds is how far behind the object store this volume may fall before
	// its host commits on age rather than on size (v6 §11). Zero is no age trigger.
	RPOTargetSeconds int32
	CurrentEpoch     int64
	State            lifecycle.VolumeState
	PrimaryHostID    string
	StandbyHostID    string
	ChainDepth       int32
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
	// Refusal is why the host that holds this volume is not serving it, and RefusalDetail is
	// the sentence the Agent sent with it. RefusalNone — the zero value — is the host saying
	// it is serving. It is a state and not a watermark; SetVolumeRefusal carries the storage
	// rule that follows from that.
	Refusal       lifecycle.Refusal
	RefusalDetail string
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
	// CommitID is the commit this snapshot names, empty until a host has reported one.
	// A snapshot under v6 is a name for a point in the published history and not a copy
	// of anything, so this is the whole of what it points at: the manifest's key is
	// derived from it (commit.ManifestKey), which is why there is no key column to
	// disagree with the bucket.
	CommitID     string
	SourceHostID string
	State        lifecycle.SnapshotState
	RequestID    string
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
//     than be told its epoch bump raced.
//   - Then existence: a mutation naming a row that is not there is ErrNotFound.
//   - Then the domain guard: lifecycle.ErrInvalidTransition, ErrEpochConflict,
//     ErrCapacityExceeded, ErrWatermarkOrder.
//
// A volume's geometry — size_bytes, block_size — is not on that list, because no
// method here changes it. The note where ResizeVolume used to be, between
// UpdateWatermarks and SetVolumeState, says why V1 has no resize at all.
type Store interface {
	// AcquireLeadership takes leadership, incrementing and returning the term. It is
	// an election, not a heartbeat: it moves the term unconditionally, for the same
	// holder too. RenewLeadership is what a leader that is already leading calls.
	AcquireLeadership(ctx context.Context, holderID string) (int64, error)
	// RenewLeadership refreshes the leader record's renewed_at under the caller's own
	// term and holder id, without moving either (term-guarded, §7). A caller that is
	// not the current leader — its term was superseded, or the row names somebody else
	// — is ErrStaleTerm, and nothing is written.
	//
	// It exists because the term guard alone leaves two holes: the leader row's stamp was
	// written by an election and never touched, so a Control Plane dead for an hour looks
	// like one that started an hour ago; and a superseded Control Plane finds out only when
	// it next mutates something. One periodic guarded write closes both.
	//
	// Deliberately not AcquireLeadership on a timer: that increments the term every tick,
	// and every admin one-shot in cmd/control-plane reads GetLeader and then writes under
	// the term it read, so a self-renewing leader would fail them at random.
	RenewLeadership(ctx context.Context, term int64, holderID string) error
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
	// agent version, format version, NVMe totals, RemoteBacklogBytes, heartbeat. It
	// deliberately does NOT carry the fleet state — that belongs to the Control
	// Plane (SetHostState),
	// and a routine heartbeat that carried it would un-cordon a draining host.
	// Committed capacity is not carried either, and could not be: it is derived
	// from the volumes that name the host (ADR-0017). Term-guarded.
	UpsertHost(ctx context.Context, term int64, h Host) error
	// GetHost returns a host.
	GetHost(ctx context.Context, hostID string) (Host, error)
	// ListHosts returns every host ordered by host id (deterministic, INV-02).
	ListHosts(ctx context.Context) ([]Host, error)
	// SetHostState transitions a host's fleet state (term-guarded, §28.1). The move
	// is checked against the lifecycle table: an unknown value is
	// lifecycle.ErrUnknownState, an illegal move lifecycle.ErrInvalidTransition.
	//
	// reason says who is asking (ADR-0013 §3, §5). It is recorded as the host's
	// cordon_reason when state is CORDONED and cleared otherwise, and it is also the
	// authority the write carries: a lifecycle.CordonPressure write is refused with
	// lifecycle.ErrCordonHeld against a host an operator cordoned, so the automatic
	// 70%-used loop can never take a host out of a cordon a human put it in for a
	// cause the fleet cannot see. lifecycle.CordonNone is not an actor and is
	// rejected — a state change with no recorded author is one no operator can
	// interpret afterwards.
	SetHostState(ctx context.Context, term int64, hostID string, state lifecycle.HostState, reason lifecycle.CordonReason) error
	// RenewHostLease renews (or grants) a host's lease with the given TTL
	// (term-guarded). A host the fleet has recorded as DEAD is refused with
	// ErrHostNotServing: that state is the Control Plane asserting the writer is
	// gone — the same assertion promotion accepts as a reason to skip the fencing
	// wait — so a routine heartbeat must not be able to re-arm it. A CORDONED or
	// DRAINING host still renews: both are still serving the volumes they hold, and
	// stopping their ACKs for the whole evacuation is the failure that would cause.
	//
	// There is no revocation window and no way to take a lease back. Both once existed so that the lease a promotion revoked to fence a source could not be
	// re-armed by the source's next heartbeat; ADR-0026 then withdrew the promotion,
	// and the ADR's own amendment says what is left of the window is empty — "a
	// revocation stops nothing on the data path", because the lease is a liveness
	// signal the Control Plane reads and the data path never consults. Block/Unblock/
	// RevokeHostLease outlived their only caller by two waves. They are deleted rather
	// than kept for stage 2 because a mechanism nothing exercises is a mechanism
	// nobody can trust when it is finally needed: what stage 2 needs is a fence that
	// follows the *volume*, and that is not this code with a caller added.
	RenewHostLease(ctx context.Context, term int64, hostID string, ttlSeconds int) error
	// GetHostLease returns a host's lease, or ErrNotFound if it holds none.
	//
	// No binary calls it, and it stays because it is the only way to observe what
	// RenewHostLease did from outside the store: cpserver's heartbeat test asserts
	// that a heartbeat renewed the lease by reading the lease back, rather than by
	// trusting the handler's return — which is the assertion CLAUDE.md asks for and
	// the one that would go missing if this were deleted with the write verbs above.
	GetHostLease(ctx context.Context, hostID string) (HostLease, error)

	// CreateVolume inserts a volume (term-guarded). It is idempotent and never
	// destructive: for an id that already exists it converges instead of aborting —
	// two operators running rebuild-metadata at once must both finish — but it never
	// lowers current_epoch, shrinks size_bytes, rewinds a watermark, blanks an
	// owner, or rewrites the lifecycle state. Ownership and state move only through
	// BumpVolumeEpoch, SetVolumePrimaryHost and SetVolumeState.
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
	// ListVolumes returns every volume ordered by volume id, including the ones placed
	// nowhere — which are the whole reason it exists and are unreachable through
	// ListVolumesByHost: an unplaced volume's primary is NULL, which no host id matches (not
	// even the empty one, ErrInvalidID at the boundary). rebuild-metadata restores every
	// volume with no placement, so after the one event that most needs an answer *every*
	// volume is in that set.
	//
	// A fleet-wide scan and on no data path: this is for a human reading the catalog.
	ListVolumes(ctx context.Context) ([]Volume, error)
	// BumpVolumeEpoch advances the epoch to expectedEpoch+1 and sets the primary
	// host, term-guarded, returning the new epoch (§12.3). It is a compare-and-set,
	// not an increment: the caller chooses which epoch to grant by reading the
	// volume first, and expectedEpoch is what it read. A volume that has moved on
	// since is ErrEpochConflict and nothing is written — otherwise each of n racing
	// callers burns an epoch and the last one writes its own host into
	// primary_host_id, naming an owner that never won the S3 epoch object and never
	// got a lease.
	//
	// controlplane.Place is what calls it: every attach grants an epoch the volume has
	// never been served under, so a host that gets the volume back cannot resume the WAL
	// directory its previous session left behind. It does not write the §7 state, which
	// is why Place follows it with SetVolumePrimaryHost rather than using it alone.
	BumpVolumeEpoch(ctx context.Context, term int64, volumeID, primaryHostID string, expectedEpoch int64) (int64, error)
	// SetVolumePrimaryHost places a volume on a host, or clears its placement when
	// primaryHostID is empty (term-guarded). It writes the §7 state that goes with the
	// ownership in the same statement — PlacedState says which, and why they must not be two
	// writes. It is the only mutation of primary_host_id that is not a promotion: before it
	// the column was write-once, so an attach was permanent.
	//
	// **Moving straight from one host to another is refused with ErrAlreadyPlaced**; the
	// caller clears first and places afterwards, and that sentinel carries the reason.
	//
	// **The epoch is not touched by this write, in either direction.** controlplane.Place
	// grants a fresh epoch before calling this; the two are separate writes because:
	//
	//   - detaching grants the token to nobody, so bumping inside a write that clears the
	//     owner would burn an epoch no host holds;
	//   - the release does not need it — cpserver.applyReport compares primary_host_id
	//     against the reporting host *before* the epoch, so a cleared volume answers
	//     NOT_PRIMARY, and "" can never be a reporting host (the RPC refuses it);
	//   - an Agent restart must keep its epoch: it is not a placement, so the desired state
	//     repeats the epoch and the Agent re-attaches to its own WAL (ADR-0024).
	//
	// Re-writing the placement a volume already has is a no-op, not an error.
	SetVolumePrimaryHost(ctx context.Context, term int64, volumeID, primaryHostID string) error
	// ClearVolumeParent records that a volume descends from nothing any more:
	// parent_snapshot_id back to NULL and chain_depth back to 0 (term-guarded).
	//
	// It is the one write `lineage.Flatten` cannot make and cannot do without. CreateVolume's
	// conflict path COALESCEs the column so a converging rebuild can never drop a clone's
	// link — which also means no write here could say a lineage had ended, while everything
	// that *counted* lineage kept counting it: `controlplane.Clone`'s ceiling, and a delete of
	// the old parent still seeing a descendant (ErrHasDescendants).
	//
	// Clearing a volume that already descends from nothing is a no-op: an operator re-running
	// a flatten must be able to run it twice. It does not touch the descriptor — the flatten
	// rewrites that first, and a second writer here is a second home for the fact.
	ClearVolumeParent(ctx context.Context, term int64, volumeID string) error
	// DeleteVolume removes a volume and its snapshots from the catalog (term-guarded).
	// There is no DELETING state and no timer: the recovery window belongs to the bucket's
	// own versioning and lifecycle policy, and a second window in a column would drift from
	// the one that controls the bytes. The undo is `-rebuild-metadata`, built for losing the
	// whole database.
	//
	// The snapshots go in the same write: a snapshot row whose volume is gone is unreadable
	// (its manifest lives under the volume's prefix) and satisfies no foreign key.
	//
	// ErrHasDescendants if anything still descends from those snapshots. ErrNotFound if the
	// volume is not there, which a re-run of a completed delete reads as "already done".
	//
	// It does not check placement: `primary_host_id IS NULL` is the delete command's
	// precondition, a statement about a host this Store cannot see.
	DeleteVolume(ctx context.Context, term int64, volumeID string) error
	// UpdateWatermarks lazily updates the informative watermarks (term-guarded).
	// A report violating published ≤ durable ≤ local is ErrWatermarkOrder (INV-03).
	// A report that is merely *late* — an epoch-N primary's, delivered after epoch
	// N+1 published its own — is not an error and is not applied: each watermark is
	// monotonic, because promotion does not change the CP term and this number is
	// what an operator reads during an incident.
	UpdateWatermarks(ctx context.Context, term int64, volumeID string, local, durable, published int64) error
	// SetVolumeRefusal records why the host holding a volume is not serving it, or clears the
	// record when it is (lifecycle.RefusalNone). Term-guarded, and — unlike UpdateWatermarks —
	// qualified by the reporting host and epoch: a watermark is monotonic and a late report is
	// harmless under GREATEST, while a refusal is a state about right now, so the write is
	// last-report-wins **within** (hostID, epoch) and a no-op outside it.
	//
	// A report naming the wrong host or epoch is not an error — it is a fenced writer whose
	// opinion is void — and returns nil. ErrNotFound only for a volume not in the catalog.
	SetVolumeRefusal(ctx context.Context, term int64, volumeID, hostID string, epoch int64,
		refusal lifecycle.Refusal, detail string) error
	// There is no ResizeVolume here, and **V1 does not resize a volume**. §3's objective 14
	// and §9's config-space propagation have no mechanism behind them: blockdev.Device fixes
	// its capacity at construction; the guest cannot be told at all, because announcing a new
	// capacity needs VHOST_USER_BACKEND_CONFIG_CHANGE_MSG over the backend request channel
	// and internal/vhost deliberately does not offer VHOST_USER_PROTOCOL_F_BACKEND_REQ (a
	// test pins that it is not offered); and descriptor.json carries size_bytes and is
	// written only at create, so a resized volume disagrees with the object
	// -rebuild-metadata restores it from (INV-20).
	//
	// Rejected: keep the correct grow-only method and wait — an uncallable verb reads as a
	// feature that exists. Bringing it back belongs in the same increment as the Agent,
	// blockdev, vhost and guest-lane work. metadatatest's VolumeGeometryIsImmutable asserts
	// the immutability this leaves.

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
	// ListPendingSnapshots returns the CREATING snapshots of the volumes hostID is
	// primary for, oldest first (§19). It is how a snapshot request reaches an Agent:
	// the Control Plane puts the oldest id in the volume's desired state, and the
	// Agent converges on it.
	//
	// It follows the *volume*, not snapshots.source_host_id, because the request names
	// a volume and only the host serving it can freeze it. source_host_id is stamped
	// on completion by the host that actually took it, which is what §20's placement
	// rule 1 reads later.
	ListPendingSnapshots(ctx context.Context, hostID string) ([]Snapshot, error)
	// ListUnfinishedSnapshots returns every snapshot waiting on something, fleet-wide and
	// ordered by snapshot id: CREATING (waiting on an Agent) and DELETING (waiting on a
	// reclaim ADR-0026 deleted, so waiting for ever). PUBLISHED and FAILED are finished.
	//
	// It is not ListPendingSnapshots without the host argument: that one joins through
	// volumes.primary_host_id, so a CREATING snapshot of a detached volume — precisely the
	// stuck one — belongs to no host and appears in no per-host listing.
	//
	// The two states are hard-coded: one caller, one question, and a state filter would let
	// a future caller ask for PUBLISHED — a fleet-wide scan of the largest table here.
	ListUnfinishedSnapshots(ctx context.Context) ([]Snapshot, error)
	// PublishSnapshot moves CREATING → PUBLISHED, recording the two facts only the host
	// that took it knows: the commit its history is named by, and which host reported it
	// (term-guarded).
	//
	// The manifest key is not among them and must not be. It is derived from the commit
	// id, so a catalog and a bucket cannot disagree about where a snapshot lives — which
	// is exactly what a key believed from the Agent allowed, and why the column is gone.
	//
	// Reporting the same publication twice is a no-op, because the Agent keeps reporting
	// until the request stops arriving. Reporting a *different* commit at a published id
	// is refused: INV-16.
	PublishSnapshot(ctx context.Context, term int64, snapshotID, commitID, sourceHostID string) error
	// SetSnapshotState moves a snapshot through the §19 lifecycle (term-guarded,
	// transition-guarded in the write). Without it a snapshot whose publication
	// crashed stays CREATING for ever: PublishSnapshot is the only other way out of
	// that state, and it is the one the crash proved will not arrive, so nothing
	// could mark the row FAILED and nothing could ever ask for its objects back.
	SetSnapshotState(ctx context.Context, term int64, snapshotID string, state lifecycle.SnapshotState) error
}

// There are no operation methods. §7's reconciliation operations — RecordOperation,
// UpdateOperation, GetOperation, ListLiveOperationsByHost and the `operations` table under
// them — were the interface of the drain, promotion and recovery ADR-0026 withdrew, and
// each had exactly one caller: the contract test. They come back with the writer that
// populates them, along with ADR-0017's second capacity term (internal/schema/schema.sql).
