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
	// ErrHostNotServing means the operation needs a host the fleet still considers a
	// writer, and this one is DEAD (§28.1). Marking a host dead is the Control Plane
	// asserting that its writer is gone; handing it a fresh lease afterwards
	// contradicts that assertion.
	ErrHostNotServing = errors.New("metadata: host is not serving")
	// ErrAlreadyPlaced means SetVolumePrimaryHost was asked to move a volume straight
	// from one host to another. It is refused, and the reason is the only thing that
	// keeps two writers off one volume today: a host serves exactly the volumes
	// GetDesiredState lists for it, and it learns it has lost one on its *next* poll.
	// A single write that named a new owner would therefore hand the volume to the
	// destination while the source is still serving it, for a poll interval, with both
	// guests writing and both Agents publishing an image over the same manifest — one
	// of them silently losing its session to the CAS.
	//
	// The caller detaches first and places afterwards, which passes the volume through
	// "no host" and gives the source the same teardown it gets on any other release
	// (stop, publish, close). That is not exclusion either — an operator who places
	// again within one poll interval has re-created the window by hand — and closing
	// it properly is the §7 state machine's job, not this write's. What this refusal
	// buys is that no *single* catalog write can open it.
	ErrAlreadyPlaced = errors.New("metadata: volume is already placed on another host")

	// ErrUnversionedDEK is a volume written with DEKKeyID 0 — see CheckDEKKeyID.
	ErrUnversionedDEK = errors.New("metadata: a volume's DEK must carry a version")

	// ErrHasDescendants means a volume cannot be removed while another volume — or
	// another volume's snapshot — still descends from one of its snapshots. It is the
	// catalog half of DELETION-AND-RECLAIM-SPEC's first precondition, and it is a
	// safety net rather than the check an operator meets: since publishing stopped
	// flattening, `parent_snapshot_id` is write-once by construction, so the catalog
	// keeps naming a parent a FLATTEN has already dissolved in the bucket. The command
	// therefore asks the *bucket* who descends from what and flattens them first; this
	// refuses the write that would leave a snapshot row referenced by nothing it can
	// still be read through.
	//
	// Postgres would refuse it anyway — snapshots.snapshot_id is referenced by both
	// volumes.parent_snapshot_id and snapshots.parent_snapshot_id — and that is
	// precisely why the sentinel exists: an integrity violation surfacing as a driver
	// error is one no caller can branch on, and the sim has no foreign keys at all.
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

// PlacedState is the §7 volume state that belongs with an ownership write: a volume
// with a writer is ACTIVE, a volume with none is DETACHED. It is here, next to
// CheckWatermarkOrder and for the same reason, because both stores have to derive it
// identically — and because it is the answer to "what does clearing the primary host
// mean for the state".
//
// The two are one fact, so SetVolumePrimaryHost writes them together rather than
// leaving the state to a second call. Two writes have a window, and the window is not
// harmless in either order: a volume left ACTIVE with no host is one that
// controlplane.RequestSnapshot accepts (it checks only the state) and that no Agent
// can ever take, so the snapshot sits CREATING for ever; a volume left DETACHED while
// a host still serves it is one the catalog says nobody is writing while a guest is.
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
	//
	// ADR-0017's second term — what an in-flight operation plan had reserved on the
	// host but not yet placed — went with the operations table it was read from
	// (internal/schema/schema.sql carries the reasoning, including what it does and
	// does not cost).
	//
	// It is therefore ignored on the way in: UpsertHost cannot set it, and neither
	// can anything else. A number S3 or SQL can recompute is a cache, never an
	// authority, and the cheapest cache to keep honest is the one that does not
	// exist.
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

// CapacityBound is the §28.2 oversubscription ceiling a write must respect, stated
// by the caller that took the placement decision so there is one copy of the rule.
//
// placement.Choose is pure and advisory: two operations that read the fleet before
// either reserved anything pick the same destination and both proceed, and the host
// lands past the declared bound with neither caller having made a mistake.
// Re-checking in Go only narrows that window; the bound is a bound only when the
// statement that places the bytes evaluates it. So it travels *with* the write:
//
//	committed(HostID) + AddBytes <= Limit   and   used(HostID) <= UsedLimit
//
// where committed is the derived value (ADR-0017) as it stands immediately before
// the write and used is the host's own last measurement of its device. The second
// half is the ADR-0013 gap: the first bounds what the fleet has *promised* the host,
// which is not what fills it — under ADR-0026 a session's WAL stays local until the
// volume stops, and no reservation covers a byte of it. Both travel together because
// both are the same decision, taken once by placement.Policy.Bound.
//
// A write that carries no bound is not a placement decision — rebuild-metadata
// recreating volumes that already occupy their hosts — and a bound is never applied
// to a write that gives capacity back: a host can be over its ceiling for reasons
// that have nothing to do with the caller (a tightened policy, a device that came
// back smaller), and refusing the write that brings it down would wedge every
// release of that host.
//
// CreateVolume is the only write that takes one today. SetVolumePrimaryHost — the
// other way a volume comes to occupy a host — takes none, which is an open gap and
// not a decision (recorded against D6 in docs/plan/tracks/TRACK-D.md).
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
	// UsedLimit is the highest *measured* used value the host may already show and
	// still take the write (placement.Policy.UsedLimit). It charges AddBytes
	// nothing: a volume does not occupy its declared size the moment it is placed,
	// and assuming it does is the assumption oversubscription exists to deny — the
	// reasoning is at placement.Policy.Admits, which evaluates the same rule.
	//
	// Zero is fail-closed the same way Limit is, and here it bites in production
	// rather than in a test: every real device measures something, so a bound built
	// by hand without this field refuses every placement. That is the intended
	// direction — Policy.Bound is what builds these, and a second builder is the
	// second copy of the rule.
	UsedLimit int64
}

// Volume is the durable-volume record (§8). Watermarks are informative (§5.8).
type Volume struct {
	VolumeID      string
	SizeBytes     int64
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
	// There is no revocation window and no way to take a lease back. ADR-0016 stage 1
	// added both so that the lease a promotion revoked to fence a source could not be
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
	// ListVolumes returns every volume ordered by volume id, including the ones
	// placed nowhere.
	//
	// Those are the whole reason it exists, and they are unreachable through
	// ListVolumesByHost: an unplaced volume's primary is NULL, which no host id
	// matches — not even the empty one, which is ErrInvalidID at the boundary. So a
	// caller iterating the fleet's hosts and unioning their listings sees exactly the
	// volumes that are already being served and none of the ones that are not, which
	// inverts what the question is usually asked for. rebuild-metadata restores every
	// volume with no placement (no object records one), so after the one event that
	// most needs an answer, *every* volume is in the set the per-host read cannot
	// return.
	//
	// It is a fleet-wide scan and it is not on any data path: cpserver answers Agents
	// from the per-host listings, and this exists for a human reading the catalog.
	ListVolumes(ctx context.Context) ([]Volume, error)
	// BumpVolumeEpoch advances the epoch to expectedEpoch+1 and sets the primary
	// host, term-guarded, returning the new epoch (§12.3). It is a compare-and-set,
	// not an increment: a promotion chooses which epoch to grant by reading the
	// volume first, and expectedEpoch is what it read. A volume that has moved on
	// since is ErrEpochConflict and nothing is written — otherwise each of n racing
	// promoters burns an epoch and the last one writes its own host into
	// primary_host_id, naming an owner that never won the S3 epoch object and never
	// got a lease.
	BumpVolumeEpoch(ctx context.Context, term int64, volumeID, primaryHostID string, expectedEpoch int64) (int64, error)
	// SetVolumePrimaryHost places a volume on a host, or clears its placement when
	// primaryHostID is empty (term-guarded). It writes the §7 state that goes with the
	// ownership in the same statement — PlacedState says which, and why they must not
	// be two writes.
	//
	// It is the only mutation of primary_host_id that is not a promotion. Until it
	// existed the column was write-once: CreateVolume set it and its converging upsert
	// protected it with COALESCE, so an attach was permanent — a volume could not be
	// detached, could not be re-placed, and the volumes rebuild-metadata restores with
	// no host could never be given one.
	//
	// **Moving straight from one host to another is refused with ErrAlreadyPlaced.**
	// The caller clears first and places afterwards; that sentinel carries the reason.
	//
	// **The epoch is not touched, in either direction.** Three things say so:
	//
	//   - An epoch is a fencing token, granted by BumpVolumeEpoch's compare-and-set to
	//     a writer that won it (§12.3). Detaching grants it to nobody, so incrementing
	//     here would burn a token no host holds.
	//   - Nothing needs it to. What stops a released host's reports being accepted is
	//     the ownership check, not the epoch: cpserver.applyReport compares
	//     primary_host_id against the reporting host *before* it looks at the epoch, so
	//     a cleared volume answers NOT_PRIMARY — which is exactly the outcome that makes
	//     the Agent fence the volume and tear it down. That is sufficient here, and it
	//     is sufficient because "" can never be a reporting host: the RPC refuses an
	//     empty host_id at the boundary, so a cleared owner matches nobody rather than
	//     matching everybody.
	//   - It would cost a re-attach its local data. The Agent's WAL lives at
	//     <data-dir>/wal/<volume-id>/<epoch>, so a bumped epoch is a fresh empty root:
	//     a volume detached and re-attached to the same host would abandon whatever the
	//     teardown publish did not carry (publish failures are logged and continue).
	//     Detach has to be reversible.
	//
	// A stale term is ErrStaleTerm, a missing volume ErrNotFound, a §7 move the table
	// forbids lifecycle.ErrInvalidTransition. Re-writing the placement a volume already
	// has is a no-op, not an error — the operator who re-runs the command after a
	// timeout must not be told it failed.
	SetVolumePrimaryHost(ctx context.Context, term int64, volumeID, primaryHostID string) error
	// ClearVolumeParent records that a volume descends from nothing any more:
	// parent_snapshot_id back to NULL and chain_depth back to 0 (term-guarded).
	//
	// It is the one write `lineage.Flatten` cannot make and cannot do without.
	// CreateVolume's conflict path is
	// `parent_snapshot_id = COALESCE(volumes.parent_snapshot_id, EXCLUDED...)`, which
	// makes the column write-once so that a converging rebuild can never drop a
	// clone's link — correct, and it also means no write on this Store could say a
	// lineage had ended. Nothing read wrong because of that: the Agent takes the link
	// from `descriptor.json`, which the flatten rewrites. Everything that *counted*
	// lineage did — `controlplane.Clone`'s ceiling kept refusing clones of a volume
	// that is back at depth 0, and a delete of the old parent kept seeing a descendant
	// whose snapshot rows it must not orphan (ErrHasDescendants). This is that write,
	// and it is why a flatten now unblocks a delete instead of only appearing to.
	//
	// Clearing a volume that already descends from nothing is a no-op, not an error:
	// an operator re-running a flatten, and a delete flattening several descendants in
	// one pass, must both be able to run twice.
	//
	// It does not touch the descriptor. The bucket is the authority a rebuild trusts
	// (INV-20), the flatten rewrites it before this is called, and a second writer of
	// that object here would be a second place the fact lives.
	ClearVolumeParent(ctx context.Context, term int64, volumeID string) error
	// DeleteVolume removes a volume and its snapshots from the catalog
	// (term-guarded). There is no DELETING state and no timer: the row goes, and the
	// recovery window belongs entirely to the bucket's own versioning and lifecycle
	// policy (DELETION-AND-RECLAIM-SPEC's decision of 2026-08-07). Putting a retention
	// window in a column as well as in a bucket policy makes two of them, and they
	// drift; only one of the two controls the bytes.
	//
	// **The undo is `-rebuild-metadata`**, which reconstructs both rows from the
	// descriptor and the snapshot manifests while the bucket still holds their
	// non-current versions. That is not a mechanism this method needs to provide — it
	// is the one built for losing the whole database.
	//
	// The volume's snapshots go with it, in the same write. A snapshot row whose
	// volume is gone is a row nothing can read (its manifest lives under the volume's
	// prefix) and a foreign key nothing can satisfy, so leaving the two to separate
	// calls would leave a window in which the catalog states a snapshot of a volume
	// that does not exist.
	//
	// ErrHasDescendants if anything still descends from one of those snapshots.
	// ErrNotFound if the volume is not there — which a re-run of a delete that already
	// removed the row will get, and which its caller reads as "already done".
	//
	// It does not check placement. `primary_host_id` being NULL is the delete
	// command's precondition and it belongs there: it is a statement about a host that
	// is still serving a device, which this Store cannot see and could not enforce
	// against an Agent that has not polled yet.
	DeleteVolume(ctx context.Context, term int64, volumeID string) error
	// UpdateWatermarks lazily updates the informative watermarks (term-guarded).
	// A report violating published ≤ durable ≤ local is ErrWatermarkOrder (INV-03).
	// A report that is merely *late* — an epoch-N primary's, delivered after epoch
	// N+1 published its own — is not an error and is not applied: each watermark is
	// monotonic, because promotion does not change the CP term and this number is
	// what an operator reads during an incident.
	UpdateWatermarks(ctx context.Context, term int64, volumeID string, local, durable, published int64) error
	// There is no ResizeVolume here any more, and **V1 does not resize a volume**.
	// §3's objective 14 ("resize online (grow)") has no verb behind it; §9's promise
	// that "el grow se propaga vía actualización del config space + notificación" has
	// no mechanism behind it either.
	//
	// The method that was here grew size_bytes under the term guard and refused a
	// shrink with ErrShrinkNotAllowed, and it was correct. What it was not was a
	// resize: **a row that grows is not a volume that grows.** The rest of the path
	// does not exist, and every step of it is missing, not merely untested —
	//
	//   - cpserver.GetDesiredState already sends size_bytes to the Agent on every
	//     poll, and agent.VolumeManager.Apply returns at its epoch check before it
	//     reads the field, so a grown row reaches the Agent every few seconds and
	//     changes nothing;
	//   - blockdev.New fixes a Device's capacity at construction and blockdev.Device
	//     has no way to change it, so even a restart-driven resize means tearing the
	//     volume down — which publishes the session and takes the guest's device away;
	//   - the guest cannot be told in any case. Announcing a new capacity needs
	//     VHOST_USER_BACKEND_CONFIG_CHANGE_MSG over the backend request channel, and
	//     internal/vhost deliberately does not offer VHOST_USER_PROTOCOL_F_BACKEND_REQ
	//     (a test pins that it is not offered). Without it QEMU raises no virtio
	//     configuration-change interrupt and the guest never re-reads its capacity.
	//   - descriptor.json carries size_bytes and is written only at create and clone,
	//     so a resized volume's descriptor kept the old size — and -rebuild-metadata
	//     reads exactly that object to reconstruct the row (INV-20). Keeping the method
	//     was therefore not neutral: it was the one way to make the catalog and the
	//     bucket disagree about a volume's size, with nothing to notice.
	//
	// **The rejected alternative was to keep it and wait.** Thirty correct lines cost
	// nothing to hold, and a future resize would have to restate the §3 rule. But an
	// uncallable verb reads to the next person as a feature that exists, and this one
	// had a defect behind it rather than a gap. Bringing it back is one commit —
	// the query, the two store methods, the contract cases — and it belongs in the
	// same increment as the Agent, blockdev, vhost and guest-lane work above, which is
	// what makes resize a verb instead of a column write.
	//
	// The immutability this leaves is asserted, not assumed: metadatatest's
	// VolumeGeometryIsImmutable runs every mutation on the Store against a fresh
	// volume and reads its size and block size back.

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
	// ListUnfinishedSnapshots returns every snapshot that is waiting on something,
	// fleet-wide and ordered by snapshot id: the CREATING ones, which are waiting on
	// an Agent, and the DELETING ones, which are waiting on a reclaim that ADR-0026
	// deleted and will therefore wait for ever. PUBLISHED and FAILED are finished —
	// one of them succeeded, the other is a request that is over — so neither is
	// anybody's outstanding work.
	//
	// It is not ListPendingSnapshots without the host argument, and the difference is
	// the point of it. That one joins through volumes.primary_host_id, so a CREATING
	// snapshot of a volume that has since been detached — or of every volume, after a
	// rebuild — belongs to no host and appears in no per-host listing at all. It is
	// precisely the snapshot that is stuck, and it is precisely the one the read that
	// drives the Agents cannot see.
	//
	// The two states are hard-coded rather than taken as a parameter: there is one
	// caller and one question, and a state filter callers pass would let a future one
	// ask for PUBLISHED — a fleet-wide unbounded scan of the largest table here, which
	// is a listing API, not an incident read.
	ListUnfinishedSnapshots(ctx context.Context) ([]Snapshot, error)
	// PublishSnapshot moves CREATING → PUBLISHED, recording the three facts only the
	// host that took it knows: the §19 sequence the copy was frozen at, the manifest
	// it wrote, and which host did it (term-guarded).
	//
	// Reporting the same publication twice is a no-op, because the Agent keeps
	// reporting until the request stops arriving. Reporting a *different* sequence at
	// a published id is refused: INV-16.
	PublishSnapshot(ctx context.Context, term int64, snapshotID string, targetSequence int64, sourceHostID, manifestKey string) error
	// SetSnapshotState moves a snapshot through the §19 lifecycle (term-guarded,
	// transition-guarded in the write). Without it a snapshot whose publication
	// crashed stays CREATING forever and the catalog side of GC never sees it.
	SetSnapshotState(ctx context.Context, term int64, snapshotID string, state lifecycle.SnapshotState) error
}

// There are no operation methods. §7's reconciliation operations —
// RecordOperation, UpdateOperation, GetOperation, ListLiveOperationsByHost, and the
// `operations` table under them — were the interface of the drain, the promotion and
// the recovery ADR-0026 withdrew, and after it nothing wrote a row: each of the four
// had exactly one caller and it was the contract test. They are deleted rather than
// kept for the mechanism's return, because a store method with a lifecycle, a
// capacity bound and a duplicate-request rule reads as something the Control Plane
// uses, and the next reader has no way to tell that it does not.
//
// What comes back with cross-host movement is ADR-0017's second capacity term (see
// internal/schema/schema.sql), and it comes back with the writer that populates it.
