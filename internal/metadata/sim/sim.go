// Package sim is the in-memory, deterministic metadata.Store used by the DST
// harness. The fencing protocol (§12) is proven here under simulated
// partitions and clock drift — a real Postgres cannot be deterministic under those
// conditions. Timestamps come from an injected now-function so runs are reproducible.
package sim

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
)

// Store is a deterministic in-memory metadata store. Safe for concurrent use.
type Store struct {
	now func() time.Time

	mu           sync.Mutex
	leaderTerm   int64
	leaderHolder string
	leaderAt     time.Time

	hosts  map[string]metadata.Host
	leases map[string]metadata.HostLease
	vols   map[string]metadata.Volume
	snaps  map[string]metadata.Snapshot
}

// New returns an empty store whose timestamps come from now (e.g. a sim clock's Wall).
func New(now func() time.Time) *Store {
	return &Store{
		now:    now,
		hosts:  map[string]metadata.Host{},
		leases: map[string]metadata.HostLease{},
		vols:   map[string]metadata.Volume{},
		snaps:  map[string]metadata.Snapshot{},
	}
}

var _ metadata.Store = (*Store)(nil)

// checkTerm is the §7 guard. Term 0 is never valid: it is what a Control Plane that
// never won an election passes, and before the first AcquireLeadership there is no
// leader to agree with it.
func (s *Store) checkTerm(term int64) error {
	if s.leaderTerm == 0 || term != s.leaderTerm {
		return metadata.ErrStaleTerm
	}
	return nil
}

// requireID rejects an empty identifier. A row keyed on "" is invisible to every
// lookup that follows, so an unset config field must fail loudly at the boundary
// rather than become a row nobody can find. (The sim deliberately does not
// constrain identifier *syntax* — that is a Postgres/UUIDv7 concern, INV-22.)
func requireID(kind, id string) error {
	if id == "" {
		return fmt.Errorf("%w: empty %s id", metadata.ErrInvalidID, kind)
	}
	return nil
}

func (s *Store) AcquireLeadership(_ context.Context, holderID string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leaderTerm++
	s.leaderHolder = holderID
	s.leaderAt = s.now()
	return s.leaderTerm, nil
}

// RenewLeadership refreshes the leader's stamp under its own term and holder, moving
// neither. metadata.Store carries why that is a different act from AcquireLeadership;
// what this implementation adds is that the holder is compared as well as the term, so
// the sim answers a superseded holder exactly as the SQL predicate does.
func (s *Store) RenewLeadership(_ context.Context, term int64, holderID string) error {
	if err := requireID("holder", holderID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
	}
	if holderID != s.leaderHolder {
		return metadata.ErrStaleTerm
	}
	s.leaderAt = s.now()
	return nil
}

// Now is the store's own clock — the one that stamps every timestamp below, and so
// the one a fencing deadline must be measured against (§12.1).
func (s *Store) Now(_ context.Context) (time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.now(), nil
}

func (s *Store) GetLeader(_ context.Context) (metadata.Leader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leaderTerm == 0 {
		return metadata.Leader{}, metadata.ErrNotFound
	}
	return metadata.Leader{Term: s.leaderTerm, HolderID: s.leaderHolder, RenewedAt: s.leaderAt}, nil
}

func (s *Store) UpsertHost(_ context.Context, term int64, h metadata.Host) error {
	if err := requireID("host", h.HostID); err != nil {
		return err
	}
	if !h.State.Valid() {
		return fmt.Errorf("%w: host state %q", lifecycle.ErrUnknownState, h.State)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
	}
	// A heartbeat refreshes what the host knows about itself. The fleet state is the
	// Control Plane's (§28.1): letting a heartbeat carry it un-cordons a host that
	// is being drained. Committed capacity is derived (ADR-0017), so whatever the
	// caller put in the field is dropped rather than stored.
	if cur, exists := s.hosts[h.HostID]; exists {
		h.State = cur.State
		// The reason travels with the state it explains, or a heartbeat would leave
		// a CORDONED host with an empty cordon_reason and an operator with no way to
		// tell why it is out of service (ADR-0013 §3).
		h.CordonReason = cur.CordonReason
	} else {
		// A host introducing itself is ACTIVE, and ACTIVE carries no reason. The
		// caller's field is dropped for the same reason its State is only read here.
		h.CordonReason = lifecycle.CordonNone
	}
	h.NVMeCommittedBytes = 0
	h.LastHeartbeat = s.now()
	s.hosts[h.HostID] = h
	return nil
}

func (s *Store) GetHost(_ context.Context, hostID string) (metadata.Host, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.hosts[hostID]
	if !ok {
		return metadata.Host{}, metadata.ErrNotFound
	}
	h.NVMeCommittedBytes = s.committedLocked(hostID)
	return h, nil
}

func (s *Store) ListHosts(_ context.Context) ([]metadata.Host, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	hosts := make([]metadata.Host, 0, len(s.hosts))
	for id, h := range s.hosts {
		h.NVMeCommittedBytes = s.committedLocked(id)
		hosts = append(hosts, h)
	}
	sort.Slice(hosts, func(i, j int) bool { return hosts[i].HostID < hosts[j].HostID })
	return hosts, nil
}

// committedLocked is ADR-0017's derived §28.2 capacity, computed the same way the
// host_committed_bytes view computes it in SQL: what the host holds. Nothing is stored, so
// a resumed pass computes the same answer as the pass that crashed. ADR-0017's second term
// went with the operations table (internal/schema/schema.sql).
func (s *Store) committedLocked(hostID string) int64 {
	var total int64
	for _, v := range s.vols {
		if v.PrimaryHostID == hostID {
			total += v.SizeBytes
		}
	}
	return total
}

// boundLocked is the §28.2 ceiling and the ADR-0013 fill ceiling evaluated where the write
// happens, against the derived committed value and the host's last measurement as they
// stand immediately before it. Nil is not a placement decision. Both arms are here rather
// than one here and one in the caller: they must be answered from the same row at the same
// instant.
func (s *Store) boundLocked(b *metadata.CapacityBound) error {
	if b == nil {
		return nil
	}
	h, ok := s.hosts[b.HostID]
	if !ok {
		return metadata.ErrNotFound
	}
	if after := s.committedLocked(b.HostID) + b.AddBytes; after > b.Limit {
		return fmt.Errorf("%w: host %s would hold %d committed bytes, the policy admits %d",
			metadata.ErrCapacityExceeded, b.HostID, after, b.Limit)
	}
	if h.NVMeUsedBytes > b.UsedLimit {
		return fmt.Errorf("%w: host %s measures %d used bytes, the policy takes new volumes below %d",
			metadata.ErrCapacityExceeded, b.HostID, h.NVMeUsedBytes, b.UsedLimit)
	}
	return nil
}

func (s *Store) SetHostState(_ context.Context, term int64, hostID string, state lifecycle.HostState, reason lifecycle.CordonReason) error {
	if err := requireID("host", hostID); err != nil {
		return err
	}
	if !state.Valid() {
		return fmt.Errorf("%w: host state %q", lifecycle.ErrUnknownState, state)
	}
	if !reason.Authority() {
		return fmt.Errorf("%w: cordon reason %q is not an authority", lifecycle.ErrUnknownState, reason)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
	}
	h, ok := s.hosts[hostID]
	if !ok {
		return metadata.ErrNotFound
	}
	if err := h.State.Transition(state); err != nil {
		return err
	}
	// The authority check is here, next to the transition check, for the same reason
	// the pg store puts it in the UPDATE's predicate: it decides whether the write
	// happens at all, so it must not be something a caller could forget to do first.
	if !reason.MayOverwrite(h.CordonReason) {
		return fmt.Errorf("%w: host %s is cordoned by %s, %s may not change it",
			lifecycle.ErrCordonHeld, hostID, h.CordonReason, reason)
	}
	h.State = state
	// A reason belongs to a cordon and dies with it: a host that has just gone back
	// to ACTIVE carrying "DEVICE_PRESSURE" is a row the next reader will believe.
	h.CordonReason = lifecycle.CordonNone
	if state == lifecycle.HostCordoned {
		h.CordonReason = reason
	}
	s.hosts[hostID] = h
	return nil
}

func (s *Store) RenewHostLease(_ context.Context, term int64, hostID string, ttlSeconds int) error {
	if err := requireID("host", hostID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
	}
	// A lease belongs to a registered host (the FK in the schema). Granting one to
	// an unknown id invents a fencing token for a host nobody can find.
	h, exists := s.hosts[hostID]
	if !exists {
		return metadata.ErrNotFound
	}
	// A DEAD host has been declared gone by the Control Plane itself; re-arming its
	// lease contradicts that while the new primary is materialising the epoch.
	if !h.State.Serving() {
		return fmt.Errorf("%w: host %s is %s", metadata.ErrHostNotServing, hostID, h.State)
	}
	now := s.now()
	l, ok := s.leases[hostID]
	if !ok {
		l = metadata.HostLease{HostID: hostID, GrantedAt: now}
	}
	l.LastRenewal = now
	l.TTLSeconds = int32(ttlSeconds)
	s.leases[hostID] = l
	return nil
}

func (s *Store) GetHostLease(_ context.Context, hostID string) (metadata.HostLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.leases[hostID]
	if !ok {
		return metadata.HostLease{}, metadata.ErrNotFound
	}
	return l, nil
}

func (s *Store) CreateVolume(_ context.Context, term int64, v metadata.Volume, bound *metadata.CapacityBound) error {
	if err := requireID("volume", v.VolumeID); err != nil {
		return err
	}
	if !v.State.Valid() {
		return fmt.Errorf("%w: volume state %q", lifecycle.ErrUnknownState, v.State)
	}
	// INV-03 at birth: a row created out of order can never be repaired, because
	// every later report only moves each watermark forward.
	if err := metadata.CheckWatermarkOrder(v.LocalSequence, v.DurableSequence, v.PublishedSequence); err != nil {
		return err
	}
	if err := metadata.CheckDEKKeyID(v.DEKKeyID); err != nil {
		return err
	}
	// The pg half has a foreign key; this is the same rule where sim can enforce it.
	// A clone naming a snapshot nobody created is a clone that reads zeros.
	if v.ParentSnapshotID != "" {
		s.mu.Lock()
		_, ok := s.snaps[v.ParentSnapshotID]
		s.mu.Unlock()
		if !ok {
			return fmt.Errorf("%w: parent snapshot %s", metadata.ErrNotFound, v.ParentSnapshotID)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
	}
	if err := s.boundLocked(bound); err != nil {
		return err
	}
	if cur, exists := s.vols[v.VolumeID]; exists {
		v = converge(cur, v)
	}
	s.vols[v.VolumeID] = v
	return nil
}

// converge merges a re-create onto the row that is already there. Two operators
// running rebuild-metadata at once both see ErrNotFound and both create; the loser
// must not undo the winner. Everything that is authority — the epoch, ownership,
// the lifecycle state, the watermarks, the size — is kept at its highest/existing
// value; everything that is description comes from the new record.
func converge(cur, next metadata.Volume) metadata.Volume {
	next.CurrentEpoch = max(cur.CurrentEpoch, next.CurrentEpoch)
	// Geometry is authority, and it comes from the row that already exists rather than
	// from the newcomer. It was grow-only, on a §3 resize rule whose only verb was
	// withdrawn (DEV-0023), which left a rewrite of `size_bytes` in a bucket object able
	// to grow a live volume's device — and `block_size`, which had no rule at all, able
	// to change what a guest addresses under a running kernel.
	if cur.SizeBytes != 0 {
		next.SizeBytes, next.BlockSize = cur.SizeBytes, cur.BlockSize
	}
	next.LocalSequence = max(cur.LocalSequence, next.LocalSequence)
	next.DurableSequence = max(cur.DurableSequence, next.DurableSequence)
	next.PublishedSequence = max(cur.PublishedSequence, next.PublishedSequence)
	next.State = cur.State                       // moves only through SetVolumeState (§7)
	next.FencingStartedAt = cur.FencingStartedAt // and neither does its fence record
	if cur.PrimaryHostID != "" {
		next.PrimaryHostID = cur.PrimaryHostID
	}
	if cur.StandbyHostID != "" {
		next.StandbyHostID = cur.StandbyHostID
	}
	// Key material is authority, not description: a re-create carrying a *newer* wrapped DEK
	// would re-key a live volume — which is what a clone pointed at an existing id did, and
	// what a rebuild reading somebody else's descriptor would do. Set once, at provision or
	// clone.
	if cur.DEKKeyID != 0 {
		next.DEKWrapped, next.DEKKeyID, next.KEKID = cur.DEKWrapped, cur.DEKKeyID, cur.KEKID
	}
	// Lineage is authority too: a re-create that moved a volume's parent would change
	// which history its reads walk through, which is the same failure one level up.
	next.ParentSnapshotID = cur.ParentSnapshotID
	next.ChainDepth = max(cur.ChainDepth, next.ChainDepth)
	return next
}

func (s *Store) GetVolume(_ context.Context, volumeID string) (metadata.Volume, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.vols[volumeID]
	if !ok {
		return metadata.Volume{}, metadata.ErrNotFound
	}
	return v, nil
}

func (s *Store) ListVolumesByHost(_ context.Context, hostID string) ([]metadata.Volume, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var vols []metadata.Volume
	for _, v := range s.vols {
		if v.PrimaryHostID == hostID {
			vols = append(vols, v)
		}
	}
	sort.Slice(vols, func(i, j int) bool { return vols[i].VolumeID < vols[j].VolumeID })
	return vols, nil
}

// ListVolumes returns every volume, placed or not, in volume-id order.
func (s *Store) ListVolumes(_ context.Context) ([]metadata.Volume, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	vols := make([]metadata.Volume, 0, len(s.vols))
	for _, v := range s.vols {
		vols = append(vols, v)
	}
	sort.Slice(vols, func(i, j int) bool { return vols[i].VolumeID < vols[j].VolumeID })
	return vols, nil
}

func (s *Store) BumpVolumeEpoch(_ context.Context, term int64, volumeID, primaryHostID string, expectedEpoch int64) (int64, error) {
	if err := requireID("volume", volumeID); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return 0, err
	}
	v, ok := s.vols[volumeID]
	if !ok {
		return 0, metadata.ErrNotFound
	}
	// Compare-and-set, not increment: the caller computed its target from the epoch
	// it read, and a volume that moved on since belongs to another promoter.
	if v.CurrentEpoch != expectedEpoch {
		return 0, fmt.Errorf("%w: volume %s is at %d, expected %d",
			metadata.ErrEpochConflict, volumeID, v.CurrentEpoch, expectedEpoch)
	}
	v.CurrentEpoch++
	v.PrimaryHostID = primaryHostID
	s.vols[volumeID] = v
	return v.CurrentEpoch, nil
}

// SetVolumePrimaryHost places a volume on a host or clears its placement, together
// with the §7 state that goes with it (metadata.PlacedState). The order of the guards
// is the contract's: term, then existence, then the domain — and within the domain the
// hand-over is diagnosed before the transition, because "detach it first" is an
// instruction the caller can act on and "invalid transition" is not.
func (s *Store) SetVolumePrimaryHost(_ context.Context, term int64, volumeID, primaryHostID string) error {
	if err := requireID("volume", volumeID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
	}
	v, ok := s.vols[volumeID]
	if !ok {
		return metadata.ErrNotFound
	}
	if v.PrimaryHostID != "" && primaryHostID != "" && v.PrimaryHostID != primaryHostID {
		return fmt.Errorf("%w: volume %s is placed on %s, detach it before placing it on %s",
			metadata.ErrAlreadyPlaced, volumeID, v.PrimaryHostID, primaryHostID)
	}
	state := metadata.PlacedState(primaryHostID)
	if err := v.State.Transition(state); err != nil {
		return err
	}
	v.PrimaryHostID = primaryHostID
	v.State = state
	// Neither ACTIVE nor DETACHED is FENCING_WAIT, and leaving that state clears the
	// dwell record (ADR-0015) so the next promotion waits its own. Same rule as
	// SetVolumeState's, spelled here rather than shared: there are two lines of it and
	// a helper would hide which write owns the field.
	v.FencingStartedAt = time.Time{}
	// And the refusal, for the same shape of reason one step out: it is a statement
	// about a host, made by that host, and this write is the volume leaving that host.
	// Left behind, a volume detached from the machine that could not open it would keep
	// printing NOT_SERVED wherever it landed next, until that host's first report
	// happened to overwrite it.
	v.Refusal, v.RefusalDetail = lifecycle.RefusalNone, ""
	s.vols[volumeID] = v
	return nil
}

// ClearVolumeParent says a volume descends from nothing any more. It is the write
// CreateVolume's COALESCE deliberately forbids to a *converging* create and that a
// flatten cannot do without; metadata.Store carries the whole reasoning.
//
// A volume that already descends from nothing is a no-op rather than an error, for
// the same reason clearing a placement twice is: an operator re-running a command
// after a timeout must not be told it failed.
func (s *Store) ClearVolumeParent(_ context.Context, term int64, volumeID string) error {
	if err := requireID("volume", volumeID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
	}
	v, ok := s.vols[volumeID]
	if !ok {
		return metadata.ErrNotFound
	}
	v.ParentSnapshotID, v.ChainDepth = "", 0
	s.vols[volumeID] = v
	return nil
}

// DeleteVolume removes a volume and its snapshots. metadata.Store carries why there
// is no DELETING state and why the snapshots go in the same write.
//
// The descendant refusal is the sim's copy of two foreign keys Postgres has and this
// store does not. It is not a convenience: without it the two implementations differ
// on the one write in this interface that destroys a row, and the DST harness would
// be proving something about a catalog production does not have.
func (s *Store) DeleteVolume(_ context.Context, term int64, volumeID string) error {
	if err := requireID("volume", volumeID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
	}
	if _, ok := s.vols[volumeID]; !ok {
		return metadata.ErrNotFound
	}
	doomed := map[string]bool{}
	for id, snap := range s.snaps {
		if snap.VolumeID == volumeID {
			doomed[id] = true
		}
	}
	// Both directions, because a snapshot can descend from a snapshot: a clone of this
	// volume's snapshot that took a snapshot of its own leaves a row in the second set
	// and not the first, and it is the one an implementation forgets — every other
	// place in this tree talks about volumes.parent_snapshot_id.
	//
	// The lowest id wins rather than whichever the map yields, so the message a
	// re-run prints is the message the first run printed (INV-02).
	var volDesc, snapDesc string
	for _, v := range s.vols {
		if v.VolumeID != volumeID && doomed[v.ParentSnapshotID] && (volDesc == "" || v.VolumeID < volDesc) {
			volDesc = v.VolumeID
		}
	}
	for _, snap := range s.snaps {
		if snap.VolumeID != volumeID && doomed[snap.ParentSnapshotID] && (snapDesc == "" || snap.SnapshotID < snapDesc) {
			snapDesc = snap.SnapshotID
		}
	}
	switch {
	case volDesc != "":
		return fmt.Errorf("%w: volume %s descends from a snapshot of %s", metadata.ErrHasDescendants, volDesc, volumeID)
	case snapDesc != "":
		return fmt.Errorf("%w: snapshot %s descends from a snapshot of %s", metadata.ErrHasDescendants, snapDesc, volumeID)
	}
	for id := range doomed {
		delete(s.snaps, id)
	}
	delete(s.vols, volumeID)
	return nil
}

// SetVolumeRefusal records, or clears, why the volume's host is not serving it.
// metadata.Store carries the whole reasoning; the two lines that matter here are the
// guard and its non-error.
func (s *Store) SetVolumeRefusal(_ context.Context, term int64, volumeID, hostID string, epoch int64,
	refusal lifecycle.Refusal, detail string,
) error {
	if err := requireID("volume", volumeID); err != nil {
		return err
	}
	if !refusal.Valid() {
		return fmt.Errorf("%w: volume refusal %q", lifecycle.ErrUnknownState, refusal)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
	}
	v, ok := s.vols[volumeID]
	if !ok {
		return metadata.ErrNotFound
	}
	// The fencing guard, and the reason this is not a read-then-write in the caller:
	// between a GetVolume and this line the volume can be promoted away, and the whole
	// point of the column is that a host the fleet has moved past cannot write it. In
	// Postgres it is one UPDATE ... WHERE primary_host_id = $ AND current_epoch = $;
	// here it is these three lines, and both must answer the same way.
	if v.PrimaryHostID != hostID || v.CurrentEpoch != epoch {
		return nil
	}
	// A detail with no refusal is a sentence about nothing — it would outlive the
	// condition it explains, which is the mistake hosts.cordon_reason exists not to
	// repeat. Cleared together, always.
	v.Refusal, v.RefusalDetail = refusal, detail
	if !refusal.Refused() {
		v.RefusalDetail = ""
	}
	s.vols[volumeID] = v
	return nil
}

func (s *Store) UpdateWatermarks(_ context.Context, term int64, volumeID string, local, durable, published int64) error {
	if err := requireID("volume", volumeID); err != nil {
		return err
	}
	if err := metadata.CheckWatermarkOrder(local, durable, published); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
	}
	v, ok := s.vols[volumeID]
	if !ok {
		return metadata.ErrNotFound
	}
	// Monotonic per column. A report from epoch N delivered after epoch N+1 has
	// published its own passes the term guard (promotion does not change the CP
	// term), so this is the only thing standing between a retry queue and a
	// durable_sequence that goes backwards during an incident. Component-wise max
	// preserves published ≤ durable ≤ local.
	v.LocalSequence = max(v.LocalSequence, local)
	v.DurableSequence = max(v.DurableSequence, durable)
	v.PublishedSequence = max(v.PublishedSequence, published)
	s.vols[volumeID] = v
	return nil
}

func (s *Store) CreateSnapshot(_ context.Context, term int64, snap metadata.Snapshot) error {
	if err := requireID("snapshot", snap.SnapshotID); err != nil {
		return err
	}
	if !snap.State.Valid() {
		return fmt.Errorf("%w: snapshot state %q", lifecycle.ErrUnknownState, snap.State)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
	}
	// INV-16: a snapshot never changes once it is in the catalog, so a duplicate
	// create — the concurrent-rebuild case — converges to a no-op rather than
	// overwriting the row or aborting the run half-way.
	if _, exists := s.snaps[snap.SnapshotID]; exists {
		return nil
	}
	s.snaps[snap.SnapshotID] = snap
	return nil
}

func (s *Store) SetSnapshotState(_ context.Context, term int64, snapshotID string, state lifecycle.SnapshotState) error {
	if err := requireID("snapshot", snapshotID); err != nil {
		return err
	}
	if !state.Valid() {
		return fmt.Errorf("%w: snapshot state %q", lifecycle.ErrUnknownState, state)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
	}
	snap, ok := s.snaps[snapshotID]
	if !ok {
		return metadata.ErrNotFound
	}
	if err := snap.State.Transition(state); err != nil {
		return err
	}
	snap.State = state
	s.snaps[snapshotID] = snap
	return nil
}

// ListPendingSnapshots returns the CREATING snapshots of the volumes this host is
// primary for, oldest first — the requests its desired state must carry.
func (s *Store) ListPendingSnapshots(_ context.Context, hostID string) ([]metadata.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []metadata.Snapshot
	for _, snap := range s.snaps {
		if snap.State != lifecycle.SnapshotCreating {
			continue
		}
		// Through the volume, not through snap.SourceHostID: the request names a
		// volume, and the host that can freeze it is whichever one serves it now.
		if v, ok := s.vols[snap.VolumeID]; !ok || v.PrimaryHostID != hostID {
			continue
		}
		out = append(out, snap)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SnapshotID < out[j].SnapshotID })
	return out, nil
}

// ListUnfinishedSnapshots returns the snapshots nothing has closed out — CREATING
// (owed by an Agent) and DELETING (owed by a reclaim ADR-0026 deleted) — fleet-wide,
// regardless of which host, if any, serves the volume they belong to.
func (s *Store) ListUnfinishedSnapshots(_ context.Context) ([]metadata.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]metadata.Snapshot, 0, len(s.snaps))
	for _, snap := range s.snaps {
		if !snap.State.Unfinished() {
			continue
		}
		out = append(out, snap)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SnapshotID < out[j].SnapshotID })
	return out, nil
}

// PublishSnapshot records what the host that took the snapshot observed.
func (s *Store) PublishSnapshot(_ context.Context, term int64, snapshotID, commitID, sourceHostID string) error {
	if err := requireID("snapshot", snapshotID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
	}
	snap, ok := s.snaps[snapshotID]
	if !ok {
		return metadata.ErrNotFound
	}
	// A second report of the same publication is the convergence working — the Agent
	// keeps reporting until the request stops arriving — so it is a no-op. A different
	// sequence at the same id is not: INV-16 says PUBLISHED never changes.
	if snap.State == lifecycle.SnapshotPublished {
		if snap.CommitID == commitID {
			return nil
		}
		return fmt.Errorf("%w: snapshot %s is PUBLISHED at commit %s, reported at %s",
			lifecycle.ErrInvalidTransition, snapshotID, snap.CommitID, commitID)
	}
	if err := snap.State.Transition(lifecycle.SnapshotPublished); err != nil {
		return err
	}
	snap.State = lifecycle.SnapshotPublished
	snap.CommitID = commitID
	snap.SourceHostID = sourceHostID
	s.snaps[snapshotID] = snap
	return nil
}

func (s *Store) GetSnapshot(_ context.Context, snapshotID string) (metadata.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, ok := s.snaps[snapshotID]
	if !ok {
		return metadata.Snapshot{}, metadata.ErrNotFound
	}
	return snap, nil
}

// SetVolumeState moves a volume through the §7 ownership machine.
func (s *Store) SetVolumeState(_ context.Context, term int64, volumeID string, state lifecycle.VolumeState) error {
	if err := requireID("volume", volumeID); err != nil {
		return err
	}
	if !state.Valid() {
		return fmt.Errorf("%w: volume state %q", lifecycle.ErrUnknownState, state)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
	}
	v, ok := s.vols[volumeID]
	if !ok {
		return metadata.ErrNotFound
	}
	if err := v.State.Transition(state); err != nil {
		return err
	}
	v.State = state
	// The fence observation lands with the state it belongs to (ADR-0015). Entering
	// FENCING_WAIT starts the dwell by this store's clock; re-entering it does not
	// move the instant, or a resumable promotion would push its own deadline forward
	// on every pass; leaving it clears the record, so the next promotion of this
	// volume waits its own dwell instead of inheriting an elapsed one.
	switch {
	case state != lifecycle.VolumeFencingWait:
		v.FencingStartedAt = time.Time{}
	case v.FencingStartedAt.IsZero():
		v.FencingStartedAt = s.now()
	}
	s.vols[volumeID] = v
	return nil
}
