// Package sim is the in-memory, deterministic metadata.Store used by the DST
// harness (ADR-0006). The fencing protocol (§12) is proven here under simulated
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
	ops    map[string]metadata.Operation
	snaps  map[string]metadata.Snapshot
}

// New returns an empty store whose timestamps come from now (e.g. a sim clock's Wall).
func New(now func() time.Time) *Store {
	return &Store{
		now:    now,
		hosts:  map[string]metadata.Host{},
		leases: map[string]metadata.HostLease{},
		vols:   map[string]metadata.Volume{},
		ops:    map[string]metadata.Operation{},
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
	// A heartbeat refreshes what the host knows about itself. The fleet state and
	// the committed-capacity ledger are the Control Plane's (§28.1/§28.2): letting a
	// heartbeat carry them un-cordons a host that is being drained and zeroes the
	// ledger the drain is about to release against.
	if cur, exists := s.hosts[h.HostID]; exists {
		h.State = cur.State
		h.NVMeCommittedBytes = cur.NVMeCommittedBytes
	}
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
	return h, nil
}

func (s *Store) ListHosts(_ context.Context) ([]metadata.Host, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	hosts := make([]metadata.Host, 0, len(s.hosts))
	for _, h := range s.hosts {
		hosts = append(hosts, h)
	}
	sort.Slice(hosts, func(i, j int) bool { return hosts[i].HostID < hosts[j].HostID })
	return hosts, nil
}

func (s *Store) SetHostState(_ context.Context, term int64, hostID string, state lifecycle.HostState) error {
	if err := requireID("host", hostID); err != nil {
		return err
	}
	if !state.Valid() {
		return fmt.Errorf("%w: host state %q", lifecycle.ErrUnknownState, state)
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
	h.State = state
	s.hosts[hostID] = h
	return nil
}

// CommitHostCapacity applies one change to the ledger under the lock, so the rules
// the pg adapter states as predicates hold here for the same reason: the read and
// the write are one step. The diagnosis order is the contract's — the expectation
// first, because when the ledger is not where the caller last saw it, everything
// else the caller computed from that read (the bound included) is about another
// world; then the underflow; then the bound.
func (s *Store) CommitHostCapacity(_ context.Context, term int64, hostID string, c metadata.CapacityChange) error {
	if err := requireID("host", hostID); err != nil {
		return err
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
	after := h.NVMeCommittedBytes + c.DeltaBytes
	switch {
	case c.Expect != nil && *c.Expect != h.NVMeCommittedBytes:
		return fmt.Errorf("%w: host %s holds %d committed bytes, the change expected %d",
			metadata.ErrCapacityConflict, hostID, h.NVMeCommittedBytes, *c.Expect)
	case after < 0:
		return metadata.ErrCapacityUnderflow
	case c.DeltaBytes > 0 && after > c.Limit:
		return fmt.Errorf("%w: host %s would hold %d committed bytes, the policy admits %d",
			metadata.ErrCapacityExceeded, hostID, after, c.Limit)
	}
	h.NVMeCommittedBytes = after
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

// RevokeHostLease drops a host's lease. Idempotent: no lease is the state asked for.
func (s *Store) RevokeHostLease(_ context.Context, term int64, hostID string) error {
	if err := requireID("host", hostID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
	}
	if _, exists := s.hosts[hostID]; !exists {
		return metadata.ErrNotFound
	}
	delete(s.leases, hostID)
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

func (s *Store) CreateVolume(_ context.Context, term int64, v metadata.Volume) error {
	if err := requireID("volume", v.VolumeID); err != nil {
		return err
	}
	if !v.State.Valid() {
		return fmt.Errorf("%w: volume state %q", lifecycle.ErrUnknownState, v.State)
	}
	if v.Durability == "" {
		v.Durability = lifecycle.DurabilityRemote // the §14.8 default, as in the DB
	}
	if !v.Durability.Valid() {
		return fmt.Errorf("%w: durability %q", lifecycle.ErrUnknownState, v.Durability)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
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
	next.SizeBytes = max(cur.SizeBytes, next.SizeBytes) // §3: grow-only
	next.LocalSequence = max(cur.LocalSequence, next.LocalSequence)
	next.DurableSequence = max(cur.DurableSequence, next.DurableSequence)
	next.PublishedSequence = max(cur.PublishedSequence, next.PublishedSequence)
	next.State = cur.State // moves only through SetVolumeState (§7)
	if cur.PrimaryHostID != "" {
		next.PrimaryHostID = cur.PrimaryHostID
	}
	if cur.StandbyHostID != "" {
		next.StandbyHostID = cur.StandbyHostID
	}
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

func (s *Store) UpdateWatermarks(_ context.Context, term int64, volumeID string, local, durable, published int64) error {
	if err := requireID("volume", volumeID); err != nil {
		return err
	}
	if published > durable || durable > local {
		return fmt.Errorf("%w: published=%d durable=%d local=%d",
			metadata.ErrWatermarkOrder, published, durable, local)
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

func (s *Store) ResizeVolume(_ context.Context, term int64, volumeID string, newSizeBytes int64) error {
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
	if newSizeBytes < v.SizeBytes {
		return metadata.ErrShrinkNotAllowed
	}
	v.SizeBytes = newSizeBytes
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
	s.vols[volumeID] = v
	return nil
}

func (s *Store) RecordOperation(_ context.Context, term int64, op metadata.Operation) (bool, error) {
	if err := requireID("operation", op.OperationID); err != nil {
		return false, err
	}
	if !op.Kind.Valid() {
		return false, fmt.Errorf("%w: operation kind %q", lifecycle.ErrUnknownState, op.Kind)
	}
	if !op.Phase.Valid() {
		return false, fmt.Errorf("%w: operation phase %q", lifecycle.ErrUnknownState, op.Phase)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return false, err
	}
	if _, exists := s.ops[op.OperationID]; exists {
		return false, nil // duplicate request (§18)
	}
	s.ops[op.OperationID] = op
	return true, nil
}

func (s *Store) UpdateOperation(_ context.Context, term int64, op metadata.Operation) error {
	if err := requireID("operation", op.OperationID); err != nil {
		return err
	}
	if !op.Phase.Valid() {
		return fmt.Errorf("%w: operation phase %q", lifecycle.ErrUnknownState, op.Phase)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
	}
	cur, ok := s.ops[op.OperationID]
	if !ok {
		return metadata.ErrNotFound
	}
	if err := cur.Phase.Transition(op.Phase); err != nil {
		return err
	}
	cur.Phase, cur.CurrentState, cur.Error = op.Phase, op.CurrentState, op.Error
	s.ops[op.OperationID] = cur
	return nil
}

func (s *Store) ListOperationsByHost(_ context.Context, hostID string) ([]metadata.Operation, error) {
	// An empty id would match every operation recorded with no host at all, which is
	// the opposite of what any caller of this means (in Postgres host_id is NULL for
	// those, and NULL matches nothing).
	if err := requireID("host", hostID); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var ops []metadata.Operation
	for _, op := range s.ops {
		if op.HostID == hostID {
			ops = append(ops, op)
		}
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i].OperationID < ops[j].OperationID })
	return ops, nil
}

func (s *Store) GetOperation(_ context.Context, operationID string) (metadata.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.ops[operationID]
	if !ok {
		return metadata.Operation{}, metadata.ErrNotFound
	}
	return op, nil
}
