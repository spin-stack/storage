// Package sim is the in-memory, deterministic metadata.Store used by the DST
// harness (ADR-0006). The fencing protocol (§12) is proven here under simulated
// partitions and clock drift — a real Postgres cannot be deterministic under those
// conditions. Timestamps come from an injected now-function so runs are reproducible.
package sim

import (
	"context"
	"sync"
	"time"

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
}

// New returns an empty store whose timestamps come from now (e.g. a sim clock's Wall).
func New(now func() time.Time) *Store {
	return &Store{
		now:    now,
		hosts:  map[string]metadata.Host{},
		leases: map[string]metadata.HostLease{},
		vols:   map[string]metadata.Volume{},
		ops:    map[string]metadata.Operation{},
	}
}

func (s *Store) checkTerm(term int64) error {
	if term != s.leaderTerm {
		return metadata.ErrStaleTerm
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

func (s *Store) GetLeader(_ context.Context) (metadata.Leader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.leaderTerm == 0 {
		return metadata.Leader{}, metadata.ErrNotFound
	}
	return metadata.Leader{Term: s.leaderTerm, HolderID: s.leaderHolder, RenewedAt: s.leaderAt}, nil
}

func (s *Store) UpsertHost(_ context.Context, term int64, h metadata.Host) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
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

func (s *Store) RenewHostLease(_ context.Context, term int64, hostID string, ttlSeconds int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
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

func (s *Store) CreateVolume(_ context.Context, term int64, v metadata.Volume) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
	}
	s.vols[v.VolumeID] = v
	return nil
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

func (s *Store) BumpVolumeEpoch(_ context.Context, term int64, volumeID, primaryHostID string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return 0, err
	}
	v, ok := s.vols[volumeID]
	if !ok {
		return 0, metadata.ErrNotFound
	}
	v.CurrentEpoch++
	v.PrimaryHostID = primaryHostID
	s.vols[volumeID] = v
	return v.CurrentEpoch, nil
}

func (s *Store) UpdateWatermarks(_ context.Context, term int64, volumeID string, local, durable, published int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkTerm(term); err != nil {
		return err
	}
	v, ok := s.vols[volumeID]
	if !ok {
		return metadata.ErrNotFound
	}
	v.LocalSequence, v.DurableSequence, v.PublishedSequence = local, durable, published
	s.vols[volumeID] = v
	return nil
}

func (s *Store) RecordOperation(_ context.Context, op metadata.Operation) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.ops[op.OperationID]; exists {
		return false, nil // duplicate request (§18)
	}
	s.ops[op.OperationID] = op
	return true, nil
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
