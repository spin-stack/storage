package sim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strings"
	"sync"

	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// ErrLostResponse models a PUT that persisted server-side but whose response was
// lost in transit — the §14.5 / §23 case that idempotent retry + HEAD must
// resolve. ErrThrottled models coordinated backend throttling (§24).
var (
	ErrLostResponse = errors.New("simio/sim: put response lost")
	ErrThrottled    = errors.New("simio/sim: throttled")
)

// ObjectStore is a deterministic in-memory object store with strong GET/HEAD
// read-after-write and (optionally) eventually consistent LIST, plus injectable
// lost responses and throttling.
type ObjectStore struct {
	mu   sync.Mutex
	objs map[string]*simObject
	// LIST consistency: when eventual, keys become List-visible only after Settle.
	eventualList bool
	// one-shot faults
	lostResponse map[string]bool
	throttle     int
}

type simObject struct {
	data      []byte
	etag      string
	listReady bool
}

// NewObjectStore returns an empty store with strongly consistent LIST.
func NewObjectStore() *ObjectStore {
	return &ObjectStore{
		objs:         map[string]*simObject{},
		lostResponse: map[string]bool{},
	}
}

// SetEventualList toggles eventually consistent LIST. When on, a freshly Put key
// is not returned by List until Settle is called (GET/HEAD still see it).
func (s *ObjectStore) SetEventualList(eventual bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eventualList = eventual
}

// Settle makes all objects List-visible (models LIST catching up).
func (s *ObjectStore) Settle() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, o := range s.objs {
		o.listReady = true
	}
}

// InjectLostResponse makes the next Put to key persist but return ErrLostResponse.
func (s *ObjectStore) InjectLostResponse(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lostResponse[key] = true
}

// InjectThrottle makes the next n operations return ErrThrottled.
func (s *ObjectStore) InjectThrottle(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.throttle = n
}

func (s *ObjectStore) throttled() bool {
	if s.throttle > 0 {
		s.throttle--
		return true
	}
	return false
}

func simEtag(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (s *ObjectStore) Put(_ context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.throttled() {
		return objectstore.PutResult{}, ErrThrottled
	}
	existing, exists := s.objs[key]
	if opts.IfNoneMatch && exists {
		return objectstore.PutResult{}, objectstore.ErrPreconditionFailed
	}
	if opts.IfMatch != "" && (!exists || existing.etag != opts.IfMatch) {
		return objectstore.PutResult{}, objectstore.ErrPreconditionFailed
	}

	stored := &simObject{
		data:      append([]byte(nil), data...),
		etag:      simEtag(data),
		listReady: !s.eventualList,
	}
	s.objs[key] = stored

	if s.lostResponse[key] {
		delete(s.lostResponse, key)
		// Persisted, but the caller sees a lost response and must retry.
		return objectstore.PutResult{}, ErrLostResponse
	}
	return objectstore.PutResult{ETag: stored.etag}, nil
}

func (s *ObjectStore) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.throttled() {
		return nil, ErrThrottled
	}
	o, ok := s.objs[key]
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return append([]byte(nil), o.data...), nil
}

func (s *ObjectStore) Head(_ context.Context, key string) (objectstore.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.throttled() {
		return objectstore.ObjectInfo{}, ErrThrottled
	}
	o, ok := s.objs[key]
	if !ok {
		return objectstore.ObjectInfo{}, objectstore.ErrNotFound
	}
	return objectstore.ObjectInfo{Key: key, Size: int64(len(o.data)), ETag: o.etag}, nil
}

func (s *ObjectStore) List(_ context.Context, prefix string) ([]objectstore.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.throttled() {
		return nil, ErrThrottled
	}
	var out []objectstore.ObjectInfo
	for key, o := range s.objs {
		if !o.listReady || !strings.HasPrefix(key, prefix) {
			continue
		}
		out = append(out, objectstore.ObjectInfo{Key: key, Size: int64(len(o.data)), ETag: o.etag})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (s *ObjectStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.throttled() {
		return ErrThrottled
	}
	if _, ok := s.objs[key]; !ok {
		return objectstore.ErrNotFound
	}
	delete(s.objs, key)
	return nil
}
