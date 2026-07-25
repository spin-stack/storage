package sim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

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
	// clk stamps object modification times so the GC's grace period is testable and
	// deterministic; the zero value keeps the old behaviour (an unset clock stamps
	// the zero time, which is simply "old").
	clk  *Clock
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
	// marked models a versioned bucket's delete marker (§21.3): the object stops
	// answering reads and listings, and its bytes stay until the lifecycle sweeps
	// them. Nothing in this package removes a marked object's data — that is the
	// structural half of INV-14.
	marked bool
	// superseded records that the object was written again *after* it was marked, so
	// the marked version is no longer what a Restore would surface. Answering such a
	// restore with the newer bytes is the one outcome an operator must never get:
	// they believe the un-GC runbook worked and rebuild a volume from content that
	// was never the content that was marked (§21.3).
	superseded bool
	createdAt  time.Time
}

// NewObjectStore returns an empty store with strongly consistent LIST.
func NewObjectStore() *ObjectStore {
	return &ObjectStore{
		objs:         map[string]*simObject{},
		lostResponse: map[string]bool{},
	}
}

// SetClock makes Put stamp LastModified from clk (§21.3 grace period).
func (s *ObjectStore) SetClock(clk *Clock) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clk = clk
}

func (s *ObjectStore) now() time.Time {
	if s.clk == nil {
		return time.Time{}
	}
	return s.clk.Wall()
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
	rewroteAMarkedKey := exists && existing.marked
	if rewroteAMarkedKey {
		// A delete marker is the latest version: to a conditional write the object
		// does not exist, which is how S3 behaves on a versioned bucket.
		exists = false
	}
	if opts.IfNoneMatch && exists {
		return objectstore.PutResult{}, objectstore.ErrPreconditionFailed
	}
	if opts.IfMatch != "" && (!exists || existing.etag != opts.IfMatch) {
		return objectstore.PutResult{}, objectstore.ErrPreconditionFailed
	}

	stored := &simObject{
		data:       append([]byte(nil), data...),
		etag:       simEtag(data),
		listReady:  !s.eventualList,
		superseded: rewroteAMarkedKey,
		createdAt:  s.now(),
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
	if !ok || o.marked {
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
	if !ok || o.marked {
		return objectstore.ObjectInfo{}, objectstore.ErrNotFound
	}
	return objectstore.ObjectInfo{Key: key, Size: int64(len(o.data)), ETag: o.etag, LastModified: o.createdAt}, nil
}

func (s *ObjectStore) List(_ context.Context, prefix string) ([]objectstore.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.throttled() {
		return nil, ErrThrottled
	}
	var out []objectstore.ObjectInfo
	for key, o := range s.objs {
		if o.marked {
			continue
		}
		if !o.listReady || !strings.HasPrefix(key, prefix) {
			continue
		}
		out = append(out, objectstore.ObjectInfo{
			Key: key, Size: int64(len(o.data)), ETag: o.etag, LastModified: o.createdAt,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// Delete places a delete marker over the object (§21.3). It never destroys data: the
// bytes remain and Restore brings them back, which is what makes a GC mistake
// survivable (INV-14). Permanent removal belongs to the bucket lifecycle, which this
// interface deliberately cannot reach.
func (s *ObjectStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.throttled() {
		return ErrThrottled
	}
	o, ok := s.objs[key]
	if !ok || o.marked {
		return objectstore.ErrNotFound
	}
	o.marked = true
	// This version is now the one a restore would bring back.
	o.superseded = false
	return nil
}

// Restore removes the delete marker, the operator action behind "un-GC this"
// (INV-14). ErrNotFound if the key carries no marker; ErrRestoreSuperseded if it was
// written again after being marked, because the marked version is then no longer
// what a restore surfaces and returning the newer bytes would be silently wrong.
func (s *ObjectStore) Restore(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objs[key]
	if !ok {
		return objectstore.ErrNotFound
	}
	if o.superseded {
		return fmt.Errorf("%w: %s", objectstore.ErrRestoreSuperseded, key)
	}
	if !o.marked {
		return objectstore.ErrNotFound
	}
	o.marked = false
	return nil
}

// Marked reports whether a delete marker covers the key (for checkers and tests).
func (s *ObjectStore) Marked(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objs[key]
	return ok && o.marked
}
