package sim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
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
	// listLag delays List-visibility by that many further store operations, which is
	// how a real eventually consistent listing behaves: it catches up on its own,
	// after a while. 0 = strongly consistent.
	listLag int
	// ops counts every store operation, so a lag can be expressed in operations
	// rather than in wall time (which the store does not have).
	ops uint64
	// one-shot faults
	lostResponse map[string]bool
	throttle     int
	keyThrottle  map[string]int
	// standing backend defects: not transient failures but a backend that does not
	// have the property the protocol assumes. Each is a configuration or conformance
	// reality (§6.1), and each disables one of the fences silently.
	permanentDelete     bool
	ignorePreconditions bool
	staleRead           map[string]bool
	// A listing that goes *backwards*: the remaining budget of stale listings and the
	// PRNG that decides how far behind each of them is (InjectStaleListing).
	staleListings int
	staleListRand *rand.Rand
}

type simObject struct {
	data      []byte
	etag      string
	listReady bool
	// wroteAt is the operation count at which this version was written. It orders the
	// objects by age for a listing that is behind the data, which loses the newest
	// writes first whatever their keys sort like.
	wroteAt uint64
	// visibleAt is the operation count from which List reports this version, when the
	// store was configured with a finite lag. 0 means "not on a lag" — either already
	// listable (listReady) or waiting for Settle (eventualList).
	visibleAt uint64
	// marked models a versioned bucket's delete marker (§21.3): the object stops
	// answering reads and listings and its bytes stay — nothing here removes them, which
	// is the structural half of INV-14.
	marked bool
	// superseded records that the object was written again *after* it was marked, so the
	// marked version is no longer what a Restore would surface (ErrRestoreSuperseded).
	superseded bool
	createdAt  time.Time
	// prev is the version this one replaced, retained only so InjectStaleRead can
	// serve it. A real backend keeps it for its own reasons (versioning, replication
	// lag); nothing outside that injector may read it.
	prev *simObject
}

// NewObjectStore returns an empty store with strongly consistent LIST.
func NewObjectStore() *ObjectStore {
	return &ObjectStore{
		objs:         map[string]*simObject{},
		lostResponse: map[string]bool{},
		keyThrottle:  map[string]int{},
		staleRead:    map[string]bool{},
	}
}

// SetClock makes Put stamp LastModified from clk, so a scenario that reads the field
// sees the simulated instant and not a zero time.
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

// SetListLag makes a freshly written key List-visible only after n further store
// operations, modelling an eventually consistent listing that catches up by itself
// (§6.1). n <= 0 restores a strongly consistent LIST.
//
// The counter is operations, not time, so a scenario can draw n from its PRNG and stay
// reproducible. SetEventualList(true) is the same thing with an unbounded lag, and takes
// precedence while it is on.
func (s *ObjectStore) SetListLag(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n < 0 {
		n = 0
	}
	s.listLag = n
}

// Settle makes all objects List-visible (models LIST catching up), whatever the
// configured lag.
func (s *ObjectStore) Settle() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, o := range s.objs {
		o.listReady = true
		o.visibleAt = 0
	}
}

// begin counts an operation. Every exported store method except List calls it under
// the lock, so a LIST lag expressed in operations advances with the scenario and
// nothing else.
func (s *ObjectStore) begin() { s.ops++ }

// listVisible reports whether List should report o now. Callers hold s.mu.
func (s *ObjectStore) listVisible(o *simObject) bool {
	if o.marked {
		return false
	}
	return o.listReady || (o.visibleAt > 0 && s.ops >= o.visibleAt)
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

// InjectThrottleKey makes the next n operations that name key return ErrThrottled,
// leaving every other key alone. It is what lets a scenario aim a fault at one step
// of a protocol — the epoch object's CAS, the boundary PUT — instead of counting how
// many unrelated reads the production code happens to make first, a coupling that
// silently relocates the fault as soon as that code adds a HEAD.
//
// List is not covered: a listing names a prefix, not a key, and a whole-store refusal
// is what InjectThrottle is for.
func (s *ObjectStore) InjectThrottleKey(key string, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keyThrottle[key] = n
}

// InjectPermanentDelete makes Delete destroy the object instead of placing a
// reversible marker: the bytes are gone and Restore reports ErrNotFound. This is a
// bucket without versioning — one checkbox, no error anywhere, and every mark the GC
// writes becomes irreversible (§21.3, INV-14). It is a standing property of the
// store, not a one-shot fault, because that is how the mistake actually presents.
func (s *ObjectStore) InjectPermanentDelete() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.permanentDelete = true
}

// InjectIgnorePreconditions makes Put accept If-None-Match and If-Match
// unconditionally, modelling a backend whose conditional writes are advisory. Every
// create-only publication in the system — snapshot manifests, epoch boundaries, the
// epoch object's CAS — is then an overwrite, and nothing reports an error (§6.1).
func (s *ObjectStore) InjectIgnorePreconditions() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ignorePreconditions = true
}

// InjectStaleRead makes Get and Head of key answer with the version that key held
// before its most recent write, until ClearStaleRead. It models a backend without
// read-after-write consistency on a small, frequently rewritten object — a replica
// serving a lagging copy. §12.4's epoch fence is exactly one such object, so this is
// the fault that quietly turns "am I still the writer?" into the wrong answer.
//
// A key with no earlier version reads as absent: to that replica the write has not
// happened at all.
func (s *ObjectStore) InjectStaleRead(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.staleRead[key] = true
}

// InjectStaleListing makes the next n listings that would return something answer from
// an index replica that is *behind the data*: each omits a non-empty set of the most
// recently written keys, r deciding how far behind it is.
//
// This is the direction SetEventualList and SetListLag cannot express — they withhold
// keys nobody has seen yet, so every number derived from a listing only ever grows. A
// real LIST offers no monotonic-read guarantee: the second of two listings can be served
// by the older replica. Everything the system derives from a listing is a number
// (recovery's contiguous prefix, the checkpointer's published point, a promotion's
// boundary), and a listing that goes backwards shrinks it with no error.
//
// The draw is taken once per affected listing, after the keys are sorted, and orders them
// by write order with the key as tie-break, so it never depends on map iteration
// (INV-02). A listing that would return nothing does not spend the budget. n <= 0 or a
// nil r is a no-op.
func (s *ObjectStore) InjectStaleListing(r *rand.Rand, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r == nil || n <= 0 {
		return
	}
	s.staleListings = n
	s.staleListRand = r
}

// dropRecentWrites removes the keys a listing served by a lagging index replica would
// not have yet. out must already be sorted by key. Callers hold s.mu.
func (s *ObjectStore) dropRecentWrites(out []objectstore.ObjectInfo) []objectstore.ObjectInfo {
	if s.staleListings <= 0 || len(out) == 0 {
		return out
	}
	s.staleListings--
	behind := 1 + s.staleListRand.Intn(len(out))

	byAge := append([]objectstore.ObjectInfo(nil), out...)
	sort.SliceStable(byAge, func(i, j int) bool {
		return s.objs[byAge[i].Key].wroteAt > s.objs[byAge[j].Key].wroteAt
	})
	missing := make(map[string]bool, behind)
	for _, o := range byAge[:behind] {
		missing[o.Key] = true
	}
	kept := make([]objectstore.ObjectInfo, 0, len(out)-behind)
	for _, o := range out {
		if !missing[o.Key] {
			kept = append(kept, o)
		}
	}
	return kept
}

// ClearStaleRead lets key catch up.
func (s *ObjectStore) ClearStaleRead(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.staleRead, key)
}

// visible returns the version a reader of key sees, honouring an injected stale read.
// Callers hold s.mu.
func (s *ObjectStore) visible(key string) (*simObject, bool) {
	o, ok := s.objs[key]
	if !ok {
		return nil, false
	}
	if s.staleRead[key] {
		if o.prev == nil {
			return nil, false
		}
		o = o.prev
	}
	if o.marked {
		return nil, false
	}
	return o, true
}

func (s *ObjectStore) throttled() bool {
	if s.throttle > 0 {
		s.throttle--
		return true
	}
	return false
}

// throttledKey consumes one refusal budgeted for key. A refusal happens before any
// state changes, so a throttled PUT leaves nothing behind.
func (s *ObjectStore) throttledKey(key string) bool {
	n, ok := s.keyThrottle[key]
	if !ok || n <= 0 {
		return false
	}
	if n == 1 {
		delete(s.keyThrottle, key)
	} else {
		s.keyThrottle[key] = n - 1
	}
	return true
}

func simEtag(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (s *ObjectStore) Put(_ context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.begin()
	if s.throttled() || s.throttledKey(key) {
		return objectstore.PutResult{}, ErrThrottled
	}
	return s.put(key, data, opts)
}

// PutStream reads the body first and then stores it exactly as Put does: the store is in
// memory, so there is nothing to stream to, and the value of the method here is that a
// scenario exercises the same code path production takes.
//
// The refusals come before the body is touched, which is what a real backend does with a
// throttled or precondition-failed request, and what keeps a fault injected at a key from
// depending on how the caller produces its bytes.
func (s *ObjectStore) PutStream(_ context.Context, key string, body io.Reader, size int64, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.begin()
	if s.throttled() || s.throttledKey(key) {
		return objectstore.PutResult{}, ErrThrottled
	}
	if size < 0 {
		return objectstore.PutResult{}, fmt.Errorf("simio/sim: a body of %d bytes is not a body", size)
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(body, data); err != nil {
		return objectstore.PutResult{}, fmt.Errorf("simio/sim: reading %d bytes for %s: %w", size, key, err)
	}
	return s.put(key, data, opts)
}

// put is the half of a PUT that touches the map. Callers hold s.mu and have already
// counted the operation and consumed any injected fault.
func (s *ObjectStore) put(key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	existing, exists := s.objs[key]
	rewroteAMarkedKey := exists && existing.marked
	if rewroteAMarkedKey {
		// A delete marker is the latest version: to a conditional write the object
		// does not exist, which is how S3 behaves on a versioned bucket.
		exists = false
	}
	if !s.ignorePreconditions {
		if opts.IfNoneMatch && exists {
			return objectstore.PutResult{}, objectstore.ErrPreconditionFailed
		}
		if opts.IfMatch != "" && (!exists || existing.etag != opts.IfMatch) {
			return objectstore.PutResult{}, objectstore.ErrPreconditionFailed
		}
	}

	stored := &simObject{
		data:       append([]byte(nil), data...),
		etag:       simEtag(data),
		listReady:  !s.eventualList && s.listLag == 0,
		wroteAt:    s.ops,
		superseded: rewroteAMarkedKey,
		createdAt:  s.now(),
	}
	if !s.eventualList && s.listLag > 0 {
		stored.visibleAt = s.ops + uint64(s.listLag)
	}
	if existing != nil {
		// Keep one older version so a stale read has something to serve; drop its own
		// chain so the retained history stays bounded.
		older := *existing
		older.prev = nil
		stored.prev = &older
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
	s.begin()
	if s.throttled() || s.throttledKey(key) {
		return nil, ErrThrottled
	}
	o, ok := s.visible(key)
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return append([]byte(nil), o.data...), nil
}

// GetStream is Get with the bytes behind a reader. It goes through the same visibility,
// throttling and fault surface, because a scenario that could fault Get and not this one
// would leave the whole recovery path unsimulated.
func (s *ObjectStore) GetStream(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	data, err := s.Get(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}

func (s *ObjectStore) Head(_ context.Context, key string) (objectstore.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.begin()
	if s.throttled() || s.throttledKey(key) {
		return objectstore.ObjectInfo{}, ErrThrottled
	}
	o, ok := s.visible(key)
	if !ok {
		return objectstore.ObjectInfo{}, objectstore.ErrNotFound
	}
	return objectstore.ObjectInfo{Key: key, Size: int64(len(o.data)), ETag: o.etag, LastModified: o.createdAt}, nil
}

// List does not advance the lag counter: observing a backend does not make it catch
// up, and a caller that polls the listing must not be able to hurry it along.
func (s *ObjectStore) List(_ context.Context, prefix string) ([]objectstore.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.throttled() {
		return nil, ErrThrottled
	}
	var out []objectstore.ObjectInfo
	for key, o := range s.objs {
		if !s.listVisible(o) || !strings.HasPrefix(key, prefix) {
			continue
		}
		out = append(out, objectstore.ObjectInfo{
			Key: key, Size: int64(len(o.data)), ETag: o.etag, LastModified: o.createdAt,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return s.dropRecentWrites(out), nil
}

// Delete places a delete marker over the object (§21.3); the bytes remain and Restore
// brings them back (INV-14).
func (s *ObjectStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.begin()
	if s.throttled() || s.throttledKey(key) {
		return ErrThrottled
	}
	o, ok := s.objs[key]
	if !ok || o.marked {
		return objectstore.ErrNotFound
	}
	if s.permanentDelete {
		// No versioning: the bytes are gone and nothing can bring them back.
		delete(s.objs, key)
		return nil
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
	s.begin()
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
