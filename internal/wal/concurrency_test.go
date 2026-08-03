package wal_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// A Log serves two callers that do not take turns: the guest's virtqueue loop, which
// writes and reads, and the Agent's reconciliation, which flushes, checkpoints and
// truncates. Nothing has caught this so far because nothing has been concurrent —
// vhost.queueLoop is one goroutine per virtqueue and the device offers one queue — so
// every Backend call has been serialized by construction. The moment the Agent drives
// a checkpoint on a live volume that stops being true.
//
// These tests are the specification of what "safe for concurrent use" has to mean
// here. Two of them are about races; the other two are about what the lock may NOT
// do, which is the harder half: a mutex held across an object-store PUT would put S3
// latency in the guest's WRITE path (§5.3, INV-18) and would stop the Agent reading
// the watermarks exactly when an operator needs them most — during an S3 stall, when
// the gap those watermarks report is the RPO that is growing.

// gateStore serialises access to a sim store and, when armed, holds every Put until
// it is released. The sim store is single-goroutine by design (it is the DST
// harness's), so the mutex is what makes it usable from a test with real goroutines;
// a production store is concurrency-safe already.
type gateStore struct {
	mu    sync.Mutex
	inner *sim.ObjectStore

	// arrived receives the key of every Put that reaches the gate; release lets the
	// blocked Puts through. Both nil = pass straight through.
	arrived chan string
	release chan struct{}

	puts map[string]int
}

func newGateStore(blocking bool) *gateStore {
	g := &gateStore{inner: sim.NewObjectStore(), puts: map[string]int{}}
	if blocking {
		g.arrived = make(chan string, 16)
		g.release = make(chan struct{})
	}
	return g
}

func (g *gateStore) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	if g.arrived != nil {
		g.arrived <- key
		<-g.release
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.puts[key]++
	return g.inner.Put(ctx, key, data, opts)
}

func (g *gateStore) Get(ctx context.Context, key string) ([]byte, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.inner.Get(ctx, key)
}

func (g *gateStore) Head(ctx context.Context, key string) (objectstore.ObjectInfo, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.inner.Head(ctx, key)
}

func (g *gateStore) List(ctx context.Context, prefix string) ([]objectstore.ObjectInfo, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.inner.List(ctx, prefix)
}

func (g *gateStore) Delete(ctx context.Context, key string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.inner.Delete(ctx, key)
}

func (g *gateStore) Restore(ctx context.Context, key string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.inner.Restore(ctx, key)
}

// putCount reports how many times a key was written — the evidence for "each batch
// was uploaded once".
func (g *gateStore) putCount() map[string]int {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]int, len(g.puts))
	for k, v := range g.puts {
		out[k] = v
	}
	return out
}

// concurrentLog builds a log for the concurrency arms. It used to be remote and leased;
// with one ACK contract (§14.8) there is only one kind of log, and what these tests are
// about — that mu is not held where it must not be — is unchanged.
func concurrentLog(t *testing.T, store objectstore.Store, clk *sim.Clock, lm *lease.Manager) *wal.Log {
	t.Helper()
	vol := [16]byte{9}
	l := wal.NewLog(sim.NewDisk(), "wal", clk, vol, 1, wal.Limits{})
	return l
}

func grantedLease(clk *sim.Clock) *lease.Manager {
	lm := lease.NewManager(clk, time.Hour)
	lm.Grant()
	return lm
}

// TestConcurrentGuestIOAndAgentCheckpointDoNotRace drives the exact shape that is
// about to become real: the guest's queue loop writing and reading while the Agent
// flushes, publishes and truncates, plus a heartbeat reading the watermarks. Run
// under -race (task ci does), an unguarded Log fails this on the first pass.
//
// The assertion at the end matters as much as the race detector: whatever the
// interleaving, the watermarks must still come out ordered (INV-03). A torn read of
// the trio would satisfy the race detector and still be wrong.
func TestConcurrentGuestIOAndAgentCheckpointDoNotRace(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	l := concurrentLog(t, newGateStore(false), clk, grantedLease(clk))

	var wg sync.WaitGroup
	wg.Add(4)

	// The guest: writes and reads through the virtqueue.
	go func() {
		defer wg.Done()
		buf := make([]byte, 512)
		for i := range 300 {
			_, _ = l.Write(uint64(i%64)*512, []byte("payload-from-the-guest"), 0)
			_ = l.Read(uint64(i%64)*512, buf) // an unlayered log never fails a read
		}
	}()

	// The Agent: makes writes durable.
	go func() {
		defer wg.Done()
		for range 60 {
			_ = l.Flush(ctx)
		}
	}()

	// The Agent: checkpoints and reclaims. Errors are expected and ignored — a stale
	// read of `durable` simply means this pass publishes nothing.
	go func() {
		defer wg.Done()
		for range 60 {
			w := l.Watermarks()
			_ = l.AdvancePublished(w.Durable)
			_ = l.TruncateLocal(l.Watermarks().Published)
		}
	}()

	// The heartbeat: reports device and volume state upward.
	go func() {
		defer wg.Done()
		for range 300 {
			_ = l.Watermarks()
			_ = l.Broken()
			_ = l.UnflushedBytes()
			_, _ = l.LocalBytes()
		}
	}()

	wg.Wait()

	w := l.Watermarks()
	if w.Published > w.Durable || w.Durable > w.Local {
		t.Fatalf("watermarks left unordered by concurrent use (INV-03): %+v", w)
	}
}

// TestConcurrentFlushesUploadEachBatchOnce covers what releasing the lock around the
// upload would otherwise break. Two flushes that both read the pending list and both
// account for having drained it would drop batches nobody uploaded — the pending list
// is consumed by position, so a double removal discards records that exist on this
// host alone.
//
// Idempotent PUT (§14.5) makes a duplicate upload harmless in the store; it does not
// make a double removal harmless in the batcher. Hence: durable steps are serialized.
func TestConcurrentFlushesUploadEachBatchOnce(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	store := newGateStore(false)
	l := concurrentLog(t, store, clk, grantedLease(clk))

	for i := range 8 {
		if _, err := l.Write(uint64(i)*512, []byte("record"), 0); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = l.Flush(ctx)
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent flush %d: %v", i, err)
		}
	}
	for key, n := range store.putCount() {
		if n != 1 {
			t.Errorf("object %s was uploaded %d times; concurrent flushes must not re-upload a drained batch", key, n)
		}
	}
	if w := l.Watermarks(); w.Durable != w.Local {
		t.Fatalf("after four concurrent flushes durable=%d local=%d: a batch was dropped without being uploaded", w.Durable, w.Local)
	}
}
