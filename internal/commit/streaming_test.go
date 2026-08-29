package commit_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// refuseBufferedLayer is a store that will not take a layer through the []byte PUT. It is
// how "the sealed layer is never held whole" is asserted from outside: an implementation
// that buffered it would have to hand the bytes to Put, and this store says no.
type refuseBufferedLayer struct {
	objectstore.Store
}

var errBuffered = errors.New("the layer was handed over as one buffer")

func (s *refuseBufferedLayer) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	if strings.HasPrefix(key, commit.LayerPrefix) {
		return objectstore.PutResult{}, errBuffered
	}
	return s.Store.Put(ctx, key, data, opts)
}

func TestTheLayerReachesTheStoreAsAStream(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	base, d := sim.NewObjectStore(), dek(t, volumeID)
	plain := layerBytes(t, 4096*5+11)

	m, err := commit.Publish(t.Context(), &refuseBufferedLayer{Store: base}, d, bytes.NewReader(plain), request(volumeID))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	// And the object that landed is the object the manifest names, digest and length.
	body, err := base.Get(t.Context(), m.Layer.ObjectKey)
	if err != nil {
		t.Fatal(err)
	}
	if got := commit.Digest(body); got != m.Layer.SHA256 {
		t.Fatalf("the stored layer hashes to %s, the manifest says %s", got, m.Layer.SHA256)
	}
	if int64(len(body)) != m.Layer.SizeBytes {
		t.Fatalf("the stored layer is %d bytes, the manifest says %d", len(body), m.Layer.SizeBytes)
	}
	var out bytes.Buffer
	if err := commit.Fetch(t.Context(), base, d, m, &out); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if !bytes.Equal(out.Bytes(), plain) {
		t.Fatal("the layer did not come back as it went in")
	}
}

// driftingLayer answers with one file on the first pass and a different file of exactly
// the same length on every pass after it — a torn read, bit rot between two opens,
// anything that makes the source stop being what it was hashed as. The length is equal on
// purpose: the store's size contract catches nothing here, and the digest is the only
// thing standing between this and an object at layers/sha256/<d> that does not hash to
// <d>, which Fetch refuses for ever while Commit() said SUCCESS.
type driftingLayer struct {
	first, second []byte
	pass          int
	off           int
}

func (r *driftingLayer) body() []byte {
	if r.pass <= 1 {
		return r.first
	}
	return r.second
}

func (r *driftingLayer) Read(p []byte) (int, error) {
	if r.off >= len(r.body()) {
		return 0, io.EOF
	}
	n := copy(p, r.body()[r.off:])
	r.off += n
	return n, nil
}

func (r *driftingLayer) Seek(int64, int) (int64, error) {
	r.pass++
	r.off = 0
	return 0, nil
}

func TestALayerThatChangesBetweenPassesIsNotPublished(t *testing.T) {
	t.Parallel()
	volumeID := newID()
	store, d := sim.NewObjectStore(), dek(t, volumeID)
	plain := layerBytes(t, 4096*3)
	drifted := append([]byte(nil), plain...)
	drifted[len(drifted)-1] ^= 0x01

	req := request(volumeID)
	_, err := commit.Publish(t.Context(), store, d, &driftingLayer{first: plain, second: drifted}, req)
	if !errors.Is(err, commit.ErrSealDrifted) {
		t.Fatalf("want ErrSealDrifted, got %v", err)
	}
	// Nothing was published: no HEAD, no manifest.
	if _, err := commit.ReadHeadCommit(t.Context(), store, volumeID); !errors.Is(err, commit.ErrNoHead) {
		t.Fatalf("HEAD after a refused publish: %v", err)
	}
	if _, err := store.Get(t.Context(), commit.ManifestKey(volumeID, req.CommitID)); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("a manifest was published for a layer that was refused: %v", err)
	}
	// And the content-addressed key is free again, so the same layer can still be
	// published once the source stops moving. Leaving the drifted object there would
	// refuse this layer for ever — the key is create-only and its content is compared.
	m, err := commit.Publish(t.Context(), store, d, bytes.NewReader(plain), req)
	if err != nil {
		t.Fatalf("republishing the same layer after the refusal: %v", err)
	}
	body, err := store.Get(t.Context(), m.Layer.ObjectKey)
	if err != nil {
		t.Fatal(err)
	}
	if got := commit.Digest(body); got != m.Layer.SHA256 {
		t.Fatalf("the stored layer hashes to %s, the manifest says %s", got, m.Layer.SHA256)
	}
}

// The §28 numbers this package owns. Asserted on the values, not on the fact that
// something was recorded: a latency recorded as zero and a CAS failure counted on a
// publish that succeeded both satisfy "the series exists".
func TestPublishRecordsItsLatency(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	p, err := obs.NewTestProvider("commit-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(ctx) })

	volumeID := newID()
	clk := sim.NewClock(time.Unix(0, 0))
	store := &tickingStore{Store: sim.NewObjectStore(), clk: clk, tick: 250 * time.Millisecond}
	d := dek(t, volumeID)

	if _, err := commit.Publish(ctx, store, d, bytes.NewReader(layerBytes(t, 4096)), request(volumeID),
		commit.WithTelemetry(clk, p.Recorder())); err != nil {
		t.Fatal(err)
	}

	// Four store operations reach the clock on a first commit (the HEAD read's HEAD, the
	// layer, the manifest, the CAS), so the elapsed time is a number this test can state
	// rather than a range: 4 x 250ms.
	got, err := p.HistogramSeries(ctx, "commit_publish_latency_seconds")
	if err != nil {
		t.Fatal(err)
	}
	series := got[`{volume="`+volumeID+`"}`]
	if series.Count != 1 || series.Sum != 1 {
		t.Fatalf("commit_publish_latency_seconds = %+v, want one sample of 1s", series)
	}
	// The upload's own share of it: the layer PUT is one of the four operations, and the
	// two passes over the layer touch nothing but memory.
	up, err := p.HistogramSeries(ctx, "layer_upload_duration_seconds")
	if err != nil {
		t.Fatal(err)
	}
	if s := up[`{volume="`+volumeID+`"}`]; s.Count != 1 || s.Sum != 0.25 {
		t.Fatalf("layer_upload_duration_seconds = %+v, want one sample of 0.25s", s)
	}
}

// tickingStore advances the clock on every operation, which is the only way a duration
// this deterministic can exist at all: the sim clock does not move on its own.
type tickingStore struct {
	objectstore.Store
	clk  *sim.Clock
	tick time.Duration
}

func (s *tickingStore) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	s.clk.Advance(s.tick)
	return s.Store.Put(ctx, key, data, opts)
}

func (s *tickingStore) PutStream(ctx context.Context, key string, body io.Reader, size int64, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	s.clk.Advance(s.tick)
	return s.Store.PutStream(ctx, key, body, size, opts)
}

func (s *tickingStore) Head(ctx context.Context, key string) (objectstore.ObjectInfo, error) {
	s.clk.Advance(s.tick)
	return s.Store.Head(ctx, key)
}

func TestALostCASIsCounted(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	p, err := obs.NewTestProvider("commit-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(ctx) })

	volumeID := newID()
	store, d := sim.NewObjectStore(), dek(t, volumeID)
	clk := sim.NewClock(time.Unix(0, 0))
	rec := p.Recorder()

	// A commit that wins, so the counter has a chance to be wrong in both directions.
	if _, err := commit.Publish(ctx, store, d, bytes.NewReader(layerBytes(t, 100)), request(volumeID),
		commit.WithTelemetry(clk, rec)); err != nil {
		t.Fatal(err)
	}
	if got, _ := p.CounterSeries(ctx, "cas_failures_total"); len(got) != 0 {
		t.Fatalf("a successful publish counted a CAS failure: %v", got)
	}

	// And one that loses: HEAD moves under it between its own HEAD read and its CAS.
	req := request(volumeID)
	moved := &moveHeadBeforeCAS{Store: store, volumeID: volumeID, t: t}
	if _, err := commit.Publish(ctx, moved, d, bytes.NewReader(layerBytes(t, 100)), req,
		commit.WithTelemetry(clk, rec)); !errors.Is(err, commit.ErrHeadMoved) {
		t.Fatalf("want ErrHeadMoved, got %v", err)
	}
	got, err := p.CounterSeries(ctx, "cas_failures_total")
	if err != nil {
		t.Fatal(err)
	}
	if got[`{volume="`+volumeID+`"}`] != 1 {
		t.Fatalf("cas_failures_total = %v, want one for volume %s", got, volumeID)
	}
}

// moveHeadBeforeCAS publishes somebody else's commit in the window between this commit's
// HEAD read and its CAS — the split brain the CAS exists to catch.
type moveHeadBeforeCAS struct {
	objectstore.Store
	volumeID string
	t        *testing.T
	done     bool
}

func (s *moveHeadBeforeCAS) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	if !s.done && key == commit.HeadKey(s.volumeID) {
		s.done = true
		if err := commit.CASHead(ctx, s.Store, s.volumeID, newID(), opts.IfMatch); err != nil {
			s.t.Fatalf("the other writer's CAS: %v", err)
		}
	}
	return s.Store.Put(ctx, key, data, opts)
}
