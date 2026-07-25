package checkpoint_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/checkpoint"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// Create is two steps in a fixed order (§21.1): publish the checkpoint create-only,
// then advance published. The gap between them is a crash boundary, and it is not
// re-enterable. If the PUT persists but its response is lost — or the process dies
// after the PUT — every later attempt hits the create-only precondition and returns
// an error, so published never advances and local WAL for that volume can never be
// truncated while it stays idle. The host's NVMe fills, which stalls writes for that
// volume and for every co-tenant sharing the disk.
//
// The same precondition failure also hides the one case that is not a retry: a
// *different* checkpoint already sitting at this sequence is two writers claiming one
// epoch, and it must not look like an ordinary conflict.

// flushN writes and flushes n records, so the log's durable watermark is n.
func flushN(t *testing.T, w *checkpointWorld, n int) {
	t.Helper()
	ctx := context.Background()
	for i := range n {
		if _, err := w.log.Write(uint64(i)*4096, []byte("payload"), 0); err != nil {
			t.Fatal(err)
		}
		if err := w.log.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

// TestCreateRetriesThroughALostPublishResponse: the PUT persisted, the ACK did not.
// Retrying is the only thing the caller can do, and it has to work.
func TestCreateRetriesThroughALostPublishResponse(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	flushN(t, w, 2)

	key := checkpoint.Key(format.UUIDString(w.vol), 1, 2)
	w.store.InjectLostResponse(key)

	c := checkpoint.NewCheckpointer(w.store)
	if _, err := c.Create(ctx, w.log, w.vol, 1); err == nil {
		t.Fatal("setup: the lost response should surface as an error")
	}
	if got := w.log.Watermarks().Published; got != 0 {
		t.Fatalf("published advanced to %d on a failed publish", got)
	}

	cp, err := c.Create(ctx, w.log, w.vol, 1)
	if err != nil {
		t.Fatalf("the retry found its own checkpoint already stored and failed anyway: %v — "+
			"published stays at %d and local WAL can never be truncated", err, w.log.Watermarks().Published)
	}
	if cp.DurableSequence != 2 {
		t.Fatalf("checkpoint sequence = %d, want 2", cp.DurableSequence)
	}
	if got := w.log.Watermarks().Published; got != 2 {
		t.Fatalf("published = %d, want 2 — the retry must complete the second half of §21.1", got)
	}
	if err := w.log.TruncateLocal(2); err != nil {
		t.Fatalf("truncation is still blocked after a successful retry: %v", err)
	}
}

// TestCreateAdoptsAnIdenticalCheckpoint: the crash-after-publish shape. The process
// died between the PUT and AdvancePublished; on restart the checkpoint it would
// write is byte-for-byte the one already there.
func TestCreateAdoptsAnIdenticalCheckpoint(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	flushN(t, w, 2)

	objects, err := recovery.ObjectKeysUpTo(ctx, w.store, w.vol, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	same := checkpoint.Checkpoint{
		VolumeID:        format.UUIDString(w.vol),
		Epoch:           1,
		DurableSequence: 2,
		Objects:         objects,
		RootDigest:      checkpoint.Digest(2, objects),
	}
	if err := checkpoint.Publish(ctx, w.store, same); err != nil {
		t.Fatal(err)
	}

	cp, err := checkpoint.NewCheckpointer(w.store).Create(ctx, w.log, w.vol, 1)
	if err != nil {
		t.Fatalf("an identical published checkpoint must be adopted, not rejected: %v", err)
	}
	if cp.RootDigest != same.RootDigest {
		t.Fatalf("adopted digest %q, want %q", cp.RootDigest, same.RootDigest)
	}
	if got := w.log.Watermarks().Published; got != 2 {
		t.Fatalf("published = %d, want 2", got)
	}
}

// cpUnreadableStore lets the create-only PUT report "already there" while the object
// itself cannot be read back — a throttled GET right after the precondition failure.
type cpUnreadableStore struct {
	objectstore.Store
}

func (s cpUnreadableStore) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasPrefix(key, "checkpoints/") {
		return nil, sim.ErrThrottled
	}
	return s.Store.Get(ctx, key)
}

// TestCreateDoesNotAdoptWhatItCannotRead: "something is already at this key" is not
// enough to advance published. Adopting an unread object would unlock truncation
// against a checkpoint nobody verified.
func TestCreateDoesNotAdoptWhatItCannotRead(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	flushN(t, w, 2)

	objects, err := recovery.ObjectKeysUpTo(ctx, w.store, w.vol, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkpoint.Publish(ctx, w.store, checkpoint.Checkpoint{
		VolumeID: format.UUIDString(w.vol), Epoch: 1, DurableSequence: 2,
		Objects: objects, RootDigest: checkpoint.Digest(2, objects),
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := checkpoint.NewCheckpointer(cpUnreadableStore{Store: w.store}).Create(ctx, w.log, w.vol, 1); err == nil {
		t.Fatal("Create adopted a checkpoint it could not read")
	}
	if got := w.log.Watermarks().Published; got != 0 {
		t.Fatalf("published advanced to %d against an unread checkpoint", got)
	}
}

// TestCreateReportsAForeignCheckpointAsAConflict: a different checkpoint at the same
// sequence is not a retry. It is the signature of two writers in one epoch, and it
// must be distinguishable from an ordinary precondition failure — and it must never
// unlock truncation.
func TestCreateReportsAForeignCheckpointAsAConflict(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	flushN(t, w, 2)

	alien := checkpoint.Checkpoint{
		VolumeID:        format.UUIDString(w.vol),
		Epoch:           1,
		DurableSequence: 2,
		Objects:         []string{"wal/somebody-elses/1/0000000000000001.wal"},
	}
	alien.RootDigest = checkpoint.Digest(alien.DurableSequence, alien.Objects)
	if err := checkpoint.Publish(ctx, w.store, alien); err != nil {
		t.Fatal(err)
	}

	_, err := checkpoint.NewCheckpointer(w.store).Create(ctx, w.log, w.vol, 1)
	if err == nil {
		t.Fatal("publishing over a foreign checkpoint must fail")
	}
	if !errors.Is(err, checkpoint.ErrCheckpointConflict) {
		t.Fatalf("a foreign checkpoint at our sequence must be reported as a conflict, got %v", err)
	}
	if got := w.log.Watermarks().Published; got != 0 {
		t.Fatalf("published advanced to %d against a checkpoint this log did not write", got)
	}
	if err := w.log.TruncateLocal(2); !errors.Is(err, wal.ErrTruncateAboveDurable) {
		t.Fatalf("truncation must stay refused after a conflict, got %v", err)
	}
}
