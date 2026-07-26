package checkpoint_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/checkpoint"
	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// A checkpoint is two exclusive acts at once: it writes into checkpoints/<vol>/<epoch>/,
// a namespace exactly one host may claim, and it advances published — the number that
// authorises deleting the local copy of the data (INV-13, §21.1).
//
// The epoch *number* cannot establish that exclusivity. The two durable records of a
// promotion — volumes.primary_host_id in PostgreSQL and the S3 epoch object — are
// written by two different steps of §12.3, so a promoter that died between them, or
// one that was overtaken, leaves them naming different hosts at the same epoch. Both
// hosts then read "epoch 1" and both are right about the number; only one of them was
// granted it. Wave 2 put the holder in the object (epoch.Record.HolderID,
// epoch.VerifyHolder); these tests are what makes the checkpoint publisher use it.
//
// The gap is expressed through an optional interface, so a missing gate fails as a
// test rather than as a build break — the idiom internal/epoch/holder_test.go uses.

const (
	hostGranted = "00000000-0000-7000-8000-0000000000a1" // the host the epoch was granted to
	hostNamedPG = "00000000-0000-7000-8000-0000000000a2" // the host a stale PostgreSQL row names
)

// holderScoped is the publisher identity a checkpointer needs: which host it is
// publishing for. Without it a checkpointer can only ask "is the number still mine?",
// which both hosts above answer yes to.
type holderScoped interface {
	HeldBy(hostID string) *checkpoint.Checkpointer
}

func heldBy(t *testing.T, c *checkpoint.Checkpointer, hostID string) *checkpoint.Checkpointer {
	t.Helper()
	h, ok := any(c).(holderScoped)
	if !ok {
		t.Fatal("checkpoint.Checkpointer cannot say which host it publishes for: no HeldBy (§12.4). " +
			"A checkpoint is therefore gated on the epoch number alone, so the host a crashed " +
			"promoter left named in volumes.primary_host_id publishes into the epoch the object " +
			"store granted to somebody else — and advances published, authorising it to truncate " +
			"local WAL (INV-13)")
	}
	return h.HeldBy(hostID)
}

// grantEpoch puts the epoch object at `ep`, granted to holder. An empty holder leaves
// the epoch owned by nobody, which is what Init writes for a volume that has just been
// created or rebuilt (§22.5).
func grantEpoch(t *testing.T, store objectstore.Store, volumeID string, ep uint64, holder string) {
	t.Helper()
	ctx := context.Background()
	es := epoch.NewStore(store)
	etag, err := es.Init(ctx, volumeID, ep-1)
	if err != nil {
		t.Fatalf("init epoch %d: %v", ep-1, err)
	}
	if _, err := es.Grant(ctx, volumeID, etag, ep, holder); err != nil {
		t.Fatalf("granting epoch %d to %q: %v", ep, holder, err)
	}
}

// checkpointWorldAt is a volume with two flushed WAL objects under epoch 1, ready to
// checkpoint.
func checkpointWorldAt(t *testing.T) *checkpointWorld {
	t.Helper()
	w := newWorld(t)
	for i := range 2 {
		if _, err := w.log.Write(uint64(i)*4096, []byte("payload"), 0); err != nil {
			t.Fatal(err)
		}
		if err := w.log.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	return w
}

// TestAnUnnamedPublisherCannotPublishIntoAHeldEpoch is the hole itself, and it needs
// none of the new surface to state: the epoch object says epoch 1 was granted to
// hostGranted, and a checkpointer that cannot prove it is that host publishes into
// checkpoints/<vol>/1/ anyway, because the only question ever asked is the number.
// It then advances published, which is what authorises discarding the local WAL.
func TestAnUnnamedPublisherCannotPublishIntoAHeldEpoch(t *testing.T) {
	ctx := context.Background()
	w := checkpointWorldAt(t)
	grantEpoch(t, w.store, format.UUIDString(v7Vol()), 1, hostGranted)

	_, err := checkpoint.NewCheckpointer(w.store).Create(ctx, w.log, w.vol, 1)
	if !errors.Is(err, epoch.ErrNotHolder) {
		t.Fatalf("Create = %v, want %v: a publisher that cannot name itself must not claim "+
			"an epoch the object store granted to %s", err, epoch.ErrNotHolder, hostGranted)
	}
	if got := w.log.Watermarks().Published; got != 0 {
		t.Fatalf("published advanced to %d on a publication this host does not own (INV-13)", got)
	}
}

// TestOnlyTheEpochHolderMayPublishACheckpoint is the difference the number-only check
// cannot see: same volume, same epoch number, two hosts, one grant.
//
// Two of the cases must stay open, and they are the reason this is not simply
// "VerifyHolder everywhere". An epoch nobody was granted is legitimate — Init writes
// one for a fresh or rebuilt volume (§22.5) and its first writer has to be able to
// checkpoint, or the volume's local WAL can never be truncated and the host's NVMe
// fills. So is a volume with no epoch object at all: §12.4 makes the object defence in
// depth over §12.2 + the §7 term, not a precondition for publishing.
func TestOnlyTheEpochHolderMayPublishACheckpoint(t *testing.T) {
	vid := format.UUIDString(v7Vol())
	tests := []struct {
		name      string
		arrange   func(t *testing.T, store objectstore.Store)
		publisher string
		want      error
	}{
		{
			name:      "the host the epoch was granted to",
			arrange:   func(t *testing.T, s objectstore.Store) { grantEpoch(t, s, vid, 1, hostGranted) },
			publisher: hostGranted,
		},
		{
			name:      "the host a stale PostgreSQL row names, at the very same epoch",
			arrange:   func(t *testing.T, s objectstore.Store) { grantEpoch(t, s, vid, 1, hostGranted) },
			publisher: hostNamedPG,
			want:      epoch.ErrNotHolder,
		},
		{
			name:      "the holder, at an epoch that has since moved on",
			arrange:   func(t *testing.T, s objectstore.Store) { grantEpoch(t, s, vid, 2, hostGranted) },
			publisher: hostGranted,
			want:      epoch.ErrEpochChanged,
		},
		{
			name:      "an epoch nobody was granted (a fresh or rebuilt volume, §22.5)",
			arrange:   func(t *testing.T, s objectstore.Store) { grantEpoch(t, s, vid, 1, "") },
			publisher: hostGranted,
		},
		{
			name:      "no epoch object at all (§12.4: defence in depth, not a precondition)",
			arrange:   func(t *testing.T, s objectstore.Store) {},
			publisher: hostGranted,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			w := checkpointWorldAt(t)
			tc.arrange(t, w.store)

			c := heldBy(t, checkpoint.NewCheckpointer(w.store), tc.publisher)
			_, err := c.Create(ctx, w.log, w.vol, 1)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Create = %v, want %v", err, tc.want)
			}
			if tc.want == nil {
				return
			}
			// A refused publication must leave nothing behind: no object in the
			// epoch's namespace, and no authority to discard the local copy.
			objs, lerr := w.store.List(ctx, "checkpoints/")
			if lerr != nil {
				t.Fatal(lerr)
			}
			if len(objs) != 0 {
				t.Fatalf("a refused checkpoint still published %d object(s): %v", len(objs), objs)
			}
			if got := w.log.Watermarks().Published; got != 0 {
				t.Fatalf("a refused checkpoint advanced published to %d — that authorises truncating "+
					"local WAL over data this host does not own (INV-13)", got)
			}
		})
	}
}

// fenceOnPublish grants the epoch away the moment the checkpoint object lands, which
// is the window a check made only *before* publishing cannot cover: the PUT succeeded,
// and what happens next is a local truncation authorised by a publication the writer
// no longer owns.
type fenceOnPublish struct {
	objectstore.Store
	volumeID string
	fired    bool
}

func (s *fenceOnPublish) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	res, err := s.Store.Put(ctx, key, data, opts)
	if err != nil || s.fired || !strings.HasPrefix(key, "checkpoints/") {
		return res, err
	}
	s.fired = true
	es := epoch.NewStore(s.Store)
	_, etag, gerr := es.Current(ctx, s.volumeID)
	if gerr != nil {
		return res, gerr
	}
	if _, gerr := es.Grant(ctx, s.volumeID, etag, 2, hostNamedPG); gerr != nil {
		return res, gerr
	}
	return res, nil
}

// TestTheEpochIsRecheckedBeforeTruncationIsAuthorised: the epoch object can move while
// a publication is in flight. The object itself cannot be unwritten — it is create-only
// — but the step that follows it can still be refused, and that step is the one that
// loses data: advancing published is what lets local WAL be discarded (INV-13).
func TestTheEpochIsRecheckedBeforeTruncationIsAuthorised(t *testing.T) {
	ctx := context.Background()
	vid := format.UUIDString(v7Vol())

	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := v7Vol()
	fenced := &fenceOnPublish{Store: store, volumeID: vid}
	l := remoteLog(t, store, clk, vol)
	for i := range 2 {
		if _, err := l.Write(uint64(i)*4096, []byte("payload"), 0); err != nil {
			t.Fatal(err)
		}
		if err := l.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	grantEpoch(t, store, vid, 1, hostGranted)

	c := heldBy(t, checkpoint.NewCheckpointer(fenced), hostGranted)
	if _, err := c.Create(ctx, l, vol, 1); !errors.Is(err, epoch.ErrEpochChanged) {
		t.Fatalf("Create = %v, want %v: the publisher was fenced while its checkpoint was in flight", err, epoch.ErrEpochChanged)
	}
	if got := l.Watermarks().Published; got != 0 {
		t.Fatalf("published advanced to %d after the epoch moved under the publish", got)
	}
	if err := l.TruncateLocal(2); !errors.Is(err, wal.ErrTruncateAboveDurable) {
		t.Fatalf("truncation must stay refused after a fenced publish, got %v", err)
	}
}
