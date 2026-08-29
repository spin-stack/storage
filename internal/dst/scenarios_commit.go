package dst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// commitScenarios are v6 §9's arms: what the publish protocol must do when the object
// store is not the well-behaved one a unit test injects.
func commitScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "two-hosts-cannot-both-publish", Run: scenarioTwoHostsCannotBothPublish},
	}
}

// scenarioTwoHostsCannotBothPublish: two hosts each believe they own one volume, and the
// compare-and-set on HEAD is the only thing between them.
//
// The race is arranged with a stale read rather than with goroutines, and it is the more
// faithful of the two. What actually happens in a fencing failure is not two writes at
// the same instant — it is a host acting on a picture of HEAD that has stopped being
// true, which is precisely what a replica serving a lagging copy produces. Nothing here
// depends on scheduling, so the same seed gives the same trace (INV-02).
//
// The invariant is not "the second publish fails". It is that the published history stays
// a *chain*: every commit HEAD comes to name has the commit it replaced as its parent.
// A second writer that wins produces two commits with one parent, which is a fork — and a
// fork is two hosts' worth of guest writes, each claiming to be the volume.
func scenarioTwoHostsCannotBothPublish(s *Sim) error {
	ctx := context.Background()
	volumeID := ids.NewAt(1<<40, s.Rand).String()
	enc, err := volumeKey(s, volumeID)
	if err != nil {
		return err
	}
	store := &recordingStore{Store: s.Store, sim: s}

	// Two commits by the host that really owns the volume, so the race below is run
	// against a volume with a history rather than an empty prefix.
	first, err := publishOne(ctx, s, store, enc, volumeID, "host-a", 1)
	if err != nil {
		return fmt.Errorf("host-a's first commit: %w", err)
	}
	second, err := publishOne(ctx, s, store, enc, volumeID, "host-a", 2)
	if err != nil {
		return fmt.Errorf("host-a's second commit: %w", err)
	}

	// host-b now reads HEAD from a replica that is one write behind: to it, the volume's
	// current commit is still host-a's first, and the ETag it will compare against is
	// that one's.
	s.Store.InjectStaleRead(commit.HeadKey(volumeID))
	s.Emit(Event{Kind: EventFault, Msg: "HEAD reads one version behind for host-b"})

	_, err = publishOne(ctx, s, store, enc, volumeID, "host-b", 3)
	if err == nil {
		return errors.New("host-b published against a HEAD that had already moved")
	}
	if !errors.Is(err, commit.ErrHeadMoved) {
		return fmt.Errorf("host-b's publish failed for the wrong reason: %w", err)
	}
	s.Store.ClearStaleRead(commit.HeadKey(volumeID))

	// And what is in the bucket is host-a's chain, unchanged and readable end to end.
	head, _, err := commit.ReadHead(ctx, s.Store, volumeID)
	if err != nil {
		return fmt.Errorf("reading HEAD after the race: %w", err)
	}
	if head.CommitID != second {
		return fmt.Errorf("HEAD names %s, want host-a's second commit %s", head.CommitID, second)
	}
	for id, want := second, first; id != ""; {
		m, err := commit.ReadManifest(ctx, s.Store, volumeID, id)
		if err != nil {
			return fmt.Errorf("the chain from HEAD reaches %s, which is not readable: %w", id, err)
		}
		var out bytes.Buffer
		if err := commit.Fetch(ctx, s.Store, enc, m, &out); err != nil {
			return fmt.Errorf("commit %s cannot be fetched: %w", id, err)
		}
		if id == second && m.ParentCommitID != want {
			return fmt.Errorf("commit %s has parent %s, want %s", id, m.ParentCommitID, want)
		}
		id = m.ParentCommitID
	}
	return nil
}

// publishOne publishes one layer and records what happened, whichever way it went.
//
// The event carries the manifest's own parent and the error, not a judgement made here:
// what the checker reads is what the protocol produced.
func publishOne(ctx context.Context, s *Sim, store objectstore.Store, enc *crypto.Encryption, volumeID, host string, n int) (string, error) {
	plain := bytes.Repeat([]byte(fmt.Sprintf("layer-%d-of-%s ", n, host)), 512)
	commitID := ids.NewAt(int64(1<<40+n), s.Rand).String()
	m, err := commit.Publish(ctx, store, enc, bytes.NewReader(plain), commit.Request{
		VolumeID: volumeID, CommitID: commitID, LayerID: ids.NewAt(int64(1<<40+100+n), s.Rand).String(),
		Epoch: int64(n), VirtualSize: 1 << 30,
	})
	s.Emit(Event{
		Kind: EventPublish, Host: host, CommitID: commitID,
		ParentCommitID: m.ParentCommitID, OK: err == nil,
		Msg: fmt.Sprintf("host=%s commit=%s parent=%q ok=%t", host, commitID, m.ParentCommitID, err == nil),
	})
	if err != nil {
		return "", err
	}
	return commitID, nil
}

// volumeKey builds a deterministic DEK bound to the volume. s.Rand is the only source of
// randomness in a run (INV-02), so the same seed produces the same key and the same
// ciphertext — which is what makes a trace comparable across runs at all.
func volumeKey(s *Sim, volumeID string) (*crypto.Encryption, error) {
	dek, err := crypto.GenerateDEK(s.Rand, 1)
	if err != nil {
		return nil, err
	}
	id, err := uuid.Parse(volumeID)
	if err != nil {
		return nil, err
	}
	return crypto.NewEncryption(dek, id)
}

// recordingStore emits an event for every object that reaches the store. It exists so a
// checker reads the bytes that were really written rather than a claim about them.
type recordingStore struct {
	objectstore.Store
	sim *Sim
}

func (r *recordingStore) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	res, err := r.Store.Put(ctx, key, data, opts)
	r.emit(key, data, err)
	return res, err
}

// PutStream is recorded too, and it is the one a layer takes. Without this the object a
// checker most wants to read — the sealed layer — reaches the store through a method the
// recorder does not see, and every assertion about it passes vacuously.
//
// The body is read whole here, which is exactly what the streaming PUT exists to avoid;
// that is affordable only because a scenario's layers are kilobytes and the recording
// already keeps every object in memory.
func (r *recordingStore) PutStream(ctx context.Context, key string, body io.Reader, size int64, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	data, err := io.ReadAll(io.LimitReader(body, size))
	if err != nil {
		return objectstore.PutResult{}, err
	}
	res, perr := r.Store.PutStream(ctx, key, bytes.NewReader(data), size, opts)
	r.emit(key, data, perr)
	return res, perr
}

func (r *recordingStore) emit(key string, data []byte, err error) {
	r.sim.Emit(Event{
		Kind: EventPut, Key: key, Body: data, OK: err == nil,
		Msg: fmt.Sprintf("key=%s bytes=%d ok=%t", key, len(data), err == nil),
	})
}
