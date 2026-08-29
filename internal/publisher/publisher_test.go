package publisher_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/publisher"
	"github.com/spin-stack/storage/internal/qcow"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// fakeKeys is the Control Plane's half: it hands over a volume's wrapped DEK, or refuses
// — which is what being fenced looks like from here.
type fakeKeys struct {
	keys map[string]agent.VolumeKeys
	err  error
}

func (k *fakeKeys) VolumeKeys(_ context.Context, volumeID string) (agent.VolumeKeys, error) {
	if k.err != nil {
		return agent.VolumeKeys{}, k.err
	}
	got, ok := k.keys[volumeID]
	if !ok {
		return agent.VolumeKeys{}, errors.New("no such volume")
	}
	return got, nil
}

// fakeFiles is the sealed layer on disk.
type fakeFiles struct {
	files map[string][]byte
	err   error
}

func (f *fakeFiles) Open(path string) (io.ReadSeekCloser, error) {
	if f.err != nil {
		return nil, f.err
	}
	body, ok := f.files[path]
	if !ok {
		return nil, errors.New("no such file: " + path)
	}
	return nopSeekCloser{bytes.NewReader(body)}, nil
}

// nopSeekCloser is a layer file: readable, rewindable, and closing it costs nothing. The
// publish protocol reads a layer twice, so io.NopCloser is not enough here.
type nopSeekCloser struct{ *bytes.Reader }

func (nopSeekCloser) Close() error { return nil }

type world struct {
	pub   *publisher.Publisher
	store *sim.ObjectStore
	keys  *fakeKeys
	files *fakeFiles
	kms   *crypto.DevKMS
	vol   string
	layer qcow.SealedLayer
	plain []byte
}

func newWorld(t *testing.T) *world {
	t.Helper()
	var kek [crypto.DEKSize]byte
	if _, err := rand.Read(kek[:]); err != nil {
		t.Fatalf("drawing a KEK: %v", err)
	}
	kms := crypto.NewDevKMS(kek, crypto.KEKID(kek))
	dek, err := crypto.GenerateDEK(rand.Reader, 3)
	if err != nil {
		t.Fatalf("generating a DEK: %v", err)
	}
	volumeID := ids.New().String()
	// Wrapped under the volume that carries it: a DEK is custody-bound now, so a wrap
	// minted for one volume does not unwrap for another.
	wrapped, err := kms.WrapDEK(rand.Reader, dek, uuid.MustParse(volumeID))
	if err != nil {
		t.Fatalf("wrapping it: %v", err)
	}
	plain := make([]byte, 200_000)
	if _, err := rand.Read(plain); err != nil {
		t.Fatalf("drawing a layer: %v", err)
	}
	layer := qcow.SealedLayer{
		VolumeID: volumeID, LayerID: ids.New().String(), CommitID: ids.New().String(),
		Path:  "/data/volumes/" + volumeID + "/layers/x.qcow2",
		Epoch: 4, PlainBytes: int64(len(plain)), VirtualSize: 1 << 30,
	}
	w := &world{
		store: sim.NewObjectStore(),
		keys: &fakeKeys{keys: map[string]agent.VolumeKeys{volumeID: {
			VolumeID: volumeID, DEKWrapped: wrapped, KEKID: kms.KEKID(), DEKKeyID: dek.KeyID,
		}}},
		files: &fakeFiles{files: map[string][]byte{layer.Path: plain}},
		kms:   kms, vol: volumeID, layer: layer, plain: plain,
	}
	w.pub = publisher.New(w.store, kms, w.keys, w.files)
	return w
}

// TestPublishPutsTheLayerWhereARecoveryWillLookForIt is the seam this component exists
// for, end to end with nothing faked but the Control Plane and the disk: a sealed file
// becomes a commit that HEAD names and that reads back as the bytes that went in.
func TestPublishPutsTheLayerWhereARecoveryWillLookForIt(t *testing.T) {
	t.Parallel()
	w := newWorld(t)

	if err := w.pub.Publish(t.Context(), w.layer); err != nil {
		t.Fatalf("publishing: %v", err)
	}
	head, _, err := commit.ReadHead(t.Context(), w.store, w.vol)
	if err != nil {
		t.Fatalf("reading HEAD: %v", err)
	}
	if head.CommitID != w.layer.CommitID {
		t.Errorf("HEAD names %q, want the commit that was published, %q", head.CommitID, w.layer.CommitID)
	}
	m, err := commit.ReadManifest(t.Context(), w.store, w.vol, head.CommitID)
	if err != nil {
		t.Fatalf("reading the manifest: %v", err)
	}
	switch {
	case m.Epoch != w.layer.Epoch:
		t.Errorf("the commit records epoch %d, want the fencing token this host held, %d", m.Epoch, w.layer.Epoch)
	case m.VirtualSize != w.layer.VirtualSize:
		t.Errorf("the commit reconstructs a volume of %d bytes, want %d", m.VirtualSize, w.layer.VirtualSize)
	case m.Layer.LayerID != w.layer.LayerID:
		t.Errorf("the commit names layer %q, want %q", m.Layer.LayerID, w.layer.LayerID)
	}

	// And the bytes. Fetched with a key built the same way a recovery would build one,
	// out of the wrapped material and nothing this test kept around.
	keys, _ := w.keys.VolumeKeys(t.Context(), w.vol)
	dek, err := w.kms.UnwrapDEK(keys.DEKWrapped, keys.DEKKeyID, uuid.MustParse(w.vol))
	if err != nil {
		t.Fatalf("unwrapping: %v", err)
	}
	enc, err := crypto.NewEncryption(dek, uuid.MustParse(w.vol))
	if err != nil {
		t.Fatalf("binding: %v", err)
	}
	var got bytes.Buffer
	if err := commit.Fetch(t.Context(), w.store, enc, m, &got); err != nil {
		t.Fatalf("fetching: %v", err)
	}
	if !bytes.Equal(got.Bytes(), w.plain) {
		t.Fatal("the layer did not survive the round trip")
	}
}

// TestPublishStopsWhenTheKeyCannotBeHad. A Control Plane that will not hand over a
// volume's key is this host having been fenced, arriving through a different door — and
// the one thing that must not happen is the layer going up unsealed instead.
func TestPublishStopsWhenTheKeyCannotBeHad(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		mut  func(*world)
		want string
	}{
		{
			name: "the control plane refuses",
			mut:  func(w *world) { w.keys.err = errors.New("permission_denied: not this volume's host") },
			want: "not this volume's host",
		},
		{
			name: "the wrapped key does not unwrap under this host's KEK",
			mut: func(w *world) {
				k := w.keys.keys[w.vol]
				k.DEKWrapped = bytes.Repeat([]byte{7}, len(k.DEKWrapped))
				w.keys.keys[w.vol] = k
			},
			want: "unwrapping",
		},
		{
			name: "the sealed layer is not on disk",
			mut:  func(w *world) { w.files.err = errors.New("no such file or directory") },
			want: "opening the sealed layer",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			tt.mut(w)

			err := w.pub.Publish(t.Context(), w.layer)
			if err == nil {
				t.Fatal("it published")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("the error does not say what went wrong: %v", err)
			}
			// Nothing reached the bucket. A commit that is half-made is garbage a sweep
			// collects; a HEAD that names one is a promise that cannot be kept.
			objs, lerr := w.store.List(t.Context(), "")
			if lerr != nil {
				t.Fatalf("listing: %v", lerr)
			}
			if len(objs) != 0 {
				t.Errorf("a failed publish left %d objects in the bucket", len(objs))
			}
		})
	}
}

// TestPublishIsIdempotentThroughThisLayerToo: the Manager retries the same SealedLayer
// every cycle until it lands, so "the same layer again" is the ordinary case and not an
// edge one.
func TestPublishIsIdempotentThroughThisLayerToo(t *testing.T) {
	t.Parallel()
	w := newWorld(t)
	for i := range 3 {
		if err := w.pub.Publish(t.Context(), w.layer); err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	objs, err := w.store.List(t.Context(), "volumes/"+w.vol+"/commits/")
	if err != nil {
		t.Fatalf("listing: %v", err)
	}
	if len(objs) != 1 {
		t.Errorf("three attempts at one commit left %d manifests", len(objs))
	}
}
