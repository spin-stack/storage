package qcow_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/qcow"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// A chain of four: three published layers and the tip the guest writes to. The sizes are
// distinct so that a plan which collapsed the wrong set could not add up to the same
// number by accident.
const (
	oldestLayer = "0198c0de-0000-7000-8000-000000000a11"
	middleLayer = "0198c0de-0000-7000-8000-000000000a22"
	newestLayer = "0198c0de-0000-7000-8000-000000000a33"
	oldestBytes = 100 << 20
	middleBytes = 200 << 20
	newestBytes = 300 << 20
	tipBytes    = 50 << 20
	// readBytes is what a convert reads: the three published layers, and not the tip.
	readBytes = oldestBytes + middleBytes + newestBytes
	// diskBytes is every layer file of this volume, tip included — §21's
	// local_disk_bytes, which is about the disk and not about the chain.
	diskBytes = readBytes + tipBytes

	collapseOldest = "0198c0de-0000-7000-8000-0000000c0011"
	collapseMiddle = "0198c0de-0000-7000-8000-0000000c0022"
	collapseNewest = "0198c0de-0000-7000-8000-0000000c0033"
)

// What each layer of the fixture puts on the guest's disk. Every value is distinct, so the
// composition of a chain names the layers it was made of: a collapse that took the wrong
// prefix, or took it in the wrong order, produces bytes that belong to a different set of
// files.
//
// The tip's byte is the one that must never appear in a compacted root. It is the guest's
// unpublished writes: a commit that carried them would be claiming a state no commit
// promised, and the value 0xD3 at offset 3 is what saying so looks like from outside.
var layerBytesAt = map[string]map[int]byte{
	oldestLayer: {0: 0xA0, 1: 0xA1, 2: 0xA2, 3: 0xA3},
	middleLayer: {1: 0xB1, 2: 0xB2},
	newestLayer: {2: 0xC2},
	layerID:     {3: 0xD3},
}

// image is what the qemu-img model writes into a layer file: the bytes that layer itself
// defines, and the file it is stacked on. It is JSON so that a test can read a layer back
// through qcow.Paths — the same bytes the publisher would upload — rather than through a
// field of the fake.
type image struct {
	Backing string         `json:"backing,omitempty"`
	Size    int64          `json:"size"`
	Data    map[string]int `json:"data"`
	Corrupt bool           `json:"corrupt,omitempty"`
}

// qemuImg models the pinned qemu-img over that format: `info`, `info --backing-chain`,
// `convert` and `rebase -u`.
//
// A model and not a recorder, because the property this feature has to be held to is about
// bytes: "the new root reconstructs what the prefix reconstructed" cannot be checked
// against a fake that answers a canned string. It records every argv as well — what the
// process was asked to do is the other half, and the half a plan that quietly ran a convert
// would satisfy on its own.
type qemuImg struct {
	mu    sync.Mutex
	paths *fakePaths
	runs  [][]string
	// locked is the tip a guest holds. Reading it is refused, and *writing* any file of
	// the chain under it is refused too: QEMU opens the whole backing chain, so a rebase
	// wants a write lock the guest is holding — measured against the pinned 11.1.1, which
	// reads a sealed backing layer under a running guest and refuses the live tip. That is
	// what makes "nothing in a live chain is repointed" an assertion about the tool and
	// not about our own bookkeeping.
	locked string
	// convertErr, when set, fails every convert after the temporary file is written: a
	// process killed with its output half there.
	convertErr error
	// backRoot, when set, is a backing file the convert leaves in the image it produces,
	// which is the one result that must never be published.
	backRoot string
}

func (q *qemuImg) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.runs = append(q.runs, append([]string{name}, args...))
	switch {
	case len(args) > 0 && args[0] == "--version":
		return []byte("qemu-img version 11.1.1"), nil
	case len(args) > 0 && args[0] == "info":
		return q.info(args[len(args)-1], slices.Contains(args, "--backing-chain"))
	case len(args) > 0 && args[0] == "convert":
		return nil, q.convert(args[len(args)-2], args[len(args)-1])
	case len(args) > 0 && args[0] == "rebase":
		return nil, q.rebase(args[len(args)-1], args[slices.Index(args, "-b")+1])
	}
	return nil, fmt.Errorf("fake qemu-img: %s", strings.Join(args, " "))
}

func (q *qemuImg) info(path string, walk bool) ([]byte, error) {
	var out []map[string]any
	for path != "" {
		img, err := q.read(path)
		if err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"filename": path, "format": "qcow2", "virtual-size": img.Size,
			"full-backing-filename": img.Backing,
			"format-specific":       map[string]any{"type": "qcow2", "data": map[string]any{"corrupt": img.Corrupt}},
		})
		if !walk {
			break
		}
		path = img.Backing
	}
	if walk {
		return json.Marshal(out)
	}
	return json.Marshal(out[0])
}

// convert flattens src's whole backing chain into dst, which is what `qemu-img convert`
// does and the whole reason a compaction is one command: the result stands on nothing.
func (q *qemuImg) convert(src, dst string) error {
	flat := image{Data: map[string]int{}, Backing: q.backRoot}
	var chain []image
	for path := src; path != ""; {
		img, err := q.read(path)
		if err != nil {
			return err
		}
		chain = append(chain, img)
		path = img.Backing
	}
	// Oldest first, so the layer nearest the guest wins.
	for i := len(chain) - 1; i >= 0; i-- {
		maps.Copy(flat.Data, chain[i].Data)
		flat.Size = chain[i].Size
	}
	q.write(dst, flat)
	return q.convertErr
}

// rebase -u rewrites one header and opens nothing else: the backing file is not read, so a
// path that names nothing is written just as happily as one that does.
func (q *qemuImg) rebase(path, onto string) error {
	if q.held(path) {
		return fmt.Errorf(`qemu-img: Failed to get shared "write" lock on %s`, path)
	}
	img, err := q.raw(path)
	if err != nil {
		return err
	}
	img.Backing = onto
	q.write(path, img)
	return nil
}

func (q *qemuImg) read(path string) (image, error) {
	if path == q.locked {
		return image{}, fmt.Errorf(`qemu-img: Failed to get shared "write" lock on %s`, path)
	}
	return q.raw(path)
}

// held is a file a running QEMU has open — the tip it writes to and every layer under it
// — and so a file no other process may take a write lock on.
func (q *qemuImg) held(path string) bool {
	for at := q.locked; at != ""; {
		if at == path {
			return true
		}
		img, err := q.raw(at)
		if err != nil {
			return false
		}
		at = img.Backing
	}
	return false
}

func (q *qemuImg) raw(path string) (image, error) {
	body, err := q.paths.ReadFile(path)
	if err != nil {
		return image{}, fmt.Errorf("qemu-img: Could not open %q: %w", path, err)
	}
	var img image
	if err := json.Unmarshal(body, &img); err != nil {
		return image{}, err
	}
	return img, nil
}

func (q *qemuImg) write(path string, img image) {
	body, err := json.Marshal(img)
	if err != nil {
		panic(err)
	}
	q.paths.put(path, body, int64(len(body)))
}

func (q *qemuImg) commands() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]string, 0, len(q.runs))
	for _, r := range q.runs {
		out = append(out, strings.Join(r, " "))
	}
	return out
}

// converts is every convert this qemu-img was asked to run, as `<source> -> <target>`.
func (q *qemuImg) converts() []string {
	var out []string
	for _, cmd := range q.commands() {
		if fields := strings.Fields(cmd); len(fields) > 3 && fields[1] == "convert" {
			out = append(out, fields[len(fields)-2]+" -> "+fields[len(fields)-1])
		}
	}
	return out
}

// rebases is every rebase, as `<image> -> <new backing>`.
func (q *qemuImg) rebases() []string {
	var out []string
	for _, cmd := range q.commands() {
		fields := strings.Fields(cmd)
		if len(fields) > 3 && fields[1] == "rebase" {
			out = append(out, fields[len(fields)-1]+" -> "+fields[slices.Index(fields, "-b")+1])
		}
	}
	return out
}

// put is a file appearing on the disk with a length, which is what the qemu-img model does
// and what a rotation's `create` does. fakePaths tracks presence and size separately,
// because in every other test they come from different places.
func (p *fakePaths) put(path string, body []byte, size int64) {
	if err := p.WriteAtomic(path, body); err != nil {
		panic(err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sizes[path] = size
}

// publishedLayer is one commit that reached the object store, with the bytes that went with
// it — read out of the layer file at publish time, the way the real publisher opens it.
type publishedLayer struct {
	layer qcow.SealedLayer
	body  []byte
}

type fakePublisher struct {
	// paths is whatever filesystem the layer is on: the fake one in this file, or the real
	// one under the lane that drives the pinned qemu-img.
	paths     interface{ ReadFile(string) ([]byte, error) }
	published []publishedLayer
	// err, when set, is what every publish fails with.
	err error
}

func (f *fakePublisher) Publish(_ context.Context, l qcow.SealedLayer) error {
	if f.err != nil {
		return f.err
	}
	body, err := f.paths.ReadFile(l.Path)
	if err != nil {
		return fmt.Errorf("publishing %s: %w", l.Path, err)
	}
	f.published = append(f.published, publishedLayer{layer: l, body: body})
	return nil
}

// roots is the commits that were published as compacted roots.
func (f *fakePublisher) roots() []qcow.SealedLayer {
	var out []qcow.SealedLayer
	for _, p := range f.published {
		if p.layer.ReplacesCommitID != "" {
			out = append(out, p.layer)
		}
	}
	return out
}

// host is a Manager over the fake disk, with everything a test needs to see what it did.
type host struct {
	m      *qcow.Manager
	paths  *fakePaths
	q      *qemuImg
	pub    *fakePublisher
	dialer *fakeDialer
	rec    *fakeRecovery
	log    *bytes.Buffer
}

// deepChain is a host serving one volume with a four-layer chain: three published layers
// and a tip. With a guest attached the whole chain is locked, so anything that reached for
// one of those files would be refused by qemu-img the way a real one is.
func deepChain(t *testing.T, policy qcow.CompactionPolicy, attached bool) *host {
	t.Helper()

	tip := qcow.LayerImage(root, vol, layerID)
	p := newPathsAt(tip)
	q := &qemuImg{paths: p}
	backing := ""
	// Written in chain order, oldest first, because each layer records the one under it.
	for _, l := range []struct {
		id    string
		bytes int64
	}{{oldestLayer, oldestBytes}, {middleLayer, middleBytes}, {newestLayer, newestBytes}, {layerID, tipBytes}} {
		img := image{Backing: backing, Size: size, Data: map[string]int{}}
		for off, b := range layerBytesAt[l.id] {
			img.Data[fmt.Sprint(off)] = int(b)
		}
		path := qcow.LayerImage(root, vol, l.id)
		body, err := json.Marshal(img)
		if err != nil {
			t.Fatal(err)
		}
		p.put(path, body, l.bytes)
		backing = path
	}
	if err := qcow.WriteState(p, root, vol, qcow.State{
		Commits: []qcow.CommitLayer{
			{CommitID: collapseOldest, LayerID: oldestLayer},
			{CommitID: collapseMiddle, LayerID: middleLayer},
			{CommitID: collapseNewest, LayerID: newestLayer},
		},
		Layers: []string{layerID, newestLayer},
	}); err != nil {
		t.Fatalf("writing the state of a volume with three published commits: %v", err)
	}
	scripts := map[string][]string{}
	if attached {
		q.locked = tip
		scripts[qcow.QMPSocket(root, vol)] = attachedTo(tip)
	}
	return newHost(t, policy, p, q, scripts)
}

// newHost builds the Manager over a disk a test has already laid out, which is what lets a
// second one come back over the same files after a "restart".
func newHost(t *testing.T, policy qcow.CompactionPolicy, p *fakePaths, q *qemuImg, scripts map[string][]string) *host {
	t.Helper()
	var out bytes.Buffer
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(restore) })

	pub := &fakePublisher{paths: p}
	d := &fakeDialer{scripts: scripts}
	rec := bornEmpty()
	m, err := qcow.New(t.Context(), qcow.Config{
		Root: root, QemuImg: "/qemu-img", ProbeTimeout: time.Second, Compaction: policy,
	}, qcow.Deps{
		Clock:     sim.NewClock(time.Unix(1_700_000_000, 0)),
		Disk:      sim.NewDisk(),
		Runner:    q,
		Paths:     p,
		Dialer:    d,
		Recovery:  rec,
		Publisher: pub,
	})
	if err != nil {
		t.Fatalf("building a manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return &host{m: m, paths: p, q: q, pub: pub, dialer: d, rec: rec, log: &out}
}

// cycle is one turn of the reconciliation loop.
func (h *host) cycle(t *testing.T) error {
	t.Helper()
	return h.m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)})
}

func (h *host) state(t *testing.T) qcow.State {
	t.Helper()
	st, err := qcow.ReadState(h.paths, root, vol)
	if err != nil {
		t.Fatalf("reading the record: %v", err)
	}
	return st
}

// chainUnder is the files a guest reading `image` reads through, top first, as qemu-img
// walks them off the disk.
func (h *host) chainUnder(t *testing.T, image string) []string {
	t.Helper()
	var out []string
	for image != "" {
		img, err := h.q.raw(image)
		if err != nil {
			t.Fatalf("walking the chain: %v", err)
		}
		out = append(out, image)
		image = img.Backing
	}
	return out
}

// readBack is what a guest reads through a chain, offset by offset: the bytes, not the
// metadata. It walks the file the same way qemu-img would, out of the fake filesystem, so
// what it reports is what is on the disk and not what the Manager believes it wrote.
func readBack(t *testing.T, p *fakePaths, path string) map[int]byte {
	t.Helper()
	out := map[int]byte{}
	var stack []image
	for path != "" {
		body, err := p.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s back: %v", path, err)
		}
		var img image
		if err := json.Unmarshal(body, &img); err != nil {
			t.Fatalf("decoding %s: %v", path, err)
		}
		stack = append(stack, img)
		path = img.Backing
	}
	for i := len(stack) - 1; i >= 0; i-- {
		for off, b := range stack[i].Data {
			var n int
			if _, err := fmt.Sscan(off, &n); err != nil {
				t.Fatal(err)
			}
			out[n] = byte(b)
		}
	}
	return out
}

// bytesOf decodes the layer a publish carried, as the object store would receive it.
func bytesOf(t *testing.T, body []byte) map[int]byte {
	t.Helper()
	var img image
	if err := json.Unmarshal(body, &img); err != nil {
		t.Fatalf("decoding the published layer: %v", err)
	}
	if img.Backing != "" {
		t.Fatalf("the published root is backed by %q, so it reconstructs nothing on its own", img.Backing)
	}
	out := map[int]byte{}
	for off, b := range img.Data {
		var n int
		if _, err := fmt.Sscan(off, &n); err != nil {
			t.Fatal(err)
		}
		out[n] = byte(b)
	}
	return out
}

// TestACompactedRootReconstructsWhatTheCollapsedPrefixDid is the sentence the whole act
// rests on: a commit that returned SUCCESS is never lost, so the root that replaces the
// prefix must reconstruct, byte for byte, what the prefix reconstructed.
//
// Asserted on the bytes that reached the object store, and not on the plan, the log or the
// commit ids: a compaction that collapsed two layers instead of three, or that stacked them
// the wrong way round, produces a manifest that looks exactly as right.
func TestACompactedRootReconstructsWhatTheCollapsedPrefixDid(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 4}, true)

	if err := h.cycle(t); err != nil {
		t.Fatalf("applying: %v", err)
	}

	if len(h.pub.published) != 1 {
		t.Fatalf("a four-layer chain past its threshold published %d commits, want one root:\n%s", len(h.pub.published), h.log)
	}
	published := h.pub.published[0]
	if published.layer.ReplacesCommitID != collapseNewest {
		t.Errorf("the compacted commit says it replaces %q; the prefix it flattens reconstructs %s",
			published.layer.ReplacesCommitID, collapseNewest)
	}
	// What the prefix reconstructed, read out of the files the guest is still sitting on.
	want := readBack(t, h.paths, qcow.LayerImage(root, vol, newestLayer))
	if got := bytesOf(t, published.body); !maps.Equal(got, want) {
		t.Errorf("the published root reconstructs %v and the prefix it replaces reconstructs %v", got, want)
	}
	// And the guest's unpublished writes are not in it. The tip is not a commit; a root
	// carrying it would be a commit claiming bytes nothing ever promised.
	for off, b := range bytesOf(t, published.body) {
		if b == layerBytesAt[layerID][off] {
			t.Errorf("offset %d of the published root is 0x%X, which is what the tip a guest is writing to holds", off, b)
		}
	}
	// No file of the live chain was handed to qemu-img: the model refuses every one of
	// them on the write lock, the way the pinned 11.1.1 refuses a running guest's images,
	// so a convert or a rebase that named one would have failed outright.
	for _, cmd := range h.q.commands() {
		if strings.Contains(cmd, layerID) {
			t.Errorf("qemu-img was asked about the tip a guest has open: %s", cmd)
		}
	}
	if got := h.q.converts(); len(got) != 1 {
		t.Errorf("qemu-img ran %d converts, want exactly one: %v", len(got), got)
	}
	if !strings.Contains(h.q.converts()[0], newestLayer) {
		t.Errorf("the convert reads %v, and the top of the published prefix is layer %s", h.q.converts(), newestLayer)
	}
}

// TestTheRootsManifestClaimsTheGuestsDiskAndNotTheFlattenedFile. The virtual size is what a
// restore recreates the tip at, and it comes from the manifest — so a root that claimed the
// length of its own qcow2 would be a commit that returned SUCCESS and that no host could
// ever rebuild.
func TestTheRootsManifestClaimsTheGuestsDiskAndNotTheFlattenedFile(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 4}, true)

	if err := h.cycle(t); err != nil {
		t.Fatalf("applying: %v", err)
	}
	if len(h.pub.published) != 1 {
		t.Fatalf("want one published root, got %d:\n%s", len(h.pub.published), h.log)
	}
	layer := h.pub.published[0].layer
	if layer.VirtualSize != size {
		t.Errorf("the root claims a %d-byte disk and the guest's is %d", layer.VirtualSize, size)
	}
	onDisk, err := h.paths.Size(layer.Path)
	if err != nil {
		t.Fatal(err)
	}
	if layer.VirtualSize == onDisk {
		t.Errorf("the root claims the length of the flattened file (%d) as the guest's disk", onDisk)
	}
	if layer.PlainBytes != onDisk {
		t.Errorf("the root reports %d bytes on this host and the file is %d", layer.PlainBytes, onDisk)
	}
}

// TestACollapseDoesNotMoveTheRPOAnchor. The age this host reports is the age of durable
// *guest* writes, and a compaction publishes nothing the guest wrote. Moving the anchor
// makes a volume that has not committed for an hour look fresh, which postpones the commit
// that would have made it true.
func TestACollapseDoesNotMoveTheRPOAnchor(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 4}, true)
	const behindBy = 90 * time.Second
	st := h.state(t)
	st.LastCommitAt = time.Unix(1_700_000_000, 0).Add(-behindBy).UnixMilli()
	if err := qcow.WriteState(h.paths, root, vol, st); err != nil {
		t.Fatal(err)
	}

	if err := h.cycle(t); err != nil {
		t.Fatalf("applying: %v", err)
	}
	if len(h.pub.roots()) != 1 {
		t.Fatalf("the chain was not collapsed:\n%s", h.log)
	}
	got, err := h.m.Volumes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].LastCommitAge != behindBy {
		t.Fatalf("this host reports an RPO of %v after a compaction; the guest's newest durable write is %v old",
			got[0].LastCommitAge, behindBy)
	}
}

// TestNothingInALiveChainIsRepointed. The last step of a collapse is a `qemu-img rebase -u`
// on the layer above the prefix, and a running QEMU holds every file of the chain it has
// open. v6 §5 forbids reaching past that, so the collapse publishes its root and waits.
func TestNothingInALiveChainIsRepointed(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 4}, true)

	for range 3 {
		if err := h.cycle(t); err != nil {
			t.Fatalf("applying: %v", err)
		}
	}
	if got := h.q.rebases(); len(got) != 0 {
		t.Errorf("a layer of a chain a guest has open was repointed: %v", got)
	}
	// The chain the guest reads is the one it had: every layer of the prefix, in order.
	want := []string{layerID, newestLayer, middleLayer, oldestLayer}
	var got []string
	for _, path := range h.chainUnder(t, qcow.LayerImage(root, vol, layerID)) {
		got = append(got, qcow.LayerIDOfImage(path))
	}
	if !slices.Equal(got, want) {
		t.Errorf("the guest's chain is %v, want %v", got, want)
	}
	// And the collapse is still on the books, because it is not finished.
	st := h.state(t)
	if st.Compacting == nil || !st.Compacting.Published {
		t.Fatalf("the record of the published root is %+v, and the chain has not been repointed at it", st.Compacting)
	}
	if st.Compacting.RebaseLayerID != layerID {
		t.Errorf("the collapse owes a rebase of layer %s; the layer above the prefix is %s", st.Compacting.RebaseLayerID, layerID)
	}
}

// TestTheChainIsRepointedAtTheRootOnceTheGuestLetsGo is the other half: what is owed is
// paid, without the volume having to be granted, restored or restarted.
func TestTheChainIsRepointedAtTheRootOnceTheGuestLetsGo(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 4}, true)
	if err := h.cycle(t); err != nil {
		t.Fatalf("the cycle that collapses: %v", err)
	}
	root0 := h.state(t).Compacting
	if root0 == nil {
		t.Fatalf("no collapse happened:\n%s", h.log)
	}

	// The guest stops: the QMP socket answers nothing and no file is locked.
	h.q.locked = ""
	h.dialer.scripts = map[string][]string{}
	if err := h.cycle(t); err != nil {
		t.Fatalf("the cycle after the guest stopped: %v", err)
	}

	if got := h.chainUnder(t, qcow.LayerImage(root, vol, layerID)); len(got) != 2 || got[1] != qcow.LayerImage(root, vol, root0.LayerID) {
		t.Errorf("the guest's chain is %v; it should be the tip over the compacted root alone", got)
	}
	st := h.state(t)
	if st.Compacting != nil {
		t.Errorf("the collapse is still on the books after the chain was repointed: %+v", st.Compacting)
	}
	want := []qcow.CommitLayer{{CommitID: root0.CommitID, LayerID: root0.LayerID}}
	if !slices.Equal(st.Commits, want) {
		t.Errorf("this host records commits %v; the prefix has been replaced by %v", st.Commits, want)
	}
	// The layers the root replaced are still there. A guest that was reading through them a
	// moment ago is not something a transformation gets to delete.
	for _, layer := range []string{oldestLayer, middleLayer, newestLayer} {
		if there, err := h.paths.Exists(qcow.LayerImage(root, vol, layer)); err != nil || !there {
			t.Errorf("layer %s was deleted by a compaction (%v)", layer, err)
		}
	}
	// And nothing under the tip reads as a layer this host owes the object store: a prefix
	// layer left in the record after leaving Commits would be published all over again.
	if got := st.SealedBelow(layerID); len(got) != 0 {
		t.Errorf("this host now believes it owes %v, and every one of those is a published layer", got)
	}
}

// TestACollapseKeepsTheCommitsThatLandedWhileItWaited. A guest goes on writing while the
// rebase is owed, and what it seals goes on being published: those commits sit above the
// root and they are the history. A record that dropped them would make this host offer to
// publish a published layer all over again, and would tell §21 a chain is shorter than the
// one the guest is reading.
func TestACollapseKeepsTheCommitsThatLandedWhileItWaited(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 4}, true)
	if err := h.cycle(t); err != nil {
		t.Fatalf("the cycle that collapses: %v", err)
	}
	root0 := h.state(t).Compacting
	if root0 == nil || !root0.Published {
		t.Fatalf("the root was not published:\n%s", h.log)
	}

	// The guest rotates: the old tip is sealed under a new one, and the next cycle
	// publishes it as an ordinary commit while the collapse is still waiting.
	const nextTip = "0198c0de-0000-7000-8000-0000000f2058"
	h.q.write(qcow.LayerImage(root, vol, nextTip), image{
		Backing: qcow.LayerImage(root, vol, layerID), Size: size, Data: map[string]int{"4": 0xE4},
	})
	if err := qcow.SyncPointer(h.paths, root, vol, qcow.LayerImage(root, vol, nextTip)); err != nil {
		t.Fatal(err)
	}
	st := h.state(t)
	st.Layers = []string{nextTip, layerID}
	if err := qcow.WriteState(h.paths, root, vol, st); err != nil {
		t.Fatal(err)
	}
	h.q.locked = qcow.LayerImage(root, vol, nextTip)
	h.dialer.scripts = map[string][]string{qcow.QMPSocket(root, vol): attachedTo(qcow.LayerImage(root, vol, nextTip))}
	if err := h.cycle(t); err != nil {
		t.Fatalf("the cycle that publishes the rotated layer: %v", err)
	}
	rotated := h.pub.published[len(h.pub.published)-1].layer
	if rotated.LayerID != layerID {
		t.Fatalf("the rotation published layer %s, want the old tip %s", rotated.LayerID, layerID)
	}

	// The guest stops, and the collapse finishes.
	h.q.locked = ""
	h.dialer.scripts = map[string][]string{}
	if err := h.cycle(t); err != nil {
		t.Fatalf("the cycle that repoints the chain: %v", err)
	}
	want := []qcow.CommitLayer{
		{CommitID: root0.CommitID, LayerID: root0.LayerID},
		{CommitID: rotated.CommitID, LayerID: rotated.LayerID},
	}
	if got := h.state(t).Commits; !slices.Equal(got, want) {
		t.Errorf("this host records commits %v, want %v: the commit that landed while the collapse waited is part of the history", got, want)
	}
	if got := h.state(t).SealedBelow(nextTip); len(got) != 0 {
		t.Errorf("this host now believes it owes %v, and every one of those is a published layer", got)
	}
	// The record agreeing is not the claim. A rebase moves one file's backing pointer, and
	// moving the wrong one — the tip, rather than the layer directly above the prefix —
	// leaves a record that reads exactly like this one and a disk with everything in
	// between unhooked. So the chain is walked and the guest's disk read back.
	wantChain := []string{
		qcow.LayerImage(root, vol, nextTip),
		qcow.LayerImage(root, vol, layerID),
		qcow.LayerImage(root, vol, root0.LayerID),
	}
	if got := h.chainUnder(t, qcow.LayerImage(root, vol, nextTip)); !slices.Equal(got, wantChain) {
		t.Errorf("the guest's chain is %v, want %v: the layer published while the collapse waited belongs between the tip and the root", got, wantChain)
	}
	wantBytes := map[int]byte{0: 0xA0, 1: 0xB1, 2: 0xC2, 3: 0xD3, 4: 0xE4}
	if got := readBack(t, h.paths, qcow.LayerImage(root, vol, nextTip)); !maps.Equal(got, wantBytes) {
		t.Errorf("the guest's disk reads back as %v, want %v", got, wantBytes)
	}
}

// TestASecondCollapseRunsOnceTheChainReadsThroughTheFirstRoot is what §21 is for: chains
// cannot grow without limit, and a compaction that can only ever happen once does not bound
// anything. It is also the one this feature failed: with the local chain left on the layers
// the root replaced, every later collapse walks a chain that is not the record's and is
// refused as a fork — once a heartbeat, for the life of the volume.
func TestASecondCollapseRunsOnceTheChainReadsThroughTheFirstRoot(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 4}, true)
	if err := h.cycle(t); err != nil {
		t.Fatalf("the cycle that collapses: %v", err)
	}
	h.q.locked = ""
	h.dialer.scripts = map[string][]string{}
	if err := h.cycle(t); err != nil {
		t.Fatalf("the cycle that repoints the chain: %v", err)
	}
	first := h.state(t).Commits
	if len(first) != 1 {
		t.Fatalf("the first collapse did not finish: %v", first)
	}

	// The volume goes on being served: a rotation seals the old tip under a new one, and
	// the sealed layer becomes an ordinary commit. That is the chain the second collapse
	// has to be able to read.
	const nextTip = "0198c0de-0000-7000-8000-0000000f2058"
	h.q.write(qcow.LayerImage(root, vol, nextTip), image{
		Backing: qcow.LayerImage(root, vol, layerID), Size: size, Data: map[string]int{"4": 0xE4},
	})
	if err := qcow.SyncPointer(h.paths, root, vol, qcow.LayerImage(root, vol, nextTip)); err != nil {
		t.Fatal(err)
	}
	st := h.state(t)
	st.Layers = []string{nextTip, layerID}
	if err := qcow.WriteState(h.paths, root, vol, st); err != nil {
		t.Fatal(err)
	}

	// The guest is back, on the new tip, and the cycle publishes the layer it sealed
	// (v6 §11 keeps one layer in flight, so this is a cycle of its own).
	h2 := newHost(t, qcow.CompactionPolicy{AtLayers: 2}, h.paths, h.q, map[string][]string{
		qcow.QMPSocket(root, vol): attachedTo(qcow.LayerImage(root, vol, nextTip)),
	})
	h2.q.locked = qcow.LayerImage(root, vol, nextTip)
	h2.pub.published = h.pub.published
	if err := h2.cycle(t); err != nil {
		t.Fatalf("the cycle that publishes the rotated layer: %v", err)
	}
	// And stops again, which is when the second collapse can finish.
	h2.q.locked = ""
	h2.dialer.scripts = map[string][]string{}
	if err := h2.cycle(t); err != nil {
		t.Fatalf("the cycle that collapses a second time: %v", err)
	}

	roots := h2.pub.roots()
	if len(roots) != 2 {
		t.Fatalf("the volume was collapsed %d times, want twice:\n%s", len(roots), h2.log)
	}
	second := roots[1]
	if second.ReplacesCommitID == collapseNewest || second.ReplacesCommitID == roots[0].CommitID {
		// The second root must flatten the commit the *rotation* published, not the one
		// the first root already replaced.
		t.Errorf("the second root replaces %s, which is not the newest commit of the history", second.ReplacesCommitID)
	}
	// The guest's chain: the tip over the second root, and nothing else.
	got := h2.chainUnder(t, qcow.LayerImage(root, vol, nextTip))
	if len(got) != 2 || got[1] != qcow.LayerImage(root, vol, second.LayerID) {
		t.Errorf("the chain under the tip is %v; it should be the tip over the second root alone", got)
	}
	// And what it reconstructs is still every write, including the one made after the
	// first collapse.
	want := map[int]byte{0: 0xA0, 1: 0xB1, 2: 0xC2, 3: 0xD3, 4: 0xE4}
	if bytes := readBack(t, h2.paths, qcow.LayerImage(root, vol, nextTip)); !maps.Equal(bytes, want) {
		t.Errorf("the guest's disk reads back as %v, want %v", bytes, want)
	}
}

// TestACollapseIsNotPlannedWhileOneIsUnfinished. Two roots for one prefix is a full convert
// and a full upload per heartbeat, and the second one publishes bytes the first already
// published.
func TestACollapseIsNotPlannedWhileOneIsUnfinished(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 4}, true)

	for range 3 {
		if err := h.cycle(t); err != nil {
			t.Fatalf("applying: %v", err)
		}
	}
	if got := h.q.converts(); len(got) != 1 {
		t.Fatalf("three cycles ran %d converts: %v", len(got), got)
	}
	if len(h.pub.published) != 1 {
		t.Fatalf("three cycles published %d roots, want one:\n%s", len(h.pub.published), h.log)
	}
}

// TestACompactionInterruptedBeforeItsCommitLandsFinishesTheSameOne. A host that died
// between the CAS and its own note would otherwise meet a HEAD it has no memory of, which
// is indistinguishable from a chain another host has moved past — and the volume is refused
// from then on. The record written before the convert is what makes the retry the same
// commit.
func TestACompactionInterruptedBeforeItsCommitLandsFinishesTheSameOne(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 4}, true)
	h.pub.err = fmt.Errorf("the object store did not answer")

	if err := h.cycle(t); err == nil {
		t.Fatal("a publish that failed was reported as a compaction that worked")
	}
	st := h.state(t)
	if st.Compacting == nil {
		t.Fatal("a collapse whose publish failed left no record, so the next cycle starts a second root")
	}
	began := *st.Compacting
	if began.Published {
		t.Fatal("a publish that failed was recorded as one that landed")
	}

	h.pub.err = nil
	if err := h.cycle(t); err != nil {
		t.Fatalf("the retry: %v", err)
	}
	if len(h.pub.published) != 1 {
		t.Fatalf("the retry published %d commits, want one", len(h.pub.published))
	}
	if got := h.pub.published[0].layer; got.CommitID != began.CommitID || got.LayerID != began.LayerID {
		t.Errorf("the retry published commit %s / layer %s, and the collapse began as %s / %s",
			got.CommitID, got.LayerID, began.CommitID, began.LayerID)
	}
	if got := h.q.converts(); len(got) != 1 {
		t.Errorf("the retry converted again over a root that was already on the disk: %v", got)
	}
}

// TestACrashBetweenTheCASAndTheRecordDoesNotRefuseTheVolume. HEAD names a commit this host
// published and has not written down anywhere else, which is exactly the shape of a chain
// another host has moved past. Refusing it stops a guest, permanently and for this host's
// own bookkeeping: nothing ever puts that commit into Commits afterwards.
func TestACrashBetweenTheCASAndTheRecordDoesNotRefuseTheVolume(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 4}, true)
	if err := h.cycle(t); err != nil {
		t.Fatalf("the cycle that collapses: %v", err)
	}
	began := h.state(t).Compacting
	if began == nil {
		t.Fatalf("no collapse happened:\n%s", h.log)
	}

	// The process comes back, and the object store says HEAD is the root it published —
	// which is in no list this host holds, because the record of it never landed.
	h2 := newHost(t, qcow.CompactionPolicy{AtLayers: 4}, h.paths, h.q, map[string][]string{
		qcow.QMPSocket(root, vol): attachedTo(qcow.LayerImage(root, vol, layerID)),
	})
	h2.rec.head = began.CommitID
	st := h.state(t)
	st.Compacting.Published = false
	if err := qcow.WriteState(h.paths, root, vol, st); err != nil {
		t.Fatal(err)
	}
	if err := h2.cycle(t); err != nil {
		t.Fatalf("the cycle after the restart: %v", err)
	}
	got, err := h2.m.Volumes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED {
		t.Fatalf("the volume was refused after a crash between the CAS and the record: %+v\n%s", got, h2.log)
	}
	if sent := h2.dialer.sent(); strings.Contains(sent, "stop") {
		t.Errorf("the guest was stopped for a commit this host published itself: %s", sent)
	}
}

// TestAnUnfinishedCollapseOutlivesThePolicyThatStartedIt. The root may already be in the
// bucket with HEAD naming it, and this host's record of that is the only thing standing
// between the next open and "the published history is at a commit this host did not write".
// So a collapse that began is finished whatever the threshold says now.
func TestAnUnfinishedCollapseOutlivesThePolicyThatStartedIt(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 4}, false)
	h.pub.err = fmt.Errorf("the object store did not answer")
	if err := h.cycle(t); err == nil {
		t.Fatal("a publish that failed was reported as a collapse that worked")
	}
	began := *h.state(t).Compacting

	// The operator raises the threshold out of reach, and the process comes back.
	h2 := newHost(t, qcow.CompactionPolicy{AtLayers: 64}, h.paths, h.q, nil)
	if err := h2.cycle(t); err != nil {
		t.Fatalf("the cycle after the restart: %v", err)
	}
	if len(h2.pub.published) != 1 || h2.pub.published[0].layer.CommitID != began.CommitID {
		t.Fatalf("the root this host began is not the one it published: %+v, began as %+v", h2.pub.published, began)
	}
	st := h2.state(t)
	if st.Compacting != nil || len(st.Commits) != 1 || st.Commits[0].CommitID != began.CommitID {
		t.Errorf("this host's record of the published history is %+v, and HEAD names %s", st.Commits, began.CommitID)
	}
}

// TestACollapseIsAbandonedWhenTheHistoryMovesUnderIt. The flattened bytes reconstruct one
// commit. A root landing on a HEAD that has moved past it takes every commit in between out
// of the history — and each of those returned SUCCESS.
func TestACollapseIsAbandonedWhenTheHistoryMovesUnderIt(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 4}, true)
	h.pub.err = fmt.Errorf("the object store did not answer")
	if err := h.cycle(t); err == nil {
		t.Fatal("a publish that failed was reported as a collapse that worked")
	}
	began := *h.state(t).Compacting

	// A commit lands above the prefix while the collapse is stuck: an ordinary rotation,
	// published by this host between one cycle and the next.
	const laterLayer = "0198c0de-0000-7000-8000-000000000a44"
	const laterCommit = "0198c0de-0000-7000-8000-0000000c0044"
	h.paths.put(qcow.LayerImage(root, vol, laterLayer), []byte(`{"size":268435456,"data":{}}`), 1<<20)
	st := h.state(t)
	st.Commits = append(st.Commits, qcow.CommitLayer{CommitID: laterCommit, LayerID: laterLayer})
	if err := qcow.WriteState(h.paths, root, vol, st); err != nil {
		t.Fatal(err)
	}

	h.pub.err = nil
	if err := h.cycle(t); err != nil {
		t.Fatalf("the cycle after the history moved: %v", err)
	}
	for _, p := range h.pub.published {
		if p.layer.CommitID == began.CommitID {
			t.Fatalf("a root that flattens %s was published over a history that now ends at %s",
				began.ReplacesCommitID, laterCommit)
		}
	}
	if got := h.state(t).Compacting; got != nil && got.CommitID == began.CommitID {
		t.Errorf("the abandoned collapse is still on the books: %+v", got)
	}
}

// TestAConvertThatDidNotFinishPublishesNothing. The image is built under a temporary name
// and renamed, so nothing that is not a whole root can ever be given a layer's name — and
// the half-written file is left where the sweep can find it rather than published.
func TestAConvertThatDidNotFinishPublishesNothing(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 4}, true)
	h.q.convertErr = fmt.Errorf("qemu-img: Killed")

	if err := h.cycle(t); err == nil {
		t.Fatal("a convert that was killed was reported as a compaction that worked")
	}
	if len(h.pub.published) != 0 {
		t.Fatalf("a convert that was killed published %+v", h.pub.published)
	}
	// The name the root would have had is not on the disk: only the temporary one is.
	final := qcow.LayerImage(root, vol, h.state(t).Compacting.LayerID)
	there, err := h.paths.Exists(final)
	if err != nil {
		t.Fatal(err)
	}
	if there {
		t.Errorf("%s exists, so a file a convert did not finish is wearing a layer's name", final)
	}
	if there, err := h.paths.Exists(final + ".compacting"); err != nil || !there {
		t.Errorf("the half-written image is not at %s (%v); nothing else would ever name it", final+".compacting", err)
	}
}

// TestARootThatIsNotARootIsNotPublished. A convert that left a backing file behind would
// publish a commit whose layer needs a file no recovery is ever told to fetch: a commit that
// returned SUCCESS and reconstructs a hole.
func TestARootThatIsNotARootIsNotPublished(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 4}, true)
	h.q.backRoot = qcow.LayerImage(root, vol, oldestLayer)

	err := h.cycle(t)
	if err == nil || !strings.Contains(err.Error(), "not a root") {
		t.Fatalf("a flattened image that kept a backing file: want a refusal saying it is not a root, got %v", err)
	}
	if len(h.pub.published) != 0 {
		t.Fatalf("an image that is not a root was published: %+v", h.pub.published)
	}
	// Removed rather than left: the next cycle must convert again, not adopt this.
	tmp := qcow.LayerImage(root, vol, h.state(t).Compacting.LayerID) + ".compacting"
	if there, err := h.paths.Exists(tmp); err != nil || there {
		t.Errorf("the refused image is still at the temporary name (%v), so a retry would find it", err)
	}
}

// TestACollapseSetThatIsNotTheChainIsRefused. The set comes out of this host's record, and a
// record can name commits of a history this host no longer serves — after a fork it keeps
// them so a later rebuild can skip a download. Converting the top of a set like that would
// publish a root that has nothing to do with the chain being served, so the layers are
// walked with qemu-img first and the set is checked against what the walk found.
func TestACollapseSetThatIsNotTheChainIsRefused(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 4}, true)
	// A commit whose layer is not in the chain under the tip: the shape a fork leaves.
	const strayLayer = "0198c0de-0000-7000-8000-000000000aff"
	h.paths.put(qcow.LayerImage(root, vol, strayLayer), []byte(`{"size":268435456,"data":{}}`), 1<<20)
	st := h.state(t)
	st.Commits = append([]qcow.CommitLayer{{CommitID: "0198c0de-0000-7000-8000-0000000c00ff", LayerID: strayLayer}}, st.Commits...)
	if err := qcow.WriteState(h.paths, root, vol, st); err != nil {
		t.Fatal(err)
	}

	err := h.cycle(t)
	if err == nil || !strings.Contains(err.Error(), strayLayer) {
		t.Fatalf("a collapse set that is not the chain: want a refusal naming %s, got %v", strayLayer, err)
	}
	if len(h.pub.published) != 0 {
		t.Fatalf("a root was published out of a set that is not the chain: %+v", h.pub.published)
	}
	if got := h.q.converts(); len(got) != 0 {
		t.Fatalf("a convert ran over a set that is not the chain: %v", got)
	}
}

// TestAHeadThatMovedUnderACompactionFencesTheVolume. A compaction is a publish, so it meets
// the same fence: the alternative is a host going on serving a guest whose writes can never
// land.
func TestAHeadThatMovedUnderACompactionFencesTheVolume(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 4}, true)
	h.pub.err = fmt.Errorf("%w: another host published", commit.ErrHeadMoved)

	if err := h.cycle(t); err == nil {
		t.Fatal("a compaction that lost the CAS was reported as a success")
	}
	st := h.state(t)
	if st.Fenced == nil {
		t.Fatal("this host is not this volume's writer any more and nothing durable says so; the next restart resumes the guest")
	}
	if got := st.Fenced.Refusal; got != int32(storagev1.VolumeRefusal_VOLUME_REFUSAL_PUBLISH_FENCED) {
		t.Errorf("the record says refusal %d, want PUBLISH_FENCED", got)
	}
}

// TestACompactionWaitsForTheLayerThisHostStillOwes. v6 §11 keeps one layer in flight: a root
// that landed while an older sealed layer was still owed would put a commit into the history
// ahead of its predecessor.
func TestACompactionWaitsForTheLayerThisHostStillOwes(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 4}, true)
	// A layer sealed under the tip and not published: what a rotation whose publish has
	// not run yet leaves. It is discovered by the reconciliation, which owes it.
	const sealed = "0198c0de-0000-7000-8000-000000000a44"
	h.paths.put(qcow.LayerImage(root, vol, sealed), []byte(`{"size":268435456,"data":{}}`), 1<<20)
	st := h.state(t)
	st.Layers = []string{layerID, sealed, newestLayer}
	if err := qcow.WriteState(h.paths, root, vol, st); err != nil {
		t.Fatal(err)
	}

	if err := h.cycle(t); err != nil {
		t.Fatalf("applying: %v", err)
	}
	if got := h.q.converts(); len(got) != 0 {
		t.Errorf("a chain with an unpublished sealed layer was collapsed anyway: %v", got)
	}
	if got := h.pub.roots(); len(got) != 0 {
		t.Errorf("a root was published while this host still owed layer %s: %+v", sealed, got)
	}
}

// TestWhenAChainIsDueForCompaction is v6 §19's policy, asserted on whether the collapse
// actually ran.
func TestWhenAChainIsDueForCompaction(t *testing.T) {
	tests := []struct {
		name    string
		policy  qcow.CompactionPolicy
		trigger string
	}{
		{
			// The zero policy is what every caller has until somebody measures one, and it
			// must be silent rather than guess at v6 §19's example numbers.
			name:   "no policy at all",
			policy: qcow.CompactionPolicy{},
		},
		{
			name:   "a chain shallower than the threshold",
			policy: qcow.CompactionPolicy{AtLayers: 5},
		},
		{
			name:    "deep enough",
			policy:  qcow.CompactionPolicy{AtLayers: 4},
			trigger: "trigger=chain_depth",
		},
		{
			name:   "the incremental layers are under the size threshold",
			policy: qcow.CompactionPolicy{AtBytes: readBytes + 1},
		},
		{
			// The size arm measures what a convert reads, not what the volume occupies:
			// the tip is not collapsed, so its bytes are not the decision.
			name:    "the incremental layers are exactly at the size threshold",
			policy:  qcow.CompactionPolicy{AtBytes: readBytes},
			trigger: "trigger=incremental_bytes",
		},
		{
			name:    "both",
			policy:  qcow.CompactionPolicy{AtLayers: 2, AtBytes: 1},
			trigger: "trigger=chain_depth+incremental_bytes",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := deepChain(t, tt.policy, true)
			if err := h.cycle(t); err != nil {
				t.Fatalf("applying: %v", err)
			}
			switch {
			case tt.trigger == "" && len(h.pub.published) != 0:
				t.Fatalf("a chain that is not due was collapsed: %v", h.q.converts())
			case tt.trigger == "":
				return
			case len(h.pub.published) != 1:
				t.Fatalf("a chain that is due was not collapsed:\n%s", h.log)
			case !strings.Contains(h.log.String(), tt.trigger):
				t.Errorf("want %q in what was reported:\n%s", tt.trigger, h.log)
			}
		})
	}
}

// TestADueChainWithNothingToCollapseIsSilent. Depth counts the tip and the sealed layer this
// host has not published; neither is something a convert may touch — the tip is under a
// guest and a sealed layer is not a commit — so a chain can be past its threshold with
// nothing at all to collapse.
func TestADueChainWithNothingToCollapseIsSilent(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 2}, true)
	// No commits at all, and one layer sealed under the tip: depth 2, collapse nothing.
	if err := qcow.WriteState(h.paths, root, vol, qcow.State{Layers: []string{layerID, oldestLayer}}); err != nil {
		t.Fatalf("writing the state of a volume that has never published: %v", err)
	}

	if err := h.cycle(t); err != nil {
		t.Fatalf("applying: %v", err)
	}
	if got := h.q.converts(); len(got) != 0 {
		t.Fatalf("a chain whose collapse set is empty was converted: %v", got)
	}
	// The sealed layer under the tip is published as the ordinary commit it is; what must
	// not happen is a root.
	if got := h.pub.roots(); len(got) != 0 {
		t.Fatalf("a chain whose collapse set is empty published a root: %+v", got)
	}
}

// TestAPlanThatCannotBeMeasuredIsNotActedOn. The numbers decide whether the collapse happens
// at all, so a layer that cannot be stat'd stops it — rather than a plan with a hole in its
// arithmetic, which is a convert over a set nobody could measure.
func TestAPlanThatCannotBeMeasuredIsNotActedOn(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 4}, true)
	h.paths.remove(qcow.LayerImage(root, vol, middleLayer))

	err := h.cycle(t)
	if err == nil || !strings.Contains(err.Error(), middleLayer) {
		t.Fatalf("a chain with a layer that could not be measured: want an error naming %s, got %v", middleLayer, err)
	}
	if got := h.q.converts(); len(got) != 0 || len(h.pub.published) != 0 {
		t.Errorf("a plan nobody could measure was acted on: %v %+v", got, h.pub.published)
	}
}

// logLine is the last log line containing needle, or "" when there is none.
func logLine(out *bytes.Buffer, needle string) string {
	found := ""
	for _, l := range strings.Split(out.String(), "\n") {
		if strings.Contains(l, needle) {
			found = l
		}
	}
	return found
}

// TestWhatACompactionReports is v6 §21's two chain numbers plus what the collapse costs, on
// the line that is written *before* the convert — which is the line that is there when a
// collapse does not finish, and the only one an operator has to read then.
// local_disk_bytes is the directory and not the record: a layer that survived the sweep
// because it was minted before the tip is space somebody is paying for.
func TestWhatACompactionReports(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{AtLayers: 4}, true)
	const orphanBytes = 7 << 20
	h.paths.put(qcow.LayerImage(root, vol, "0198c0de-0000-7000-8000-0000000000ff"), []byte(`{"size":1,"data":{}}`), orphanBytes)

	if err := h.cycle(t); err != nil {
		t.Fatalf("applying: %v", err)
	}
	line := logLine(h.log, "collapsing this volume's published prefix")
	if line == "" {
		t.Fatalf("a collapse that happened was not reported:\n%s", h.log)
	}
	for _, want := range []string{
		"chain_depth=4",
		fmt.Sprintf("local_disk_bytes=%d", diskBytes+orphanBytes),
		"trigger=chain_depth",
		"collapsed_layers=3",
		"collapsed_oldest=" + oldestLayer,
		"collapsed_newest=" + newestLayer,
		"into_commit_id=" + collapseNewest,
		fmt.Sprintf("read_bytes=%d", readBytes),
		fmt.Sprintf("write_bytes_at_most=%d", size),
		fmt.Sprintf("virtual_size=%d", size),
	} {
		if !strings.Contains(line, want) {
			t.Errorf("the report does not carry %q:\n%s", want, line)
		}
	}
}

// TestTheReportCarriesTheChainAndTheDiskItOccupies. chain_depth and local_disk_bytes are
// two of §28's numbers and they reach the Control Plane through Volumes(), not through a
// log line: the compaction report is written only when a policy is set and the chain is
// past it, and no caller in this tree sets one. So the assertion is on the report, with no
// policy at all — which is what every host in the fleet has.
func TestTheReportCarriesTheChainAndTheDiskItOccupies(t *testing.T) {
	h := deepChain(t, qcow.CompactionPolicy{}, true)

	if err := h.cycle(t); err != nil {
		t.Fatalf("applying: %v", err)
	}
	if got := h.q.converts(); len(got) != 0 {
		t.Fatalf("no policy is set and a chain was collapsed anyway: %v", got)
	}
	got, err := h.m.Volumes(t.Context())
	if err != nil {
		t.Fatalf("Volumes: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("this host holds %d volumes, want 1", len(got))
	}
	if got[0].ChainDepth != 4 {
		t.Errorf("chain depth = %d, want the 4 layers a guest reads through", got[0].ChainDepth)
	}
	if got[0].LocalDiskBytes != diskBytes {
		t.Errorf("local disk bytes = %d, want the %d every layer file of this volume occupies",
			got[0].LocalDiskBytes, diskBytes)
	}
}

func TestACompactionPolicyThatCannotMeanAnythingIsRefused(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		policy qcow.CompactionPolicy
		want   string
	}{
		{
			// Every volume has one layer, so a threshold of one calls every volume due for
			// a collapse of nothing.
			name:   "one layer",
			policy: qcow.CompactionPolicy{AtLayers: 1},
			want:   "below the one layer every volume has",
		},
		{name: "negative layers", policy: qcow.CompactionPolicy{AtLayers: -1}, want: "cannot be negative"},
		{name: "negative bytes", policy: qcow.CompactionPolicy{AtBytes: -1}, want: "cannot be negative"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := qcow.Config{
				Root: root, QemuImg: "/qemu-img", ProbeTimeout: time.Second, Compaction: tt.policy,
			}.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validating %+v: want an error saying %q, got %v", tt.policy, tt.want, err)
			}
		})
	}
}
