//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/qcow"
	"github.com/spin-stack/storage/internal/simio/real"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/testinfra"
)

// The volume this lane collapses. Small, because every layer of it is written in full.
const (
	compactSize   = int64(4 << 20)
	compactLayers = 4
)

// TestACompactedRootReadsBackAsTheChainItReplaced is the byte-level half of "a commit that
// returned SUCCESS is never lost", against the pinned qemu-img rather than a model of it.
//
// Everything else about compaction can be asserted in-process: which files the plan names,
// what the process was asked to run, what the record says afterwards. What none of those
// can settle is whether the image `qemu-img convert` actually produced holds the same disk
// the chain held, and whether the chain a `rebase -u` leaves behind still resolves and
// still reads the same. Those are the two claims the feature makes, so the disk is read
// back three times and compared byte for byte: through the prefix, through the root that
// replaced it, and through the tip after it has been repointed.
func TestACompactedRootReadsBackAsTheChainItReplaced(t *testing.T) {
	qemuImg := testinfra.Binary(t, "qemu-img")
	dir := t.TempDir()
	root := filepath.Join(dir, "data")
	vol := ids.New().String()
	files := real.NewPaths()
	for _, d := range []string{qcow.LayersDir(root, vol), filepath.Dir(qcow.ActivePointer(root, vol))} {
		if err := files.MkdirAll(d); err != nil {
			t.Fatalf("making %s: %v", d, err)
		}
	}

	// Four layers, each one a full-disk pattern written over the one below it. Full and
	// not partial because what a partial write leaves in a qcow2 is qemu-img's business,
	// and what this asserts is ours: every layer composes to a *different* disk, so the
	// bytes that come back name the layers they were made of.
	var layers []string
	for i, fill := range []byte{0xA0, 0xB1, 0xC2, 0xD3} {
		id := ids.New().String()
		image := qcow.LayerImage(root, vol, id)
		create := []string{"create", "-f", "qcow2", image, "4M"}
		if i > 0 {
			create = []string{"create", "-f", "qcow2", "-b", qcow.LayerImage(root, vol, layers[i-1]), "-F", "qcow2", image, "4M"}
		}
		run(t, qemuImg, create...)
		// The pattern carries the layer's own index in every fourth byte, so a disk read
		// back says which layer's write it is showing.
		patch := filepath.Join(dir, "patch.raw")
		body := bytes.Repeat([]byte{fill, byte(i), 0x5A, 0xC3}, int(compactSize)/4)
		if err := files.WriteAtomic(patch, body); err != nil {
			t.Fatalf("writing the pattern: %v", err)
		}
		// -O is not redundant with -n: without it the *target* is probed, and a qcow2 that
		// has allocated nothing is probed as a raw file of a couple of hundred kilobytes,
		// which qemu-img then refuses as smaller than its input.
		run(t, qemuImg, "convert", "-n", "-f", "raw", "-O", "qcow2", patch, image)
		layers = append(layers, id)
	}
	prefixTop := qcow.LayerImage(root, vol, layers[2])
	tip := qcow.LayerImage(root, vol, layers[3])
	if err := files.WriteAtomic(qcow.ActivePointer(root, vol), []byte(tip)); err != nil {
		t.Fatalf("writing the pointer: %v", err)
	}
	if err := qcow.WriteState(files, root, vol, qcow.State{
		Commits: []qcow.CommitLayer{
			{CommitID: ids.New().String(), LayerID: layers[0]},
			{CommitID: ids.New().String(), LayerID: layers[1]},
			{CommitID: ids.New().String(), LayerID: layers[2]},
		},
		Layers: []string{layers[3], layers[2]},
	}); err != nil {
		t.Fatalf("recording the published prefix: %v", err)
	}
	// What the prefix reconstructs, and what the guest's whole chain reconstructs, read
	// before anything is collapsed.
	wantPrefix := raw(t, files, qemuImg, dir, "prefix", prefixTop)
	wantGuest := raw(t, files, qemuImg, dir, "guest", tip)

	pub := &collectingPublisher{files: files}
	m, err := qcow.New(t.Context(), qcow.Config{
		Root: root, QemuImg: qemuImg, ProbeTimeout: 30 * time.Second,
		Compaction: qcow.CompactionPolicy{AtLayers: compactLayers},
	}, qcow.Deps{
		Clock: sim.NewClock(time.Unix(1_700_000_000, 0)),
		Disk:  sim.NewDisk(),
		// No VM: the QMP socket does not exist, so the volume is unattached and the
		// collapse can finish in one cycle. A rebase under a running guest is what v6 §5
		// forbids, and internal/qcow's own tests hold that end.
		Runner: real.NewRunner(), Paths: files, Dialer: real.NewUnixDialer(),
		Recovery: noHistory{}, Publisher: pub,
	})
	if err != nil {
		t.Fatalf("building a manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })

	if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{{
		VolumeId: vol, SizeBytes: compactSize, Epoch: 1,
		State: storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}}); err != nil {
		t.Fatalf("the cycle that collapses the chain: %v", err)
	}
	if len(pub.published) != 1 || pub.published[0].ReplacesCommitID == "" {
		t.Fatalf("the chain was not published as one root: %+v", pub.published)
	}
	got := pub.published[0]

	// The disk, read back through the pinned qemu-img: what the prefix reconstructed, and
	// what the commit that replaces it reconstructs.
	if flat := raw(t, files, qemuImg, dir, "root", got.Path); !bytes.Equal(flat, wantPrefix) {
		t.Errorf("the compacted root does not read back as the prefix it replaces: %s", firstDifference(flat, wantPrefix))
	}
	// And it is not the guest's disk: the tip is unpublished, so a root carrying it would
	// be a commit claiming writes no commit ever promised.
	if flat := raw(t, files, qemuImg, dir, "root", got.Path); bytes.Equal(flat, wantGuest) {
		t.Error("the compacted root reads back as the tip a guest was writing to, so it carries unpublished writes")
	}
	// A root stands on nothing. One that kept a backing file would be a commit whose layer
	// needs a file no recovery is ever told to fetch.
	if out := run(t, qemuImg, "info", "--output=json", got.Path); bytes.Contains(out, []byte("backing-filename")) {
		t.Errorf("the published root has a backing file: %s", out)
	}
	if got.VirtualSize != compactSize {
		t.Errorf("the root claims a %d-byte disk; the guest's is %d", got.VirtualSize, compactSize)
	}

	// The other half: the chain the guest reads is now the tip over the root, it resolves
	// end to end — `--backing-chain` opens every layer and exits 1 when one is missing —
	// and every byte the guest could read before is still there.
	chain := walk(t, qemuImg, tip)
	if len(chain) != 2 || chain[1] != got.Path {
		t.Errorf("the guest's chain is %v; it should be the tip over the compacted root alone", chain)
	}
	if after := raw(t, files, qemuImg, dir, "after", tip); !bytes.Equal(after, wantGuest) {
		t.Errorf("the guest's disk changed when the chain was repointed: %s", firstDifference(after, wantGuest))
	}
}

// collectingPublisher stands in for the object store: it keeps what it was handed, and the
// layer file is still on disk for the assertions to read.
type collectingPublisher struct {
	files     *real.Paths
	published []qcow.SealedLayer
}

func (p *collectingPublisher) Publish(_ context.Context, l qcow.SealedLayer) error {
	if _, err := p.files.Size(l.Path); err != nil {
		return fmt.Errorf("publishing %s: %w", l.Path, err)
	}
	p.published = append(p.published, l)
	return nil
}

// noHistory is the object store answering that this volume has never published, which is
// what makes the local chain the only one there is.
type noHistory struct{}

func (noHistory) RestoreFrom(_ context.Context, l qcow.Lineage, _ int64) (qcow.Restored, error) {
	return qcow.Restored{}, fmt.Errorf("volume %s: %w", l.VolumeID, commit.ErrNoHead)
}

func (noHistory) Current(_ context.Context, volumeID string) (string, error) {
	return "", fmt.Errorf("volume %s: %w", volumeID, commit.ErrNoHead)
}

// raw is an image's guest-visible content, through its whole backing chain.
func raw(t *testing.T, files *real.Paths, qemuImg, dir, name, image string) []byte {
	t.Helper()
	out := filepath.Join(dir, name+".raw")
	run(t, qemuImg, "convert", "-O", "raw", image, out)
	body, err := files.ReadFile(out)
	if err != nil {
		t.Fatalf("reading %s back: %v", out, err)
	}
	return body
}

// walk is the files a guest reading `image` reads through, top first.
func walk(t *testing.T, qemuImg, image string) []string {
	t.Helper()
	var chain []struct {
		Filename string `json:"filename"`
	}
	if err := json.Unmarshal(run(t, qemuImg, "info", "--output=json", "--backing-chain", image), &chain); err != nil {
		t.Fatalf("decoding the chain under %s: %v", image, err)
	}
	out := make([]string, 0, len(chain))
	for _, l := range chain {
		out = append(out, l.Filename)
	}
	return out
}

func run(t *testing.T, name string, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v: %v\n%s", name, args, err, out)
	}
	return out
}

// firstDifference says where two disks stop agreeing, because a diff of four megabytes is
// not a message anybody reads.
func firstDifference(got, want []byte) string {
	if len(got) != len(want) {
		return fmt.Sprintf("the two images are %d and %d bytes", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			return fmt.Sprintf("offset %d: 0x%X against 0x%X", i, got[i], want[i])
		}
	}
	return "no difference"
}
