package agent_test

import (
	"log/slog"
	"strconv"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// An operator watching a clone take minutes to stop can now read why.
//
// A clone's first publish flattens: `image.uploadChunks` walks `view.Ranges()`, and cow
// reports the parent's ranges merged with this layer's, so the whole inherited dataset is
// re-sealed and re-uploaded under the clone's own prefix — whatever the clone itself wrote.
// internal/image's TestACloneFirstStopCopiesWhatItInherited measures that in bytes; this is
// the seam, and it asserts the two things an operator has: **the bucket** and **the line
// the process printed**.
//
// The parent's own stop is asserted too, and that is not padding. A message printed on
// every publish would satisfy any assertion about the clone's, and this is precisely the
// shape CLAUDE.md lists as a test that proves nothing — so the parent, which inherited
// nothing, must print the *other* line.
func TestAnOperatorIsToldWhenAStopIsCopyingItsParentsDataset(t *testing.T) {
	// Eight consecutive blocks, so the parent's data is one range and therefore one
	// chunk — the golden-image shape, and the one where the copy is least avoidable.
	const parentBlocks = 8
	const parentBytes = parentBlocks * testBlockSize

	var out syncBuffer
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(restore) })

	store := sim.NewObjectStore()

	parent := cloneSession(t, "/var/lib/spin-parent", sim.NewDisk(), store)
	p := desiredVolume(t, 1)
	pdev := serveClone(t, parent, p)
	for i := range int64(parentBlocks) {
		writeBlock(t, pdev, i*testBlockSize, 0xA1)
	}
	snapID := ids.New().String()
	if _, err := parent.Snapshot(t.Context(), p.GetVolumeId(), snapID); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := parent.Close(t.Context()); err != nil {
		t.Fatalf("closing the parent's Agent: %v", err)
	}

	// The clone writes one block and stops. One block is the whole point: what it pays is
	// not proportional to it.
	c := desiredVolume(t, 1)
	c.ParentSnapshotId, c.ParentVolumeId = snapID, p.GetVolumeId()
	clone := cloneSession(t, "/var/lib/spin-clone", sim.NewDisk(), store)
	writeBlock(t, serveClone(t, clone, c), 0, 0xC2)
	if err := clone.Close(t.Context()); err != nil {
		t.Fatalf("closing the clone's Agent: %v", err)
	}

	// The bucket first, because it is the fact and the log line is only a report of it.
	parentResident := chunkBytesUnder(t, store, p.GetVolumeId())
	cloneResident := chunkBytesUnder(t, store, c.GetVolumeId())
	t.Logf("the parent's prefix holds %d chunk bytes for %d bytes of guest data; the clone's holds %d after writing %d bytes",
		parentResident, parentBytes, cloneResident, testBlockSize)
	if cloneResident != parentResident {
		t.Errorf("the clone's prefix holds %d chunk bytes and its parent's holds %d; a first stop copies the whole inherited dataset, so they must match",
			cloneResident, parentResident)
	}

	// And the line that says so, on the clone's stop, carrying the number.
	const copying = "which copies the dataset it inherited from its parent"
	cloneLine := logLineFor(t, out.String(), c.GetVolumeId(), "publishing the volume's image")
	if !strings.Contains(cloneLine, copying) {
		t.Errorf("the clone's publish printed %q; an operator waiting on it has no way to know it is copying %d inherited bytes",
			cloneLine, parentBytes)
	}
	for _, want := range []string{
		"inherited_bytes=" + strconv.FormatInt(parentBytes, 10),
		"image_bytes=" + strconv.FormatInt(parentBytes, 10),
		"parent_snapshot_id=" + snapID,
	} {
		if !strings.Contains(cloneLine, want) {
			t.Errorf("the clone's publish line %q does not carry %q", cloneLine, want)
		}
	}

	// The parent inherited nothing, so it must print the other line. Without this the
	// assertions above pass for a message printed unconditionally.
	parentLine := logLineFor(t, out.String(), p.GetVolumeId(), "publishing the volume's image")
	if strings.Contains(parentLine, copying) {
		t.Errorf("the parent's publish claims it is copying an inherited dataset: %q", parentLine)
	}
	if !strings.Contains(parentLine, "image_bytes="+strconv.FormatInt(parentBytes, 10)) {
		t.Errorf("the parent's publish line %q does not carry the size of the image it wrote", parentLine)
	}
}

// chunkBytesUnder is what the bucket holds for one volume's data, which is what it is
// billed for. Manifests are excluded: they are structural, and their size is decided by
// how many chunks there are rather than by how much guest data was stored.
func chunkBytesUnder(t *testing.T, store objectstore.Store, volumeID string) int64 {
	t.Helper()
	// image.Prefix's shape, spelled out: this asserts on where the bytes landed, and
	// deriving the answer from the function under observation would assert nothing.
	objs, err := store.List(t.Context(), "image/"+volumeID+"/chunks/")
	if err != nil {
		t.Fatalf("listing %s: %v", volumeID, err)
	}
	var n int64
	for _, o := range objs {
		n += o.Size
	}
	return n
}

// logLineFor is the last line mentioning both a message and a volume id. Last, not first:
// a volume publishes once per session and the interesting session is the most recent one.
func logLineFor(t *testing.T, log, volumeID, msg string) string {
	t.Helper()
	var found string
	for _, line := range strings.Split(log, "\n") {
		if strings.Contains(line, volumeID) && strings.Contains(line, msg) {
			found = line
		}
	}
	if found == "" {
		t.Fatalf("no %q line for volume %s in:\n%s", msg, volumeID, log)
	}
	return found
}
