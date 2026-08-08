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

// What a clone's stop puts in the bucket, and what it tells the operator it is doing.
//
// internal/image measures the cost against views it builds itself; this is the seam, and
// it asserts the two things an operator has: **the bucket** and **the line the process
// printed**. It is the test that would have caught an Agent that resolved the lineage
// wrongly — the whole key space is decided by one id that fetchBase derives from a walk,
// and every assertion inside internal/image passes when that id is supplied by hand.
//
// The parent's data is deliberately **two disjoint regions**, so it is two chunks and the
// clone touches one of them. That is the only shape in which the numbers say anything:
// with a single chunk, "the clone re-uploaded everything it inherited" and "the clone
// paid for the chunk it wrote in" produce the same bytes.
//
// The parent's own stop is asserted too, and that is not padding. A message printed on
// every publish would satisfy any assertion about the clone's, and this is precisely the
// shape CLAUDE.md lists as a test that proves nothing — so the parent, which inherited
// nothing, must print the *other* line.
func TestACloneStoresOnlyWhatItTouchedInItsLineageAndSaysSo(t *testing.T) {
	// Two regions of four blocks with a four-block gap between them, so cow reports two
	// ranges and the publisher makes two chunks.
	const (
		regionBlocks = 4
		regionBytes  = regionBlocks * testBlockSize
		secondRegion = 2 * regionBytes
		parentBytes  = 2 * regionBytes
	)

	var out syncBuffer
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(restore) })

	store := sim.NewObjectStore()

	parent := cloneSession(t, "/var/lib/spin-parent", sim.NewDisk(), store)
	p := desiredVolume(t, 1)
	// The descriptor a provisioner would have written. The clone's Agent reads it to
	// find whether the parent descends from anything itself, and refuses to attach when
	// it is missing rather than guessing that the lineage ends there (agent.parentChain).
	writeDescriptor(t, store, p.GetVolumeId(), lineageLink{})
	pdev := serveClone(t, parent, p)
	for i := range int64(regionBlocks) {
		writeBlock(t, pdev, i*testBlockSize, 0xA1)
		writeBlock(t, pdev, secondRegion+i*testBlockSize, 0xB2)
	}
	snapID := ids.New().String()
	if _, err := parent.Snapshot(t.Context(), p.GetVolumeId(), snapID); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := parent.Close(t.Context()); err != nil {
		t.Fatalf("closing the parent's Agent: %v", err)
	}

	lineage := chunkBytesUnder(t, store, "chunks/"+p.GetVolumeId()+"/")
	if lineage == 0 {
		t.Fatalf("the parent's stop stored no chunk bytes under chunks/%s/", p.GetVolumeId())
	}

	// The clone writes one block, into the first region only, and stops.
	c := desiredVolume(t, 1)
	c.ParentSnapshotId, c.ParentVolumeId = snapID, p.GetVolumeId()
	clone := cloneSession(t, "/var/lib/spin-clone", sim.NewDisk(), store)
	writeBlock(t, serveClone(t, clone, c), 0, 0xC2)
	if err := clone.Close(t.Context()); err != nil {
		t.Fatalf("closing the clone's Agent: %v", err)
	}

	// The bucket first, because it is the fact and the log line is only a report of it.
	//
	// Nothing under the clone's own prefix: that is where every chunk in this repository
	// lived until the lineage key space, and an Agent that failed to resolve its root
	// would put them back there while every read in this test still passed, because it
	// would then look for them there too.
	if own := chunkBytesUnder(t, store, "image/"+c.GetVolumeId()+"/chunks/"); own != 0 {
		t.Errorf("the clone stored %d chunk bytes under its own prefix; a clone's chunks belong to its lineage", own)
	}
	grew := chunkBytesUnder(t, store, "chunks/"+p.GetVolumeId()+"/") - lineage
	t.Logf("the parent's two regions are %d chunk bytes; the clone's stop added %d after writing %d bytes into one of them",
		lineage, grew, testBlockSize)
	// One region, not two: the region the clone never touched is byte-identical to its
	// parent's, so the publisher Heads its key, finds it, and transfers nothing.
	if want := lineage / 2; grew != want {
		t.Errorf("the clone's stop added %d bytes to its lineage's chunk store, want %d — it wrote into one of two regions, so it pays for one",
			grew, want)
	}

	// And the line that says so, on the clone's stop, carrying the numbers.
	const inherited = "names the dataset it inherited from its parent"
	cloneLine := logLineFor(t, out.String(), c.GetVolumeId(), "publishing the volume's image")
	if !strings.Contains(cloneLine, inherited) {
		t.Errorf("the clone's publish printed %q; an operator waiting on it has no way to know it is writing an image over %d inherited bytes",
			cloneLine, parentBytes)
	}
	for _, want := range []string{
		"inherited_bytes=" + strconv.FormatInt(parentBytes, 10),
		"image_bytes=" + strconv.FormatInt(parentBytes, 10),
		"parent_snapshot_id=" + snapID,
		// The root an operator needs to find the bytes at all: with the chunk store
		// shared, "where is this volume's data" is no longer answered by its own id.
		"lineage_root=" + p.GetVolumeId(),
	} {
		if !strings.Contains(cloneLine, want) {
			t.Errorf("the clone's publish line %q does not carry %q", cloneLine, want)
		}
	}

	// The parent inherited nothing, so it must print the other line. Without this the
	// assertions above pass for a message printed unconditionally.
	parentLine := logLineFor(t, out.String(), p.GetVolumeId(), "publishing the volume's image")
	if strings.Contains(parentLine, inherited) {
		t.Errorf("the parent's publish claims it inherited a dataset: %q", parentLine)
	}
	if !strings.Contains(parentLine, "image_bytes="+strconv.FormatInt(parentBytes, 10)) {
		t.Errorf("the parent's publish line %q does not carry the size of the image it wrote", parentLine)
	}
}

// chunkBytesUnder is what the bucket holds under one prefix, which is what it is billed
// for. Manifests are excluded by the prefixes callers pass: they are structural, and
// their size is decided by how many chunks there are rather than by how much guest data
// was stored.
//
// The prefixes are spelled out at the call sites rather than taken from image.ChunksPrefix
// — this asserts on where the bytes landed, and deriving the answer from the function
// under observation would assert nothing.
func chunkBytesUnder(t *testing.T, store objectstore.Store, prefix string) int64 {
	t.Helper()
	objs, err := store.List(t.Context(), prefix)
	if err != nil {
		t.Fatalf("listing %s: %v", prefix, err)
	}
	var n int64
	for _, o := range objs {
		n += o.Size
	}
	return n
}

// logLineFor is the last line whose message is msg and whose volume_id is this volume.
// Last, not first: a volume publishes once per session and the interesting session is the
// most recent one.
//
// It matches on `volume_id=` and not on the bare id, which is not fussiness: since the
// publish line carries `lineage_root`, a clone's line mentions its parent's id too, and a
// looser match handed the clone's line back when asked for the parent's — and the parent's
// assertions are the ones that stop this file from proving nothing.
func logLineFor(t *testing.T, log, volumeID, msg string) string {
	t.Helper()
	var found string
	for _, line := range strings.Split(log, "\n") {
		if strings.Contains(line, "volume_id="+volumeID) && strings.Contains(line, msg) {
			found = line
		}
	}
	if found == "" {
		t.Fatalf("no %q line for volume %s in:\n%s", msg, volumeID, log)
	}
	return found
}
