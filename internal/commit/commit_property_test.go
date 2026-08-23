package commit_test

import (
	"bytes"
	"crypto/rand"
	"reflect"
	"testing"

	"pgregory.net/rapid"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/framed"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

func id(rt *rapid.T, label string) string {
	return ids.NewAt(int64(rapid.IntRange(1, 1<<40).Draw(rt, label)), rand.Reader).String()
}

func genManifest(rt *rapid.T) commit.Manifest {
	parent := ""
	if rapid.Bool().Draw(rt, "has_parent") {
		parent = id(rt, "parent_ms")
	}
	digest := rapid.StringMatching(`[0-9a-f]{64}`).Draw(rt, "sha256")
	return commit.Manifest{
		// Drawn, not stamped: Write must overwrite whatever is here, and a generator
		// that only ever produced the right number could not show that.
		FormatVersion:  rapid.IntRange(0, 9).Draw(rt, "format_version"),
		VolumeID:       id(rt, "volume_ms"),
		CommitID:       id(rt, "commit_ms"),
		ParentCommitID: parent,
		Epoch:          int64(rapid.IntRange(0, 1<<20).Draw(rt, "epoch")),
		VirtualSize:    int64(rapid.IntRange(1, 1<<42).Draw(rt, "virtual_size")),
		Layer: commit.Layer{
			ObjectKey:  commit.LayerKey(digest),
			SizeBytes:  int64(rapid.IntRange(1, 1<<40).Draw(rt, "layer_size")),
			SHA256:     digest,
			FrameBytes: int32(rapid.IntRange(512, 1<<22).Draw(rt, "frame_bytes")),
			LayerID:    id(rt, "layer_ms"),
		},
	}
}

// TestManifestRoundTrips is the base case every corruption test needs: without it, a
// truncation test that always errors would pass against a Read that never worked.
func TestManifestRoundTrips(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		want := genManifest(rt)
		store := sim.NewObjectStore()
		if err := commit.WriteManifest(t.Context(), store, want); err != nil {
			rt.Fatalf("write: %v", err)
		}
		got, err := commit.ReadManifest(t.Context(), store, want.VolumeID, want.CommitID)
		if err != nil {
			rt.Fatalf("read: %v", err)
		}
		if got.FormatVersion != framed.FormatVersion {
			rt.Fatalf("Write stamped format_version %d over a drawn %d; want %d",
				got.FormatVersion, want.FormatVersion, framed.FormatVersion)
		}
		// Every other field compared as a whole, so a field added to the struct and
		// forgotten by json is caught here rather than by whoever needed it.
		gotBlank, wantBlank := got, want
		gotBlank.FormatVersion, wantBlank.FormatVersion = 0, 0
		if !reflect.DeepEqual(gotBlank, wantBlank) {
			rt.Fatalf("round trip changed the manifest:\n want %+v\n  got %+v", want, got)
		}
	})
}

// TestManifestTruncationIsDetected: cut the object at any byte and Read must fail. The
// failure mode ruled out is a short read decoding into a manifest with default values —
// a commit that names no layer, has no parent, and reconstructs a volume of size zero.
func TestManifestTruncationIsDetected(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		m := genManifest(rt)
		store := sim.NewObjectStore()
		if err := commit.WriteManifest(t.Context(), store, m); err != nil {
			rt.Fatalf("write: %v", err)
		}
		key := commit.ManifestKey(m.VolumeID, m.CommitID)
		full, err := store.Get(t.Context(), key)
		if err != nil {
			rt.Fatalf("get: %v", err)
		}
		at := rapid.IntRange(0, len(full)-1).Draw(rt, "truncate_at")
		if _, err := store.Put(t.Context(), key, full[:at], objectstore.PutOptions{}); err != nil {
			rt.Fatalf("put truncated: %v", err)
		}
		if got, err := commit.ReadManifest(t.Context(), store, m.VolumeID, m.CommitID); err == nil {
			rt.Fatalf("a manifest truncated to %d/%d bytes decoded as %+v", at, len(full), got)
		}
	})
}

// TestManifestBitFlipIsDetected: any bit, at any offset, must make Read fail. The silent
// case is the one this is for — a flipped digit in `virtual_size` or in the layer's
// `sha256` yields a different and perfectly valid manifest, and a recovery would then
// rebuild the volume at the wrong size or refuse a layer that is intact.
func TestManifestBitFlipIsDetected(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		m := genManifest(rt)
		store := sim.NewObjectStore()
		if err := commit.WriteManifest(t.Context(), store, m); err != nil {
			rt.Fatalf("write: %v", err)
		}
		key := commit.ManifestKey(m.VolumeID, m.CommitID)
		full, err := store.Get(t.Context(), key)
		if err != nil {
			rt.Fatalf("get: %v", err)
		}
		at := rapid.IntRange(0, len(full)-1).Draw(rt, "byte")
		bit := rapid.IntRange(0, 7).Draw(rt, "bit")
		corrupt := bytes.Clone(full)
		corrupt[at] ^= 1 << bit
		if _, err := store.Put(t.Context(), key, corrupt, objectstore.PutOptions{}); err != nil {
			rt.Fatalf("put corrupt: %v", err)
		}
		if got, err := commit.ReadManifest(t.Context(), store, m.VolumeID, m.CommitID); err == nil {
			rt.Fatalf("a manifest with bit %d of byte %d flipped decoded as %+v", bit, at, got)
		}
	})
}

// TestHeadSurvivesCorruption is the same pair over the one mutable object, and it is
// where it matters most: a manifest that will not decode costs one commit, and a HEAD
// that will not decode — or worse, decodes into a different commit id — is the whole
// volume pointed at somebody else's history.
func TestHeadSurvivesCorruption(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		volumeID, commitID := id(rt, "volume_ms"), id(rt, "commit_ms")
		store := sim.NewObjectStore()
		if err := commit.CASHead(t.Context(), store, volumeID, commitID, ""); err != nil {
			rt.Fatalf("creating HEAD: %v", err)
		}
		full, err := store.Get(t.Context(), commit.HeadKey(volumeID))
		if err != nil {
			rt.Fatalf("get: %v", err)
		}
		broken := bytes.Clone(full)
		if rapid.Bool().Draw(rt, "truncate") {
			broken = broken[:rapid.IntRange(0, len(full)-1).Draw(rt, "truncate_at")]
		} else {
			at := rapid.IntRange(0, len(full)-1).Draw(rt, "byte")
			broken[at] ^= 1 << rapid.IntRange(0, 7).Draw(rt, "bit")
		}
		if _, err := store.Put(t.Context(), commit.HeadKey(volumeID), broken, objectstore.PutOptions{}); err != nil {
			rt.Fatalf("put broken: %v", err)
		}
		if h, _, err := commit.ReadHead(t.Context(), store, volumeID); err == nil {
			rt.Fatalf("a damaged HEAD decoded as %+v", h)
		}
	})
}
