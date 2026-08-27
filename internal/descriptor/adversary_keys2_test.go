package descriptor_test

import (
	"testing"

	"github.com/spin-stack/storage/internal/descriptor"
)

// VolumeOfKey decides which objects under `volumes/` its two callers must treat as
// volume descriptors, and it says of itself that it exists because "two components that
// parse the same string two ways disagree silently". It is looser than the thing it is
// the inverse of: Key is only ever called with a v7 uuid, and this accepts any run of
// bytes with no slash in it — including none at all.
//
// Neither caller can survive that. controlplane.listDescriptors and
// controlplane.refuseIfAnythingDescends both read every key this says yes to, and both
// abort the whole operation when one cannot be read — correctly, because a descriptor
// they cannot parse is a volume they cannot account for. So one object at
// `volumes//descriptor.json`, which names no volume and can be written by anything with
// bucket access, stops -rebuild-metadata for the entire fleet and stops the crypto-shred
// of every volume in it. The refusal is unclearable from the operator's side: the error
// names an object, and deleting objects is what they were trying to do.
//
// The fix is here rather than at either caller, for the reason the doc comment already
// gives: a key that names no volume is not a volume's key, and both callers must reach
// that conclusion the same way.
func TestAdversaryVolumeOfKeyClaimsKeysThatNameNoVolume(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		// The empty id. `Key("")` produces exactly this, and nothing else in the tree
		// can produce it, so a round trip through Key/VolumeOfKey does not notice.
		{"no id at all", "volumes//descriptor.json"},
		// Not a uuid. Every volume id in this system is a v7 uuid (INV-22): the catalog
		// has a CHECK on the version nibble, ids.Parse is what controlplane.checkKey
		// runs before it will unwrap anything, and a key this shape can only have been
		// written by something that is not this fleet's Control Plane.
		{"a name, not an id", "volumes/notes/descriptor.json"},
		{"a uuid that is not v7", "volumes/00000000-0000-4000-8000-000000000000/descriptor.json"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if id, ok := descriptor.VolumeOfKey(tt.key); ok {
				t.Fatalf("VolumeOfKey(%q) = %q, true: every reader of %s now has to read that "+
					"object, and refuses the operation it was in the middle of when it cannot",
					tt.key, id, descriptor.Prefix)
			}
		})
	}
}

// The control, in the same shape: a real key still resolves. Without it the test above
// would pass against a VolumeOfKey that rejected everything.
func TestAdversaryVolumeOfKeyStillResolvesARealVolume(t *testing.T) {
	const id = "01900000-0000-7000-8000-000000000001"
	got, ok := descriptor.VolumeOfKey(descriptor.Key(id))
	if !ok || got != id {
		t.Fatalf("VolumeOfKey(Key(%s)) = %q, %v", id, got, ok)
	}
}
