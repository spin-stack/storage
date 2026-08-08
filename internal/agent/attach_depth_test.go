package agent_test

import (
	"context"
	"testing"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// What depth costs at attach, in object-store reads, now that an image is a delta.
//
// This is the number the depth ceiling is chosen against (§20.1, and step 4 of
// CHUNK-ADDRESSING-SPEC, which turns it into a refusal at create). Before this increment
// the question barely existed: publishing flattened, so a clone that had stopped once read
// its own manifest and nothing else, whatever its depth. Now every attach composes the
// whole ancestry, so the read path is linear in the chain and the constant is worth having
// written down rather than reasoned about.
//
// **Both sessions are measured, and the second is the one that changed.** A clone with no
// image of its own always read through its ancestors; a clone that has stopped once did
// not, and now does. If those two numbers are equal, depth costs the same for the life of
// the volume, which is what makes a ceiling a bound and not a delay.
//
// GETs, not bytes: what grows with depth is the number of round trips — a descriptor, a
// snapshot manifest and that manifest's chunks, per link — and against a real object store
// each is a request whose latency does not amortise. The bytes are the ancestors' data,
// which the volume would have to read wherever it lived.
func TestWhatDepthCostsAtAttach(t *testing.T) {
	// Each ancestor writes one block nothing above it ever touches, so the assertion that
	// the clone reads every one of them is what proves the walk went all the way down —
	// and a lineage where the reads pass is the only one whose cost is worth quoting.
	tests := []struct {
		name  string
		links int
	}{
		{name: "a clone of a volume that descends from nothing", links: 1},
		{name: "a clone of a clone", links: 2},
		{name: "a chain of three", links: 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &countingGets{Store: sim.NewObjectStore()}
			kms, dek, wrapped := lineageKeys(t)

			var link lineageLink
			for i := range tc.links {
				link = publishAncestor(t, store, dek, ancestorSpec{
					parent: link,
					writes: map[int64]byte{int64(i) * 4 * testBlockSize: byte(0xA1 + i)},
				})
			}

			v := desiredVolume(t, 1)
			v.ParentSnapshotId, v.ParentVolumeId = link.snapshot, link.volume

			// First session: no image of its own, so the ancestry *is* the read view.
			first := lineageManager(t, store, kms, wrapped, dek.KeyID, "/var/lib/spin-depth-1")
			store.reset()
			dev := serveClone(t, first, v)
			firstAttach := store.count()
			for i := range tc.links {
				readBlock(t, dev, int64(i)*4*testBlockSize, byte(0xA1+i),
					"the range only one ancestor wrote; a walk that stopped short reads zeros here")
			}
			// Stopping is what gives it an image of its own, which is the whole difference
			// between the two sessions.
			writeBlock(t, dev, 32*testBlockSize, 0xC2)
			if err := first.Close(t.Context()); err != nil {
				t.Fatalf("closing the clone's first Agent: %v", err)
			}

			// Second session: same bucket, a fresh host. Its own image is a delta, so the
			// ancestry has to be composed again.
			second := lineageManager(t, store, kms, wrapped, dek.KeyID, "/var/lib/spin-depth-2")
			store.reset()
			again := serveClone(t, second, v)
			secondAttach := store.count()
			for i := range tc.links {
				readBlock(t, again, int64(i)*4*testBlockSize, byte(0xA1+i),
					"a restarted clone still reads what its ancestors wrote")
			}
			readBlock(t, again, 32*testBlockSize, 0xC2, "a restarted clone reads what it wrote itself")

			t.Logf("depth %d: attaching costs %d object reads with no image of its own, %d with one",
				tc.links, firstAttach, secondAttach)

			// Four reads per link, and the arithmetic is worth spelling out because it is
			// what a ceiling is arithmetic on: the ancestor's descriptor is one Get, its
			// snapshot manifest is a Head and a Get (image.readManifest checks existence
			// before it reads), and its chunks are one Get each — one here, because each
			// ancestor in this fixture wrote one block. **A real golden image is many
			// chunks and this is where the depth cost actually lives**: the per-link
			// constant is 3 + (chunks that ancestor's manifest names), and only the 3 is
			// bounded by anything the ceiling controls.
			const perLink = 4
			// Plus one, in the first session: the Head of its own manifest, which is not
			// there. That is how ErrNotPublished is reached and it costs a round trip.
			if want := tc.links*perLink + 1; firstAttach != want {
				t.Errorf("a first attach at depth %d cost %d object reads, want %d (%d per link, plus the Head that finds no image of its own)",
					tc.links, firstAttach, want, perLink)
			}
			// Plus three, on the restart: a Head and a Get for its own manifest, and a Get
			// for the one chunk that manifest names. That it pays for the ancestry *at all*
			// is the change — it used to read its own flattened manifest and stop, at any
			// depth.
			if want := tc.links*perLink + 3; secondAttach != want {
				t.Errorf("a restart at depth %d cost %d object reads, want %d — a clone reads through its ancestry in every session, not only its first",
					tc.links, secondAttach, want)
			}
		})
	}
}

// countingGets counts the reads an attach issues. Head is counted with Get because both
// are round trips to the store, and an attach's cost is round trips: the manifest read
// alone is one of each.
type countingGets struct {
	objectstore.Store
	gets int
}

func (c *countingGets) Get(ctx context.Context, key string) ([]byte, error) {
	c.gets++
	return c.Store.Get(ctx, key)
}

func (c *countingGets) Head(ctx context.Context, key string) (objectstore.ObjectInfo, error) {
	c.gets++
	return c.Store.Head(ctx, key)
}

func (c *countingGets) reset()     { c.gets = 0 }
func (c *countingGets) count() int { return c.gets }
