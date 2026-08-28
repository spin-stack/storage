package controlplane_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/framed"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/placement"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// rewriteDescriptor is the adversary: one PUT of a well-formed object under a key the
// bucket already holds. It reads the real descriptor, changes one field, and writes it
// back through descriptor.Write, so the result is framed with a matching digest, carries
// the current format_version, names its own volume, and — because crypto.wrapAAD binds
// only the volume id and the DEK version — carries a wrapped DEK that still unwraps
// perfectly under this fleet's KEK. Every check RebuildMetadata performs passes.
func rewriteDescriptor(t *testing.T, store objectstore.Store, volumeID string, mut func(*descriptor.Descriptor)) {
	t.Helper()
	d, err := descriptor.Read(t.Context(), store, volumeID)
	if err != nil {
		t.Fatal(err)
	}
	mut(&d)
	if err := descriptor.Write(t.Context(), store, d); err != nil {
		t.Fatal(err)
	}
}

// The key columns are not the only ones a rebuild takes wholesale from the bucket.
//
// checkKey closed the swap of `dek_wrapped`, and both halves of the upsert say the
// converging path never regresses: the epoch, the size, the watermarks, the ownership
// columns and `state` all keep the higher or existing value. `block_size` does not. Both
// implementations assign it unconditionally — volumes.sql `block_size = EXCLUDED.block_size`,
// sim.converge leaves it in `next` — so one PUT of an otherwise honest descriptor changes
// the geometry of a volume a guest is writing to right now, and `-rebuild-metadata`, which
// is documented as safe to re-run against a live catalog, is the verb that applies it.
//
// `size_bytes` is GREATEST, which stops it shrinking and lets it grow without limit: the
// same PUT hands the guest a device larger than the volume that was provisioned.
//
// Asserted on the catalog row, not on the error: a rebuild that returns nil and writes
// the wrong geometry is exactly the shape being hunted here.
func TestAdversaryDescriptorRewritesALiveVolumesGeometry(t *testing.T) {
	ctx := t.Context()
	f := newFleet(t)
	victim := f.provision(t)
	before, err := f.md.GetVolume(ctx, victim)
	if err != nil {
		t.Fatal(err)
	}

	// Control: the rebuild alone changes nothing, so the failures below are the PUT's
	// doing and not the rebuild's. Without this the test could be red for a reason that
	// has nothing to do with an adversary.
	if _, err := controlplane.RebuildMetadata(ctx, f.md, f.store, f.kms, f.term); err != nil {
		t.Fatalf("an honest rebuild of an untouched bucket: %v", err)
	}
	if honest, err := f.md.GetVolume(ctx, victim); err != nil || honest.BlockSize != before.BlockSize || honest.SizeBytes != before.SizeBytes {
		t.Fatalf("an honest rebuild moved the geometry on its own: %+v (%v)", honest, err)
	}

	rewriteDescriptor(t, f.store, victim, func(d *descriptor.Descriptor) {
		d.BlockSize = 512
		d.SizeBytes = before.SizeBytes * 4
	})

	if _, err := controlplane.RebuildMetadata(ctx, f.md, f.store, f.kms, f.term); err != nil {
		t.Logf("the rebuild refused it: %v", err)
	}

	got, err := f.md.GetVolume(ctx, victim)
	if err != nil {
		t.Fatal(err)
	}
	if got.BlockSize != before.BlockSize {
		t.Errorf("a bucket write changed a live volume's block size: %d -> %d", before.BlockSize, got.BlockSize)
	}
	if got.SizeBytes != before.SizeBytes {
		t.Errorf("a bucket write changed a live volume's size: %d -> %d", before.SizeBytes, got.SizeBytes)
	}
}

// The lineage ceiling counts with a number the bucket supplies.
//
// MaxChainDepth is enforced by reading `parent.ChainDepth` out of the catalog, and
// `chain_depth` is the third column both upserts take wholesale from the descriptor
// (volumes.sql: `chain_depth = EXCLUDED.chain_depth`). So a volume parked at the ceiling —
// one whose clones are refused with ErrChainTooDeep and told to FLATTEN — is returned to
// depth 0 by one PUT plus a rebuild, and the ceiling §20.1 states as a bound on read
// amplification is gone for that lineage.
//
// The assertion is the refusal an operator sees, not the column: a clone that succeeds
// where the ceiling says it must not is the whole observable consequence.
func TestAdversaryChainDepthResetReopensALineageAtTheCeiling(t *testing.T) {
	ctx := t.Context()
	md, store, term := cpStore(t)
	kms := testKMS(t)
	addHost(t, md, term, cloneHostA, lifecycle.HostActive, 1<<40)

	root := ids.New().String()
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: root, SizeBytes: 1 << 30, BlockSize: 65536, DEKKeyID: 1,
		State: lifecycle.VolumeActive, DEKWrapped: wrapFor(t, kms, root, 1), KEKID: "kek-test",
	}, nil); err != nil {
		t.Fatal(err)
	}
	// The root has no descriptor of its own here (it was created straight into the
	// catalog), so the lineage the rebuild sees is the clones — which is all this needs.
	snap := publishSnapshotOf(t, md, term, root)
	deepest := root
	for depth := 1; depth <= controlplane.MaxChainDepth; depth++ {
		c, err := controlplane.Clone(ctx, md, store, kms, &ramp{}, placement.Policy{}, nil, term, snap, ids.New().String())
		if err != nil {
			t.Fatalf("a clone at depth %d was refused below the ceiling: %v", depth, err)
		}
		snap, deepest = publishSnapshotOf(t, md, term, c.VolumeID), c.VolumeID
	}
	if _, err := controlplane.Clone(ctx, md, store, kms, &ramp{}, placement.Policy{}, nil, term, snap, ids.New().String()); !errors.Is(err, controlplane.ErrChainTooDeep) {
		t.Fatalf("the ceiling did not hold before the attack: %v", err)
	}

	// Control: a rebuild of the untouched bucket leaves the ceiling where it was.
	if _, err := controlplane.RebuildMetadata(ctx, md, store, kms, term); err != nil {
		t.Fatalf("an honest rebuild of an untouched bucket: %v", err)
	}
	if _, err := controlplane.Clone(ctx, md, store, kms, &ramp{}, placement.Policy{}, nil, term, snap, ids.New().String()); !errors.Is(err, controlplane.ErrChainTooDeep) {
		t.Fatalf("an honest rebuild reopened the lineage on its own: %v", err)
	}

	rewriteDescriptor(t, store, deepest, func(d *descriptor.Descriptor) { d.ChainDepth = 0 })
	if _, err := controlplane.RebuildMetadata(ctx, md, store, kms, term); err != nil {
		t.Logf("the rebuild refused it: %v", err)
	}

	if _, err := controlplane.Clone(ctx, md, store, kms, &ramp{}, placement.Policy{}, nil, term, snap, ids.New().String()); !errors.Is(err, controlplane.ErrChainTooDeep) {
		t.Errorf("a bucket write reopened a lineage at the ceiling: the clone was admitted, %v", err)
	}
}

// A rebuild is the second producer of catalog rows, and it applies none of the first
// one's rules.
//
// Provision refuses a block size that is not a positive multiple of 512 — a device
// nothing can address — and refuses a non-positive size. RebuildMetadata builds the same
// metadata.Volume straight out of an object it read and hands it to CreateVolume, which
// checks the id, the state, the watermark order and the DEK version and nothing about
// geometry. So a descriptor that is corrupt in a way the digest cannot see (a zeroed
// field is still a field) becomes a catalog row describing a volume that cannot be
// served, recorded as ACTIVE, with the rebuild reporting success.
func TestAdversaryRebuildRecordsGeometryProvisionWouldRefuse(t *testing.T) {
	tests := []struct {
		name string
		mut  func(*descriptor.Descriptor)
	}{
		{"a block size of zero", func(d *descriptor.Descriptor) { d.BlockSize = 0 }},
		{"a block size no device can address", func(d *descriptor.Descriptor) { d.BlockSize = 7 }},
		{"a volume of no size", func(d *descriptor.Descriptor) { d.SizeBytes = 0 }},
		{"a volume of negative size", func(d *descriptor.Descriptor) { d.SizeBytes = -4096 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			f := newFleet(t)
			vol := f.provision(t)
			rewriteDescriptor(t, f.store, vol, tt.mut)

			// A catalog that has never seen this fleet: nothing here is answered by a row
			// that was already right.
			fresh := newFleet(t)
			if _, err := controlplane.RebuildMetadata(ctx, fresh.md, f.store, fresh.kms, fresh.term); err != nil {
				return // refused, which is the wanted behaviour
			}
			got, err := fresh.md.GetVolume(ctx, vol)
			if err != nil {
				t.Fatal(err)
			}
			t.Errorf("the rebuild reported success and recorded %d bytes at a block size of %d",
				got.SizeBytes, got.BlockSize)
		})
	}
}

// The forgery detector can be silenced into blaming the operator, by the forger.
//
// checkKey compares `kek_id` first and says "this is a wrong -kek-file, not a forged
// descriptor". That field is cleartext, unauthenticated and chosen by whoever wrote the
// object, so one PUT of a foreign object under a key that parses as a descriptor key aborts
// the whole rebuild — no volume is recovered, with no override and nothing naming the
// healthy volumes it skipped — and sends the operator after a KEK file that does not exist,
// at the one moment the catalog is already gone. The object here is not even a plausible
// volume: its id is not a uuid, so the check two lines further down would have said
// something true.
func TestAdversaryOneForeignObjectDeniesTheWholeRebuild(t *testing.T) {
	ctx := t.Context()
	f := newFleet(t)
	healthy := f.provision(t)

	body, err := json.Marshal(descriptor.Descriptor{
		FormatVersion: framed.FormatVersion, VolumeID: "notes", KEKID: "kek-somebody-elses",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Put(ctx, "volumes/notes/descriptor.json", framed.Frame(body), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	fresh := newFleet(t)
	sum, rerr := controlplane.RebuildMetadata(ctx, fresh.md, f.store, fresh.kms, fresh.term)
	if _, err := fresh.md.GetVolume(ctx, healthy); err != nil {
		t.Errorf("one foreign object denied recovery of every healthy volume: %d recorded, %v (%v)", sum.Volumes, err, rerr)
	}
	if rerr != nil && strings.Contains(rerr.Error(), "-kek-file") {
		t.Errorf("the object chose its own diagnosis: an operator loses the catalog and is sent after a KEK file: %v", rerr)
	}
}
