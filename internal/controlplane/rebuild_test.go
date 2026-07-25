package controlplane_test

import (
	"context"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/epoch"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// TestRebuildMetadataFromS3 is INV-20: with PostgreSQL wiped, rebuild-metadata
// reconstructs the volumes from the self-describing S3 layout.
func TestRebuildMetadataFromS3(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	epochs := epoch.NewStore(store)

	// Two volumes exist in S3 (descriptor + epoch object), nothing in PG.
	descs := []descriptor.Descriptor{
		{VolumeID: volID, SizeBytes: 1 << 30, BlockSize: 65536, Durability: "remote", KEKID: "k1", DEKWrapped: []byte{1, 2}},
		{VolumeID: host1 /*any v7 id*/, SizeBytes: 2 << 30, BlockSize: 65536, Durability: "local", KEKID: "k2", DEKWrapped: []byte{3}},
	}
	epochsByVol := map[string]uint64{volID: 5, host1: 0}
	for _, d := range descs {
		if err := descriptor.Write(ctx, store, d); err != nil {
			t.Fatal(err)
		}
		if _, err := epochs.Init(ctx, d.VolumeID, epochsByVol[d.VolumeID]); err != nil {
			t.Fatal(err)
		}
	}

	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	md := metasim.New(clk.Wall)
	term, _ := md.AcquireLeadership(ctx, "cp")

	n, err := controlplane.RebuildMetadata(ctx, store, epochs, md, term)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("rebuilt %d volumes, want 2", n)
	}

	// The rebuilt volume takes its epoch from the authoritative epoch object.
	v, err := md.GetVolume(ctx, volID)
	if err != nil {
		t.Fatal(err)
	}
	if v.CurrentEpoch != 5 || v.SizeBytes != 1<<30 || v.KEKID != "k1" || v.State != "REBUILT" {
		t.Fatalf("rebuilt volume wrong: %+v", v)
	}

	// Rebuild is idempotent: a second pass adds nothing.
	if n2, _ := controlplane.RebuildMetadata(ctx, store, epochs, md, term); n2 != 0 {
		t.Fatalf("second rebuild should add 0, added %d", n2)
	}
}

func TestDescriptorRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := sim.NewObjectStore()
	d := descriptor.Descriptor{VolumeID: volID, SizeBytes: 42, BlockSize: 65536, Durability: "remote", CurrentEpoch: 3, KEKID: "k", DEKWrapped: []byte{9}}
	if err := descriptor.Write(ctx, store, d); err != nil {
		t.Fatal(err)
	}
	got, err := descriptor.Read(ctx, store, volID)
	if err != nil {
		t.Fatal(err)
	}
	if got.VolumeID != d.VolumeID || got.SizeBytes != d.SizeBytes || got.CurrentEpoch != d.CurrentEpoch ||
		got.KEKID != d.KEKID || string(got.DEKWrapped) != string(d.DEKWrapped) {
		t.Fatalf("descriptor round-trip: got %+v want %+v", got, d)
	}
	ids, _ := descriptor.ListVolumeIDs(ctx, store)
	if len(ids) != 1 || ids[0] != volID {
		t.Fatalf("list volume ids = %v", ids)
	}
	// Reading a missing descriptor errors.
	if _, err := descriptor.Read(ctx, store, "no-such-volume"); err == nil {
		t.Fatal("reading a missing descriptor should error")
	}
	// Listing an empty store yields nothing.
	if empty, _ := descriptor.ListVolumeIDs(ctx, sim.NewObjectStore()); len(empty) != 0 {
		t.Fatalf("empty store should list no volumes, got %v", empty)
	}
}
