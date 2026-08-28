package cpserver_test

import (
	"testing"

	"connectrpc.com/connect"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/metadata"
)

// TestTheDesiredStateCarriesTheWatermarksTheCatalogHolds.
//
// The Agent decides at attach whether finding no image, or replaying short, means "this
// volume is new" or "this volume's data is missing", and only the catalog can tell the two
// apart. ADR-0021 keeps the Agent from looking anything up and these two columns were not
// on the wire, so a guest read zeros while the fleet's own record sat in Postgres.
// Asserted against the report the Agent would act on, because the row was never wrong.
func TestTheDesiredStateCarriesTheWatermarksTheCatalogHolds(t *testing.T) {
	f := newFixture(t)
	f.createVolume(t, metadata.Volume{
		VolumeID: "vol-a", SizeBytes: 1 << 30, BlockSize: 4096,
		CurrentEpoch: 3, PrimaryHostID: hostA,
	})
	// Through the store's own writer, so the ordering rule it enforces
	// (published ≤ durable ≤ local) is the one these numbers came out of.
	if err := f.md.UpdateWatermarks(t.Context(), f.term, "vol-a", 91, 74, 12); err != nil {
		t.Fatal(err)
	}

	resp, err := f.srv.GetDesiredState(t.Context(), connect.NewRequest(&storagev1.GetDesiredStateRequest{HostId: hostA}))
	if err != nil {
		t.Fatal(err)
	}
	vols := resp.Msg.GetVolumes()
	if len(vols) != 1 {
		t.Fatalf("got %d volumes, want 1", len(vols))
	}
	if got := vols[0].GetPublishedSequence(); got != 12 {
		t.Errorf("published_sequence = %d, want 12: an Agent that finds no image cannot tell a new volume from one whose image is gone", got)
	}
	if got := vols[0].GetDurableSequence(); got != 74 {
		t.Errorf("durable_sequence = %d, want 74: an Agent that replays short has nothing to compare against, and rolls the volume back in silence", got)
	}
}

// TestAVolumeNobodyHasReportedOnCarriesZeroWatermarks pins the other end, because the
// Agent treats zero as "the catalog has nothing to say, boot normally". A default that
// arrived as anything else would refuse every genuinely new volume — the whole fleet, on
// the day this landed.
func TestAVolumeNobodyHasReportedOnCarriesZeroWatermarks(t *testing.T) {
	f := newFixture(t)
	f.createVolume(t, metadata.Volume{
		VolumeID: "vol-new", SizeBytes: 1 << 30, BlockSize: 4096,
		CurrentEpoch: 1, PrimaryHostID: hostA,
	})

	resp, err := f.srv.GetDesiredState(t.Context(), connect.NewRequest(&storagev1.GetDesiredStateRequest{HostId: hostA}))
	if err != nil {
		t.Fatal(err)
	}
	v := resp.Msg.GetVolumes()[0]
	if v.GetPublishedSequence() != 0 || v.GetDurableSequence() != 0 {
		t.Fatalf("a volume nothing has reported on came back published=%d durable=%d, want 0/0",
			v.GetPublishedSequence(), v.GetDurableSequence())
	}
}
