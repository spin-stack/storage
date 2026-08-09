package agent_test

import (
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// publishedSequenceIn returns what the bucket says this volume's image is at. It is the
// number every assertion in this file is against, because it is the only one that is not
// the Agent agreeing with itself: the manifest is the object a later attach reads, and
// `published_sequence` in the catalog is a claim about exactly it.
func publishedSequenceIn(t *testing.T, store *sim.ObjectStore, volumeID string) int64 {
	t.Helper()
	u, err := ids.Parse(volumeID)
	if err != nil {
		t.Fatalf("volume id %q: %v", volumeID, err)
	}
	_, man, _, err := image.Load(t.Context(), store, nil, image.OwnLineage([16]byte(u)), nil)
	if err != nil {
		t.Fatalf("reading the image this volume published: %v", err)
	}
	if man.Sequence == 0 {
		t.Fatal("the published image is at sequence 0: there is no number here for a report to be wrong about")
	}
	return int64(man.Sequence)
}

// TestAVolumeGivenUpAfterALeaseLapseReportsWhatItPublished is the one teardown whose
// number the fleet can still hear and never did.
//
// A lease that lapses tears the runtime down and publishes the image (Fence -> remove),
// and the Control Plane still names this host the volume's primary at this epoch — so
// this host's next report is *accepted*. What it carried was the published sequence the
// log was resumed with, which for a volume that never had an image is zero, and
// ErrImageMissing — the floor that stops a volume coming up as a blank device — arms only
// above zero. Publish an image, lose it, re-place the volume, and it boots empty with no
// error anywhere.
//
// The assertion is against the manifest in the bucket, not against a sequence counted
// here: the report is a claim about that object.
func TestAVolumeGivenUpAfterALeaseLapseReportsWhatItPublished(t *testing.T) {
	t.Parallel()
	store := sim.NewObjectStore()
	r := newPublishRig(t, store)
	v := desiredVolume(t, 1)
	if err := r.m.Apply(t.Context(), []*storagev1.DesiredVolume{v}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	writeOneBlock(t, r.m, v.GetVolumeId())

	if err := r.m.Fence(t.Context(), []string{v.GetVolumeId()},
		storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST, "the lease lapsed"); err != nil {
		t.Fatalf("Fence: %v", err)
	}

	want := publishedSequenceIn(t, store, v.GetVolumeId())
	got := reportFor(t, r.m, v.GetVolumeId())
	if got.PublishedSequence != want {
		t.Fatalf("this host published its image at sequence %d and reports published_sequence=%d; "+
			"the catalog learns the number only when some future attach reads the manifest, and until then "+
			"a volume whose image goes missing comes up as a blank device",
			want, got.PublishedSequence)
	}
	if got.Refusal != storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST {
		t.Fatalf("the same report says refusal=%s; the sequence must ride on the refusal, not replace it", got.Refusal)
	}
	mustBeAcceptable(t, got)
}

// mustBeAcceptable applies the rule the Control Plane applies to the whole report before
// it applies anything in it (cpserver.applyReport -> metadata.UpdateWatermarks).
//
// It is the real function and not a restatement of it, because the failure it catches is
// invisible from this side: a trio out of order is answered with an *outcome*, not an
// error, so the Agent's cycle succeeds, its log says nothing, and the refusal travelling
// on the same report is dropped along with the watermarks. That is how a published
// sequence bolted onto a report of zeros silently un-reported a volume this host had
// given up — caught by running the two binaries, and pinned here so it cannot come back.
func mustBeAcceptable(t *testing.T, st agent.VolumeStatus) {
	t.Helper()
	if err := metadata.CheckWatermarkOrder(st.LocalSequence, st.DurableSequence, st.PublishedSequence); err != nil {
		t.Fatalf("the Control Plane refuses this whole report (local=%d durable=%d published=%d): %v — "+
			"and with it the refusal it was carrying, as an outcome rather than an error, so nothing anywhere says so",
			st.LocalSequence, st.DurableSequence, st.PublishedSequence, err)
	}
}

// TestAHostStillServingReportsTheImageItPublishedForAnotherVolume is the same number seen
// from the side that keeps running. One volume is given up and published; the other is
// untouched and still has a guest. The report that carries the survivor also has to carry
// the published point of the one that left, because this process is the only thing that
// knows it — the alternative is waiting for a future InstallBase somewhere else.
func TestAHostStillServingReportsTheImageItPublishedForAnotherVolume(t *testing.T) {
	t.Parallel()
	store := sim.NewObjectStore()
	r := newPublishRig(t, store)
	gone, stays := desiredVolume(t, 1), desiredVolume(t, 1)
	if err := r.m.Apply(t.Context(), []*storagev1.DesiredVolume{gone, stays}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	writeOneBlock(t, r.m, gone.GetVolumeId())
	writeOneBlock(t, r.m, stays.GetVolumeId())

	if err := r.m.Fence(t.Context(), []string{gone.GetVolumeId()},
		storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST, "the lease lapsed"); err != nil {
		t.Fatalf("Fence: %v", err)
	}

	want := publishedSequenceIn(t, store, gone.GetVolumeId())
	if got := reportFor(t, r.m, gone.GetVolumeId()); got.PublishedSequence != want {
		t.Fatalf("the host is still serving %s and reports published_sequence=%d for %s, whose image is in the bucket at %d",
			stays.GetVolumeId(), got.PublishedSequence, gone.GetVolumeId(), want)
	}
	// The survivor is untouched: nothing published for it, and a number invented here
	// would be a floor no object backs.
	if got := reportFor(t, r.m, stays.GetVolumeId()); got.PublishedSequence != 0 {
		t.Fatalf("volume %s has published nothing and is reported at published_sequence=%d",
			stays.GetVolumeId(), got.PublishedSequence)
	}
	mustBeAcceptable(t, reportFor(t, r.m, gone.GetVolumeId()))
	mustBeAcceptable(t, reportFor(t, r.m, stays.GetVolumeId()))
}
