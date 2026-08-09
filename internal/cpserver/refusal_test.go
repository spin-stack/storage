package cpserver_test

import (
	"testing"

	"connectrpc.com/connect"
	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
)

// TestEveryWireRefusalHasAStoredOne is the other half of the closed-vocabulary bargain,
// and the half a schema test cannot see: the enum on the wire and the vocabulary in the
// catalog have to be the same set, in both directions.
//
// It is asserted through the handler rather than by reading the mapping function, because
// the mapping function is exactly the thing that would be wrong. A value the handler
// cannot translate fails the *whole* report RPC — the Agent's watermarks and its snapshot
// outcome go with it — so a refusal added on the wire and forgotten here does not
// degrade, it takes the reporting path down for that host. And a stored value with no
// wire spelling is a refusal no Agent can ever express, which is the vocabulary quietly
// shrinking back towards the silence this field was added to end.
func TestEveryWireRefusalHasAStoredOne(t *testing.T) {
	f := newFixture(t)
	f.createVolume(t, metadata.Volume{
		VolumeID: "vol-a", SizeBytes: 1 << 30, BlockSize: 4096, CurrentEpoch: 1, PrimaryHostID: hostA,
	})

	// Every value the wire declares, taken from the generated descriptor rather than
	// from a list written here: a list would be the third copy, and it would be the one
	// nobody updates.
	values := storagev1.VolumeRefusal(0).Descriptor().Values()
	seen := map[lifecycle.Refusal]bool{}
	for i := range values.Len() {
		wire := storagev1.VolumeRefusal(values.Get(i).Number())
		t.Run(wire.String(), func(t *testing.T) {
			resp, err := f.srv.ReportVolumeState(t.Context(), connect.NewRequest(&storagev1.ReportVolumeStateRequest{
				HostId: hostA,
				Volumes: []*storagev1.VolumeReport{{
					VolumeId: "vol-a", Epoch: 1, Refusal: wire, RefusalDetail: "because",
				}},
			}))
			if err != nil {
				t.Fatalf("the whole report RPC failed because of one refusal value: %v", err)
			}
			if got := resp.Msg.GetResults()[0].GetOutcome(); got != storagev1.ReportOutcome_REPORT_OUTCOME_ACCEPTED {
				t.Fatalf("outcome = %v, want ACCEPTED", got)
			}
			v, err := f.md.GetVolume(t.Context(), "vol-a")
			if err != nil {
				t.Fatal(err)
			}
			if wire == storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED && v.Refusal != lifecycle.RefusalNone {
				t.Fatalf("an unset refusal stored %q; unset is the Agent saying it is serving", v.Refusal)
			}
			if wire != storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED && !v.Refusal.Refused() {
				t.Fatalf("%s stored %q, which is not a refusal at all", wire, v.Refusal)
			}
			seen[v.Refusal] = true
		})
	}

	// And nothing in the catalog's vocabulary is unreachable from the wire.
	for _, r := range lifecycle.Refusals() {
		if !seen[r] {
			t.Errorf("the catalog can store %q and no VolumeRefusal maps onto it: no Agent can ever say it", r)
		}
	}
}

// TestARefusalIsNotResurrectedByAFencedWriter is the storage rule at the seam it matters
// at, and it is deliberately not a store test: the guard has to survive the *handler's*
// read-then-write, where the volume can move between the epoch check and the update.
//
// A watermark from a fenced writer is merged with GREATEST and is harmless. A refusal is
// a state, so the same late report would mark a volume NOT SERVED while its successor is
// serving it — with the term guard passing, because a promotion does not move the CP
// term. It is the one thing that makes "last report wins" safe.
func TestARefusalIsNotResurrectedByAFencedWriter(t *testing.T) {
	f := newFixture(t)
	f.createVolume(t, metadata.Volume{
		VolumeID: "vol-a", SizeBytes: 1 << 30, BlockSize: 4096, CurrentEpoch: 4, PrimaryHostID: hostA,
	})

	report := func(host string, epoch int64, r storagev1.VolumeRefusal) storagev1.ReportOutcome {
		t.Helper()
		resp, err := f.srv.ReportVolumeState(t.Context(), connect.NewRequest(&storagev1.ReportVolumeStateRequest{
			HostId: host,
			Volumes: []*storagev1.VolumeReport{{
				VolumeId: "vol-a", Epoch: epoch, Refusal: r, RefusalDetail: "the bucket has no manifest",
			}},
		}))
		if err != nil {
			t.Fatal(err)
		}
		return resp.Msg.GetResults()[0].GetOutcome()
	}

	if got := report(hostA, 4, storagev1.VolumeRefusal_VOLUME_REFUSAL_IMAGE_MISSING); got != storagev1.ReportOutcome_REPORT_OUTCOME_ACCEPTED {
		t.Fatalf("the volume's own host at its own epoch was refused: %v", got)
	}
	v, _ := f.md.GetVolume(t.Context(), "vol-a")
	if v.Refusal != lifecycle.RefusalImageMissing || v.RefusalDetail == "" {
		t.Fatalf("the refusal did not land: %+v", v)
	}

	// The volume is promoted away. Everything about the old writer's next report is
	// still well-formed; it is simply no longer entitled to an opinion.
	if _, err := f.md.BumpVolumeEpoch(t.Context(), f.term, "vol-a", hostB, 4); err != nil {
		t.Fatal(err)
	}
	if got := report(hostA, 4, storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST); got != storagev1.ReportOutcome_REPORT_OUTCOME_NOT_PRIMARY {
		t.Fatalf("the fenced writer's report outcome = %v", got)
	}

	// The successor says it is serving, and that clears it — the clearing mechanism is
	// the ordinary report and nothing else.
	if got := report(hostB, 5, storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED); got != storagev1.ReportOutcome_REPORT_OUTCOME_ACCEPTED {
		t.Fatalf("the new primary's report outcome = %v", got)
	}
	v, _ = f.md.GetVolume(t.Context(), "vol-a")
	if v.Refusal != lifecycle.RefusalNone || v.RefusalDetail != "" {
		t.Fatalf("a volume its new host is serving still reads as refused: %+v", v)
	}

	// And the fenced writer cannot put it back. This is the assertion the whole
	// host-and-epoch predicate exists for: it is a report that arrives *after* the
	// promotion, from a process that has not learned about it yet, which is the ordinary
	// shape of a slow retry rather than an exotic race.
	if got := report(hostA, 4, storagev1.VolumeRefusal_VOLUME_REFUSAL_IMAGE_MISSING); got != storagev1.ReportOutcome_REPORT_OUTCOME_NOT_PRIMARY {
		t.Fatalf("the fenced writer's late report outcome = %v", got)
	}
	v, _ = f.md.GetVolume(t.Context(), "vol-a")
	if v.Refusal != lifecycle.RefusalNone {
		t.Fatalf("a writer the fleet moved past marked a volume its successor is serving: %+v", v)
	}
}

// Two guards stand between a stale Agent and a volume it no longer holds, and this pins
// the first one. The store's own qualification — the UPDATE is predicated on the host
// and the epoch — is the second, and it lives in the shared contract because both
// implementations must have it.
//
// Written after a version of this test that asserted the second guard through this
// handler and PASSED WITH THAT GUARD REMOVED: the report never reached the store,
// because the epoch check here had already rejected it. A test that cannot fail for the
// reason it names is the thing this repository keeps shipping, so what it pins now is
// the guard it actually exercises.
func TestAReportFromAHostTheFleetMovedPastIsRefusedBeforeItCanSayAnything(t *testing.T) {
	f := newFixture(t)
	f.createVolume(t, metadata.Volume{
		VolumeID: "vol-moved", SizeBytes: 1 << 30, BlockSize: 4096, CurrentEpoch: 2, PrimaryHostID: hostB,
	})

	report := func(t *testing.T, host string, epoch int64, refusal storagev1.VolumeRefusal) *storagev1.ReportVolumeStateResponse {
		t.Helper()
		resp, err := f.srv.ReportVolumeState(t.Context(), connect.NewRequest(&storagev1.ReportVolumeStateRequest{
			HostId: host,
			Volumes: []*storagev1.VolumeReport{{
				VolumeId: "vol-moved", Epoch: epoch, Refusal: refusal, RefusalDetail: "from " + host,
			}},
		}))
		if err != nil {
			t.Fatalf("ReportVolumeState(%s): %v", host, err)
		}
		return resp.Msg
	}

	// The host that holds it says it cannot serve it. That is the truth and it lands.
	report(t, hostB, 2, storagev1.VolumeRefusal_VOLUME_REFUSAL_IMAGE_MISSING)
	got, err := f.md.GetVolume(t.Context(), "vol-moved")
	if err != nil {
		t.Fatal(err)
	}
	if got.Refusal != lifecycle.RefusalImageMissing {
		t.Fatalf("the host holding the volume reported IMAGE_MISSING and the catalog says %q", got.Refusal)
	}

	// The previous host is still running and still believes it owns the volume at the
	// epoch it was granted. Its report is refused as a whole — the RPC succeeds and the
	// per-volume outcome says so, because one stale volume must not fail a host's entire
	// heartbeat — and the refusal of the host that actually holds it is untouched.
	msg := report(t, hostA, 1, storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED)
	if len(msg.GetResults()) != 1 || msg.GetResults()[0].GetOutcome() == storagev1.ReportOutcome_REPORT_OUTCOME_ACCEPTED {
		t.Errorf("a report from a host the fleet moved past was accepted: %v", msg.GetResults())
	}
	got, err = f.md.GetVolume(t.Context(), "vol-moved")
	if err != nil {
		t.Fatal(err)
	}
	if got.Refusal != lifecycle.RefusalImageMissing {
		t.Errorf("a stale host's report cleared the refusal: %q — the operator is now told a volume nobody is serving is fine", got.Refusal)
	}

	// And the holder clearing it works, or the column would never come back.
	report(t, hostB, 2, storagev1.VolumeRefusal_VOLUME_REFUSAL_UNSPECIFIED)
	got, err = f.md.GetVolume(t.Context(), "vol-moved")
	if err != nil {
		t.Fatal(err)
	}
	if got.Refusal != lifecycle.RefusalNone || got.RefusalDetail != "" {
		t.Errorf("the holder reported it is serving again and the catalog still says %q/%q; a reason that outlives its cause is the one way this column misleads",
			got.Refusal, got.RefusalDetail)
	}
}
