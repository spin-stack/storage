package controlplane_test

import (
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
)

const (
	snapReqID = "00000000-0000-7000-8000-0000000000e2"
	olderSnap = "00000000-0000-7000-8000-0000000000e3"
)

// A snapshot request is one catalog row and nothing else: no call to the host, no
// operation, no reservation. The Agent finds it in its desired state (§19, ADR-0021),
// so what this must get right is the row.
func TestRequestSnapshotRecordsWhatOnlyTheControlPlaneKnows(t *testing.T) {
	ctx := t.Context()
	md, _, term := cpStore(t)
	// The volume is itself a clone, so the request must carry its parent link on.
	if err := md.CreateSnapshot(ctx, term, metadata.Snapshot{
		SnapshotID: olderSnap, VolumeID: parentVol, Epoch: 1,
		State: lifecycle.SnapshotPublished, RequestID: reqID,
	}); err != nil {
		t.Fatal(err)
	}
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: parentVol, SizeBytes: 1 << 30, BlockSize: 65536, DEKKeyID: 3, State: lifecycle.VolumeActive,
		PrimaryHostID: cloneHostA, CurrentEpoch: 7, ParentSnapshotID: olderSnap,
	}, nil); err != nil {
		t.Fatal(err)
	}

	got, err := controlplane.RequestSnapshot(ctx, md, term, parentVol, snapID, snapReqID)
	if err != nil {
		t.Fatalf("RequestSnapshot: %v", err)
	}
	if got.State != lifecycle.SnapshotCreating {
		t.Fatalf("state = %q, want CREATING", got.State)
	}
	// The epoch the request was made under. A sequence means nothing without the epoch
	// that numbered it (§12.3), and the Agent reports only the sequence.
	if got.Epoch != 7 {
		t.Errorf("epoch = %d, want the volume's 7", got.Epoch)
	}
	// A snapshot of a clone descends from the clone's own parent, so a chain read from
	// the catalog is the chain the objects form.
	if got.ParentSnapshotID != olderSnap {
		t.Errorf("parent snapshot = %q, want the volume's", got.ParentSnapshotID)
	}
	// Empty on purpose: these are facts only the host that takes it can know, and a
	// number here would name a point no copy corresponds to.
	if got.CommitID != "" || got.SourceHostID != "" {
		t.Errorf("the Control Plane guessed at the host's facts: %+v", got)
	}

	stored, err := md.GetSnapshot(ctx, snapID)
	if err != nil {
		t.Fatalf("the row was not written: %v", err)
	}
	if stored.VolumeID != parentVol || stored.State != lifecycle.SnapshotCreating {
		t.Fatalf("stored = %+v", stored)
	}
}

// Only a volume with a live writer can be snapshotted. One being fenced or promoted has
// no host that can freeze it, and the sequence it would name belongs to an epoch the
// fleet is in the middle of leaving.
func TestRequestSnapshotRefusesAVolumeWithNoLiveWriter(t *testing.T) {
	tests := []struct {
		name    string
		state   lifecycle.VolumeState
		primary string
	}{
		{"fencing", lifecycle.VolumeFencingWait, cloneHostA},
		{"no primary", lifecycle.VolumeActive, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			md, _, term := cpStore(t)
			if err := md.CreateVolume(ctx, term, metadata.Volume{
				VolumeID: parentVol, SizeBytes: 1 << 30, BlockSize: 65536, DEKKeyID: 3, State: tc.state,
				PrimaryHostID: tc.primary, CurrentEpoch: 1,
			}, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := controlplane.RequestSnapshot(ctx, md, term, parentVol, snapID, snapReqID); err == nil {
				t.Fatal("a volume with no live writer was accepted for a snapshot")
			}
			if _, err := md.GetSnapshot(ctx, snapID); !errors.Is(err, metadata.ErrNotFound) {
				t.Fatalf("a refused request left a row behind: %v", err)
			}
		})
	}
}

// A volume that does not exist is the store's answer, not a nil row with a nice message.
func TestRequestSnapshotOfAnUnknownVolume(t *testing.T) {
	md, _, term := cpStore(t)
	if _, err := controlplane.RequestSnapshot(t.Context(), md, term, parentVol, snapID, snapReqID); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}
