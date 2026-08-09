package controlplane_test

import (
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/placement"
	"github.com/spin-stack/storage/internal/simio/sim"
)

const (
	placeVol  = "00000000-0000-7000-8000-000000000071"
	fullHost  = "00000000-0000-7000-8000-000000000072"
	quietHost = "00000000-0000-7000-8000-000000000073"
	// The admitted host sorts *last*, deliberately: with the ids the other way round a
	// Place that ignored the policy and took the first host in the listing would still
	// pass every assertion below.
	roomyHost  = "00000000-0000-7000-8000-000000000074"
	absentHost = "00000000-0000-7000-8000-00000000007f"
)

// placeWorld is the fleet an operator finds after losing the database: one volume the
// rebuild restored with no primary, and three hosts of which exactly one may take it —
// the other two are the two ways a host is excluded (§28.1's fleet state and ADR-0013's
// fill ceiling), so a Place that ignored the policy would land on the wrong one rather
// than merely on a different one.
func placeWorld(t *testing.T) (metadata.Store, int64) {
	t.Helper()
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	md := metasim.New(clk.Wall)
	term, err := md.AcquireLeadership(ctx, "cp")
	if err != nil {
		t.Fatal(err)
	}
	hosts := []metadata.Host{
		{HostID: roomyHost, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40},
		// Inside its promises and out of device: ADR-0013's arm, not §28.2's.
		{HostID: fullHost, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40, NVMeUsedBytes: 900 << 30},
		{HostID: quietHost, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40},
	}
	for _, h := range hosts {
		if err := md.UpsertHost(ctx, term, h); err != nil {
			t.Fatal(err)
		}
	}
	// Empty and idle, and still not a candidate: cordoning is a state, not a capacity.
	if err := md.SetHostState(ctx, term, quietHost, lifecycle.HostCordoned, lifecycle.CordonOperator); err != nil {
		t.Fatal(err)
	}
	// Epoch 1, which is what Provision writes: epoch 0 is the absence of an epoch, and
	// a fixture at 0 would let an off-by-one in the bump below look like a fresh grant.
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: placeVol, SizeBytes: 1 << 30, BlockSize: 65536, CurrentEpoch: 1,
		State: lifecycle.VolumeDetached, DEKWrapped: []byte{7}, KEKID: "kek", DEKKeyID: 42,
	}, nil); err != nil {
		t.Fatal(err)
	}
	return md, term
}

// serving reports whether the volume is in what GetDesiredState would hand this host —
// the same observable D2's placement case asserts on. A store that wrote the column and
// answered the listing from somewhere else would still fail here.
func serving(t *testing.T, md metadata.Store, hostID string) bool {
	t.Helper()
	vols, err := md.ListVolumesByHost(t.Context(), hostID)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range vols {
		if v.VolumeID == placeVol {
			return true
		}
	}
	return false
}

func placedNowhere(t *testing.T, md metadata.Store) bool {
	t.Helper()
	return !serving(t, md, roomyHost) && !serving(t, md, fullHost) && !serving(t, md, quietHost)
}

// TestPlaceChoosesTheHostThePolicyAdmits: with no host named, the §20 order decides,
// and the two hosts it must skip are skipped for two different reasons.
func TestPlaceChoosesTheHostThePolicyAdmits(t *testing.T) {
	md, term := placeWorld(t)
	got, err := controlplane.Place(t.Context(), md, placement.Policy{}, term, placeVol, "")
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if got.Host.HostID != roomyHost {
		t.Fatalf("placed on %s, want the one host that admits it (%s)", got.Host.HostID, roomyHost)
	}
	if !serving(t, md, roomyHost) {
		t.Fatal("the volume is not in the desired state of the host it was placed on")
	}
	if serving(t, md, fullHost) || serving(t, md, quietHost) {
		t.Fatal("the volume reached a host the policy excludes")
	}
}

// TestPlaceHonoursANamedHostThePolicyWouldNotChoose: naming a host is an override, and
// it has to work on the host an operator is most likely to name — the cordoned one
// whose device already holds the volume's bytes. What it must not do is hide it, so the
// returned host carries the state the caller reports.
func TestPlaceHonoursANamedHostThePolicyWouldNotChoose(t *testing.T) {
	md, term := placeWorld(t)
	got, err := controlplane.Place(t.Context(), md, placement.Policy{}, term, placeVol, quietHost)
	if err != nil {
		t.Fatalf("Place on a named cordoned host: %v", err)
	}
	if got.Host.HostID != quietHost || got.Host.State != lifecycle.HostCordoned {
		t.Fatalf("Place returned %s/%s, want the named host and the state that says it is out of service",
			got.Host.HostID, got.Host.State)
	}
	if !serving(t, md, quietHost) {
		t.Fatal("the named host was not given the volume")
	}
}

// TestPlaceRefusesWhenNoHostAdmitsTheVolume: a fleet with nowhere to put it must say so
// and write nothing. Placing it anyway would name an owner that will never serve it,
// which reads exactly like a healthy placement in every listing.
func TestPlaceRefusesWhenNoHostAdmitsTheVolume(t *testing.T) {
	md, term := placeWorld(t)
	if err := md.SetHostState(t.Context(), term, roomyHost, lifecycle.HostDraining, lifecycle.CordonOperator); err != nil {
		t.Fatal(err)
	}
	_, err := controlplane.Place(t.Context(), md, placement.Policy{}, term, placeVol, "")
	if !errors.Is(err, placement.ErrNoCapacity) {
		t.Fatalf("Place onto a fleet with no candidate: want ErrNoCapacity, got %v", err)
	}
	if !placedNowhere(t, md) {
		t.Fatal("a refused placement still put the volume on a host")
	}
}

// TestPlaceRefusesAHandOverAndAMissingHost: the two refusals that come from the store
// and the catalog rather than from the policy.
func TestPlaceRefusesAHandOverAndAMissingHost(t *testing.T) {
	md, term := placeWorld(t)
	ctx := t.Context()
	if _, err := controlplane.Place(ctx, md, placement.Policy{}, term, placeVol, roomyHost); err != nil {
		t.Fatal(err)
	}

	// A → B in one write would leave two Agents serving one volume for a poll interval.
	if _, err := controlplane.Place(ctx, md, placement.Policy{}, term, placeVol, quietHost); !errors.Is(err, metadata.ErrAlreadyPlaced) {
		t.Fatalf("a straight hand-over: want ErrAlreadyPlaced, got %v", err)
	}
	if serving(t, md, quietHost) || !serving(t, md, roomyHost) {
		t.Fatal("a refused hand-over moved the volume anyway")
	}

	if err := md.SetVolumePrimaryHost(ctx, term, placeVol, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := controlplane.Place(ctx, md, placement.Policy{}, term, placeVol, absentHost); !errors.Is(err, metadata.ErrNotFound) {
		t.Fatalf("a host nobody registered: want ErrNotFound, got %v", err)
	}
	if !placedNowhere(t, md) {
		t.Fatal("placing onto an unregistered host wrote a placement")
	}
}

// TestPlaceUnderAStaleTermWritesNothing: the guard is the store's (§7), and this asserts
// that Place does not do its reads, decide, and then write past it.
func TestPlaceUnderAStaleTermWritesNothing(t *testing.T) {
	md, term := placeWorld(t)
	if _, err := md.AcquireLeadership(t.Context(), "cp-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := controlplane.Place(t.Context(), md, placement.Policy{}, term, placeVol, ""); !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("Place under a stale term: want ErrStaleTerm, got %v", err)
	}
	if !placedNowhere(t, md) {
		t.Fatal("a zombie Control Plane placed a volume")
	}
}

// desiredEpoch is the epoch the volume carries in what GetDesiredState would hand this
// host, or 0 if the host is not being told about the volume at all. It is the observable
// that matters: the Agent puts exactly this number in its WAL path
// (<data-dir>/wal/<volume-id>/<epoch>), so two attaches that produce the same number are
// two sessions that open the same directory.
func desiredEpoch(t *testing.T, md metadata.Store, hostID string) int64 {
	t.Helper()
	vols, err := md.ListVolumesByHost(t.Context(), hostID)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range vols {
		if v.VolumeID == placeVol {
			return v.CurrentEpoch
		}
	}
	return 0
}

// TestPlaceGrantsAFreshEpochToEveryAttach is the silent-corruption case, in the catalog.
//
// A volume moves A -> B, B's guest writes and B publishes, and the volume comes back to
// A. If A is handed the epoch it held before, it opens the WAL directory its *previous*
// session left behind and lays those records over the image B published — older bytes on
// top of newer ones, with no error anywhere. The only thing that makes the stale
// directory unreachable is the epoch, because the epoch is in its path.
func TestPlaceGrantsAFreshEpochToEveryAttach(t *testing.T) {
	md, term := placeWorld(t)
	ctx := t.Context()

	first, err := controlplane.Place(ctx, md, placement.Policy{}, term, placeVol, roomyHost)
	if err != nil {
		t.Fatalf("first attach: %v", err)
	}
	if first.Epoch <= 1 {
		t.Fatalf("the first attach was granted epoch %d, want one past the volume's 1", first.Epoch)
	}
	if got := desiredEpoch(t, md, roomyHost); got != first.Epoch {
		t.Fatalf("the desired state hands epoch %d, Place reported %d", got, first.Epoch)
	}

	// A -> nowhere -> B. The detach grants nothing, so it must not move the epoch: an
	// epoch is a fencing token and burning one on a release names no writer.
	if err := md.SetVolumePrimaryHost(ctx, term, placeVol, ""); err != nil {
		t.Fatal(err)
	}
	if got := desiredEpoch(t, md, roomyHost); got != 0 {
		t.Fatalf("the detached volume is still in %s's desired state at epoch %d", roomyHost, got)
	}
	vol, err := md.GetVolume(ctx, placeVol)
	if err != nil {
		t.Fatal(err)
	}
	if vol.CurrentEpoch != first.Epoch {
		t.Fatalf("the detach moved the epoch from %d to %d", first.Epoch, vol.CurrentEpoch)
	}

	second, err := controlplane.Place(ctx, md, placement.Policy{}, term, placeVol, quietHost)
	if err != nil {
		t.Fatalf("attaching to the second host: %v", err)
	}
	if second.Epoch <= first.Epoch {
		t.Fatalf("the second attach was granted epoch %d, the first held %d", second.Epoch, first.Epoch)
	}

	// And back to the host that already has a WAL directory for `first.Epoch`.
	if err := md.SetVolumePrimaryHost(ctx, term, placeVol, ""); err != nil {
		t.Fatal(err)
	}
	third, err := controlplane.Place(ctx, md, placement.Policy{}, term, placeVol, roomyHost)
	if err != nil {
		t.Fatalf("attaching back to the first host: %v", err)
	}
	if third.Epoch == first.Epoch {
		t.Fatalf("the returning host was handed epoch %d again: it will resume the WAL it wrote before %s served the volume",
			third.Epoch, quietHost)
	}
	// Strictly past everything the volume has ever been served under, not merely
	// different from the last one: the WAL root has to be new on *every* host that has
	// ever held this volume, and the epoch is the only part of the path that can make it
	// so.
	if third.Epoch <= second.Epoch {
		t.Fatalf("the third attach was granted epoch %d, the second held %d", third.Epoch, second.Epoch)
	}
	if got := desiredEpoch(t, md, roomyHost); got != third.Epoch {
		t.Fatalf("the desired state hands epoch %d, Place reported %d", got, third.Epoch)
	}
	// The volume is ACTIVE, not left in the state a bare epoch bump would leave it: a
	// volume with a writer that the catalog calls DETACHED is one no snapshot request
	// will ever be accepted for.
	vol, err = md.GetVolume(ctx, placeVol)
	if err != nil {
		t.Fatal(err)
	}
	if vol.State != lifecycle.VolumeActive || vol.PrimaryHostID != roomyHost {
		t.Fatalf("after the attach the volume is %s on %q, want ACTIVE on %s", vol.State, vol.PrimaryHostID, roomyHost)
	}
}

// TestPlaceBurnsNoEpochOnARefusalOrARepeat: the fencing token moves when — and only
// when — a host is granted a volume it did not hold.
//
// The repeat is the one that costs something if it is wrong. `-attach-volume` re-run
// after a timeout must stay the no-op the store made it: bumping there would tear down a
// running guest's device and abandon whatever the teardown publish did not carry, for a
// command that changed nothing.
func TestPlaceBurnsNoEpochOnARefusalOrARepeat(t *testing.T) {
	md, term := placeWorld(t)
	ctx := t.Context()

	placed, err := controlplane.Place(ctx, md, placement.Policy{}, term, placeVol, roomyHost)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		call func(t *testing.T)
	}{
		{"a repeat of the placement the volume already has", func(t *testing.T) {
			again, aerr := controlplane.Place(ctx, md, placement.Policy{}, term, placeVol, roomyHost)
			if aerr != nil {
				t.Fatalf("re-attaching to the host the volume is already on: %v", aerr)
			}
			if again.Epoch != placed.Epoch {
				t.Fatalf("re-attaching to the same host moved the epoch %d -> %d", placed.Epoch, again.Epoch)
			}
		}},
		{"a straight hand-over", func(t *testing.T) {
			_, aerr := controlplane.Place(ctx, md, placement.Policy{}, term, placeVol, quietHost)
			if !errors.Is(aerr, metadata.ErrAlreadyPlaced) {
				t.Fatalf("hand-over: want ErrAlreadyPlaced, got %v", aerr)
			}
		}},
		{"a host nobody registered", func(t *testing.T) {
			_, aerr := controlplane.Place(ctx, md, placement.Policy{}, term, placeVol, absentHost)
			if !errors.Is(aerr, metadata.ErrNotFound) {
				t.Fatalf("unregistered host: want ErrNotFound, got %v", aerr)
			}
		}},
		{"a zombie Control Plane", func(t *testing.T) {
			if _, aerr := md.AcquireLeadership(ctx, "cp-b"); aerr != nil {
				t.Fatal(aerr)
			}
			_, aerr := controlplane.Place(ctx, md, placement.Policy{}, term, placeVol, roomyHost)
			if !errors.Is(aerr, metadata.ErrStaleTerm) {
				t.Fatalf("stale term: want ErrStaleTerm, got %v", aerr)
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.call(t)
			vol, gerr := md.GetVolume(ctx, placeVol)
			if gerr != nil {
				t.Fatal(gerr)
			}
			if vol.CurrentEpoch != placed.Epoch {
				t.Fatalf("the epoch moved %d -> %d with no host granted the volume", placed.Epoch, vol.CurrentEpoch)
			}
		})
	}
}
