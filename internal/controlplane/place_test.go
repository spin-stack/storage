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
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: placeVol, SizeBytes: 1 << 30, BlockSize: 65536,
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
	host, err := controlplane.Place(t.Context(), md, placement.Policy{}, term, placeVol, "")
	if err != nil {
		t.Fatalf("Place: %v", err)
	}
	if host.HostID != roomyHost {
		t.Fatalf("placed on %s, want the one host that admits it (%s)", host.HostID, roomyHost)
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
	host, err := controlplane.Place(t.Context(), md, placement.Policy{}, term, placeVol, quietHost)
	if err != nil {
		t.Fatalf("Place on a named cordoned host: %v", err)
	}
	if host.HostID != quietHost || host.State != lifecycle.HostCordoned {
		t.Fatalf("Place returned %s/%s, want the named host and the state that says it is out of service",
			host.HostID, host.State)
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
