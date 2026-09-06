package controlplane_test

import (
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// Detaching a volume is what an operator does *first* when moving one, and the instruction
// that made it safe — "detached, observed to have stopped, and then placed" — lived in a
// log line. Nothing enforced it. So the one path to two guests writing one volume that
// this system actually has is an operator reacting to a partition: detach, place, and the
// incumbent, which cannot be reached to be told anything, goes on serving.
//
// The dwell is one half of the answer. The other is the Agent pausing its own guests when
// nothing has confirmed its claim (agent.Loop.isolationGrace); neither is sound alone, and
// the inequality between them is what makes the pair sound.
func TestDetachWillNotTakeAVolumeFromAHostStillServingIt(t *testing.T) {
	t.Parallel()
	const (
		vol   = "00000000-0000-7000-8000-0000000000a1"
		host  = "00000000-0000-7000-8000-0000000000a2"
		dwell = 90 * time.Second
	)
	now := time.Unix(1_700_000_000, 0).UTC()

	tests := []struct {
		name string
		// heartbeatAgo is how long the host has been silent.
		heartbeatAgo time.Duration
		refusal      lifecycle.Refusal
		force        bool
		wantRefused  bool
	}{{
		name:         "a host that reports it is serving, and was heard from just now",
		heartbeatAgo: time.Second,
		wantRefused:  true,
	}, {
		name:         "the same host, forced: the operator says they watched the guest stop",
		heartbeatAgo: time.Second,
		force:        true,
	}, {
		// The host itself saying so, which is the observation the instruction asked for.
		name:         "a host that reports it stopped serving the volume",
		heartbeatAgo: time.Second,
		refusal:      lifecycle.RefusalLeaseLost,
	}, {
		// And the refusal the Agent's own isolation response sends, which is this pair
		// closing: the incumbent paused its guest and said so.
		name:         "a host that paused the guest because it could confirm nothing",
		heartbeatAgo: time.Second,
		refusal:      lifecycle.RefusalIsolated,
	}, {
		name:         "a host nothing has heard from for the whole dwell",
		heartbeatAgo: dwell,
	}, {
		name:         "silent, but not for long enough",
		heartbeatAgo: dwell - time.Second,
		wantRefused:  true,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			// The store stamps last_heartbeat with its own clock, which is the rule the
			// schema states and the reason a fixture cannot simply declare one. So the
			// host is upserted in the past and the clock is walked forward to now.
			clk := sim.NewClock(now.Add(-tc.heartbeatAgo))
			md := metasim.New(clk.Wall)
			term, err := md.AcquireLeadership(ctx, "cp")
			if err != nil {
				t.Fatal(err)
			}
			if err := md.UpsertHost(ctx, term, metadata.Host{
				HostID: host, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
			}); err != nil {
				t.Fatal(err)
			}
			clk.Advance(tc.heartbeatAgo)
			if err := md.CreateVolume(ctx, term, metadata.Volume{
				VolumeID: vol, SizeBytes: 1 << 30, BlockSize: 65536,
				State: lifecycle.VolumeActive, CurrentEpoch: 1, PrimaryHostID: host,
				DEKWrapped: []byte{7}, KEKID: "kek", DEKKeyID: 42,
				Progress: metadata.VolumeProgress{Refusal: tc.refusal},
			}, nil); err != nil {
				t.Fatal(err)
			}

			err = controlplane.Detach(ctx, md, term, vol, dwell, now, tc.force)
			if tc.wantRefused {
				if !errors.Is(err, controlplane.ErrStillServing) {
					t.Fatalf("Detach = %v, want ErrStillServing — this is the one command that can put two guests on one volume", err)
				}
				// And it must not have half-happened: a volume detached in the catalog
				// while its host serves on is the split brain with an audit trail.
				v, gerr := md.GetVolume(ctx, vol)
				if gerr != nil {
					t.Fatal(gerr)
				}
				if v.PrimaryHostID != host {
					t.Fatalf("the volume was detached anyway: primary_host_id = %q", v.PrimaryHostID)
				}
				return
			}
			if err != nil {
				t.Fatalf("Detach: %v", err)
			}
			v, gerr := md.GetVolume(ctx, vol)
			if gerr != nil {
				t.Fatal(gerr)
			}
			if v.PrimaryHostID != "" {
				t.Fatalf("primary_host_id = %q after a detach that was allowed", v.PrimaryHostID)
			}
		})
	}
}

// A volume nobody holds detaches without a word: there is no incumbent to race.
func TestDetachOfAnUnplacedVolumeIsNotRefused(t *testing.T) {
	t.Parallel()
	const vol = "00000000-0000-7000-8000-0000000000a3"
	ctx := t.Context()
	now := time.Unix(1_700_000_000, 0).UTC()
	clk := sim.NewClock(now)
	md := metasim.New(clk.Wall)
	term, err := md.AcquireLeadership(ctx, "cp")
	if err != nil {
		t.Fatal(err)
	}
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: vol, SizeBytes: 1 << 30, BlockSize: 65536, State: lifecycle.VolumeActive,
		CurrentEpoch: 1, DEKWrapped: []byte{7}, KEKID: "kek", DEKKeyID: 42,
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := controlplane.Detach(ctx, md, term, vol, 90*time.Second, now, false); err != nil {
		t.Fatalf("Detach of an unplaced volume: %v", err)
	}
}
