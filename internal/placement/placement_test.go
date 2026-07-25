package placement_test

import (
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/placement"
)

const gib = int64(1) << 30

// host is a compact constructor for the table below.
func host(id, state string, total, committed int64) metadata.Host {
	return metadata.Host{
		HostID: id, State: state,
		NVMeTotalBytes: total, NVMeCommittedBytes: committed,
	}
}

// TestChooseFollowsPlacementOrder covers the §20 order (source → cached/standby →
// any) together with the §28.1/§28.2 rules: cordoned/draining/dead hosts are never
// chosen and the declared oversubscription ratio bounds every choice.
func TestChooseFollowsPlacementOrder(t *testing.T) {
	policy := placement.Policy{MaxOversubscription: 2.0}

	tests := []struct {
		name  string
		hosts []metadata.Host
		req   placement.Request
		want  string
		err   error
	}{
		{
			name: "source host wins even when emptier hosts exist",
			hosts: []metadata.Host{
				host("h-a", metadata.HostActive, 100*gib, 90*gib),
				host("h-b", metadata.HostActive, 100*gib, 0),
			},
			req:  placement.Request{SizeBytes: 10 * gib, SourceHostID: "h-a"},
			want: "h-a",
		},
		{
			name: "cordoned source falls through to a cached host",
			hosts: []metadata.Host{
				host("h-a", metadata.HostCordoned, 100*gib, 0),
				host("h-b", metadata.HostActive, 100*gib, 50*gib),
				host("h-c", metadata.HostActive, 100*gib, 0),
			},
			req:  placement.Request{SizeBytes: gib, SourceHostID: "h-a", CachedHostIDs: []string{"h-b"}},
			want: "h-b",
		},
		{
			name: "source without capacity falls through to a cached host",
			hosts: []metadata.Host{
				host("h-a", metadata.HostActive, 100*gib, 195*gib),
				host("h-b", metadata.HostActive, 100*gib, 100*gib),
			},
			req:  placement.Request{SizeBytes: 10 * gib, SourceHostID: "h-a", CachedHostIDs: []string{"h-b"}},
			want: "h-b",
		},
		{
			name: "draining cached host is skipped for any active host",
			hosts: []metadata.Host{
				host("h-a", metadata.HostDraining, 100*gib, 0),
				host("h-b", metadata.HostDraining, 100*gib, 0),
				host("h-c", metadata.HostActive, 100*gib, 10*gib),
			},
			req:  placement.Request{SizeBytes: gib, SourceHostID: "h-a", CachedHostIDs: []string{"h-b"}},
			want: "h-c",
		},
		{
			name: "dead host is never chosen",
			hosts: []metadata.Host{
				host("h-a", metadata.HostDead, 100*gib, 0),
			},
			req: placement.Request{SizeBytes: gib},
			err: placement.ErrNoCapacity,
		},
		{
			name: "oversubscription boundary is inclusive",
			hosts: []metadata.Host{
				host("h-a", metadata.HostActive, 100*gib, 190*gib),
			},
			req:  placement.Request{SizeBytes: 10 * gib, SourceHostID: "h-a"},
			want: "h-a",
		},
		{
			name: "one byte past the boundary is refused",
			hosts: []metadata.Host{
				host("h-a", metadata.HostActive, 100*gib, 190*gib),
			},
			req: placement.Request{SizeBytes: 10*gib + 1, SourceHostID: "h-a"},
			err: placement.ErrNoCapacity,
		},
		{
			name: "a host with no NVMe is never chosen",
			hosts: []metadata.Host{
				host("h-a", metadata.HostActive, 0, 0),
			},
			req: placement.Request{SizeBytes: 1},
			err: placement.ErrNoCapacity,
		},
		{
			name: "least-committed host wins among equals",
			hosts: []metadata.Host{
				host("h-a", metadata.HostActive, 100*gib, 60*gib),
				host("h-b", metadata.HostActive, 100*gib, 20*gib),
				host("h-c", metadata.HostActive, 100*gib, 40*gib),
			},
			req:  placement.Request{SizeBytes: gib},
			want: "h-b",
		},
		{
			name: "ties break on host id, deterministically",
			hosts: []metadata.Host{
				host("h-z", metadata.HostActive, 100*gib, 10*gib),
				host("h-a", metadata.HostActive, 100*gib, 10*gib),
			},
			req:  placement.Request{SizeBytes: gib},
			want: "h-a",
		},
		{
			name:  "no hosts at all",
			hosts: nil,
			req:   placement.Request{SizeBytes: 1},
			err:   placement.ErrNoCapacity,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := policy.Choose(tc.hosts, tc.req)
			if tc.err != nil {
				if !errors.Is(err, tc.err) {
					t.Fatalf("want %v, got host=%q err=%v", tc.err, got, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("chose %q, want %q", got, tc.want)
			}
		})
	}
}

// TestChooseIsDeterministic is the INV-02 requirement on any policy the DST harness
// drives: input order must not change the outcome.
func TestChooseIsDeterministic(t *testing.T) {
	policy := placement.Policy{MaxOversubscription: 2.0}
	forward := []metadata.Host{
		host("h-a", metadata.HostActive, 100*gib, 30*gib),
		host("h-b", metadata.HostActive, 100*gib, 30*gib),
		host("h-c", metadata.HostActive, 200*gib, 60*gib),
	}
	reversed := []metadata.Host{forward[2], forward[1], forward[0]}

	req := placement.Request{SizeBytes: gib}
	first, err := policy.Choose(forward, req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := policy.Choose(reversed, req)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("choice depends on input order: %q vs %q", first, second)
	}
	// And the input is left untouched.
	if forward[0].HostID != "h-a" || forward[0].NVMeCommittedBytes != 30*gib {
		t.Fatalf("Choose mutated its input: %+v", forward[0])
	}
}

// TestCommittedRatio is the value behind the host_nvme_committed_ratio alert (§28.2).
func TestCommittedRatio(t *testing.T) {
	tests := []struct {
		name string
		h    metadata.Host
		want float64
	}{
		{"half committed", host("h", metadata.HostActive, 100*gib, 50*gib), 0.5},
		{"oversubscribed", host("h", metadata.HostActive, 100*gib, 250*gib), 2.5},
		{"no capacity reported", host("h", metadata.HostActive, 0, 10), 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := placement.CommittedRatio(tc.h); got != tc.want {
				t.Fatalf("ratio = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestZeroPolicyRefusesOversubscription documents the safe default: an unset policy
// means "no oversubscription" (committed <= total), never "unbounded".
func TestZeroPolicyRefusesOversubscription(t *testing.T) {
	var policy placement.Policy
	hosts := []metadata.Host{host("h-a", metadata.HostActive, 100*gib, 95*gib)}

	if _, err := policy.Choose(hosts, placement.Request{SizeBytes: 10 * gib}); !errors.Is(err, placement.ErrNoCapacity) {
		t.Fatalf("zero policy must not oversubscribe, got %v", err)
	}
	if got, err := policy.Choose(hosts, placement.Request{SizeBytes: 5 * gib}); err != nil || got != "h-a" {
		t.Fatalf("within total: host=%q err=%v", got, err)
	}
}
