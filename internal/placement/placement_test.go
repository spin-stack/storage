package placement_test

import (
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/placement"
)

const gib = int64(1) << 30

// host is a compact constructor for the table below.
func host(id string, state lifecycle.HostState, total, committed int64) metadata.Host {
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
				host("h-a", lifecycle.HostActive, 100*gib, 90*gib),
				host("h-b", lifecycle.HostActive, 100*gib, 0),
			},
			req:  placement.Request{SizeBytes: 10 * gib, SourceHostID: "h-a"},
			want: "h-a",
		},
		{
			name: "cordoned source falls through to a cached host",
			hosts: []metadata.Host{
				host("h-a", lifecycle.HostCordoned, 100*gib, 0),
				host("h-b", lifecycle.HostActive, 100*gib, 50*gib),
				host("h-c", lifecycle.HostActive, 100*gib, 0),
			},
			req:  placement.Request{SizeBytes: gib, SourceHostID: "h-a", CachedHostIDs: []string{"h-b"}},
			want: "h-b",
		},
		{
			name: "source without capacity falls through to a cached host",
			hosts: []metadata.Host{
				host("h-a", lifecycle.HostActive, 100*gib, 195*gib),
				host("h-b", lifecycle.HostActive, 100*gib, 100*gib),
			},
			req:  placement.Request{SizeBytes: 10 * gib, SourceHostID: "h-a", CachedHostIDs: []string{"h-b"}},
			want: "h-b",
		},
		{
			name: "draining cached host is skipped for any active host",
			hosts: []metadata.Host{
				host("h-a", lifecycle.HostDraining, 100*gib, 0),
				host("h-b", lifecycle.HostDraining, 100*gib, 0),
				host("h-c", lifecycle.HostActive, 100*gib, 10*gib),
			},
			req:  placement.Request{SizeBytes: gib, SourceHostID: "h-a", CachedHostIDs: []string{"h-b"}},
			want: "h-c",
		},
		{
			name: "dead host is never chosen",
			hosts: []metadata.Host{
				host("h-a", lifecycle.HostDead, 100*gib, 0),
			},
			req: placement.Request{SizeBytes: gib},
			err: placement.ErrNoCapacity,
		},
		{
			name: "oversubscription boundary is inclusive",
			hosts: []metadata.Host{
				host("h-a", lifecycle.HostActive, 100*gib, 190*gib),
			},
			req:  placement.Request{SizeBytes: 10 * gib, SourceHostID: "h-a"},
			want: "h-a",
		},
		{
			name: "one byte past the boundary is refused",
			hosts: []metadata.Host{
				host("h-a", lifecycle.HostActive, 100*gib, 190*gib),
			},
			req: placement.Request{SizeBytes: 10*gib + 1, SourceHostID: "h-a"},
			err: placement.ErrNoCapacity,
		},
		{
			name: "a host with no NVMe is never chosen",
			hosts: []metadata.Host{
				host("h-a", lifecycle.HostActive, 0, 0),
			},
			req: placement.Request{SizeBytes: 1},
			err: placement.ErrNoCapacity,
		},
		{
			name: "least-committed host wins among equals",
			hosts: []metadata.Host{
				host("h-a", lifecycle.HostActive, 100*gib, 60*gib),
				host("h-b", lifecycle.HostActive, 100*gib, 20*gib),
				host("h-c", lifecycle.HostActive, 100*gib, 40*gib),
			},
			req:  placement.Request{SizeBytes: gib},
			want: "h-b",
		},
		{
			name: "ties break on host id, deterministically",
			hosts: []metadata.Host{
				host("h-z", lifecycle.HostActive, 100*gib, 10*gib),
				host("h-a", lifecycle.HostActive, 100*gib, 10*gib),
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
		host("h-a", lifecycle.HostActive, 100*gib, 30*gib),
		host("h-b", lifecycle.HostActive, 100*gib, 30*gib),
		host("h-c", lifecycle.HostActive, 200*gib, 60*gib),
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
		{"half committed", host("h", lifecycle.HostActive, 100*gib, 50*gib), 0.5},
		{"oversubscribed", host("h", lifecycle.HostActive, 100*gib, 250*gib), 2.5},
		{"no capacity reported", host("h", lifecycle.HostActive, 0, 10), 0},
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
	hosts := []metadata.Host{host("h-a", lifecycle.HostActive, 100*gib, 95*gib)}

	if _, err := policy.Choose(hosts, placement.Request{SizeBytes: 10 * gib}); !errors.Is(err, placement.ErrNoCapacity) {
		t.Fatalf("zero policy must not oversubscribe, got %v", err)
	}
	if got, err := policy.Choose(hosts, placement.Request{SizeBytes: 5 * gib}); err != nil || got != "h-a" {
		t.Fatalf("within total: host=%q err=%v", got, err)
	}
}

// Finding 12 (low). §28.2 declares a hard bound — committed/total may not exceed
// MaxOversubscription — and Policy.Choose is the only place that evaluates it at
// placement time. Choose is pure: it reads a host list and returns a name. Two
// callers that read the same list (a drain and a clone, or two drains) both get the
// same destination, both commit, and the destination lands past the bound with
// nobody having made a mistake.
//
// The bound therefore lives in the write that reserves — one statement that both
// adds the bytes and refuses if the result breaks the policy (metadata.
// CapacityChange.Limit, fed by Policy.Limit). This test is the other end of that
// rule: the number the write is handed has to be the one Choose used, so the check
// stays callable and stays a single copy.
func TestTwoPlacementsRacingForOneDestination(t *testing.T) {
	policy := placement.Policy{MaxOversubscription: 2.0}
	dest := host("h-dest", lifecycle.HostActive, 100*gib, 150*gib)
	hosts := []metadata.Host{dest}
	req := placement.Request{SizeBytes: 40 * gib}

	// Both operations read the fleet before either has committed anything.
	first, err := policy.Choose(hosts, req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := policy.Choose(hosts, req)
	if err != nil {
		t.Fatal(err)
	}
	if first != "h-dest" || second != "h-dest" {
		t.Fatalf("placements chose %q and %q, want the only host twice", first, second)
	}

	// The first reservation lands. The second is now the one that breaks §28.2, and
	// the policy must say so when it is re-checked against the state that exists —
	// which is the whole point of the check being callable at commit time.
	dest.NVMeCommittedBytes += req.SizeBytes
	if !policy.Admits(dest, req.SizeBytes) {
		return // correctly refused
	}
	t.Fatalf("committing the second placement takes %s to %d/%d bytes, past the declared %.1fx bound",
		dest.HostID, dest.NVMeCommittedBytes+req.SizeBytes, dest.NVMeTotalBytes, policy.MaxOversubscription)
}

// TestAdmitsIsTheBoundChooseUsed: the commit-time check and the placement-time check
// must be the same rule. Two rules is how a host ends up holding what placement
// believed it refused.
func TestAdmitsIsTheBoundChooseUsed(t *testing.T) {
	tests := []struct {
		name   string
		policy placement.Policy
		host   metadata.Host
		size   int64
		want   bool
	}{
		{"exactly at the bound is admitted", placement.Policy{MaxOversubscription: 2.0}, host("h", lifecycle.HostActive, 100*gib, 150*gib), 50 * gib, true},
		{"one byte past the bound is refused", placement.Policy{MaxOversubscription: 2.0}, host("h", lifecycle.HostActive, 100*gib, 150*gib), 50*gib + 1, false},
		{"the zero policy is no oversubscription", placement.Policy{}, host("h", lifecycle.HostActive, 100*gib, 100*gib), 1, false},
		{"a cordoned host admits nothing", placement.Policy{MaxOversubscription: 2.0}, host("h", lifecycle.HostCordoned, 100*gib, 0), gib, false},
		{"a host reporting no NVMe admits nothing", placement.Policy{MaxOversubscription: 2.0}, host("h", lifecycle.HostActive, 0, 0), 1, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.policy.Admits(tc.host, tc.size); got != tc.want {
				t.Fatalf("Admits = %v, want %v", got, tc.want)
			}
			// Choose must agree: it is the same bound.
			_, err := tc.policy.Choose([]metadata.Host{tc.host}, placement.Request{SizeBytes: tc.size})
			if chose := err == nil; chose != tc.want {
				t.Fatalf("Choose admitted %v while Admits said %v", chose, tc.want)
			}
		})
	}
}

// filling is a host that has *measured* used bytes on its device, which is the input
// admission ignored until ADR-0013's second surviving gap was closed: the heartbeat
// has always shipped it and nothing read it.
func filling(id string, total, committed, used int64) metadata.Host {
	h := host(id, lifecycle.HostActive, total, committed)
	h.NVMeUsedBytes = used
	return h
}

// TestAdmitsCountsWhatTheHostIsUsing: the promise and the measurement are two
// different questions, and a host can pass one while failing the other. Under
// ADR-0026 a session's whole WAL stays on the device until the volume stops, so
// what fills a host is what its guests write — a quantity no reservation covers and
// no committed byte predicts.
func TestAdmitsCountsWhatTheHostIsUsing(t *testing.T) {
	const total = 100 * gib

	tests := []struct {
		name   string
		policy placement.Policy
		host   metadata.Host
		size   int64
		want   bool
	}{
		{
			// The case the oversubscription bound alone cannot see: nothing has been
			// promised here at all, and the device is nearly gone.
			name:   "well inside its promises and nearly out of device",
			policy: placement.Policy{MaxOversubscription: 2.0},
			host:   filling("h", total, 0, 90*gib),
			size:   gib,
		},
		{
			name:   "exactly at the fill ceiling still takes work",
			policy: placement.Policy{MaxOversubscription: 2.0},
			host:   filling("h", total, 0, 85*gib),
			size:   gib,
			want:   true,
		},
		{
			name:   "one byte past the fill ceiling does not",
			policy: placement.Policy{MaxOversubscription: 2.0},
			host:   filling("h", total, 0, 85*gib+1),
			size:   gib,
		},
		{
			// The zero value is the ADR-0013 ceiling, not "unbounded": a policy
			// written before the field existed never decided that a full device may
			// keep receiving volumes.
			name:   "the zero policy still refuses a filling device",
			policy: placement.Policy{},
			host:   filling("h", total, 0, 86*gib),
			size:   gib,
		},
		{
			name:   "an operator may declare that a device fills completely",
			policy: placement.Policy{MaxUsedRatio: 1.0},
			host:   filling("h", total, 0, 99*gib),
			size:   gib,
			want:   true,
		},
		{
			// Trap: "used == 0 means we have not measured yet, so refuse" would
			// refuse the emptiest host in the fleet. There is no such state — total
			// and used are one statfs in one heartbeat — and the host that has never
			// reported is already refused by the total below.
			name:   "a device measured empty is the best host there is",
			policy: placement.Policy{MaxOversubscription: 2.0},
			host:   filling("h", total, 0, 0),
			size:   gib,
			want:   true,
		},
		{
			name:   "a host that never measured its device is refused, as before",
			policy: placement.Policy{MaxOversubscription: 2.0},
			host:   filling("h", 0, 0, 0),
			size:   gib,
		},
		{
			// The measurement arm charges the request nothing, so a volume larger
			// than the remaining physical headroom is still placeable on a device
			// that is not yet filling. Charging it would assume a thin volume
			// occupies its declared size on arrival, which is the assumption
			// oversubscription exists to deny.
			name:   "a volume bigger than the free space lands on a device with room to spare",
			policy: placement.Policy{MaxOversubscription: 2.0},
			host:   filling("h", total, 0, 50*gib),
			size:   80 * gib,
			want:   true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.policy.Admits(tc.host, tc.size); got != tc.want {
				t.Fatalf("Admits = %v, want %v", got, tc.want)
			}
			// Choose must agree, and so must the numbers handed to the write: one
			// rule, evaluated in three places, is the whole arrangement.
			_, err := tc.policy.Choose([]metadata.Host{tc.host}, placement.Request{SizeBytes: tc.size})
			if chose := err == nil; chose != tc.want {
				t.Fatalf("Choose admitted %v while Admits said %v", chose, tc.want)
			}
			b := tc.policy.Bound(tc.host, tc.size)
			fits := tc.host.NVMeCommittedBytes+b.AddBytes <= b.Limit && tc.host.NVMeUsedBytes <= b.UsedLimit
			if tc.host.NVMeTotalBytes > 0 && fits != tc.want {
				t.Fatalf("the bound handed to the write admits %v while Admits said %v (%+v)", fits, tc.want, b)
			}
		})
	}
}

// TestChooseSkipsAFillingHost is the same rule reached through the §20 order: the
// source host holds the data, so step 1 wants it, and a source whose device is
// filling has to fall through exactly as a cordoned one does. Its data being local
// is worth nothing if writing to it is what breaks the host.
func TestChooseSkipsAFillingHost(t *testing.T) {
	policy := placement.Policy{MaxOversubscription: 2.0}
	hosts := []metadata.Host{
		filling("h-source", 100*gib, 0, 95*gib),
		filling("h-other", 100*gib, 50*gib, 10*gib),
	}
	got, err := policy.Choose(hosts, placement.Request{SizeBytes: gib, SourceHostID: "h-source"})
	if err != nil || got != "h-other" {
		t.Fatalf("Choose = %q (%v), want h-other: a source host at 95%% of its device takes no new volume", got, err)
	}
	// And with nowhere to fall through to, the answer is no capacity rather than a
	// host that cannot hold it.
	if _, err := policy.Choose(hosts[:1], placement.Request{SizeBytes: gib, SourceHostID: "h-source"}); !errors.Is(err, placement.ErrNoCapacity) {
		t.Fatalf("Choose on a filling fleet = %v, want ErrNoCapacity", err)
	}
}
