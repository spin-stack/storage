package main

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/real"
)

// testTTL is the lease TTL every report below is rendered against. It is the flag's
// default, because the staleness threshold is the flag: a heartbeat older than the
// lease the Control Plane grants is a host the fleet has no current fact about.
const testTTL = 30 * time.Second

// Fixed UUIDv7s (INV-22), so the report's rows can be asserted by name.
const (
	activeHost   = "00000000-0000-7000-8000-0000000000a1"
	cordonedHost = "00000000-0000-7000-8000-0000000000a2"
	servedVol    = "00000000-0000-7000-8000-0000000000b1"
	strandedVol  = "00000000-0000-7000-8000-0000000000b2"
	deepVol      = "00000000-0000-7000-8000-0000000000b3"
	stuckSnap    = "00000000-0000-7000-8000-0000000000c1"
	doneSnap     = "00000000-0000-7000-8000-0000000000c2"
	requestID    = "00000000-0000-7000-8000-0000000000e1"
)

// row returns the whitespace-separated fields of the printed line that starts with
// key, so an assertion is about the values in the report and not about the column
// widths tabwriter chose. It fails the test when the line is absent — a report that
// silently omits a row is the failure this whole command exists to prevent.
func row(t *testing.T, out, key string) []string {
	t.Helper()
	for line := range strings.Lines(out) {
		if fields := strings.Fields(line); len(fields) > 0 && fields[0] == key {
			return fields
		}
	}
	t.Fatalf("no line for %s in:\n%s", key, out)
	return nil
}

func mustSay(t *testing.T, out, want string) {
	t.Helper()
	if !strings.Contains(out, want) {
		t.Fatalf("report does not say %q:\n%s", want, out)
	}
}

// TestFleetStatusShowsWhatTheHostScopedReadsCannot drives the report against a
// catalog in the shape an incident leaves behind — a host cordoned by device
// pressure, a volume nobody is serving, and a snapshot that will never be taken
// because the volume it belongs to has no host — and asserts on the text the process
// writes, not on what it read.
//
// Every one of those three rows is invisible to the reads the Control Plane serves:
// ListVolumesByHost and ListPendingSnapshots are both scoped to a host, and none of
// these belongs to one.
func TestFleetStatusShowsWhatTheHostScopedReadsCannot(t *testing.T) {
	ctx := t.Context()
	// The catalog's clock, driven by hand: the heartbeat ages in the report are
	// differences against it, so they are exact strings here rather than "about a
	// minute".
	now := time.Unix(1_700_000_000, 0).UTC()
	md := metasim.New(func() time.Time { return now })

	term, err := md.AcquireLeadership(ctx, "cp-a")
	if err != nil {
		t.Fatal(err)
	}

	now = now.Add(-90 * time.Second)
	if err := md.UpsertHost(ctx, term, metadata.Host{
		HostID: cordonedHost, State: lifecycle.HostActive,
		NVMeTotalBytes: 1 << 40, NVMeUsedBytes: 900 << 30,
	}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(85 * time.Second) // the healthy host heartbeated 5s ago
	if err := md.UpsertHost(ctx, term, metadata.Host{
		HostID: activeHost, State: lifecycle.HostActive,
		NVMeTotalBytes: 1 << 40, NVMeUsedBytes: 100 << 30,
	}); err != nil {
		t.Fatal(err)
	}
	if err := md.SetHostState(ctx, term, cordonedHost, lifecycle.HostCordoned, lifecycle.CordonPressure); err != nil {
		t.Fatal(err)
	}

	for _, v := range []metadata.Volume{
		{VolumeID: servedVol, SizeBytes: 1 << 30, BlockSize: 65536, CurrentEpoch: 3,
			State: lifecycle.VolumeActive, PrimaryHostID: activeHost,
			DEKWrapped: []byte{7}, KEKID: "kek", DEKKeyID: 42},
		// What -rebuild-metadata restores: no object records a placement, so the volume
		// comes back with no primary.
		{VolumeID: strandedVol, SizeBytes: 2 << 30, BlockSize: 65536, CurrentEpoch: 9,
			State:      lifecycle.VolumeDetached,
			DEKWrapped: []byte{7}, KEKID: "kek", DEKKeyID: 42},
	} {
		if err := md.CreateVolume(ctx, term, v, nil); err != nil {
			t.Fatal(err)
		}
	}

	if err := md.CreateSnapshot(ctx, term, metadata.Snapshot{
		SnapshotID: stuckSnap, VolumeID: strandedVol, Epoch: 9, TargetSequence: 0,
		RootDigest: "d", State: lifecycle.SnapshotCreating, RequestID: requestID,
	}); err != nil {
		t.Fatal(err)
	}
	if err := md.CreateSnapshot(ctx, term, metadata.Snapshot{
		SnapshotID: doneSnap, VolumeID: servedVol, Epoch: 3, TargetSequence: 77,
		RootDigest: "d", State: lifecycle.SnapshotPublished, SourceHostID: activeHost,
		ManifestKey: "image/v/snapshots/s.json", RequestID: requestID,
	}); err != nil {
		t.Fatal(err)
	}

	// A clone of doneSnap that has itself been cloned to the ceiling. It is created after
	// the snapshot it points at, because that is the order the catalog's foreign key
	// admits — and it is the row an operator is looking for when a clone was refused.
	if err := md.CreateVolume(ctx, term, metadata.Volume{
		VolumeID: deepVol, SizeBytes: 1 << 30, BlockSize: 65536, CurrentEpoch: 1,
		State: lifecycle.VolumeActive, PrimaryHostID: activeHost,
		ChainDepth: controlplane.MaxChainDepth, ParentSnapshotID: doneSnap,
		DEKWrapped: []byte{7}, KEKID: "kek", DEKKeyID: 42,
	}, nil); err != nil {
		t.Fatal(err)
	}

	now = now.Add(5 * time.Second)
	var buf bytes.Buffer
	if err := fleetReport(ctx, md, &buf, testTTL); err != nil {
		t.Fatalf("fleetReport: %v", err)
	}
	out := buf.String()
	t.Log("\n" + out)

	mustSay(t, out, "as of 2023-11-14T22:13:20Z")
	mustSay(t, out, "LEADER  cp-a  term 1")

	// The cordoned host: an operator has to be able to read *why* it is out of service
	// and how full it is, or the row says only that something is wrong. Its heartbeat
	// is 1m30s old against a 30s lease, so the STATE cell says the catalog's answer is
	// no longer a current fact — the fixture was written before anything looked.
	mustSay(t, out, "HOSTS (2, 1 not taking placements, 1 with no heartbeat in the last 30s")
	if got, want := row(t, out, cordonedHost),
		[]string{cordonedHost, "STALE(CORDONED)", "DEVICE_PRESSURE", "900.0GiB", "(88%)", "1.0TiB", "0B", "1m30s"}; !equal(got, want) {
		t.Errorf("cordoned host row = %v, want %v", got, want)
	}
	if got, want := row(t, out, activeHost),
		[]string{activeHost, "ACTIVE", "-", "100.0GiB", "(10%)", "1.0TiB", "2.0GiB", "5s"}; !equal(got, want) {
		t.Errorf("active host row = %v, want %v", got, want)
	}

	// The volume nobody serves, and the count in the header that answers the question
	// on its own.
	mustSay(t, out, "VOLUMES (3, 1 with no primary host, 1 at the depth ceiling of 5")
	if got, want := row(t, out, strandedVol),
		[]string{strandedVol, "-", "DETACHED", "9", "2.0GiB", "0", "-"}; !equal(got, want) {
		t.Errorf("unplaced volume row = %v, want %v", got, want)
	}
	if got, want := row(t, out, servedVol),
		[]string{servedVol, activeHost, "ACTIVE", "3", "1.0GiB", "0", "-"}; !equal(got, want) {
		t.Errorf("served volume row = %v, want %v", got, want)
	}
	// The volume at the ceiling, which is the one this section can answer for and the
	// `chain_depth` series cannot: a clone of it is refused until someone flattens it,
	// and until then its depth is a fact about the fleet rather than an event.
	if got, want := row(t, out, deepVol),
		[]string{deepVol, activeHost, "ACTIVE", "1", "1.0GiB", "5", doneSnap}; !equal(got, want) {
		t.Errorf("volume at the ceiling row = %v, want %v", got, want)
	}

	// The snapshot nothing will ever take, and — the part that makes it actionable —
	// that no host is going to.
	mustSay(t, out, "SNAPSHOTS NOT FINISHED (1)")
	if got, want := row(t, out, stuckSnap),
		[]string{stuckSnap, strandedVol, "CREATING", "9", "-"}; !equal(got, want) {
		t.Errorf("stuck snapshot row = %v, want %v", got, want)
	}
	// A published snapshot is finished. If it appears here, the section is a listing of
	// the snapshots table and not an answer to "what is outstanding" — and on a real
	// catalog that difference is thousands of rows against three.
	//
	// Scoped to the section rather than to the whole report, which it was until a volume
	// in this fixture acquired a parent link: a published snapshot appears legitimately in
	// the volumes section as the thing a clone was made from, so a report-wide search now
	// answers a question nobody asked.
	if unfinished := out[strings.Index(out, "SNAPSHOTS NOT FINISHED"):]; strings.Contains(unfinished, doneSnap) {
		t.Errorf("the report lists a PUBLISHED snapshot as unfinished:\n%s", out)
	}
}

// TestFleetStatusOfAnEmptyCatalogSaysSo: the report has to distinguish "nothing is
// wrong" from "the command did not run". A section that printed nothing, and a
// missing leader that returned an error, both read as the latter — and the second is
// worse than useless during an incident, since a fleet with no elected Control Plane
// is exactly when someone runs this.
func TestFleetStatusOfAnEmptyCatalogSaysSo(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	md := metasim.New(func() time.Time { return now })

	var buf bytes.Buffer
	if err := fleetReport(t.Context(), md, &buf, testTTL); err != nil {
		t.Fatalf("fleetReport with no leader: %v", err)
	}
	out := buf.String()
	t.Log("\n" + out)

	mustSay(t, out, "LEADER  none")
	for _, section := range []string{
		"HOSTS (0, 0 not taking placements, 0 with no heartbeat in the last 30s",
		"VOLUMES (0, 0 with no primary host, 0 at the depth ceiling",
		"SNAPSHOTS NOT FINISHED (0)",
	} {
		mustSay(t, out, section)
	}
	if n := strings.Count(out, "(none)"); n != 3 {
		t.Errorf("empty sections marked with (none): %d, want 3:\n%s", n, out)
	}
}

// TestFleetStatusDoesNotCallAHostWithNoHeartbeatActive is the first of the three
// states a readiness run reproduced: `kill -9` an Agent and the report says ACTIVE
// for ever, because ACTIVE is a column somebody has to write and the thing that
// writes it is the process that just died.
//
// The fix is not a new state in the catalog — it is time. The report is rendered
// against the lease TTL the Control Plane grants, so a host whose last heartbeat is
// older than its own lease is one the fleet holds no current fact about, and the
// STATE cell has to say that before it says anything else.
func TestFleetStatusDoesNotCallAHostWithNoHeartbeatActive(t *testing.T) {
	ctx := t.Context()
	now := time.Unix(1_700_000_000, 0).UTC()
	md := metasim.New(func() time.Time { return now })

	term, err := md.AcquireLeadership(ctx, "cp-a")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{activeHost, cordonedHost} {
		if err := md.UpsertHost(ctx, term, metadata.Host{
			HostID: id, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// One lease TTL and a second past the last heartbeat of both hosts: what the
	// operator in the readiness run was looking at 40 s after a kill -9.
	now = now.Add(testTTL + time.Second)
	// One host comes back — the other is the dead one. Two rows, so the assertion is
	// that the report *distinguishes* them rather than that it decorates every row.
	if err := md.UpsertHost(ctx, term, metadata.Host{
		HostID: activeHost, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
	}); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := fleetReport(ctx, md, &buf, testTTL); err != nil {
		t.Fatalf("fleetReport: %v", err)
	}
	out := buf.String()
	t.Log("\n" + out)

	// The dead host. `ACTIVE` is what the catalog still says and what placement will
	// still read, and it is exactly the word that must not appear on its own.
	dead := row(t, out, cordonedHost)
	if dead[1] == "ACTIVE" {
		t.Errorf("a host with no heartbeat for %s is printed ACTIVE:\n%s", testTTL+time.Second, out)
	}
	if got, want := dead[1], "STALE(ACTIVE)"; got != want {
		t.Errorf("dead host STATE = %q, want %q:\n%s", got, want, out)
	}
	if got, want := dead[len(dead)-1], "31s"; got != want {
		t.Errorf("dead host HEARTBEAT = %q, want %q:\n%s", got, want, out)
	}
	// The live one is untouched: a report that marks everything marks nothing.
	if got, want := row(t, out, activeHost)[1], "ACTIVE"; got != want {
		t.Errorf("live host STATE = %q, want %q:\n%s", got, want, out)
	}
	// And the count is in the header, where the answer to "is anything wrong" is,
	// next to the sentence that says placement has not been taught this yet.
	mustSay(t, out, "HOSTS (2, 0 not taking placements, 1 with no heartbeat in the last 30s")
}

// TestFleetStatusMarksALeaderNothingHasSeenRunning is the second state: both Control
// Planes SIGTERMed, and 25 s later the report still printed `LEADER cp-b term 4
// renewed 5m29s ago` — the same shape it prints for a live one, because renewed_at is
// stamped once at election and never again.
//
// What the report can prove without a renewal it does not have: every host heartbeat
// is a term-guarded write, so a heartbeat stamped after this term was created is a
// process holding this term running at that instant. The election itself is the same
// kind of proof at time zero. Anything older than a lease TTL is a leader nothing has
// seen since.
func TestFleetStatusMarksALeaderNothingHasSeenRunning(t *testing.T) {
	tests := []struct {
		name string
		// gap is how long before the report the last heartbeat under this term
		// landed. A leader that has been leading longer than that has been seen.
		since time.Duration
		want  string
		notes string
	}{
		{
			name:  "a heartbeat it accepted a moment ago",
			since: 3 * time.Second,
			want:  "last seen 3s ago",
		},
		{
			name:  "nothing under this term for longer than a lease",
			since: testTTL + 5*time.Second,
			want:  "NOT SEEN for 35s",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			now := time.Unix(1_700_000_000, 0).UTC()
			md := metasim.New(func() time.Time { return now })

			// Elected ten minutes ago in both cases: the age of the election is what
			// the report used to answer with, and it must not be what decides.
			now = now.Add(-10 * time.Minute)
			term, err := md.AcquireLeadership(ctx, "cp-b")
			if err != nil {
				t.Fatal(err)
			}
			now = now.Add(10*time.Minute - tc.since)
			if err := md.UpsertHost(ctx, term, metadata.Host{
				HostID: activeHost, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
			}); err != nil {
				t.Fatal(err)
			}
			now = now.Add(tc.since)

			var buf bytes.Buffer
			if err := fleetReport(ctx, md, &buf, testTTL); err != nil {
				t.Fatalf("fleetReport: %v", err)
			}
			out := buf.String()
			t.Log("\n" + out)

			mustSay(t, out, "LEADER  cp-b  term 1  elected 10m0s ago")
			mustSay(t, out, tc.want)
			// "renewed" was the word that made a dead process read as a live one. It
			// described a write nothing performs.
			if strings.Contains(out, "renewed") {
				t.Errorf("the report still says a term is renewed, and nothing renews one:\n%s", out)
			}
		})
	}
}

// countingLeaderReads is a metadata.Store that counts GetLeader. Embedding rather
// than implementing: the guard uses one method of a thirty-method interface, and a
// hand-written stub for the other twenty-nine would be a compile error every time
// somebody adds one.
type countingLeaderReads struct {
	metadata.Store
	n atomic.Int64
}

func (c *countingLeaderReads) GetLeader(ctx context.Context) (metadata.Leader, error) {
	c.n.Add(1)
	return c.Store.GetLeader(ctx)
}

// TestASupersededControlPlaneStopsRunning is the third state: a second Control Plane
// takes the term, and the first keeps listening, keeps accepting connections, and
// fails every mutation with `stale control-plane term` for ever. §7 says it detects
// the condition and terminates itself; it did not, because nothing ever looked.
func TestASupersededControlPlaneStopsRunning(t *testing.T) {
	ctx := t.Context()
	now := time.Unix(1_700_000_000, 0).UTC()
	md := metasim.New(func() time.Time { return now })
	counting := &countingLeaderReads{Store: md}

	term, err := md.AcquireLeadership(ctx, "cp-a")
	if err != nil {
		t.Fatal(err)
	}

	// A real clock at a millisecond cadence: the guard's own loop is what is under
	// test, and its unit is "did it look again", not "how long did it wait".
	clk := real.NewClock()

	// While cp-a still holds the term the guard must not end the process. Proved by
	// the loop still looking when the context is cancelled, not by it having returned
	// nothing — a guard that returned on its first tick would also "not exit".
	held, cancelHeld := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- watchTerm(held, counting, clk, term, "cp-a", time.Millisecond) }()
	for counting.n.Load() < 3 {
		if err := clk.Sleep(ctx, time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	cancelHeld()
	if err := <-done; err != nil {
		t.Fatalf("the guard ended a Control Plane that still holds its term: %v", err)
	}

	// Now a second Control Plane takes over, exactly as starting one does.
	if _, err := md.AcquireLeadership(ctx, "cp-b"); err != nil {
		t.Fatal(err)
	}
	// Bounded, so "it never exits" — the state that was reproduced — fails as an
	// assertion here rather than as the package's test timeout. Two seconds is two
	// thousand of the guard's intervals.
	superseded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err = watchTerm(superseded, counting, clk, term, "cp-a", time.Millisecond)
	if err == nil {
		t.Fatal("a superseded Control Plane kept running")
	}
	// The message is the operator's only account of why the process is gone, so it
	// names both terms and the holder that took over.
	for _, want := range []string{"cp-b", "term 2", "term 1"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the exit error does not say %q: %v", want, err)
		}
	}
}

// TestTermCheckIntervalStaysUnderTheLease: the guard's cadence is derived from
// -lease-ttl rather than being its own flag, because the lease is the fleet's unit of
// "how long a fact may be believed" — a superseded Control Plane has to be gone
// before the window it was still writing in can be trusted again.
func TestTermCheckIntervalStaysUnderTheLease(t *testing.T) {
	tests := []struct {
		ttl  time.Duration
		want time.Duration
	}{
		{30 * time.Second, 10 * time.Second},
		{3 * time.Second, time.Second},
		{time.Second, time.Second}, // the floor: never busier than once a second
		{0, time.Second},
	}
	for _, tc := range tests {
		if got := termCheckInterval(tc.ttl); got != tc.want {
			t.Errorf("termCheckInterval(%s) = %s, want %s", tc.ttl, got, tc.want)
		}
		if got := termCheckInterval(tc.ttl); tc.ttl > time.Second && got >= tc.ttl {
			t.Errorf("termCheckInterval(%s) = %s, which is not inside the lease", tc.ttl, got)
		}
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
