package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
)

// Fixed UUIDv7s (INV-22), so the report's rows can be asserted by name.
const (
	activeHost   = "00000000-0000-7000-8000-0000000000a1"
	cordonedHost = "00000000-0000-7000-8000-0000000000a2"
	servedVol    = "00000000-0000-7000-8000-0000000000b1"
	strandedVol  = "00000000-0000-7000-8000-0000000000b2"
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

	now = now.Add(5 * time.Second)
	var buf bytes.Buffer
	if err := fleetReport(ctx, md, &buf); err != nil {
		t.Fatalf("fleetReport: %v", err)
	}
	out := buf.String()
	t.Log("\n" + out)

	mustSay(t, out, "as of 2023-11-14T22:13:20Z")
	mustSay(t, out, "LEADER  cp-a  term 1")

	// The cordoned host: an operator has to be able to read *why* it is out of service
	// and how full it is, or the row says only that something is wrong.
	mustSay(t, out, "HOSTS (2, 1 not taking placements)")
	if got, want := row(t, out, cordonedHost),
		[]string{cordonedHost, "CORDONED", "DEVICE_PRESSURE", "900.0GiB", "(88%)", "1.0TiB", "0B", "1m30s"}; !equal(got, want) {
		t.Errorf("cordoned host row = %v, want %v", got, want)
	}
	if got, want := row(t, out, activeHost),
		[]string{activeHost, "ACTIVE", "-", "100.0GiB", "(10%)", "1.0TiB", "1.0GiB", "5s"}; !equal(got, want) {
		t.Errorf("active host row = %v, want %v", got, want)
	}

	// The volume nobody serves, and the count in the header that answers the question
	// on its own.
	mustSay(t, out, "VOLUMES (2, 1 with no primary host)")
	if got, want := row(t, out, strandedVol),
		[]string{strandedVol, "-", "DETACHED", "9", "2.0GiB", "-"}; !equal(got, want) {
		t.Errorf("unplaced volume row = %v, want %v", got, want)
	}
	if got, want := row(t, out, servedVol),
		[]string{servedVol, activeHost, "ACTIVE", "3", "1.0GiB", "-"}; !equal(got, want) {
		t.Errorf("served volume row = %v, want %v", got, want)
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
	if strings.Contains(out, doneSnap) {
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
	if err := fleetReport(t.Context(), md, &buf); err != nil {
		t.Fatalf("fleetReport with no leader: %v", err)
	}
	out := buf.String()
	t.Log("\n" + out)

	mustSay(t, out, "LEADER  none")
	for _, section := range []string{
		"HOSTS (0, 0 not taking placements)",
		"VOLUMES (0, 0 with no primary host)",
		"SNAPSHOTS NOT FINISHED (0)",
	} {
		mustSay(t, out, section)
	}
	if n := strings.Count(out, "(none)"); n != 3 {
		t.Errorf("empty sections marked with (none): %d, want 3:\n%s", n, out)
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
