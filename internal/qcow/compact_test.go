package qcow_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/qcow"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// A chain of four: three published layers and the tip the guest writes to. The sizes are
// distinct so that a plan which collapsed the wrong set could not add up to the same
// number by accident.
const (
	oldestLayer = "0198c0de-0000-7000-8000-000000000a11"
	middleLayer = "0198c0de-0000-7000-8000-000000000a22"
	newestLayer = "0198c0de-0000-7000-8000-000000000a33"
	oldestBytes = 100 << 20
	middleBytes = 200 << 20
	newestBytes = 300 << 20
	tipBytes    = 50 << 20
	// readBytes is what a convert would read: the three published layers, and not the
	// tip.
	readBytes = oldestBytes + middleBytes + newestBytes
	// diskBytes is every layer file of this volume, tip included — §21's
	// local_disk_bytes, which is about the disk and not about the chain.
	diskBytes = readBytes + tipBytes
)

// deepChain is a host holding a four-layer chain for one volume, with nothing attached at
// the QMP socket. It returns the Manager, the disk, the qemu-img it drives and the log
// everything it says goes to.
func deepChain(t *testing.T, policy qcow.CompactionPolicy) (*qcow.Manager, *fakePaths, *fakeRunner, *bytes.Buffer) {
	t.Helper()

	tip := qcow.LayerImage(root, vol, layerID)
	p := newPathsAt(tip)
	for layer, size := range map[string]int64{
		oldestLayer: oldestBytes, middleLayer: middleBytes, newestLayer: newestBytes, layerID: tipBytes,
	} {
		image := qcow.LayerImage(root, vol, layer)
		p.present[image] = true
		p.sizes[image] = size
	}
	if err := qcow.WriteState(p, root, vol, qcow.State{Commits: []qcow.CommitLayer{
		{CommitID: "0198c0de-0000-7000-8000-0000000c0011", LayerID: oldestLayer},
		{CommitID: "0198c0de-0000-7000-8000-0000000c0022", LayerID: middleLayer},
		{CommitID: "0198c0de-0000-7000-8000-0000000c0033", LayerID: newestLayer},
	}}); err != nil {
		t.Fatalf("writing the state of a volume with three published commits: %v", err)
	}

	r := &fakeRunner{info: infoJSON("qcow2", size, false), version: "qemu-img version 11.1.1"}

	var out bytes.Buffer
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(restore) })

	m, err := qcow.New(t.Context(), qcow.Config{
		Root: root, QemuImg: "/qemu-img", ProbeTimeout: time.Second, Compaction: policy,
	}, qcow.Deps{
		Clock:    sim.NewClock(time.Unix(1_700_000_000, 0)),
		Disk:     sim.NewDisk(),
		Runner:   r,
		Paths:    p,
		Dialer:   &fakeDialer{scripts: map[string][]string{}},
		Recovery: bornEmpty(),
	})
	if err != nil {
		t.Fatalf("building a manager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m, p, r, &out
}

// compactionLine is the one line this whole feature produces, or "" when there is none.
func compactionLine(t *testing.T, out *bytes.Buffer) string {
	t.Helper()
	for _, line := range strings.Split(out.String(), "\n") {
		if strings.Contains(line, "compaction threshold") {
			return line
		}
	}
	return ""
}

// TestAChainPastItsCompactionThresholdIsReported is v6 §19's policy and plan, and the
// assertion is deliberately on the log line: nothing here compacts anything, so the
// report *is* the feature. An operator has to be able to read which layers would be
// collapsed and what the collapse would cost before the chain becomes an incident.
func TestAChainPastItsCompactionThresholdIsReported(t *testing.T) {
	m, p, r, out := deepChain(t, qcow.CompactionPolicy{AtLayers: 4})

	if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("applying: %v", err)
	}

	line := compactionLine(t, out)
	if line == "" {
		t.Fatalf("a four-layer chain with a threshold of four said nothing:\n%s", out.String())
	}
	for _, want := range []string{
		"chain_depth=4",
		fmt.Sprintf("local_disk_bytes=%d", diskBytes),
		"trigger=chain_depth",
		"collapse_layers=3",
		"collapse_oldest=" + oldestLayer,
		"collapse_newest=" + newestLayer,
		"into_commit_id=0198c0de-0000-7000-8000-0000000c0033",
		// What a convert would read is the three published layers and not the tip; what
		// it would write cannot exceed the guest's disk, which here is the smaller of the
		// two.
		fmt.Sprintf("read_bytes=%d", readBytes),
		fmt.Sprintf("write_bytes_at_most=%d", size),
		fmt.Sprintf("virtual_size=%d", size),
	} {
		if !strings.Contains(line, want) {
			t.Errorf("the report does not carry %q:\n%s", want, line)
		}
	}
	// The act is a human's. A plan that ran the convert or unlinked a layer would be the
	// thing this whole increment refuses to be.
	if len(p.removed) != 0 {
		t.Errorf("planning a compaction removed files: %v", p.removed)
	}
	// On what qemu-img was actually asked to run, and not on the log's wording: a plan
	// that ran the convert would print nothing about it, and an assertion that greps the
	// log for the word passes either way.
	for _, cmd := range r.commands() {
		if strings.Contains(cmd, "convert") {
			t.Errorf("planning a compaction ran a convert: %v", r.commands())
		}
	}
}

func TestWhenAChainIsDueForCompaction(t *testing.T) {
	tests := []struct {
		name    string
		policy  qcow.CompactionPolicy
		trigger string
	}{
		{
			// The zero policy is what every caller has until somebody measures one, and
			// it must be silent rather than guess at v6 §19's example numbers.
			name:   "no policy at all",
			policy: qcow.CompactionPolicy{},
		},
		{
			name:   "a chain shallower than the threshold",
			policy: qcow.CompactionPolicy{AtLayers: 5},
		},
		{
			name:    "deep enough",
			policy:  qcow.CompactionPolicy{AtLayers: 4},
			trigger: "trigger=chain_depth",
		},
		{
			name:   "the incremental layers are under the size threshold",
			policy: qcow.CompactionPolicy{AtBytes: readBytes + 1},
		},
		{
			// The size arm measures what a convert would read, not what the volume
			// occupies: the tip is not collapsed, so its bytes are not the decision.
			name:    "the incremental layers are exactly at the size threshold",
			policy:  qcow.CompactionPolicy{AtBytes: readBytes},
			trigger: "trigger=incremental_bytes",
		},
		{
			name:    "both",
			policy:  qcow.CompactionPolicy{AtLayers: 2, AtBytes: 1},
			trigger: "trigger=chain_depth+incremental_bytes",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, _, _, out := deepChain(t, tt.policy)
			if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
				t.Fatalf("applying: %v", err)
			}
			line := compactionLine(t, out)
			switch {
			case tt.trigger == "" && line != "":
				t.Fatalf("a chain that is not due was reported: %s", line)
			case tt.trigger == "":
				return
			case line == "":
				t.Fatalf("a chain that is due said nothing:\n%s", out.String())
			case !strings.Contains(line, tt.trigger):
				t.Errorf("want %q in the report: %s", tt.trigger, line)
			}
		})
	}
}

// TestACompactionWarningIsNotRepeatedEveryCycle. The condition stays true until a human
// acts on it, and the reconciliation loop runs this every heartbeat: a line per cycle is
// how an operator learns to filter out the one message this feature exists to send.
func TestACompactionWarningIsNotRepeatedEveryCycle(t *testing.T) {
	m, _, _, out := deepChain(t, qcow.CompactionPolicy{AtLayers: 4})

	for range 3 {
		if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
			t.Fatalf("applying: %v", err)
		}
	}
	if n := strings.Count(out.String(), "compaction threshold"); n != 1 {
		t.Fatalf("three cycles produced %d compaction warnings, want 1:\n%s", n, out.String())
	}
}

// TestAPlanThatCannotBeMeasuredIsNotReported. The numbers are what the report is for, so
// a layer that cannot be stat'd produces an error and no line — rather than a plan with a
// hole in its arithmetic, which is the shape an operator would act on.
func TestAPlanThatCannotBeMeasuredIsNotReported(t *testing.T) {
	m, p, _, out := deepChain(t, qcow.CompactionPolicy{AtLayers: 4})
	p.remove(qcow.LayerImage(root, vol, middleLayer))

	err := m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)})
	if err == nil || !strings.Contains(err.Error(), middleLayer) {
		t.Fatalf("a chain with a layer that could not be measured: want an error naming %s, got %v", middleLayer, err)
	}
	if line := compactionLine(t, out); line != "" {
		t.Errorf("a plan was reported with a layer nobody could measure: %s", line)
	}
}

func TestACompactionPolicyThatCannotMeanAnythingIsRefused(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		policy qcow.CompactionPolicy
		want   string
	}{
		{
			// Every volume has one layer, so a threshold of one calls every volume due
			// for a collapse of nothing.
			name:   "one layer",
			policy: qcow.CompactionPolicy{AtLayers: 1},
			want:   "below the one layer every volume has",
		},
		{name: "negative layers", policy: qcow.CompactionPolicy{AtLayers: -1}, want: "cannot be negative"},
		{name: "negative bytes", policy: qcow.CompactionPolicy{AtBytes: -1}, want: "cannot be negative"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := qcow.Config{
				Root: root, QemuImg: "/qemu-img", ProbeTimeout: time.Second, Compaction: tt.policy,
			}.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validating %+v: want an error saying %q, got %v", tt.policy, tt.want, err)
			}
		})
	}
}

// TestACompactionWarningIsRepeatedWhenTheChainDeepens is the other half of the latch. A
// condition that stays true is said once; a condition that gets *worse* is said again,
// because the number an operator acts on is the depth and a chain that went from four
// layers to five while nobody acted is the news. Latching on "said it once" instead
// leaves the volume silent for the rest of the process's life.
func TestACompactionWarningIsRepeatedWhenTheChainDeepens(t *testing.T) {
	m, p, _, out := deepChain(t, qcow.CompactionPolicy{AtLayers: 4})
	if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("applying: %v", err)
	}

	// A fourth commit lands: one more layer under the tip, one more layer on the disk.
	const fourthLayer = "0198c0de-0000-7000-8000-000000000a44"
	image := qcow.LayerImage(root, vol, fourthLayer)
	p.present[image] = true
	p.sizes[image] = 400 << 20
	st, err := qcow.ReadState(p, root, vol)
	if err != nil {
		t.Fatalf("reading the state back: %v", err)
	}
	st.Commits = append(st.Commits, qcow.CommitLayer{
		CommitID: "0198c0de-0000-7000-8000-0000000c0044", LayerID: fourthLayer,
	})
	if err := qcow.WriteState(p, root, vol, st); err != nil {
		t.Fatalf("recording a fourth commit: %v", err)
	}

	if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("applying after the chain deepened: %v", err)
	}
	if n := strings.Count(out.String(), "chain_depth=5"); n != 1 {
		t.Fatalf("a chain that grew from four layers to five reported depth 5 %d times, want 1:\n%s", n, out.String())
	}
}

// TestADueChainWithNothingToCollapseIsSilent. Depth counts the tip and the sealed layer
// this host has not published; neither is something a convert may touch — the tip is
// under a guest and a sealed layer is not a commit — so a chain can be past its threshold
// with nothing at all to collapse. The report is built out of the collapse set, and one
// with no layers in it is both a warning nobody can act on and, if the plan is built
// anyway, an index into an empty slice.
func TestADueChainWithNothingToCollapseIsSilent(t *testing.T) {
	m, p, _, out := deepChain(t, qcow.CompactionPolicy{AtLayers: 2})
	// No commits at all, and one layer sealed under the tip: depth 2, collapse nothing.
	if err := qcow.WriteState(p, root, vol, qcow.State{
		Layers: []string{layerID, oldestLayer},
	}); err != nil {
		t.Fatalf("writing the state of a volume that has never published: %v", err)
	}

	if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("applying: %v", err)
	}
	if line := compactionLine(t, out); line != "" {
		t.Fatalf("a chain whose collapse set is empty was reported: %s", line)
	}
}

// TestLocalDiskBytesCountsALayerNoRecordNames. local_disk_bytes is what the volume
// occupies on this host, so it is the directory and not the record: a layer that survived
// the sweep because it was minted before the tip is space an operator is paying for, and
// a number derived from the chain would leave it out of the one report that is about
// running out of disk.
func TestLocalDiskBytesCountsALayerNoRecordNames(t *testing.T) {
	m, p, _, out := deepChain(t, qcow.CompactionPolicy{AtLayers: 4})
	const orphanBytes = 7 << 20
	orphan := qcow.LayerImage(root, vol, "0198c0de-0000-7000-8000-0000000000ff")
	p.present[orphan] = true
	p.sizes[orphan] = orphanBytes

	if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{active(vol, 1)}); err != nil {
		t.Fatalf("applying: %v", err)
	}
	line := compactionLine(t, out)
	if want := fmt.Sprintf("local_disk_bytes=%d", diskBytes+orphanBytes); !strings.Contains(line, want) {
		t.Fatalf("the report does not carry %q, so a layer no record names is invisible:\n%s", want, line)
	}
	// And it is still not in the plan: a convert collapses the chain, not whatever else
	// is in the directory.
	if !strings.Contains(line, "collapse_layers=3") {
		t.Errorf("a layer no record names reached the collapse set:\n%s", line)
	}
}
