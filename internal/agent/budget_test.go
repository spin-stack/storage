package agent_test

import (
	"context"
	"strings"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// TestAnAgentWithNoDeviceBudgetDoesNotStart. A zero budget is not "unbounded by
// choice", it is the state every Agent this repository has ever run was in: nothing set
// wal.Limits, so no production WAL had a write-path bound of any kind and the first
// thing a filling device did was ENOSPC in the middle of a guest's WRITE.
//
// The evidence is not only the error. A manager that refused and had already claimed
// the data directory would leave a lock file behind and the next Agent could not start
// — so the assertion is on the directory, which is what an operator (and the retry)
// sees.
func TestAnAgentWithNoDeviceBudgetDoesNotStart(t *testing.T) {
	t.Parallel()
	d := sim.NewDisk()
	f := newListenerFactory()
	_, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin", SocketDir: "/run/spin",
	}, agent.VolumeManagerDeps{
		Clock:   sim.NewClock(time.Unix(1_700_000_000, 0).UTC()),
		Disk:    d,
		Listen:  f.listen,
		Mapper:  unusedMapper{},
		EventFD: unusedEventFD,
	})
	if err == nil {
		t.Fatal("an Agent with no device budget started; nothing would bound its guests' writes (ADR-0013 §1)")
	}
	if !strings.Contains(err.Error(), "device budget") {
		t.Fatalf("the refusal does not say what is missing: %v", err)
	}
	names, err := d.List("/var/lib/spin/")
	if err != nil {
		t.Fatalf("listing the data directory: %v", err)
	}
	if len(names) != 0 {
		t.Fatalf("the refused Agent left %v in the data directory", names)
	}
}

// TestABudgetIsADividedDevice pins the arithmetic every host's write path now rests
// on: the device is measured rather than declared, the guests get a share of it that
// leaves the reserve untouched, and one volume gets a share of *that* rather than the
// whole budget.
func TestABudgetIsADividedDevice(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		total      int64
		maxVolumes int
		wantErr    bool
	}{
		{name: "a device that never answered statfs is refused", total: 0, maxVolumes: 4, wantErr: true},
		{name: "a device measured as negative is refused", total: -1, maxVolumes: 4, wantErr: true},
		{name: "no fan-out to divide by is refused", total: 1 << 40, maxVolumes: 0, wantErr: true},
		{name: "a device too small for its fan-out is refused", total: 100, maxVolumes: 1 << 20, wantErr: true},
		{name: "a 1 TiB NVMe with the default fan-out", total: 1 << 40, maxVolumes: agent.DefaultMaxVolumes},
		{name: "a small dev-box partition", total: 4 << 30, maxVolumes: 4},
		{name: "one big volume on the whole device", total: 1 << 40, maxVolumes: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b, err := agent.NewBudget(disk.Usage{TotalBytes: tc.total}, tc.maxVolumes)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("a %d-byte device divided by %d was accepted: %+v", tc.total, tc.maxVolumes, b)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewBudget: %v", err)
			}
			switch {
			case b.DeviceBytes != tc.total:
				t.Fatalf("the budget describes a %d-byte device, statfs said %d", b.DeviceBytes, tc.total)
			case b.ReserveBytes <= 0:
				t.Fatalf("a %d-byte device reserved %d bytes: the reserve is not headroom", tc.total, b.ReserveBytes)
			case b.GuestBytes+b.ReserveBytes > tc.total:
				t.Fatalf("guests may hold %d and %d is reserved, on a %d-byte device",
					b.GuestBytes, b.ReserveBytes, tc.total)
			case b.Share() <= 0:
				t.Fatalf("each of %d volumes gets %d bytes", tc.maxVolumes, b.Share())
			case b.Share()*int64(tc.maxVolumes) > b.GuestBytes:
				// The property the whole type exists for: N volumes at their share do
				// not exceed what the device set aside for them.
				t.Fatalf("%d volumes at %d bytes each exceed the %d-byte guest budget",
					tc.maxVolumes, b.Share(), b.GuestBytes)
			}

			// The Log's bound is the share, and the segment is a fraction of it —
			// one segment is the least a truncation can give back, so a log whose
			// segment is its whole share could never return anything.
			limits := b.Limits()
			if limits.MaxLocalBytes != b.Share() {
				t.Fatalf("a volume's log is bounded at %d, its share is %d", limits.MaxLocalBytes, b.Share())
			}
			if limits.SegmentBytes <= 0 || limits.SegmentBytes > b.Share()/2 {
				t.Fatalf("segments of %d bytes inside a %d-byte share", limits.SegmentBytes, b.Share())
			}
		})
	}
}

// TestAHostServesNoMoreVolumesThanItsBudgetWasDividedBy. The share only bounds the
// device if the number of shares is bounded too: MaxVolumes+1 volumes each holding
// GuestBytes/MaxVolumes is a host that can hold more than its budget, which is the
// unsummed backlog ADR-0013 §1 exists for arriving through the only door left.
//
// Refusing an attach is a local, defensive power (ADR-0013 §5) and this does not touch
// what the Control Plane decides: the volume stays in the desired state and Apply
// retries it every cycle.
func TestAHostServesNoMoreVolumesThanItsBudgetWasDividedBy(t *testing.T) {
	t.Parallel()
	f := newListenerFactory()
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin", SocketDir: "/run/spin",
		Budget: agent.Budget{DeviceBytes: 8 << 20, ReserveBytes: 1 << 20, GuestBytes: 4 << 20, MaxVolumes: 2},
	}, agent.VolumeManagerDeps{
		Clock:   sim.NewClock(time.Unix(1_700_000_000, 0).UTC()),
		Disk:    sim.NewDisk(),
		Listen:  f.listen,
		Mapper:  unusedMapper{},
		EventFD: unusedEventFD,
	})
	if err != nil {
		t.Fatalf("NewVolumeManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close(context.Background()) }) //nolint:usetesting // see newTestManager

	desired := []*storagev1.DesiredVolume{desiredVolume(t, 1), desiredVolume(t, 1), desiredVolume(t, 1)}
	if err := m.Apply(t.Context(), desired); err == nil {
		t.Fatal("a host whose budget is divided by 2 accepted a third volume with no complaint")
	}

	// What matters is not the error but which volumes exist: two served, one not, and
	// the two that are serving are the ones that were admitted first.
	var served int
	for _, d := range desired {
		if _, ok := m.Device(d.GetVolumeId()); ok {
			served++
		}
	}
	if served != 2 {
		t.Fatalf("the host serves %d volumes; its device budget was divided into 2 shares", served)
	}
	if _, ok := m.Device(desired[2].GetVolumeId()); ok {
		t.Fatal("the volume past the fan-out is being served: its writes are bounded by a share nobody counted")
	}
}
