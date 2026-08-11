package agent_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/ids"
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
			// Empty: these rows are about dividing a device, and a device that is
			// already occupied is refused before the arithmetic runs (see
			// TestADeviceThatCannotHoldTheReserveIsRefusedRatherThanDivided).
			b, err := agent.NewBudget(disk.Usage{TotalBytes: tc.total, AvailBytes: tc.total}, testMemory, tc.maxVolumes)
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

// occupy puts n bytes on d under name, and is how these tests produce a device that is
// already full when the Agent starts. The bytes go through the disk rather than into a
// disk.Usage literal on purpose: what NewBudget is handed in production is a
// measurement (real.Disk.Usage, a statfs), so a test that hands it a struct it wrote
// itself can agree with a derivation that is wrong about real devices.
func occupy(t *testing.T, d *sim.Disk, name string, n int64) {
	t.Helper()
	f, err := d.Create(name)
	if err != nil {
		t.Fatalf("creating %s: %v", name, err)
	}
	defer func() { _ = f.Close() }()
	if written, err := f.Append(make([]byte, n)); err != nil || int64(written) != n {
		t.Fatalf("occupying %d bytes of the device: wrote %d, %v", n, written, err)
	}
}

// occupiedDevice is the device the two tests below start their Agent on: small enough
// to fill with real bytes, large enough that its share survives the fan-out.
const occupiedDevice = 64 << 20

// TestADeviceThatCannotHoldTheReserveIsRefusedRatherThanDivided.
//
// NewBudget read TotalBytes and nothing else, so an Agent brought up on a filesystem
// another tenant had already filled divided it exactly as it would divide an empty one:
// sixteen shares of a device with nothing in it. The guests discovered the difference as
// ENOSPC — a partial append, after which every write on that volume fails — and the
// publish at stop discovered it as an fdatasync that could not allocate, which is a whole
// session lost. The reserve is what is supposed to stop the second one, and a reserve
// computed as a fraction of a capacity that belongs to somebody else is not headroom, it
// is arithmetic.
//
// The rows either side of the refusal are the point of it: statfs reports one number for
// "occupied" and cannot say whose bytes those are, so a device that is half full is still
// divided (see NewBudget for that residual, and for why it is not closed by guessing).
func TestADeviceThatCannotHoldTheReserveIsRefusedRatherThanDivided(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		occupy  int64
		wantErr bool
	}{
		{name: "a dedicated, empty device is divided", occupy: 0},
		{
			// Not a refusal, deliberately: these bytes are indistinguishable from this
			// Agent's own unpublished WAL, and refusing them refuses every restart.
			name:   "a device half occupied is still divided, because nothing here can say whose bytes those are",
			occupy: occupiedDevice / 2,
		},
		{
			// 3 MiB free against a 3.2 MiB reserve: whoever those bytes belong to,
			// the reserve does not fit behind them.
			name:   "a device with less free than the reserve it would promise is refused",
			occupy: occupiedDevice - 3<<20, wantErr: true,
		},
		{name: "a device with nothing free at all is refused", occupy: occupiedDevice, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d := sim.NewDisk()
			d.SetDeviceBudget(occupiedDevice)
			if tc.occupy > 0 {
				occupy(t, d, "another-tenant/blob", tc.occupy)
			}
			u, err := d.Usage()
			if err != nil {
				t.Fatalf("measuring the device: %v", err)
			}
			b, err := agent.NewBudget(u, testMemory, 4)
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("NewBudget on a device with %d bytes free: %v", u.AvailBytes, err)
				}
				// The division is of the device, not of what is free: see
				// TestARestartIsNotDividedByWhatItsOwnLastSessionLeftFree.
				if b.GuestBytes != int64(agent.GuestRatio*float64(u.TotalBytes))-b.ReserveBytes {
					t.Fatalf("a %d-byte device with %d occupied gave its guests %d bytes",
						occupiedDevice, tc.occupy, b.GuestBytes)
				}
				return
			}
			if err == nil {
				t.Fatalf("a device with %d bytes free of %d was divided into 4 shares of %d, keeping a %d-byte reserve that does not exist",
					u.AvailBytes, u.TotalBytes, b.Share(), b.ReserveBytes)
			}
			// The operator's half: the sentence has to carry the numbers there is an
			// action for, the way the socket-path refusal does. What is free and what
			// the reserve needed are the two that decide whether to clear the disk or
			// give this Agent one of its own.
			reserve := int64(agent.ReserveRatio * float64(u.TotalBytes))
			for _, want := range []string{
				fmt.Sprint(u.AvailBytes), fmt.Sprint(u.TotalBytes), fmt.Sprint(reserve),
			} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("the refusal does not name %s, so nothing in it says how full the device is: %v", want, err)
				}
			}
		})
	}
}

// TestARestartIsNotDividedByWhatItsOwnLastSessionLeftFree is the other half of the
// decision, and the reason the check above is a precondition rather than a smaller share.
//
// A session's WAL stays on the device for the whole session and is given back at the
// *next* attach, when the published image is installed as the log's base — so at the one
// moment this derivation runs, an Agent that is restarting is looking at a device its own
// last session has filled, minutes before that space comes back. An Agent that divided
// what was free at that instant would hand a resumed volume a share smaller than the log
// it is about to resume, and the bound is on a log's whole footprint rather than on new
// writes: every guest on the host would take an I/O error on a device that is 80% free.
//
// So the assertion is that nothing moves. Same device, same fan-out, four fifths of it
// occupied by this Agent's own WAL, and every number the guests are bounded by is the one
// the empty device produced.
func TestARestartIsNotDividedByWhatItsOwnLastSessionLeftFree(t *testing.T) {
	t.Parallel()
	empty := sim.NewDisk()
	empty.SetDeviceBudget(occupiedDevice)
	fresh, err := empty.Usage()
	if err != nil {
		t.Fatalf("measuring the empty device: %v", err)
	}
	first, err := agent.NewBudget(fresh, testMemory, 4)
	if err != nil {
		t.Fatalf("NewBudget on the empty device: %v", err)
	}

	// What the previous session left: four volumes' WAL, inside the budget above, still
	// on the device because nothing has attached yet to install a base over it.
	d := sim.NewDisk()
	d.SetDeviceBudget(occupiedDevice)
	for i := range 4 {
		occupy(t, d, fmt.Sprintf("wal/%s/1/%06d.seg", ids.New(), i), first.Share())
	}
	u, err := d.Usage()
	if err != nil {
		t.Fatalf("measuring the device the restart sees: %v", err)
	}
	if u.UsedBytes*10 < u.TotalBytes*7 {
		t.Fatalf("this test needs a device its own WAL has mostly filled; %d of %d is occupied", u.UsedBytes, u.TotalBytes)
	}

	restarted, err := agent.NewBudget(u, testMemory, 4)
	if err != nil {
		t.Fatalf("an Agent restarting on a device holding %d bytes of its own WAL would not start: %v", u.UsedBytes, err)
	}
	if restarted != first {
		t.Fatalf("a restart on a device holding its own WAL was divided differently: %+v, want %+v", restarted, first)
	}
	// The number a resumed log is actually bounded by, and the one that would put it
	// into backpressure retroactively if this moved.
	if restarted.Limits().MaxLocalBytes != first.Limits().MaxLocalBytes {
		t.Fatalf("a resumed volume's log is bounded at %d bytes; on the empty device the same volume got %d",
			restarted.Limits().MaxLocalBytes, first.Limits().MaxLocalBytes)
	}
}

// testMemory is the machine the device-budget cases above are divided on. It is a
// separate axis from the device — a host can have a big disk and little RAM, and the
// point of the table above is the disk — so it is held fixed there and driven on its own
// below.
const testMemory = 32 << 30

// TestTheReadViewBoundIsOneVolumesShareOfTheMemoryThisAgentHas is the counterpart of
// TestABudgetIsADividedDevice for the one per-volume structure the *guest* sizes.
//
// Before this, `wal.Limits.MaxViewBytes` came from a constant somebody chose: 256 MiB,
// on every machine, so 16 volumes on an 8 GiB host were entitled to 8.5 GiB of read view
// in RSS and the OOM killer was the only thing that would notice. A bound that is not
// derived from the machine is wrong on every machine but the one it was tried on.
//
// The cases are the machines that break a constant: the one it was chosen for, a
// container two orders of magnitude smaller than its host, and a host too small to be
// divided at all.
func TestTheReadViewBoundIsOneVolumesShareOfTheMemoryThisAgentHas(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		memory     int64
		maxVolumes int
		want       int64
		wantErr    bool
	}{
		{
			// The number the deleted constant was chosen for, reproduced by the
			// derivation on the machine it was chosen on. That is the evidence that
			// the ratios are the old judgement rewritten as a division, not a new
			// guess: 32 GiB × 0.25 ÷ 16 ÷ 2 = 256 MiB.
			name:   "the 32 GiB host the old constant was sized for gets the old constant",
			memory: 32 << 30, maxVolumes: agent.DefaultMaxVolumes, want: 256 << 20,
		},
		{
			// The case that matters most, and the one no constant can serve: an Agent
			// in a 2 GiB container. 256 MiB × 16 volumes × 2 (the RSS factor) is
			// 8 GiB of read view inside a 2 GiB limit — a 4x overcommit of a limit
			// whose enforcement is the OOM killer taking the whole process.
			name:   "a 2 GiB container sizes itself from the container",
			memory: 2 << 30, maxVolumes: agent.DefaultMaxVolumes, want: 16 << 20,
		},
		{
			// The same container with the fan-out an operator lowers to fit it: the
			// remedy the refusal below names, and it has to actually work.
			name:   "lowering the fan-out is what buys a volume a bigger view",
			memory: 2 << 30, maxVolumes: 2, want: 128 << 20,
		},
		{
			name:   "a 512 GiB host is not held to a 256 MiB constant",
			memory: 512 << 30, maxVolumes: agent.DefaultMaxVolumes, want: 4 << 30,
		},
		{
			// Not a limit, not a default: an Agent that could not measure its memory
			// has nothing to divide, and a Budget that quietly filled in a number
			// would be the constant this test exists to remove, wearing a fallback's
			// clothes.
			name:   "a machine whose memory was never measured is refused",
			memory: 0, maxVolumes: agent.DefaultMaxVolumes, wantErr: true,
		},
		{
			name:   "a negative measurement is refused",
			memory: -1, maxVolumes: 4, wantErr: true,
		},
		{
			// The only threshold that is not a matter of taste: a share under what one
			// extent's structure costs (cow.ExtentOverheadBytes) admits no write at
			// all, so the volume takes ErrViewBound on its first WRITE and — nothing
			// but DISCARD shrinks a view — never gets out of it. 8 KiB × 0.25 ÷ 16 ÷ 2
			// is 64 bytes. See NewBudget for why nothing between here and comfortable
			// is refused, and why nothing is floored.
			name:   "a machine that cannot give a volume a single extent is refused",
			memory: 8 << 10, maxVolumes: agent.DefaultMaxVolumes, wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b, err := agent.NewBudget(disk.Usage{TotalBytes: 1 << 40, AvailBytes: 1 << 40}, tc.memory, tc.maxVolumes)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("a %d-byte machine divided by %d was accepted, with a %d-byte view bound per volume",
						tc.memory, tc.maxVolumes, b.ViewShare())
				}
				return
			}
			if err != nil {
				t.Fatalf("NewBudget: %v", err)
			}
			if b.MemoryBytes != tc.memory {
				t.Fatalf("the budget describes a %d-byte machine, the measurement said %d", b.MemoryBytes, tc.memory)
			}
			if b.ViewShare() != tc.want {
				t.Fatalf("one volume's read view is bounded at %d bytes on a %d-byte machine serving %d volumes, want %d",
					b.ViewShare(), tc.memory, tc.maxVolumes, tc.want)
			}
			// The property the division exists for, and the one a per-volume constant
			// cannot have: every volume at its bound, at the measured RSS factor, is
			// still inside the fraction of the machine set aside for read views. A
			// bound that only satisfies "some bound exists" passes without it.
			if rss := b.ViewShare() * int64(tc.maxVolumes) * agent.ViewRSSFactor; rss > int64(agent.ViewRatio*float64(tc.memory)) {
				t.Fatalf("%d volumes at %d bytes cost %d bytes of RSS on a %d-byte machine: the machine is not divided",
					tc.maxVolumes, b.ViewShare(), rss, tc.memory)
			}
			// The bound reaches the Log, which is the only place it does anything.
			if got := b.Limits().MaxViewBytes; got != b.ViewShare() {
				t.Fatalf("the Log is handed a %d-byte view bound; the volume's share is %d", got, b.ViewShare())
			}
		})
	}
}

// TestTheViewShareDoesNotMoveWhenVolumesArrive. The share is one volume's slice of the
// fan-out, not of the volumes attached right now, for the reason the device share is
// static: a bound that shrank when a second volume attached would put a guest that was
// writing happily into backpressure because a *different* volume arrived on the host,
// and a Log documents its limits as fixed for its life.
//
// The evidence is the manager's, not the type's: a Budget is a value and cannot change,
// so the way this property gets lost is a manager that recomputes. Attach volumes, and
// ask what a Log built after them is bounded by.
func TestTheViewShareDoesNotMoveWhenVolumesArrive(t *testing.T) {
	t.Parallel()
	b, err := agent.NewBudget(disk.Usage{TotalBytes: 1 << 40, AvailBytes: 1 << 40}, 32<<30, 4)
	if err != nil {
		t.Fatalf("NewBudget: %v", err)
	}
	before := b.Limits().MaxViewBytes

	f := newListenerFactory()
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin", SocketDir: "/run/spin", Budget: b,
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

	if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{desiredVolume(t, 1), desiredVolume(t, 1)}); err != nil {
		t.Fatalf("attaching two volumes: %v", err)
	}
	if after := b.Limits().MaxViewBytes; after != before {
		t.Fatalf("a volume's read-view bound moved from %d to %d because other volumes attached", before, after)
	}
	if before != 1<<30 {
		t.Fatalf("each of 4 volumes on a 32 GiB machine is bounded at %d bytes, want 1 GiB", before)
	}
}

// TestAGuestIsRefusedAtTheViewBoundItsAgentDerived is the assertion that makes the
// derivation load-bearing rather than printed.
//
// Everything above is arithmetic on a value type, and arithmetic is satisfied by an
// Agent that computes the share, logs it, and hands its Logs a constant anyway — which
// is precisely the failure this repository keeps finding: a well-tested number no caller
// uses. So this one goes through the production path end to end (NewBudget →
// VolumeManager → the volume's Log) and asserts on what the *guest* gets back: the
// offset at which its writes start failing, and the sentence it fails with.
//
// The machine is 256 MiB across 4 volumes, which derives an 8 MiB view — deliberately
// far below wal.DefaultMaxViewBytes, so an Agent still holding the old constant would
// take every write this test issues and never refuse one.
func TestAGuestIsRefusedAtTheViewBoundItsAgentDerived(t *testing.T) {
	t.Parallel()
	const (
		machine   = 256 << 20
		volumes   = 4
		blockSize = 4096
		// Bigger than the derived view, so the bound is reached before the device is.
		volumeSize = 64 << 20
	)
	b, err := agent.NewBudget(disk.Usage{TotalBytes: 1 << 40, AvailBytes: 1 << 40}, machine, volumes)
	if err != nil {
		t.Fatalf("NewBudget: %v", err)
	}
	if b.ViewShare() != 8<<20 {
		t.Fatalf("this test is built on an 8 MiB view share; the derivation gives %d", b.ViewShare())
	}

	f := newListenerFactory()
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin", SocketDir: "/run/spin", Budget: b,
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

	vol := &storagev1.DesiredVolume{
		VolumeId: ids.New().String(), SizeBytes: volumeSize, BlockSize: blockSize,
		Epoch: 1, State: storagev1.VolumeState_VOLUME_STATE_ACTIVE,
	}
	if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{vol}); err != nil {
		t.Fatalf("attaching the volume: %v", err)
	}
	dev, ok := m.Device(vol.GetVolumeId())
	if !ok {
		t.Fatal("the volume is not being served")
	}

	// Distinct offsets, because the bound is on the *view* — rewriting one block for
	// ever costs nothing and would loop until the volume was full of nothing.
	block := make([]byte, blockSize)
	var accepted int64
	var refusal error
	for off := int64(0); off+blockSize <= volumeSize; off += blockSize {
		if _, err := dev.WriteAt(block, off); err != nil {
			refusal = err
			break
		}
		accepted += blockSize
	}
	if refusal == nil {
		t.Fatalf("the guest wrote %d bytes of distinct blocks — the whole volume — with no refusal; its read view was bounded at %d bytes, or at nothing",
			accepted, b.ViewShare())
	}
	// The remedy in the sentence is the guest's own (DISCARD, which is exempt from this
	// bound), not the operator's — a view bound reported as a full device would send an
	// operator to stop and republish a volume an fstrim would have freed.
	if !strings.Contains(refusal.Error(), "read view") {
		t.Fatalf("the guest was refused with a sentence that does not name the read view: %v", refusal)
	}
	// Where it was refused, and this is the number a constant cannot produce: the view
	// costs its payload plus cow.ExtentOverheadBytes per extent, so a guest writing
	// 4 KiB blocks is refused a little before it has written the share itself.
	if accepted > b.ViewShare() || accepted < b.ViewShare()/2 {
		t.Fatalf("the guest wrote %d bytes before its read view was refused; the share this Agent derived is %d",
			accepted, b.ViewShare())
	}
}
