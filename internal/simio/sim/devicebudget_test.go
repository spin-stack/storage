package sim_test

import (
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// The device budget is the simulated counterpart of statfs (ADR-0013 §1) and is per
// *device*, not per file: N volumes each inside their own InjectENOSPC cap can still fill
// the device between them, which is the case ADR-0013 §1 exists for.

func write(t *testing.T, d *sim.Disk, name string, n int) disk.File {
	t.Helper()
	f, err := d.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Append(make([]byte, n)); err != nil {
		t.Fatal(err)
	}
	return f
}

// TestDeviceBudgetIsWhatUsageReports: the numbers come from the files that are
// actually on the device, not from the budget alone. A budget that reported itself
// back would be a stub.
func TestDeviceBudgetIsWhatUsageReports(t *testing.T) {
	d := sim.NewDisk()
	d.SetDeviceBudget(1000)

	u, err := d.Usage()
	if err != nil {
		t.Fatal(err)
	}
	if u != (disk.Usage{TotalBytes: 1000, UsedBytes: 0, AvailBytes: 1000}) {
		t.Fatalf("empty device reports %+v", u)
	}

	f := write(t, d, "volumes/a/wal", 300)
	write(t, d, "volumes/b/wal", 250)
	if u, err = d.Usage(); err != nil || u.UsedBytes != 550 || u.AvailBytes != 450 {
		t.Fatalf("after 550 bytes: %+v err=%v", u, err)
	}

	// Truncation gives the bytes back — the reclamation ADR-0013 §4 is about.
	if err := f.Truncate(100); err != nil {
		t.Fatal(err)
	}
	if u, err = d.Usage(); err != nil || u.UsedBytes != 350 || u.AvailBytes != 650 {
		t.Fatalf("after truncating to 100: %+v err=%v", u, err)
	}

	if err := d.Remove("volumes/b/wal"); err != nil {
		t.Fatal(err)
	}
	if u, err = d.Usage(); err != nil || u.UsedBytes != 100 || u.AvailBytes != 900 {
		t.Fatalf("after removing a file: %+v err=%v", u, err)
	}
}

// TestUsageWithoutABudgetIsRefused: a scenario that asks a simulated disk how full
// it is without ever having said how big it is has asked a question with no answer.
// Inventing one — a total of zero, or an unbounded device that is never full — would
// make every threshold derived from it meaningless in exactly the runs meant to
// exercise pressure.
func TestUsageWithoutABudgetIsRefused(t *testing.T) {
	if _, err := sim.NewDisk().Usage(); !errors.Is(err, sim.ErrNoDeviceBudget) {
		t.Fatalf("Usage on an unsized device = %v, want ErrNoDeviceBudget", err)
	}
}

// TestDeviceBudgetIsAnENOSPCCeilingAcrossFiles: the budget is a device, so it fills
// like one. Two volumes, neither of them individually capped, exhaust it between
// them and the write that crosses it gets the partial append + ErrNoSpace a real
// ENOSPC delivers (INV-05).
func TestDeviceBudgetIsAnENOSPCCeilingAcrossFiles(t *testing.T) {
	d := sim.NewDisk()
	d.SetDeviceBudget(100)
	write(t, d, "volumes/a/wal", 60)

	b, err := d.Create("volumes/b/wal")
	if err != nil {
		t.Fatal(err)
	}
	n, err := b.Append(make([]byte, 60))
	if !errors.Is(err, disk.ErrNoSpace) {
		t.Fatalf("append past the device budget = %v, want ErrNoSpace", err)
	}
	if n != 40 {
		t.Fatalf("partial append wrote %d bytes, want the 40 that fit", n)
	}
	if u, err := d.Usage(); err != nil || u.UsedBytes != 100 || u.AvailBytes != 0 {
		t.Fatalf("a full device reports %+v err=%v", u, err)
	}

	// Full stays full: the failure mode worth simulating is what the caller does
	// after the first ENOSPC, not the first one.
	if n, err := b.Append([]byte{1}); n != 0 || !errors.Is(err, disk.ErrNoSpace) {
		t.Fatalf("append to a full device = (%d, %v), want (0, ErrNoSpace)", n, err)
	}
	if err := b.Truncate(60); !errors.Is(err, disk.ErrNoSpace) {
		t.Fatalf("growing a file on a full device = %v, want ErrNoSpace", err)
	}

	// Reclaiming on one volume lets the other one write again: the whole point of
	// the checkpoint-then-truncate chain (INV-13).
	if err := d.Remove("volumes/a/wal"); err != nil {
		t.Fatal(err)
	}
	if n, err := b.Append(make([]byte, 50)); n != 50 || err != nil {
		t.Fatalf("after reclaiming: append = (%d, %v), want (50, nil)", n, err)
	}
}

// TestDeviceBudgetAndAPerFileCapBothBind: the two ceilings are different failures
// (one volume's quota, and the device) and neither may mask the other.
func TestDeviceBudgetAndAPerFileCapBothBind(t *testing.T) {
	d := sim.NewDisk()
	d.SetDeviceBudget(1000)
	d.InjectENOSPC("volumes/a/wal", 10)

	f, err := d.Create("volumes/a/wal")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := f.Append(make([]byte, 20)); n != 10 || !errors.Is(err, disk.ErrNoSpace) {
		t.Fatalf("the per-file cap did not bind: (%d, %v)", n, err)
	}
	u, err := d.Usage()
	if err != nil || u.UsedBytes != 10 {
		t.Fatalf("the device did not charge the bytes the file did take: %+v err=%v", u, err)
	}
}
