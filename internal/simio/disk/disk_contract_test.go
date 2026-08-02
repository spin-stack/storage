package disk_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/real"
	"github.com/spin-stack/storage/internal/simio/sim"
)

func impls(t *testing.T) map[string]disk.Disk {
	t.Helper()
	rd, err := real.NewDisk(t.TempDir())
	if err != nil {
		t.Fatalf("real disk: %v", err)
	}
	return map[string]disk.Disk{
		"real": rd,
		"sim":  sim.NewDisk(),
	}
}

func TestAppendSyncReadRoundtrip(t *testing.T) {
	for name, d := range impls(t) {
		t.Run(name, func(t *testing.T) {
			f, err := d.Create("wal/0-0.wal")
			if err != nil {
				t.Fatal(err)
			}
			data := []byte("hello, durable world")
			n, err := f.Append(data)
			if err != nil || n != len(data) {
				t.Fatalf("append: n=%d err=%v", n, err)
			}
			if err := f.Sync(); err != nil {
				t.Fatal(err)
			}
			sz, err := f.Size()
			if err != nil || sz != int64(len(data)) {
				t.Fatalf("size: %d err=%v", sz, err)
			}
			got := make([]byte, len(data))
			if _, err := f.ReadAt(got, 0); err != nil && !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			if !bytes.Equal(got, data) {
				t.Fatalf("roundtrip mismatch: %q != %q", got, data)
			}
		})
	}
}

func TestReadPastEndIsEOF(t *testing.T) {
	for name, d := range impls(t) {
		t.Run(name, func(t *testing.T) {
			f, _ := d.Create("f")
			_, _ = f.Append([]byte("abc"))
			buf := make([]byte, 8)
			n, err := f.ReadAt(buf, 0)
			if !errors.Is(err, io.EOF) {
				t.Fatalf("want EOF, got n=%d err=%v", n, err)
			}
			if n != 3 {
				t.Fatalf("want 3 bytes before EOF, got %d", n)
			}
		})
	}
}

func TestTruncate(t *testing.T) {
	for name, d := range impls(t) {
		t.Run(name, func(t *testing.T) {
			f, _ := d.Create("f")
			_, _ = f.Append([]byte("0123456789"))
			if err := f.Truncate(4); err != nil {
				t.Fatal(err)
			}
			sz, _ := f.Size()
			if sz != 4 {
				t.Fatalf("want size 4, got %d", sz)
			}
		})
	}
}

func TestRenameRemoveExistsList(t *testing.T) {
	for name, d := range impls(t) {
		t.Run(name, func(t *testing.T) {
			f, _ := d.Create("a/one.wal")
			_ = f.Sync()
			f2, _ := d.Create("a/two.wal")
			_ = f2.Sync()

			if ok, _ := d.Exists("a/one.wal"); !ok {
				t.Fatal("expected a/one.wal to exist")
			}
			if err := d.Rename("a/one.wal", "a/renamed.wal"); err != nil {
				t.Fatal(err)
			}
			if ok, _ := d.Exists("a/one.wal"); ok {
				t.Fatal("old name should be gone after rename")
			}
			names, _ := d.List("a/")
			if len(names) != 2 {
				t.Fatalf("want 2 files under a/, got %v", names)
			}
			if err := d.Remove("a/two.wal"); err != nil {
				t.Fatal(err)
			}
			names, _ = d.List("a/")
			if len(names) != 1 || names[0] != "a/renamed.wal" {
				t.Fatalf("unexpected list after remove: %v", names)
			}
		})
	}
}

func TestOpenMissingIsErrNotExist(t *testing.T) {
	for name, d := range impls(t) {
		t.Run(name, func(t *testing.T) {
			_, err := d.Open("nope")
			if !errors.Is(err, disk.ErrNotExist) {
				t.Fatalf("want ErrNotExist, got %v", err)
			}
		})
	}
}

// TestUsageReportsADevice is the statfs contract (ADR-0013): a Disk can say how big
// the device under it is and how much of it is left. Everything the Agent decides
// about local pressure is a ratio of these numbers, so an implementation that cannot
// produce them leaves the thresholds fed by an estimate.
//
// Only the invariants are asserted here. "Used grows when you write" is exact in the
// simulator — the device is ours alone there — and cannot be asserted against a real
// filesystem the rest of the machine is also writing to. That asymmetry is the whole
// reason for the call: what statfs adds over summing our own files is precisely the
// space other tenants occupy.
func TestUsageReportsADevice(t *testing.T) {
	rd, err := real.NewDisk(t.TempDir())
	if err != nil {
		t.Fatalf("real disk: %v", err)
	}
	sd := sim.NewDisk()
	sd.SetDeviceBudget(1 << 30)

	for name, d := range map[string]disk.Disk{"real": rd, "sim": sd} {
		t.Run(name, func(t *testing.T) {
			u, err := d.Usage()
			if err != nil {
				t.Fatalf("Usage: %v", err)
			}
			switch {
			case u.TotalBytes <= 0:
				t.Fatalf("a device with no capacity: %+v", u)
			case u.UsedBytes < 0 || u.UsedBytes > u.TotalBytes:
				t.Fatalf("used outside the device: %+v", u)
			case u.AvailBytes < 0 || u.AvailBytes > u.TotalBytes:
				t.Fatalf("available outside the device: %+v", u)
			case u.UsedBytes+u.AvailBytes > u.TotalBytes:
				// A real filesystem reserves blocks that are neither used nor
				// available; nothing may claim more than the device holds.
				t.Fatalf("used + available exceeds the device: %+v", u)
			}
		})
	}
}

// --- sim-specific: crash model and fault injection ---

func TestSimCrashLosesUnsyncedData(t *testing.T) {
	d := sim.NewDisk()
	f, _ := d.Create("wal")
	_, _ = f.Append([]byte("durable"))
	_ = f.Sync()
	_, _ = f.Append([]byte("-vulnerable"))
	// no Sync

	d.Crash()

	sz, _ := f.Size()
	if sz != int64(len("durable")) {
		t.Fatalf("after crash want only synced bytes, got size %d", sz)
	}
	got := make([]byte, sz)
	_, _ = f.ReadAt(got, 0)
	if string(got) != "durable" {
		t.Fatalf("after crash want %q, got %q", "durable", got)
	}
}

func TestSimShortAppendFault(t *testing.T) {
	d := sim.NewDisk()
	f, _ := d.Create("wal")
	d.InjectShortAppend("wal", 3)
	n, err := f.Append([]byte("0123456789"))
	if !errors.Is(err, sim.ErrShortWrite) {
		t.Fatalf("want ErrShortWrite, got %v", err)
	}
	if n != 3 {
		t.Fatalf("want 3 bytes written, got %d", n)
	}
	sz, _ := f.Size()
	if sz != 3 {
		t.Fatalf("want size 3 after short write, got %d", sz)
	}
}

func TestSimTornTailTruncatesDurable(t *testing.T) {
	d := sim.NewDisk()
	f, _ := d.Create("wal")
	_, _ = f.Append([]byte("committed"))
	_ = f.Sync()
	_, _ = f.Append([]byte("-more"))
	_ = f.Sync()

	// A torn write clips the durable tail to 9 bytes, then a crash reverts caches.
	d.TornTail("wal", 9)
	d.Crash()
	if err := f.Close(); err != nil { // sim Close is a no-op but must be callable
		t.Fatal(err)
	}
	sz, _ := f.Size()
	if sz != 9 {
		t.Fatalf("after torn tail want size 9, got %d", sz)
	}
	got := make([]byte, sz)
	_, _ = f.ReadAt(got, 0)
	if string(got) != "committed" {
		t.Fatalf("torn tail content = %q, want committed", got)
	}
}

func TestSimSyncLossThenCrashLosesData(t *testing.T) {
	d := sim.NewDisk()
	f, _ := d.Create("wal")
	_, _ = f.Append([]byte("data"))
	d.InjectSyncLoss("wal")
	if err := f.Sync(); err != nil { // reports success but does not persist
		t.Fatal(err)
	}
	d.Crash()
	sz, _ := f.Size()
	if sz != 0 {
		t.Fatalf("lost sync + crash must lose data, got size %d", sz)
	}
}

// The lock is DEV-0014's primitive: §10 assumes "un proceso por host" and nothing
// enforced it, so two Agents on one --data-dir both resumed the same segment files and
// both appended to them. Every property below is one the Agent depends on, and both
// implementations must have all of them or the simulation is modelling a different
// world from the one that ships.
func TestLock(t *testing.T) {
	for name, d := range impls(t) {
		t.Run(name, func(t *testing.T) {
			held, err := d.Lock("data/agent.lock")
			if err != nil {
				t.Fatalf("first lock: %v", err)
			}

			// Exclusive, and non-blocking about it: a second caller is told no rather
			// than parked. A blocking lock would leave the second Agent hung with no
			// output, which is harder to diagnose than a refusal.
			if _, err := d.Lock("data/agent.lock"); !errors.Is(err, disk.ErrLocked) {
				t.Fatalf("second lock: want ErrLocked, got %v", err)
			}

			// Per name, so two data directories on one host do not exclude each other.
			other, err := d.Lock("other/agent.lock")
			if err != nil {
				t.Fatalf("a different name must be lockable: %v", err)
			}
			if err := other.Close(); err != nil {
				t.Fatal(err)
			}

			// Released on Close, so a supervisor that stops one Agent and starts
			// another in the same directory works.
			if err := held.Close(); err != nil {
				t.Fatalf("release: %v", err)
			}
			again, err := d.Lock("data/agent.lock")
			if err != nil {
				t.Fatalf("a released lock must be retakeable: %v", err)
			}

			// Closing twice must not free a lock someone else has since taken — the
			// shape of every use-after-free. Take the second close first, then prove
			// the *live* holder still holds it.
			if err := again.Close(); err != nil {
				t.Fatal(err)
			}
			live, err := d.Lock("data/agent.lock")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = live.Close() }()
			if err := again.Close(); err != nil {
				t.Fatalf("a second Close must be a no-op: %v", err)
			}
			if _, err := d.Lock("data/agent.lock"); !errors.Is(err, disk.ErrLocked) {
				t.Fatalf("a double Close released a lock another holder owns: %v", err)
			}
		})
	}
}
