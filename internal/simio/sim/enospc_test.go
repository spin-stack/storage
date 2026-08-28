package sim_test

import (
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/simio/sim"
)

// ENOSPC is the one disk fault that is not one-shot: the first failure arrives as a
// partial append and every append after it fails until somebody reclaims space. Modelling
// it as a short append is what let the WAL's out-of-space path go untested — the
// interesting part is what happens *after* the first failure.
func TestInjectENOSPCFillsTheDeviceAndStaysFull(t *testing.T) {
	tests := []struct {
		name     string
		capacity int64
		// appends are applied in order; want is the expected (n, full) per append.
		appends []int
		wantN   []int
		wantErr []bool
	}{
		{
			name:     "the append that crosses the limit writes only what fits",
			capacity: 10,
			appends:  []int{6, 6, 1},
			wantN:    []int{6, 4, 0},
			wantErr:  []bool{false, true, true},
		},
		{
			name:     "an append that exactly fills the device succeeds",
			capacity: 8,
			appends:  []int{8, 1},
			wantN:    []int{8, 0},
			wantErr:  []bool{false, true},
		},
		{
			name:     "a device with no free space at all fails the first append",
			capacity: 0,
			appends:  []int{4},
			wantN:    []int{0},
			wantErr:  []bool{true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := sim.NewDisk()
			f, err := d.Create("wal")
			if err != nil {
				t.Fatal(err)
			}
			d.InjectENOSPC("wal", tc.capacity)

			for i, size := range tc.appends {
				n, err := f.Append(make([]byte, size))
				if n != tc.wantN[i] {
					t.Fatalf("append %d wrote %d bytes, want %d", i, n, tc.wantN[i])
				}
				if got := errors.Is(err, sim.ErrNoSpace); got != tc.wantErr[i] {
					t.Fatalf("append %d: ErrNoSpace = %t (err=%v), want %t", i, got, err, tc.wantErr[i])
				}
			}
			// Whatever was accepted is still readable: ENOSPC loses the tail of the
			// rejected append, never what the device already took.
			size, err := f.Size()
			if err != nil {
				t.Fatal(err)
			}
			if size > tc.capacity {
				t.Fatalf("file grew to %d bytes past a %d-byte device", size, tc.capacity)
			}
		})
	}
}

// Reclaiming space is the only way out of ENOSPC, and it is what the WAL's local
// truncation is for (§21.1). A device that stays full after a truncate would make the
// recovery half of the out-of-space story untestable.
func TestENOSPCTruncateReclaimsSpace(t *testing.T) {
	d := sim.NewDisk()
	f, _ := d.Create("wal")
	d.InjectENOSPC("wal", 16)

	if _, err := f.Append(make([]byte, 16)); err != nil {
		t.Fatalf("filling the device exactly should succeed: %v", err)
	}
	if _, err := f.Append([]byte{1}); !errors.Is(err, sim.ErrNoSpace) {
		t.Fatalf("want ErrNoSpace on a full device, got %v", err)
	}
	if err := f.Truncate(8); err != nil {
		t.Fatal(err)
	}
	if n, err := f.Append(make([]byte, 8)); err != nil || n != 8 {
		t.Fatalf("after reclaiming 8 bytes the append should fit: n=%d err=%v", n, err)
	}
}

// ClearENOSPC models the operator/GC outcome an alert is supposed to produce: the
// device has space again and the log can make progress without being recreated.
func TestClearENOSPCGivesTheDeviceItsSpaceBack(t *testing.T) {
	d := sim.NewDisk()
	f, _ := d.Create("wal")
	d.InjectENOSPC("wal", 4)

	if _, err := f.Append(make([]byte, 8)); !errors.Is(err, sim.ErrNoSpace) {
		t.Fatalf("want ErrNoSpace, got %v", err)
	}
	d.ClearENOSPC("wal")
	if n, err := f.Append(make([]byte, 64)); err != nil || n != 64 {
		t.Fatalf("after ClearENOSPC the append should succeed: n=%d err=%v", n, err)
	}
}

// A full device must not make the bytes it already holds unreadable, and a Sync of
// them must still report durability: the WAL's rollback path (truncate back to the
// last intact record, then report the error) runs entirely on a full device.
func TestENOSPCLeavesAcceptedBytesDurable(t *testing.T) {
	d := sim.NewDisk()
	f, _ := d.Create("wal")
	d.InjectENOSPC("wal", 8)

	if _, err := f.Append([]byte("abcdefgh")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("syncing bytes the device already took must succeed: %v", err)
	}
	if _, err := f.Append([]byte("x")); !errors.Is(err, sim.ErrNoSpace) {
		t.Fatalf("want ErrNoSpace, got %v", err)
	}
	d.Crash()
	got := make([]byte, 8)
	if _, err := f.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if string(got) != "abcdefgh" {
		t.Fatalf("post-crash content = %q, want the synced bytes", got)
	}
}

// Growing a file with Truncate allocates just as an append does, so it must hit the
// same wall — otherwise a caller could grow its way out of ENOSPC.
func TestENOSPCRefusesATruncateThatGrows(t *testing.T) {
	d := sim.NewDisk()
	f, _ := d.Create("wal")
	d.InjectENOSPC("wal", 8)

	if err := f.Truncate(8); err != nil {
		t.Fatalf("growing to exactly the device size must succeed: %v", err)
	}
	if err := f.Truncate(9); !errors.Is(err, sim.ErrNoSpace) {
		t.Fatalf("growing past the device = %v, want ErrNoSpace", err)
	}
	if size, _ := f.Size(); size != 8 {
		t.Fatalf("a refused Truncate changed the size to %d", size)
	}
}

// A cap can also arrive *below* what the file already holds — a quota lowered under a
// running volume. The bytes already written stay readable; nothing more fits.
func TestENOSPCCapBelowCurrentSize(t *testing.T) {
	d := sim.NewDisk()
	f, _ := d.Create("wal")
	if _, err := f.Append(make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	d.InjectENOSPC("wal", 10)

	if n, err := f.Append([]byte{1}); n != 0 || !errors.Is(err, sim.ErrNoSpace) {
		t.Fatalf("append under a lowered cap = (%d, %v), want (0, ErrNoSpace)", n, err)
	}
	if size, _ := f.Size(); size != 100 {
		t.Fatalf("the bytes already written must stay: size = %d", size)
	}
}
