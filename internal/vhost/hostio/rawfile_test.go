package hostio

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/spin-stack/storage/internal/vhost"
)

// pattern is a deterministic, position-dependent filler: a run of a single byte
// would let an off-by-one offset pass every assertion in the file.
func pattern(seed byte, n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = seed ^ byte(i*7+3)
	}
	return p
}

func tempPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "device.raw")
}

func TestCreateRawFileHasTheCapacityItWasAskedFor(t *testing.T) {
	path := tempPath(t)
	const size = 8 << 20
	d, err := CreateRawFile(path, size)
	if err != nil {
		t.Fatalf("CreateRawFile: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if got := d.Size(); got != size {
		t.Fatalf("Size %d, want %d", got, size)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != size {
		t.Fatalf("the file on disk is %d bytes, want %d", fi.Size(), size)
	}
}

func TestCreateRawFileRejectsACapacityNoDeviceCanHave(t *testing.T) {
	tests := []struct {
		name string
		size int64
	}{
		{"zero", 0},
		{"negative", -1},
		{"not a whole sector", 4*vhost.SectorSize + 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if d, err := CreateRawFile(tempPath(t), tc.size); err == nil {
				_ = d.Close()
				t.Fatalf("CreateRawFile(%d) succeeded", tc.size)
			}
		})
	}
}

// TestOpenRawFileTakesTheCapacityFromTheFile is the path an operator uses: a
// device file that already exists is served at the size it already has.
func TestOpenRawFileTakesTheCapacityFromTheFile(t *testing.T) {
	path := tempPath(t)
	const size = 4 << 20
	d, err := CreateRawFile(path, size)
	if err != nil {
		t.Fatal(err)
	}
	want := pattern(0x5a, vhost.SectorSize)
	if _, err := d.WriteAt(want, 3*vhost.SectorSize); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenRawFile(path)
	if err != nil {
		t.Fatalf("OpenRawFile: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if got := reopened.Size(); got != size {
		t.Fatalf("Size %d, want %d", got, size)
	}
	got := make([]byte, len(want))
	if _, err := reopened.ReadAt(got, 3*vhost.SectorSize); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("what was written before the close is not what came back after it")
	}
}

func TestOpenRawFileRefusesWhatIsNotADevice(t *testing.T) {
	dir := t.TempDir()
	short := filepath.Join(dir, "short.raw")
	if err := os.WriteFile(short, make([]byte, vhost.SectorSize+1), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		path string
	}{
		{"a file that is not there", filepath.Join(dir, "absent.raw")},
		{"a partial trailing sector", short},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if d, err := OpenRawFile(tc.path); err == nil {
				_ = d.Close()
				t.Fatal("want an error, got a device")
			}
		})
	}
}

// TestRawFileRoundTripsAtEveryOffset. The offsets are the ones an off-by-one
// lands on: the first sector, the last, and a span that ends exactly at the end.
func TestRawFileRoundTripsAtEveryOffset(t *testing.T) {
	const size = 1 << 20
	d, err := CreateRawFile(tempPath(t), size)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })

	tests := []struct {
		name   string
		off    int64
		length int
	}{
		{"the first sector", 0, vhost.SectorSize},
		{"a sector in the middle", 512 * vhost.SectorSize, vhost.SectorSize},
		{"the last sector", size - vhost.SectorSize, vhost.SectorSize},
		{"a multi-sector span ending at the end", size - 8*vhost.SectorSize, 8 * vhost.SectorSize},
		{"the whole device", 0, size},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			want := pattern(byte(tc.off), tc.length)
			n, err := d.WriteAt(want, tc.off)
			if err != nil {
				t.Fatalf("WriteAt: %v", err)
			}
			if n != len(want) {
				t.Fatalf("WriteAt wrote %d of %d", n, len(want))
			}
			got := make([]byte, tc.length)
			n, err = d.ReadAt(got, tc.off)
			if err != nil {
				t.Fatalf("ReadAt: %v", err)
			}
			if n != len(got) {
				t.Fatalf("ReadAt read %d of %d", n, len(got))
			}
			if !bytes.Equal(got, want) {
				t.Fatal("the device does not hold what was written to it")
			}
		})
	}
}

// TestRawFileRefusesWhatIsOffTheDevice. virtio-blk has no way to tell a guest "I
// did 3 of your 8 sectors": the request completes OK or IOERR, so a transfer
// that would run off the end has to fail before it starts, not half-succeed.
func TestRawFileRefusesWhatIsOffTheDevice(t *testing.T) {
	const size = 64 * vhost.SectorSize
	d, err := CreateRawFile(tempPath(t), size)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })

	tests := []struct {
		name   string
		off    int64
		length int
	}{
		{"starting past the end", size, vhost.SectorSize},
		{"starting well past the end", size * 4, vhost.SectorSize},
		{"straddling the end", size - 1, 2},
		{"a negative offset", -1, vhost.SectorSize},
		{"longer than the device", 0, size + 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := make([]byte, tc.length)
			if _, err := d.ReadAt(buf, tc.off); !errors.Is(err, vhost.ErrOutOfRange) {
				t.Fatalf("ReadAt: want ErrOutOfRange, got %v", err)
			}
			if _, err := d.WriteAt(buf, tc.off); !errors.Is(err, vhost.ErrOutOfRange) {
				t.Fatalf("WriteAt: want ErrOutOfRange, got %v", err)
			}
		})
	}
}

// TestRawFileWriteDoesNotGrowTheDevice. A WriteAt that extended the file would
// hand the guest capacity the device never advertised, and the guest addresses
// what GET_CONFIG told it, not what the file happens to be.
func TestRawFileWriteDoesNotGrowTheDevice(t *testing.T) {
	path := tempPath(t)
	const size = 16 * vhost.SectorSize
	d, err := CreateRawFile(path, size)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if _, err := d.WriteAt(pattern(1, vhost.SectorSize), size); !errors.Is(err, vhost.ErrOutOfRange) {
		t.Fatalf("want ErrOutOfRange, got %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != size {
		t.Fatalf("the file grew to %d bytes; the device advertises %d", fi.Size(), size)
	}
}

// TestRawFileFlushSurvivesTheData is as much as a unit test can say about
// durability: Flush returns without error and the bytes are still there. What it
// actually has to do — reach stable media — is not observable from inside the
// process, and proving it needs a crash, which is Phase 04's DST arm once wal.Log
// is behind this interface.
func TestRawFileFlushSurvivesTheData(t *testing.T) {
	path := tempPath(t)
	d, err := CreateRawFile(path, 64*vhost.SectorSize)
	if err != nil {
		t.Fatal(err)
	}
	want := pattern(0xc3, 2*vhost.SectorSize)
	if _, err := d.WriteAt(want, 4*vhost.SectorSize); err != nil {
		t.Fatal(err)
	}
	if err := d.Flush(t.Context()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw[4*vhost.SectorSize:6*vhost.SectorSize], want) {
		t.Fatal("the flushed bytes are not in the file")
	}
}

// TestRawFileFailsAfterClose: every entry point has to refuse rather than
// operate on a descriptor the runtime may have handed to someone else.
func TestRawFileFailsAfterClose(t *testing.T) {
	d, err := CreateRawFile(tempPath(t), 8*vhost.SectorSize)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, vhost.SectorSize)
	if _, err := d.ReadAt(buf, 0); err == nil {
		t.Fatal("ReadAt succeeded on a closed device")
	}
	if _, err := d.WriteAt(buf, 0); err == nil {
		t.Fatal("WriteAt succeeded on a closed device")
	}
	if err := d.Flush(t.Context()); err == nil {
		t.Fatal("Flush succeeded on a closed device")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

var _ vhost.Backend = (*RawFile)(nil)

// Discard punches a hole, which is the one implementation that gives virtio both of the
// things it asks of a discard at once: the range reads back as zeros AND the space goes
// back to the filesystem. Asserted on both, because writing zeros would satisfy the
// first while doing the exact opposite of the second — consuming space to service a
// request whose purpose was to free it — and a guest told OK would keep trimming a
// device that grows.
//
// KEEP_SIZE is what stops the file being truncated, which would change the capacity the
// guest was told about mid-session, so the size is asserted too.
func TestDiscardFreesTheRangeAndLeavesTheCapacityAlone(t *testing.T) {
	const size = 1 << 20
	d, err := CreateRawFile(tempPath(t), size)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })

	full := pattern(0x40, size)
	if _, err := d.WriteAt(full, 0); err != nil {
		t.Fatal(err)
	}
	if err := d.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	before := blocksUsed(t, d)

	// Punch the middle half, so both edges are load-bearing: a discard that rounded
	// outward would take bytes the guest still holds.
	const off, length = size / 4, size / 2
	if err := d.Discard(off, length); err != nil {
		t.Fatalf("Discard: %v", err)
	}

	got := make([]byte, size)
	if _, err := d.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[off:off+length], make([]byte, length)) {
		t.Errorf("the discarded range reads %x, want zeros", got[off:off+8])
	}
	if !bytes.Equal(got[:off], full[:off]) {
		t.Error("the bytes before the discarded range were disturbed")
	}
	if !bytes.Equal(got[off+length:], full[off+length:]) {
		t.Error("the bytes after the discarded range were disturbed")
	}
	if d.Size() != size {
		t.Errorf("capacity moved to %d: KEEP_SIZE is what stops a discard changing what the guest was told", d.Size())
	}
	if after := blocksUsed(t, d); after >= before {
		t.Errorf("the file occupies %d blocks after discarding %d bytes and %d before: nothing was returned to the filesystem",
			after, length, before)
	}
}

// WriteZeroes has the same observable and ignores may_unmap, which is conforming: the
// flag permits releasing the space rather than requiring it. Out of range is refused for
// both, because clamping a discard is data loss with an OK status — the guest is told
// the range it named is gone and a different one is.
func TestWriteZeroesAndTheRangeChecks(t *testing.T) {
	const size = 1 << 20
	d, err := CreateRawFile(tempPath(t), size)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if _, err := d.WriteAt(pattern(0x40, size), 0); err != nil {
		t.Fatal(err)
	}
	for _, unmap := range []bool{false, true} {
		if err := d.WriteZeroes(0, 4096, unmap); err != nil {
			t.Fatalf("WriteZeroes(unmap=%v): %v", unmap, err)
		}
	}
	got := make([]byte, 4096)
	if _, err := d.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, make([]byte, 4096)) {
		t.Errorf("WriteZeroes left %x", got[:8])
	}

	for _, tc := range []struct {
		name        string
		off, length int64
	}{
		{"past the end", size, 512},
		{"straddling the end", size - 256, 512},
		{"negative offset", -512, 512},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := d.Discard(tc.off, tc.length); !errors.Is(err, vhost.ErrOutOfRange) {
				t.Errorf("Discard(%d, %d) = %v, want ErrOutOfRange", tc.off, tc.length, err)
			}
			if err := d.WriteZeroes(tc.off, tc.length, true); !errors.Is(err, vhost.ErrOutOfRange) {
				t.Errorf("WriteZeroes(%d, %d) = %v, want ErrOutOfRange", tc.off, tc.length, err)
			}
		})
	}
}

// blocksUsed is the filesystem's own accounting, which is the only way to tell a hole
// from a run of zero bytes: both read back identically.
func blocksUsed(t *testing.T, d *RawFile) int64 {
	t.Helper()
	fi, err := d.f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("no stat blocks on this platform; a hole cannot be told from zeros")
	}
	return st.Blocks
}
