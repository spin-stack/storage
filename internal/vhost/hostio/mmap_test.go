package hostio

import (
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

// memfd is a file that exists only in memory — the same thing QEMU shares its
// guest RAM through (-object memory-backend-memfd), so mapping one here is the
// production path rather than an approximation of it.
func memfd(t *testing.T, size int64) *os.File {
	t.Helper()
	fd, err := unix.MemfdCreate("vhost-test", unix.MFD_CLOEXEC)
	if err != nil {
		t.Fatalf("memfd_create: %v", err)
	}
	f := os.NewFile(uintptr(fd), "memfd")
	t.Cleanup(func() { _ = f.Close() })
	if err := f.Truncate(size); err != nil {
		t.Fatalf("truncate memfd: %v", err)
	}
	return f
}

// TestMappedMemoryIsSharedBothWays. MAP_SHARED is not a tuning choice: a
// MAP_PRIVATE mapping reads correctly, completes every request, and writes the
// guest's data into a copy nobody ever looks at.
func TestMappedMemoryIsSharedBothWays(t *testing.T) {
	const size = 1 << 20
	f := memfd(t, size)
	m := NewMapper()

	b, err := m.Map(f, 0, size)
	if err != nil {
		t.Fatalf("Map: %v", err)
	}
	if len(b) != size {
		t.Fatalf("mapped %d bytes, want %d", len(b), size)
	}

	// Written through the mapping, read through the file.
	copy(b[4096:], []byte("backend-to-guest"))
	got := make([]byte, 16)
	if _, err := f.ReadAt(got, 4096); err != nil {
		t.Fatal(err)
	}
	if string(got) != "backend-to-guest" {
		t.Fatalf("the file does not see the mapping's stores: %q", got)
	}

	// Written through the file, read through the mapping.
	if _, err := f.WriteAt([]byte("guest-to-backend"), 8192); err != nil {
		t.Fatal(err)
	}
	if string(b[8192:8208]) != "guest-to-backend" {
		t.Fatalf("the mapping does not see the file's stores: %q", b[8192:8208])
	}

	if err := m.Unmap(b); err != nil {
		t.Fatalf("Unmap: %v", err)
	}
}

// TestMapHonoursTheRegionOffset. SET_MEM_TABLE gives each region an offset into
// the descriptor it arrived with, and getting it wrong reads plausible bytes
// from the wrong place rather than failing.
func TestMapHonoursTheRegionOffset(t *testing.T) {
	const size = 1 << 20
	pageSize := int64(os.Getpagesize())
	f := memfd(t, size)
	if _, err := f.WriteAt([]byte("at-the-offset"), pageSize*2); err != nil {
		t.Fatal(err)
	}
	m := NewMapper()
	b, err := m.Map(f, uint64(pageSize*2), 4096)
	if err != nil {
		t.Fatalf("Map: %v", err)
	}
	defer func() { _ = m.Unmap(b) }()
	if string(b[:13]) != "at-the-offset" {
		t.Fatalf("the mapping starts somewhere else: %q", b[:13])
	}
}

func TestMapRefusesARegionItCannotHonour(t *testing.T) {
	f := memfd(t, 1<<20)
	m := NewMapper()
	tests := []struct {
		name         string
		file         *os.File
		offset, size uint64
	}{
		{"no descriptor", nil, 0, 4096},
		{"zero bytes", f, 0, 0},
		{"an unaligned offset", f, 1, 4096},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if b, err := m.Map(tc.file, tc.offset, tc.size); err == nil {
				_ = m.Unmap(b)
				t.Fatal("Map succeeded")
			}
		})
	}
}

// TestUnmapRefusesASliceItDidNotCreate. munmap on an arbitrary slice unmaps Go
// heap memory and takes the process down at some later, unrelated moment — so
// the failure would never point back here.
func TestUnmapRefusesASliceItDidNotCreate(t *testing.T) {
	m := NewMapper()
	if err := m.Unmap(make([]byte, 4096)); err == nil {
		t.Fatal("Unmap accepted a slice from the Go heap")
	}
	f := memfd(t, 1<<20)
	b, err := m.Map(f, 0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Unmap(b); err != nil {
		t.Fatal(err)
	}
	if err := m.Unmap(b); err == nil {
		t.Fatal("Unmap accepted the same mapping twice")
	}
}

func TestUnmapOfNothingIsNotAnError(t *testing.T) {
	if err := NewMapper().Unmap(nil); err != nil {
		t.Fatalf("Unmap(nil): %v", err)
	}
}
