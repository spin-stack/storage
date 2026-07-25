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
