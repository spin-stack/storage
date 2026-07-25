package real_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/real"
)

func TestRealDiskLifecycleAndClose(t *testing.T) {
	d, err := real.NewDisk(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f, err := d.Create("a/b.wal")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Append([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if sz, _ := f.Size(); sz != 5 {
		t.Fatalf("size = %d, want 5", sz)
	}
	if err := f.Truncate(3); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil { // exercises real Close
		t.Fatalf("close: %v", err)
	}

	// Reopen, read back the truncated content.
	g, err := d.Open("a/b.wal")
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	buf := make([]byte, 3)
	if _, err := g.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "hel" {
		t.Fatalf("read = %q, want hel", buf)
	}
}

func TestRealDiskErrorPaths(t *testing.T) {
	d, _ := real.NewDisk(t.TempDir())
	if _, err := d.Open("missing"); !errors.Is(err, disk.ErrNotExist) {
		t.Fatalf("open missing: want ErrNotExist, got %v", err)
	}
	if err := d.Rename("missing", "other"); !errors.Is(err, disk.ErrNotExist) {
		t.Fatalf("rename missing: want ErrNotExist, got %v", err)
	}
	if ok, _ := d.Exists("missing"); ok {
		t.Fatal("missing should not exist")
	}
}

func TestRealObjectStoreErrorPaths(t *testing.T) {
	ctx := context.Background()
	s, err := real.NewObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "missing"); err == nil {
		t.Fatal("Get missing should error")
	}
	if _, err := s.Head(ctx, "missing"); err == nil {
		t.Fatal("Head missing should error")
	}
	if err := s.Delete(ctx, "missing"); err == nil {
		t.Fatal("Delete missing should error")
	}
	// Put then Head/Delete succeed.
	if _, err := s.Put(ctx, "k", []byte("v"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Head(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
}

func TestRealClockTimerStop(t *testing.T) {
	c := real.NewClock()
	timer := c.NewTimer(time.Hour)
	if !timer.Stop() { // exercises realTimer.Stop before firing
		t.Fatal("Stop should report true for a timer that had not fired")
	}
}

func TestRealClockSleepContext(t *testing.T) {
	c := real.NewClock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Sleep(ctx, time.Hour); err == nil {
		t.Fatal("Sleep should return the context error when cancelled")
	}
}
