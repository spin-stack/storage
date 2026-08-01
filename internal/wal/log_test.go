package wal_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

func newLog(t *testing.T, limits wal.Limits) (*wal.Log, *sim.Clock, *sim.Disk) {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	return wal.NewLog(d, "wal", clk, [16]byte{}, 1, limits), clk, d
}

func TestLogWriteReadBack(t *testing.T) {
	l, _, _ := newLog(t, wal.Limits{MaxUnflushedBytes: 1 << 20})
	if _, err := l.Write(0, []byte("hello"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Write(8, []byte("world"), 0); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 13)
	if err := l.Read(0, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(buf, []byte("hello\x00\x00\x00world")) {
		t.Fatalf("read-back mismatch: %q", buf)
	}
	if l.Watermarks().Local != 2 {
		t.Fatalf("local watermark = %d, want 2", l.Watermarks().Local)
	}
}

func TestLogDiscardReadsZero(t *testing.T) {
	l, _, _ := newLog(t, wal.Limits{MaxUnflushedBytes: 1 << 20})
	_, _ = l.Write(0, []byte{1, 2, 3, 4}, 0)
	if _, err := l.Discard(1, 2); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if err := l.Read(0, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(buf, []byte{1, 0, 0, 4}) {
		t.Fatalf("discard read-back: %v", buf)
	}
}

func TestBackpressureOnBytes(t *testing.T) {
	l, _, _ := newLog(t, wal.Limits{MaxUnflushedBytes: 200}) // ~1 record fits (104 hdr + payload)
	if _, err := l.Write(0, make([]byte, 32), 0); err != nil {
		t.Fatalf("first write should fit: %v", err)
	}
	// Second write pushes past 200 bytes unflushed → backpressure.
	if _, err := l.Write(64, make([]byte, 64), 0); err != wal.ErrBackpressure {
		t.Fatalf("want ErrBackpressure, got %v", err)
	}
	// After Sync, accounting resets and writes resume.
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Write(64, make([]byte, 64), 0); err != nil {
		t.Fatalf("after sync write should fit: %v", err)
	}
}

func TestBackpressureOnAge(t *testing.T) {
	l, clk, _ := newLog(t, wal.Limits{MaxUnflushedBytes: 1 << 20, MaxUnflushedAge: 30 * time.Second})
	if _, err := l.Write(0, []byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	clk.Advance(31 * time.Second) // oldest unflushed now too old
	if _, err := l.Write(8, []byte("y"), 0); err != wal.ErrBackpressure {
		t.Fatalf("want ErrBackpressure on age, got %v", err)
	}
}

func TestWatermarkOrderingEnforced(t *testing.T) {
	l, _, _ := newLog(t, wal.Limits{MaxUnflushedBytes: 1 << 20})
	for range 5 {
		if _, err := l.Write(0, []byte("z"), 0); err != nil {
			t.Fatal(err)
		}
	}
	// local=5. durable cannot exceed local.
	if err := l.AdvanceDurable(6); err != wal.ErrWatermarkOrder {
		t.Fatalf("durable>local should fail, got %v", err)
	}
	if err := l.AdvanceDurable(3); err != nil {
		t.Fatal(err)
	}
	// published cannot exceed durable.
	if err := l.AdvancePublished(4); err != wal.ErrWatermarkOrder {
		t.Fatalf("published>durable should fail, got %v", err)
	}
	if err := l.AdvancePublished(3); err != nil {
		t.Fatal(err)
	}
	w := l.Watermarks()
	if w.Published > w.Durable || w.Durable > w.Local {
		t.Fatalf("ordering violated: %+v", w)
	}
}
