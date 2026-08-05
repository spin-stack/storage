package wal_test

import (
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// boundVol is the volume the device-bound tests write; v7-shaped (INV-22).
var boundVol = [16]byte{0: 0xb0, 6: 0x70, 8: 0x80, 15: 0x0d}

func boundLog(t *testing.T, d *sim.Disk, max, segBytes int64) *wal.Log {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	return wal.NewLog(d, "wal", clk, boundVol, 1, wal.Limits{MaxLocalBytes: max, SegmentBytes: segBytes})
}

// fill writes 512-byte records until one is refused, and returns how many were taken
// and the error that stopped it.
func fill(t *testing.T, l *wal.Log) (int, error) {
	t.Helper()
	for i := range 1000 {
		if _, err := l.Write(uint64(i)*4096, make([]byte, 512), 0); err != nil {
			return i, err
		}
	}
	t.Fatal("a thousand records fit inside the bound; the test proves nothing")
	return 0, nil
}

// TestALogStopsAtItsShareOfTheDevice is the WAL half of ADR-0013 §1: a log handed a
// share of the device budget refuses the WRITE that would take it past that share, and
// what it holds on the device — measured by reading the device, not by asking the log —
// stays inside it.
//
// MaxUnflushedBytes could not do this job and that is why the field is new. Sync clears
// the unflushed count on every guest fsync, so under any workload that fsyncs it reads
// zero while the segments keep growing: the test below fsyncs half-way through
// precisely so that a bound implemented on the unflushed count would let the log sail
// past its share.
func TestALogStopsAtItsShareOfTheDevice(t *testing.T) {
	const share = 8 << 10
	d := sim.NewDisk()
	l := boundLog(t, d, share, 1024)

	// One fsync in the middle, which is what a guest does and what clears every
	// unflushed counter in the log.
	if _, err := l.Write(0, make([]byte, 512), 0); err != nil {
		t.Fatal(err)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}

	taken, err := fill(t, l)
	if !errors.Is(err, wal.ErrBackpressure) {
		t.Fatalf("the WRITE past the share failed with %v, want backpressure after %d records", err, taken)
	}
	if got := walSize(t, l); got > share {
		t.Fatalf("the log holds %d bytes on the device, past its %d-byte share", got, share)
	}
	// A share nothing could be written into would satisfy the line above and bound
	// nothing: the guest has to have got most of its share.
	if got := walSize(t, l); got < share/2 {
		t.Fatalf("the log holds %d bytes of a %d-byte share: it was refused far too early", got, share)
	}
}

// TestReclaimedBytesReturnToTheShare pins what the bound measures: the segments the log
// still holds, not everything it has ever appended. Truncation unlinks whole segments
// (ADR-0013 §4), and a bound that counted the log's lifetime output would keep a volume
// in backpressure over bytes that are no longer on the device — which under ADR-0026 is
// exactly the state a volume is in after its image reaches the bucket.
func TestReclaimedBytesReturnToTheShare(t *testing.T) {
	const share = 8 << 10
	d := sim.NewDisk()
	l := boundLog(t, d, share, 1024)

	taken, err := fill(t, l)
	if !errors.Is(err, wal.ErrBackpressure) {
		t.Fatalf("the WRITE past the share failed with %v", err)
	}
	full := walSize(t, l)

	// A checkpoint's worth: everything below the middle record is unlinked.
	upTo := uint64(taken / 2)
	if err := l.AdvanceDurable(uint64(taken)); err != nil {
		t.Fatal(err)
	}
	if err := l.AdvancePublished(upTo); err != nil {
		t.Fatal(err)
	}
	if err := l.TruncateLocal(upTo); err != nil {
		t.Fatal(err)
	}
	freed := full - walSize(t, l)
	if freed <= 0 {
		t.Fatalf("truncating to %d unlinked nothing (%d bytes before and after)", upTo, full)
	}

	// The bound followed the device down: the volume writes again, and still stops
	// inside its share.
	if _, err := l.Write(1<<20, make([]byte, 512), 0); err != nil {
		t.Fatalf("a WRITE after %d bytes were reclaimed: %v", freed, err)
	}
	if _, err := fill(t, l); !errors.Is(err, wal.ErrBackpressure) {
		t.Fatalf("after reclaiming %d bytes the log stopped bounding itself", freed)
	}
	if got := walSize(t, l); got > share {
		t.Fatalf("the log holds %d bytes on the device, past its %d-byte share", got, share)
	}
}

// TestAResumedLogCountsTheSegmentsItFound is the case a counter gets wrong: an Agent
// that restarts re-attaches at the same epoch (ADR-0024) over a WAL directory that is
// already at its share. A log that started its accounting at zero would hand the guest
// another whole share of a device that has none — and it would be the *restart* path,
// the one that runs after something already went wrong.
func TestAResumedLogCountsTheSegmentsItFound(t *testing.T) {
	const share = 8 << 10
	d := sim.NewDisk()
	l := boundLog(t, d, share, 1024)
	if _, err := fill(t, l); !errors.Is(err, wal.ErrBackpressure) {
		t.Fatal("the first log never reached its share")
	}
	onDisk := walSize(t, l)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	resumed, err := wal.Resume(d, "wal", clk, boundVol, 1, 0,
		wal.Limits{MaxLocalBytes: share, SegmentBytes: 1024}, nil)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	t.Cleanup(func() { _ = resumed.Close() })

	if _, err := resumed.Write(1<<20, make([]byte, 512), 0); !errors.Is(err, wal.ErrBackpressure) {
		t.Fatalf("a log resumed over %d bytes of its %d-byte share took a WRITE with %v", onDisk, share, err)
	}
}
