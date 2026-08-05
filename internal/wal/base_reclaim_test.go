package wal_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// Installing a base reclaims the segments it makes redundant, and this is what happens
// when the device will not let it.
//
// The decision under test is that the volume refuses to serve rather than carrying on.
// Carrying on is defensible on the face of it — the read view is correct either way, and
// the only thing lost is device bytes — and it was rejected because the disk that just
// refused to open a file this process listed a moment ago is the disk the next guest
// WRITE is about to be appended to. A volume that treats that as cosmetic serves from a
// device it has already caught failing.
//
// The failure is produced by taking a segment away behind the log's back, after the scan
// found it and before the base arrives, which is what a disk losing a file looks like
// from in here. sim.Disk has injectors for a short append, a lost fdatasync and a torn
// tail, and none of them can fail an unlink.
func TestALogThatCannotReclaimItsRedundantWALRefusesToServe(t *testing.T) {
	d := sim.NewDisk()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := [16]byte{0x79}
	limits := wal.Limits{SegmentBytes: baseTestSegmentBytes}

	payload := bytes.Repeat([]byte{0xC3}, 4096)
	l := wal.NewLog(d, "wal", clk, vol, 1, limits)
	for i := range 6 {
		if _, err := l.Write(uint64(i)*4096, payload, 0); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	resumed, err := wal.ResumeAwaitingBase(d, "wal", clk, vol, 1, limits, nil)
	if err != nil {
		t.Fatalf("ResumeAwaitingBase: %v", err)
	}
	defer func() { _ = resumed.Close() }()

	names := resumed.SegmentNames()
	if len(names) < 2 {
		t.Fatalf("the fixture left %d segment(s); with one, reclaim has nothing it is allowed to unlink and cannot fail",
			len(names))
	}
	if err := d.Remove(names[0]); err != nil {
		t.Fatalf("removing %s behind the log's back: %v", names[0], err)
	}

	// 6 is every record the fixture wrote, so the base covers all of them and reclaim is
	// asked for every segment but the newest — including the one that is now missing.
	if err := resumed.InstallBase(baseHolding(payload), 6); err == nil {
		t.Fatal("installing a base over a WAL it could not reclaim reported success: the volume would serve from a device that has begun refusing operations")
	}

	// And the volume is refusable: baseWait is deliberately left open so the caller's
	// FailBase is what resolves it. A log that had closed it would have released every
	// parked read as if nothing were wrong.
	resumed.FailBase(errors.New("the reclaim failed"))
	if err := resumed.Read(0, make([]byte, len(payload))); !errors.Is(err, wal.ErrBaseUnavailable) {
		t.Fatalf("a read after a failed reclaim returned %v, want ErrBaseUnavailable", err)
	}
}
