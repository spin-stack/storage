package wal_test

import (
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// reopen returns a second handle on the same WAL file, which is what an agent
// restart has: the file is still there, full of records, and the process that knew
// the watermarks is gone (§16 ATTACHING→ACTIVE validates the epoch, it does not
// bump it).
func reopen(t *testing.T, d *sim.Disk, name string) disk.File {
	t.Helper()
	f, err := d.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// TestNewLogOverANonEmptyWALIsRefused: NewLog always starts the sequence space at
// the boundary it was given, so building one over a WAL that already holds records
// re-issues sequences 1..N under the same (volume, epoch). Under encryption that is
// GCM nonce reuse (§15.2 derives the nonce from volume/epoch/sequence, INV-15), and
// in the bucket it is two different objects claiming the same span (INV-21). It also
// serves an empty read view for data that is in the WAL and in S3.
//
// A constructor that cannot see the file's contents must not be the one that decides
// this: the first append is where the damage starts, so that is where it fails
// closed.
func TestNewLogOverANonEmptyWALIsRefused(t *testing.T) {
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	f, err := d.Create("wal/active.wal")
	if err != nil {
		t.Fatal(err)
	}
	vol := [16]byte{11}
	l1 := wal.NewLog(f, clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	for i := range 3 {
		if _, err := l1.Write(uint64(i)*64, []byte("pre-crash"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := l1.Sync(); err != nil {
		t.Fatal(err)
	}
	sizeBefore, _ := f.Size()

	// The agent restarts and re-attaches at the same epoch.
	l2 := wal.NewLog(reopen(t, d, "wal/active.wal"), clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	seq, err := l2.Write(4096, []byte("post-restart"), 0)
	if err == nil {
		t.Fatalf("a log rebuilt over a non-empty WAL wrote sequence %d, re-issuing the sequence space", seq)
	}

	// Nothing was appended: the rejected write must not leave a record behind.
	sizeAfter, _ := f.Size()
	if sizeAfter != sizeBefore {
		t.Fatalf("refused write still appended %d bytes", sizeAfter-sizeBefore)
	}
}
