package wal_test

import (
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// viewVol is the volume the read-view bound tests write; v7-shaped (INV-22).
var viewVol = [16]byte{0: 0x71, 6: 0x70, 8: 0x80, 15: 0x0e}

// viewLog builds a log whose only bound is the read view: MaxLocalBytes is left at 0 so
// that a refusal can only have come from memory, never from the device.
func viewLog(t *testing.T, maxView int64) *wal.Log {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	return wal.NewLog(sim.NewDisk(), "wal", clk, viewVol, 1,
		wal.Limits{MaxViewBytes: maxView, SegmentBytes: 4 << 20})
}

// writeDistinct writes 4 KiB blocks at distinct offsets until one is refused, and
// returns how many were taken and the error that stopped it.
func writeDistinct(t *testing.T, l *wal.Log, cap int) (int, error) {
	t.Helper()
	block := make([]byte, 4096)
	for i := range cap {
		if _, err := l.Write(uint64(i)*4096, block, 0); err != nil {
			return i, err
		}
	}
	t.Fatalf("%d distinct blocks fit inside the bound; the test proves nothing", cap)
	return 0, nil
}

// The read view is the one per-volume structure whose size the guest decides, and until
// this test nothing bounded it: an Agent measured at 1.55 GiB RSS on 195,658 distinct
// 4 KiB writes, monotonic, with the OOM killer as the limit — which takes every other
// tenant's volume on the host with it.
//
// The assertion is on what the guest and the operator see: the error the WRITE returns
// (ErrBackpressure, which the vhost front-end already turns into an I/O error) and the
// read_view_bytes gauge the Agent scrapes. Neither is a flag this code sets.
func TestAGuestIsRefusedWhenTheReadViewReachesItsMemoryBound(t *testing.T) {
	ctx := t.Context()
	p, err := obs.NewTestProvider("wal-view-bound")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(ctx) }()

	const bound = 4 << 20
	l := viewLog(t, bound)
	l.SetRecorder(obs.NewRecorder(p.Metrics), "vol-e")

	taken, err := writeDistinct(t, l, 4096)
	if !errors.Is(err, wal.ErrBackpressure) {
		t.Fatalf("the WRITE past the memory bound failed with %v after %d blocks, want backpressure", err, taken)
	}

	// A bound that refused the guest at once would satisfy the line above and be
	// useless: the guest has to have got most of what it was promised.
	if got := int64(taken) * 4096; got < bound/2 {
		t.Fatalf("the guest wrote %d bytes of a %d-byte view bound before being refused", got, bound)
	}
	// And the view must not have sailed past the bound. It may exceed it by the one
	// request that was in flight when it crossed — that is what charging the view as it
	// stands, rather than the record before it lands, buys — and by nothing more.
	if err := l.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	values, err := p.GaugeValues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	held, ok := values["read_view_bytes"]
	if !ok {
		t.Fatalf("nothing collected for read_view_bytes; got %v", values)
	}
	if int64(held) > bound+4096 {
		t.Fatalf("the view holds %v bytes, past its %d-byte bound by more than one request", held, bound)
	}
}

// The bound is on what the view *holds*, not on what the guest has written. A guest
// rewriting a working set that fits is the ordinary case — a database checkpointing the
// same pages, a filesystem journal — and it must never be throttled, however many bytes
// it puts through the log.
//
// This is the test that fails if the bound is put on anything cumulative: the appended
// bytes, the sequence, the segments retained. It writes 40x the bound and holds 1/16 of
// it.
func TestAGuestRewritingItsWorkingSetIsNeverRefused(t *testing.T) {
	const bound = 4 << 20
	l := viewLog(t, bound)

	block := make([]byte, 4096)
	const blocks = 64 // 256 KiB live, whatever the guest does to it
	for round := range 640 {
		for i := range blocks {
			if _, err := l.Write(uint64(i)*4096, block, 0); err != nil {
				t.Fatalf("round %d, block %d: a guest inside its bound was refused: %v", round, i, err)
			}
		}
	}
}

// A view over its bound refusing the only records that can shrink it is a volume with no
// way back: DISCARD and WRITE_ZEROES are what free the extents (§14.6), and a guest that
// trims must be able to write again afterwards.
//
// The proof is a WRITE that succeeds after the trim and reads back — not merely a DISCARD
// that returns nil.
func TestADiscardIsTakenWhileTheViewIsOverItsBound(t *testing.T) {
	const bound = 4 << 20
	l := viewLog(t, bound)

	taken, err := writeDistinct(t, l, 4096)
	if !errors.Is(err, wal.ErrBackpressure) {
		t.Fatalf("setup: want backpressure after %d blocks, got %v", taken, err)
	}

	// Trim the first half of what the guest wrote.
	half := uint64(taken/2) * 4096
	if _, err := l.Discard(0, uint32(half)); err != nil {
		t.Fatalf("a DISCARD was refused while the view was over its bound: %v", err)
	}

	// Not one write: the guest has to be able to go on working. A bound that let a
	// single record through and then closed again would satisfy one assertion and leave
	// the volume just as stuck.
	want := []byte("after the trim")
	block := make([]byte, 4096)
	for round := range 128 {
		for i := range taken / 4 {
			if _, err := l.Write(uint64(i)*4096, block, 0); err != nil {
				t.Fatalf("round %d, block %d: the guest is still stuck after trimming: %v", round, i, err)
			}
		}
	}
	if _, err := l.Write(0, want, 0); err != nil {
		t.Fatalf("a WRITE after the trim was still refused: %v", err)
	}
	got := make([]byte, len(want))
	if err := l.Read(0, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("read %q after the trim, want %q", got, want)
	}
}

// Zero means the default, not unbounded — the distinction this test exists for, because
// every Agent this repository has run had `MaxViewBytes` unset and no memory bound at
// all. A constant nothing applies is the same as no constant.
//
// It is the one heavy test here (it drives a view past 256 MiB with 32 MiB records, the
// cheapest way to get there) and it is worth its cost: with 0 read as "unbounded" this is
// the only assertion in the suite that goes red.
func TestALogWhoseCallerSetNoMemoryBoundStillHasOne(t *testing.T) {
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	l := wal.NewLog(sim.NewDisk(), "wal", clk, viewVol, 1, wal.Limits{SegmentBytes: 64 << 20})

	const record = 32 << 20
	block := make([]byte, record)
	var err error
	var taken int
	for i := range 32 { // 1 GiB of distinct offsets, 4x the default
		if _, err = l.Write(uint64(i)*record, block, 0); err != nil {
			taken = i
			break
		}
	}
	if !errors.Is(err, wal.ErrBackpressure) {
		t.Fatalf("a log built with wal.Limits{} took %d MiB without complaint (%v): "+
			"MaxViewBytes 0 is being read as unbounded", int64(taken)*record>>20, err)
	}
	if got := int64(taken) * record; got < wal.DefaultMaxViewBytes/2 {
		t.Fatalf("the default bound refused the guest after %d bytes, far short of %d",
			got, wal.DefaultMaxViewBytes)
	}
}
