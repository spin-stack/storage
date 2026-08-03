package wal_test

import (
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

func remoteLeasedLog(t *testing.T, store *sim.ObjectStore, clk *sim.Clock, lm *lease.Manager) *wal.Log {
	t.Helper()
	d := sim.NewDisk()
	vol := [16]byte{7}
	l := wal.NewLog(d, "wal", clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(
		wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(store, 5),
		lm,
	)
	return l
}

// TestRemoteFlushWithALeaseButNoUploaderFailsClosed: `remote` is the default mode, so
// a log that was handed a lease but no remote path is in remote mode with nothing
// able to PUT. It ACKs the FLUSH and advances durable_sequence past anything S3 can
// produce — the same fail-open shape DEV-0004 removed, through a different door. A
// missing uploader is not "nothing to upload"; it is an unbacked durability claim.
func TestRemoteFlushWithALeaseButNoUploaderFailsClosed(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())

	d := sim.NewDisk()
	vol := [16]byte{13}
	l := wal.NewLog(d, "wal", clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(nil, nil, leaseOK{}) // a lease, no batcher, no uploader

	if _, err := l.Write(0, []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(ctx); err == nil {
		t.Fatal("a remote FLUSH with no uploader must not ACK: nothing can reach S3")
	}
	if w := l.Watermarks(); w.Durable != 0 {
		t.Fatalf("durable advanced to %d with an empty bucket", w.Durable)
	}
	if objs, _ := store.List(ctx, "wal/"); len(objs) != 0 {
		t.Fatalf("expected an empty bucket, got %d objects", len(objs))
	}
}

// TestFUAWriteSelfFencesWhenTheLeaseExpired: the FLUSH rule of §12.2 applies to the
// FUA ACK too — a write the guest is told is durable while this host no longer owns
// the volume is the split-brain the lease exists to prevent.
func TestFUAWriteSelfFencesWhenTheLeaseExpired(t *testing.T) {
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	lm := lease.NewManager(clk, 10*time.Second)
	lm.Grant()

	l := remoteLeasedLog(t, store, clk, lm)
	clk.Advance(11 * time.Second) // the lease expires before the write

	if _, err := l.Write(0, []byte("fua-payload"), format.FlagFUA); err == nil {
		t.Fatal("a FUA write must not complete while the host lease is invalid")
	}
	if l.Watermarks().Durable != 0 {
		t.Fatalf("durable advanced to %d on an unfenced FUA write", l.Watermarks().Durable)
	}
}

// TestFlushSelfFencesWhenLeaseExpired is INV-06 / §12.2: the object reaches S3, but
// because the lease is invalid at the instant of ACK, the write is NOT confirmed —
// durable does not advance and the log self-fences.
func TestFlushSelfFencesWhenLeaseExpired(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	lm := lease.NewManager(clk, 10*time.Second)
	lm.Grant()

	l := remoteLeasedLog(t, store, clk, lm)
	_, _ = l.Write(0, []byte("data"), 0)

	clk.Advance(11 * time.Second) // lease expires, no renewal

	err := l.Flush(ctx)
	if !errors.Is(err, wal.ErrSelfFenced) {
		t.Fatalf("expired lease flush: want ErrSelfFenced, got %v", err)
	}
	if l.Watermarks().Durable != 0 {
		t.Fatalf("durable must NOT advance with an invalid lease, got %d", l.Watermarks().Durable)
	}
	if !l.Fenced() {
		t.Fatal("log should have self-fenced")
	}
	// The object nonetheless landed in S3 (the §12.2 case: PUT succeeded, ACK did not).
	if objs, _ := store.List(ctx, "wal/"); len(objs) != 1 {
		t.Fatalf("object should be in S3 despite no ACK, got %d", len(objs))
	}
	// Once fenced, further flushes refuse.
	if err := l.Flush(ctx); !errors.Is(err, wal.ErrSelfFenced) {
		t.Fatalf("fenced log should keep refusing, got %v", err)
	}
}

// TestLocalModeIgnoresLeaseForFlush is §14.8 rule 3 / §23: a `local` volume ACKs a
// FLUSH after local fdatasync even with an expired lease (PG-down does not stop it).
func TestLocalModeIgnoresLeaseForFlush(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	lm := lease.NewManager(clk, 10*time.Second)
	lm.Grant()

	l := remoteLeasedLog(t, store, clk, lm)
	l.SetDurabilityMode(wal.ModeLocal)
	_, _ = l.Write(0, []byte("data"), 0)

	clk.Advance(30 * time.Second) // lease long expired
	if err := l.Flush(ctx); err != nil {
		t.Fatalf("local-mode flush must ACK regardless of the lease: %v", err)
	}
	if l.Fenced() {
		t.Fatal("local mode must not self-fence on FLUSH")
	}
}

// TestWriteFUASelfFencesWhenTheLeaseExpired: §12.2 gates the FUA ACK exactly as it
// gates the FLUSH ACK — the object may be in S3, but a host that no longer owns the
// volume must not confirm the write.
func TestWriteFUASelfFencesWhenTheLeaseExpired(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	lm := lease.NewManager(clk, 10*time.Second)
	lm.Grant()

	l := remoteLeasedLog(t, store, clk, lm)
	clk.Advance(11 * time.Second) // the lease expires with no renewal

	if _, err := l.WriteFUA(ctx, 0, []byte("fua-payload")); !errors.Is(err, wal.ErrSelfFenced) {
		t.Fatalf("want ErrSelfFenced, got %v", err)
	}
	if l.Watermarks().Durable != 0 {
		t.Fatalf("durable advanced to %d on an unfenced FUA write", l.Watermarks().Durable)
	}
	if !l.Fenced() {
		t.Fatal("the log should have self-fenced")
	}
}

// TestWriteFUAWithoutALeaseFailsClosed: DEV-0004 for the FUA path — a remote volume
// with nothing fencing it must not ACK.
func TestWriteFUAWithoutALeaseFailsClosed(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	vol := [16]byte{16}
	l := wal.NewLog(d, "wal", clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), nil)

	if _, err := l.WriteFUA(ctx, 0, []byte("fua-payload")); !errors.Is(err, wal.ErrNoLease) {
		t.Fatalf("want ErrNoLease, got %v", err)
	}
	if l.Watermarks().Durable != 0 {
		t.Fatalf("durable advanced to %d on a fenceless FUA write", l.Watermarks().Durable)
	}
}

// TestLocalModeWriteFUAAcksOnFdatasync: §14.8 rule 3 applies to FUA as well — a
// `local` volume ACKs on the local sync, and S3 catches up asynchronously.
func TestLocalModeWriteFUAAcksOnFdatasync(t *testing.T) {
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	l := wal.NewLog(d, "wal", clk, [16]byte{17}, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.SetDurabilityMode(wal.ModeLocal)

	seq, err := l.WriteFUA(ctx, 0, []byte("fua-payload"))
	if err != nil {
		t.Fatalf("a local-mode FUA write must ACK on fdatasync: %v", err)
	}
	if seq != 1 {
		t.Fatalf("sequence = %d, want 1", seq)
	}
	if l.Fenced() {
		t.Fatal("local mode must not self-fence")
	}
}

// TestModeForCoversEveryDurability: the data path and the Control Plane must agree on
// the §14.8 vocabulary. If a mode is added to lifecycle and not mapped here, this
// fails — the two ends cannot drift apart silently (ADR-0009).
func TestModeForCoversEveryDurability(t *testing.T) {
	for _, d := range lifecycle.Durabilities() {
		mode, err := wal.ModeFor(d)
		if err != nil {
			t.Fatalf("durability %q has no data-path mode: %v", d, err)
		}
		if got := mode.Durability(); got != d {
			t.Fatalf("round trip %q -> %v -> %q", d, mode, got)
		}
		if mode.String() != d.String() {
			t.Fatalf("String() = %q, want %q", mode.String(), d)
		}
	}
}

// TestModeForRejectsUnknown: an unmapped value fails closed instead of quietly
// promising remote durability the volume never asked for.
func TestModeForRejectsUnknown(t *testing.T) {
	if _, err := wal.ModeFor(lifecycle.Durability("eventual")); !errors.Is(err, lifecycle.ErrUnknownState) {
		t.Fatalf("want ErrUnknownState, got %v", err)
	}
}

// TestRemoteModeWithoutALeaseFailsClosed is DEV-0004: the durable-ACK rule of §12.2
// was opt-in — a remote-durability log built without a lease checker ACKed FLUSH with
// no lease at all. A missing fence must fail closed, not open.
func TestRemoteModeWithoutALeaseFailsClosed(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())

	d := sim.NewDisk()
	vol := [16]byte{7}
	l := wal.NewLog(d, "wal", clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), nil)

	if _, err := l.Write(0, []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(ctx); !errors.Is(err, wal.ErrNoLease) {
		t.Fatalf("remote FLUSH without a lease checker must fail closed, got %v", err)
	}
	if w := l.Watermarks(); w.Durable != 0 {
		t.Fatalf("durable advanced to %d on a fenceless flush", w.Durable)
	}
}

// TestLocalModeWithoutALeaseStillAcks is the other half of §14.8 rule 3: in local
// mode the lease does not gate the FLUSH ACK, so no lease is required.
func TestLocalModeWithoutALeaseStillAcks(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())

	d := sim.NewDisk()
	vol := [16]byte{8}
	l := wal.NewLog(d, "wal", clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 5), nil)
	l.SetDurabilityMode(wal.ModeLocal)

	if _, err := l.Write(0, []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(ctx); err != nil {
		t.Fatalf("local-mode FLUSH must ACK without a lease: %v", err)
	}
	if l.Fenced() {
		t.Fatal("local mode must not self-fence for a missing lease")
	}
	// The remote durable watermark stays put on purpose: in local mode the ACK is on
	// fdatasync and S3 catches up asynchronously (§14.8), which is what the RPO
	// metric measures.
}

// TestPublishedNeverWalksBackwards: the published point is the floor INV-13 enforces
// truncation against, and TruncateLocal has already discarded the records below it —
// they exist in a verified checkpoint and nowhere else on this host. Letting it move
// *down* therefore does not merely lose a number: it re-opens a range that has been
// reclaimed, so the next check compares a live truncation floor against a smaller
// published point and the WAL can be told to serve records whose bytes are gone.
//
// The reachable path is a stale listing (§24, and the DST harness models it): a
// checkpoint recomputes the published point from what S3 lists, and a listing that is
// momentarily behind proves less than the previous one did. The ordering rule guarded
// against durable and against nothing else, so the lower value was accepted with no
// error anywhere.
func TestPublishedNeverWalksBackwards(t *testing.T) {
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	lm := lease.NewManager(clk, 10*time.Second)
	lm.Grant()
	l := remoteLeasedLog(t, store, clk, lm)
	for range 3 {
		if _, err := l.Write(0, []byte("x"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := l.AdvancePublished(3); err != nil {
		t.Fatal(err)
	}
	if err := l.TruncateLocal(3); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		seq  uint64
		want error
	}{
		{name: "a stale listing proves less than the last one", seq: 0, want: wal.ErrWatermarkOrder},
		{name: "one object short of the published point", seq: 2, want: wal.ErrWatermarkOrder},
		{name: "the same point again is idempotent, not a regression", seq: 3, want: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := l.AdvancePublished(tc.seq)
			if !errors.Is(err, tc.want) {
				t.Fatalf("AdvancePublished(%d) = %v, want %v", tc.seq, err, tc.want)
			}
			if got := l.Watermarks().Published; got < 3 {
				t.Fatalf("published fell to %d under WAL already reclaimed to 3", got)
			}
		})
	}
}
