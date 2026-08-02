package wal_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// BUILD-INVENTORY increment 5. Before this, a restart after truncation served zeros for
// every reclaimed range — no error, no degraded flag, no log line — because Log.view was
// rebuilt from local segments only and TruncateLocal unlinks exactly those.
//
// The setup below is the reproduction from VIEW-ADOPTION-SPEC.md, and the detail that
// makes it a test rather than a formality is SegmentBytes: reclaim unlinks only *sealed*
// segments, so a single record in the still-open one truncates nothing and the read comes
// back correct whatever the view does. Nine Resume tests missed this by not looking.

const baseTestSegmentBytes = 8192

// truncatedVolume writes six sealed segments, flushes them into the store, publishes and
// truncates — and asserts the truncation actually unlinked files, which is the whole
// premise.
func truncatedVolume(t *testing.T) (*sim.Disk, *sim.ObjectStore, *sim.Clock, [16]byte, uint64, []byte) {
	t.Helper()
	ctx := t.Context()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	store := sim.NewObjectStore()
	vol := [16]byte{0x77}

	lm := lease.NewManager(clk, time.Minute)
	lm.Grant()

	limits := wal.Limits{SegmentBytes: baseTestSegmentBytes}
	l := wal.NewLog(d, "wal", clk, vol, 1, limits)
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(store, 3), lm)

	payload := bytes.Repeat([]byte{0xAB}, 4096)
	for i := range 6 {
		if _, err := l.Write(uint64(i)*4096, payload, 0); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := l.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	w := l.Watermarks()

	before, _ := d.List("wal")
	if err := l.AdvancePublished(w.Durable); err != nil {
		t.Fatalf("AdvancePublished: %v", err)
	}
	if err := l.TruncateLocal(w.Durable); err != nil {
		t.Fatalf("TruncateLocal: %v", err)
	}
	after, _ := d.List("wal")
	if len(after) >= len(before) {
		t.Fatalf("truncation unlinked nothing (%d segments before, %d after): the test proves nothing",
			len(before), len(after))
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return d, store, clk, vol, w.Durable, payload
}

// TestAResumedVolumeReadsWhatTruncationReclaimed is the hole, closed.
func TestAResumedVolumeReadsWhatTruncationReclaimed(t *testing.T) {
	ctx := t.Context()
	d, store, clk, vol, _, payload := truncatedVolume(t)

	l, err := wal.ResumeAwaitingBase(d, "wal", clk, vol, 1,
		wal.Limits{SegmentBytes: baseTestSegmentBytes}, nil)
	if err != nil {
		t.Fatalf("ResumeAwaitingBase: %v", err)
	}
	defer func() { _ = l.Close() }()

	base, recovered, err := recovery.Recover(ctx, store, nil, vol, 1)
	if err != nil {
		t.Fatalf("recovery.Recover: %v", err)
	}
	if err := l.InstallBase(base, recovered); err != nil {
		t.Fatalf("InstallBase: %v", err)
	}

	got := make([]byte, len(payload))
	if err := l.Read(0, got); err != nil {
		t.Fatalf("read after resume: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("a resumed volume reads %x where %x was written, ACKed durable and verified",
			got[:8], payload[:8])
	}

	// The base brings the watermarks with it. published lands on the recovered point
	// because those objects are verified — which is what made the truncation legal, and
	// INV-13 would otherwise refuse to reclaim ranges the store already holds — and
	// local is raised to at least it, so the next append cannot reuse a sequence an
	// object already carries.
	w := l.Watermarks()
	if w.Published != recovered || w.Durable != recovered {
		t.Errorf("watermarks %+v after installing a base covering sequence %d", w, recovered)
	}
	if w.Local < recovered {
		t.Errorf("local=%d is below the recovered point %d: the next append would reuse a sequence",
			w.Local, recovered)
	}
}

// TestAResumedVolumeRefusesToServeZerosWhenTheBaseIsLost is decision 3 of the spec: an
// unreachable object store must make the volume refuse, not answer. Zeros where data
// belongs are indistinguishable from a fresh volume, and that is the failure mode this
// whole increment exists to remove.
func TestAResumedVolumeRefusesToServeZerosWhenTheBaseIsLost(t *testing.T) {
	d, _, clk, vol, _, payload := truncatedVolume(t)

	l, err := wal.ResumeAwaitingBase(d, "wal", clk, vol, 1,
		wal.Limits{SegmentBytes: baseTestSegmentBytes}, nil)
	if err != nil {
		t.Fatalf("ResumeAwaitingBase: %v", err)
	}
	defer func() { _ = l.Close() }()

	l.FailBase(errors.New("the object store is unreachable"))

	err = l.Read(0, make([]byte, len(payload)))
	if err == nil {
		t.Fatal("a read was answered with no base: the volume served zeros for reclaimed ranges")
	}
	if !errors.Is(err, wal.ErrBaseUnavailable) {
		t.Errorf("read failed with %v, want ErrBaseUnavailable", err)
	}
}

// TestReadsWaitForTheBaseRatherThanAnsweringEarly is what makes a lazy fetch safe. The
// volume is served immediately — that is the point of lazy — so the only thing standing
// between a guest and a wrong answer is that its read blocks.
func TestReadsWaitForTheBaseRatherThanAnsweringEarly(t *testing.T) {
	ctx := t.Context()
	d, store, clk, vol, _, payload := truncatedVolume(t)

	l, err := wal.ResumeAwaitingBase(d, "wal", clk, vol, 1,
		wal.Limits{SegmentBytes: baseTestSegmentBytes}, nil)
	if err != nil {
		t.Fatalf("ResumeAwaitingBase: %v", err)
	}
	defer func() { _ = l.Close() }()

	read := make(chan error, 1)
	got := make([]byte, len(payload))
	go func() { read <- l.Read(0, got) }()

	select {
	case err := <-read:
		t.Fatalf("the read was answered before the base arrived (err=%v, bytes %x)", err, got[:8])
	default:
	}

	base, recovered, err := recovery.Recover(ctx, store, nil, vol, 1)
	if err != nil {
		t.Fatalf("recovery.Recover: %v", err)
	}
	if err := l.InstallBase(base, recovered); err != nil {
		t.Fatalf("InstallBase: %v", err)
	}

	select {
	case err := <-read:
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("read %x, want %x", got[:8], payload[:8])
		}
	case <-ctx.Done():
		t.Fatal("the read never returned after the base was installed")
	}
}

// A DISCARD replayed from the local segments must survive the base arriving underneath
// it. This is the tombstone rule, tested where it actually bites: an unlayered view
// would have forgotten the discard, and the base would hand the guest back the bytes it
// asked to erase (§14.6).
func TestADiscardIsNotUndoneByTheBase(t *testing.T) {
	base := cow.NewIntervalMap()
	base.Overwrite(0, bytes.Repeat([]byte{0xAB}, 4096))

	d := sim.NewDisk()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := [16]byte{0x78}

	l, err := wal.ResumeAwaitingBase(d, "wal", clk, vol, 1, wal.Limits{}, nil)
	if err != nil {
		t.Fatalf("ResumeAwaitingBase: %v", err)
	}
	defer func() { _ = l.Close() }()

	if _, err := l.Discard(0, 4096); err != nil {
		t.Fatalf("Discard: %v", err)
	}
	if err := l.InstallBase(base, 0); err != nil {
		t.Fatalf("InstallBase: %v", err)
	}

	got := make([]byte, 4096)
	if err := l.Read(0, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, make([]byte, 4096)) {
		t.Fatalf("a discarded range came back as %x: the base undid the DISCARD (§14.6)", got[:8])
	}
}

// TestBasePendingIsTrueOnlyWhileTheBaseIsAwaited exists because of what it prevents. A
// resumed log reports durable = 0 until its base arrives, and a durability scheduler that
// checkpoints in that window compares the object store's real durable point against 0 and
// concludes another writer is in the epoch — fencing a healthy host out of its own volume
// on every restart (ADR-0023 acting on a false witness). The scheduler asks this.
func TestBasePendingIsTrueOnlyWhileTheBaseIsAwaited(t *testing.T) {
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())

	plain := wal.NewLog(sim.NewDisk(), "wal", clk, [16]byte{0x91}, 1, wal.Limits{})
	defer func() { _ = plain.Close() }()
	if plain.BasePending() {
		t.Error("a log that was never resumed is waiting for a base")
	}

	d := sim.NewDisk()
	l, err := wal.ResumeAwaitingBase(d, "wal", clk, [16]byte{0x92}, 1, wal.Limits{}, nil)
	if err != nil {
		t.Fatalf("ResumeAwaitingBase: %v", err)
	}
	defer func() { _ = l.Close() }()
	if !l.BasePending() {
		t.Fatal("a log resumed awaiting a base does not report it as pending")
	}
	if err := l.InstallBase(cow.NewIntervalMap(), 0); err != nil {
		t.Fatalf("InstallBase: %v", err)
	}
	if l.BasePending() {
		t.Error("the base is still pending after it was installed")
	}

	// A failed base resolves it too: the reads stop waiting, they just fail.
	l2, err := wal.ResumeAwaitingBase(d, "wal2", clk, [16]byte{0x93}, 1, wal.Limits{}, nil)
	if err != nil {
		t.Fatalf("ResumeAwaitingBase: %v", err)
	}
	defer func() { _ = l2.Close() }()
	l2.FailBase(errors.New("the store is gone"))
	if l2.BasePending() {
		t.Error("the base is still pending after the attempt failed")
	}
}
