package wal_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// TEST-GAPS (Open, known-weaker): "The uploader has no backoff between retries, so a
// coordinated throttle exhausts the budget faster than the backend recovers."
//
// A retry loop with no wait is not a retry loop: every attempt lands inside the same
// throttling window the previous one lost to, the whole budget is spent in the time
// it takes to make N round trips, and the FLUSH returns ErrUploadRetriesExhausted for
// an outage a one-second pause would have ridden out. The write is not lost — it
// stays in the local WAL and the next FLUSH retries — but durable_sequence stops
// advancing and the remote gap (the RPO an operator reads) grows for no reason.
//
// INV-01: the wait comes from the injected clock, never time.Sleep.

// stepClock is a clock whose Sleep returns immediately, advancing simulated time by
// exactly what was asked and recording it. sim.Clock's own Sleep blocks until another
// goroutine calls Advance — correct for the simulator, useless for asserting what a
// retry loop waited for.
type stepClock struct {
	*sim.Clock
	slept   []time.Duration
	onSleep func() error
}

func newStepClock() *stepClock {
	return &stepClock{Clock: sim.NewClock(time.Unix(1_700_000_000, 0).UTC())}
}

func (c *stepClock) Sleep(ctx context.Context, d time.Duration) error {
	c.slept = append(c.slept, d)
	c.Advance(d)
	if c.onSleep != nil {
		if err := c.onSleep(); err != nil {
			return err
		}
	}
	return ctx.Err()
}

var _ clock.Clock = (*stepClock)(nil)

// throttledUntil is a backend in a coordinated throttling window: every operation is
// refused until the clock passes a deadline. It is the shape the finding names — the
// backend recovers on its own after a while, and whether the uploader is still there
// to see it depends entirely on whether it waited.
type throttledUntil struct {
	*sim.ObjectStore
	clk   clock.Clock
	until clock.Instant
	ops   int
}

func (s *throttledUntil) throttling() bool {
	s.ops++
	return s.clk.Now() < s.until
}

func (s *throttledUntil) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	if s.throttling() {
		return objectstore.PutResult{}, sim.ErrThrottled
	}
	return s.ObjectStore.Put(ctx, key, data, opts)
}

func (s *throttledUntil) List(ctx context.Context, prefix string) ([]objectstore.ObjectInfo, error) {
	if s.throttling() {
		return nil, sim.ErrThrottled
	}
	return s.ObjectStore.List(ctx, prefix)
}

func oneBatch(first, last uint64) *wal.ClosedBatch {
	records, err := wal.Serialize([]wal.Record{{
		Type: 1, VolumeID: [16]byte{3}, Epoch: 1, Sequence: first, Offset: 0,
		Length: 4, Payload: []byte("data"),
	}})
	if err != nil {
		panic(err)
	}
	return &wal.ClosedBatch{
		VolumeID: [16]byte{3}, Epoch: 1, First: first, Last: last,
		Count: uint32(last - first + 1), Records: records,
	}
}

// TestUploadRidesOutAThrottleThatOutlastsTheRoundTrips is the finding: the same
// budget against the same outage succeeds with a backoff and is exhausted without
// one. Both arms use maxAttempts=4, so this is not "more retries" — it is the same
// number of retries spread over the time the backend needed.
func TestUploadRidesOutAThrottleThatOutlastsTheRoundTrips(t *testing.T) {
	const budget = 4

	t.Run("without a backoff the budget burns instantly", func(t *testing.T) {
		clk := newStepClock()
		store := &throttledUntil{ObjectStore: sim.NewObjectStore(), clk: clk, until: clock.Instant(500 * time.Millisecond)}
		u := wal.NewUploader(store, budget)
		if _, err := u.Upload(context.Background(), oneBatch(1, 1)); !errors.Is(err, wal.ErrUploadRetriesExhausted) {
			t.Fatalf("upload = %v, want ErrUploadRetriesExhausted", err)
		}
		if len(clk.slept) != 0 {
			t.Fatalf("an uploader with no backoff configured slept %v", clk.slept)
		}
	})

	t.Run("with a backoff the same budget outlasts it", func(t *testing.T) {
		clk := newStepClock()
		store := &throttledUntil{ObjectStore: sim.NewObjectStore(), clk: clk, until: clock.Instant(500 * time.Millisecond)}
		u := wal.NewUploader(store, budget, wal.WithBackoff(clk, wal.Backoff{Base: 100 * time.Millisecond, Max: time.Second}))
		key, err := u.Upload(context.Background(), oneBatch(1, 1))
		if err != nil {
			t.Fatalf("upload with a backoff: %v", err)
		}
		if key == "" {
			t.Fatal("upload returned an empty key")
		}
		// 100 + 200 + 400 = 700ms of simulated waiting across three retries.
		want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
		if len(clk.slept) != len(want) {
			t.Fatalf("slept %v, want %v", clk.slept, want)
		}
		for i, d := range want {
			if clk.slept[i] != d {
				t.Fatalf("wait %d = %v, want %v (slept %v)", i, clk.slept[i], d, clk.slept)
			}
		}
	})
}

// TestBackoffDelaySchedule pins the schedule itself: exponential from Base, capped at
// Max, no wait before the first attempt, and no randomness — a jittered delay would
// make two DST runs of one seed diverge, and spreading a fleet's retries is the
// caller's policy, not the WAL's.
func TestBackoffDelaySchedule(t *testing.T) {
	tests := []struct {
		name    string
		backoff wal.Backoff
		want    []time.Duration
	}{
		{
			name:    "exponential from base",
			backoff: wal.Backoff{Base: 10 * time.Millisecond, Max: time.Minute},
			want:    []time.Duration{0, 10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond, 80 * time.Millisecond},
		},
		{
			name:    "capped at max",
			backoff: wal.Backoff{Base: 10 * time.Millisecond, Max: 25 * time.Millisecond},
			want:    []time.Duration{0, 10 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond, 25 * time.Millisecond},
		},
		{
			name:    "no max is no cap",
			backoff: wal.Backoff{Base: time.Second},
			want:    []time.Duration{0, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second},
		},
		{
			name:    "a zero base is no wait at all",
			backoff: wal.Backoff{},
			want:    []time.Duration{0, 0, 0, 0, 0},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for attempt, want := range tc.want {
				if got := tc.backoff.Delay(attempt); got != want {
					t.Fatalf("Delay(%d) = %v, want %v", attempt, got, want)
				}
			}
		})
	}
}

// TestUploadBackoffStopsOnContextCancellation: a wait must not outlive the caller.
// The context half of this finding was already closed for the attempt loop; the sleep
// is the new place a cancelled FLUSH could otherwise sit for the whole schedule.
func TestUploadBackoffStopsOnContextCancellation(t *testing.T) {
	clk := newStepClock()
	ctx, cancel := context.WithCancel(context.Background())
	clk.onSleep = func() error { cancel(); return nil }

	store := &throttledUntil{ObjectStore: sim.NewObjectStore(), clk: clk, until: clock.Instant(time.Hour)}
	u := wal.NewUploader(store, 10, wal.WithBackoff(clk, wal.Backoff{Base: time.Second, Max: time.Minute}))
	if _, err := u.Upload(ctx, oneBatch(1, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("upload = %v, want context.Canceled", err)
	}
	if len(clk.slept) != 1 {
		t.Fatalf("the uploader kept waiting after the caller was gone: %v", clk.slept)
	}
}

// TestUploadDoesNotWaitAfterTheLastAttempt: waiting once the budget is spent delays
// the error the caller is going to get anyway, which on a FLUSH is guest-visible
// latency bought for nothing.
func TestUploadDoesNotWaitAfterTheLastAttempt(t *testing.T) {
	clk := newStepClock()
	store := &throttledUntil{ObjectStore: sim.NewObjectStore(), clk: clk, until: clock.Instant(time.Hour)}
	u := wal.NewUploader(store, 3, wal.WithBackoff(clk, wal.Backoff{Base: time.Second, Max: time.Minute}))
	if _, err := u.Upload(context.Background(), oneBatch(1, 1)); !errors.Is(err, wal.ErrUploadRetriesExhausted) {
		t.Fatalf("upload = %v, want ErrUploadRetriesExhausted", err)
	}
	if len(clk.slept) != 2 { // three attempts, two gaps
		t.Fatalf("slept %v across 3 attempts, want 2 waits", clk.slept)
	}
}

// TestUploadDoesNotWaitOnAHardFailure: a divergent object is not going to become
// non-divergent, so the schedule must not be spent on it.
func TestUploadDoesNotWaitOnAHardFailure(t *testing.T) {
	ctx := context.Background()
	clk := newStepClock()
	store := sim.NewObjectStore()
	u := wal.NewUploader(store, 5, wal.WithBackoff(clk, wal.Backoff{Base: time.Second, Max: time.Minute}))

	// A different writer already claims sequences 1-1 with different content, so its
	// key differs only in the content-hash suffix.
	first, err := u.Upload(ctx, oneBatch(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	other := oneBatch(1, 1)
	other.Records = append(append([]byte(nil), other.Records...), 0)
	if _, err := u.Upload(ctx, other); !errors.Is(err, wal.ErrDivergentObject) {
		t.Fatalf("upload over %q = %v, want ErrDivergentObject", first, err)
	}
	if len(clk.slept) != 0 {
		t.Fatalf("a hard failure was retried with waits: %v", clk.slept)
	}
}
