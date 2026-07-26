package sim_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// The three standing backend defects below are not transient faults: they are a store
// that does not have a property the protocol assumes. Each is one configuration
// checkbox away in production, none of them reports an error, and each silently
// disables a fence — which is why the DST checkers are proven against them rather than
// against fabricated events.

func TestInjectPermanentDeleteRemovesTheBytes(t *testing.T) {
	ctx := context.Background()
	s := sim.NewObjectStore()
	if _, err := s.Put(ctx, "k", []byte("v"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	// A versioned bucket: the marker hides the object and Restore brings it back.
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if err := s.Restore(ctx, "k"); err != nil {
		t.Fatalf("a marked object must be restorable: %v", err)
	}

	// Versioning off: the same call destroys it.
	s.InjectPermanentDelete()
	if err := s.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "k"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("Get after a permanent delete = %v, want ErrNotFound", err)
	}
	if err := s.Restore(ctx, "k"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("Restore after a permanent delete = %v, want ErrNotFound", err)
	}
	if s.Marked("k") {
		t.Fatal("a permanently deleted key must not report as merely marked")
	}
}

func TestInjectIgnorePreconditionsMakesCreateOnlyAnOverwrite(t *testing.T) {
	ctx := context.Background()
	s := sim.NewObjectStore()
	if _, err := s.Put(ctx, "k", []byte("first"), objectstore.PutOptions{IfNoneMatch: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "k", []byte("second"), objectstore.PutOptions{IfNoneMatch: true}); !errors.Is(err, objectstore.ErrPreconditionFailed) {
		t.Fatalf("a conforming backend must refuse the second create-only PUT, got %v", err)
	}

	s.InjectIgnorePreconditions()
	if _, err := s.Put(ctx, "k", []byte("second"), objectstore.PutOptions{IfNoneMatch: true}); err != nil {
		t.Fatalf("with preconditions ignored the overwrite must land: %v", err)
	}
	if got, _ := s.Get(ctx, "k"); string(got) != "second" {
		t.Fatalf("object = %q, want the overwriting content", got)
	}
	// If-Match is advisory too: a stale ETag no longer protects anything.
	if _, err := s.Put(ctx, "k", []byte("third"), objectstore.PutOptions{IfMatch: "not-the-etag"}); err != nil {
		t.Fatalf("with preconditions ignored a stale If-Match must land: %v", err)
	}
}

func TestInjectStaleReadServesThePreviousVersion(t *testing.T) {
	ctx := context.Background()
	s := sim.NewObjectStore()
	if _, err := s.Put(ctx, "k", []byte("v1"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	first, err := s.Head(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "k", []byte("v2"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	s.InjectStaleRead("k")
	got, err := s.Get(ctx, "k")
	if err != nil || string(got) != "v1" {
		t.Fatalf("stale Get = %q err=%v, want the previous version", got, err)
	}
	// HEAD must agree with GET, otherwise a read that pairs the two (epoch.Current)
	// would detect the fault by accident rather than be fooled by it.
	info, err := s.Head(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if info.ETag != first.ETag {
		t.Fatalf("stale HEAD returned ETag %q, want the previous version's %q", info.ETag, first.ETag)
	}

	// A key whose only version is being hidden reads as absent: to that replica the
	// write has not happened.
	if _, err := s.Put(ctx, "fresh", []byte("v"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	s.InjectStaleRead("fresh")
	if _, err := s.Get(ctx, "fresh"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("a stale read with no earlier version = %v, want ErrNotFound", err)
	}

	s.ClearStaleRead("k")
	if got, _ := s.Get(ctx, "k"); string(got) != "v2" {
		t.Fatalf("after catching up Get = %q, want v2", got)
	}
}

// The monotonic clock is the one thing lease safety cannot recover from (§12.1), so
// the simulation has to be able to break it on purpose.
func TestInjectMonotonicRegression(t *testing.T) {
	tests := []struct {
		name     string
		advance  time.Duration
		regress  time.Duration
		wantMono time.Duration
	}{
		{name: "steps back by the requested amount", advance: 10 * time.Second, regress: 4 * time.Second, wantMono: 6 * time.Second},
		{name: "clamps at the origin", advance: 3 * time.Second, regress: 30 * time.Second, wantMono: 0},
		{name: "zero is a no-op", advance: 5 * time.Second, regress: 0, wantMono: 5 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
			c.Advance(tc.advance)
			c.InjectMonotonicRegression(tc.regress)
			if got := time.Duration(c.Now()); got != tc.wantMono {
				t.Fatalf("monotonic = %s, want %s", got, tc.wantMono)
			}
			// Wall time follows monotonic, so a regression is visible in both.
			wantWall := time.Unix(1_700_000_000, 0).UTC().Add(tc.wantMono)
			if got := c.Wall(); !got.Equal(wantWall) {
				t.Fatalf("wall = %s, want %s", got, wantWall)
			}
		})
	}
}
