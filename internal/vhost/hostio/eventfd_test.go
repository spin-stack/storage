package hostio

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
)

// newTestEventFD wraps a freshly created eventfd the way the device wraps one
// that arrived over SET_VRING_KICK.
func newTestEventFD(t *testing.T) (*EventFD, *os.File) {
	t.Helper()
	f, err := NewCallEventFD()
	if err != nil {
		t.Fatalf("NewCallEventFD: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	e, err := NewEventFD(f)
	if err != nil {
		t.Fatalf("NewEventFD: %v", err)
	}
	return e.(*EventFD), f
}

func TestNewEventFDNeedsADescriptor(t *testing.T) {
	if _, err := NewEventFD(nil); err == nil {
		t.Fatal("want an error for a nil descriptor")
	}
}

// TestSignalWakesAWait is the doorbell, both directions: the value written is
// irrelevant, the wakeup is the point.
func TestSignalWakesAWait(t *testing.T) {
	e, _ := newTestEventFD(t)
	t.Cleanup(func() { _ = e.Close() })
	if err := e.Signal(); err != nil {
		t.Fatalf("Signal: %v", err)
	}
	if err := e.Wait(t.Context()); err != nil {
		t.Fatalf("Wait after Signal: %v", err)
	}
}

// TestEventFDDoesNotAdoptTheDescriptorItWasGiven. The Device owns the file that
// arrived over SCM_RIGHTS and closes it on reset; if NewEventFD adopted it
// rather than duplicating it, the Device's close would silently disarm a live
// doorbell (or, worse, close a descriptor number the runtime had reused).
func TestEventFDDoesNotAdoptTheDescriptorItWasGiven(t *testing.T) {
	e, f := newTestEventFD(t)
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := f.Write(make([]byte, 8)); err != nil {
		t.Fatalf("closing the EventFD closed the caller's descriptor: %v", err)
	}
}

// TestCloseUnblocksAWaitInFlight is the one that matters.
//
// The queue loop parks in Wait and is stopped by closing the kick — that is what
// server.go's shutdown path is built on, and its comment says so. It was not
// true: Wait held the EventFD's mutex across the blocking read, so Close queued
// behind it forever and the session hung on teardown. It only ever appeared to
// work because the session cancels its context first, and the deadline that
// cancellation installs is what actually released the read.
//
// A regression here does not fail the test, it hangs it; `go test -timeout`
// reports that as the same red.
func TestCloseUnblocksAWaitInFlight(t *testing.T) {
	e, _ := newTestEventFD(t)

	entered := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(entered)
		// Deliberately not t.Context(): the point is that Close alone stops a
		// waiter, with no cancellation helping.
		done <- e.Wait(t.Context())
	}()
	<-entered

	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-done; !errors.Is(err, io.EOF) {
		t.Fatalf("Wait returned %v, want io.EOF once the descriptor is closed", err)
	}
}

// TestCloseIsIdempotent: the queue loop's shutdown path and the session's
// teardown can both reach it.
func TestCloseIsIdempotent(t *testing.T) {
	e, _ := newTestEventFD(t)
	if err := e.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := e.Wait(t.Context()); !errors.Is(err, io.EOF) {
		t.Fatalf("Wait on a closed EventFD returned %v, want io.EOF", err)
	}
}

// TestWaitReturnsWhenTheContextIsCancelled: the other half of stopping a queue
// loop.
func TestWaitReturnsWhenTheContextIsCancelled(t *testing.T) {
	e, _ := newTestEventFD(t)
	t.Cleanup(func() { _ = e.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- e.Wait(ctx) }()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait returned %v, want context.Canceled", err)
	}
}
