package vhost

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

// The tests in this file never poll and never sleep. §25.1/INV-01 keeps
// time.Now and time.Sleep out of this tree, and a test that waits on a
// condition by sampling it is also a test that is flaky on a loaded machine:
// every wait here is a channel the code under test actually closes. A genuine
// hang fails the run through go test's own timeout, which is the only clock
// this package needs.

// eventFactory hands out the fake eventfds the queue loop wires itself to, and
// announces when it has done so.
//
// The call event's hook is installed here, at construction, rather than by the
// test once `wired` closes. The queue loop drains and signals as soon as it
// exists — for a request published before the handshake, that is *before* the
// factory returns — so a hook installed afterwards both races the loop and
// misses the completion it was waiting for.
type eventFactory struct {
	onSignal func()

	mu    sync.Mutex
	kick  *fakeEvent
	call  *fakeEvent
	n     int
	wired chan struct{}
	// newKick carries every kick descriptor handed out, in order. A real guest
	// reconfigures the device at least once — firmware brings it up, then hands off
	// to the OS, which brings it up again with fresh rings and fresh eventfds — so
	// "the kick" is not one object for the life of a session. Buffered and never
	// drained by the factory, so a test can pick up the nth whenever it asks.
	newKick chan *fakeEvent
}

func newEventFactory() *eventFactory {
	return &eventFactory{wired: make(chan struct{}), newKick: make(chan *fakeEvent, 8)}
}

func (f *eventFactory) make(*os.File) (EventFD, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	e := newFakeEvent()
	// Odd descriptors are kicks and even ones calls, in the order queueLoop.ensure
	// asks for them.
	if f.n%2 == 1 {
		f.newKick <- e
	} else {
		e.signal = f.onSignal
	}
	switch f.n {
	case 1:
		f.kick = e
	case 2:
		f.call = e
		close(f.wired)
	}
	return e, nil
}

// nthKick blocks until the backend has asked for n kick descriptors and returns the
// nth. Blocking on the channel rather than polling a clock is not only INV-01: the
// queue loop wires itself up asynchronously, and any timeout a test picked would be a
// guess about a machine it is not running on.
func (f *eventFactory) nthKick(t *testing.T, n int) *fakeEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var e *fakeEvent
	for range n {
		select {
		case e = <-f.newKick:
		case <-ctx.Done():
			t.Fatalf("the backend never asked for kick descriptor %d", n)
		}
	}
	return e
}

func TestNewServerRejectsAnUnusableConfig(t *testing.T) {
	g := newFakeGuest(128)
	ok := Config{Backend: NewRawDevice(1 << 20), Mapper: &fakeMapper{g: g}}
	f := newEventFactory()
	tests := []struct {
		name    string
		ln      Listener
		cfg     Config
		eventFD EventFDFunc
	}{
		{"no listener", nil, ok, f.make},
		{"no event factory", newFakeListener(), ok, nil},
		{"a config no device can be built from", newFakeListener(), Config{}, f.make},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewServer(tc.ln, tc.cfg, tc.eventFD); err == nil {
				t.Fatal("want an error, got a server")
			}
		})
	}
}

// session starts a server over a simulated front-end and runs the handshake.
type session struct {
	g    *fakeGuest
	raw  *RawDevice
	conn *fakeConn
	ln   *fakeListener
	f    *eventFactory
	srv  *Server
	done chan error
	// completed receives once per notification the backend raises.
	completed chan struct{}
}

func newSession(t *testing.T) *session {
	t.Helper()
	g := newFakeGuest(128)
	s := &session{
		g:         g,
		raw:       NewRawDevice(testDeviceSize),
		conn:      newFakeConn(),
		f:         newEventFactory(),
		done:      make(chan error, 1),
		completed: make(chan struct{}, 64),
	}
	s.f.onSignal = func() { s.completed <- struct{}{} }
	s.ln = newFakeListener(s.conn)
	srv, err := NewServer(s.ln, Config{Backend: s.raw, Mapper: &fakeMapper{g: g}, QueueSize: g.num, Serial: "spin"}, s.f.make)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	s.srv = srv
	ctx := t.Context()
	go func() { s.done <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		_ = s.conn.Close()
		_ = s.ln.Close()
	})
	return s
}

// handshake pushes the front-end's sequence and drains the four replies the
// backend owes: GET_FEATURES, GET_PROTOCOL_FEATURES, GET_QUEUE_NUM, GET_CONFIG.
func (s *session) handshake(t *testing.T) {
	t.Helper()
	for _, m := range s.g.handshakeMessages() {
		s.conn.in <- m
	}
	for range 4 {
		<-s.conn.out
	}
	<-s.f.wired
}

// idle blocks until the queue loop has finished a drain and is waiting on the
// kick. Publishing into the ring before that edge exists is a data race with
// the loop's own reads — inherent to a shared virtqueue, and detectable here
// only because both halves are goroutines rather than processes.
func (s *session) idle() { <-s.f.kick.waiting }

// TestServerServesAGuestEndToEnd drives the whole session the way a front-end
// does: handshake over the connection, kick, completion, notification.
func TestServerServesAGuestEndToEnd(t *testing.T) {
	s := newSession(t)
	s.handshake(t)

	if d := s.srv.Device(); d == nil || !d.Ready() {
		t.Fatal("the device is not ready once the queue loop has wired itself up")
	}
	s.idle()

	data := pattern(0x9e, 2*SectorSize)
	bufs := s.g.publish(0, readable(blkHeader(blkTypeOut, 4)), readable(data), writable(1))
	if err := s.f.kick.Signal(); err != nil {
		t.Fatal(err)
	}
	<-s.completed

	if got := bufs[2][0]; got != blkStatusOK {
		t.Fatalf("status %d, want OK", got)
	}
	if got := s.raw.Snapshot(4*SectorSize, len(data)); !bytes.Equal(got, data) {
		t.Fatal("the device does not hold what the guest wrote")
	}
	if s.g.usedIdx() != 1 {
		t.Fatalf("used index %d, want 1", s.g.usedIdx())
	}

	// A front-end that disconnects ends the session cleanly; the server then
	// waits for the next one, and stops when the listener closes.
	_ = s.conn.Close()
	_ = s.ln.Close()
	if err := <-s.done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Serve: %v", err)
	}
}

// TestServerDrainsWorkQueuedBeforeTheKick. The kick is edge-triggered: a
// request published between the last drain and the wait would sit in the ring
// until some unrelated kick arrived, which is a hung guest.
func TestServerDrainsWorkQueuedBeforeTheKick(t *testing.T) {
	s := newSession(t)
	data := pattern(0x1d, SectorSize)
	// Publish before the queue loop exists, and never kick.
	s.g.publish(0, readable(blkHeader(blkTypeOut, 2)), readable(data), writable(1))

	s.handshake(t)
	<-s.completed

	if got := s.raw.Snapshot(2*SectorSize, len(data)); !bytes.Equal(got, data) {
		t.Fatal("a request queued before the kick was never served")
	}
}

// TestSessionEndsWhenTheQueueLoopFails: a device that stops completing requests
// while the handshake keeps answering is a hung guest with a healthy-looking
// backend.
func TestSessionEndsWhenTheQueueLoopFails(t *testing.T) {
	s := newSession(t)
	s.handshake(t)
	s.idle()

	// A descriptor pointing nowhere: fatal to the ring, and therefore to the
	// session.
	s.g.publish(0, readable(blkHeader(blkTypeFlush, 0)), writable(1))
	d, _ := readDesc(s.g.descTable(), 0)
	d.addr = 0xdead_0000_0000
	writeDesc(s.g.descTable(), 0, d)
	if err := s.f.kick.Signal(); err != nil {
		t.Fatal(err)
	}
	// No shutdown from the test: the queue loop's failure is what must end the
	// session, and a test that closed the connection itself would pass even if
	// it did not.
	if err := <-s.done; !errors.Is(err, ErrRing) {
		t.Fatalf("want ErrRing out of Serve, got %v", err)
	}
}

func TestSessionEndsOnAProtocolViolation(t *testing.T) {
	s := newSession(t)
	s.conn.in <- msg(ReqSetOwner, nil)
	s.conn.in <- msg(ReqGetInflightFd, make([]byte, 16))
	if err := <-s.done; !errors.Is(err, ErrProtocol) {
		t.Fatalf("want ErrProtocol, got %v", err)
	}
}

// TestServeReturnsWhenTheContextIsCancelled. Serve's contract is "accepts
// connections until ctx is done or the listener is closed", and the first half
// of that was never true: with no front-end attached, Serve is parked in
// Accept, which no cancellation reaches. An Agent that cannot stop serving a
// socket cannot be shut down — it is Increment 3.2's rolling restart failing
// before 3.2 is written.
//
// If this regresses the test does not fail, it hangs, and `go test -timeout`
// turns that into the same red.
func TestServeReturnsWhenTheContextIsCancelled(t *testing.T) {
	g := newFakeGuest(128)
	ln := newFakeListener()
	t.Cleanup(func() { _ = ln.Close() })
	srv, err := NewServer(ln, Config{Backend: NewRawDevice(testDeviceSize), Mapper: &fakeMapper{g: g}}, newEventFactory().make)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Serve returned %v, want context.Canceled", err)
	}
}

func TestServeStopsWhenTheListenerCloses(t *testing.T) {
	g := newFakeGuest(128)
	ln := newFakeListener()
	srv, err := NewServer(ln, Config{Backend: NewRawDevice(testDeviceSize), Mapper: &fakeMapper{g: g}}, newEventFactory().make)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(t.Context()) }()
	_ = ln.Close()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Serve: %v", err)
	}
}

// TestAReinitialisedDeviceIsStillServed is DEV-0018 reduced to its mechanism.
//
// Every real boot configures this device twice: the firmware brings it up, boots an OS,
// and the OS's driver brings it up again — GET_VRING_BASE to stop the queue, then the
// whole configuration replayed with **new** kick and call eventfds. The connection never
// drops, so this is not reconnection (3.2); it is one session with two set-ups.
//
// The queue loop started once and captured the first kick. After the hand-off it was
// parked on a descriptor nothing would ever signal again, so the second driver's very
// first request sat in the ring for ever. A real Linux guest hung with `virtio_blk`
// registered and the right capacity printed — and every test in this package passed,
// because the fake front-end only ever configured the device once.
func TestAReinitialisedDeviceIsStillServed(t *testing.T) {
	s := newSession(t)
	s.handshake(t)
	s.idle()

	// The hand-off. The guest's ring state goes with it: a fresh driver publishes from
	// index 0 into a ring the backend must also read from 0.
	for _, m := range s.g.reinitMessages() {
		s.conn.in <- m
	}

	// The second kick is the one that matters. Waiting for the backend to ask for it is
	// also the first assertion: a backend that never noticed the reconfiguration would
	// never ask.
	kick2 := s.f.nthKick(t, 2)
	// The same discipline s.idle() enforces for the first loop: publishing into the
	// ring before the loop is parked on the kick races its own reads. Inherent to a
	// shared virtqueue, and visible here only because both halves are goroutines.
	<-kick2.waiting

	data := pattern(0x5b, SectorSize)
	s.g.publish(0, readable(blkHeader(blkTypeOut, 8)), readable(data), writable(1))
	if err := kick2.Signal(); err != nil {
		t.Fatal(err)
	}

	served, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	select {
	case <-s.completed:
	case <-served.Done():
		t.Fatal("the request published after the device was re-initialised was never served")
	}
	if got := s.raw.Snapshot(8*SectorSize, len(data)); !bytes.Equal(got, data) {
		t.Fatal("the device does not hold what the second driver wrote")
	}
}
