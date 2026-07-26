package vhost

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync"
	"testing"
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
}

func newEventFactory() *eventFactory { return &eventFactory{wired: make(chan struct{})} }

func (f *eventFactory) make(*os.File) (EventFD, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	e := newFakeEvent()
	switch f.n {
	case 1:
		f.kick = e
	case 2:
		e.signal = f.onSignal
		f.call = e
		close(f.wired)
	}
	return e, nil
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
