package vhost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
)

// Conn is one vhost-user connection: a message stream that also carries file
// descriptors. It is defined here rather than taken from simio/network because
// the descriptors are the point — see the package doc on INV-01.
type Conn interface {
	// Recv returns the next message, or io.EOF when the front-end disconnects.
	Recv() (Message, error)
	// Send writes one message.
	Send(Message) error
	Close() error
}

// Listener accepts vhost-user connections. vhost-user is one front-end per
// socket, so the server serves them one at a time.
type Listener interface {
	Accept() (Conn, error)
	Close() error
}

// EventFD is a virtqueue notification descriptor: the front-end's kick (the
// guest has queued work) or the backend's call (a completion is ready). The
// production implementation wraps an eventfd; a test uses a channel, which is
// what lets the whole session loop run without a kernel object.
type EventFD interface {
	// Wait blocks until the descriptor is signalled, ctx is done, or it is
	// closed (which returns io.EOF).
	Wait(ctx context.Context) error
	// Signal wakes whoever is waiting on the other side.
	Signal() error
	Close() error
}

// EventFDFunc adapts a descriptor received over SET_VRING_KICK/CALL into an
// EventFD.
type EventFDFunc func(*os.File) (EventFD, error)

// Server serves one vhost-user socket. Increment 3.1 stops at "the front-end
// disconnected": reconnection with the guest still running is Increment 3.2,
// and pretending to do it here would mean a device that comes back without the
// in-flight requests it owed the guest.
type Server struct {
	ln      Listener
	cfg     Config
	eventFD EventFDFunc

	mu      sync.Mutex
	current *Device
}

// NewServer serves cfg's backend on ln. eventFD turns the kick/call descriptors
// the front-end sends into waitable objects.
func NewServer(ln Listener, cfg Config, eventFD EventFDFunc) (*Server, error) {
	if ln == nil {
		return nil, fmt.Errorf("vhost: a listener is required")
	}
	if eventFD == nil {
		return nil, fmt.Errorf("vhost: an EventFDFunc is required")
	}
	// Build a device once up front so a bad Config fails at construction
	// rather than on the first connection, when the guest is already booting.
	if _, err := NewDevice(cfg); err != nil {
		return nil, err
	}
	return &Server{ln: ln, cfg: cfg, eventFD: eventFD}, nil
}

// Device returns the device serving the current connection, or nil. It is how a
// test — including the QEMU lane — asks what the front-end actually negotiated.
func (s *Server) Device() *Device {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current
}

// Serve accepts connections until ctx is done or the listener is closed.
func (s *Server) Serve(ctx context.Context) error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
				return ctx.Err()
			}
			return fmt.Errorf("vhost: accept: %w", err)
		}
		if err := s.session(ctx, conn); err != nil && ctx.Err() == nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// session runs one front-end connection to completion.
//
// Two loops share the device: this one, which answers configuration messages,
// and a queue loop that waits on the kick descriptor and drains the ring. They
// are separate because the front-end keeps sending configuration while the
// guest is doing I/O, and serving a ring from inside the message loop would
// stall the handshake behind a slow backend.
func (s *Server) session(ctx context.Context, conn Conn) (err error) {
	dev, err := NewDevice(s.cfg)
	if err != nil {
		_ = conn.Close()
		return err
	}
	s.mu.Lock()
	s.current = dev
	s.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	// A queue loop that dies takes the connection down with it. Without this
	// the message loop keeps answering configuration messages over a device
	// that no longer completes anything, and the front-end sees a healthy
	// backend attached to a hung guest.
	q := &queueLoop{dev: dev, eventFD: s.eventFD, onFail: func() { _ = conn.Close() }}
	defer func() {
		cancel()
		q.stop()
		// A queue loop that died took the data path with it. Its error is the
		// session's unless the message loop already produced one: a device that
		// stops completing requests while the handshake keeps answering is the
		// failure mode that looks like a hung guest.
		if err == nil {
			err = q.failure()
		}
		dev.Close()
		_ = conn.Close()
	}()

	for {
		m, rerr := conn.Recv()
		if rerr != nil {
			if errors.Is(rerr, io.EOF) || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("vhost: receive: %w", rerr)
		}
		reply, herr := dev.Handle(ctx, m)
		if herr != nil {
			return herr
		}
		if reply != nil {
			if serr := conn.Send(*reply); serr != nil {
				return fmt.Errorf("vhost: send %s reply: %w", reply.Request, serr)
			}
		}
		// The kick descriptor is the last thing a front-end sets before the
		// queue goes live, so this is where the data path starts. Re-checking
		// after every message (rather than only after SET_VRING_KICK) also
		// covers the front-end that enables the queue afterwards.
		if err := q.ensure(ctx); err != nil {
			return err
		}
	}
}

// queueLoop owns the goroutine that drains the virtqueue.
type queueLoop struct {
	dev     *Device
	eventFD EventFDFunc
	onFail  func()

	mu      sync.Mutex
	started bool
	kick    EventFD
	call    EventFD
	done    chan struct{}
	err     error
}

// ensure starts the queue loop once the device is ready and has a kick.
func (q *queueLoop) ensure(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.started {
		return q.err
	}
	kickFD := q.dev.Kick()
	if kickFD == nil || !q.dev.Ready() {
		return nil
	}
	kick, err := q.eventFD(kickFD)
	if err != nil {
		return fmt.Errorf("vhost: kick descriptor: %w", err)
	}
	var call EventFD
	if f := q.dev.Call(); f != nil {
		if call, err = q.eventFD(f); err != nil {
			_ = kick.Close()
			return fmt.Errorf("vhost: call descriptor: %w", err)
		}
	}
	q.kick, q.call, q.started = kick, call, true
	q.done = make(chan struct{})
	go q.run(ctx)
	return nil
}

func (q *queueLoop) run(ctx context.Context) {
	defer close(q.done)
	for {
		// Drain before waiting. The kick is edge-triggered and coalescing: the
		// guest may have queued several requests behind one signal, and it may
		// have queued one *after* our last drain and before this wait, in which
		// case waiting first would hang the device until the next unrelated
		// kick.
		if err := q.drain(ctx); err != nil {
			q.fail(err)
			return
		}
		if err := q.kick.Wait(ctx); err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				q.fail(err)
			}
			return
		}
	}
}

func (q *queueLoop) drain(ctx context.Context) error {
	served, notify, err := q.dev.ProcessQueue(ctx)
	if err != nil {
		return err
	}
	if served > 0 && notify && q.call != nil {
		if err := q.call.Signal(); err != nil {
			return fmt.Errorf("vhost: signalling %d completions: %w", served, err)
		}
	}
	return nil
}

func (q *queueLoop) fail(err error) {
	q.mu.Lock()
	first := q.err == nil
	if first {
		q.err = err
	}
	q.mu.Unlock()
	if first && q.onFail != nil {
		q.onFail()
	}
}

// failure reports the error that stopped the queue loop, if any.
func (q *queueLoop) failure() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.err
}

func (q *queueLoop) stop() {
	q.mu.Lock()
	kick, call, done, started := q.kick, q.call, q.done, q.started
	q.mu.Unlock()
	if !started {
		return
	}
	// Closing the kick is what unblocks Wait; the loop then exits on its own.
	_ = kick.Close()
	<-done
	if call != nil {
		_ = call.Close()
	}
}
