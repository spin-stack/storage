package hostio

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/spin-stack/storage/internal/vhost"
)

// EventFD wraps a virtqueue notification descriptor.
//
// The descriptors QEMU sends over SET_VRING_KICK and SET_VRING_CALL are
// eventfds: an 8-byte counter where a read consumes and returns the accumulated
// count and blocks at zero, and a write adds to it. Wrapping them in an os.File
// puts them under the Go runtime's poller, which is what makes Wait cancellable
// — a blocking raw read would have to be interrupted with a signal.
type EventFD struct {
	f *os.File

	mu     sync.Mutex
	closed bool
	buf    [8]byte
}

// deadlineInThePast expires a pending read immediately. It is a fixed instant
// rather than "now" on purpose: §25.1/INV-01 forbids time.Now outside simio,
// and any moment in 1970 serves equally well as "already expired".
var deadlineInThePast = time.Unix(1, 0)

func deadlineNow() time.Time { return deadlineInThePast }

// NewEventFD wraps a notification descriptor received from the front-end. It
// duplicates rather than adopts: the original stays the Device's, and the copy
// is put into non-blocking mode *before* os.NewFile sees it.
//
// That ordering is the whole trick. os.NewFile registers a descriptor with the
// Go runtime's poller only if it is already non-blocking; a descriptor that
// arrives blocking gives a Wait that pins an OS thread and cannot be
// interrupted by Close or by a deadline — which is how a queue loop nobody can
// stop is born. QEMU does create its eventfds with EFD_NONBLOCK, and the flag
// does survive SCM_RIGHTS (it lives on the open file description), but a
// backend whose shutdown path depends on the front-end's flag choice is a
// backend that hangs the first time some other front-end makes a different one.
func NewEventFD(f *os.File) (vhost.EventFD, error) {
	if f == nil {
		return nil, errors.New("hostio: nil notification descriptor")
	}
	fd, err := unix.Dup(int(f.Fd()))
	if err != nil {
		return nil, fmt.Errorf("hostio: duplicating notification descriptor: %w", err)
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("hostio: making notification descriptor non-blocking: %w", err)
	}
	unix.CloseOnExec(fd)
	return &EventFD{f: os.NewFile(uintptr(fd), f.Name())}, nil
}

// Wait blocks until the descriptor is signalled. It returns io.EOF once the
// EventFD is closed, which is how the queue loop learns the session is over.
func (e *EventFD) Wait(ctx context.Context) error {
	// Cancellation goes through the deadline rather than a goroutine per wait:
	// a queue loop waits once per kick, and a goroutine per kick would be a
	// goroutine per guest I/O burst.
	stop := context.AfterFunc(ctx, func() { _ = e.f.SetReadDeadline(deadlineNow()) })
	defer stop()

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return io.EOF
	}
	if _, err := io.ReadFull(e.f, e.buf[:]); err != nil {
		if errors.Is(err, os.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return io.EOF
		}
		if errors.Is(err, os.ErrDeadlineExceeded) && ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("hostio: waiting on notification descriptor: %w", err)
	}
	return nil
}

// Signal wakes the other side. The value is 1 because an eventfd accumulates:
// the reader gets the number of signals it missed, and every value but 0 and
// the overflow sentinel behaves identically.
func (e *EventFD) Signal() error {
	var b [8]byte
	binary.NativeEndian.PutUint64(b[:], 1)
	if _, err := e.f.Write(b[:]); err != nil {
		return fmt.Errorf("hostio: signalling notification descriptor: %w", err)
	}
	return nil
}

// Close releases the descriptor and unblocks a Wait.
func (e *EventFD) Close() error {
	e.mu.Lock()
	already := e.closed
	e.closed = true
	e.mu.Unlock()
	if already {
		return nil
	}
	return e.f.Close()
}

// NewCallEventFD is an eventfd this process creates rather than receives. It
// exists for tests and for a front-end simulator: production only ever adopts
// descriptors QEMU sent.
func NewCallEventFD() (*os.File, error) {
	fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		return nil, fmt.Errorf("hostio: eventfd: %w", err)
	}
	return os.NewFile(uintptr(fd), "eventfd"), nil
}

var _ vhost.EventFD = (*EventFD)(nil)
