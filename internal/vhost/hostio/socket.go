package hostio

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/spin-stack/storage/internal/vhost"
)

// maxFilesPerMessage bounds the descriptors one message may carry. The largest
// legitimate case is a SET_MEM_TABLE with VHOST_MEMORY_BASELINE_NREGIONS (8)
// regions; the bound exists so a peer cannot make this process accept
// descriptors until it hits its rlimit.
const maxFilesPerMessage = 8

// controlBufferSize is the ancillary-data buffer one Recv needs. It holds one
// descriptor *more* than any legitimate message carries, on purpose: the kernel
// silently drops whatever does not fit and sets MSG_CTRUNC, so a buffer sized to
// exactly the limit makes an over-limit message indistinguishable from a
// conforming one and leaves the count check below unreachable. With room for one
// extra, too many descriptors arrive intact and are refused by name.
var controlBufferSize = unix.CmsgSpace((maxFilesPerMessage + 1) * 4)

// Listen creates a listening Unix socket at path, removing a stale one left by
// a previous run.
//
// The backend listens and QEMU connects, which is the orientation Increment 3.2
// needs: a front-end that reconnects has something to reconnect *to* only if the
// socket outlives the backend process's connection, and QEMU's `reconnect=`
// chardev option is written for exactly this direction.
func Listen(path string) (vhost.Listener, error) {
	// A leftover socket file makes bind fail with EADDRINUSE even though
	// nothing is listening, which turns every restart after a hard kill into a
	// manual cleanup step.
	if err := unix.Unlink(path); err != nil && !errors.Is(err, unix.ENOENT) {
		return nil, fmt.Errorf("hostio: removing stale socket %s: %w", path, err)
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("hostio: listening on %s: %w", path, err)
	}
	return &listener{ln: ln.(*net.UnixListener)}, nil
}

type listener struct{ ln *net.UnixListener }

func (l *listener) Accept() (vhost.Conn, error) {
	c, err := l.ln.AcceptUnix()
	if err != nil {
		return nil, err
	}
	return &conn{c: c, ctrl: make([]byte, controlBufferSize)}, nil
}

func (l *listener) Close() error { return l.ln.Close() }

// conn is a vhost-user message stream over a Unix socket.
//
// Reads are serialized by their own mutex, writes by theirs: the session's
// message loop is the only reader, but a completion path may send while a reply
// is in flight, and a torn message on this socket desynchronizes the protocol
// permanently.
type conn struct {
	c *net.UnixConn

	rmu  sync.Mutex
	ctrl []byte
	hdr  [vhost.HeaderSize]byte

	wmu sync.Mutex
}

// Recv reads one message and the descriptors that came with it.
//
// The descriptors arrive with the *first* byte of the message, so the header
// read is the one that must collect ancillary data. Reading the header with a
// plain Read would drop them silently — SCM_RIGHTS with no buffer to land in is
// discarded by the kernel, and the failure appears much later as a memory table
// with regions and no files.
func (c *conn) Recv() (vhost.Message, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()

	n, oobn, flags, _, err := c.c.ReadMsgUnix(c.hdr[:], c.ctrl)
	if err != nil {
		return vhost.Message{}, err
	}
	files, err := parseFiles(c.ctrl[:oobn])
	if err != nil {
		return vhost.Message{}, err
	}
	// MSG_CTRUNC means the kernel dropped descriptors that did not fit. Carrying
	// on would mean serving a SET_MEM_TABLE whose regions have no files, and the
	// symptom — a device that maps nothing — points nowhere near here.
	if flags&unix.MSG_CTRUNC != 0 {
		closeAll(files)
		return vhost.Message{}, fmt.Errorf("%w: the message's descriptors were truncated in transit", vhost.ErrProtocol)
	}
	if n < vhost.HeaderSize {
		// A short header read means the peer sent a partial message or closed
		// mid-write. Either way this connection can no longer be framed.
		closeAll(files)
		if n == 0 {
			return vhost.Message{}, io.EOF
		}
		return vhost.Message{}, fmt.Errorf("%w: header read returned %d bytes", vhost.ErrProtocol, n)
	}
	m, size, err := vhost.DecodeHeader(c.hdr[:])
	if err != nil {
		closeAll(files)
		return vhost.Message{}, err
	}
	m.Files = files
	if size > 0 {
		m.Payload = make([]byte, size)
		if _, err := io.ReadFull(c.c, m.Payload); err != nil {
			closeAll(files)
			return vhost.Message{}, fmt.Errorf("hostio: reading %d-byte %s payload: %w", size, m.Request, err)
		}
	}
	return m, nil
}

// Send writes one message. Replies never carry descriptors in this backend —
// the only back-end-to-front-end fd transfers are GET_INFLIGHT_FD (Increment
// 3.3) and the backend request channel, neither of which is advertised — so
// Send deliberately refuses to send any, rather than silently dropping them.
func (c *conn) Send(m vhost.Message) error {
	if len(m.Files) != 0 {
		return fmt.Errorf("hostio: %s reply carries %d descriptors; this backend sends none", m.Request, len(m.Files))
	}
	buf := make([]byte, vhost.HeaderSize+len(m.Payload))
	vhost.EncodeHeader(buf, m)
	copy(buf[vhost.HeaderSize:], m.Payload)

	c.wmu.Lock()
	defer c.wmu.Unlock()
	if _, err := c.c.Write(buf); err != nil {
		return err
	}
	return nil
}

func (c *conn) Close() error { return c.c.Close() }

// parseFiles turns the ancillary data of a recvmsg into open files.
func parseFiles(oob []byte) ([]*os.File, error) {
	if len(oob) == 0 {
		return nil, nil
	}
	msgs, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return nil, fmt.Errorf("hostio: parsing ancillary data: %w", err)
	}
	var files []*os.File
	for _, msg := range msgs {
		fds, err := unix.ParseUnixRights(&msg)
		if err != nil {
			// Not an SCM_RIGHTS message. Skipping is right: SCM_CREDENTIALS
			// and friends are informational and carry nothing to leak.
			continue
		}
		for _, fd := range fds {
			files = append(files, os.NewFile(uintptr(fd), fmt.Sprintf("vhost-user-fd-%d", fd)))
		}
	}
	if len(files) > maxFilesPerMessage {
		closeAll(files)
		return nil, fmt.Errorf("%w: message carries %d descriptors (limit %d)", vhost.ErrProtocol, len(files), maxFilesPerMessage)
	}
	return files, nil
}

func closeAll(files []*os.File) {
	for _, f := range files {
		_ = f.Close()
	}
}
