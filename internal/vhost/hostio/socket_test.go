package hostio

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/spin-stack/storage/internal/vhost"
)

// The tests in this file are the only ones that see the wire. internal/vhost is
// proven against a simulated front-end, which by construction cannot tell us
// whether a descriptor survives SCM_RIGHTS or whether a header and its payload
// are framed the way QEMU writes them. That second question is settled for good
// only by integration/vhost against real QEMU; this file settles the first, and
// covers the failure paths a real front-end is not obliging enough to produce.

// socketPath keeps the path short. A Unix socket address is capped at 108 bytes
// by the kernel, and t.TempDir() builds its directory out of the *test's name*:
// the longest name in this file already spends 48 of them, subtests add more,
// and the failure is a bind error that reads like a permissions problem.
//
//nolint:usetesting // t.TempDir() is the rule; sun_path is 108 bytes and does not care.
func socketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "vhostsock")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s")
}

// frontEnd is the client half: what QEMU does to this socket, in Go.
type frontEnd struct {
	c *net.UnixConn
}

// dial connects to ln and returns both ends of the conversation.
func dial(t *testing.T, path string, ln vhost.Listener) (*frontEnd, vhost.Conn) {
	t.Helper()
	type accepted struct {
		c   vhost.Conn
		err error
	}
	ch := make(chan accepted, 1)
	go func() {
		c, err := ln.Accept()
		ch <- accepted{c, err}
	}()
	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial %s: %v", path, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	a := <-ch
	if a.err != nil {
		t.Fatalf("Accept: %v", a.err)
	}
	t.Cleanup(func() { _ = a.c.Close() })
	return &frontEnd{c: c.(*net.UnixConn)}, a.c
}

// send writes one vhost-user message, with its descriptors riding on the first
// byte the way QEMU sends them.
func (f *frontEnd) send(t *testing.T, m vhost.Message) {
	t.Helper()
	buf := make([]byte, vhost.HeaderSize+len(m.Payload))
	vhost.EncodeHeader(buf, m)
	copy(buf[vhost.HeaderSize:], m.Payload)
	var oob []byte
	if len(m.Files) > 0 {
		fds := make([]int, len(m.Files))
		for i, file := range m.Files {
			fds[i] = int(file.Fd())
		}
		oob = unix.UnixRights(fds...)
	}
	if _, _, err := f.c.WriteMsgUnix(buf, oob, nil); err != nil {
		t.Fatalf("WriteMsgUnix: %v", err)
	}
}

func (f *frontEnd) recv(t *testing.T) vhost.Message {
	t.Helper()
	var hdr [vhost.HeaderSize]byte
	if _, err := io.ReadFull(f.c, hdr[:]); err != nil {
		t.Fatalf("reading reply header: %v", err)
	}
	m, n, err := vhost.DecodeHeader(hdr[:])
	if err != nil {
		t.Fatalf("DecodeHeader: %v", err)
	}
	if n > 0 {
		m.Payload = make([]byte, n)
		if _, err := io.ReadFull(f.c, m.Payload); err != nil {
			t.Fatalf("reading reply payload: %v", err)
		}
	}
	return m
}

func listen(t *testing.T) (string, vhost.Listener) {
	t.Helper()
	path := socketPath(t)
	ln, err := Listen(path)
	if err != nil {
		t.Fatalf("Listen(%s): %v", path, err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return path, ln
}

func u64(v uint64) []byte {
	p := make([]byte, 8)
	binary.LittleEndian.PutUint64(p, v)
	return p
}

// TestMessagesRoundTripOverTheSocket: framing in both directions, including a
// payload, over a real SOCK_STREAM.
func TestMessagesRoundTripOverTheSocket(t *testing.T) {
	path, ln := listen(t)
	fe, be := dial(t, path, ln)

	tests := []struct {
		name    string
		request vhost.Request
		payload []byte
	}{
		{"no payload", vhost.ReqGetFeatures, nil},
		{"a u64 payload", vhost.ReqSetFeatures, u64(vhost.DeviceFeatures)},
		{"a large payload", vhost.ReqSetMemTable, make([]byte, 4096)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fe.send(t, vhost.Message{Request: tc.request, Flags: 1, Payload: tc.payload})
			got, err := be.Recv()
			if err != nil {
				t.Fatalf("Recv: %v", err)
			}
			if got.Request != tc.request {
				t.Fatalf("request %s, want %s", got.Request, tc.request)
			}
			if len(got.Payload) != len(tc.payload) {
				t.Fatalf("payload of %d bytes, want %d", len(got.Payload), len(tc.payload))
			}
			if err := be.Send(vhost.Message{Request: tc.request, Flags: 1 | 4, Payload: u64(7)}); err != nil {
				t.Fatalf("Send: %v", err)
			}
			reply := fe.recv(t)
			if !reply.IsReply() || binary.LittleEndian.Uint64(reply.Payload) != 7 {
				t.Fatalf("reply came back as %+v", reply)
			}
		})
	}
}

// TestDescriptorsSurviveTheCrossing is the reason this transport cannot live
// behind simio: the memory table, the kick and the call are kernel objects, and
// the only proof that one arrived is that it still works on this side.
func TestDescriptorsSurviveTheCrossing(t *testing.T) {
	path, ln := listen(t)
	fe, be := dial(t, path, ln)

	sent, err := NewCallEventFD()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sent.Close() }()

	fe.send(t, vhost.Message{Request: vhost.ReqSetVringKick, Flags: 1, Payload: u64(0), Files: []*os.File{sent}})
	m, err := be.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	defer m.CloseFiles()
	if len(m.Files) != 1 {
		t.Fatalf("message carries %d descriptors, want 1", len(m.Files))
	}
	// Signal the descriptor the *front-end* still holds and wait on the one that
	// crossed: same open file description, or this is a different eventfd.
	e, err := NewEventFD(m.Files[0])
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	if _, err := sent.Write(u64(1)); err != nil {
		t.Fatal(err)
	}
	if err := e.Wait(t.Context()); err != nil {
		t.Fatalf("the descriptor that crossed is not the one that was sent: %v", err)
	}
}

// TestRecvReportsAClosedFrontEndAsEOF. A front-end that goes away is the normal
// end of a session, not an error: Increment 3.2 turns this into a reconnection.
func TestRecvReportsAClosedFrontEndAsEOF(t *testing.T) {
	path, ln := listen(t)
	fe, be := dial(t, path, ln)
	_ = fe.c.Close()
	if _, err := be.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after the front-end closed: %v, want io.EOF", err)
	}
}

// TestRecvRefusesAHeaderItCannotFrame: a peer that writes fewer than twelve
// bytes and stops has desynchronized the stream, and there is no way back.
func TestRecvRefusesAHeaderItCannotFrame(t *testing.T) {
	path, ln := listen(t)
	fe, be := dial(t, path, ln)
	if _, err := fe.c.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	_ = fe.c.Close()
	if _, err := be.Recv(); !errors.Is(err, vhost.ErrProtocol) {
		t.Fatalf("Recv on a torn header: %v, want ErrProtocol", err)
	}
}

// TestRecvRefusesALengthItWouldHaveToAllocate: the length is a peer-controlled
// uint32, and trusting it is how a 12-byte message becomes four gigabytes.
func TestRecvRefusesALengthItWouldHaveToAllocate(t *testing.T) {
	path, ln := listen(t)
	fe, be := dial(t, path, ln)
	var hdr [vhost.HeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(vhost.ReqSetMemTable))
	binary.LittleEndian.PutUint32(hdr[4:8], 1)
	binary.LittleEndian.PutUint32(hdr[8:12], 0xffff_ffff)
	if _, err := fe.c.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := be.Recv(); !errors.Is(err, vhost.ErrProtocol) {
		t.Fatalf("Recv on an absurd length: %v, want ErrProtocol", err)
	}
}

// TestRecvRefusesATruncatedPayload: the header promised bytes the peer never
// wrote.
func TestRecvRefusesATruncatedPayload(t *testing.T) {
	path, ln := listen(t)
	fe, be := dial(t, path, ln)
	var hdr [vhost.HeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(vhost.ReqSetFeatures))
	binary.LittleEndian.PutUint32(hdr[4:8], 1)
	binary.LittleEndian.PutUint32(hdr[8:12], 8)
	if _, err := fe.c.Write(append(hdr[:], 1, 2, 3)); err != nil {
		t.Fatal(err)
	}
	_ = fe.c.Close()
	if _, err := be.Recv(); err == nil {
		t.Fatal("Recv accepted a payload that was never fully written")
	}
}

// TestRecvRefusesMoreDescriptorsThanAnyMessageNeeds. Eight is
// VHOST_MEMORY_BASELINE_NREGIONS; a peer that sends more is either broken or
// walking this process to its descriptor rlimit.
func TestRecvRefusesMoreDescriptorsThanAnyMessageNeeds(t *testing.T) {
	path, ln := listen(t)
	fe, be := dial(t, path, ln)

	files := make([]*os.File, maxFilesPerMessage+1)
	for i := range files {
		f, err := NewCallEventFD()
		if err != nil {
			t.Fatal(err)
		}
		files[i] = f
		defer func() { _ = f.Close() }()
	}
	fe.send(t, vhost.Message{Request: vhost.ReqSetMemTable, Flags: 1, Payload: u64(0), Files: files})
	if _, err := be.Recv(); !errors.Is(err, vhost.ErrProtocol) {
		t.Fatalf("Recv with %d descriptors: %v, want ErrProtocol", len(files), err)
	}
}

// TestSendRefusesToCarryDescriptors. The only back-end-to-front-end descriptor
// transfers are GET_INFLIGHT_FD (Increment 3.3) and the backend request channel,
// neither of which this backend advertises. Dropping them silently would produce
// a front-end waiting on a descriptor that never arrives.
func TestSendRefusesToCarryDescriptors(t *testing.T) {
	path, ln := listen(t)
	_, be := dial(t, path, ln)
	f, err := NewCallEventFD()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if err := be.Send(vhost.Message{Request: vhost.ReqGetFeatures, Flags: 5, Files: []*os.File{f}}); err == nil {
		t.Fatal("Send accepted descriptors this backend has no way to deliver")
	}
}

// TestListenRemovesAStaleSocket. A hard kill leaves the socket file behind, and
// bind fails with EADDRINUSE even though nothing is listening — which would turn
// every crash into a manual cleanup step before the Agent could come back.
func TestListenRemovesAStaleSocket(t *testing.T) {
	path := socketPath(t)
	first, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	// Close the listener but leave the file: this is what a SIGKILL looks like.
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	second, err := Listen(path)
	if err != nil {
		t.Fatalf("Listen over a stale socket file: %v", err)
	}
	_ = second.Close()
}

func TestListenRefusesAPathItCannotBind(t *testing.T) {
	if ln, err := Listen(filepath.Join(socketPath(t), "no", "such", "dir", "s")); err == nil {
		_ = ln.Close()
		t.Fatal("Listen succeeded on a path that does not exist")
	}
}

func TestAcceptReportsAClosedListener(t *testing.T) {
	_, ln := listen(t)
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Accept on a closed listener: %v, want net.ErrClosed", err)
	}
}
