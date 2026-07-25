package real

import (
	"context"
	"encoding/binary"
	"io"
	"net"

	"github.com/spin-stack/storage/internal/simio/network"
)

// Network is a TCP-backed message transport with 4-byte length-delimited frames.
// It is used where a real socket is needed (dev, integration); the Control-Plane
// RPC surface itself is gRPC in later phases.
type Network struct{}

// NewNetwork returns a TCP-backed network.
func NewNetwork() *Network { return &Network{} }

func (*Network) Listen(addr string) (network.Listener, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &realListener{l: l}, nil
}

func (*Network) Dial(ctx context.Context, addr string) (network.Conn, error) {
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return &realConn{c: c}, nil
}

type realListener struct {
	l net.Listener
}

func (r *realListener) Accept(_ context.Context) (network.Conn, error) {
	c, err := r.l.Accept()
	if err != nil {
		return nil, err
	}
	return &realConn{c: c}, nil
}

func (r *realListener) Addr() string { return r.l.Addr().String() }
func (r *realListener) Close() error { return r.l.Close() }

type realConn struct {
	c net.Conn
}

func (r *realConn) Send(_ context.Context, msg []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(msg)))
	if _, err := r.c.Write(hdr[:]); err != nil {
		return err
	}
	_, err := r.c.Write(msg)
	return err
}

func (r *realConn) Recv(_ context.Context) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r.c, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	buf := make([]byte, n)
	if _, err := io.ReadFull(r.c, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (r *realConn) Close() error { return r.c.Close() }
