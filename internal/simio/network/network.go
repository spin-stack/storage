// Package network is the simulable message-transport interface (§25.1, INV-01).
// It provides length-delimited message exchange over named endpoints, enough to
// model Control-Plane <-> Agent messaging (§6) under simulation, including
// partitions (§12, §23). Production code depends on Network, never on the net
// package directly. The package is named `network` to avoid shadowing stdlib net.
package network

import (
	"context"
	"errors"
)

// Sentinel errors.
var (
	// ErrClosed is returned on operations against a closed conn/listener.
	ErrClosed = errors.New("simio/network: closed")
	// ErrPartitioned is returned when a message cannot be delivered because one
	// endpoint is partitioned from the other (simulation only).
	ErrPartitioned = errors.New("simio/network: partitioned")
	// ErrNoListener is returned when dialing an address with no listener.
	ErrNoListener = errors.New("simio/network: no listener at address")
)

// Network creates listeners and dials connections by address.
type Network interface {
	Listen(addr string) (Listener, error)
	Dial(ctx context.Context, addr string) (Conn, error)
}

// Listener accepts inbound connections.
type Listener interface {
	Accept(ctx context.Context) (Conn, error)
	Addr() string
	Close() error
}

// Conn is a bidirectional message stream.
type Conn interface {
	// Send delivers one message (length-delimited).
	Send(ctx context.Context, msg []byte) error
	// Recv returns the next message.
	Recv(ctx context.Context) ([]byte, error)
	Close() error
}
