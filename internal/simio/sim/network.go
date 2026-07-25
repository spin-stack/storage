package sim

import (
	"context"
	"sync"

	"github.com/spin-stack/storage/internal/simio/network"
)

// Network is a deterministic in-memory message network. Listeners are registered
// by address; Dial pairs a client conn with a server conn accepted by the
// listener. Partition drops delivery between isolated addresses (§12, §23).
type Network struct {
	mu         sync.Mutex
	listeners  map[string]*simListener
	partitions map[string]bool // addresses currently isolated
}

// NewNetwork returns an empty network.
func NewNetwork() *Network {
	return &Network{
		listeners:  map[string]*simListener{},
		partitions: map[string]bool{},
	}
}

// Partition isolates addr: sends to or from it fail with ErrPartitioned.
func (n *Network) Partition(addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitions[addr] = true
}

// Heal removes addr's partition.
func (n *Network) Heal(addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.partitions, addr)
}

func (n *Network) isPartitioned(addr string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.partitions[addr]
}

func (n *Network) Listen(addr string) (network.Listener, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	l := &simListener{net: n, addr: addr, incoming: make(chan *simConn, 16), done: make(chan struct{})}
	n.listeners[addr] = l
	return l, nil
}

func (n *Network) Dial(ctx context.Context, addr string) (network.Conn, error) {
	n.mu.Lock()
	l, ok := n.listeners[addr]
	n.mu.Unlock()
	if !ok {
		return nil, network.ErrNoListener
	}

	// A conn pair shares two directed queues. clientAddr is synthetic ("dialer").
	c2s := make(chan []byte, 64)
	s2c := make(chan []byte, 64)
	client := &simConn{net: n, localAddr: "dialer", remoteAddr: addr, in: s2c, out: c2s, done: make(chan struct{})}
	server := &simConn{net: n, localAddr: addr, remoteAddr: "dialer", in: c2s, out: s2c, done: make(chan struct{})}

	select {
	case l.incoming <- server:
		return client, nil
	case <-l.done:
		return nil, network.ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type simListener struct {
	net      *Network
	addr     string
	incoming chan *simConn
	done     chan struct{}
	once     sync.Once
}

func (l *simListener) Accept(ctx context.Context) (network.Conn, error) {
	select {
	case c := <-l.incoming:
		return c, nil
	case <-l.done:
		return nil, network.ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *simListener) Addr() string { return l.addr }

func (l *simListener) Close() error {
	l.once.Do(func() {
		close(l.done)
		l.net.mu.Lock()
		delete(l.net.listeners, l.addr)
		l.net.mu.Unlock()
	})
	return nil
}

type simConn struct {
	net        *Network
	localAddr  string
	remoteAddr string
	in         chan []byte
	out        chan []byte
	done       chan struct{}
	once       sync.Once
}

func (c *simConn) Send(ctx context.Context, msg []byte) error {
	if c.net.isPartitioned(c.localAddr) || c.net.isPartitioned(c.remoteAddr) {
		return network.ErrPartitioned
	}
	cp := append([]byte(nil), msg...)
	select {
	case c.out <- cp:
		return nil
	case <-c.done:
		return network.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *simConn) Recv(ctx context.Context) ([]byte, error) {
	select {
	case msg := <-c.in:
		return msg, nil
	case <-c.done:
		return nil, network.ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *simConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return nil
}
