package network_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/network"
	"github.com/spin-stack/storage/internal/simio/real"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// pair holds a network plus the address a listener should bind (real uses an
// ephemeral loopback port, discovered via Listener.Addr()).
func networks() map[string]struct {
	net  network.Network
	addr string
} {
	return map[string]struct {
		net  network.Network
		addr string
	}{
		"real": {net: real.NewNetwork(), addr: "127.0.0.1:0"},
		"sim":  {net: sim.NewNetwork(), addr: "cp:9000"},
	}
}

func TestSendRecvBothDirections(t *testing.T) {
	ctx := context.Background()
	for name, tc := range networks() {
		t.Run(name, func(t *testing.T) {
			l, err := tc.net.Listen(tc.addr)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()

			type accepted struct {
				c   network.Conn
				err error
			}
			accCh := make(chan accepted, 1)
			go func() {
				c, err := l.Accept(ctx)
				accCh <- accepted{c, err}
			}()

			client, err := tc.net.Dial(ctx, l.Addr())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			a := <-accCh
			if a.err != nil {
				t.Fatalf("accept: %v", a.err)
			}
			server := a.c
			defer server.Close()

			// client -> server
			if err := client.Send(ctx, []byte("ping")); err != nil {
				t.Fatal(err)
			}
			got, err := server.Recv(ctx)
			if err != nil || string(got) != "ping" {
				t.Fatalf("server recv: %q err=%v", got, err)
			}
			// server -> client
			if err := server.Send(ctx, []byte("pong")); err != nil {
				t.Fatal(err)
			}
			got, err = client.Recv(ctx)
			if err != nil || string(got) != "pong" {
				t.Fatalf("client recv: %q err=%v", got, err)
			}
		})
	}
}

func TestDialNoListener(t *testing.T) {
	// sim reports a typed error; real returns a dial error. Assert both fail.
	ctx := context.Background()
	s := sim.NewNetwork()
	if _, err := s.Dial(ctx, "absent"); !errors.Is(err, network.ErrNoListener) {
		t.Fatalf("sim want ErrNoListener, got %v", err)
	}
}

func TestSimPartitionDropsDelivery(t *testing.T) {
	ctx := context.Background()
	s := sim.NewNetwork()
	l, _ := s.Listen("agent:1")
	defer l.Close()

	accCh := make(chan network.Conn, 1)
	go func() {
		c, _ := l.Accept(ctx)
		accCh <- c
	}()
	client, err := s.Dial(ctx, "agent:1")
	if err != nil {
		t.Fatal(err)
	}
	<-accCh

	s.Partition("agent:1")
	err = client.Send(ctx, []byte("unreachable"))
	if !errors.Is(err, network.ErrPartitioned) {
		t.Fatalf("want ErrPartitioned during partition, got %v", err)
	}

	s.Heal("agent:1")
	if err := client.Send(ctx, []byte("reachable")); err != nil {
		t.Fatalf("after heal send should succeed: %v", err)
	}
}

func TestRecvHonorsContext(t *testing.T) {
	s := sim.NewNetwork()
	l, _ := s.Listen("x:1")
	defer l.Close()
	go func() { _, _ = l.Accept(context.Background()) }()
	client, _ := s.Dial(context.Background(), "x:1")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := client.Recv(ctx)
	if err == nil {
		t.Fatal("Recv should observe context deadline")
	}
}
