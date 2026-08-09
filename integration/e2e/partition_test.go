//go:build e2e

package e2e

import (
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/testinfra"
)

// leaseTTL is what the Control Plane in this lane's fixture grants (-lease-ttl 30s).
// It is repeated here rather than plumbed, because the assertion below is about a
// deadline the *Agent* computes from what the Control Plane told it, and a test that
// took the number from the same variable the fixture passes could not tell the two
// apart.
const leaseTTL = 30 * time.Second

// proxy is a TCP relay a test can cut. It is how this lane partitions one process from
// another: killing the Control Plane would be a different experiment — the Agent would
// still be able to observe that it is gone, and gone is not what a partition looks like
// from inside a host. Here the Agent's connections stop carrying bytes and its dials stop
// completing, which is what a network in front of it failing actually does.
type proxy struct {
	ln net.Listener

	mu    sync.Mutex
	conns []net.Conn
	cut   bool
}

func startProxy(t *testing.T, target string) *proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{ln: ln}
	t.Cleanup(func() { p.cutOff() })

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", target)
			if err != nil {
				_ = c.Close()
				continue
			}
			p.track(c, up)
			go func() { _, _ = io.Copy(up, c) }()
			go func() { _, _ = io.Copy(c, up) }()
		}
	}()
	return p
}

func (p *proxy) track(conns ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cut {
		for _, c := range conns {
			_ = c.Close()
		}
		return
	}
	p.conns = append(p.conns, conns...)
}

func (p *proxy) url() string { return "http://" + p.ln.Addr().String() }

// cutOff is the partition. Both halves are needed: closing the listener only stops
// *new* dials, and the Agent's Connect client keeps its HTTP/1 connection alive across
// heartbeats — so a test that only closed the listener would go on heartbeating happily
// and prove nothing.
func (p *proxy) cutOff() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cut {
		return
	}
	p.cut = true
	_ = p.ln.Close()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

// TestAPartitionedAgentGivesUpItsSocket is the blocker, from outside the process.
//
// Partition a running Agent from the Control Plane and it looks dead to the fleet while
// its socket is still bound and its device still answering. After one lease TTL the
// Control Plane is entitled to give the volume to another host — and until the Agent gave
// it up on its own clock, that is two hosts serving one volume: two guests booted, both
// wrote, both fsynced, both were told it was durable, and nothing anywhere refused.
//
// The assertion is on the socket file, which is the thing a second guest would be started
// against. A field on the Agent saying "expired" next to a bound socket is the bug.
func TestAPartitionedAgentGivesUpItsSocket(t *testing.T) {
	d := start(t)

	cp, err := url.Parse(d.cpURL)
	if err != nil {
		t.Fatal(err)
	}
	px := startProxy(t, cp.Host)

	// The Agent reaches the Control Plane only through the proxy. Everything else — the
	// object store, its data directory — stays reachable, which is what makes this a
	// partition rather than a host failure.
	agent := testinfra.Start(t, testinfra.ProcessConfig{
		Name: "volume-agent",
		Path: d.agentBin,
		Args: append([]string{
			"-host-id", d.hostID,
			"-control-plane", px.url(),
			"-data-dir", d.dataDir,
			"-vhost-socket-dir", d.sockDir,
			"-kek-file", d.kekFile,
			"-heartbeat-interval", "1s",
		}, d.storeArgs()...),
		Env: d.agentEnv,
	})
	agent.WaitForLine(t, "key-encryption key loaded", startup)

	d.waitForHost(t)
	d.seedVolume(t)
	volumeID := waitForServedVolume(t, d)

	socket := filepath.Join(d.sockDir, volumeID+".sock")
	waitFor(t, startup, "the Agent to bind "+socket, func() bool {
		_, err := os.Stat(socket)
		return err == nil
	})

	px.cutOff()

	// One TTL plus slack for the cycle that notices. The slack is generous on purpose:
	// what is under test is that this happens at all, and a lane that failed because a
	// loaded CI runner took an extra second would be a lane nobody trusts.
	waitFor(t, leaseTTL+30*time.Second, "the partitioned Agent to give up "+socket, func() bool {
		_, err := os.Stat(socket)
		return os.IsNotExist(err)
	})

	// It gave the socket up as a *running process*, which is the whole shape of the
	// blocker: an Agent that had crashed would have released the socket too, and would
	// have been the easy case. Proven by the process still printing — its cycles keep
	// failing against a Control Plane it cannot reach, once per heartbeat interval.
	before := len(agent.Output())
	waitFor(t, 30*time.Second, "the Agent to print another line, proving it is still running", func() bool {
		return len(agent.Output()) > before
	})

	// And it said so. An operator whose guest just lost its disk has one question, and
	// the answer has to be in the log of the process that did it.
	if !anyLineContains(agent.Output(), "lease has expired") {
		t.Fatalf("the Agent gave up %s without saying why; an operator has no way to tell this from a crash", socket)
	}
}

func anyLineContains(lines []string, want string) bool {
	for _, l := range lines {
		if strings.Contains(l, want) {
			return true
		}
	}
	return false
}
