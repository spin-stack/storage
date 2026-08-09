//go:build e2e

package e2e

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/testinfra"
)

// holdingLine is what the Agent prints, on a schedule, while it refuses to let go.
const holdingLine = "agent is holding unpublished data and will not release its data directory"

// TestAnAgentThatCannotPublishHoldsItsDataDirectory is the shutdown contract's second
// observable, as the owner decided it on 2026-08-04: the object store is unreachable and
// the Agent is SIGTERM'd, and the thing being asserted is that **it does not exit**.
//
// The whole design is mechanical. A flock is released by the kernel when the process
// exits, so "refuse to release the data directory" can only mean "do not exit" — and
// therefore the only honest proof is a *process* that is still there, still holding, and
// still saying so, with a bucket that still has nothing in it. Nothing in-process can
// stand in for that: a Go test can call Close and watch it block, but it cannot show that
// the lock the kernel owns is still refused to somebody else, and the second Agent below
// is exactly the "somebody else".
//
// Then the store comes back **without the Agent being touched**, and the image appears —
// and only then does it exit 0. That last ordering is the point of the increment: today's
// binary exits 0 immediately, with the bucket empty and the operator's supervisor told
// everything went fine.
//
// Proven able to fail. Making the teardown give up after the first failed attempt — which
// is what the code did before this increment — turns this red at the holding line:
//
//	agent-1 exited (exit status 0) without ever printing "agent is holding unpublished data…"
//
// The store is broken with a TCP proxy rather than by stopping the container, and that is
// not only convenience: `testcontainers` would give the Agent a *new* port when it came
// back, and "the store came back" has to be the same endpoint the Agent was configured
// with at start-up or the test is proving something about DNS instead.
func TestAnAgentThatCannotPublishHoldsItsDataDirectory(t *testing.T) {
	d := start(t)
	proxy := newStoreProxy(t, d.store.Endpoint)
	agent := d.startAgentThrough(t, "agent-1", proxy.endpoint)

	d.waitForHost(t)
	d.seedVolume(t)
	volumeID := waitForServedVolume(t, d)
	agent.waitForLine(t, "serving volume", startup)
	// The read view has to have landed *while the store was up*. A volume that never
	// resolved its base is refused for a different reason entirely — its image would be
	// missing everything it held before this session — and that refusal is not retried and
	// does not hold. Without this wait the scenario could pass having proven the other
	// rule.
	agent.waitForLine(t, "read view recovered from the object store", startup)

	if keys := volumeKeys(t, d, volumeID); len(keys) != 0 {
		t.Fatalf("%d object(s) for volume %s before the Agent stopped: the assertions below would be vacuous:\n%v",
			len(keys), volumeID, keys)
	}

	proxy.down()
	agent.signal(t, syscall.SIGTERM)

	// (1) It says so, more than once. A process deliberately refusing to die that says it
	// once and goes quiet is indistinguishable from one that hung.
	agent.waitForCount(t, holdingLine, 2, 90*time.Second)
	if code, done := agent.exited(); done {
		t.Fatalf("the Agent exited (%d) with nothing published for %s: a session that reached no bucket was released anyway", code, volumeID)
	}

	// (2) It still owns the data directory, and the witness is another process being
	// refused by the kernel — not a flag this one set.
	second := testinfra.Start(t, testinfra.ProcessConfig{
		Name: "agent-2",
		Path: d.agentBin,
		Args: append([]string{
			"-host-id", ids.New().String(),
			"-control-plane", d.cpURL,
			"-data-dir", d.dataDir, // the one agent-1 is holding
			"-vhost-socket-dir", d.sockDir,
			"-kek-file", d.kekFile,
		}, d.storeArgs()...),
		Env: d.agentEnv,
	})
	if err := second.Wait(t, 30*time.Second); err == nil {
		t.Fatal("a second Agent claimed the data directory the first is still holding unpublished data in (§10, DEV-0014)")
	}
	// Polled rather than read once. A process's last line and its exit are two events, and
	// nothing orders them: Wait returns when the kernel reaped it, while the line is still
	// in a pipe this test has not drained. Reading Output() at that instant passed on the
	// first run of this scenario and failed on the second, which is the definition of an
	// assertion that proves nothing.
	waitFor(t, 10*time.Second, "the second Agent's refusal to reach the log", func() bool {
		for _, line := range second.Output() {
			if strings.Contains(line, "already using") && strings.Contains(line, d.dataDir) {
				return true
			}
		}
		return false
	})

	// (3) And there is still nothing in the bucket, which is what makes (1) and (2) mean
	// "holding" rather than "published and lingering".
	if keys := volumeKeys(t, d, volumeID); len(keys) != 0 {
		t.Fatalf("volume %s has published %v while the store is unreachable", volumeID, keys)
	}

	// (4) The fleet can see it. The heartbeat is the difference between a host that is up
	// and stuck and a host that is gone, and it is the reason the owner chose holding over
	// exiting non-zero — so it is asserted where the fleet actually reads it, in the
	// catalog, rather than in a log line.
	beat := lastHeartbeat(t, d)
	waitFor(t, 30*time.Second, "the held host to heartbeat again", func() bool {
		return lastHeartbeat(t, d).After(beat)
	})

	// The store comes back, and nothing touches the Agent.
	proxy.up()

	if code := agent.waitExit(t, 120*time.Second); code != 0 {
		t.Fatalf("the Agent exited %d after publishing succeeded:\n%s", code, strings.Join(agent.output(), "\n"))
	}
	keys := volumeKeys(t, d, volumeID)
	if len(keys) == 0 {
		t.Fatalf("the Agent exited 0 and nothing was published for %s: the session it held on to for all that time was dropped at the end:\n%s",
			volumeID, strings.Join(agent.output(), "\n"))
	}
	t.Logf("the held session landed as %v", keys)
	agent.assertSaid(t, "volume-agent stopped")
}

// lastHeartbeat is when the Control Plane last heard from this host, as the catalog
// records it. Read from the database rather than from the Agent's own output: a host that
// prints "heartbeat" and reaches nobody is exactly the failure this is checking against.
func lastHeartbeat(t *testing.T, d *deployment) time.Time {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), d.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var at time.Time
	if err := pool.QueryRow(t.Context(),
		`SELECT last_heartbeat FROM hosts WHERE host_id = $1`, d.hostID).Scan(&at); err != nil {
		t.Fatalf("reading the host's last heartbeat: %v", err)
	}
	return at
}

// startAgentThrough starts an Agent whose object store is reached at endpoint — a proxy
// this test can break — and which is otherwise the Agent every other scenario starts.
//
// -shutdown-grace is short here because of what it *is*: the bound on one attempt, not on
// the wait. Nothing in this scenario needs it to expire, and a short value only makes the
// test's failure mode faster if a PUT ever hangs instead of failing.
func (d *deployment) startAgentThrough(t *testing.T, name, endpoint string) *heldAgent {
	t.Helper()
	a := startHeldAgent(t, name, d.agentBin, []string{
		"-host-id", d.hostID,
		"-control-plane", d.cpURL,
		"-data-dir", d.dataDir,
		"-vhost-socket-dir", d.sockDir,
		"-kek-file", d.kekFile,
		"-heartbeat-interval", "1s",
		"-shutdown-grace", "15s",
		"-s3-bucket", bucket,
		"-s3-endpoint", endpoint,
		"-s3-region", d.store.Region,
	}, d.agentEnv)
	a.waitForLine(t, "key-encryption key loaded", startup)
	return a
}

// --- a process this test can signal ---------------------------------------------
//
// heldAgent is testinfra.Process's shape with the three things this scenario needs and
// that type does not expose: sending a signal *without* waiting for the exit, asking
// whether the process is still there, and reading the exit code rather than asserting it
// is zero. `internal/testinfra` belongs to another track this wave, so it grew here
// instead; the two should collapse into one when Signal/ExitCode land there.

type heldAgent struct {
	name string
	cmd  *exec.Cmd

	mu     sync.Mutex
	lines  []string
	waiter chan struct{}

	done   chan struct{}
	pumped chan struct{}
	err    error
}

func startHeldAgent(t *testing.T, name, path string, args, env []string) *heldAgent {
	t.Helper()
	cmd := exec.Command(path, args...)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("%s: stdout: %v", name, err)
	}
	cmd.Stderr = cmd.Stdout

	a := &heldAgent{
		name: name, cmd: cmd,
		waiter: make(chan struct{}), done: make(chan struct{}), pumped: make(chan struct{}),
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("%s: starting %s: %v", name, path, err)
	}
	t.Logf("%s: started (pid %d): %s %s", name, cmd.Process.Pid, path, strings.Join(args, " "))

	go a.pump(t, out)
	go func() {
		a.err = cmd.Wait()
		close(a.done)
	}()
	t.Cleanup(func() {
		// SIGKILL and not SIGINT: this is the teardown, and the process under test is one
		// whose entire purpose is to refuse to exit on a signal. A polite cleanup would
		// hang the suite for as long as the scenario's failure lasted.
		_ = cmd.Process.Kill()
		<-a.done
		<-a.pumped
	})
	return a
}

func (a *heldAgent) pump(t *testing.T, r io.Reader) {
	defer close(a.pumped)
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		t.Logf("%s | %s", a.name, line)
		a.mu.Lock()
		a.lines = append(a.lines, line)
		close(a.waiter)
		a.waiter = make(chan struct{})
		a.mu.Unlock()
	}
}

func (a *heldAgent) output() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.lines...)
}

// count is how many lines so far contain want. The holding line is asserted by count and
// not by presence, because "it said it once" and "it keeps saying it" are different
// claims and only the second one is the design.
func (a *heldAgent) count(want string) int {
	n := 0
	for _, l := range a.output() {
		if strings.Contains(l, want) {
			n++
		}
	}
	return n
}

func (a *heldAgent) waitForCount(t *testing.T, want string, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		a.mu.Lock()
		next := a.waiter
		a.mu.Unlock()
		if a.count(want) >= n {
			return
		}
		select {
		case <-next:
		case <-a.done:
			if a.count(want) >= n {
				return
			}
			t.Fatalf("%s exited (%v) having printed %q %d time(s), wanted %d:\n%s",
				a.name, a.err, want, a.count(want), n, strings.Join(a.output(), "\n"))
		case <-deadline:
			t.Fatalf("%s printed %q %d time(s) in %s, wanted %d:\n%s",
				a.name, want, a.count(want), timeout, n, strings.Join(a.output(), "\n"))
		}
	}
}

func (a *heldAgent) waitForLine(t *testing.T, want string, timeout time.Duration) {
	t.Helper()
	a.waitForCount(t, want, 1, timeout)
}

func (a *heldAgent) assertSaid(t *testing.T, want string) {
	t.Helper()
	if a.count(want) == 0 {
		t.Fatalf("%s never printed %q:\n%s", a.name, want, strings.Join(a.output(), "\n"))
	}
}

func (a *heldAgent) signal(t *testing.T, sig os.Signal) {
	t.Helper()
	if err := a.cmd.Process.Signal(sig); err != nil {
		t.Fatalf("%s: %v: %v", a.name, sig, err)
	}
	t.Logf("%s: sent %v", a.name, sig)
}

// exited reports the exit code if the process is gone, and false while it is still there.
func (a *heldAgent) exited() (int, bool) {
	select {
	case <-a.done:
		return a.exitCode(), true
	default:
		return 0, false
	}
}

func (a *heldAgent) waitExit(t *testing.T, timeout time.Duration) int {
	t.Helper()
	select {
	case <-a.done:
		return a.exitCode()
	case <-time.After(timeout):
		t.Fatalf("%s did not exit within %s of the object store coming back:\n%s",
			a.name, timeout, strings.Join(a.output(), "\n"))
		return -1
	}
}

func (a *heldAgent) exitCode() int {
	var ee *exec.ExitError
	switch {
	case a.err == nil:
		return 0
	case errors.As(a.err, &ee):
		return ee.ExitCode()
	default:
		return -1
	}
}

// --- an object store that can be taken away and given back ----------------------

// storeProxy is a TCP proxy in front of the real backend. While it is down it accepts
// connections and closes them immediately, and it drops the ones already open — which is
// what an object store that has gone away looks like to an HTTP client with a warm
// connection pool. Leaving the listener bound the whole time is deliberate: the Agent was
// configured with this address at start-up and never re-reads it, so the address has to
// survive the outage for "the store came back" to mean what it says.
type storeProxy struct {
	endpoint string
	target   string

	mu    sync.Mutex
	down_ bool
	conns []net.Conn
}

func newStoreProxy(t *testing.T, target string) *storeProxy {
	t.Helper()
	u, err := url.Parse(target)
	if err != nil {
		t.Fatalf("parsing the object store endpoint %q: %v", target, err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &storeProxy{target: u.Host, endpoint: fmt.Sprintf("http://%s", ln.Addr().String())}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.handle(c)
		}
	}()
	t.Logf("object store proxy: %s -> %s", p.endpoint, target)
	return p
}

func (p *storeProxy) handle(c net.Conn) {
	up, err := func() (net.Conn, error) {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.down_ {
			return nil, errors.New("down")
		}
		up, err := net.Dial("tcp", p.target)
		if err != nil {
			return nil, err
		}
		p.conns = append(p.conns, c, up)
		return up, nil
	}()
	if err != nil {
		_ = c.Close()
		return
	}
	go func() { _, _ = io.Copy(up, c); _ = up.Close() }()
	_, _ = io.Copy(c, up)
	_ = c.Close()
}

func (p *storeProxy) down() {
	p.mu.Lock()
	p.down_ = true
	conns := p.conns
	p.conns = nil
	p.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (p *storeProxy) up() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.down_ = false
}
