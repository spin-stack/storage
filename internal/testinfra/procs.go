//go:build integration || e2e

package testinfra

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Running the real binaries, as processes.
//
// Every other lane in this repository drives Go types in-process. That is where the
// correctness proofs belong — but it means nothing has ever exercised the two things a
// deployment is made of: a `control-plane` and a `volume-agent` that were started with
// flags, that found each other over a socket, and that can be killed. `exec.Command`
// appears twice in the tree and both are QEMU.
//
// This is deliberately not a framework. It starts a binary, lets a test wait for a line
// it printed, and kills it. The "restart the Agent" arm of the e2e lane is a literal
// SIGKILL, because that is the failure ADR-0024 reasons about.

// Binary resolves one of this project's binaries in _output/bin and fails — naming the
// command that produces it — when it is not there.
//
// **A failure and never a skip.** It used to be either, decided by SPIN_REQUIRE_PROOFS:
// outside the merge gate a developer who had not run `task build:cmd` had not broken
// anything, and inside it a lane that quietly declined to start the two binaries was the
// gate reporting success for work it did not do. That mechanism existed for the artefacts
// a *guest* lane needed — a pinned QEMU, a kernel, an initramfs — which a laptop could
// reasonably not have, and it went with those lanes.
//
// It does not come back for this: `task test:e2e` depends on `build:cmd`, so the binaries
// are always built by the time a test asks for one, and the only way this site can fire is
// the lane and the build disagreeing about a name. That is a defect on every machine, and
// skipping it would hide it on all of them.
func Binary(t *testing.T, name string) string {
	t.Helper()
	root, err := repoRoot()
	if err != nil {
		t.Fatalf("locating the repository root: %v", err)
	}
	path := filepath.Join(root, "_output", "bin", name)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("%s is not built: %v\nrun: task build:cmd", path, err)
	}
	return path
}

// repoRoot walks up from the test's working directory to the module root. Tests run in
// their own package directory, and the binaries are at a path relative to the module.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("no go.mod found above the working directory")
		}
		dir = parent
	}
}

// Process is one running binary. Its combined output is tee'd to the test log as it
// arrives, so a failure shows what the process was doing rather than only that it
// exited — and `WaitForLine` reads the same stream, which is how a test waits for
// readiness without a sleep.
type Process struct {
	Name string

	cmd    *exec.Cmd
	cancel context.CancelFunc
	stdin  io.WriteCloser

	mu     sync.Mutex
	lines  []string
	waiter chan struct{} // closed and replaced on every new line

	done   chan struct{}
	pumped chan struct{}
	err    error
}

// ProcessConfig is what to start.
type ProcessConfig struct {
	// Name labels the process in the test log. Two Agents in one test are told apart
	// by this and nothing else.
	Name string
	Path string
	Args []string
	// Env is added to the parent environment. Credentials go here rather than on the
	// command line: storecfg takes them from the SDK's default chain precisely so they
	// do not land in `ps` output (see internal/storecfg).
	Env []string
	// Stdin gives the process a pipe on its standard input instead of /dev/null, so a
	// test can send it something. Exactly one caller needs it and it is the reason the
	// field exists: QEMU's `-serial stdio` wires this process's stdin to the guest's
	// ttyS0, which is the only channel a host test has to *tell a running guest*
	// anything (see StartLinuxGuest).
	//
	// Off by default rather than always on, because the two daemons are started the way
	// a supervisor starts them, and a supervisor hands a daemon /dev/null. A binary that
	// grew a stdin read would then block here and nowhere else, which is precisely the
	// kind of difference between the lane and production this file exists to remove.
	Stdin bool
}

// Start launches the process and arranges for it to be killed when the test ends.
func Start(t *testing.T, cfg ProcessConfig) *Process {
	t.Helper()

	// Not t.Context(): the process is killed from t.Cleanup, which runs after the test
	// context is cancelled — a cancelled context there would kill it before the test's
	// own teardown had a chance to look at it.
	ctx, cancel := context.WithCancel(context.Background()) //nolint:usetesting // see above

	cmd := exec.CommandContext(ctx, cfg.Path, cfg.Args...)
	cmd.Env = append(os.Environ(), cfg.Env...)
	// Kill, not interrupt: this is the teardown path, and a binary that hangs on
	// shutdown must not hang the test suite. Stop() is how a test asks politely.
	cmd.Cancel = func() error { return cmd.Process.Kill() }

	out, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatalf("%s: stdout: %v", cfg.Name, err)
	}
	cmd.Stderr = cmd.Stdout

	p := &Process{
		Name: cfg.Name, cmd: cmd, cancel: cancel,
		waiter: make(chan struct{}), done: make(chan struct{}), pumped: make(chan struct{}),
	}
	if cfg.Stdin {
		in, err := cmd.StdinPipe()
		if err != nil {
			cancel()
			t.Fatalf("%s: stdin: %v", cfg.Name, err)
		}
		p.stdin = in
	}
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("%s: starting %s: %v", cfg.Name, cfg.Path, err)
	}
	t.Logf("%s: started (pid %d): %s %s", cfg.Name, cmd.Process.Pid, cfg.Path, strings.Join(cfg.Args, " "))

	go p.pump(t, out)
	go func() {
		p.err = cmd.Wait()
		close(p.done)
	}()

	t.Cleanup(func() {
		cancel()
		<-p.done
		// And the pump, not only the process. cmd.Wait closes the read end after the
		// process exits, so the scanner is finishing at that instant — and it calls
		// t.Logf. A pump still draining after the last cleanup returns logs into a
		// finished test, which Go turns into a panic in whatever test runs next. It has
		// not bitten yet because a daemon's last words are one line; the guest lane
		// pumps a kernel's several hundred through here.
		<-p.pumped
	})
	return p
}

// pump tees the process's output to the test log and records it for WaitForLine.
func (p *Process) pump(t *testing.T, r io.Reader) {
	defer close(p.pumped)
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		t.Logf("%s | %s", p.Name, line)
		p.mu.Lock()
		p.lines = append(p.lines, line)
		close(p.waiter)
		p.waiter = make(chan struct{})
		p.mu.Unlock()
	}
}

// WaitForLine blocks until the process has printed a line containing want, the process
// exits, or the timeout elapses.
//
// Waiting on the process's own output rather than on a duration is the whole point: a
// `sleep 2` that usually works is the thing CLAUDE.md calls a stop signal, and it fails
// as a flake on a loaded machine rather than as a diagnosis.
func (p *Process) WaitForLine(t *testing.T, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		p.mu.Lock()
		for _, l := range p.lines {
			if strings.Contains(l, want) {
				p.mu.Unlock()
				return
			}
		}
		next := p.waiter
		p.mu.Unlock()

		select {
		case <-next:
		case <-p.done:
			// One more look: the line may have arrived in the same instant the process
			// exited, and reporting "never printed it" then would be false.
			p.mu.Lock()
			for _, l := range p.lines {
				if strings.Contains(l, want) {
					p.mu.Unlock()
					return
				}
			}
			p.mu.Unlock()
			t.Fatalf("%s exited (%v) without ever printing %q", p.Name, p.err, want)
		case <-deadline:
			t.Fatalf("%s did not print %q within %s", p.Name, want, timeout)
		}
	}
}

// WriteLine sends one line to the process's standard input, which exists only when
// ProcessConfig.Stdin asked for it.
//
// It fails the test when the write does not land, and that is the point rather than a
// convenience: the caller is telling something that is supposed to be running to do
// something, and EPIPE here means it was already gone. Swallowing that would turn "the
// guest obeyed" into "the guest was dead and nobody noticed", which is the exact class
// of vacuous assertion this lane exists to remove.
func (p *Process) WriteLine(t *testing.T, line string) {
	t.Helper()
	if p.stdin == nil {
		t.Fatalf("%s: WriteLine needs ProcessConfig.Stdin", p.Name)
	}
	if _, err := io.WriteString(p.stdin, line+"\n"); err != nil {
		t.Fatalf("%s: writing %q to stdin: %v (the process is gone?)", p.Name, line, err)
	}
}

// Kill stops the process the way a crash does: SIGKILL, no cleanup, no flush. This is
// the arm ADR-0024 is about — the WAL and the bucket are left exactly as they were at
// that instant, and the next incarnation has to make sense of them.
func (p *Process) Kill(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("%s: kill: %v", p.Name, err)
	}
	<-p.done
	t.Logf("%s: killed", p.Name)
}

// Stop asks the process to shut down and waits for it. It is the graceful counterpart
// to Kill: a test that means "the operator restarted it" should use this, so that a
// clean shutdown path staying clean is also covered.
func (p *Process) Stop(t *testing.T, timeout time.Duration) {
	t.Helper()
	if err := p.cmd.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("%s: interrupt: %v", p.Name, err)
	}
	select {
	case <-p.done:
	case <-time.After(timeout):
		p.cancel()
		<-p.done
		t.Fatalf("%s did not exit within %s of SIGINT; killed", p.Name, timeout)
	}
	// A graceful stop that exits non-zero is a failed stop, so the check belongs here
	// rather than in each caller. This is the half a supervisor sees: it sends the
	// signal and reads the status, and a daemon that tears down badly is only
	// distinguishable from one that tears down well by this number.
	if p.err != nil {
		t.Fatalf("%s exited badly after SIGINT: %v\n%s", p.Name, p.err, strings.Join(p.Output(), "\n"))
	}
	t.Logf("%s: stopped", p.Name)
}

// Wait blocks until the process exits and returns its error. It is for the binaries
// that are *supposed* to exit — `control-plane -seed-volume` provisions one volume and
// leaves.
func (p *Process) Wait(t *testing.T, timeout time.Duration) error {
	t.Helper()
	select {
	case <-p.done:
		return p.err
	case <-time.After(timeout):
		p.cancel()
		<-p.done
		return fmt.Errorf("%s did not exit within %s", p.Name, timeout)
	}
}

// Output returns everything the process has printed so far, for an assertion that has
// to look at the whole stream rather than wait for one line.
func (p *Process) Output() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.lines...)
}
