//go:build integration || e2e

package testinfra

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// This file is the QEMU guest, available to any lane rather than to one.
//
// It moved here from integration/vhost when the e2e lane needed it. That lane boots a
// guest against a device an *in-process* agent.VolumeManager serves; the e2e lane needs
// the same guest against the socket a real volume-agent process bound, because that is
// the only way to observe the binary's own wiring — its flags, its S3 credentials, its
// Control-Plane-driven lease — deciding a durable ACK. Two copies of a QEMU command line
// would drift, and the one that drifted would be the one nobody ran.

// GuestBootTimeout is generous on purpose. The kernel boots under TCG wherever /dev/kvm
// is absent — every CI runner — and a kernel plus an initramfs is a great deal more work
// than a boot sector.
const GuestBootTimeout = 10 * time.Minute

// outputRoot is where task build:qemu / build:guest / fetch:kernel put their artefacts.
func outputRoot() string {
	if root := os.Getenv("QEMU_OUTPUT_DIR"); root != "" {
		return root
	}
	return filepath.Join("..", "..", "_output")
}

// GuestImages returns the kernel and initramfs, or skips — unless the caller is the merge
// gate, which may not skip. missingInput carries that decision and the reason for it.
func GuestImages(t *testing.T) (kernel, initramfs string) {
	t.Helper()
	root := outputRoot()
	kernel = filepath.Join(root, "guest", "vmlinux")
	initramfs = filepath.Join(root, "guest", "initramfs.cpio.gz")
	for _, p := range []string{kernel, initramfs} {
		if _, err := os.Stat(p); err != nil {
			missingInput(t, fmt.Sprintf("the guest lane's %s is not there: %v", p, err),
				"task fetch:kernel && task build:guest")
		}
	}
	return kernel, initramfs
}

// QEMUPaths locates the pinned QEMU and its firmware — what `task build:qemu` extracts
// into _output — or reports them missing.
func QEMUPaths(t *testing.T) (bin, bios string) {
	t.Helper()
	root := outputRoot()
	bin = filepath.Join(root, "bin", "qemu-system-x86_64")
	bios = filepath.Join(root, "share", "spin-stack", "qemu")
	if _, err := os.Stat(bin); err != nil {
		missingInput(t, "no QEMU at "+bin, "task build:qemu")
	}
	if _, err := os.Stat(filepath.Join(bios, "bios-256k.bin")); err != nil {
		missingInput(t, "no QEMU firmware at "+bios, "task build:qemu")
	}
	return bin, bios
}

// The contract with integration/guestinit, from the host's side. guestinit is a `main`
// package, so these strings cannot be shared as constants with the program that reads and
// prints them — they are written down here instead, next to the code that depends on
// them, rather than spelled out again in every lane.
const (
	// GuestHold puts guestinit in its long-running mode: it writes and fsyncs a pattern
	// in a loop, prints GuestAlive after each one, and stops when it is told to.
	//
	// It exists because every guest this repository ever booted wrote once and powered
	// off, so nothing could ever be observed *while* a guest was running — every
	// snapshot, restart and clone in these lanes happened over a device whose guest had
	// already gone.
	GuestHold = "spin.mode=hold"

	// GuestAlive is the heartbeat, printed after the fsync of each iteration and never
	// before it. Waiting for one is therefore waiting for a FLUSH this backend answered,
	// which is what makes it evidence rather than a timer.
	GuestAlive = "GUESTINIT-ALIVE"

	// guestStop is what Guest.Stop sends down the serial line. A word rather than a
	// signal to QEMU: SIGINT tears the machine down from outside, which proves nothing
	// about the guest and leaves the volume mid-write. This has to travel *through* the
	// guest — it is read by a process inside the VM, which then powers the machine off
	// itself — so a guest that answers it was demonstrably alive at that instant.
	guestStop = "GUESTCTL-STOP"
)

// Guest is a QEMU that is still running. It is the whole reason this file changed shape:
// the previous entry point ended in cmd.Run(), so a test could have a guest or have
// something else happen, never both.
type Guest struct {
	proc *Process
}

// StartLinuxGuest boots kernel+initramfs against sock and returns while the guest is
// still running. The caller drives it through the console — waiting for a line it
// printed, telling it to stop — and QEMU is killed by the test's cleanup regardless.
func StartLinuxGuest(t *testing.T, sock, kernel, initramfs string, extraCmdline ...string) *Guest {
	t.Helper()
	bin, bios := QEMUPaths(t)
	return &Guest{proc: Start(t, ProcessConfig{
		Name: "guest",
		Path: bin,
		Args: guestArgs(bios, sock, kernel, initramfs, extraCmdline),
		// The return channel. `-serial stdio` is bidirectional and this half of it was
		// never used: the guest's ttyS0 input is this pipe, which is how Stop reaches a
		// program running inside the VM.
		Stdin: true,
	})}
}

// WaitForLine blocks until the guest's console has carried a line containing want.
func (g *Guest) WaitForLine(t *testing.T, want string, timeout time.Duration) {
	t.Helper()
	g.proc.WaitForLine(t, want, timeout)
}

// Console is everything the guest has printed so far — the kernel's log and guestinit's
// verdicts, in the order the serial port carried them.
func (g *Guest) Console() string { return strings.Join(g.proc.Output(), "\n") }

// Stop tells the guest to finish and waits for the machine to go down, returning the
// console output.
//
// Deliberately not a kill: what a test wants to know after driving a live guest is that
// the volume was left in a state the guest agreed to, and a QEMU killed from outside
// leaves a half-written block and a WAL nobody promised anything about. The guest powers
// itself off, so what this waits for is QEMU exiting because the machine did.
func (g *Guest) Stop(t *testing.T, timeout time.Duration) string {
	t.Helper()
	g.proc.WriteLine(t, guestStop)
	code, out := g.Wait(t, timeout)
	if code != 0 {
		t.Fatalf("the guest was told to stop and QEMU exited %d:\n%s", code, tailOf(out, 40))
	}
	return out
}

// Wait blocks until QEMU exits and returns its status and the guest's console output.
func (g *Guest) Wait(t *testing.T, timeout time.Duration) (int, string) {
	t.Helper()
	err := g.proc.Wait(t, timeout)
	out := g.Console()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return 0, out
	case errors.As(err, &exitErr):
		return exitErr.ExitCode(), out
	default:
		// Either QEMU never got started, or it outlived the deadline and was killed —
		// both are "the guest did not finish", and neither is a verdict.
		t.Fatalf("the guest did not finish within %s (TCG is slow, but not this slow): %v\n%s",
			timeout, err, tailOf(out, 40))
		return -1, ""
	}
}

// RunLinuxGuest boots kernel+initramfs against sock and returns QEMU's exit code and the
// guest's console output once the guest has powered itself off.
//
// It is the blocking form, kept because most lanes want exactly it: a guest that writes,
// verifies and goes away. Everything it does now happens through the handle above, so
// there is one QEMU command line and one place a boot can be debugged from.
func RunLinuxGuest(t *testing.T, sock, kernel, initramfs string, extraCmdline ...string) (int, string) {
	t.Helper()
	return StartLinuxGuest(t, sock, kernel, initramfs, extraCmdline...).Wait(t, GuestBootTimeout)
}

// guestArgs is the QEMU command line, in one place because two entry points now use it.
func guestArgs(bios, sock, kernel, initramfs string, extraCmdline []string) []string {
	return []string{
		"-L", bios,
		"-machine", "q35,accel=kvm:tcg,memory-backend=mem",
		"-m", "256M",
		// vhost-user requires the front-end's RAM to be shareable: the whole protocol is
		// this process mapping it.
		"-object", "memory-backend-memfd,id=mem,size=256M,share=on",
		"-chardev", "socket,id=vhostchr0,path=" + sock,
		"-device", "vhost-user-blk-pci,chardev=vhostchr0,num-queues=1",
		"-kernel", kernel,
		"-initrd", initramfs,
		// The verdict channel. guestinit prints to fd 1, which the kernel wires to
		// console=, so the lane reads it off the serial port — and `-serial stdio` is why
		// this one cannot use the boot-sector lane's `-nodefaults`: that suppresses the
		// serial device along with the option ROMs.
		//
		// `panic=-1` matters as much as the rest: PID 1 exiting panics the kernel, and a
		// panicking kernel would otherwise sit there until the timeout, reporting a hang
		// where there was a crash.
		"-append", strings.Join(append([]string{"console=ttyS0", "panic=-1"}, extraCmdline...), " "),
		"-serial", "stdio",
		"-display", "none",
		"-vga", "none",
		"-net", "none",
		"-no-reboot",
	}
}

// tailOf keeps the last n lines. A kernel log is hundreds of lines of boot spam and the
// interesting part is always where it stopped.
func tailOf(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// VerdictLines pulls the guest's own lines out of a kernel log, so a failure report is
// the guest's message rather than four hundred lines of boot spam.
func VerdictLines(out string) string {
	var keep []string
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "GUESTINIT-") {
			keep = append(keep, line)
		}
	}
	if len(keep) == 0 {
		return out
	}
	return strings.Join(keep, "\n")
}
