//go:build integration || e2e

package testinfra

import (
	"bytes"
	"context"
	"errors"
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

// GuestImages returns the kernel and initramfs, or skips.
//
// Skipping rather than failing is the same call the rest of the lane makes about QEMU: a
// lane whose *input* has not been built has not found a defect, and a red build meaning
// "you did not run task build:guest" trains people to ignore red builds. CI builds them
// (ADR-0025), so CI never skips.
func GuestImages(t *testing.T) (kernel, initramfs string) {
	t.Helper()
	root := outputRoot()
	kernel = filepath.Join(root, "guest", "vmlinux")
	initramfs = filepath.Join(root, "guest", "initramfs.cpio.gz")
	for _, p := range []string{kernel, initramfs} {
		if _, err := os.Stat(p); err != nil {
			t.Skipf("%s is not built (run `task build:guest` and `task fetch:kernel`): %v", p, err)
		}
	}
	return kernel, initramfs
}

// QEMUPaths locates the pinned QEMU and its firmware. The binary and the BIOS blobs are
// what `task build:qemu` extracts into _output; the lane skips rather than fails when they
// are absent, because a developer who has not built QEMU has not broken anything.
func QEMUPaths(t *testing.T) (bin, bios string) {
	t.Helper()
	root := outputRoot()
	bin = filepath.Join(root, "bin", "qemu-system-x86_64")
	bios = filepath.Join(root, "share", "spin-stack", "qemu")
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("no QEMU at %s — run: task build:qemu", bin)
	}
	if _, err := os.Stat(filepath.Join(bios, "bios-256k.bin")); err != nil {
		t.Skipf("no firmware at %s — run: task build:qemu", bios)
	}
	return bin, bios
}

// RunLinuxGuest boots kernel+initramfs against sock and returns QEMU's exit code and the
// guest's console output.
func RunLinuxGuest(t *testing.T, ctx context.Context, sock, kernel, initramfs string, extraCmdline ...string) (int, string) {
	t.Helper()
	bin, bios := QEMUPaths(t)

	args := []string{
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

	cctx, cancel := context.WithTimeout(ctx, GuestBootTimeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, bin, args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out

	err := cmd.Run()
	if cctx.Err() != nil {
		t.Fatalf("the guest did not finish within %s (TCG is slow, but not this slow):\n%s",
			GuestBootTimeout, tailOf(out.String(), 40))
	}
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return 0, out.String()
	case errors.As(err, &exitErr):
		return exitErr.ExitCode(), out.String()
	default:
		t.Fatalf("QEMU: %v\n%s", err, out.String())
		return -1, ""
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
