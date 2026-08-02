//go:build integration

package vhost_test

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

	"github.com/spin-stack/storage/internal/wal"
)

// The one thing no other test in this repository can prove: that a **real Linux kernel**
// decides, on its own, that a write must be made durable now, and that our backend's
// answer is the one `fsync(2)`'s contract requires.
//
// Every other lane here boots a 512-byte boot sector under SeaBIOS. SeaBIOS reaches the
// backend through INT 13h, which has **no flush verb** — so the FLUSH those tests observe
// is one the *test* issued, not one a guest asked for. `integration/guestinit` was written
// for exactly this (increment 7) and built into an initramfs by `task build:guest`… and
// then nothing booted it. `task guest:verify` asserted the inputs existed; no Go file in
// the tree referenced either the kernel or the initramfs. This is the test that was
// missing, and until it existed the claim that "a Linux guest boots the lane" was not
// backed by anything runnable.

// guestBootTimeout is generous on purpose. The kernel boots under TCG wherever /dev/kvm
// is absent — every CI runner — and a kernel plus an initramfs is a great deal more work
// than a boot sector.
const guestBootTimeout = 10 * time.Minute

// guestImages returns the kernel and initramfs, or skips.
//
// Skipping rather than failing is the same call the rest of this lane makes about QEMU: a
// lane whose *input* has not been built has not found a defect, and a red build meaning
// "you did not run task build:guest" trains people to ignore red builds. CI builds them
// (ADR-0025), so CI never skips.
func guestImages(t *testing.T) (kernel, initramfs string) {
	t.Helper()
	root := os.Getenv("QEMU_OUTPUT_DIR")
	if root == "" {
		root = filepath.Join("..", "..", "_output")
	}
	kernel = filepath.Join(root, "guest", "vmlinux")
	initramfs = filepath.Join(root, "guest", "initramfs.cpio.gz")
	for _, p := range []string{kernel, initramfs} {
		if _, err := os.Stat(p); err != nil {
			t.Skipf("%s is not built (run `task build:guest` and `task fetch:kernel`): %v", p, err)
		}
	}
	return kernel, initramfs
}

// TestALinuxGuestIssuesFLUSH boots the pinned kernel with integration/guestinit as PID 1,
// against a device this backend serves out of a real WAL.
//
// The guest writes a pattern with a plain buffered write, calls fsync, reads it back
// through a fresh descriptor, and prints a verdict. What that exercises, and nothing else
// in this repository does:
//
//   - the kernel's virtio_blk driver deciding a FLUSH is required, because the device
//     advertised a volatile write cache;
//   - our §14.4 ACK path answering it — the object verified and the lease valid at the
//     instant of the ACK — with a *real* guest blocked on the answer;
//   - `fsync` returning success only because of that, since a backend that failed the
//     FLUSH would fail the syscall, and a guest whose fsync fails is entitled to consider
//     its data lost.
func TestALinuxGuestIssuesFLUSH(t *testing.T) {
	kernel, initramfs := guestImages(t)
	ctx := t.Context()

	// The same WAL-backed lane the other tests use, with nothing seeded: this guest
	// brings its own filesystem (an initramfs in RAM) and touches the device only where
	// it means to.
	l := startWAL(t, ctx, wal.Limits{}, func(func(uint64, []byte)) {})

	code, out := runLinuxGuest(t, ctx, l.sock, kernel, initramfs)

	// The verdict first: it says *what* failed, where an exit code says only that
	// something did.
	switch {
	case strings.Contains(out, "GUESTINIT-FAIL"):
		t.Fatalf("the guest reported a failure:\n%s", verdictLines(out))
	case !strings.Contains(out, "GUESTINIT-PASS"):
		t.Fatalf("the guest never reported a verdict (exit %d). It must always print one and power off:\n%s",
			code, out)
	}

	// And the backend's side of the same event, because a guest that printed PASS while
	// this backend did nothing durable would make the whole lane a tautology.
	//
	// Asserted as the §14.4 *outcome* rather than as a message count: the guest's write
	// reached the WAL, and a FLUSH — which is the only thing that advances `durable` —
	// put it in a verified object. Nothing else in this test issues one; the test never
	// touches l.dev.
	w := l.log.Watermarks()
	switch {
	case w.Local == 0:
		t.Fatal("the guest's write never reached the WAL")
	case w.Durable == 0:
		t.Fatalf("the guest's fsync advanced nothing: local=%d, durable=0 — no FLUSH reached this backend", w.Local)
	}
	objs, err := l.store.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) == 0 {
		t.Fatal("durable advanced with nothing in the object store (INV-07)")
	}
	t.Logf("a real kernel's fsync made %d record(s) durable in %d verified object(s)", w.Durable, len(objs))
}

// runLinuxGuest boots kernel+initramfs against sock and returns QEMU's exit code and the
// guest's console output.
func runLinuxGuest(t *testing.T, ctx context.Context, sock, kernel, initramfs string) (int, string) {
	t.Helper()
	bin, bios := qemuPaths(t)

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
		"-append", "console=ttyS0 panic=-1",
		"-serial", "stdio",
		"-display", "none",
		"-vga", "none",
		"-net", "none",
		"-no-reboot",
	}

	cctx, cancel := context.WithTimeout(ctx, guestBootTimeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, bin, args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out

	err := cmd.Run()
	if cctx.Err() != nil {
		t.Fatalf("the guest did not finish within %s (TCG is slow, but not this slow):\n%s",
			guestBootTimeout, tailOf(out.String(), 40))
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

// verdictLines pulls the guest's own lines out of a kernel log, so a failure report is
// the guest's message rather than four hundred lines of boot spam.
func verdictLines(out string) string {
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
