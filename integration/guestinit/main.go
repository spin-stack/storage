//go:build linux

// Command guestinit is PID 1 inside the test guest.
//
// It exists to prove the one thing no test in this repository can: that a **real Linux
// guest** issues VIRTIO_BLK_T_FLUSH and that our backend's answer is the one the guest's
// fsync(2) contract requires. Everything below that — the six §14.4 steps, the verified
// object, the lease check — is covered by unit tests against a simulated front-end.
// What was never covered is the top of the stack: a kernel deciding, on its own, that
// this write must be made durable now.
//
// SeaBIOS cannot do it (INT 13h has no flush verb), so the previous lane booted a 512-byte
// boot sector and stopped at READ/WRITE. This is the replacement, and it is deliberately
// a Go program rather than an /init shell script: it can assert, and it can report *what*
// failed rather than an exit code.
//
// It is PID 1 in an initramfs, so there is no libc, no shell, no mount table and nothing
// to clean up after it. It never returns: PID 1 exiting panics the kernel, which reads as
// a crash rather than a verdict, so it always powers the machine off itself.
package main

import (
	"bytes"
	"fmt"
	"os"
	"syscall"
)

// The device the backend serves over vhost-user-blk. virtio_blk is compiled into the
// kernel (CONFIG_VIRTIO_BLK=y), so it appears without loading a module.
const device = "/dev/vda"

// The offset the host asserts on. Deliberately not 0: the first sector of a block device
// attracts writes from anything that probes it, and a pattern found at 0 could have been
// put there by something other than this program.
const (
	writeOffset  = 1 << 20 // 1 MiB in
	patternBytes = 4096
	// blocks is how many patternBytes-sized blocks are written, and stride is how far
	// apart. More than one block on purpose: a WAL segment is sealed by the append that
	// would overflow it, so a single record leaves the only segment open and a
	// truncation with nothing to reclaim — which would make the host's
	// checkpoint-and-restart lane vacuous.
	//
	// They are *scattered* rather than consecutive for the same reason, discovered the
	// hard way: the guest's page cache merges adjacent dirty blocks into one virtio
	// request, so eight consecutive writes arrived as a single 32 KiB record and sealed
	// nothing. A stride the kernel cannot coalesce across is what makes them eight.
	blocks = 8
	stride = 64 << 10
)

// verdict lines the host test greps for. The prefix is unlikely to appear in kernel
// output, and the host asserts on the exact strings — a lane that "passes" because its
// pattern stopped matching is the failure mode this guards against.
const (
	verdictPass = "GUESTINIT-PASS"
	verdictFail = "GUESTINIT-FAIL"
)

func main() {
	// Best-effort: a guest that cannot mount /proc can still open a block device.
	_ = syscall.Mount("proc", "/proc", "proc", 0, "")
	_ = syscall.Mount("sysfs", "/sys", "sysfs", 0, "")
	// devtmpfs is not best-effort in one respect: it is what creates /dev/console.
	// The initramfs is built by an unprivileged `cpio`, so it contains no device
	// nodes, and the kernel therefore could not open an initial console for PID 1 —
	// it warns and execs init with *no stdio at all*. Every line this program printed
	// went to a closed descriptor, and the lane saw a guest that booted, said nothing
	// and hung. Mounting devtmpfs is what makes the console exist; opening it is the
	// next line.
	_ = syscall.Mount("devtmpfs", "/dev", "devtmpfs", 0, "")
	openConsole()

	if err := run(verifyOnly()); err != nil {
		report("%s %v", verdictFail, err)
	} else {
		report("%s", verdictPass)
	}
	powerOff()
}

// verifyOnly reports whether the host asked this boot to *check* the device rather than
// write to it, via `spin.mode=verify` on the kernel command line.
//
// The second boot of a volume is the only way to prove what a checkpoint and a
// truncation left behind: the first boot writes and fsyncs, the host publishes and
// reclaims, and this boot reads the same range back. Doing it in one boot would prove
// nothing — the data would still be in the page cache and in the local WAL.
func verifyOnly() bool {
	cmdline, err := os.ReadFile("/proc/cmdline")
	if err != nil {
		return false
	}
	return bytes.Contains(cmdline, []byte("spin.mode=verify"))
}

func run(verify bool) error {
	// Plain O_RDWR: no O_SYNC, no O_DIRECT. Either would make the write durable on its
	// own and leave fsync nothing to do — which would prove less, not more. The point
	// is that a *buffered* write plus fsync produces a FLUSH, because that is what a
	// filesystem on this device does.
	f, err := os.OpenFile(device, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("opening %s: %w", device, err)
	}
	defer func() { _ = f.Close() }()

	pattern := make([]byte, patternBytes)
	for i := range pattern {
		pattern[i] = byte('A' + (i % 23))
	}

	if verify {
		// Nothing written and nothing synced: whatever comes back was put there by a
		// previous boot and survived whatever the host did in between.
		return readBack(pattern)
	}

	for i := range blocks {
		off := int64(writeOffset + i*stride)
		if _, err := f.WriteAt(pattern, off); err != nil {
			return fmt.Errorf("writing at %d: %w", off, err)
		}
	}

	// The whole reason this program exists. fsync(2) on a block device the kernel knows
	// has a volatile write cache emits VIRTIO_BLK_T_FLUSH; our backend turns that into
	// Log.Flush, which under §14.4 must not return until the record is in a verified
	// object and the lease was valid at the instant of the ACK. If the backend answers
	// with an error, fsync fails here — and a guest whose fsync fails is a guest whose
	// database is entitled to consider its data lost, which is exactly the contract we
	// want observed from this side.
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync (the FLUSH this lane exists for): %w", err)
	}

	return readBack(pattern)
}

// readBack re-reads the range through a *fresh* descriptor, so the answer cannot come
// from this process's own page cache.
func readBack(pattern []byte) error {
	g, err := os.Open(device)
	if err != nil {
		return fmt.Errorf("reopening %s: %w", device, err)
	}
	defer func() { _ = g.Close() }()

	got := make([]byte, patternBytes)
	for i := range blocks {
		off := int64(writeOffset + i*stride)
		if _, err := g.ReadAt(got, off); err != nil {
			return fmt.Errorf("reading back at %d: %w", off, err)
		}
		if !bytes.Equal(got, pattern) {
			return fmt.Errorf("read-back mismatch at %d: the device did not return what fsync said was durable", off)
		}
	}
	return nil
}

// console is where the verdict goes. It is opened explicitly rather than assumed,
// because PID 1 in an initramfs with no device nodes is handed no stdio: the kernel
// prints "unable to open an initial console" and execs init anyway.
var console *os.File

// openConsole points the verdict at /dev/console, which devtmpfs has just created. It
// is deliberately quiet on failure: there would be nowhere to report the failure to,
// and the host's timeout is the honest signal that the guest could not speak.
func openConsole() {
	if f, err := os.OpenFile("/dev/console", os.O_WRONLY, 0); err == nil {
		console = f
		return
	}
	// A kernel that *did* give us stdio (a console node baked into the image) still
	// works: fd 1 is then the console it opened.
	console = os.Stdout
}

// report writes a line to the console — the serial port the host is reading.
func report(format string, args ...any) {
	if console == nil {
		console = os.Stdout
	}
	_, _ = fmt.Fprintf(console, format+"\n", args...)
	// PID 1 has no one to flush its buffers on exit, and an unflushed verdict is an
	// invisible one.
	_ = console.Sync()
}

// powerOff halts the machine. PID 1 returning would panic the kernel, which the host
// cannot tell from a real crash — so the verdict is always followed by a clean exit.
func powerOff() {
	_, _, _ = syscall.Syscall(syscall.SYS_REBOOT,
		uintptr(linuxRebootMagic1), uintptr(linuxRebootMagic2),
		uintptr(linuxRebootCmdPowerOff))
	// If that returned, the machine is not going down on its own. Spin rather than
	// return: a panic here would be reported as a guest crash.
	for {
		// Nothing to do with the error: Pause returns only when a signal arrives, and
		// there is no handler and nowhere left to report to.
		_ = syscall.Pause()
	}
}

// The reboot(2) constants, spelled out because syscall does not export the power-off
// command on every Go version.
const (
	linuxRebootMagic1      = 0xfee1dead
	linuxRebootMagic2      = 672274793
	linuxRebootCmdPowerOff = 0x4321fedc
)
