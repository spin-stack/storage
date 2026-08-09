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
//
// Three modes, chosen by `spin.mode=` on the kernel command line and described where they
// are declared below. The third — hold — is the one that makes a guest something a host
// test can act *upon* rather than wait for: it keeps writing until the host tells it to
// stop, over the return direction of the same serial line the verdicts go out on.
package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
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

// Where the long-running mode writes. Past the range above, on purpose: a hold-mode run
// and a write/verify run of the same volume must not be able to satisfy each other's
// assertions, and overlapping ranges are how that happens by accident.
const (
	holdOffset = 2 << 20
	holdBlocks = 8
)

// verdict lines the host test greps for. The prefix is unlikely to appear in kernel
// output, and the host asserts on the exact strings — a lane that "passes" because its
// pattern stopped matching is the failure mode this guards against.
// Block-layer ioctls. They are written as literals with their kernel names because
// that is what a reader checks against include/uapi/linux/fs.h; golang.org/x/sys is
// not a dependency of the guest binary, which is static and deliberately tiny.
const (
	blkDiscard = 0x1277 // BLKDISCARD
	blkFlsBuf  = 0x1261 // BLKFLSBUF
)

const (
	verdictPass = "GUESTINIT-PASS"
	verdictFail = "GUESTINIT-FAIL"
	// verdictAlive is the heartbeat of the long-running mode, printed after each
	// iteration's fsync and never before it, so a host that saw one knows a FLUSH was
	// answered rather than knowing that time passed.
	verdictAlive = "GUESTINIT-ALIVE"
	// stopCommand is what the host sends down the serial line to end a hold-mode run.
	// The guest powers itself off in response, which is the difference between "the
	// test stopped the VM" and "the guest finished": only the second leaves the volume
	// in a state something inside the guest agreed to.
	stopCommand = "GUESTCTL-STOP"
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

	if err := run(mode()); err != nil {
		report("%s %v", verdictFail, err)
	} else {
		report("%s", verdictPass)
	}
	powerOff()
}

// The three things a boot can be asked to do, via `spin.mode=` on the kernel command
// line.
const (
	// modeWrite writes, fsyncs and reads back, then powers off. The default, and what
	// the FLUSH proof needs.
	modeWrite = "write"
	// modeVerify checks the device without writing to it. The second boot of a volume
	// is the only way to prove what the host left behind: the first boot writes and
	// fsyncs, the host publishes, and this boot reads the same range back. Doing it in
	// one boot would prove nothing — the data would still be in the page cache and in
	// the local WAL.
	modeVerify = "verify"
	// modeDiscard writes a pattern, fsyncs it, then asks the KERNEL to discard part
	// of it with BLKDISCARD and checks that the range reads back as zeros while its
	// neighbours do not. It is the only thing in this tree that proves a real Linux
	// block layer can reach wal.Log.Discard: the whole discard mechanism was complete
	// and unreachable because internal/vhost withheld VIRTIO_BLK_F_DISCARD, and a
	// host-side unit test cannot notice a missing feature bit — only a driver that
	// refuses to send the request can.
	modeDiscard = "discard"
	// modeHold keeps writing until the host says stop. It exists so that something else
	// can happen while a guest is running: until it did, every guest in this repository
	// wrote once and powered off, and so every snapshot, restart and clone the lanes
	// exercised happened over a device nobody was using.
	modeHold = "hold"
)

// holdInterval paces the hold loop.
//
// A sleep in this repository is normally a stop signal, and it is not one here for a
// reason that does not generalise: this program runs inside the VM, on the far side of
// the interface simio models (DEV-0013), and there is no injected clock to reach. What
// the pacing buys is the loop being *observable* — unpaced, a 4 KiB write and an fsync
// per iteration bury the heartbeat the host is waiting for under thousands of lines a
// second and fill the host's WAL with hundreds of megabytes of a pattern nobody reads.
const holdInterval = 50 * time.Millisecond

// mode reports what this boot was asked to do. An unrecognised value is an error rather
// than a fall-back to the default: `spin.mode=hodl` silently doing the write-and-verify
// run would leave a lane green while testing something else entirely.
func mode() string {
	cmdline, err := os.ReadFile("/proc/cmdline")
	if err != nil {
		return modeWrite
	}
	for _, word := range strings.Fields(string(cmdline)) {
		if v, ok := strings.CutPrefix(word, "spin.mode="); ok {
			return v
		}
	}
	return modeWrite
}

func run(m string) error {
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

	switch m {
	case modeVerify:
		// Nothing written and nothing synced: whatever comes back was put there by a
		// previous boot and survived whatever the host did in between.
		return readBack(pattern, writeOffsets()...)
	case modeDiscard:
		return discardRoundTrip(f, pattern)
	case modeHold:
		return hold(f, pattern)
	case modeWrite:
	default:
		return fmt.Errorf("unknown spin.mode=%s on the kernel command line", m)
	}

	for _, off := range writeOffsets() {
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

	return readBack(pattern, writeOffsets()...)
}

// discardOffsets is the three-block window modeDiscard uses: it writes all three,
// discards the middle one, and expects the outer two to survive. A discard that took
// the whole request range, or rounded outward to some granularity, fails on the
// neighbours rather than on the target — which is the failure this shape is for.
func discardOffsets() (before, target, after int64) {
	base := int64(writeOffset)
	return base, base + stride, base + 2*stride
}

// discardRoundTrip is the guest half of the DISCARD proof.
//
// The request is issued with the BLKDISCARD ioctl rather than by mounting a
// filesystem and running fstrim: fstrim would prove the same thing through several
// more layers, each of which can decide not to issue a discard for reasons of its own
// (the filesystem's own free-space bookkeeping, a mount option, an alignment rule),
// and a lane that silently stops exercising the thing it is named after is the exact
// failure this repository keeps paying for. BLKDISCARD goes to the block layer, and
// the block layer sends it only if the device negotiated the feature — so the ioctl
// failing with EOPNOTSUPP *is* the assertion that the bit was advertised.
func discardRoundTrip(f *os.File, pattern []byte) error {
	before, target, after := discardOffsets()
	for _, off := range []int64{before, target, after} {
		if _, err := f.WriteAt(pattern, off); err != nil {
			return fmt.Errorf("discard mode: writing at %d: %w", off, err)
		}
	}
	// fsync first: a discard of a range still sitting in the page cache would be
	// racing the writeback of the very data it is meant to remove.
	if err := f.Sync(); err != nil {
		return fmt.Errorf("discard mode: fsync before the discard: %w", err)
	}

	// BLKDISCARD takes a two-element array of u64: offset then length, in bytes.
	rng := [2]uint64{uint64(target), uint64(len(pattern))}
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), blkDiscard,
		uintptr(unsafe.Pointer(&rng[0]))); errno != 0 {
		return fmt.Errorf("discard mode: BLKDISCARD of %d bytes at %d: %w "+
			"(EOPNOTSUPP here means the device never advertised VIRTIO_BLK_F_DISCARD)",
			len(pattern), target, errno)
	}

	// Drop the page cache for this device so the read below comes from the backend
	// and not from pages the kernel still holds. Without this the test could pass on
	// a backend that ignored the discard entirely.
	if err := dropCache(f); err != nil {
		return err
	}

	g, err := os.Open(device)
	if err != nil {
		return fmt.Errorf("discard mode: reopening %s: %w", device, err)
	}
	defer func() { _ = g.Close() }()

	got := make([]byte, len(pattern))
	if _, err := g.ReadAt(got, target); err != nil {
		return fmt.Errorf("discard mode: reading the discarded range: %w", err)
	}
	for i, b := range got {
		if b != 0 {
			return fmt.Errorf("discard mode: byte %d of the discarded range is %#x, want 0", i, b)
		}
	}
	for _, off := range []int64{before, after} {
		if _, err := g.ReadAt(got, off); err != nil {
			return fmt.Errorf("discard mode: reading the neighbour at %d: %w", off, err)
		}
		if !bytes.Equal(got, pattern) {
			return fmt.Errorf("discard mode: the block at %d was cleared and should not have been", off)
		}
	}
	return nil
}

// dropCache invalidates this device's page cache. BLKFLSBUF is the block layer's own
// verb for it and needs no /proc tunable, so it works in an initramfs that mounted
// nothing.
func dropCache(f *os.File) error {
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), blkFlsBuf, 0); errno != 0 {
		return fmt.Errorf("discard mode: BLKFLSBUF: %w", errno)
	}
	return nil
}

// writeOffsets is where the write/verify pair puts its pattern.
func writeOffsets() []int64 {
	offs := make([]int64, blocks)
	for i := range offs {
		offs[i] = int64(writeOffset + i*stride)
	}
	return offs
}

// hold writes and fsyncs the pattern in a loop, announcing each one, until the host says
// stop. It is what a "live guest" means on this side: a device with real virtio traffic
// on it at the moment the host does something else.
//
// The stop is checked *after* an iteration rather than before, so a run always leaves at
// least one write behind however quickly the host stops it — an assertion about what a
// live guest wrote must not be able to pass over a guest that wrote nothing.
func hold(f *os.File, pattern []byte) error {
	stopped, err := watchForStop()
	if err != nil {
		return err
	}
	for i := 1; ; i++ {
		off := int64(holdOffset + ((i-1)%holdBlocks)*stride)
		if _, err := f.WriteAt(pattern, off); err != nil {
			return fmt.Errorf("hold: writing at %d: %w", off, err)
		}
		// Per iteration, and it is the point: the heartbeat below is printed only after
		// a FLUSH the host answered, so "the guest is alive" is a statement about the
		// backend serving it and not about a loop spinning.
		if err := f.Sync(); err != nil {
			return fmt.Errorf("hold: fsync at %d: %w", off, err)
		}
		report("%s %d %d", verdictAlive, i, off)

		if stopped.Load() {
			// Read back what this run last wrote, through a fresh descriptor. Without
			// it a hold run could report PASS having written to a device that answered
			// every request with nothing.
			return readBack(pattern, off)
		}
		time.Sleep(holdInterval)
	}
}

// watchForStop reads the console — the other direction of the serial line the verdicts
// go out on — and latches the host's stop command.
//
// A whole channel for one word looks like a lot; the alternatives were worse. Killing
// QEMU proves nothing about the guest and leaves a write half-issued; a fixed iteration
// count makes the guest's lifetime a race against whatever the host is doing; and a
// sentinel the host writes into the block device would need the host to reach around the
// Agent that owns it.
func watchForStop() (*atomic.Bool, error) {
	var stopped atomic.Bool
	in, err := os.OpenFile("/dev/console", os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("opening the console to listen for %s: %w", stopCommand, err)
	}
	go func() {
		sc := bufio.NewScanner(in)
		for sc.Scan() {
			if strings.Contains(sc.Text(), stopCommand) {
				stopped.Store(true)
				return
			}
		}
	}()
	return &stopped, nil
}

// readBack re-reads the given offsets through a *fresh* descriptor, so the answer cannot
// come from this process's own page cache.
func readBack(pattern []byte, offsets ...int64) error {
	g, err := os.Open(device)
	if err != nil {
		return fmt.Errorf("reopening %s: %w", device, err)
	}
	defer func() { _ = g.Close() }()

	got := make([]byte, patternBytes)
	for _, off := range offsets {
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
