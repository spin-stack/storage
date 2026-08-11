//go:build linux

// Command guestinit is PID 1 inside the test guest.
//
// It exists to prove the one thing no test in this repository can: that a **real Linux
// guest** issues VIRTIO_BLK_T_FLUSH and that our backend's answer is the one the guest's
// fsync(2) contract requires. Everything below that — an fdatasync of the local WAL and
// the durable watermark it advances — is the host's to prove; from in here the only
// question is whether fsync returned, and whether the bytes come back afterwards.
package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
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
	// stop-and-restart lane vacuous.
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
	// The three lines a walking hold run prints about the host's read-view bound, in the
	// order they can legally appear. They are separate verdicts rather than one because
	// each is a different claim and a test should be able to fail on the missing one:
	//
	//   - verdictRefused: a write was turned away. The bound acted at all.
	//   - verdictTrimmed: the guest gave the walked range back with BLKDISCARD. The
	//     escape hatch was reachable *while over the bound* — a DISCARD refused here
	//     would make the bound a one-way door.
	//   - verdictRecovered: the guest wrote walkRecoverBlocks more blocks afterwards.
	//     The door opened, and stayed open long enough to work through.
	verdictRefused   = "GUESTINIT-REFUSED"
	verdictTrimmed   = "GUESTINIT-TRIMMED"
	verdictRecovered = "GUESTINIT-RECOVERED"
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
	//
	// Where it writes is a second choice, `spin.hold=`, below.
	modeHold = "hold"
)

// The two shapes a hold run can have, chosen by `spin.hold=` on the kernel command line.
//
// A second axis rather than two more `spin.mode=` values, because everything else about a
// hold run is the same in both — the heartbeat, the stop word, the read-back at the end.
// Only *where the writes land* differs, and that single difference decides whether the
// host's read view grows.
const (
	// holdRotate rewrites holdBlocks blocks in rotation. The default, and the original
	// behaviour, kept because it is not the weaker case: it pins the read view at 32 KiB
	// however long the run lasts, which is the only way to show that a steady-state guest
	// — a database checkpointing the same pages, a journal — is *never* throttled by a
	// bound on that view. Half of what a memory bound has to get right is not firing.
	holdRotate = "rotate"

	// holdWalk writes distinct offsets, walking the device upwards from holdOffset, so
	// the read view grows by a block per iteration.
	//
	// It exists because the host's read-view bound had never been crossed end to end and
	// could not be: rotate mode's view is pinned by construction, so a soak of any length
	// reaches 32 KiB and stops. The bound is the one per-volume structure whose size the
	// guest decides, and the measurement behind it (1.55 GiB of host RSS for one volume at
	// 195,658 distinct 4 KiB writes, monotonic, with the OOM killer as the only limit) is
	// a *walking* workload. A generator that cannot walk cannot reach it.
	//
	// When a write is refused, this shape gives the walked range back with BLKDISCARD and
	// carries on, because that is the other half of the bound: DISCARD deliberately
	// carries no view charge so that a guest which has crossed has a way down. If the trim
	// did not work the bound would be a one-way door, and this run says so rather than
	// hanging.
	holdWalk = "walk"
)

// walkHeartbeat is how many blocks a walk puts down between heartbeats. Rotate mode
// announces every iteration and can afford to — it is paced at 50ms — where a walk runs
// flat out and would bury the handful of lines that matter under thousands that do not.
const walkHeartbeat = 32

// walkRetries is how many times a refused offset is tried again before the guest trims.
//
// The host's bound is documented to be permanent for the session: nothing shrinks the read
// view except a DISCARD. So a retry that succeeded with nothing given back in between
// would mean the refusal came from somewhere else — a transient, a device bound, a
// coincidence — and a run that then went on to "prove" the escape hatch would be proving
// it against a door that was never shut. This makes that case a failure with a name.
const walkRetries = 3

// walkRecoverBlocks is how many blocks must land after a trim before the guest announces
// it has recovered. One would satisfy "a write got through"; what the escape hatch has to
// buy is a guest that can go on working, which is a different claim.
const walkRecoverBlocks = 64

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
func mode() string { return cmdline("spin.mode=", modeWrite) }

// shape reports which hold run this boot asked for. Same rule: an unrecognised value is
// refused by the caller, so `spin.hold=wlak` cannot quietly deliver the rotating run and
// leave a test that needed a growing read view green over a view that never grew.
func shape() string { return cmdline("spin.hold=", holdRotate) }

// cmdline returns the value of a `key=` word on the kernel command line, or dflt.
func cmdline(key, dflt string) string {
	raw, err := os.ReadFile("/proc/cmdline")
	if err != nil {
		return dflt
	}
	for _, word := range strings.Fields(string(raw)) {
		if v, ok := strings.CutPrefix(word, key); ok {
			return v
		}
	}
	return dflt
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
		switch s := shape(); s {
		case holdRotate:
			return hold(f, pattern)
		case holdWalk:
			return walk(f, pattern)
		default:
			return fmt.Errorf("unknown spin.hold=%s on the kernel command line", s)
		}
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
	// Log.Flush, which must not return until an fdatasync of the local WAL segments has
	// returned (§14.8). If the backend answers with an error, fsync fails here — and a
	// guest whose fsync fails is entitled to consider its data lost, which is exactly the
	// contract this lane exists to observe from the guest's side.
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

	if err := discardRange(f, target, int64(len(pattern))); err != nil {
		return fmt.Errorf("discard mode: %w", err)
	}

	// Drop the page cache for this device so the read below comes from the backend
	// and not from pages the kernel still holds. Without this the test could pass on
	// a backend that ignored the discard entirely.
	if err := dropCache(f); err != nil {
		return fmt.Errorf("discard mode: %w", err)
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

// discardRange asks the block layer to unmap [off, off+length).
//
// BLKDISCARD takes a two-element array of u64: offset then length, in bytes. The block
// layer forwards it **only if the device negotiated VIRTIO_BLK_F_DISCARD**, so the ioctl
// failing with EOPNOTSUPP is itself the assertion that the feature bit was never on the
// wire — which is a defect no host-side test can see, because a unit test calls the
// backend method directly and never negotiates anything.
func discardRange(f *os.File, off, length int64) error {
	rng := [2]uint64{uint64(off), uint64(length)}
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), blkDiscard,
		uintptr(unsafe.Pointer(&rng[0]))); errno != 0 {
		return fmt.Errorf("BLKDISCARD of %d bytes at %d: %w "+
			"(EOPNOTSUPP here means the device never advertised VIRTIO_BLK_F_DISCARD)",
			length, off, errno)
	}
	return nil
}

// dropCache invalidates this device's page cache. BLKFLSBUF is the block layer's own
// verb for it and needs no /proc tunable, so it works in an initramfs that mounted
// nothing.
func dropCache(f *os.File) error {
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), blkFlsBuf, 0); errno != 0 {
		return fmt.Errorf("BLKFLSBUF: %w", errno)
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

// walk is the sustained-load generator the host's read-view bound needs, and the reason
// it had to be written is that the bound had never been crossed by anything.
//
// It writes **distinct** offsets — a block, an fsync, the next block — walking the device
// upwards from holdOffset. Each block is a region of the volume nothing has written
// before, so the host's read view gains an entry per iteration and its cost grows
// monotonically, which is exactly the shape of the workload that put an Agent at 1.55 GiB
// of RSS for one volume. Rotate mode, which rewrites eight blocks forever, cannot produce
// it at any duration.
//
// Three things can end a walk and each is a different verdict:
//
//   - the host refuses a write. Expected: the bound acted. The guest trims what it walked,
//     which is the only thing that shrinks a read view, and carries on — and the writes
//     that land afterwards are the proof the bound is not a one-way door.
//   - the host takes every block to the end of the device. A failure, and the one this
//     shape exists to be able to report: a bound that refuses nothing is a bound that is
//     not there, and every assertion downstream of it would have passed.
//   - the host says stop. The guest reads back what it last wrote, through a cold cache,
//     and passes.
func walk(f *os.File, pattern []byte) error {
	// The device's own size, asked of the device rather than assumed: the walk has to
	// know where the end is to be able to report reaching it, and a constant here would
	// be a second copy of a number the host already owns.
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("walk: sizing %s: %w", device, err)
	}
	stopped, err := watchForStop()
	if err != nil {
		return err
	}

	block := int64(len(pattern))
	off := int64(holdOffset)
	// trimFrom is the oldest offset still charged to the host's read view: everything
	// from here to off. A trim gives back exactly that and no more, so a walk never
	// discards a range it has already given back.
	trimFrom := off
	var written, trims, sinceTrim int
	// Nothing to recover from until something has been refused, so a run that is stopped
	// before it ever crosses the bound does not claim it recovered.
	recovered := true

	for {
		if off+block > size {
			return fmt.Errorf("walk: put %d distinct %d-byte blocks down to the end of the %d-byte "+
				"device and every one of them was taken: nothing bounded the host's read view",
				written, block, size)
		}
		if err := writeThrough(f, pattern, off); err != nil {
			report("%s %d %v", verdictRefused, off, err)

			// The same offset again, with nothing given back in between. See walkRetries:
			// only a trim shrinks the view, so a success here means the refusal was not
			// the bound and the escape hatch below would be proving nothing.
			for i := range walkRetries {
				if err := writeThrough(f, pattern, off); err == nil {
					return fmt.Errorf("walk: the block at %d was refused and then taken on retry %d "+
						"with nothing trimmed in between: whatever refused it was not a bound on the "+
						"read view, which only a DISCARD can clear", off, i+1)
				}
			}

			// The escape hatch. Everything walked since the last trim goes back in one
			// BLKDISCARD; a DISCARD carries no read-view charge precisely so that a guest
			// which has crossed the bound can issue this one.
			length := off - trimFrom
			if length == 0 {
				return fmt.Errorf("walk: refused at %d with nothing written since the last trim: "+
					"there is nothing left to give back and no way under the bound", off)
			}
			if err := discardRange(f, trimFrom, length); err != nil {
				return fmt.Errorf("walk: the bound is a one-way door — a guest over it could not trim: %w", err)
			}
			trims++
			sinceTrim, recovered, trimFrom = 0, false, off
			report("%s %d %d %d", verdictTrimmed, trims, off-length, length)
			continue
		}

		written++
		sinceTrim++
		if written%walkHeartbeat == 0 {
			report("%s %d %d", verdictAlive, written, off)
		}
		if !recovered && sinceTrim >= walkRecoverBlocks {
			recovered = true
			report("%s %d %d %d", verdictRecovered, trims, sinceTrim, off)
		}
		off += block

		// Checked after a write and never before one, so the offset read back below is
		// always one this run put down *since the last trim* — reading back a block the
		// guest itself discarded would fail for the one reason that is not a defect.
		if stopped.Load() {
			if err := dropCache(f); err != nil {
				return fmt.Errorf("walk: %w", err)
			}
			return readBack(pattern, off-block)
		}
	}
}

// writeThrough writes one block and fsyncs it, reporting whichever step failed first.
//
// Both steps, because that is the shape in which a refused WRITE reaches a guest. The
// write(2) is buffered: it dirties a page and returns success, and the device does not see
// the request until writeback. So a backend that refuses it fails the **fsync** — which is
// the contract that matters, since a guest whose fsync fails is entitled to consider its
// data lost, and a database meeting this bound would meet it exactly here.
//
// Deliberately not O_DIRECT or O_SYNC, for the same reason the rest of this program is
// not: either would make the write its own durable step and prove less.
func writeThrough(f *os.File, pattern []byte, off int64) error {
	if _, err := f.WriteAt(pattern, off); err != nil {
		return fmt.Errorf("write at %d: %w", off, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync of the write at %d: %w", off, err)
	}
	return nil
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
