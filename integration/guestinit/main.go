//go:build linux

// Command guestinit is PID 1 inside the test guest.
//
// It exists to prove the one thing no host-side test can: that a **real Linux guest**
// boots off the qcow2 file this system prepared, writes to it, and finds those bytes
// again on a second boot. Everything on this side of the virtio boundary is the
// guest's own — the page cache, the block layer, fsync — and that is the point: the
// host asserts on what it prepared, and this asserts on what a kernel actually got.
//
// It does three things and no more. `spin.mode=write` writes a pattern, fsyncs it, reads
// it back and powers off; `spin.mode=verify` only reads it back, which is the half that
// can only be true if the previous boot's bytes survived the VM being shut down; and
// `spin.mode=hold` does the write run and then stays up until the host says stop, so that
// something else can happen on the host while a guest is actually using the disk. The
// Agent restarting under a running guest is the case that needs it: an Agent that ran an
// offline tool over a live image would be refused by QEMU's own write lock, and nothing
// can observe that while every guest writes once and powers itself off.
//
// A hold run also takes `spin.churn=<MiB>`, and then it does not sit still: it rewrites
// that much of a region of its own, over and over, until the host says stop. It is what
// gives the host a guest that is *still writing* rather than one that is merely still
// running — rotation seals the tip under a live VM, and a guest sitting idle would prove
// that a chain can be rotated, not that a guest can be rotated out from under.
//
// It writes until told to stop rather than a fixed amount because the host is waiting on
// something else entirely: the rotations the writing causes. A fixed amount is a guess at
// how much data that takes on a disk nobody has measured, and the first guess was wrong
// in the direction that makes a demonstration pass while proving nothing — 64 MiB landed
// inside a single reconcile cycle, so the guest was finished before the Agent had looked
// once.
//
// The churn is deliberately elsewhere on the device, so the pattern the verify boot reads
// back is untouched by it and stays the assertion it was.
//
// Every run also takes `spin.slot=<n>`, which moves the pattern — its offsets and its
// bytes — into a region of its own. It is what lets one demonstration tell two guests'
// writes apart: a lineage's generations each write their own slot, so a descendant
// verifying slot 0 is asserting on the bytes the oldest volume's guest wrote and nothing
// else could have put there.
package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// device is the disk QEMU was given the volume's active image as. virtio_blk is
// compiled into the pinned kernel, so it appears without a module being loaded.
const device = "/dev/vda"

// Where the pattern goes. Deliberately not offset 0: the first sector of a block device
// attracts writes from anything that probes it, and a pattern found at 0 could have
// been put there by something other than this program.
const (
	writeOffset  = 1 << 20 // 1 MiB in
	patternBytes = 4096
	// Several blocks, scattered rather than consecutive, because the guest's page cache
	// merges adjacent dirty blocks into one virtio request — so eight consecutive
	// writes would reach the image as one, and prove one thing rather than eight.
	blocks = 8
	stride = 64 << 10
	// churnOffset is where a hold run's churn goes: far enough past the pattern that no
	// amount of it can reach the bytes the verify boot asserts on.
	churnOffset    = 32 << 20
	churnChunk     = 1 << 20
	churnSyncEvery = 4 << 20
	// slotStride separates one guest's pattern from another's, selected by `spin.slot=<n>`
	// and 0 for every boot that does not ask. A lineage needs it: each generation's guest
	// writes into a slot of its own, so a descendant reading slot 0 back is reading bytes
	// only the oldest volume's guest ever wrote — and a chain missing that generation
	// returns zeros there instead of somebody else's copy of the same pattern.
	slotStride = 4 << 20
)

// The verdict lines the host greps for. The host asserts on these exact strings: a lane
// that "passes" because its pattern stopped matching is the failure this guards against.
const (
	verdictPass = "GUESTINIT-PASS"
	verdictFail = "GUESTINIT-FAIL"
	// verdictHeld is printed after the write run's fsync has returned and never before
	// it, so a host that saw the line knows the disk is in use rather than knowing that
	// a VM started.
	verdictHeld = "GUESTINIT-HELD"
	// stopCommand is what the host sends down the serial line to end a hold run. The
	// guest powers itself off in response, which is the difference between "the host
	// killed the VM" and "the guest finished" — and only the second leaves a volume in a
	// state something inside the guest agreed to.
	stopCommand = "GUESTCTL-STOP"
	// verdictChurned is printed once the churn has stopped and its last fsync has
	// returned, so a host that saw it knows the bytes reached the image rather than the
	// page cache. It carries how many MiB the run wrote in total.
	verdictChurned = "GUESTINIT-CHURNED"
)

const (
	modeWrite  = "write"
	modeVerify = "verify"
	modeHold   = "hold"
)

// console is where the verdict goes. PID 1 in an initramfs built by an unprivileged
// cpio has no device nodes, so the kernel execs it with no stdio at all; mounting
// devtmpfs is what makes /dev/console exist to be opened.
var console *os.File

func main() {
	// Best-effort: a guest that cannot mount /proc can still open a block device.
	_ = syscall.Mount("proc", "/proc", "proc", 0, "")
	_ = syscall.Mount("sysfs", "/sys", "sysfs", 0, "")
	_ = syscall.Mount("devtmpfs", "/dev", "devtmpfs", 0, "")
	openConsole()

	if err := run(mode()); err != nil {
		report("%s %v", verdictFail, err)
	} else {
		report("%s", verdictPass)
	}
	powerOff()
}

// mode reports what this boot was asked to do. An unrecognised value is an error rather
// than a fall-back: `spin.mode=verfiy` silently performing the write run would leave a
// lane green having proved the opposite of what it claims.
func mode() string {
	raw, err := os.ReadFile("/proc/cmdline")
	if err != nil {
		return modeWrite
	}
	for _, word := range strings.Fields(string(raw)) {
		if v, ok := strings.CutPrefix(word, "spin.mode="); ok {
			return v
		}
	}
	return modeWrite
}

func run(m string) error {
	slot := cmdlineInt("spin.slot")
	pattern := make([]byte, patternBytes)
	for i := range pattern {
		// The slot is in the bytes as well as in the offset, so a read that came back
		// from the wrong slot fails rather than matching whatever was in the other one.
		pattern[i] = byte('A' + ((i + int(slot)) % 23))
	}

	switch m {
	case modeVerify:
		// Nothing written and nothing synced: whatever comes back was put there by a
		// previous boot and survived the machine being shut down.
		return readBack(pattern, slot)
	case modeWrite, modeHold:
	default:
		return fmt.Errorf("unknown spin.mode=%s on the kernel command line", m)
	}

	// Plain O_RDWR: no O_SYNC, no O_DIRECT. Either would make the write durable on its
	// own and leave fsync nothing to do, which would prove less rather than more. What
	// is being exercised is a *buffered* write plus fsync, because that is what a
	// filesystem on this device does.
	f, err := os.OpenFile(device, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("opening %s: %w", device, err)
	}
	defer func() { _ = f.Close() }()

	for _, off := range offsets(slot) {
		if _, err := f.WriteAt(pattern, off); err != nil {
			return fmt.Errorf("writing at %d: %w", off, err)
		}
	}
	// The guest's durability boundary, and in this design the whole of it: fsync(2) on
	// a block device with a volatile write cache emits VIRTIO_BLK_T_FLUSH, QEMU turns
	// that into an fdatasync of the qcow2 file, and that is what an ACK now claims —
	// local durability and nothing further.
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	if m == modeHold {
		report("%s", verdictHeld)
		if err := churn(f, cmdlineInt("spin.churn"), stopped()); err != nil {
			return err
		}
	}
	return readBack(pattern, slot)
}

// churn rewrites a region of mib megabytes, round and round, until stop closes.
//
// Rewriting the same region rather than marching down the device is what keeps every
// tip growing: a rotation puts an empty layer on top, so the next pass over those
// offsets allocates every cluster again in the new one. Marching would fill the device
// and stop being about rotation.
func churn(f *os.File, mib int64, stop <-chan struct{}) error {
	if mib <= 0 {
		<-stop
		return nil
	}
	block := make([]byte, churnChunk)
	for i := range block {
		block[i] = byte('a' + (i % 26))
	}
	var written, since int64
	for {
		select {
		case <-stop:
			if err := f.Sync(); err != nil {
				return fmt.Errorf("final fsync while churning: %w", err)
			}
			report("%s %d", verdictChurned, written/churnChunk)
			return nil
		default:
		}
		off := churnOffset + written%(mib*churnChunk)
		if _, err := f.WriteAt(block, off); err != nil {
			return fmt.Errorf("churning at %d: %w", off, err)
		}
		written += churnChunk
		if since += churnChunk; since >= churnSyncEvery {
			// The host is watching the *file* grow, and writes that never left the
			// guest do not grow it.
			if err := f.Sync(); err != nil {
				return fmt.Errorf("fsync while churning: %w", err)
			}
			since = 0
		}
	}
}

// stopped is the stop word, as a channel. A hold run has to keep writing while it
// listens, so the console read moves off the path that does the work.
func stopped() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := waitForStop(); err != nil {
			// Nothing to report it to that is not also the console this just lost.
			// Closing the channel ends the run, which is what a lost console means.
			return
		}
	}()
	return done
}

// cmdlineInt reads a numeric kernel-command-line parameter. Absent or unreadable is zero,
// which is the shape each of them had before it existed.
func cmdlineInt(key string) int64 {
	raw, err := os.ReadFile("/proc/cmdline")
	if err != nil {
		return 0
	}
	for _, word := range strings.Fields(string(raw)) {
		v, ok := strings.CutPrefix(word, key+"=")
		if !ok {
			continue
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0
		}
		return n
	}
	return 0
}

// waitForStop blocks until the host sends the stop word down the serial line. There is
// no timeout here and that is deliberate: the host decides when it is done, and a guest
// that powered off on a timer would end the very window the host asked for.
func waitForStop() error {
	scanner := bufio.NewScanner(console)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), stopCommand) {
			return nil
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading the console while held: %w", err)
	}
	return fmt.Errorf("the console closed before %s arrived", stopCommand)
}

func offsets(slot int64) []int64 {
	offs := make([]int64, blocks)
	for i := range offs {
		offs[i] = int64(writeOffset+i*stride) + slot*slotStride
	}
	return offs
}

// readBack reopens the device and compares. Reopening rather than reading through the
// same descriptor is deliberate in the verify boot and harmless in the write one.
func readBack(pattern []byte, slot int64) error {
	g, err := os.Open(device)
	if err != nil {
		return fmt.Errorf("opening %s: %w", device, err)
	}
	defer func() { _ = g.Close() }()

	got := make([]byte, patternBytes)
	for _, off := range offsets(slot) {
		if _, err := g.ReadAt(got, off); err != nil {
			return fmt.Errorf("reading back at %d: %w", off, err)
		}
		if !bytes.Equal(got, pattern) {
			return fmt.Errorf("read-back mismatch at %d: the disk did not return what was written to it", off)
		}
	}
	return nil
}

// openConsole opens /dev/console for reading as well as writing. The read half is what
// a hold run waits on: with `console=ttyS0` on the command line and `-serial stdio` on
// QEMU's, this descriptor is the host's own standard input.
func openConsole() {
	if f, err := os.OpenFile("/dev/console", os.O_RDWR, 0); err == nil {
		console = f
		return
	}
	// A kernel that did give us stdio (a console node baked into the image) still
	// works: fd 1 is then the console it opened.
	console = os.Stdout
}

func report(format string, args ...any) {
	if console == nil {
		console = os.Stdout
	}
	_, _ = fmt.Fprintf(console, format+"\n", args...)
	// PID 1 has no one to flush its buffers on exit, and an unflushed verdict is an
	// invisible one.
	_ = console.Sync()
}

// powerOff shuts the machine down from inside. It matters that the guest does this
// rather than the host killing QEMU: only then has something inside the VM agreed that
// the volume is in a state worth booting again, which is what the second boot asserts on.
func powerOff() {
	_, _, _ = syscall.Syscall(syscall.SYS_REBOOT,
		uintptr(linuxRebootMagic1), uintptr(linuxRebootMagic2),
		uintptr(linuxRebootCmdPowerOff))
	// If that returned, the machine is not going down on its own. Spin rather than
	// return: a panic here would be reported as a guest crash.
	for {
		_ = syscall.Pause()
	}
}

// The reboot(2) magic numbers, written as literals with their kernel names because that
// is what a reader checks against include/uapi/linux/reboot.h. golang.org/x/sys is not a
// dependency of this binary, which is static and deliberately tiny.
const (
	linuxRebootMagic1      = 0xfee1dead
	linuxRebootMagic2      = 672274793
	linuxRebootCmdPowerOff = 0x4321fedc
)
