//go:build integration

package vhost_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/testinfra"
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
	kernel, initramfs := testinfra.GuestImages(t)
	ctx := t.Context()

	// The same WAL-backed lane the other tests use, with nothing seeded: this guest
	// brings its own filesystem (an initramfs in RAM) and touches the device only where
	// it means to.
	l := startWAL(t, ctx, wal.Limits{}, func(func(uint64, []byte)) {})

	code, out := testinfra.RunLinuxGuest(t, l.sock, kernel, initramfs)

	// The verdict first: it says *what* failed, where an exit code says only that
	// something did.
	switch {
	case strings.Contains(out, "GUESTINIT-FAIL"):
		t.Fatalf("the guest reported a failure:\n%s", testinfra.VerdictLines(out))
	case !strings.Contains(out, "GUESTINIT-PASS"):
		t.Fatalf("the guest never reported a verdict (exit %d). It must always print one and power off:\n%s",
			code, out)
	}

	// And the backend's side of the same event, because a guest that printed PASS while
	// this backend did nothing would make the whole lane a tautology.
	//
	// Asserted as the outcome rather than as a message count: the guest's write reached
	// the WAL, and a FLUSH — the only thing that advances `durable` — happened. Nothing
	// else in this test issues one; the test never touches l.dev.
	//
	// It used to also require a verified object in the store. That went with ADR-0026:
	// a FLUSH is fdatasync and an ACK, so the store is empty here on purpose, and the
	// PUT count is asserted to be zero rather than the assertion being dropped — with a
	// real kernel in the loop that is the strongest form INV-18 has ever had.
	w := l.log.Watermarks()
	switch {
	case w.Local == 0:
		t.Fatal("the guest's write never reached the WAL")
	case w.Durable == 0:
		t.Fatalf("the guest's fsync advanced nothing: local=%d, durable=0 — no FLUSH reached this backend", w.Local)
	}
	if got := l.store.Puts(); got != 0 {
		t.Fatalf("a real kernel's fsync issued %d PUT(s); §14.8 says the ACK is local", got)
	}
	t.Logf("a real kernel's fsync made %d record(s) durable locally, with no object-store traffic", w.Durable)
}

// TestAGuestStaysAliveWhileTheHostWatchesItWrite is the harness proof, and the reason it
// is worth a test of its own is that until it existed, **nothing in this repository had
// ever observed anything while a guest was running**.
//
// `RunLinuxGuest` ended in `cmd.Run()`. Every lane therefore booted a guest, waited for it
// to power off, and only then did whatever it was really testing — the e2e lane's "live"
// snapshot is requested thirteen lines after the guest has gone. Four seams are
// unreachable that way: a snapshot taken mid-write, an Agent restart with a guest
// attached, a Control Plane restart under a live Agent, and the vhost reconnect path
// (RISK-10).
//
// So this asserts liveness three different ways, because each alone has a way of being
// true over a dead guest:
//
//   - the WAL grows *after* the host read a watermark — a guest that had exited could not
//     move it;
//   - a heartbeat numbered higher than any seen before that arrives afterwards — output
//     already buffered would satisfy a plain "a heartbeat exists";
//   - the guest answers a word the host sends down the serial line, by powering the
//     machine off and reporting a verdict. Nothing buffered, cached or replayed can do
//     that.
//
// Proven against a planted bug: making hold() return after its first iteration — a guest
// that starts, writes once and exits — leaves the first heartbeat and the first watermark
// intact, and the test fails at the second observation with "the WAL stopped growing".
func TestAGuestStaysAliveWhileTheHostWatchesItWrite(t *testing.T) {
	kernel, initramfs := testinfra.GuestImages(t)
	ctx := t.Context()

	l := startWAL(t, ctx, wal.Limits{}, func(func(uint64, []byte)) {})

	g := testinfra.StartLinuxGuest(t, l.sock, kernel, initramfs, testinfra.GuestHold)
	// Boot is the slow part under TCG; everything after it is the guest's own loop.
	g.WaitForLine(t, testinfra.GuestAlive, testinfra.GuestBootTimeout)

	// The host's first observation, taken with QEMU still running. Both watermarks are
	// already non-zero here — the guest fsyncs before it says anything — so what the next
	// step waits for is movement, not a first sign of life.
	before := l.log.Watermarks()
	seen := lastHeartbeat(t, g.Console())

	waitUntil(t, liveTimeout, "the WAL to grow under a running guest", func() bool {
		return l.log.Watermarks().Durable > before.Durable
	})
	after := l.log.Watermarks()

	// And a heartbeat the guest cannot have printed before the host looked. `seen+2`
	// rather than `seen+1`: the line the host read may have been printed a moment before
	// it was read, so the *next* one is not strictly after the observation, and the one
	// after that is.
	g.WaitForLine(t, fmt.Sprintf("%s %d ", testinfra.GuestAlive, seen+2), liveTimeout)

	// The other direction of the same wire. A word goes down the serial line and a
	// program inside the VM reads it, stops its loop, reads back what it last wrote and
	// powers the machine off — which is why the verdict below is evidence and QEMU's
	// exit status is not.
	out := g.Stop(t, liveTimeout)
	switch {
	case strings.Contains(out, "GUESTINIT-FAIL"):
		t.Fatalf("the guest failed while holding the device open:\n%s", testinfra.VerdictLines(out))
	case !strings.Contains(out, "GUESTINIT-PASS"):
		t.Fatalf("the guest never answered the stop it was sent:\n%s", testinfra.VerdictLines(out))
	}

	// INV-18 over a run that issued hundreds of FLUSHes rather than one. The other guest
	// test asserts it for a single fsync; a guest that syncs continuously is where a
	// stray upload on the durability path would actually show itself.
	if got := l.store.Puts(); got != 0 {
		t.Fatalf("a guest that fsynced %d times issued %d PUT(s); §14.8 says the ACK is local",
			after.Durable, got)
	}
	t.Logf("the guest advanced durable %d → %d while this test watched, and stopped when told",
		before.Durable, after.Durable)
}

// liveTimeout bounds each observation made while the guest is up. Generous against a
// loaded machine and still far below the boot timeout: the guest writes every 50ms, so
// anything this test waits for has either happened within a second or is not going to.
const liveTimeout = 60 * time.Second

// lastHeartbeat returns the highest iteration the guest has announced so far. It fails
// rather than returning zero when there is none: zero is also what a guest that never
// spoke would produce, and the caller would then wait for "ALIVE 2" and get it from the
// start of a perfectly ordinary run.
func lastHeartbeat(t *testing.T, console string) int {
	t.Helper()
	last := -1
	for _, line := range strings.Split(console, "\n") {
		_, rest, ok := strings.Cut(line, testinfra.GuestAlive+" ")
		if !ok {
			continue
		}
		field, _, _ := strings.Cut(strings.TrimSpace(rest), " ")
		n, err := strconv.Atoi(field)
		if err != nil {
			t.Fatalf("the guest printed an unparseable heartbeat %q", line)
		}
		if n > last {
			last = n
		}
	}
	if last < 0 {
		t.Fatalf("no %s line in the guest's console:\n%s", testinfra.GuestAlive, console)
	}
	return last
}

// waitUntil polls until cond holds. Polling is right and a sleep is not: what is being
// waited on is another *process* doing something, and the only alternative to asking is
// guessing how long it takes. Same shape as integration/e2e's waitFor, deliberately —
// the lanes are INV-01's documented exception (DEV-0016) and they should not each invent
// their own timing primitive.
func waitUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}
