//go:build integration

package vhost_test

import (
	"strings"
	"testing"

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

	code, out := testinfra.RunLinuxGuest(t, ctx, l.sock, kernel, initramfs)

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
