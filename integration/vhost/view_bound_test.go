//go:build integration

package vhost_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/blockdev"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/testinfra"
	"github.com/spin-stack/storage/internal/wal"
)

// The contract with integration/guestinit's walking hold run, from the host's side.
// guestinit is a `main` package, so these cannot be shared as constants with the program
// that prints them — testinfra spells GUESTINIT-ALIVE out for the same reason.
const (
	// guestWalk turns a hold run from "rewrite eight blocks forever" into "walk the
	// device, one distinct block at a time". The whole test rests on that difference:
	// the rotating run pins the read view at 32 KiB, so no soak of any length can reach
	// a bound on it.
	guestWalk = "spin.hold=walk"

	guestRefused   = "GUESTINIT-REFUSED"
	guestTrimmed   = "GUESTINIT-TRIMMED"
	guestRecovered = "GUESTINIT-RECOVERED"
)

// viewBound is the read-view ceiling this test gives the volume, and it is small on
// purpose. The default is 256 MiB (wal.DefaultMaxViewBytes); a guest writing 4 KiB blocks
// and fsyncing each one would need ~65,000 round trips through TCG to reach it, which is
// the hour this test is deliberately not. 1 MiB is ~256 blocks — the same code path, the
// same guest, minutes instead.
//
// Nothing else here is bounded: MaxLocalBytes and MaxUnflushedBytes are left at zero, so
// the *only* thing on this host that can refuse the guest's write is the memory bound.
// A test that let two bounds fire could not say which one the guest hit.
const viewBound = 1 << 20

// boundTimeout bounds each step the guest takes after it has booted. The guest runs flat
// out — no pacing in the walking shape — so a step that has not happened in two minutes is
// not going to.
const boundTimeout = 2 * time.Minute

// TestAGuestCrossesTheReadViewBoundAndTrimsItsWayBack drives wal.Limits.MaxViewBytes with
// a real kernel, which nothing had ever done.
//
// The bound exists because of a measurement: one volume at 195,658 distinct 4 KiB writes
// held 1.55 GiB of host RSS, monotonic, with the OOM killer as the only limit — and an OOM
// takes every other tenant's volume on the host with it. Crossing it fails the WRITE with
// wal.ErrBackpressure. Until this test, that sentence was proven only against a wal.Log a
// unit test called directly: no guest had ever met it, so nothing knew what a guest *sees*
// when it happens, and nothing knew whether a guest that hit it could get out.
//
// So the assertions are the guest's own experience, in order:
//
//   - it writes distinct blocks and the host takes them (heartbeats);
//   - a write is refused, and the refusal reaches it as an I/O error on `fsync` — not as a
//     hang, which the timeouts here rule out, and not as a silent success, which the walk
//     rules out by failing if it reaches the end of the device with everything taken;
//   - it trims with BLKDISCARD **while over the bound**, and the trim is accepted;
//   - it writes 64 more blocks afterwards. That is the half that makes the bound
//     survivable: without it the bound is a one-way door, and a volume that hits it is
//     finished for the session with no operator action short of stopping it.
//
// The host's side of the same events is checked alongside, because a guest reporting all
// of that against a backend doing nothing would be a tautology: the device latches the
// refusal an operator reads, the read_view_bytes gauge stays inside the ceiling, and the
// WAL keeps growing after the trim.
func TestAGuestCrossesTheReadViewBoundAndTrimsItsWayBack(t *testing.T) {
	kernel, initramfs := testinfra.GuestImages(t)
	ctx := t.Context()

	p, err := obs.NewTestProvider("vhost-view-bound")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(ctx) }()

	l := startWAL(t, ctx, wal.Limits{MaxViewBytes: viewBound, SegmentBytes: 4 << 20},
		func(func(uint64, []byte)) {})
	l.log.SetRecorder(obs.NewRecorder(p.Metrics), "vol-view-bound")

	// The operator-facing latch, which is the only view a host has of a condition the
	// guest experiences as EIO. Read before the guest boots as well as after, so "it is
	// set" cannot be satisfied by something that was already set.
	dev, ok := l.dev.(*blockdev.Device)
	if !ok {
		t.Fatalf("this lane's backend is %T, not a *blockdev.Device", l.dev)
	}
	if reason, refused := dev.RefusedForSpace(); refused {
		t.Fatalf("the device had already refused a request before the guest booted: %s", reason)
	}

	g := testinfra.StartLinuxGuest(t, l.sock, kernel, initramfs, testinfra.GuestHold, guestWalk)
	g.WaitForLine(t, testinfra.GuestAlive, testinfra.GuestBootTimeout)

	// 1. The bound acts. The guest prints this only after a write(2) it issued came back
	// as an error — from the fsync, since the write itself is buffered.
	g.WaitForLine(t, guestRefused, boundTimeout)

	// The host's half of that instant, with QEMU still running.
	reason, refused := dev.RefusedForSpace()
	if !refused {
		t.Fatal("the guest was refused a write and this device never latched it: " +
			"an operator would have no way to see the condition the guest is in")
	}
	t.Logf("the device reports: %s", reason)

	// And the ceiling held. read_view_bytes is what the Agent scrapes, and it counts the
	// payload the view holds; it may exceed the bound by the one request that was in
	// flight when it crossed, and by nothing more.
	held := gauge(t, ctx, p, "read_view_bytes")
	if int64(held) > viewBound+4096 {
		t.Fatalf("the read view holds %.0f bytes against a %d-byte bound — past it by more "+
			"than the one request that crossed it", held, viewBound)
	}
	if held < viewBound/2 {
		t.Fatalf("the guest was refused with only %.0f bytes in a %d-byte view: it did not get "+
			"most of what the bound promised it", held, viewBound)
	}

	// 2. The escape hatch is reachable from over the bound. A DISCARD carries no view
	// charge for exactly this reason; if it did, the guest would be refused here too and
	// the walk would fail rather than print this line.
	g.WaitForLine(t, guestTrimmed, boundTimeout)
	afterTrim := l.log.Watermarks()

	// 3. And the door stays open: 64 blocks written and fsynced after the trim.
	g.WaitForLine(t, guestRecovered, boundTimeout)
	if now := l.log.Watermarks(); now.Local <= afterTrim.Local {
		t.Fatalf("the guest says it wrote after trimming and the WAL did not move: local %d → %d",
			afterTrim.Local, now.Local)
	}

	out := g.Stop(t, boundTimeout)
	switch {
	case strings.Contains(out, "GUESTINIT-FAIL"):
		t.Fatalf("the guest failed while walking the device:\n%s", testinfra.VerdictLines(out))
	case !strings.Contains(out, "GUESTINIT-PASS"):
		t.Fatalf("the guest never answered the stop it was sent:\n%s", testinfra.VerdictLines(out))
	}

	// The order the three lines appeared in. Each was waited for separately above, and
	// separate waits would all be satisfied by a console that printed them backwards.
	assertOrder(t, out, guestRefused, guestTrimmed, guestRecovered)

	// INV-18 over a run that spent part of its life in backpressure. The durability path
	// is where a stray upload would hide, and being refused is the state in which a
	// backend would be most tempted to go and make room.
	if got := l.store.Puts(); got != 0 {
		t.Fatalf("a guest that crossed the read-view bound made this backend issue %d PUT(s); "+
			"§14.8 says the ACK is local", got)
	}
	w := l.log.Watermarks()
	t.Logf("the guest crossed a %d-byte read-view bound, trimmed, and wrote on: local=%d durable=%d",
		viewBound, w.Local, w.Durable)
}

// holdViewBytes is what the *rotating* hold run holds in the read view, for ever:
// guestinit's holdBlocks (8) blocks of patternBytes (4096), rewritten in place. It is a
// number from the other side of a package boundary and it is written down rather than
// derived, because the whole point of the assertion below is that it does not move.
const holdViewBytes = 8 * 4096

// rotateRounds is how many iterations the steady-state guest must complete before it is
// believed. Each is 4 KiB, so 512 of them put 2 MiB through the log — twice the bound —
// while holding 32 KiB, a thirty-second of it. Rotate mode is paced at 50ms an iteration,
// which is where the ~26s this test costs comes from; fewer rounds would be cheaper and
// would stop distinguishing "the bound is on what the view holds" from "the bound has not
// been reached yet".
const rotateRounds = 512

// TestASteadyStateGuestIsNeverRefusedByTheReadViewBound is the half of the bound that has
// to *not* fire, with a real kernel driving it.
//
// The ordinary guest is not the walking one above. It is a database checkpointing the same
// pages and a journal overwriting the same records — a working set that fits, rewritten
// without end. Such a guest must never be throttled however many bytes it puts through the
// log, and the bound only has that property because it is measured against what the read
// view *holds*. Put it on anything cumulative — bytes appended, sequence, segments
// retained — and every one of these guests eventually stops, which is a far worse failure
// than the one the bound is for.
//
// The rotating hold shape exists for this, and this is what keeps it: nothing else in the
// tree runs a real guest against a *reachable* view bound and asserts it stays silent.
// Both guests here run under the same 1 MiB ceiling; the only difference is where they
// write.
func TestASteadyStateGuestIsNeverRefusedByTheReadViewBound(t *testing.T) {
	kernel, initramfs := testinfra.GuestImages(t)
	ctx := t.Context()

	p, err := obs.NewTestProvider("vhost-view-steady")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(ctx) }()

	l := startWAL(t, ctx, wal.Limits{MaxViewBytes: viewBound, SegmentBytes: 4 << 20},
		func(func(uint64, []byte)) {})
	l.log.SetRecorder(obs.NewRecorder(p.Metrics), "vol-view-steady")
	dev, ok := l.dev.(*blockdev.Device)
	if !ok {
		t.Fatalf("this lane's backend is %T, not a *blockdev.Device", l.dev)
	}

	// The default shape, named rather than defaulted: this test is *about* which shape it
	// gets, and a silent default is the thing a later edit changes without noticing.
	g := testinfra.StartLinuxGuest(t, l.sock, kernel, initramfs, testinfra.GuestHold, "spin.hold=rotate")
	g.WaitForLine(t, fmt.Sprintf("%s %d ", testinfra.GuestAlive, rotateRounds),
		testinfra.GuestBootTimeout)

	// It got there without being refused once. Every iteration is a write and an fsync, so
	// a single refusal would have failed the guest's fsync and ended the run before this.
	if reason, refused := dev.RefusedForSpace(); refused {
		t.Fatalf("a guest rewriting %d bytes in place was refused after %d KiB of writes: %s",
			holdViewBytes, rotateRounds*4, reason)
	}
	held := gauge(t, ctx, p, "read_view_bytes")
	if int64(held) != holdViewBytes {
		t.Fatalf("after %d rewrites of the same %d blocks the read view holds %.0f bytes, not %d: "+
			"it is growing with what the guest wrote instead of with what it holds",
			rotateRounds, holdViewBytes/4096, held, holdViewBytes)
	}

	out := g.Stop(t, boundTimeout)
	switch {
	case strings.Contains(out, "GUESTINIT-FAIL"):
		t.Fatalf("the steady-state guest failed:\n%s", testinfra.VerdictLines(out))
	case !strings.Contains(out, "GUESTINIT-PASS"):
		t.Fatalf("the guest never answered the stop it was sent:\n%s", testinfra.VerdictLines(out))
	}
	w := l.log.Watermarks()
	t.Logf("%d records through a %d-byte view bound, %d bytes held, nothing refused",
		w.Local, viewBound, int64(held))
}

// gauge reads one collected gauge, failing with the whole set when it is absent — a
// missing series and a zero one are different problems and the message should say which.
func gauge(t *testing.T, ctx context.Context, p *obs.Provider, name string) float64 {
	t.Helper()
	values, err := p.GaugeValues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	v, ok := values[name]
	if !ok {
		t.Fatalf("nothing collected for %s; got %v", name, values)
	}
	return v
}

// assertOrder fails unless the given lines appear in the console in the order given.
func assertOrder(t *testing.T, console string, want ...string) {
	t.Helper()
	at := 0
	for _, w := range want {
		i := strings.Index(console[at:], w)
		if i < 0 {
			t.Fatalf("%s never appeared, or appeared before %s:\n%s", w, want[0],
				testinfra.VerdictLines(console))
		}
		at += i + len(w)
	}
}
