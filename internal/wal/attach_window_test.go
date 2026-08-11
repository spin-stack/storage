package wal_test

import (
	"bytes"
	"testing"
	"testing/synctest"

	"github.com/spin-stack/storage/internal/cow"
)

// The attach window: a log is resumed under a freshly granted epoch and served *before*
// its base has been fetched, so there is a stretch — one object-store round trip wide —
// in which a guest already has the device and the sequence space is not settled yet.
//
// `Read` has always parked on the base. `Write` did not, and a single guest write inside
// that window made `CarryForward` refuse for ever: the session that was stranded under the
// earlier epoch could no longer be taken up, the durable floor then refused the volume, and
// every retry granted another epoch and made it worse. That is the bricked volume of
// carry_test.go reached through a door the Agent opens itself — no operator mistake, no
// crash, just a guest that boots quickly.
//
// synctest is what makes it an assertion rather than a race. `synctest.Wait` returns only
// when every other goroutine in the bubble is durably blocked or done, so at that line the
// guest's write has either been appended — which is the defect — or is parked on the base,
// which is the rule. Both worlds are deterministic; there is no interval to tune.

// TestAGuestWriteBeforeTheBaseArrivesCannotStrandTheEarlierEpoch.
//
// The assertions are what a reader of the volume gets: the six records the earlier epoch
// held come back, and so does the write the guest issued during the window. A fix that
// simply dropped the early write would satisfy the first and lose the guest's bytes.
func TestAGuestWriteBeforeTheBaseArrivesCannotStrandTheEarlierEpoch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, clk, vol := newCarryFixture(t)
		writeUnpublishedSession(t, d, clk, vol, 2, nil, 6)

		// The attach: a fresh epoch, the base not yet fetched.
		l := resumeGranted(t, d, clk, vol, 3, nil)

		type written struct {
			seq uint64
			err error
		}
		guest := bytes.Repeat([]byte{0x5A}, 4096)
		done := make(chan written, 1)
		go func() {
			seq, err := l.Write(carryOffset(9), guest, 0)
			done <- written{seq, err}
		}()

		// Everything else in the bubble is now blocked or finished. The write is either in
		// the WAL or parked; nothing about what follows is timing.
		synctest.Wait()

		carried, err := l.CarryForward(0)
		if err != nil {
			t.Fatalf("a guest write during the attach window made the stranded epoch impossible to take up: %v\n"+
				"the records are on this host's device and the durable floor will refuse the volume for ever", err)
		}
		if carried.Last != 6 {
			t.Fatalf("the carry took up records %d..%d of the six the earlier epoch held", carried.First, carried.Last)
		}

		if err := l.InstallBase(cow.NewIntervalMap(), 0); err != nil {
			t.Fatalf("InstallBase: %v", err)
		}

		w := <-done
		if w.err != nil {
			t.Fatalf("the guest write issued during the attach window failed: %v", w.err)
		}
		if w.seq != 7 {
			t.Fatalf("the guest's write took sequence %d; the six carried records end at 6, so anything at or below "+
				"that is a second record under a sequence the volume has already used", w.seq)
		}

		for i := range 6 {
			if got := readBack(t, l, i); !bytes.Equal(got, carryPayload(i)) {
				t.Fatalf("record %d reads back %x..., the guest wrote %x...", i, got[:8], carryPayload(i)[:8])
			}
		}
		if got := readBack(t, l, 9); !bytes.Equal(got, guest) {
			t.Fatalf("the write the guest made during the attach window reads back %x..., it wrote %x...",
				got[:8], guest[:8])
		}
	})
}

// TestADiscardBeforeTheBaseArrivesCannotStrandTheEarlierEpoch is the same window through
// the other append path. A Linux guest trims early — `mkfs` and `fstrim` on a fresh root
// both do — and a DISCARD moves the sequence counter exactly as a WRITE does, so a rule
// that covered only WRITE would leave the volume brickable by a guest that never wrote a
// byte of data.
func TestADiscardBeforeTheBaseArrivesCannotStrandTheEarlierEpoch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, clk, vol := newCarryFixture(t)
		writeUnpublishedSession(t, d, clk, vol, 2, nil, 6)

		l := resumeGranted(t, d, clk, vol, 3, nil)

		done := make(chan error, 1)
		go func() {
			_, err := l.Discard(carryOffset(0), 4096)
			done <- err
		}()
		synctest.Wait()

		if _, err := l.CarryForward(0); err != nil {
			t.Fatalf("a guest DISCARD during the attach window made the stranded epoch impossible to take up: %v", err)
		}
		if err := l.InstallBase(cow.NewIntervalMap(), 0); err != nil {
			t.Fatalf("InstallBase: %v", err)
		}
		if err := <-done; err != nil {
			t.Fatalf("the guest DISCARD issued during the attach window failed: %v", err)
		}

		// The discard lands on top of the carried record, not under it: it was issued
		// after them, and the range it names must read as zero.
		if got := readBack(t, l, 0); !bytes.Equal(got, make([]byte, 4096)) {
			t.Fatalf("the range the guest discarded reads back %x..., it must read as zero", got[:8])
		}
		if got := readBack(t, l, 1); !bytes.Equal(got, carryPayload(1)) {
			t.Fatalf("record 1 reads back %x..., the guest wrote %x...", got[:8], carryPayload(1)[:8])
		}
	})
}
