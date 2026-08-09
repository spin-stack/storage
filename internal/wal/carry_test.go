package wal_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// The regression these are about: a session's records are ACKed to a guest's fsync and
// not published, the volume is attached again, every placement grants a fresh epoch, and
// the Agent opens a directory that does not exist. The records are on the device and
// nothing an operator can run reaches them — not another attach (a higher epoch again),
// not -rebuild-metadata, and not moving the directory by hand, because the segment header
// carries its own epoch.
//
// The assertions here are about what a *reader* of the volume gets afterwards and what is
// left on the device, never about a field CarryForward set.

const carryRoot = "wal"

const carrySegmentBytes = 8192

// carryPayload is the byte a record at index i carries, so a read-back says which record
// answered it rather than only that something did.
func carryPayload(i int) []byte { return bytes.Repeat([]byte{byte(0xA0 + i)}, 4096) }

// carryOffset scatters the records so more than one segment is written: a single open
// segment would make every assertion about segment files vacuous.
func carryOffset(i int) uint64 { return uint64(i) * 64 << 10 }

// writeUnpublishedSession writes n records under `epoch` and closes the log without
// publishing anything — the state a SIGKILLed Agent leaves behind.
func writeUnpublishedSession(t *testing.T, d *sim.Disk, clk *sim.Clock, vol [16]byte, epoch uint64, enc *wal.Encryption, n int) {
	t.Helper()
	l := wal.NewLog(d, carryRoot, clk, vol, epoch, wal.Limits{SegmentBytes: carrySegmentBytes})
	if enc != nil {
		l.EnableEncryption(enc)
	}
	for i := range n {
		if _, err := l.Write(carryOffset(i), carryPayload(i), 0); err != nil {
			t.Fatalf("write %d under epoch %d: %v", i, epoch, err)
		}
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if files, _ := d.List(wal.SegmentDir(carryRoot, vol, epoch) + "/"); len(files) < 2 {
		t.Fatalf("epoch %d holds %d segment(s); the setup was supposed to seal at least one", epoch, len(files))
	}
}

// resumeGranted opens the log the way an Agent does at attach: awaiting a base, under the
// epoch the Control Plane has just granted.
func resumeGranted(t *testing.T, d *sim.Disk, clk *sim.Clock, vol [16]byte, epoch uint64, enc *wal.Encryption) *wal.Log {
	t.Helper()
	l, err := wal.ResumeAwaitingBase(d, carryRoot, clk, vol, epoch,
		wal.Limits{SegmentBytes: carrySegmentBytes}, enc)
	if err != nil {
		t.Fatalf("resuming volume %x at epoch %d: %v", vol[:4], epoch, err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// readBack is what the guest gets: the log with its base installed, answering a read.
func readBack(t *testing.T, l *wal.Log, i int) []byte {
	t.Helper()
	buf := make([]byte, 4096)
	if err := l.Read(carryOffset(i), buf); err != nil {
		t.Fatalf("reading record %d back: %v", i, err)
	}
	return buf
}

func newCarryFixture(t *testing.T) (*sim.Disk, *sim.Clock, [16]byte) {
	t.Helper()
	return sim.NewDisk(), sim.NewClock(time.Unix(1_700_000_000, 0).UTC()), [16]byte{0x51, 0x7e}
}

// TestAGuestReadsBackWhatAnUnpublishedEpochHeldAfterAFreshGrant is the regression itself,
// at the level the wal package can state it: the volume comes back at the sequence the
// fleet was promised and a read returns the bytes, not zeros and not an error.
//
// The two assertions are separate on purpose. `RecoveredSequence` is the number the
// Agent's durable floor compares against the catalog — it decides whether the volume is
// served at all — and the read is what the guest gets once it is. A carry that moved the
// files but not the view would satisfy the first and hand the guest zeros.
func TestAGuestReadsBackWhatAnUnpublishedEpochHeldAfterAFreshGrant(t *testing.T) {
	d, clk, vol := newCarryFixture(t)
	writeUnpublishedSession(t, d, clk, vol, 2, nil, 6)

	l := resumeGranted(t, d, clk, vol, 3, nil)
	if got := l.ResumeReport().RecoveredSequence; got != 0 {
		t.Fatalf("the granted epoch's directory came back at sequence %d; it is supposed to be empty", got)
	}

	carried, err := l.CarryForward(0)
	if err != nil {
		t.Fatalf("CarryForward: %v", err)
	}
	if got := l.ResumeReport().RecoveredSequence; got != 6 {
		t.Fatalf("after taking up epoch 2 the volume reports sequence %d, and a guest was told 6 was durable "+
			"— this is the number the durability floor refuses the volume on (carried %+v)", got, carried)
	}

	if err := l.InstallBase(cow.NewIntervalMap(), 0); err != nil {
		t.Fatalf("InstallBase: %v", err)
	}
	for i := range 6 {
		if got := readBack(t, l, i); !bytes.Equal(got, carryPayload(i)) {
			t.Fatalf("record %d reads back %x..., the guest wrote %x...", i, got[:8], carryPayload(i)[:8])
		}
	}
}

// TestTheCarriedRecordsSurviveTheProcessThatCarriedThem: the point of rewriting the
// segments rather than renaming them is that the *next* process can open them. A carry
// that left epoch 2's headers in place would satisfy every in-memory assertion above and
// fail here with the foreign-epoch error, which is the error an operator hit when they
// tried the rename by hand.
func TestTheCarriedRecordsSurviveTheProcessThatCarriedThem(t *testing.T) {
	d, clk, vol := newCarryFixture(t)
	writeUnpublishedSession(t, d, clk, vol, 2, nil, 6)

	first := resumeGranted(t, d, clk, vol, 3, nil)
	if _, err := first.CarryForward(0); err != nil {
		t.Fatalf("CarryForward: %v", err)
	}
	if err := first.InstallBase(cow.NewIntervalMap(), 0); err != nil {
		t.Fatalf("InstallBase: %v", err)
	}
	// The guest carries on writing, which is the whole point of getting the volume back.
	// Its record continues the volume's sequence space rather than restarting it: a carry
	// that left the counter behind would reissue a sequence the carried records already
	// hold — a duplicate in one (volume, epoch), and one GCM nonce for two payloads.
	seq, err := first.Write(carryOffset(6), carryPayload(6), 0)
	if err != nil {
		t.Fatalf("writing after the carry: %v", err)
	}
	if seq != 7 {
		t.Fatalf("the write after a carry of sequences 1..6 was issued sequence %d", seq)
	}
	if err := first.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A plain restart at the same epoch: the ordinary path, over the directory the carry
	// wrote.
	second := resumeGranted(t, d, clk, vol, 3, nil)
	if got := second.ResumeReport().RecoveredSequence; got != 7 {
		t.Fatalf("a restart over the carried directory replayed up to sequence %d, not 7", got)
	}
	if err := second.InstallBase(cow.NewIntervalMap(), 0); err != nil {
		t.Fatalf("InstallBase: %v", err)
	}
	for i := range 7 {
		if got := readBack(t, second, i); !bytes.Equal(got, carryPayload(i)) {
			t.Fatalf("after a restart, record %d reads back %x..., the guest wrote %x...", i, got[:8], carryPayload(i)[:8])
		}
	}

	// And the sequences are the ones the fleet was told about. A carry that renumbered
	// them would read back identically and put this volume's records under sequences
	// another host's objects already claim.
	recs, err := wal.ReplaySegments(d, carryRoot, vol, 3)
	if err != nil {
		t.Fatalf("replaying epoch 3: %v", err)
	}
	for i, rec := range recs {
		if rec.Sequence != uint64(i+1) {
			t.Fatalf("carried record %d has sequence %d; carrying forward must not renumber", i, rec.Sequence)
		}
		if rec.Epoch != 3 {
			t.Fatalf("carried record %d says epoch %d, and it is filed under 3", i, rec.Epoch)
		}
	}
}

// TestADrainedEpochIsUnlinked is the leak, which is the same fix's other half: nothing
// ever deleted an old epoch directory, so one A->B->A cycle left a sealed segment beside
// the live one and every attach added another — for ever, charged against the guest
// budget the Agent measures from that filesystem.
func TestADrainedEpochIsUnlinked(t *testing.T) {
	d, clk, vol := newCarryFixture(t)
	writeUnpublishedSession(t, d, clk, vol, 2, nil, 6)

	before, _ := d.List(carryRoot + "/")
	l := resumeGranted(t, d, clk, vol, 3, nil)
	carried, err := l.CarryForward(0)
	if err != nil {
		t.Fatalf("CarryForward: %v", err)
	}

	left, _ := d.List(wal.SegmentDir(carryRoot, vol, 2) + "/")
	if len(left) != 0 {
		t.Fatalf("epoch 2 still holds %v after being carried into epoch 3; every attach would add one more", left)
	}
	if carried.Reclaimed <= 0 {
		t.Fatalf("the drained epoch gave back %d bytes, and it held %d segments", carried.Reclaimed, len(before))
	}
	if now, _ := d.List(carryRoot + "/"); len(now) >= len(before)+len(before) {
		t.Fatalf("the device holds %d segment files after a carry and held %d before: nothing was given back", len(now), len(before))
	}
}

// TestARecordTheObjectStoreAlreadyHoldsIsNotCarriedForward is the correctness condition
// spelled out in CarryForward's doc, and it is the one a "move the whole directory"
// implementation gets wrong.
//
// Sequences are per-volume. A host promoted after this one continues the sequence space
// from the image it loaded, so a record of ours at or below the image's sequence is either
// the record the image was built from — writing it twice — or a sequence somebody else
// reissued, in which case their record is in the image and is the newer one. Replaying
// ours on top would hand a guest a superseded write with no error anywhere.
//
// So the assertion is on the read: the range record 0 covers must answer with the *base*,
// which here stands in for the image, and not with the stale record still on this device.
func TestARecordTheObjectStoreAlreadyHoldsIsNotCarriedForward(t *testing.T) {
	d, clk, vol := newCarryFixture(t)
	writeUnpublishedSession(t, d, clk, vol, 2, nil, 6)

	// The image reproduces everything through sequence 4, and what it holds at record 0's
	// offset is what a later writer put there.
	newer := bytes.Repeat([]byte{0xEE}, 4096)
	base := cow.NewIntervalMap()
	base.Overwrite(carryOffset(0), newer)

	l := resumeGranted(t, d, clk, vol, 3, nil)
	carried, err := l.CarryForward(4)
	if err != nil {
		t.Fatalf("CarryForward: %v", err)
	}
	if err := l.InstallBase(base, 4); err != nil {
		t.Fatalf("InstallBase: %v", err)
	}
	if got := readBack(t, l, 0); !bytes.Equal(got, newer) {
		t.Fatalf("the range the image covers reads back %x..., which is this host's superseded record, "+
			"not the %x... the object store holds — a guest reading its own rolled-back data with no error anywhere",
			got[:8], newer[:8])
	}
	if carried.First != 5 || carried.Last != 6 || carried.Records != 2 {
		t.Fatalf("carried %+v; only sequences 5 and 6 are above what the object store reproduces", carried)
	}
	// And what was above the image is still there, which is the half that must not be
	// lost while the half above is being protected.
	if got := readBack(t, l, 5); !bytes.Equal(got, carryPayload(5)) {
		t.Fatalf("sequence 6 was ACKed and unpublished and reads back %x...", got[:8])
	}
}

// TestAnInterruptedCarryIsFinishedByTheNextAttach covers the crash window this runs in —
// it is the same window that created the problem.
//
// Two arms, because the two halves fail differently: a crash while the records are being
// appended leaves the granted epoch holding a prefix and the source intact, and a crash
// while the source is being unlinked leaves everything already carried. In both the next
// attach must reach the same end state, with every record present exactly once.
func TestAnInterruptedCarryIsFinishedByTheNextAttach(t *testing.T) {
	tests := []struct {
		name string
		// interrupt runs a first, failed attach and returns what it left behind.
		interrupt func(t *testing.T, d *sim.Disk, clk *sim.Clock, vol [16]byte)
	}{{
		name: "the device fails while the records are being appended",
		interrupt: func(t *testing.T, d *sim.Disk, clk *sim.Clock, vol [16]byte) {
			t.Helper()
			// Enough room for the header and a record or two, then ENOSPC.
			d.InjectENOSPC(wal.SegmentDir(carryRoot, vol, 3), 6000)
			l := resumeGranted(t, d, clk, vol, 3, nil)
			if _, err := l.CarryForward(0); err == nil {
				t.Fatal("the carry succeeded on a device with no room: the arm proves nothing")
			}
			if err := l.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			d.ClearENOSPC(wal.SegmentDir(carryRoot, vol, 3))
			if files, _ := d.List(wal.SegmentDir(carryRoot, vol, 2) + "/"); len(files) == 0 {
				t.Fatal("the failed carry unlinked the source anyway; there is nothing left to finish")
			}
		},
	}, {
		name: "the host dies after the records are carried and before the source is unlinked",
		interrupt: func(t *testing.T, d *sim.Disk, clk *sim.Clock, vol [16]byte) {
			t.Helper()
			source := snapshotFiles(t, d, wal.SegmentDir(carryRoot, vol, 2)+"/")
			l := resumeGranted(t, d, clk, vol, 3, nil)
			if _, err := l.CarryForward(0); err != nil {
				t.Fatalf("CarryForward: %v", err)
			}
			if err := l.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			// Put the source back exactly as it was: the unlink is the step that did
			// not survive the crash.
			restoreFiles(t, d, source)
		},
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, clk, vol := newCarryFixture(t)
			writeUnpublishedSession(t, d, clk, vol, 2, nil, 6)
			tc.interrupt(t, d, clk, vol)

			l := resumeGranted(t, d, clk, vol, 3, nil)
			if _, err := l.CarryForward(0); err != nil {
				t.Fatalf("the attach after the interruption could not finish the carry: %v", err)
			}
			if err := l.InstallBase(cow.NewIntervalMap(), 0); err != nil {
				t.Fatalf("InstallBase: %v", err)
			}
			for i := range 6 {
				if got := readBack(t, l, i); !bytes.Equal(got, carryPayload(i)) {
					t.Fatalf("record %d reads back %x... after the interrupted carry was finished", i, got[:8])
				}
			}
			// Exactly once. A retry that appended the whole source again would read back
			// correctly and leave the volume with two records per sequence, which is what
			// the next replay would refuse.
			recs, err := wal.ReplaySegments(d, carryRoot, vol, 3)
			if err != nil {
				t.Fatalf("replaying epoch 3: %v", err)
			}
			if len(recs) != 6 {
				t.Fatalf("epoch 3 holds %d records for a session of 6: %v", len(recs), sequencesOf(recs))
			}
			if left, _ := d.List(wal.SegmentDir(carryRoot, vol, 2) + "/"); len(left) != 0 {
				t.Fatalf("epoch 2 survived the finished carry: %v", left)
			}
		})
	}
}

// TestCarryForwardTouchesNeitherAnotherVolumeNorAnEpochItWasNotGranted. The rule is
// stated in the doc; this is what would have to be true for it to be a rule.
func TestCarryForwardTouchesNeitherAnotherVolumeNorAnEpochItWasNotGranted(t *testing.T) {
	d, clk, vol := newCarryFixture(t)
	other := [16]byte{0x99, 0x99}
	writeUnpublishedSession(t, d, clk, vol, 2, nil, 6)
	writeUnpublishedSession(t, d, clk, other, 2, nil, 3)
	// An epoch above the granted one: a directory this host has not been granted, or has
	// already moved past. Not its business, whatever it holds.
	writeUnpublishedSession(t, d, clk, vol, 9, nil, 3)

	otherBefore := snapshotFiles(t, d, wal.SegmentDir(carryRoot, other, 2)+"/")
	aheadBefore := snapshotFiles(t, d, wal.SegmentDir(carryRoot, vol, 9)+"/")

	l := resumeGranted(t, d, clk, vol, 3, nil)
	carried, err := l.CarryForward(0)
	if err != nil {
		t.Fatalf("CarryForward: %v", err)
	}
	if len(carried.Epochs) != 1 || carried.Epochs[0] != 2 {
		t.Fatalf("the carry drained %v; only this volume's epochs below 3 are its business", carried.Epochs)
	}
	assertUnchanged(t, d, otherBefore, "another volume's WAL")
	assertUnchanged(t, d, aheadBefore, "an epoch above the granted one")
}

// TestAnEncryptedRecordIsResealedUnderTheGrantedEpoch. The payload's GCM AAD and its
// nonce are both derived from (volume, epoch, sequence), so a carry that copied the
// ciphertext would produce a record that cannot be opened under the epoch it is now filed
// as — after the ACK, after the fsync, and only at the next read.
//
// The nonce question is the one worth being explicit about: resealing the same plaintext
// under a new nonce is safe here because the granted epoch has never issued this
// sequence and never will — the log continues from the highest carried one.
func TestAnEncryptedRecordIsResealedUnderTheGrantedEpoch(t *testing.T) {
	d, clk, vol := newCarryFixture(t)
	dek := crypto.DEK{KeyID: 7}
	copy(dek.Key[:], bytes.Repeat([]byte{0x2C}, crypto.DEKSize))
	enc, err := wal.NewEncryption(dek, vol)
	if err != nil {
		t.Fatalf("NewEncryption: %v", err)
	}
	writeUnpublishedSession(t, d, clk, vol, 2, enc, 6)
	sealed := snapshotFiles(t, d, wal.SegmentDir(carryRoot, vol, 2)+"/")

	l := resumeGranted(t, d, clk, vol, 3, enc)
	if _, err := l.CarryForward(0); err != nil {
		t.Fatalf("CarryForward: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// The next process opens the carried directory with the same DEK. This is what fails
	// if the ciphertext was copied rather than resealed.
	second := resumeGranted(t, d, clk, vol, 3, enc)
	if err := second.InstallBase(cow.NewIntervalMap(), 0); err != nil {
		t.Fatalf("InstallBase: %v", err)
	}
	for i := range 6 {
		if got := readBack(t, second, i); !bytes.Equal(got, carryPayload(i)) {
			t.Fatalf("encrypted record %d reads back %x... after being carried forward", i, got[:8])
		}
	}
	// And the bytes on the device are new bytes, not the old ones under a new name: the
	// nonce changed with the epoch, so no two files may be equal.
	carriedFiles := snapshotFiles(t, d, wal.SegmentDir(carryRoot, vol, 3)+"/")
	for _, was := range sealed {
		for _, now := range carriedFiles {
			if bytes.Equal(was.data, now.data) {
				t.Fatalf("%s was copied to %s byte for byte; an encrypted record carried across an epoch must be resealed",
					was.name, now.name)
			}
		}
	}
}

// TestCarryForwardRefusesOnceThisSessionHasWritten. The sequences under the earlier epoch
// and the sequences this session has already issued are the same numbers, so a carry here
// would put two different records under one sequence in one directory — and, for an
// encrypted volume, seal them under one nonce.
func TestCarryForwardRefusesOnceThisSessionHasWritten(t *testing.T) {
	d, clk, vol := newCarryFixture(t)
	writeUnpublishedSession(t, d, clk, vol, 2, nil, 6)

	l := resumeGranted(t, d, clk, vol, 3, nil)
	if _, err := l.Write(0, carryPayload(0), 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := l.CarryForward(0)
	if !errors.Is(err, wal.ErrCarryUnavailable) {
		t.Fatalf("a carry over a session that has already written failed with %v, which does not carry %v",
			err, wal.ErrCarryUnavailable)
	}
	if left, _ := d.List(wal.SegmentDir(carryRoot, vol, 2) + "/"); len(left) == 0 {
		t.Fatal("the refused carry unlinked the source anyway")
	}
}

// TestNothingHappensWhenThisHostHoldsNoEarlierEpoch: nearly every attach. It must not
// list, write or unlink anything, and it must not report that it did — a line printed on
// every restart is read on none of them.
func TestNothingHappensWhenThisHostHoldsNoEarlierEpoch(t *testing.T) {
	d, clk, vol := newCarryFixture(t)
	writeUnpublishedSession(t, d, clk, vol, 3, nil, 4)

	l := resumeGranted(t, d, clk, vol, 3, nil)
	carried, err := l.CarryForward(0)
	if err != nil {
		t.Fatalf("CarryForward: %v", err)
	}
	if !carried.Empty() || carried.Records != 0 {
		t.Fatalf("a volume with nothing below its granted epoch reported %+v", carried)
	}
	if got := l.ResumeReport().RecoveredSequence; got != 4 {
		t.Fatalf("the ordinary same-epoch restart came back at sequence %d, not 4", got)
	}
}

// --- helpers ---------------------------------------------------------------------

type carriedFile struct {
	name string
	data []byte
}

// snapshotFiles reads what is under a prefix, so a later assertion can say "unchanged"
// about the bytes rather than about the names.
func snapshotFiles(t *testing.T, d *sim.Disk, prefix string) []carriedFile {
	t.Helper()
	names, err := d.List(prefix)
	if err != nil {
		t.Fatalf("listing %s: %v", prefix, err)
	}
	out := make([]carriedFile, 0, len(names))
	for _, name := range names {
		f, err := d.Open(name)
		if err != nil {
			t.Fatalf("opening %s: %v", name, err)
		}
		size, err := f.Size()
		if err != nil {
			t.Fatalf("sizing %s: %v", name, err)
		}
		buf := make([]byte, size)
		if size > 0 {
			if _, err := f.ReadAt(buf, 0); err != nil {
				t.Fatalf("reading %s: %v", name, err)
			}
		}
		_ = f.Close()
		out = append(out, carriedFile{name: name, data: buf})
	}
	return out
}

// restoreFiles puts files back exactly as they were, which is what a crash between the
// carry and the unlink leaves on the device.
func restoreFiles(t *testing.T, d *sim.Disk, files []carriedFile) {
	t.Helper()
	for _, f := range files {
		w, err := d.Create(f.name)
		if err != nil {
			t.Fatalf("recreating %s: %v", f.name, err)
		}
		if _, err := w.Append(f.data); err != nil {
			t.Fatalf("rewriting %s: %v", f.name, err)
		}
		if err := w.Sync(); err != nil {
			t.Fatalf("syncing %s: %v", f.name, err)
		}
		_ = w.Close()
	}
}

func assertUnchanged(t *testing.T, d *sim.Disk, was []carriedFile, what string) {
	t.Helper()
	if len(was) == 0 {
		t.Fatalf("%s held nothing before the carry: the assertion would be vacuous", what)
	}
	for _, f := range was {
		now := snapshotFiles(t, d, f.name)
		if len(now) != 1 || !bytes.Equal(now[0].data, f.data) {
			t.Fatalf("the carry touched %s, which is %s", f.name, what)
		}
	}
}

func sequencesOf(recs []wal.Record) string {
	out := make([]uint64, len(recs))
	for i, r := range recs {
		out[i] = r.Sequence
	}
	return fmt.Sprint(out)
}

// TestCarryForwardRefusesRatherThanGuess collects the states where taking the earlier
// epoch up would mean inventing something, and asserts the two things that matter in each:
// it fails, and it has not touched the source. The records stay where they are, so an
// operator's next attach tries again — which is precisely the property the bricked state
// did not have.
func TestCarryForwardRefusesRatherThanGuess(t *testing.T) {
	tests := []struct {
		name string
		// spoil turns a healthy pair of directories into the state under test.
		spoil func(t *testing.T, d *sim.Disk, vol [16]byte)
		// encrypted writes the earlier epoch under a DEK the resuming log will not have.
		encrypted bool
		want      error
	}{{
		name: "the records held start above where the object store leaves off",
		spoil: func(t *testing.T, d *sim.Disk, vol [16]byte) {
			t.Helper()
			// The oldest segment is gone, so the directory holds a suffix that begins
			// above sequence 1 — records nothing on this fleet has.
			files := snapshotFiles(t, d, wal.SegmentDir(carryRoot, vol, 2)+"/")
			if err := d.Remove(files[0].name); err != nil {
				t.Fatalf("removing %s: %v", files[0].name, err)
			}
		},
		want: wal.ErrCarryHole,
	}, {
		name: "something that is not a segment is filed under this volume",
		spoil: func(t *testing.T, d *sim.Disk, vol [16]byte) {
			t.Helper()
			f, err := d.Create(carryRoot + "/" + format.UUIDString(vol) + "/2/notes.txt")
			if err != nil {
				t.Fatalf("planting a stray file: %v", err)
			}
			_ = f.Close()
		},
		want: wal.ErrCorruptSegment,
	}, {
		name:      "the volume is encrypted and this Agent was started without its key",
		encrypted: true,
		spoil:     func(*testing.T, *sim.Disk, [16]byte) {},
		want:      nil, // any error; there is no sentinel for a key that is simply absent
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, clk, vol := newCarryFixture(t)
			var enc *wal.Encryption
			if tc.encrypted {
				dek := crypto.DEK{KeyID: 3}
				copy(dek.Key[:], bytes.Repeat([]byte{0x5B}, crypto.DEKSize))
				var err error
				if enc, err = wal.NewEncryption(dek, vol); err != nil {
					t.Fatalf("NewEncryption: %v", err)
				}
			}
			writeUnpublishedSession(t, d, clk, vol, 2, enc, 6)
			tc.spoil(t, d, vol)

			// Resumed without the key on the encrypted arm: that is the Agent that was
			// started with no -kek-file, and it must not silently drop the records.
			l := resumeGranted(t, d, clk, vol, 3, nil)
			_, err := l.CarryForward(0)
			switch {
			case err == nil:
				t.Fatal("the carry succeeded over a state it cannot account for")
			case tc.want != nil && !errors.Is(err, tc.want):
				t.Fatalf("the carry failed with %v, which does not carry %v", err, tc.want)
			}
			if left, _ := d.List(wal.SegmentDir(carryRoot, vol, 2) + "/"); len(left) == 0 {
				t.Fatal("the refused carry unlinked the source anyway; there is nothing left for the next attach to try")
			}
		})
	}
}
