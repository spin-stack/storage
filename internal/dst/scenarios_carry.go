package dst

// Carrying an unpublished session across an epoch (wal.Log.CarryForward), with the
// device dying at every point the carry can be interrupted.
//
// The code this exercises is on the recovery path and it rewrites records, reseals
// encrypted payloads under a new epoch and unlinks directories. It exists because two
// separately-correct fixes together bricked a volume: the epoch is bumped on every
// placement, so an unpublished session's WAL was stranded under an epoch nobody would
// open again, and the durable floor then refused the volume for ever with the guest's
// bytes intact on the disk.
//
// Its whole correctness argument is that it is *interruptible* — append every kept
// record under the granted epoch, fdatasync, then unlink the sources oldest-first, so
// a crash anywhere leaves a state the next attach finishes rather than one it has to be
// rescued from. That is a claim about every intermediate state, which is precisely what
// a scenario that stages two or three hand-picked moments cannot settle. So the arm
// below dies at *every* write the carry performs, discovered by running until one does
// not die, and asserts the same end state after each.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

func carryScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "carry-forward-survives-a-crash-at-every-point", Run: scenarioCarryForwardCrashPoints},
	}
}

func carryCheckers() []Checker { return []Checker{NewCarriedRecordChecker()} }

const (
	// carryGrantedEpoch is what the Control Plane grants on the next placement — every
	// placement bumps the epoch, which is what strands the sessions below it. The
	// stranded epochs are the ones under it (carryArm.epochs).
	carryGrantedEpoch = 3
	// carryDSTSegmentBytes is small enough that a handful of records fills more than one
	// segment. That is not decoration: "unlink oldest-first" and "a crash between two
	// unlinks" are not states a single-segment source can be in.
	carryDSTSegmentBytes = 4096
	carryPayloadBytes    = 1024
	// carryMaxCrashPoints bounds the discovery loop. The carry performs on the order of
	// twenty writes for the session sizes here; a run that passes this has stopped
	// converging and the scenario should say so rather than spin.
	carryMaxCrashPoints = 128
)

// errHostDied is what the device answers once the host is gone. A crash is not an error
// a caller sees — the process is not there to see it — but a simulated one has to end
// the call somehow, and the only property that matters is that no write after the death
// point reaches the device. See deadDevice.
var errHostDied = errors.New("dst: the host died")

// CarriedRecordChecker enforces the property the carry exists to keep: **a record a
// guest was told was durable is never lost and never changes its bytes as it crosses an
// epoch.**
//
// Three rules, because the carry can break the promise three ways and only one of them
// is visible as an absence:
//
//   - Lost. The records are appended under the granted epoch and fdatasync'd *before* a
//     single source directory is unlinked. Reverse those and a host that dies in between
//     has destroyed the thing it was rescuing: the sources are gone and the copies were
//     only ever in the page cache. Nothing else in this tree watches that ordering.
//   - Changed. A carried record is re-encoded, and an encrypted one is opened under the
//     epoch it was sealed with and resealed under the granted one, because the GCM AAD
//     and the nonce are both derived from (volume, epoch, sequence). Copy the ciphertext
//     instead and the record is unreadable — after the ACK, after the fsync, and only at
//     the next read. The digest here is over the *plaintext* for exactly that reason.
//   - Duplicated. Two records under one (volume, epoch, sequence) is a reused nonce,
//     because that triple is what the nonce is derived from. It is a confidentiality
//     failure and not merely a replay ambiguity, which is why it is a rule of its own
//     rather than a corollary of "the sequences are contiguous".
//
// The judgement is over the plaintext a scan of the *device* reads back, never over what
// CarryForward reported it did.
type CarriedRecordChecker struct {
	// promised is volume -> sequence -> plaintext digest, from the records whose
	// fdatasync returned.
	promised map[string]map[uint64]string
	// seen is volume -> sequence, from the settled observations only.
	seen map[string]map[uint64]bool
	// settled records that a volume was observed after its carry had its last chance to
	// finish. Without it a scenario that returned early would leave Check with nothing
	// to compare and pass, which is the vacuous-checker failure this file is against.
	settled map[string]bool
	// dup keys one observation's (volume, epoch, sequence).
	dup       map[carryRecordKey]bool
	violation error
}

type carryRecordKey struct {
	scan     uint64
	volume   string
	epoch    uint64
	sequence uint64
}

// NewCarriedRecordChecker returns a fresh checker.
func NewCarriedRecordChecker() *CarriedRecordChecker {
	return &CarriedRecordChecker{
		promised: map[string]map[uint64]string{},
		seen:     map[string]map[uint64]bool{},
		settled:  map[string]bool{},
		dup:      map[carryRecordKey]bool{},
	}
}

func (c *CarriedRecordChecker) Name() string { return "acked-records-cross-epochs-intact" }

func (c *CarriedRecordChecker) Observe(e Event) {
	if e.Kind != EventCarry || c.violation != nil {
		return
	}
	switch e.CarryPhase {
	case CarryPromised:
		if c.promised[e.Key] == nil {
			c.promised[e.Key] = map[uint64]string{}
		}
		c.promised[e.Key][e.Sequence] = e.Digest
	case CarrySurvived:
		key := carryRecordKey{scan: e.Scan, volume: e.Key, epoch: e.Epoch, sequence: e.Sequence}
		if c.dup[key] {
			c.violation = fmt.Errorf("volume %s holds sequence %d twice under epoch %d at step %d: "+
				"the reseal nonce is derived from (volume, epoch, sequence), so this is one nonce over two payloads",
				e.Key, e.Sequence, e.Epoch, e.Step)
			return
		}
		c.dup[key] = true
		if want, ok := c.promised[e.Key][e.Sequence]; ok && want != e.Digest {
			c.violation = fmt.Errorf("volume %s sequence %d reads back as %s under epoch %d at step %d, "+
				"and the guest was ACKed on %s: a record's bytes must not change as it crosses an epoch",
				e.Key, e.Sequence, e.Digest, e.Epoch, e.Step, want)
			return
		}
		if !e.Settled {
			return
		}
		c.settled[e.Key] = true
		if c.seen[e.Key] == nil {
			c.seen[e.Key] = map[uint64]bool{}
		}
		c.seen[e.Key][e.Sequence] = true
	}
}

// Check walks the volumes and their sequences in sorted order, not in map order. A DST
// failure whose message depends on Go's map iteration is not a reproduction: the same
// seed would name a different volume on the next run, and the seed is the only thing a
// reader of a failing run has.
func (c *CarriedRecordChecker) Check() error {
	if c.violation != nil {
		return c.violation
	}
	vols := make([]string, 0, len(c.promised))
	for vol := range c.promised {
		vols = append(vols, vol)
	}
	sort.Strings(vols)
	for _, vol := range vols {
		seqs := c.promised[vol]
		if !c.settled[vol] {
			return fmt.Errorf("volume %s promised %d record(s) and no settled observation of it was recorded: "+
				"nothing was compared", vol, len(seqs))
		}
		ordered := make([]uint64, 0, len(seqs))
		for seq := range seqs {
			ordered = append(ordered, seq)
		}
		slices.Sort(ordered)
		for _, seq := range ordered {
			if !c.seen[vol][seq] {
				return fmt.Errorf("volume %s: sequence %d was ACKed as durable and is on no epoch of the device "+
					"after the carry — it was unlinked from the epoch that held it before the copy was durable", vol, seq)
			}
		}
	}
	return nil
}

// deadDevice is a simulated device that stops taking writes at a chosen one.
//
// It is a wrapper and not a new disk: every read, every listing and every byte is the
// Sim's own sim.Disk, and the only thing added is a counter over the *mutating* calls —
// Create, Append, Truncate, Sync, Remove, Rename. Arm it with the number of writes the
// host survives and the call that would be the next one is refused instead, after which
// nothing this process does reaches the device. Pair it with sim.Disk.Crash, which
// discards everything no Sync made durable, and the result is the state a real host
// leaves when the power goes at that instruction.
//
// Counting writes rather than naming moments is the point. "During the appends, between
// the fdatasync and the first unlink, between unlinks" is a list somebody wrote down
// from reading the code, and the crash the code gets wrong is the one that list forgot.
type deadDevice struct {
	d *sim.Disk
	// survives is how many more writes reach the device; negative means the host lives.
	survives int
	died     bool
	// deaf makes Sync return success without persisting anything: a device that ignores
	// fdatasync. It is the planted defect, and it is a defect of the device, never of
	// the code under test.
	deaf bool
}

func (dd *deadDevice) write() error {
	if dd.survives < 0 {
		return nil
	}
	if dd.survives == 0 {
		dd.died = true
		return errHostDied
	}
	dd.survives--
	return nil
}

func (dd *deadDevice) Create(name string) (disk.File, error) {
	if err := dd.write(); err != nil {
		return nil, err
	}
	f, err := dd.d.Create(name)
	if err != nil {
		return nil, err
	}
	return &deadFile{dd: dd, f: f}, nil
}

func (dd *deadDevice) Open(name string) (disk.File, error) {
	f, err := dd.d.Open(name)
	if err != nil {
		return nil, err
	}
	return &deadFile{dd: dd, f: f}, nil
}

func (dd *deadDevice) Remove(name string) error {
	if err := dd.write(); err != nil {
		return err
	}
	return dd.d.Remove(name)
}

func (dd *deadDevice) Rename(oldName, newName string) error {
	if err := dd.write(); err != nil {
		return err
	}
	return dd.d.Rename(oldName, newName)
}

func (dd *deadDevice) Exists(name string) (bool, error)     { return dd.d.Exists(name) }
func (dd *deadDevice) List(prefix string) ([]string, error) { return dd.d.List(prefix) }
func (dd *deadDevice) Usage() (disk.Usage, error)           { return dd.d.Usage() }
func (dd *deadDevice) Lock(name string) (io.Closer, error)  { return dd.d.Lock(name) }

type deadFile struct {
	dd *deadDevice
	f  disk.File
}

func (f *deadFile) Append(p []byte) (int, error) {
	if err := f.dd.write(); err != nil {
		return 0, err
	}
	return f.f.Append(p)
}

func (f *deadFile) Truncate(size int64) error {
	if err := f.dd.write(); err != nil {
		return err
	}
	return f.f.Truncate(size)
}

func (f *deadFile) Sync() error {
	if err := f.dd.write(); err != nil {
		return err
	}
	if f.dd.deaf {
		return nil // reported durable, not persisted
	}
	return f.f.Sync()
}

func (f *deadFile) ReadAt(p []byte, off int64) (int, error) { return f.f.ReadAt(p, off) }
func (f *deadFile) Size() (int64, error)                    { return f.f.Size() }
func (f *deadFile) Close() error                            { return f.f.Close() }

// Whether the device under the carry honours fdatasync. fdatasyncIgnored is the planted
// defect: a volatile write cache the drive lies about, which is the one fault that turns
// "append, fdatasync, then unlink" back into "append, then unlink".
const (
	honestFdatasync  = false
	fdatasyncIgnored = true
)

func scenarioCarryForwardCrashPoints(s *Sim) error {
	return carryForwardCrashPoints(s, honestFdatasync)
}

// carryArm is one shape of volume the whole crash matrix is run for.
//
// The encrypted one is not a variation: it is the half that can fail silently, because a
// ciphertext copied instead of resealed is the right length, passes every watermark
// check, and reads back as garbage only when a guest asks for it.
//
// The two-epoch one is the state an operator reaches by retrying. Every attach grants a
// fresh epoch, so a volume that was detached and reattached twice without publishing
// holds `<vol>/1/` and `<vol>/2/` when it is granted 3 — and it is only with more than
// one drained directory that the unlink order is load-bearing. Within a directory either
// order leaves a contiguous run; across two, unlinking the newest first leaves the older
// directory's *head* beside the younger directory's whole, which is a hole in the volume's
// sequence space and the one shape the next attach refuses outright.
type carryArm struct {
	tag       string
	encrypted bool
	// stranded is how many epochs below the granted one hold records.
	stranded int
}

// epochs returns the stranded epochs, ascending, ending at carryGrantedEpoch-1.
func (a carryArm) epochs() []uint64 {
	out := make([]uint64, 0, a.stranded)
	for e := carryGrantedEpoch - uint64(a.stranded); e < carryGrantedEpoch; e++ {
		out = append(out, e)
	}
	return out
}

func carryForwardCrashPoints(s *Sim, deaf bool) error {
	// Seed-driven: how many records the stranded session holds, and the DEK. How many
	// crash points there are follows from the first, and every one of them is visited
	// whatever the seed — "at every point" is the property, not a sample of it.
	records := 5 + s.Rand.Intn(3)
	var keyByte byte
	for keyByte == 0 {
		keyByte = byte(s.Rand.Intn(256))
	}

	// The arms are independent — each has its own volume and its own WAL root — so the
	// matrix is run to the end and the *first* failure is reported at the end of it,
	// rather than returned the moment it happens. That is not leniency: the checker
	// judges the whole event stream, and a scenario that returned at the first bad crash
	// point would take every later point's evidence out of the trace with it. The
	// difference is not hypothetical — the planted defect's loss is at the last crash
	// point and its first *readability* failure is a third of the way in.
	var firstErr error
	var scan, arm uint64
	configs := []carryArm{
		{tag: "plaintext", stranded: 1},
		{tag: "sealed", encrypted: true, stranded: 1},
		{tag: "two-epochs", stranded: 2},
	}
	for _, cfg := range configs {
		for survives := 0; ; survives++ {
			if survives > carryMaxCrashPoints {
				return errors.Join(firstErr, fmt.Errorf("the %s carry still had a write to die at after %d crash points",
					cfg.tag, carryMaxCrashPoints))
			}
			arm++
			died, err := carryCrashArm(s, cfg, records, keyByte, survives, deaf, arm, &scan)
			if err != nil && firstErr == nil {
				firstErr = err
			}
			if !died {
				s.Notef("the %s carry has %d write(s), and the volume came back after a crash at each of them",
					cfg.tag, survives)
				break
			}
		}
	}
	return firstErr
}

// carryCrashArm stages one crash point on its own volume and its own WAL root, so the
// arms cannot inherit each other's directories — and so that "the same sequence twice
// under one (volume, epoch)" means what it says.
//
// It reports whether the host actually died: the caller walks the crash points until one
// does not, which is how the number of them is discovered from the code rather than
// asserted from a reading of it.
func carryCrashArm(s *Sim, cfg carryArm, records int, keyByte byte, survives int, deaf bool, arm uint64, scan *uint64) (bool, error) {
	root := fmt.Sprintf("carry-%s-%03d", cfg.tag, survives)
	vol := carryVolumeID(arm)
	volKey := format.UUIDString(vol)
	limits := wal.Limits{SegmentBytes: carryDSTSegmentBytes}

	var enc *wal.Encryption
	if cfg.encrypted {
		dek := crypto.DEK{KeyID: 7}
		copy(dek.Key[:], bytes.Repeat([]byte{keyByte}, crypto.DEKSize))
		var err error
		if enc, err = wal.NewEncryption(dek, vol); err != nil {
			return false, err
		}
	}

	// (1) The session(s) that were ACKed and never published: records written under the
	// earlier epoch(s), fdatasync'd — which is the fsync a guest returned on — and the
	// process gone each time. Each stranded epoch holds a *whole* session rather than a
	// share of one, because it is a separate attach that wrote and was detached, and
	// because a directory of one segment cannot be caught mid-unlink.
	//
	// The sequence space is the volume's and not the epoch's, so the second session
	// continues it (wal.NewLogAfter). A directory that restarted at 1 would be a
	// different defect needing a different scenario.
	//
	// The promise events are emitted after the Sync and not before, so what the checker
	// holds the device to is exactly what was ACKed.
	strandedEpochs := cfg.epochs()
	total := records * len(strandedEpochs)
	for written := 0; written < total; written += records {
		epoch := strandedEpochs[written/records]
		session := wal.NewLogAfter(s.Disk, root, s.Clock, vol, epoch, uint64(written), limits)
		if enc != nil {
			session.EnableEncryption(enc)
		}
		for j := written; j < written+records; j++ {
			if _, err := session.Write(carryDSTOffset(j), carryDSTPayload(j), 0); err != nil {
				return false, fmt.Errorf("%s: writing the session stranded under epoch %d: %w", root, epoch, err)
			}
		}
		if err := session.Sync(); err != nil {
			return false, fmt.Errorf("%s: the guest's fsync under epoch %d: %w", root, epoch, err)
		}
		if err := session.Close(); err != nil {
			return false, fmt.Errorf("%s: closing the session under epoch %d: %w", root, epoch, err)
		}
		for j := written; j < written+records; j++ {
			s.Emit(Event{Kind: EventCarry, CarryPhase: CarryPromised, Key: volKey,
				Epoch: epoch, Sequence: uint64(j) + 1, Digest: carryDigestOf(carryDSTPayload(j))})
		}
	}
	// More than one file to unlink, or "a crash between two unlinks" is not a state this
	// arm can be in and every assertion about the order is vacuous.
	files := 0
	for _, epoch := range strandedEpochs {
		segs, err := wal.SegmentFiles(s.Disk, root, vol, epoch)
		if err != nil {
			return false, err
		}
		files += len(segs)
	}
	if files < 2 {
		return false, fmt.Errorf("%s: the stranded session is one segment; a crash between two unlinks is unreachable", root)
	}

	// (2) The attach that strands it: the Control Plane granted a fresh epoch, so the
	// Agent opens a directory that does not exist and the records are under the previous
	// one. The device is armed to die at the chosen write of the carry — and only of the
	// carry, so the resume above is not one of the points.
	dev := &deadDevice{d: s.Disk, survives: -1, deaf: deaf}
	stranded, err := wal.ResumeAwaitingBase(dev, root, s.Clock, vol, carryGrantedEpoch, limits, enc)
	if err != nil {
		return false, fmt.Errorf("%s: resuming under the granted epoch: %w", root, err)
	}
	dev.survives = survives
	_, _ = stranded.CarryForward(0) // the host is dying; nobody reads this error
	_ = stranded.Close()
	if dev.died {
		s.Emit(Event{Kind: EventFault, Msg: fmt.Sprintf("%s: the host died on write %d of the carry", root, survives)})
	}
	s.Disk.Crash()
	s.Emit(Event{Kind: EventRecovery, Msg: root + ": the next attach"})

	// (3) What the crash left. Every epoch, because a half-done carry is legitimately
	// spread over them and the bytes must be right in each. The error is held, not
	// returned: the settled observation at (5) is what the checker's "never lost" rule
	// rests on, and a return here would leave the run with a promise and nothing to
	// compare it against — which is a checker passing for want of evidence.
	*scan++
	_, _, interimErr := carryObserve(s, root, vol, volKey, enc, strandedEpochs, *scan, false)

	// (4) The retry. Every state above must be one the next attach finishes.
	resumed, resumeErr := wal.ResumeAwaitingBase(s.Disk, root, s.Clock, vol, carryGrantedEpoch, limits, enc)
	var carryErr error
	if resumeErr == nil {
		_, carryErr = resumed.CarryForward(0)
	}

	// (5) The settled state, read off the device *before* anything is asserted about it:
	// a scenario that returned here would take the violation out of the event stream
	// with it, and the checker is the half that has to see it.
	*scan++
	granted, leftBehind, scanErr := carryObserve(s, root, vol, volKey, enc, strandedEpochs, *scan, true)

	switch {
	case interimErr != nil:
		return dev.died, fmt.Errorf("%s: a crash on write %d left a WAL no attach can read: %w", root, survives, interimErr)
	case resumeErr != nil:
		return dev.died, fmt.Errorf("%s: the attach after a crash on write %d could not open the WAL: %w", root, survives, resumeErr)
	case carryErr != nil:
		return dev.died, fmt.Errorf("%s: the attach after a crash on write %d could not finish the carry: %w", root, survives, carryErr)
	case scanErr != nil:
		return dev.died, fmt.Errorf("%s: replaying the finished carry: %w", root, scanErr)
	}
	defer func() { _ = resumed.Close() }()

	// (6) What the device holds. Exactly the session, once each, under the granted epoch,
	// and the drained directory gone — a retry that appended the whole source again would
	// read back correctly and leave two records per sequence.
	if len(leftBehind) != 0 {
		return dev.died, fmt.Errorf("%s: the stranded epochs still hold %d record(s) after the carry finished", root, len(leftBehind))
	}
	if len(granted) != total {
		return dev.died, fmt.Errorf("%s: epoch %d holds %d records for a session of %d",
			root, carryGrantedEpoch, len(granted), total)
	}
	for i, rec := range granted {
		if rec.Sequence != uint64(i)+1 {
			return dev.died, fmt.Errorf("%s: record %d carries sequence %d; a carry must not renumber", root, i, rec.Sequence)
		}
		if rec.Epoch != carryGrantedEpoch {
			return dev.died, fmt.Errorf("%s: record %d says epoch %d and is filed under %d", root, i, rec.Epoch, carryGrantedEpoch)
		}
	}

	// (7) The number the durability floor refuses the volume on, and then the bytes a
	// guest gets. The first decides whether the volume is served at all; a carry that
	// moved the files and not the view satisfies it and hands the guest zeros.
	if got := resumed.ResumeReport().RecoveredSequence; got != uint64(total) {
		return dev.died, fmt.Errorf("%s: the volume comes back at sequence %d and a guest was ACKed on %d", root, got, total)
	}
	if err := resumed.InstallBase(cow.NewIntervalMap(), 0); err != nil {
		return dev.died, err
	}
	for i := range total {
		want := carryDSTPayload(i)
		got := make([]byte, len(want))
		if err := resumed.Read(carryDSTOffset(i), got); err != nil {
			return dev.died, fmt.Errorf("%s: reading record %d back: %w", root, i, err)
		}
		if !bytes.Equal(got, want) {
			return dev.died, fmt.Errorf("%s: record %d reads back %x…, the guest wrote %x…", root, i, got[:8], want[:8])
		}
	}
	return dev.died, nil
}

// carryObserve emits one event per WRITE record the device holds for this volume, under
// every stranded epoch and under the granted one, and returns them split by which side of
// the carry they are on.
//
// The digest is over the plaintext, so an encrypted record is opened here — which is the
// only way a ciphertext copied instead of resealed becomes visible. A record that will
// not open is reported as such rather than skipped: an unreadable record is the loudest
// possible way for its bytes to have changed.
func carryObserve(s *Sim, root string, vol [16]byte, volKey string, enc *wal.Encryption,
	stranded []uint64, scan uint64, settled bool,
) (granted, leftBehind []wal.Record, err error) {
	var firstErr error
	for _, epoch := range append(slices.Clone(stranded), carryGrantedEpoch) {
		recs, scanErr := wal.ReplaySegments(s.Disk, root, vol, epoch)
		if scanErr != nil && firstErr == nil {
			firstErr = fmt.Errorf("epoch %d: %w", epoch, scanErr)
		}
		for _, rec := range recs {
			if rec.Type != format.RecordWrite {
				continue
			}
			s.Emit(Event{Kind: EventCarry, CarryPhase: CarrySurvived, Key: volKey, Epoch: epoch,
				Sequence: rec.Sequence, Digest: carryPlaintextDigest(enc, rec), Scan: scan, Settled: settled})
			if epoch == carryGrantedEpoch {
				granted = append(granted, rec)
			} else {
				leftBehind = append(leftBehind, rec)
			}
		}
	}
	return granted, leftBehind, firstErr
}

// carryPlaintextDigest is what a record on the device reads back as.
func carryPlaintextDigest(enc *wal.Encryption, rec wal.Record) string {
	if rec.KeyID == 0 {
		return carryDigestOf(rec.Payload)
	}
	if enc == nil {
		return "sealed-and-no-key"
	}
	pt, err := enc.Decrypt(rec)
	if err != nil {
		// Deliberately one token and not the error text: a record that will not open is
		// one fact, and the trace is compared byte for byte across runs (INV-02).
		return "unopenable"
	}
	return carryDigestOf(pt)
}

func carryDigestOf(plaintext []byte) string {
	sum := sha256.Sum256(plaintext)
	return hex.EncodeToString(sum[:8])
}

// carryDSTPayload is the byte a record carries, so a read-back says which record answered
// it rather than only that something did.
func carryDSTPayload(i int) []byte { return bytes.Repeat([]byte{byte(0xA0 + i)}, carryPayloadBytes) }

// carryDSTOffset scatters the records so a wrong record answering a read is visible.
func carryDSTOffset(i int) uint64 { return uint64(i) * 64 << 10 }

// carryVolumeID is a v7-shaped id per arm. Each crash point gets its own volume, which
// is what makes "the same sequence twice under one (volume, epoch)" a statement about
// one directory at one moment.
func carryVolumeID(arm uint64) [16]byte {
	var v [16]byte
	v[0], v[1] = 0xCA, 0x44
	v[2], v[3] = byte(arm>>8), byte(arm)
	v[6], v[8] = 0x70, 0x80
	return v
}
