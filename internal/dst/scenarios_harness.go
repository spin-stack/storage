package dst

import (
	"context"
	"errors"
	"fmt"

	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// Harness-level scenarios: fault injection driven by the seed, resource exhaustion,
// and anything whose subject is the simulation itself rather than one subsystem.
// See scenarios_drain.go for why the list is split by area.

func harnessScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "disk-fills-under-sustained-write-with-s3-down", Run: scenarioDiskFillsWithS3Down},
	}
}

func harnessCheckers() []Checker { return nil }

// enospcRecord is the payload size the out-of-space scenario writes; the encoded
// record is format.RecordHeaderSize (104) larger.
const enospcRecord = 200

// scenarioDiskFillsWithS3Down is the §5.7 backlog bound under the operational case it
// exists for: S3 has been unreachable long enough that no object closed the remote
// gap and no checkpoint authorised a local truncation, so the WAL device runs out of
// space. Two arms:
//
//  1. With MaxRemoteGapBytes configured below the device, the WAL must refuse WRITEs
//     with ErrBackpressure *before* the device fills. Backpressure is an error the
//     guest understands; ENOSPC arriving as a partial append is not.
//  2. With that bound disabled — a `local` volume, or an operator who never set it —
//     the device does fill. The first ENOSPC is a partial append, and what matters is
//     what it leaves behind: no phantom sequence, every later WRITE failing the same
//     way rather than silently succeeding, and a WAL that still replays to exactly the
//     records the guest was told were accepted.
//
// Known hole (reported, not asserted here): after arm 2 the log is not `Fenced()` and
// exposes no "out of space" state at all — `wal` has no ENOSPC policy, so a caller
// cannot distinguish a full device from a transient I/O error. Asserting a degraded
// state would need an API `internal/wal` does not have; this scenario pins down
// everything that *is* observable so the missing piece is the only gap left.
func scenarioDiskFillsWithS3Down(s *Sim) error {
	if err := enospcBackpressureFirst(s); err != nil {
		return err
	}
	return enospcTailReplaysClean(s)
}

// enospcBackpressureFirst is arm 1: the remote-gap bound fires before the device does.
func enospcBackpressureFirst(s *Sim) error {
	const (
		device   = "wal/gap-bound.wal"
		capacity = 1 << 13 // 8 KiB of device
		gapBound = 1500    // ~4 records: reached long before the device is
	)
	f, err := s.Disk.Create(device)
	if err != nil {
		return err
	}
	s.Disk.InjectENOSPC(device, capacity)

	var vol [16]byte
	vol[6], vol[8] = 0x70, 0x80
	vol[15] = 0xf1
	l := wal.NewLog(f, s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20, MaxRemoteGapBytes: gapBound})
	l.EnableRemote(wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(s.Store, 2), alwaysValidLease{})

	// S3 is unreachable for the whole arm: nothing can close the gap.
	s.Store.InjectThrottle(1 << 20)
	s.Emit(Event{Kind: EventFault, Msg: "object store unreachable; remote gap cannot close"})

	accepted := 0
	for i := range 100 {
		_, err := l.Write(uint64(i)*4096, make([]byte, enospcRecord), 0)
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, wal.ErrBackpressure):
			s.Emit(Event{Kind: EventFault, Msg: fmt.Sprintf("backpressure after %d records", accepted)})
			// The bound did its job: the device still has room, so no WRITE ever saw
			// a raw ENOSPC.
			size, serr := f.Size()
			if serr != nil {
				return serr
			}
			if size >= capacity {
				return fmt.Errorf("backpressure arrived only once the device was full (%d/%d bytes)", size, capacity)
			}
			// A FLUSH cannot rescue it while S3 is down: durable must not move.
			if ferr := l.Flush(context.Background()); ferr == nil {
				return errors.New("a FLUSH ACKed while the object store was unreachable (INV-07)")
			}
			emitWatermarks(s, l)
			if w := l.Watermarks(); w.Durable != 0 {
				return fmt.Errorf("durable advanced to %d with nothing in S3", w.Durable)
			}
			return nil
		case errors.Is(err, sim.ErrNoSpace):
			return fmt.Errorf("the device filled at record %d before the WAL applied backpressure (§5.7)", i)
		default:
			return fmt.Errorf("write %d: %w", i, err)
		}
	}
	return errors.New("the remote-gap bound never fired: 100 records were accepted with S3 down")
}

// enospcTailReplaysClean is arm 2: with no gap bound the device fills, and the WAL
// must survive it — no phantom record, sticky failure, clean replay, and correct
// numbering once space is reclaimed.
func enospcTailReplaysClean(s *Sim) error {
	const (
		device   = "wal/no-bound.wal"
		capacity = 1000 // three 304-byte records fit, the fourth does not
	)
	f, err := s.Disk.Create(device)
	if err != nil {
		return err
	}
	s.Disk.InjectENOSPC(device, capacity)

	var vol [16]byte
	vol[6], vol[8] = 0x70, 0x80
	vol[15] = 0xf2
	l := wal.NewLog(f, s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})

	accepted := 0
	var full error
	for i := range 20 {
		_, err := l.Write(uint64(i)*4096, make([]byte, enospcRecord), 0)
		if err == nil {
			accepted++
			continue
		}
		full = err
		break
	}
	if full == nil {
		return errors.New("a 1000-byte device never returned ENOSPC")
	}
	if !errors.Is(full, sim.ErrNoSpace) {
		return fmt.Errorf("the first out-of-space WRITE reported %v, not ENOSPC", full)
	}
	if accepted == 0 {
		return errors.New("no record fit on the device at all; the arm proves nothing")
	}
	s.Emit(Event{Kind: EventFault, Msg: fmt.Sprintf("device full after %d records", accepted)})

	// The rejected WRITE left no phantom sequence behind.
	if got := l.Watermarks().Local; got != uint64(accepted) {
		return fmt.Errorf("local watermark = %d after a rejected WRITE, want %d", got, accepted)
	}

	// Sticky: the device does not recover on its own, so every later WRITE fails the
	// same way. A WRITE that silently succeeded here would be the guest being told
	// its data is safe on a device that cannot hold it.
	for i := range 3 {
		if _, err := l.Write(uint64(1<<20)+uint64(i), make([]byte, enospcRecord), 0); !errors.Is(err, sim.ErrNoSpace) {
			return fmt.Errorf("WRITE %d on a full device returned %v, want ENOSPC", i, err)
		}
	}
	if l.Fenced() {
		return errors.New("the log self-fenced on ENOSPC; only a lease failure may do that (§16)")
	}

	// The WAL replays to exactly the accepted records: the partial append was rolled
	// back, so replay neither resurrects a rejected record nor stops at the tear.
	if err := l.Sync(); err != nil {
		return err
	}
	size, err := f.Size()
	if err != nil {
		return err
	}
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, 0); err != nil {
		return err
	}
	recs, err := wal.Replay(buf)
	if err != nil {
		return fmt.Errorf("replay of a WAL whose tail was cut by ENOSPC: %w", err)
	}
	if len(recs) != accepted {
		return fmt.Errorf("replay returned %d records, want the %d accepted ones", len(recs), accepted)
	}
	for i, r := range recs {
		if r.Sequence != uint64(i+1) {
			return fmt.Errorf("replayed sequence %d at position %d: the ENOSPC tail broke contiguity", r.Sequence, i)
		}
	}
	emitWatermarks(s, l)

	// Space comes back (a checkpoint authorised a truncation, or an operator grew the
	// device). Numbering continues from the accepted prefix — the rejected WRITE's
	// sequence is not re-used against different content.
	s.Disk.ClearENOSPC(device)
	seq, err := l.Write(1<<21, make([]byte, enospcRecord), 0)
	if err != nil {
		return fmt.Errorf("WRITE after space was reclaimed: %w", err)
	}
	if seq != uint64(accepted+1) {
		return fmt.Errorf("the WRITE after ENOSPC took sequence %d, want %d", seq, accepted+1)
	}
	s.Notef("device filled after %d records: backpressure first, no phantom record, clean replay", accepted)
	return nil
}
