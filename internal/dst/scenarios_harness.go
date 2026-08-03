package dst

import (
	"context"
	"errors"
	"fmt"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// Harness-level scenarios: fault injection driven by the seed, resource exhaustion,
// and anything whose subject is the simulation itself rather than one subsystem.
// See scenarios_drain.go for why the list is split by area.

func harnessScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "disk-fills-under-sustained-write-with-s3-down", Run: scenarioDiskFillsWithS3Down},
		{Name: "restored-control-plane", Run: scenarioRestoredControlPlane},
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
// Both arms also pin the state the log reports about its device (`Degraded()`), which
// is what tells an operator which remedy applies. It is orthogonal to `Fenced()` on
// purpose and the scenario asserts that too: a full disk is local and recoverable, and
// handing the volume to another host over it would turn it into a failover.
// Arm 1 — "backpressure arrives before the device fills" — went with the remote gap bound
// (ADR-0026 increment 4.5). What is left is the arm that never depended on S3: a device
// that fills must leave the WAL replayable, not corrupt.
func scenarioDiskFillsWithS3Down(s *Sim) error {
	return enospcTailReplaysClean(s)
}

// enospcTailReplaysClean is arm 2: with no gap bound the device fills, and the WAL
// must survive it — no phantom record, sticky failure, clean replay, and correct
// numbering once space is reclaimed.
func enospcTailReplaysClean(s *Sim) error {
	const (
		device         = "wal-no-bound"
		unflushedBound = 1500 // ~4 records: reached long before the device fills
		capacity       = 1040 // a 64-byte segment header and three 304-byte records fit; the fourth does not
	)
	s.Disk.InjectENOSPC(device, capacity)

	var vol [16]byte
	vol[6], vol[8] = 0x70, 0x80
	vol[15] = 0xf2
	l := wal.NewLog(s.Disk, device, s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: unflushedBound})

	accepted := 0
	var full error
	for i := range 20 {
		_, err := l.Write(uint64(i)*4096, make([]byte, enospcRecord), 0)
		if err == nil {
			accepted++
			if d := l.Degraded(); d != wal.DegradedNone {
				return fmt.Errorf("the log reports %s after an accepted WRITE", d)
			}
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

	// The condition is named, not just returned: "no space" and "the backend hiccuped"
	// have different remedies and the caller has to be able to tell them apart.
	if d := l.Degraded(); d != wal.DegradedOutOfSpace {
		return fmt.Errorf("after ENOSPC the log reports %s, want %s", d, wal.DegradedOutOfSpace)
	}

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
		if d := l.Degraded(); d != wal.DegradedOutOfSpace {
			return fmt.Errorf("the device state cleared to %s while the device was still full", d)
		}
	}
	if l.Broken() {
		return errors.New("the log self-fenced on ENOSPC; only a lease failure may do that (§16)")
	}

	// The WAL replays to exactly the accepted records: the partial append was rolled
	// back, so replay neither resurrects a rejected record nor stops at the tear.
	if err := l.Sync(); err != nil {
		return err
	}
	recs, err := wal.ReplaySegments(s.Disk, device, vol, 1)
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
	// Only an append the device actually took clears the state — nothing probes it.
	if d := l.Degraded(); d != wal.DegradedNone {
		return fmt.Errorf("the log still reports %s after a WRITE the device accepted", d)
	}
	s.Notef("device filled after %d records: backpressure first, no phantom record, clean replay, state %s->%s->%s",
		accepted, wal.DegradedNone, wal.DegradedOutOfSpace, l.Degraded())
	return nil
}

// rewoundLeadership is a metadata store whose leadership row was restored from a
// backup: elections resume from an earlier term, so the same term is handed out
// twice. Nothing inside the database can tell — the term is derived from the row.
type rewoundLeadership struct {
	metadata.Store
	replay []int64
	n      int
}

func (s *rewoundLeadership) AcquireLeadership(ctx context.Context, holderID string) (int64, error) {
	if s.n < len(s.replay) {
		t := s.replay[s.n]
		s.n++
		return t, nil
	}
	return s.Store.AcquireLeadership(ctx, holderID)
}

// scenarioRestoredControlPlane is ADR-0011 under the incident it was written for: the
// Control Plane database is restored to a point before the running leader's term, so
// the next election offers a term that is still in use. Every §7 mutation is guarded
// by that number, so two processes would pass every guard at once — not a zombie and a
// leader, but two leaders, each reading the other's writes as its own resumed work.
//
// The scenario drives the real elector against the real simulated object store and
// asserts both halves: that the rewound database really does offer the live term back
// (otherwise the rest proves nothing), and that the elector refuses to return it,
// climbing past every term the bucket has ever witnessed.
func scenarioRestoredControlPlane(s *Sim) error {
	ctx := context.Background()
	md := metasim.New(s.Clock.Wall)
	elector := controlplane.NewElector(md, s.Store)

	first, err := elector.Acquire(ctx, "cp-a")
	if err != nil {
		return err
	}
	host := "00000000-0000-7000-8000-0000000000a1"
	if err := md.UpsertHost(ctx, first, metadata.Host{
		HostID: host, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
	}); err != nil {
		return err
	}
	s.Emit(Event{Kind: EventNote, Msg: fmt.Sprintf("leader cp-a at term %d", first)})

	// The restore. The row is back before cp-a's term while cp-a is still running.
	restored := &rewoundLeadership{Store: md, replay: []int64{first}}
	s.Emit(Event{Kind: EventFault, Msg: "control-plane database restored to an earlier term"})

	// The fault is real: read the database alone and it offers the live term back.
	offered, err := restored.AcquireLeadership(ctx, "cp-b")
	if err != nil {
		return err
	}
	if offered != first {
		return fmt.Errorf("the rewind did not reproduce: election offered %d, not the live %d", offered, first)
	}

	// The elector is what stops it: the claim for that term is already in the bucket.
	second, err := controlplane.NewElector(&rewoundLeadership{Store: md, replay: []int64{first}}, s.Store).
		Acquire(ctx, "cp-b")
	if err != nil {
		return err
	}
	if second <= first {
		return fmt.Errorf("a restored database re-issued term %d while cp-a still holds %d (violates §7)", second, first)
	}
	s.Emit(Event{Kind: EventNote, Msg: fmt.Sprintf("leader cp-b climbed to term %d", second)})

	// And the §7 guard is doing its job again: cp-a is now the zombie.
	err = md.UpsertHost(ctx, first, metadata.Host{
		HostID: host, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40,
	})
	if !errors.Is(err, metadata.ErrStaleTerm) {
		return fmt.Errorf("the superseded leader's mutation returned %v, want ErrStaleTerm", err)
	}
	return nil
}
