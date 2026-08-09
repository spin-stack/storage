package dst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/simio/network"
	"github.com/spin-stack/storage/internal/wal"
)

// deterministicReader yields seed-derived bytes for DEK material under DST.
type deterministicReader struct{ b byte }

func (r *deterministicReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.b
		r.b++
	}
	return len(p), nil
}

// MandatoryScenario is one entry in the §25.1 must-be-green-on-every-PR set. The
// data-path arms (real WAL records, real fencing) are added by later phases; here
// each scenario exercises the interface + fault machinery deterministically.
type MandatoryScenario struct {
	Name string
	Run  Scenario
}

// MandatoryScenarios is the set the `task dst` gate runs. It is assembled from the
// per-area lists (core here, then drain, recovery and harness) so two increments can
// add scenarios without both editing one literal.
func MandatoryScenarios() []MandatoryScenario {
	all := coreScenarios()
	all = append(all, harnessScenarios()...)
	all = append(all, walScenarios()...)
	all = append(all, carryScenarios()...)
	all = append(all, agentScenarios()...)
	all = append(all, refusalScenarios()...)
	return all
}

// coreScenarios are the §25.1 entries that predate the split by area.
func coreScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "crash-around-fdatasync", Run: scenarioCrashAroundFdatasync},
		{Name: "clock-drift-beyond-skew", Run: scenarioClockDriftBeyondSkew},
		{Name: "network-partition", Run: scenarioNetworkPartition},
		{Name: "wal-write-path-no-put", Run: scenarioWALWritePathNoPut},
		{Name: "wal-backpressure", Run: scenarioWALBackpressure},
		{Name: "encrypted-wal-no-plaintext-leak", Run: scenarioEncryptedWALNoPlaintextLeak},
		{Name: "torn-append-leaves-nothing-behind", Run: scenarioTornAppend},
	}
}

// scenarioTornAppend is the disk half of §25.1's "crash around append/fdatasync": a
// partial append (ENOSPC, a torn write) must leave the log holding exactly the
// records that were accepted. The existing crash scenario exercises the simulated
// disk's sync semantics on raw bytes; this one drives the WAL itself, which is where
// a rejected-but-persisted record turns into a phantom write or a silently truncated
// replay.
func scenarioTornAppend(s *Sim) error {
	var vol [16]byte
	vol[6], vol[8] = 0x70, 0x80
	l := wal.NewLog(s.Disk, "wal", s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})

	if _, err := l.Write(0, []byte("accepted"), 0); err != nil {
		return err
	}
	accepted := l.Watermarks().Local

	// The disk accepts a seed-dependent slice of the next record and then fails.
	limit := s.Rand.Intn(140)
	s.Disk.InjectShortAppend("wal", limit) // the WAL directory: any segment will do
	if _, err := l.Write(4096, []byte("rejected"), 0); err == nil {
		return fmt.Errorf("a short append of %d bytes was not reported to the caller", limit)
	}
	s.Emit(Event{Kind: EventFault, Msg: fmt.Sprintf("short append accepted %d bytes", limit)})

	if got := l.Watermarks().Local; got != accepted {
		return fmt.Errorf("local watermark moved to %d on a rejected write (was %d)", got, accepted)
	}
	if _, err := l.Write(8192, []byte("accepted-again"), 0); err != nil {
		return err
	}
	if err := l.Sync(); err != nil {
		return err
	}

	recs, err := wal.ReplaySegments(s.Disk, "wal", vol, 1)
	if err != nil {
		return fmt.Errorf("replay after a repaired tear: %w", err)
	}
	if len(recs) != 2 {
		return fmt.Errorf("replay returned %d records, want the 2 accepted ones", len(recs))
	}
	seen := map[uint64]bool{}
	for _, r := range recs {
		if seen[r.Sequence] {
			return fmt.Errorf("sequence %d replayed twice", r.Sequence)
		}
		seen[r.Sequence] = true
	}
	s.Emit(Event{Kind: EventWatermark, Local: l.Watermarks().Local, Durable: 0, Published: 0})
	s.Notef("torn append repaired: log holds exactly the accepted records")
	return nil
}

// Whether the background consumer is handed the scheduler the data path is using.
// unscheduledMaterializer is how INV-17 is actually lost in practice: not by the
// arbitration deciding wrongly — ioclass.Scheduler is a few lines of counter — but by
// a consumer that was never wired to it, at which point the class system is advisory
// and the first hint is a latency graph. materialize.New takes the scheduler as a
// nil-able argument, so the mistake is one omitted parameter and no error anywhere.
const ()

// failVol/failHosts are the v7-shaped ids for the full-fencing scenario.
const ()

// promoVol/promoHosts are v7-shaped ids used by the promotion scenario.
const ()

// Which lease checker the WAL is handed. honestLeaseChecker is the real manager;
// Whether the volume under test was created with a DEK. plaintextWAL is not a bug in
// the crypto — it is a Log that was never handed one, which is exactly how a plaintext
// volume reaches production, and it is what proves the INV-15 checker catches a real
// leak instead of a fabricated event.
const (
	encryptedWAL = true
	plaintextWAL = false
)

// scenarioEncryptedWALNoPlaintextLeak: with per-volume encryption, the bytes that
// will leave the host (the WAL file → later S3) contain no cleartext (§5.10,
// INV-15), yet replay+decrypt recovers the plaintext.
func scenarioEncryptedWALNoPlaintextLeak(s *Sim) error {
	return walPlaintextScenario(s, encryptedWAL)
}

func walPlaintextScenario(s *Sim, encrypted bool) error {
	dek, err := crypto.GenerateDEK(&deterministicReader{b: byte(s.Rand.Intn(200) + 1)}, 1)
	if err != nil {
		return err
	}
	enc := &wal.Encryption{DEK: dek, VolumeID: [16]byte{7}}

	l := wal.NewLog(s.Disk, "wal", s.Clock, enc.VolumeID, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	if encrypted {
		l.EnableEncryption(enc)
	}

	canary := []byte("CLEARTEXT-CANARY-DO-NOT-LEAK")
	if _, err := l.Write(0, canary, 0); err != nil {
		return err
	}

	// Inspect the bytes bound to leave the host.
	raw, err := walRawBytes(s, enc.VolumeID, 1)
	if err != nil {
		return err
	}
	leak := bytes.Contains(raw, canary)
	s.Emit(Event{Kind: EventLeavesHost, ClearLeak: leak, Msg: "wal object bytes"})
	if leak {
		return errors.New("cleartext canary present in WAL bytes")
	}

	// Recovery still works.
	recs, err := wal.ReplaySegments(s.Disk, "wal", enc.VolumeID, 1)
	if err != nil {
		return fmt.Errorf("replay: %w", err)
	}
	if len(recs) != 1 {
		return fmt.Errorf("expected 1 record, got %d", len(recs))
	}
	pt, err := enc.Decrypt(recs[0])
	if err != nil {
		return fmt.Errorf("decrypt: %w", err)
	}
	if !bytes.Equal(pt, canary) {
		return fmt.Errorf("decrypted plaintext mismatch: %q", pt)
	}
	s.Notef("encrypted WAL: 0 cleartext leak, replay+decrypt OK")
	return nil
}

// emitWatermarks records the log's current watermarks for the ordering checker.
func emitWatermarks(s *Sim, l *wal.Log) {
	w := l.Watermarks()
	s.Emit(Event{Kind: EventWatermark, Local: w.Local, Durable: w.Durable, Published: w.Published})
}

// scenarioWALWritePathNoPut: normal WRITEs go to the local WAL and are readable
// back, they issue no object-store PUT (§5.3, INV-18), and the watermarks stay
// ordered (§5.6, INV-03).
func scenarioWALWritePathNoPut(s *Sim) error {
	l := wal.NewLog(s.Disk, "wal", s.Clock, [16]byte{}, 1, wal.Limits{MaxUnflushedBytes: 1 << 20, MaxUnflushedAge: 30 * time.Second})

	n := 3 + s.Rand.Intn(6)
	written := map[uint64][]byte{}
	for i := 0; i < n; i++ {
		off := uint64(s.Rand.Intn(16)) * 8
		payload := []byte(fmt.Sprintf("rec%02d", i))
		if _, err := l.Write(off, payload, 0); err != nil {
			return fmt.Errorf("write %d: %w", i, err)
		}
		written[off] = payload
		emitWatermarks(s, l)
	}

	// Read-back matches the last write at each offset.
	for off, want := range written {
		buf := make([]byte, len(want))
		if err := l.Read(off, buf); err != nil {
			return err
		}
		if !bytes.Equal(buf, want) {
			return fmt.Errorf("read-back at %d: got %q want %q", off, buf, want)
		}
	}

	// INV-18: no PUT happened on the write path.
	objs, err := s.Store.List(context.Background(), "")
	if err != nil {
		return err
	}
	if len(objs) != 0 {
		return fmt.Errorf("write path issued %d PUTs; a normal WRITE must not PUT (§5.3)", len(objs))
	}
	s.Notef("wrote %d records, 0 PUTs", n)
	return nil
}

// scenarioWALBackpressure: exceeding the unflushed byte limit yields an explicit
// error (§5.7, INV-04), and after Sync writes resume.
func scenarioWALBackpressure(s *Sim) error {
	// Room for one record (104-byte header + small payload) but not two.
	l := wal.NewLog(s.Disk, "wal", s.Clock, [16]byte{}, 1, wal.Limits{MaxUnflushedBytes: 200})

	if _, err := l.Write(0, make([]byte, 32), 0); err != nil {
		return fmt.Errorf("first write should fit: %w", err)
	}
	if _, err := l.Write(64, make([]byte, 64), 0); !errors.Is(err, wal.ErrBackpressure) {
		return fmt.Errorf("expected backpressure, got %v", err)
	}
	s.Emit(Event{Kind: EventFault, Msg: "backpressure asserted"})

	if err := l.Sync(); err != nil {
		return err
	}
	if _, err := l.Write(64, make([]byte, 64), 0); err != nil {
		return fmt.Errorf("write after sync should resume: %w", err)
	}
	emitWatermarks(s, l)
	return nil
}

// scenarioCrashAroundFdatasync: write to the WAL then crash at a seed-chosen point
// relative to the sync. After the crash exactly the durable prefix must remain.
func scenarioCrashAroundFdatasync(s *Sim) error {
	f, err := s.Disk.Create("wal/active.wal")
	if err != nil {
		return err
	}
	durable := []byte("committed-record")
	if _, err := f.Append(durable); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	s.Emit(Event{Kind: EventDisk, Msg: "appended+synced committed-record"})

	// A second record whose fate depends on whether we sync before crashing.
	uncommitted := []byte("-in-flight")
	if _, err := f.Append(uncommitted); err != nil {
		return err
	}

	syncBeforeCrash := s.Rand.Intn(2) == 0
	if syncBeforeCrash {
		if err := f.Sync(); err != nil {
			return err
		}
		s.Emit(Event{Kind: EventFault, Msg: "sync then crash"})
	} else {
		s.Emit(Event{Kind: EventFault, Msg: "crash before sync"})
	}
	s.Disk.Crash()
	s.Emit(Event{Kind: EventRecovery, Msg: "recovering after crash"})

	// Determine what must survive.
	want := durable
	if syncBeforeCrash {
		want = append(append([]byte(nil), durable...), uncommitted...)
	}
	size, err := f.Size()
	if err != nil {
		return err
	}
	if size != int64(len(want)) {
		return fmt.Errorf("post-crash size %d, want %d (syncBeforeCrash=%t)", size, len(want), syncBeforeCrash)
	}
	got := make([]byte, size)
	if _, err := f.ReadAt(got, 0); err != nil && size > 0 {
		// io.EOF at exact end is acceptable; only fail on short read handled by size check above.
		if !bytes.Equal(got, want) {
			return fmt.Errorf("post-crash content mismatch: %q want %q", got, want)
		}
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("post-crash content mismatch: %q want %q", got, want)
	}
	return nil
}

// scenarioClockDriftBeyondSkew: inject wall skew far beyond max_clock_skew (2 s).
// Monotonic time — the basis of writer safety (§12.1) — must be unaffected; only
// wall time moves. The MonotonicClockChecker corroborates across the run.
func scenarioClockDriftBeyondSkew(s *Sim) error {
	before := s.Clock.Now()
	wallBefore := s.Clock.Wall()

	drift := time.Duration(3+s.Rand.Intn(30)) * time.Second // always > 2 s skew bound
	s.Clock.SetSkew(drift)
	s.Emit(Event{Kind: EventFault, Msg: fmt.Sprintf("inject wall skew=%s", drift)})

	if got := s.Clock.Now(); got != before {
		return fmt.Errorf("skew perturbed monotonic time: %d -> %d", before, got)
	}
	// Advance and confirm monotonic still moves normally.
	s.Tick(time.Second)
	if s.Clock.Now() <= before {
		return fmt.Errorf("monotonic did not advance after Tick")
	}
	// Wall reflects the injected skew.
	wallAfter := s.Clock.Wall()
	if wallAfter.Sub(wallBefore) < drift {
		return fmt.Errorf("wall did not reflect injected skew: delta=%s drift=%s", wallAfter.Sub(wallBefore), drift)
	}
	return nil
}

// scenarioNetworkPartition: a Control-Plane/Agent link is partitioned; sends fail
// while partitioned and resume after heal (the §12/§23 fencing precondition).
func scenarioNetworkPartition(s *Sim) error {
	ctx := context.Background()
	addr := "agent:1"
	l, err := s.Net.Listen(addr)
	if err != nil {
		return err
	}
	defer l.Close()

	accepted := make(chan network.Conn, 1)
	accErr := make(chan error, 1)
	go func() {
		c, err := l.Accept(ctx)
		if err != nil {
			accErr <- err
			return
		}
		accepted <- c
	}()

	client, err := s.Net.Dial(ctx, addr)
	if err != nil {
		return err
	}
	defer client.Close()

	select {
	case <-accepted:
	case err := <-accErr:
		return fmt.Errorf("accept: %w", err)
	}

	s.Net.Partition(addr)
	s.Emit(Event{Kind: EventFault, Msg: "partition agent:1"})
	if err := client.Send(ctx, []byte("heartbeat")); !errors.Is(err, network.ErrPartitioned) {
		return fmt.Errorf("send during partition: want ErrPartitioned, got %v", err)
	}

	s.Net.Heal(addr)
	s.Emit(Event{Kind: EventNetwork, Msg: "heal agent:1"})
	if err := client.Send(ctx, []byte("heartbeat")); err != nil {
		return fmt.Errorf("send after heal: %w", err)
	}
	return nil
}
