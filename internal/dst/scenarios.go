package dst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/simio/network"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
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

// MandatoryScenarios is the set the `task dst` gate runs.
func MandatoryScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "lost-put-idempotent-retry", Run: scenarioLostPutIdempotent},
		{Name: "crash-around-fdatasync", Run: scenarioCrashAroundFdatasync},
		{Name: "clock-drift-beyond-skew", Run: scenarioClockDriftBeyondSkew},
		{Name: "network-partition", Run: scenarioNetworkPartition},
		{Name: "wal-write-path-no-put", Run: scenarioWALWritePathNoPut},
		{Name: "wal-backpressure", Run: scenarioWALBackpressure},
		{Name: "encrypted-wal-no-plaintext-leak", Run: scenarioEncryptedWALNoPlaintextLeak},
		{Name: "remote-flush-ordering", Run: scenarioRemoteFlushOrdering},
		{Name: "idempotent-batch-upload", Run: scenarioIdempotentBatchUpload},
	}
}

func remoteLog(s *Sim, vol [16]byte) (*wal.Log, error) {
	f, err := s.Disk.Create("wal/active.wal")
	if err != nil {
		return nil, err
	}
	l := wal.NewLog(f, s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(
		wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(s.Store, 5),
	)
	return l, nil
}

// scenarioRemoteFlushOrdering is INV-07 (§14.4): durable_sequence advances only
// after the covering objects are verified in S3. A failing upload must leave
// durable where it was.
func scenarioRemoteFlushOrdering(s *Sim) error {
	ctx := context.Background()
	l, err := remoteLog(s, [16]byte{5})
	if err != nil {
		return err
	}
	if _, err := l.Write(0, []byte("durable-me"), 0); err != nil {
		return err
	}

	s.Store.InjectThrottle(5) // exhaust the uploader budget, then clear
	if err := l.Flush(ctx); err == nil {
		return errors.New("flush should fail while uploads fail")
	}
	emitWatermarks(s, l)
	if l.Watermarks().Durable != 0 {
		return fmt.Errorf("durable advanced to %d despite upload failure (INV-07)", l.Watermarks().Durable)
	}
	if objs, _ := s.Store.List(ctx, "wal/"); len(objs) != 0 {
		return fmt.Errorf("no object should be durable, got %d", len(objs))
	}

	// Retry succeeds; durable now advances.
	if err := l.Flush(ctx); err != nil {
		return fmt.Errorf("retry flush: %w", err)
	}
	emitWatermarks(s, l)
	if l.Watermarks().Durable != 1 {
		return fmt.Errorf("durable should be 1 after verified upload, got %d", l.Watermarks().Durable)
	}
	s.Emit(Event{Kind: EventObject, Msg: "batch verified in S3"})
	return nil
}

// scenarioIdempotentBatchUpload is INV-21 (§14.5): a PUT that persisted but lost
// its response reconciles on retry, and re-uploads never duplicate.
func scenarioIdempotentBatchUpload(s *Sim) error {
	ctx := context.Background()
	vol := [16]byte{6}
	b := wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig())
	enc, _ := wal.Record{Type: format.RecordWrite, Epoch: 1, Sequence: 1, Payload: []byte("batch-bytes")}.Encode()
	b.Append(1, enc, false)
	b.Flush()
	cb := b.Pending()[0]
	key, _, _ := cb.Object()

	s.Store.InjectLostResponse(key)
	up := wal.NewUploader(s.Store, 5)
	if err := up.Upload(ctx, cb); err != nil {
		return fmt.Errorf("upload should be idempotent after lost response: %w", err)
	}
	if err := up.Upload(ctx, cb); err != nil {
		return fmt.Errorf("re-upload should be idempotent: %w", err)
	}
	objs, _ := s.Store.List(ctx, "wal/")
	if len(objs) != 1 {
		return fmt.Errorf("expected exactly 1 object after idempotent retries, got %d", len(objs))
	}
	s.Notef("idempotent upload: 1 object after lost response + re-upload")
	return nil
}

// scenarioEncryptedWALNoPlaintextLeak: with per-volume encryption, the bytes that
// will leave the host (the WAL file → later S3) contain no cleartext (§5.10,
// INV-15), yet replay+decrypt recovers the plaintext.
func scenarioEncryptedWALNoPlaintextLeak(s *Sim) error {
	dek, err := crypto.GenerateDEK(&deterministicReader{b: byte(s.Rand.Intn(200) + 1)}, 1)
	if err != nil {
		return err
	}
	enc := &wal.Encryption{DEK: dek, VolumeID: [16]byte{7}}

	f, err := s.Disk.Create("wal/active.wal")
	if err != nil {
		return err
	}
	l := wal.NewLog(f, s.Clock, enc.VolumeID, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableEncryption(enc)

	canary := []byte("CLEARTEXT-CANARY-DO-NOT-LEAK")
	if _, err := l.Write(0, canary, 0); err != nil {
		return err
	}

	// Inspect the bytes bound to leave the host.
	sz, _ := f.Size()
	raw := make([]byte, sz)
	_, _ = f.ReadAt(raw, 0)
	leak := bytes.Contains(raw, canary)
	s.Emit(Event{Kind: EventLeavesHost, ClearLeak: leak, Msg: "wal object bytes"})
	if leak {
		return errors.New("cleartext canary present in WAL bytes")
	}

	// Recovery still works.
	recs, err := wal.Replay(raw)
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
	f, err := s.Disk.Create("wal/active.wal")
	if err != nil {
		return err
	}
	l := wal.NewLog(f, s.Clock, [16]byte{}, 1, wal.Limits{MaxUnflushedBytes: 1 << 20, MaxUnflushedAge: 30 * time.Second})

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
		l.Read(off, buf)
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
	f, err := s.Disk.Create("wal/active.wal")
	if err != nil {
		return err
	}
	// Room for one record (104-byte header + small payload) but not two.
	l := wal.NewLog(f, s.Clock, [16]byte{}, 1, wal.Limits{MaxUnflushedBytes: 200})

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

// scenarioLostPutIdempotent: a PUT persists but its response is lost (§14.5). The
// idempotent retry sees the object already present (412), HEADs it, and the
// checksums match => success without duplication.
func scenarioLostPutIdempotent(s *Sim) error {
	ctx := context.Background()
	key := "wal/vol/0/1-1-hash.wal"
	data := []byte("the-encrypted-batch")

	s.Store.InjectLostResponse(key)
	s.Notef("PUT with lost response injected")
	_, err := s.Store.Put(ctx, key, data, objectstore.PutOptions{IfNoneMatch: true})
	if !errors.Is(err, sim.ErrLostResponse) {
		return fmt.Errorf("expected lost-response error, got %v", err)
	}
	s.Emit(Event{Kind: EventObject, Key: key, Msg: "put response lost"})

	// Retry: create-only now fails because it persisted.
	_, err = s.Store.Put(ctx, key, data, objectstore.PutOptions{IfNoneMatch: true})
	if !errors.Is(err, objectstore.ErrPreconditionFailed) {
		return fmt.Errorf("retry expected precondition-failed, got %v", err)
	}
	// HEAD + checksum reconcile: same size and readable content => idempotent OK.
	info, err := s.Store.Head(ctx, key)
	if err != nil {
		return fmt.Errorf("HEAD after retry: %w", err)
	}
	if info.Size != int64(len(data)) {
		return fmt.Errorf("HEAD size mismatch: got %d want %d", info.Size, len(data))
	}
	got, err := s.Store.Get(ctx, key)
	if err != nil || !bytes.Equal(got, data) {
		return fmt.Errorf("content mismatch after lost-response retry: %q err=%v", got, err)
	}
	s.Emit(Event{Kind: EventObject, Key: key, Msg: "idempotent retry reconciled"})
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
