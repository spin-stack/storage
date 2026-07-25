package dst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spin-stack/storage/internal/simio/network"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

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
	}
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
