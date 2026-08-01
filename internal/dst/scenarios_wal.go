package dst

// Local-WAL segmentation scenarios (§14.7, §21.1, ADR-0013 §4) — see scenarios_drain.go
// for why the list is split by area.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spin-stack/storage/internal/blockdev"
	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/vhost"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

func walScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "wal-segments-survive-a-crash-at-every-boundary", Run: walSegmentCrashBoundaries(false)},
		{Name: "guest-device-acks-durability-only-on-flush", Run: scenarioGuestDeviceDurability},
	}
}

func walCheckers() []Checker { return nil }

// scenarioGuestDeviceDurability drives the guest-facing seam — blockdev.Device behind
// vhost.Backend — rather than wal.Log directly, and asserts the three rules that seam
// is responsible for:
//
//   - INV-18: a WRITE completes on local persistence and issues no PUT. Everything
//     above it in the tree tests this against wal.Log; here it is tested against the
//     object a guest's virtqueue actually reaches, which is where a helpful "sync on
//     every write" would be added.
//   - Read-your-writes without a FLUSH: the device serves reads from the WAL's read
//     view, so a guest never sees a hole where it just wrote.
//   - INV-06/INV-07: FLUSH is the ACK path. With a valid lease it ACKs only after the
//     covering objects are verified in S3; with an expired one it fails, durable does
//     not move, and the guest is told so.
//
// The lease here is a real lease.Manager on the simulated monotonic clock, not
// alwaysValidLease: the scenario's second half is precisely the case where it stops
// being valid.
func scenarioGuestDeviceDurability(s *Sim) error {
	ctx := context.Background()
	var vol [16]byte
	vol[6], vol[8] = 0x70, 0x80
	vol[15] = 0xbd
	const (
		root     = "wal-guest-device"
		capacity = 1 << 20
		sector   = 512
		ttl      = 10 * time.Second
	)

	lm := lease.NewManager(s.Clock, ttl)
	lm.Grant()
	l := wal.NewLog(s.Disk, root, s.Clock, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableRemote(wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(s.Store, 5), lm)
	dev, err := blockdev.New(l, capacity)
	if err != nil {
		return err
	}
	// The scenario talks to the interface, not to the implementation: what is under
	// test is what QEMU's virtqueue can reach.
	var guest vhost.Backend = dev

	// (1) The write path. Sector count and contents are seed-driven; that no PUT
	// happens is not.
	n := 3 + s.Rand.Intn(6)
	written := map[int64][]byte{}
	for i := range n {
		off := int64(s.Rand.Intn(64)) * sector
		payload := make([]byte, sector)
		for j := range payload {
			payload[j] = byte(i + 1)
		}
		if _, err := guest.WriteAt(payload, off); err != nil {
			return fmt.Errorf("guest WRITE %d at %d: %w", i, off, err)
		}
		written[off] = payload
		emitWatermarks(s, l)
	}
	objs, err := s.Store.List(ctx, "")
	if err != nil {
		return err
	}
	if len(objs) != 0 {
		return fmt.Errorf("the guest write path issued %d PUT(s); a normal WRITE must not PUT (§5.3, INV-18)", len(objs))
	}
	if w := l.Watermarks(); w.Durable != 0 {
		return fmt.Errorf("a plain WRITE advanced durable to %d", w.Durable)
	}

	// (2) Read-your-writes, with nothing flushed.
	for off, want := range written {
		got := make([]byte, len(want))
		if _, err := guest.ReadAt(got, off); err != nil {
			return fmt.Errorf("guest READ at %d: %w", off, err)
		}
		if !bytes.Equal(got, want) {
			return fmt.Errorf("read-your-writes at %d returned %x…, want %x…", off, got[:4], want[:4])
		}
	}
	s.Notef("guest wrote %d sectors through the WAL, 0 PUTs, read-your-writes intact", n)

	// (3) FLUSH with a valid lease: the ACK path runs and durable catches up to what
	// S3 now holds.
	if err := guest.Flush(ctx); err != nil {
		return fmt.Errorf("FLUSH under a valid lease: %w", err)
	}
	emitWatermarks(s, l)
	w := l.Watermarks()
	if w.Durable != w.Local {
		return fmt.Errorf("after a successful FLUSH durable=%d, local=%d", w.Durable, w.Local)
	}
	objs, err = s.Store.List(ctx, "")
	if err != nil {
		return err
	}
	if len(objs) == 0 {
		return errors.New("FLUSH ACKed with nothing in the object store (INV-07)")
	}
	acked := w.Durable

	// (4) The lease lapses. The next guest WRITE still lands locally — fencing is
	// about ACKs, not about the local append — but the FLUSH that would ACK it must
	// fail, and durable must stay exactly where the last valid ACK left it.
	if _, err := guest.WriteAt(make([]byte, sector), 0); err != nil {
		return fmt.Errorf("WRITE before the lease lapsed: %w", err)
	}
	s.Tick(ttl + time.Second)
	s.Emit(Event{Kind: EventFault, Msg: "the lease lapsed on the monotonic clock"})

	err = guest.Flush(ctx)
	if err == nil {
		return errors.New("FLUSH ACKed after the lease lapsed (INV-06)")
	}
	if !errors.Is(err, wal.ErrSelfFenced) {
		return fmt.Errorf("FLUSH after the lease lapsed: %w, want ErrSelfFenced", err)
	}
	if !l.Fenced() {
		return errors.New("the log did not self-fence")
	}
	emitWatermarks(s, l)
	if got := l.Watermarks().Durable; got != acked {
		return fmt.Errorf("durable moved from %d to %d on a FLUSH that was never ACKed", acked, got)
	}
	// Fencing is not transient: every later FLUSH fails at once. A guest can act on
	// an error; it cannot act on a device that stops answering.
	if err := guest.Flush(ctx); !errors.Is(err, wal.ErrSelfFenced) {
		return fmt.Errorf("a second FLUSH on a fenced device: %w, want ErrSelfFenced", err)
	}
	s.Notef("lease lapsed: FLUSH refused, durable held at %d, device still answering", acked)
	return l.Close()
}

// reclaimAnything is StrictOrder with INV-13's truncation floor removed and the other
// two rules untouched — one rule missing, not a Log with no rules at all, so the
// checker's failure names the rule it depends on. It is the seam OrderPolicy exists
// for (see wal.OrderPolicy).
type reclaimAnything struct{ wal.StrictOrder }

func (reclaimAnything) AllowTruncate(uint64, wal.Watermarks) error { return nil }

// walRawBytes returns the on-disk bytes of every segment of a volume's WAL,
// concatenated: what a scenario inspecting "the bytes bound to leave the host" used to
// read from the single WAL file.
func walRawBytes(s *Sim, vol [16]byte, epoch uint64) ([]byte, error) {
	names, err := wal.SegmentFiles(s.Disk, "wal", vol, epoch)
	if err != nil {
		return nil, err
	}
	var out []byte
	for _, name := range names {
		f, err := s.Disk.Open(name)
		if err != nil {
			return nil, err
		}
		size, err := f.Size()
		if err != nil {
			return nil, errors.Join(err, f.Close())
		}
		buf := make([]byte, size)
		if size > 0 {
			if _, err := f.ReadAt(buf, 0); err != nil {
				return nil, errors.Join(err, f.Close())
			}
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
		out = append(out, buf...)
	}
	return out, nil
}

// walCrashBoundary names the point in the seal/publish/reclaim sequence a host dies at.
type walCrashBoundary int

const (
	// crashBeforeSeal: records are in the open segment, nothing has been sealed.
	crashBeforeSeal walCrashBoundary = iota
	// crashAfterSeal: the segment was sealed (which writes nothing) and no new one
	// exists yet.
	crashAfterSeal
	// crashAfterPublished: the published watermark advanced and the checkpoint is in
	// S3, but not one segment has been unlinked.
	crashAfterPublished
	// crashBetweenUnlinks: the oldest segment is gone and the next one is not — the
	// state the reclaim loop passes through.
	crashBetweenUnlinks
	// crashAfterUnlinks: reclamation completed.
	crashAfterUnlinks
	walCrashBoundaryCount
)

func (b walCrashBoundary) String() string {
	switch b {
	case crashBeforeSeal:
		return "before the seal"
	case crashAfterSeal:
		return "after the seal"
	case crashAfterPublished:
		return "after published advanced, before any unlink"
	case crashBetweenUnlinks:
		return "between two unlinks"
	case crashAfterUnlinks:
		return "after the unlinks"
	default:
		return fmt.Sprintf("walCrashBoundary(%d)", int(b))
	}
}

// scenarioWALSegmentCrashBoundaries is ADR-0013 §4's required scenario: a host dies at
// each boundary of the sequence that reclaims local WAL, and every one of those on-disk
// states must be one a resumed log can read.
//
// The sequence is seal → checkpoint publishes → published advances → segments are
// unlinked oldest-first, and its safety rests on two things being true of every
// intermediate state:
//
//   - INV-13, which the unlink order encodes. The published watermark moves *before*
//     any unlink, so a crash part-way through leaves segments a later truncation
//     removes. The reverse order would remove segments the published point does not
//     yet cover, and the records in them exist on this host alone.
//   - Replayability. Sealing writes nothing to the segment being sealed, so no crash
//     can catch a sealed file half-updated; unlinking runs oldest-first, so no crash
//     can leave a hole in the middle of the directory. A resume that hard-errors on a
//     state an ordinary crash produces is as bad as one that loses data, because the
//     volume does not come back.
//
// The scenario asserts both after every crash, plus the thing the guest actually
// notices: the read view. Every record the resumed log still holds must read back the
// bytes that were written at its offset.
// walSegmentCrashBoundaries returns the scenario. reclaimAboveThePoint plants the
// violation this whole ordering exists to prevent — reclamation that runs past the
// published point — and is false everywhere but the planted-bug proof.
func walSegmentCrashBoundaries(reclaimAboveThePoint bool) Scenario {
	return func(s *Sim) error {
		// Every boundary, every run. The seed perturbs where the published point lands
		// inside the segment that holds it, which is what decides how much the
		// following truncation may reclaim; which boundaries are exercised is not left
		// to it, because "crash at each boundary" is the property.
		for b := range walCrashBoundaryCount {
			if err := walCrashArm(s, b, reclaimAboveThePoint); err != nil {
				return err
			}
		}
		return nil
	}
}

// walCrashArm stages one crash boundary on its own WAL root, so the five arms do not
// inherit each other's directories.
func walCrashArm(s *Sim, boundary walCrashBoundary, reclaimAboveThePoint bool) error {
	ctx := context.Background()
	var vol [16]byte
	vol[6], vol[8] = 0x70, 0x80
	vol[15] = 0xa7
	root := fmt.Sprintf("wal-crash-%d", int(boundary))

	s.Emit(Event{Kind: EventFault, Msg: "host dies " + boundary.String()})

	const groups, perGroup = 5, 3
	limits := wal.Limits{MaxUnflushedBytes: 1 << 20}
	l := wal.NewLog(s.Disk, root, s.Clock, vol, 1, limits)
	l.EnableRemote(wal.NewBatcher(s.Clock, vol, 1, 0, wal.DefaultBatchConfig()),
		wal.NewUploader(s.Store, 5), alwaysValidLease{})

	// Five segments' worth of records, all of them durable in S3 so the published
	// point is free to land anywhere. Rotation is driven by Seal rather than by
	// SegmentBytes: the boundary under test is the seal, not the 32 MiB that usually
	// triggers it, and writing 160 MiB to reach it would test the same code slower.
	payload := func(seq int) []byte { return fmt.Appendf(nil, "record-%02d", seq) }
	for g := range groups {
		for j := range perGroup {
			seq := g*perGroup + j
			if _, err := l.Write(uint64(seq)*4096, payload(seq), 0); err != nil {
				return fmt.Errorf("write %d: %w", seq, err)
			}
		}
		if err := l.Flush(ctx); err != nil {
			return fmt.Errorf("flush after group %d: %w", g, err)
		}
		if g == groups-1 {
			break // the newest segment stays open: only the newest ever is
		}
		if boundary == crashBeforeSeal && g == groups-2 {
			break // die with the records in an unsealed segment
		}
		if err := l.Seal(); err != nil {
			return fmt.Errorf("seal after group %d: %w", g, err)
		}
	}
	emitWatermarks(s, l)

	total := uint64(groups * perGroup)
	durable := l.Watermarks().Durable
	if durable != total && boundary != crashBeforeSeal {
		return fmt.Errorf("only %d of %d records reached S3", durable, total)
	}

	// The published point lands inside the third segment, so reclamation can take the
	// first two and must keep the third whole. Where inside it is seed-driven.
	published := uint64(2*perGroup + 1 + s.Rand.Intn(perGroup))
	segmentsBefore := len(l.SegmentNames())

	if boundary > crashAfterSeal {
		if err := l.AdvancePublished(published); err != nil {
			return fmt.Errorf("advance published to %d: %w", published, err)
		}
		emitWatermarks(s, l)
	}
	switch boundary {
	case crashBetweenUnlinks:
		// The reclaim loop unlinks oldest-first; this is the state it passes through
		// after the first unlink and before the second. Staging it directly is the
		// only way to stop inside a loop that has no yield point.
		names := l.SegmentNames()
		if len(names) < 3 {
			return fmt.Errorf("staged %d segments, need at least 3 to crash between unlinks", len(names))
		}
		if err := s.Disk.Remove(names[0]); err != nil {
			return err
		}
		s.Emit(Event{Kind: EventDisk, Msg: "unlinked the oldest segment, then died"})
	case crashAfterUnlinks:
		upTo := published
		if reclaimAboveThePoint {
			// The reclaim target stops being the published point and becomes the whole
			// log. INV-13's rule lives behind the Log's order policy, so the plant
			// substitutes that one rule and leaves the other two strict — what the
			// checker then sees is production's own TruncateLocal unlinking segments
			// whose records exist on this host alone.
			l.SetOrderPolicy(reclaimAnything{})
			upTo = l.Watermarks().Local
			s.Emit(Event{Kind: EventFault, Msg: "reclamation stopped stopping at the published point"})
		}
		if err := l.TruncateLocal(upTo); err != nil {
			return fmt.Errorf("truncate to %d: %w", upTo, err)
		}
		s.Emit(Event{Kind: EventTruncate, TruncatedUpTo: l.TruncatedUpTo(), Published: l.Watermarks().Published})
		if got := len(l.SegmentNames()); got >= segmentsBefore {
			return fmt.Errorf("truncation to %d left all %d segments: nothing was reclaimed", published, got)
		}
	}

	s.Disk.Crash()
	s.Emit(Event{Kind: EventRecovery, Msg: "resuming the WAL after the crash"})

	// (1) Every state above must be resumable. A directory a crash produced is not
	// corruption, and refusing to open it would strand the volume.
	resumed, err := wal.Resume(s.Disk, root, s.Clock, vol, 1, durable, limits, nil)
	if err != nil {
		return fmt.Errorf("resume after a crash %s: %w", boundary, err)
	}
	recs, err := wal.ReplaySegments(s.Disk, root, vol, 1)
	if err != nil {
		return fmt.Errorf("replay after a crash %s: %w", boundary, err)
	}
	if len(recs) == 0 {
		return fmt.Errorf("the WAL is empty after a crash %s", boundary)
	}

	// (2) The surviving records are one contiguous run ending at the last write:
	// reclamation takes whole segments off the front and never touches the tail.
	for i := 1; i < len(recs); i++ {
		if recs[i].Sequence != recs[i-1].Sequence+1 {
			return fmt.Errorf("crash %s left a hole: sequence %d follows %d",
				boundary, recs[i].Sequence, recs[i-1].Sequence)
		}
	}
	if last := recs[len(recs)-1].Sequence; last != l.Watermarks().Local {
		return fmt.Errorf("crash %s lost the tail: the WAL ends at %d, the log wrote up to %d",
			boundary, last, l.Watermarks().Local)
	}

	// (3) INV-13: nothing above the published point was discarded. Below it the
	// records are in a verified checkpoint; above it this host holds the only copy.
	//
	// The event is derived from the directory the crash left, not from what the log
	// meant to do: the number the checker judges is the first sequence that actually
	// survived. It is emitted before the assertion below so a violation reaches the
	// checker even when the scenario is about to name it itself.
	s.Emit(Event{Kind: EventTruncate, TruncatedUpTo: recs[0].Sequence - 1, Published: published})
	if first := recs[0].Sequence; first > published+1 {
		return fmt.Errorf("crash %s: the WAL starts at %d, above the published point %d (INV-13)",
			boundary, first, published)
	}
	if got := resumed.TruncatedUpTo(); got > resumed.Watermarks().Published && got > published {
		return fmt.Errorf("crash %s: resumed truncatedUpTo %d above published %d (INV-13)", boundary, got, published)
	}

	// (4) The read view survives. What a guest notices is not the directory listing
	// but whether its bytes are still there.
	for _, rec := range recs {
		if rec.Type != format.RecordWrite {
			continue
		}
		want := payload(int(rec.Sequence) - 1)
		got := make([]byte, len(want))
		if err := resumed.Read(rec.Offset, got); err != nil {
			return err
		}
		if string(got) != string(want) {
			return fmt.Errorf("crash %s: the read view at offset %d returned %q, want %q",
				boundary, rec.Offset, got, want)
		}
	}
	s.Notef("crash %s: %d records survived from sequence %d, replay clean, read view intact",
		boundary, len(recs), recs[0].Sequence)
	return nil
}
