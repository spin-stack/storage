//go:build integration

package vhost_test

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/blockdev"
	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/real"
	"github.com/spin-stack/storage/internal/vhost"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// walRoot is the directory under the lane's disk that holds the volume's segments.
const walRoot = "wal"

// leaseTTL is long enough that nothing here races it. The lease *expiring* is proven
// deterministically in internal/dst (guest-device-acks-durability-only-on-flush);
// what this lane is for is the parts a simulator cannot produce.
const leaseTTL = time.Hour

// walVolume is the volume id these tests write under. Not ids.New(): a fixed id keeps
// the WAL path stable across runs, and nothing here reads it back through metadata.
var walVolume = [16]byte{0x70, 0, 0, 0, 0, 0, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, 0x1a}

// countingStore counts PUTs. INV-18 says a normal WRITE issues none, and the only
// honest way to assert "none" is to count calls: an empty bucket is also what a PUT
// that failed leaves behind.
type countingStore struct {
	objectstore.Store
	mu   sync.Mutex
	puts int
}

func (s *countingStore) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	s.mu.Lock()
	s.puts++
	s.mu.Unlock()
	return s.Store.Put(ctx, key, data, opts)
}

func (s *countingStore) Puts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.puts
}

// walLane is a lane whose Backend is a wal.Log, plus the handles a test needs to
// check the WAL side of what the guest did.
type walLane struct {
	*lane
	log   *wal.Log
	disk  *real.Disk
	store *countingStore
}

// startWAL serves a guest off blockdev.Device over a real wal.Log on a real
// filesystem, seeded with the boot sector and the pattern the guest will copy.
//
// The object store is real.ObjectStore — the filesystem-backed implementation of
// objectstore.Store — rather than RustFS. What this lane exists to prove is the QEMU
// half (the guest's requests really do land in WAL records on a real disk); the S3
// half is proven against a real backend by the §6.1 conformance suite in
// integration/backend, and keeping this lane free of a container keeps `task
// test:integration:qemu` runnable wherever QEMU is.
func startWAL(t *testing.T, ctx context.Context, limits wal.Limits, seed func(write func(off uint64, b []byte))) *walLane {
	t.Helper()
	dir := laneDir(t)

	d, err := real.NewDisk(filepath.Join(dir, "nvme"))
	if err != nil {
		t.Fatalf("real.NewDisk: %v", err)
	}
	store, err := real.NewObjectStore(filepath.Join(dir, "bucket"))
	if err != nil {
		t.Fatalf("real.NewObjectStore: %v", err)
	}
	counting := &countingStore{Store: store}

	clk := real.NewClock()
	lm := lease.NewManager(clk, leaseTTL)
	lm.Grant()

	// No remote path (ADR-0026 increment 4.5): a FLUSH is fdatasync. The counting store
	// stays, and it is now a stronger statement than it was — it counts PUTs on a device
	// that has no way to issue one, so INV-18 ("S3 is not in the guest's write path") is
	// true by construction rather than by discipline.
	log := wal.NewLog(d, walRoot, clk, walVolume, 1, limits)
	t.Cleanup(func() { _ = log.Close() })

	// Seeding goes through the log, not around it: the boot sector SeaBIOS reads to
	// find the 0x55aa signature is itself a WAL record served out of the read view.
	// If read-your-writes did not work, the guest would not boot at all.
	seed(func(off uint64, b []byte) {
		if _, err := log.Write(off, b, 0); err != nil {
			t.Fatalf("seeding the WAL at %d: %v", off, err)
		}
	})

	dev, err := blockdev.New(log, deviceSize)
	if err != nil {
		t.Fatalf("blockdev.New: %v", err)
	}
	return &walLane{lane: serveLane(t, ctx, dir, dev), log: log, disk: d, store: counting}
}

// walRecordBytes is how many bytes one WRITE record of n payload bytes costs in the
// WAL. Measured rather than computed from the header size: the number this calibrates
// is a limit that must sit *between* two record counts, and a constant that drifted
// from the format would silently turn the backpressure test into a test of nothing.
func walRecordBytes(t *testing.T, n int) int64 {
	t.Helper()
	d, err := real.NewDisk(filepath.Join(laneDir(t), "calibrate"))
	if err != nil {
		t.Fatal(err)
	}
	l := wal.NewLog(d, "calibrate", real.NewClock(), walVolume, 1, wal.Limits{})
	defer func() { _ = l.Close() }()
	if _, err := l.Write(0, make([]byte, n), 0); err != nil {
		t.Fatalf("calibrating the record size: %v", err)
	}
	return l.UnflushedBytes()
}

// TestQEMUGuestWritesThroughTheWAL is ADR-0018's slice as far as a guest can drive it:
//
//	guest write → WAL append → FLUSH → verified S3 object
//
// A real QEMU 11.0.2 boots off a device whose bytes exist only in a WAL read view,
// reads a sector, writes it back to another LBA, and the bytes are then checked on the
// WAL side — in the read view *and* in the records replayed off the real filesystem,
// so a device that completed the request without appending anything would fail.
//
// The guest cannot ask for the FLUSH itself: SeaBIOS's INT 13h has no verb for it (see
// testdata/bootsector.S), so the FLUSH below is issued by the test. What that costs is
// the last link — VIRTIO_BLK_T_FLUSH arriving over the virtqueue — and that link is
// covered against the simulated front-end in internal/vhost and against a real store in
// the DST scenario. Closing it for real needs a Linux guest, which is a bigger lane
// than this one.
func TestQEMUGuestWritesThroughTheWAL(t *testing.T) {
	ctx := t.Context()
	boot := bootSector(t)
	want := bytes.Repeat([]byte("spin-stack/wal!!"), vhost.SectorSize/16)

	l := startWAL(t, ctx, wal.Limits{MaxUnflushedBytes: 1 << 20}, func(write func(uint64, []byte)) {
		write(0, boot)
		write(patternLBA*vhost.SectorSize, want)
	})

	code, output := runQEMU(t, ctx, l.lane)
	if code != guestExitOK {
		t.Fatalf("guest exited %d, want %d (the boot sector's success path).\nrequests: %v\nqemu output:\n%s",
			code, guestExitOK, l.trace.requests, output)
	}
	if _, _, ioErrors := l.trace.snapshot(); len(ioErrors) != 0 {
		t.Fatalf("the backend completed %d request(s) as I/O errors: %v", len(ioErrors), ioErrors)
	}

	t.Run("the guest's bytes are in the WAL's read view", func(t *testing.T) {
		got := make([]byte, vhost.SectorSize)
		if _, err := l.dev.ReadAt(got, resultLBA*vhost.SectorSize); err != nil {
			t.Fatalf("reading back LBA %d: %v", resultLBA, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("LBA %d holds %q…, want the pattern the guest read from LBA %d",
				resultLBA, got[:32], patternLBA)
		}
	})

	t.Run("the guest's write is a WRITE record on the real disk", func(t *testing.T) {
		recs, err := wal.ReplaySegments(l.disk, walRoot, walVolume, 1)
		if err != nil {
			t.Fatalf("replaying the WAL: %v", err)
		}
		var found *wal.Record
		for i, r := range recs {
			if r.Type == format.RecordWrite && r.Offset == resultLBA*vhost.SectorSize {
				found = &recs[i]
			}
		}
		if found == nil {
			t.Fatalf("no WRITE record at offset %d among %d records; the guest's write never reached the WAL",
				resultLBA*vhost.SectorSize, len(recs))
		}
		if !bytes.Equal(found.Payload, want) {
			t.Fatalf("the WAL record at %d carries %q…, not the guest's bytes", found.Offset, found.Payload[:32])
		}
		// Decision (1)/(3): a plain WRITE never carries FUA, because wal.Log.Write
		// implements none of the FUA ACK contract and refuses the flag on purpose.
		if found.Flags&format.FlagFUA != 0 {
			t.Fatalf("the guest's WRITE was appended with FlagFUA (%#x)", found.Flags)
		}
	})

	t.Run("the guest's write issued no PUT", func(t *testing.T) {
		// INV-18 with a real guest, a real filesystem and a real object store in the
		// loop, which is the one place a stray sync-on-write would show up.
		if got := l.store.Puts(); got != 0 {
			t.Fatalf("the guest write path issued %d PUT(s) (§5.3, INV-18)", got)
		}
		if w := l.log.Watermarks(); w.Durable != 0 {
			t.Fatalf("durable = %d before any FLUSH", w.Durable)
		}
	})

	t.Run("FLUSH advances durable, and still issues no PUT", func(t *testing.T) {
		// The subtest above proves the *write* path never touches S3. This one used to
		// prove the FLUSH path does — "FLUSH makes it durable in the object store" — and
		// that is exactly what ADR-0026 withdrew: a FLUSH is fdatasync and an ACK, and
		// the volume reaches the object store when it stops.
		//
		// So the PUT count is asserted again rather than dropped, and it is a stronger
		// statement now than it was: with a real guest, a real filesystem and a real
		// object store in the loop, *nothing on the guest's I/O path* reaches S3 at all.
		// INV-18 stops being a discipline and becomes a property of the wiring.
		if err := l.dev.Flush(ctx); err != nil {
			t.Fatalf("FLUSH: %v", err)
		}
		if got := l.store.Puts(); got != 0 {
			t.Fatalf("a FLUSH issued %d PUT(s); §14.8 says it ACKs on fdatasync alone", got)
		}
		w := l.log.Watermarks()
		if w.Durable != w.Local {
			t.Fatalf("after a successful FLUSH durable=%d, local=%d", w.Durable, w.Local)
		}
		t.Logf("a real guest's FLUSH made %d records durable locally, with no object-store traffic", w.Durable)
	})
}

// TestQEMUGuestSeesAnErrorWhenTheWALRefusesAWrite is decision (4) with a real guest in
// the loop, and it is the assertion the unit tests structurally cannot make: that a
// WAL refusal becomes something the guest can *act on*.
//
// The unflushed bound (§5.7) is calibrated to hold exactly the two seeded records, so
// SeaBIOS boots and reads normally — reads append nothing — and the guest's one INT 13h
// WRITE is refused. The backend completes it as VIRTIO_BLK_S_IOERR, SeaBIOS returns
// with CF set, and the boot sector takes its failure path.
//
// The guest exiting at all is half the assertion: a device that answered a refused
// request by dropping it, or by blocking, would leave the guest waiting on a
// completion that never comes and this test would die on its deadline instead.
func TestQEMUGuestSeesAnErrorWhenTheWALRefusesAWrite(t *testing.T) {
	ctx := t.Context()
	boot := bootSector(t)
	want := bytes.Repeat([]byte("spin-stack/full!"), vhost.SectorSize/16)

	// Room for the two seed records and not one byte more.
	limit := 2 * walRecordBytes(t, vhost.SectorSize)

	l := startWAL(t, ctx, wal.Limits{MaxUnflushedBytes: limit}, func(write func(uint64, []byte)) {
		write(0, boot)
		write(patternLBA*vhost.SectorSize, want)
	})
	if got := l.log.UnflushedBytes(); got != limit {
		t.Fatalf("the seed left %d unflushed bytes against a limit of %d; the calibration is wrong", got, limit)
	}

	code, output := runQEMU(t, ctx, l.lane)
	if code != guestExitFailed {
		t.Fatalf("guest exited %d, want %d (the boot sector's failure path): a WAL refusal must reach the guest as an I/O error.\nqemu output:\n%s",
			code, guestExitFailed, output)
	}

	_, _, ioErrors := l.trace.snapshot()
	if len(ioErrors) == 0 {
		t.Fatal("the backend reported no I/O error, so the guest failed for some other reason")
	}
	t.Logf("the backend refused %d request(s); the first was: %v", len(ioErrors), ioErrors[0])

	// Nothing was written: a refused WRITE must leave no record and no readable bytes,
	// or a guest that retries finds work it was told had failed.
	got := make([]byte, vhost.SectorSize)
	if _, err := l.dev.ReadAt(got, resultLBA*vhost.SectorSize); err != nil {
		t.Fatalf("reading back LBA %d: %v", resultLBA, err)
	}
	if !bytes.Equal(got, make([]byte, vhost.SectorSize)) {
		t.Fatalf("LBA %d is non-zero after a refused write: %q…", resultLBA, got[:32])
	}
	if w := l.log.Watermarks(); w.Local != 2 {
		t.Fatalf("local watermark = %d, want 2 (the two seed records only)", w.Local)
	}
	if got := l.store.Puts(); got != 0 {
		t.Fatalf("a refused write path issued %d PUT(s)", got)
	}
}
