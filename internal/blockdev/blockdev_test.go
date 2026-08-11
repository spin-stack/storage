package blockdev_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/blockdev"
	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/vhost"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// capacity is the device every test here serves unless it says otherwise: small,
// because nothing below addresses more than a few sectors, and a whole number of
// them because that is all a virtio-blk capacity can express.
const capacity = 1 << 20

var volume = [16]byte{0x40}

// countingStore is objectstore.Store with a PUT counter. INV-18 is "no PUT on the
// write path", and the only way to assert *no* PUT is to count calls: listing the
// bucket would also be empty after a PUT that failed, which is a different and much
// weaker statement.
type countingStore struct {
	objectstore.Store

	mu   sync.Mutex
	puts int
	fail error
}

func newCountingStore() *countingStore { return &countingStore{Store: sim.NewObjectStore()} }

func (s *countingStore) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	s.mu.Lock()
	s.puts++
	fail := s.fail
	s.mu.Unlock()
	if fail != nil {
		return objectstore.PutResult{}, fail
	}
	return s.Store.Put(ctx, key, data, opts)
}

func (s *countingStore) Puts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.puts
}

// rig is one device over one log, plus the pieces a test needs to reach behind it.
type rig struct {
	dev   *blockdev.Device
	log   *wal.Log
	clk   *sim.Clock
	disk  *sim.Disk
	store *countingStore
	lease *lease.Manager
	root  string
}

type rigOption func(*rigConfig)

type rigConfig struct {
	limits  wal.Limits
	root    string
	remote  bool
	leaseOn bool
	cap     int64
	inject  func(*sim.Disk, string)
}

func withLimits(l wal.Limits) rigOption { return func(c *rigConfig) { c.limits = l } }

// withDiskCap caps the device under the WAL root, which is the third bound and the only
// one that is not about this volume at all: the volume is inside its share and the disk
// beneath it has run out. The cap covers the directory rather than a file, because a WAL
// is a set of segments and a per-file ceiling would be lifted by rotating to the next one.
func withDiskCap(bytes int64) rigOption {
	return func(c *rigConfig) {
		c.inject = func(d *sim.Disk, root string) { d.InjectENOSPC(root, bytes) }
	}
}

func newRig(t *testing.T, opts ...rigOption) *rig {
	t.Helper()
	cfg := rigConfig{
		limits:  wal.Limits{MaxUnflushedBytes: 1 << 20},
		root:    "wal",
		remote:  true,
		leaseOn: true,
		cap:     capacity,
	}
	for _, o := range opts {
		o(&cfg)
	}

	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	if cfg.inject != nil {
		cfg.inject(d, cfg.root)
	}
	store := newCountingStore()
	l := wal.NewLog(d, cfg.root, clk, volume, 1, cfg.limits)

	lm := lease.NewManager(clk, 10*time.Second)
	lm.Grant()

	dev, err := blockdev.New(l, cfg.cap)
	if err != nil {
		t.Fatalf("blockdev.New: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return &rig{dev: dev, log: l, clk: clk, disk: d, store: store, lease: lm, root: cfg.root}
}

// records replays what actually reached the disk, which is the only evidence about
// the write path that a read view cannot fake.
func (r *rig) records(t *testing.T) []wal.Record {
	t.Helper()
	recs, err := wal.ReplaySegments(r.disk, r.root, volume, 1)
	if err != nil {
		t.Fatalf("replaying the WAL: %v", err)
	}
	return recs
}

func pattern(seed byte, n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = seed + byte(i)
	}
	return p
}

// TestDeviceIsAVhostBackend is the whole point of the package: wal.Log fits behind
// the seam without either side learning about the other.
func TestDeviceIsAVhostBackend(t *testing.T) {
	r := newRig(t)
	var b vhost.Backend = r.dev
	if got := b.Size(); got != capacity {
		t.Fatalf("Size() = %d, want %d", got, capacity)
	}
}

// TestNewRefusesACapacityNoGuestCouldBeToldAbout: the capacity in the virtio-blk
// config is in 512-byte sectors, so a partial trailing sector is either capacity the
// guest addresses and the device refuses, or capacity silently discarded.
func TestNewRefusesACapacityNoGuestCouldBeToldAbout(t *testing.T) {
	tests := []struct {
		name string
		cap  int64
	}{
		{"zero", 0},
		{"negative", -512},
		{"not a whole sector", 1000},
		{"one byte past a sector", vhost.SectorSize + 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
			l := wal.NewLog(sim.NewDisk(), "wal", clk, volume, 1, wal.Limits{})
			if _, err := blockdev.New(l, tc.cap); err == nil {
				t.Fatalf("New accepted a capacity of %d bytes", tc.cap)
			}
		})
	}
	t.Run("a nil log has nothing to serve", func(t *testing.T) {
		if _, err := blockdev.New(nil, capacity); err == nil {
			t.Fatal("New accepted a nil log")
		}
	})
}

// TestReadYourWritesWithoutFlush is decision (6): a guest that writes and reads the
// same block without a FLUSH must read what it wrote. The WAL's read view is where
// that lives, and the device has to consult it rather than any objectized copy.
func TestReadYourWritesWithoutFlush(t *testing.T) {
	r := newRig(t)
	want := pattern(0x11, vhost.SectorSize)

	if n, err := r.dev.WriteAt(want, 8*vhost.SectorSize); err != nil || n != len(want) {
		t.Fatalf("WriteAt = (%d, %v), want (%d, nil)", n, err, len(want))
	}
	if w := r.log.Watermarks(); w.Durable != 0 {
		t.Fatalf("a plain WRITE advanced durable to %d; only FLUSH/FUA may (INV-18)", w.Durable)
	}

	got := make([]byte, vhost.SectorSize)
	if n, err := r.dev.ReadAt(got, 8*vhost.SectorSize); err != nil || n != len(got) {
		t.Fatalf("ReadAt = (%d, %v), want (%d, nil)", n, err, len(got))
	}
	if string(got) != string(want) {
		t.Fatalf("read-your-writes failed: %x…, want %x…", got[:8], want[:8])
	}

	t.Run("an overwrite is what the next read sees", func(t *testing.T) {
		second := pattern(0x55, vhost.SectorSize)
		if _, err := r.dev.WriteAt(second, 8*vhost.SectorSize); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, vhost.SectorSize)
		if _, err := r.dev.ReadAt(got, 8*vhost.SectorSize); err != nil {
			t.Fatal(err)
		}
		if string(got) != string(second) {
			t.Fatalf("the read view kept the older write: %x…", got[:8])
		}
	})

	t.Run("unwritten sectors read as zero", func(t *testing.T) {
		got := make([]byte, vhost.SectorSize)
		for i := range got {
			got[i] = 0xff // the device must fill the buffer, not merely leave it alone
		}
		if _, err := r.dev.ReadAt(got, 900*vhost.SectorSize); err != nil {
			t.Fatal(err)
		}
		for i, b := range got {
			if b != 0 {
				t.Fatalf("byte %d of an unwritten sector = %#x, want 0", i, b)
			}
		}
	})

	t.Run("a read straddling written and unwritten bytes gets both", func(t *testing.T) {
		buf := make([]byte, 2*vhost.SectorSize)
		if _, err := r.dev.ReadAt(buf, 8*vhost.SectorSize); err != nil {
			t.Fatal(err)
		}
		if string(buf[:vhost.SectorSize]) != string(pattern(0x55, vhost.SectorSize)) {
			t.Fatal("the written half of the read is wrong")
		}
		for i, b := range buf[vhost.SectorSize:] {
			if b != 0 {
				t.Fatalf("byte %d of the unwritten half = %#x, want 0", i, b)
			}
		}
	})
}

// TestWriteAtNeverCarriesFUA is decision (1)/(3) as an assertion about the bytes on
// disk. wal.Log.Write refuses format.FlagFUA on purpose (ErrFUAOnWrite) because a
// plain WRITE implements none of the FUA ACK contract; the device must therefore
// never set it, and the record it appends is the proof.
func TestWriteAtNeverCarriesFUA(t *testing.T) {
	r := newRig(t)
	if _, err := r.dev.WriteAt(pattern(1, vhost.SectorSize), 0); err != nil {
		t.Fatal(err)
	}
	recs := r.records(t)
	if len(recs) != 1 {
		t.Fatalf("the WAL holds %d records, want 1", len(recs))
	}
	if recs[0].Flags&format.FlagFUA != 0 {
		t.Fatalf("the device appended a WRITE carrying FlagFUA (flags %#x)", recs[0].Flags)
	}
	if recs[0].Type != format.RecordWrite {
		t.Fatalf("record type %v, want a WRITE", recs[0].Type)
	}
	if recs[0].Offset != 0 || string(recs[0].Payload) != string(pattern(1, vhost.SectorSize)) {
		t.Fatalf("the record does not carry the guest's bytes at the guest's offset")
	}
}

// TestARefusedWriteAppendsNothing: a WRITE the guest was told failed must not be in
// the WAL. Otherwise it replays as a record the guest does not believe in, and the
// sequence it consumed is one the next accepted write reuses.
func TestARefusedWriteAppendsNothing(t *testing.T) {
	r := newRig(t, withLimits(wal.Limits{MaxUnflushedBytes: 700}))
	if _, err := r.dev.WriteAt(pattern(8, vhost.SectorSize), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := r.dev.WriteAt(pattern(9, vhost.SectorSize), vhost.SectorSize); err == nil {
		t.Fatal("the second write was accepted; the unflushed bound should have refused it")
	}
	if recs := r.records(t); len(recs) != 1 {
		t.Fatalf("the WAL holds %d records after one accepted write", len(recs))
	}
	if w := r.log.Watermarks(); w.Local != 1 {
		t.Fatalf("local watermark = %d after one accepted write", w.Local)
	}
	// And the refused bytes are not readable: a guest that retries must not find
	// them already there.
	got := make([]byte, vhost.SectorSize)
	if _, err := r.dev.ReadAt(got, vhost.SectorSize); err != nil {
		t.Fatal(err)
	}
	for i, b := range got {
		if b != 0 {
			t.Fatalf("byte %d of the refused write is readable (%#x)", i, b)
		}
	}
}

// TestRequestsOutsideTheDeviceAreRefused covers both directions and the arithmetic
// that could wrap. A wrapped offset is not a large request, it is a request for
// somebody else's data.
func TestRequestsOutsideTheDeviceAreRefused(t *testing.T) {
	r := newRig(t)
	tests := []struct {
		name string
		n    int
		off  int64
	}{
		{"starts past the end", vhost.SectorSize, capacity},
		{"ends past the end", vhost.SectorSize, capacity - 1},
		{"negative offset", vhost.SectorSize, -1},
		{"offset that would wrap", vhost.SectorSize, 1<<62 + 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := r.dev.ReadAt(make([]byte, tc.n), tc.off); !errors.Is(err, vhost.ErrOutOfRange) {
				t.Errorf("ReadAt: %v, want ErrOutOfRange", err)
			}
			if _, err := r.dev.WriteAt(make([]byte, tc.n), tc.off); !errors.Is(err, vhost.ErrOutOfRange) {
				t.Errorf("WriteAt: %v, want ErrOutOfRange", err)
			}
		})
	}
	t.Run("the last sector is inside the device", func(t *testing.T) {
		if _, err := r.dev.WriteAt(pattern(10, vhost.SectorSize), capacity-vhost.SectorSize); err != nil {
			t.Fatalf("the last sector was refused: %v", err)
		}
	})
	t.Run("an empty request is not a WAL record", func(t *testing.T) {
		before := r.log.Watermarks().Local
		if n, err := r.dev.WriteAt(nil, 0); n != 0 || err != nil {
			t.Fatalf("WriteAt(nil) = (%d, %v)", n, err)
		}
		if n, err := r.dev.ReadAt(nil, 0); n != 0 || err != nil {
			t.Fatalf("ReadAt(nil) = (%d, %v)", n, err)
		}
		if got := r.log.Watermarks().Local; got != before {
			t.Fatalf("an empty WRITE consumed sequence %d", got)
		}
	})
}

func TestConcurrentRequestsDoNotRaceTheLog(t *testing.T) {
	ctx := t.Context()
	r := newRig(t)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			off := int64(i) * vhost.SectorSize
			for range 16 {
				if _, err := r.dev.WriteAt(pattern(byte(i), vhost.SectorSize), off); err != nil {
					t.Error(err)
					return
				}
				if _, err := r.dev.ReadAt(make([]byte, vhost.SectorSize), off); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 4 {
			if err := r.dev.Flush(ctx); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	wg.Wait()
}

// The seam DISCARD spent months not crossing. Every piece below this test was
// complete, tested and unreachable: wal.Log.Discard, cow.IntervalMap.Clear, the
// RecordDiscard codec and the zero-read all worked, and internal/vhost withheld the
// feature bit, so no guest could ask. This asserts the whole path from the Backend
// method a virtio-blk request lands on down to the bytes a later read returns.
//
// It asserts on the bytes read back, not on the WAL record: a discard that appended
// the right record and did not touch the read view would satisfy any assertion on
// the log, and the guest would keep reading the data it just told us to forget.
func TestADiscardMakesTheRangeReadBackAsZeros(t *testing.T) {
	tests := []struct {
		name  string
		clear func(d *blockdev.Device, off, length int64) error
	}{
		{"DISCARD", func(d *blockdev.Device, off, length int64) error { return d.Discard(off, length) }},
		{"WRITE_ZEROES may_unmap=0", func(d *blockdev.Device, off, length int64) error {
			return d.WriteZeroes(off, length, false)
		}},
		{"WRITE_ZEROES may_unmap=1", func(d *blockdev.Device, off, length int64) error {
			return d.WriteZeroes(off, length, true)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t)
			pattern := make([]byte, 3*512)
			for i := range pattern {
				pattern[i] = byte(i%251) + 1 // never zero, so a wrong read cannot pass by accident
			}
			if _, err := r.dev.WriteAt(pattern, 512); err != nil {
				t.Fatalf("WriteAt: %v", err)
			}
			// Clear the middle sector only: a discard that cleared more than it was
			// asked to is data loss, and one that cleared less is the bug.
			if err := tc.clear(r.dev, 2*512, 512); err != nil {
				t.Fatalf("clear: %v", err)
			}

			got := make([]byte, 3*512)
			if _, err := r.dev.ReadAt(got, 512); err != nil {
				t.Fatalf("ReadAt: %v", err)
			}
			if !bytesEqual(got[512:1024], make([]byte, 512)) {
				t.Errorf("the cleared sector reads %x, want zeros", got[512:520])
			}
			if !bytesEqual(got[:512], pattern[:512]) {
				t.Errorf("the sector before the cleared range was disturbed")
			}
			if !bytesEqual(got[1024:], pattern[1024:]) {
				t.Errorf("the sector after the cleared range was disturbed")
			}
		})
	}
}

// A zero-length clear is a no-op rather than a record: a sequence and a header spent
// describing nothing, which replay would carry forever. An fstrim over an already
// empty segment produces exactly this.
func TestAZeroLengthClearIsNotARecord(t *testing.T) {
	r := newRig(t)
	before := r.log.Watermarks().Local
	if err := r.dev.Discard(0, 0); err != nil {
		t.Fatalf("Discard: %v", err)
	}
	if err := r.dev.WriteZeroes(0, 0, true); err != nil {
		t.Fatalf("WriteZeroes: %v", err)
	}
	if got := r.log.Watermarks().Local; got != before {
		t.Errorf("a zero-length clear consumed %d sequences", got-before)
	}
}

// Out of range is refused rather than clamped. Clamping a discard is data loss with
// an OK status: the guest is told the range it named is gone and a different one is.
func TestAClearOutsideTheDeviceIsRefused(t *testing.T) {
	r := newRig(t)
	for _, tc := range []struct {
		name        string
		off, length int64
	}{
		{"past the end", capacity, 512},
		{"straddling the end", capacity - 256, 512},
		{"negative offset", -512, 512},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := r.dev.Discard(tc.off, tc.length); !errors.Is(err, vhost.ErrOutOfRange) {
				t.Errorf("Discard(%d, %d) = %v, want ErrOutOfRange", tc.off, tc.length, err)
			}
		})
	}
}

// fillTheShare writes sector after sector until the device refuses one, and returns
// that refusal. It is how a guest reaches the bound in production: nothing else on the
// host consumes the volume's share, and no single request is large enough to cross it.
func fillTheShare(t *testing.T, r *rig) error {
	t.Helper()
	for i := range 4096 {
		off := int64(i%16) * vhost.SectorSize
		if _, err := r.dev.WriteAt(pattern(byte(i), vhost.SectorSize), off); err != nil {
			return err
		}
	}
	t.Fatal("4096 writes did not reach the local bound; the rig's limits are not bounding anything")
	return nil
}

// The bound the Agent sets is MaxLocalBytes — the volume's share of the device
// (agent.Budget.Limits sets that one and no other) — and *nothing clears it while the
// volume is running*. The sentence the device handed back said the opposite: "a
// successful FLUSH clears it", which is the one remedy that cannot work. An operator
// reading it does the FLUSH, watches the writes keep failing, and has learned nothing.
//
// The assertion is on the device, not on the wording alone: a FLUSH is performed and
// the next write is still refused. A test that only grepped the string would pass on a
// device that had been fixed in its message and nowhere else — and, more to the point,
// would have passed on the old message too if someone had merely reworded it.
func TestBackpressureNamesTheRemedyThatActuallyWorks(t *testing.T) {
	ctx := t.Context()
	r := newRig(t, withLimits(wal.Limits{MaxLocalBytes: 8 * 1024}))

	err := fillTheShare(t, r)
	if !errors.Is(err, wal.ErrBackpressure) {
		t.Fatalf("filling the share failed with %v, want wal.ErrBackpressure", err)
	}

	// What the sentence promises, checked against the device.
	if ferr := r.dev.Flush(ctx); ferr != nil {
		t.Fatalf("FLUSH after backpressure: %v", ferr)
	}
	if _, werr := r.dev.WriteAt(pattern(0x77, vhost.SectorSize), 0); !errors.Is(werr, wal.ErrBackpressure) {
		t.Fatalf("a FLUSH cleared the local bound (next write: %v) — then the old message was right "+
			"and this test is the thing that is wrong", werr)
	}

	msg := err.Error()
	if strings.Contains(msg, "a successful FLUSH clears it") {
		t.Fatalf("the refusal still tells the operator to FLUSH, which does not clear this bound: %s", msg)
	}
	for _, want := range []string{"FLUSH does not clear", "stop"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("the refusal does not mention %q, so it does not name what relieves the bound: %s", want, msg)
		}
	}
}

// The host's half of the same event. A guest that crosses its share gets EIO per
// request and a failed fsync; before this the Agent's log stayed at its start-up lines
// and nothing on the host knew. The Device latches the fact so that a reporter running
// at a human cadence can find it — one line per volume, not one per rejected write.
func TestADeviceRemembersThatItRefusedAGuestForSpace(t *testing.T) {
	ctx := t.Context()
	r := newRig(t, withLimits(wal.Limits{MaxLocalBytes: 8 * 1024}))

	if ref, refused := r.dev.RefusedForSpace(); refused {
		t.Fatalf("a device that has served nothing claims it refused for space: %s", ref)
	}

	if err := fillTheShare(t, r); !errors.Is(err, wal.ErrBackpressure) {
		t.Fatalf("filling the share failed with %v, want wal.ErrBackpressure", err)
	}

	ref, refused := r.dev.RefusedForSpace()
	if !refused {
		t.Fatal("the device refused a guest write for want of space and remembers nothing; the host has no way to see it")
	}
	if !strings.Contains(ref.Remedy, "share") {
		t.Fatalf("the latched remedy %q does not name the bound", ref.Remedy)
	}

	// Sticky across the one thing an operator would try. If this cleared, the reporter
	// would log the volume again on every poll for as long as the guest kept writing.
	if err := r.dev.Flush(ctx); err != nil {
		t.Fatalf("FLUSH: %v", err)
	}
	if _, still := r.dev.RefusedForSpace(); !still {
		t.Fatal("the latch cleared on a FLUSH; the bound did not")
	}
}

// A refusal that is not about space must not latch: a guest driver that walks off the
// end of the device would otherwise make the Agent announce a full disk, and the
// operator would go and grow a device that has room.
func TestOnlySpaceRefusalsLatch(t *testing.T) {
	r := newRig(t)
	if _, err := r.dev.WriteAt(pattern(1, vhost.SectorSize), capacity); !errors.Is(err, vhost.ErrOutOfRange) {
		t.Fatalf("a write past the end returned %v", err)
	}
	if ref, refused := r.dev.RefusedForSpace(); refused {
		t.Fatalf("an out-of-range write latched a space refusal: %s", ref)
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// The two bounds a guest can hit have opposite remedies, and an operator acts on the
// sentence rather than on the sentinel — so the sentence has to differ. The device bound
// is space this volume cannot get back while it runs: stop it, which publishes and
// reclaims. The read view's bound is memory, and a DISCARD gives it back with the volume
// still serving, which integration/vhost proves a real kernel can do from inside a guest.
//
// This is here because one sentinel covered four bounds, and blockdev gave a view-bound
// refusal the device bound's advice — an operator following it would take a session's
// downtime for a condition an fstrim clears.
func TestTheTwoBoundsTellTheOperatorDifferentThings(t *testing.T) {
	tests := []struct {
		name    string
		limits  wal.Limits
		fill    func(t *testing.T, d *blockdev.Device)
		wantSay string
		notSay  string
	}{
		{
			name:   "the device share is exhausted: stop the volume",
			limits: wal.Limits{MaxLocalBytes: 64 << 10},
			fill: func(t *testing.T, d *blockdev.Device) {
				// Rewrite one block, so the read view stays tiny and only the retained
				// segments grow — the device bound and nothing else.
				for range 4096 {
					if _, err := d.WriteAt(make([]byte, 512), 0); err != nil {
						return
					}
				}
			},
			wantSay: "stop the volume",
			notSay:  "DISCARD",
		},
		{
			name:   "the read view is at its bound: trim, do not stop",
			limits: wal.Limits{MaxViewBytes: 64 << 10},
			fill: func(t *testing.T, d *blockdev.Device) {
				// Distinct offsets, so the view grows while the segments stay small.
				for i := range int64(4096) {
					if _, err := d.WriteAt(make([]byte, 512), i*512); err != nil {
						return
					}
				}
			},
			wantSay: "DISCARD",
			notSay:  "stop the volume",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, withLimits(tc.limits))
			tc.fill(t, r.dev)

			// The refusal itself, taken from a write the bound turned away.
			var err error
			for i := range int64(8192) {
				if _, err = r.dev.WriteAt(make([]byte, 512), i*512); err != nil {
					break
				}
			}
			if err == nil {
				t.Fatalf("nothing was refused under %+v; the bound did not act", tc.limits)
			}
			if !errors.Is(err, wal.ErrBackpressure) {
				t.Fatalf("the guest got %v, which is not the error it understands as backpressure", err)
			}
			if !strings.Contains(err.Error(), tc.wantSay) {
				t.Errorf("the refusal does not say %q, which is the remedy for this bound:\n  %v", tc.wantSay, err)
			}
			if strings.Contains(err.Error(), tc.notSay) {
				t.Errorf("the refusal says %q, which is the OTHER bound's remedy and costs the operator the wrong thing:\n  %v", tc.notSay, err)
			}
		})
	}
}

// writeUntilRefused drives the device the way a guest does — one 512-byte request after
// another — and returns the first refusal. `off` decides *where*, and where is what
// decides which bound is reached: distinct offsets grow the read view, one offset in
// place grows only the retained segments.
func writeUntilRefused(t *testing.T, d *blockdev.Device, off func(i int64) int64) error {
	t.Helper()
	for i := range int64(8192) {
		if _, err := d.WriteAt(pattern(byte(i), 512), off(i)); err != nil {
			return err
		}
	}
	t.Fatal("8192 writes reached no bound; the rig's limits are not bounding anything")
	return nil
}

// distinct grows the read view by 512 bytes a write; inPlace grows it by nothing.
func distinct(i int64) int64 { return (i * 512) % capacity }
func inPlace(int64) int64    { return 0 }

// One latched sentence cannot label a metric, and the layer above this one proved it: the
// Agent's watcher turned every refusal into `volume_backpressure=1` with a single WARN
// that always said "for want of space". An operator could not tell a volume that needs an
// `fstrim` from one that needs a restart from a host that needs a bigger disk — and the
// Go layer had told the three apart for as long as wal.ErrViewBound has existed.
//
// So the assertion is on the machine-readable reason, not on the prose. The prose is
// asserted separately (TestTheTwoBoundsTellTheOperatorDifferentThings); a label is what
// survives the trip into a time series.
func TestTheLatchNamesWhichBoundRefusedTheGuest(t *testing.T) {
	tests := []struct {
		name string
		opts []rigOption
		off  func(int64) int64
		want blockdev.Reason
	}{
		{
			// Memory. The guest gets it back itself, with the volume still serving.
			name: "the read view's memory bound",
			opts: []rigOption{withLimits(wal.Limits{MaxViewBytes: 64 << 10})},
			off:  distinct,
			want: blockdev.ReasonViewMemory,
		},
		{
			// The volume's share of the device. Nothing gives it back until the session
			// ends, which is the documented shape of V1 (§5.7).
			name: "the volume's share of the local device",
			opts: []rigOption{withLimits(wal.Limits{MaxLocalBytes: 64 << 10})},
			off:  inPlace,
			want: blockdev.ReasonWALShare,
		},
		{
			// The disk, under a volume that never reached its own bound. The remedy is on
			// the host and it is nobody in this process's to apply.
			name: "the device itself, under a volume still inside its share",
			opts: []rigOption{withLimits(wal.Limits{}), withDiskCap(16 << 10)},
			off:  distinct,
			want: blockdev.ReasonDeviceENOSPC,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, tc.opts...)
			err := writeUntilRefused(t, r.dev, tc.off)
			if err == nil {
				t.Fatal("nothing was refused")
			}
			ref, refused := r.dev.RefusedForSpace()
			if !refused {
				t.Fatalf("the guest was refused (%v) and the device latched nothing", err)
			}
			if ref.Reason != tc.want {
				t.Fatalf("the device reports reason %q for %v\n  want %q", ref.Reason, err, tc.want)
			}
			if ref.Remedy == "" {
				t.Fatal("the reason carries no remedy; the WARN line has nothing to print")
			}
		})
	}
}

// The latch is right for the transition and wrong for ever, and this is the case that
// makes it wrong. `spin.hold=walk` proves from inside a real kernel that a guest can
// cross the read view's memory bound, BLKDISCARD its way back under it and keep writing;
// until this, the device went on reporting the refusal for the rest of the session, so
// `volume_backpressure` stayed at 1 and the operator went on being told to stop a volume
// that had already recovered.
//
// What counts as evidence that the condition ended is the write, and only the write. wal
// charges every WRITE against the view bound *before* appending, so a WRITE the log took
// is proof the view is back inside its ceiling. A DISCARD's own success proves nothing —
// clears are deliberately exempt from that bound so that a guest already over it can
// still escape — and that is asserted here rather than assumed.
func TestATrimAndAWriteEndTheReadViewRefusal(t *testing.T) {
	r := newRig(t, withLimits(wal.Limits{MaxViewBytes: 64 << 10}))

	err := writeUntilRefused(t, r.dev, distinct)
	if !errors.Is(err, wal.ErrViewBound) {
		t.Fatalf("the guest was refused with %v, not the read view's bound", err)
	}
	if ref, _ := r.dev.RefusedForSpace(); ref.Reason != blockdev.ReasonViewMemory {
		t.Fatalf("the device latched %q for a view-bound refusal", ref.Reason)
	}

	// The guest's fstrim, issued from over the bound.
	if err := r.dev.Discard(0, 64<<10); err != nil {
		t.Fatalf("a DISCARD from over the view bound was refused (%v); the bound is a one-way door", err)
	}
	if _, still := r.dev.RefusedForSpace(); !still {
		t.Fatal("the DISCARD alone cleared the latch: a clear carries no view charge, so its " +
			"success says nothing about whether the view came back under its bound")
	}

	// And now the guest carries on, which is the whole claim.
	if _, err := r.dev.WriteAt(pattern(0x33, 512), 0); err != nil {
		t.Fatalf("the guest trimmed and its next write was still refused: %v", err)
	}
	if ref, still := r.dev.RefusedForSpace(); still {
		t.Fatalf("the volume recovered and the device still reports %s — the operator is being "+
			"told to stop a volume that is writing", ref)
	}
	// A cleared latch over a device that is not really serving would satisfy the line
	// above, so the bytes are read back.
	got := make([]byte, 512)
	if _, err := r.dev.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytesEqual(got, pattern(0x33, 512)) {
		t.Fatalf("the write after the trim is not readable: %x…", got[:8])
	}
}

// The device's own ENOSPC is clearable too, and by the same kind of evidence: an append
// the device took. wal.Degraded already says exactly that and clears itself on one, so
// this latch asks it rather than guessing — and an operator who grows the disk stops
// being told the disk is full without anyone having to restart anything.
func TestGivingTheDeviceRoomEndsTheDeviceRefusal(t *testing.T) {
	r := newRig(t, withLimits(wal.Limits{}), withDiskCap(16<<10))

	err := writeUntilRefused(t, r.dev, distinct)
	if !errors.Is(err, blockdev.ErrDeviceFull) {
		t.Fatalf("the guest was refused with %v, not a full device", err)
	}
	if ref, _ := r.dev.RefusedForSpace(); ref.Reason != blockdev.ReasonDeviceENOSPC {
		t.Fatalf("the device latched %q for an ENOSPC refusal", ref.Reason)
	}

	// The operator grows the device. That is not by itself evidence — only an append the
	// device takes is, which is why wal.Degraded is sticky across it too.
	r.disk.ClearENOSPC(r.root)
	if _, still := r.dev.RefusedForSpace(); !still {
		t.Fatal("room appearing on the device cleared the latch before anything proved the device would take a byte")
	}
	if _, err := r.dev.WriteAt(pattern(0x44, 512), 0); err != nil {
		t.Fatalf("the device was grown and the next write still failed: %v", err)
	}
	if ref, still := r.dev.RefusedForSpace(); still {
		t.Fatalf("the device is taking writes again and still reports %s", ref)
	}
}

// The half that must NOT clear. The volume's share is not reclaimed mid-session (§5.7):
// what gives it back is stopping the volume, which publishes its image and drops the WAL
// — and that is a new Device, not this one. So no append this device accepts is evidence
// of anything, and the latch has to survive one.
//
// It survives a real accepted append here, not a hypothetical: a DISCARD record is 104
// bytes against a share whose last refusal was of a 616-byte one, so it fits. That is the
// guest doing the thing that ends the *other* bound and getting nowhere, which is exactly
// the operator error the reason label exists to prevent.
func TestTheDeviceShareRefusalDoesNotClearWhileTheVolumeRuns(t *testing.T) {
	ctx := t.Context()
	r := newRig(t, withLimits(wal.Limits{MaxLocalBytes: 64 << 10}))

	if err := writeUntilRefused(t, r.dev, inPlace); !errors.Is(err, wal.ErrBackpressure) {
		t.Fatalf("filling the share failed with %v", err)
	}
	if ref, _ := r.dev.RefusedForSpace(); ref.Reason != blockdev.ReasonWALShare {
		t.Fatalf("the device latched %q for a share refusal", ref.Reason)
	}

	if err := r.dev.Flush(ctx); err != nil {
		t.Fatalf("FLUSH: %v", err)
	}
	if err := r.dev.Discard(0, 512); err != nil {
		t.Fatalf("no append was accepted after the share was exhausted (%v), so this test "+
			"proves nothing about a latch surviving one; re-size the fill", err)
	}
	ref, still := r.dev.RefusedForSpace()
	if !still {
		t.Fatal("an accepted DISCARD cleared the share refusal: nothing reclaims a volume's " +
			"share while it runs, so the operator would be told the volume recovered and it has not")
	}
	if ref.Reason != blockdev.ReasonWALShare {
		t.Fatalf("the reason changed to %q after a DISCARD", ref.Reason)
	}
	if _, err := r.dev.WriteAt(pattern(0x55, 512), 0); !errors.Is(err, wal.ErrBackpressure) {
		t.Fatalf("the bound the latch describes is gone (next write: %v) — then the latch was right to clear", err)
	}
}

// Precedence, and it is the difference between advice and misdirection. A volume that has
// crossed its read view's bound and then also exhausted its share is refusing for the
// share: that is the bound wal checks first and the one the guest cannot escape. Keeping
// the earlier, clearable reason would tell the operator to fstrim and wait for a recovery
// that cannot arrive — and it could never be corrected afterwards, because the only thing
// that clears view_memory is a successful write and the share has made those impossible.
//
// The reverse cannot happen and needs no test: once the share is gone, wal refuses every
// WRITE on it before the view bound is ever consulted.
func TestTheBoundNothingClearsReplacesTheOneAGuestCanClear(t *testing.T) {
	r := newRig(t, withLimits(wal.Limits{MaxViewBytes: 32 << 10, MaxLocalBytes: 96 << 10}))

	// 512 bytes of view and 616 of device per write, so the memory bound arrives first.
	if err := writeUntilRefused(t, r.dev, distinct); !errors.Is(err, wal.ErrViewBound) {
		t.Fatalf("the first bound reached was %v, not the read view's", err)
	}
	if ref, _ := r.dev.RefusedForSpace(); ref.Reason != blockdev.ReasonViewMemory {
		t.Fatalf("the device latched %q first", ref.Reason)
	}

	// The guest trims the wrong half of the device: DISCARDs of ranges it never wrote
	// free no view at all, and each one still spends a record against the share. This is
	// how a volume ends up refusing for both bounds at once.
	var err error
	for i := range int64(8192) {
		if err = r.dev.Discard(capacity/2+(i*512)%(capacity/2), 512); err != nil {
			break
		}
	}
	if !errors.Is(err, wal.ErrBackpressure) || errors.Is(err, wal.ErrViewBound) {
		t.Fatalf("the share was not what refused the DISCARD: %v", err)
	}
	ref, _ := r.dev.RefusedForSpace()
	if ref.Reason != blockdev.ReasonWALShare {
		t.Fatalf("the device reports %q while the bound refusing it is the share: the operator "+
			"is told to fstrim a volume whose only way out is to stop", ref.Reason)
	}
}
