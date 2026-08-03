package blockdev_test

import (
	"context"
	"errors"
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
