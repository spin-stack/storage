package blockdev_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/blockdev"
	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/simio/disk"
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

func (s *countingStore) failEveryPut(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = err
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
func withRoot(r string) rigOption       { return func(c *rigConfig) { c.root = r } }
func localOnly() rigOption              { return func(c *rigConfig) { c.remote = false } }
func withoutLease() rigOption           { return func(c *rigConfig) { c.leaseOn = false } }
func withCapacity(n int64) rigOption    { return func(c *rigConfig) { c.cap = n } }
func withENOSPC(bytes int64) rigOption {
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
	if cfg.remote {
		var lc wal.LeaseChecker
		if cfg.leaseOn {
			lc = lm
		}
		l.EnableRemote(wal.NewBatcher(clk, volume, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(store, 3), lc)
	}

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

// TestWriteAtIssuesNoPut is INV-18 at the seam a guest actually reaches: the store
// receives nothing at all on the write path, and durability arrives only with FLUSH.
// The second half proves the counter can move, so "0 PUTs" is evidence and not an
// artefact of a store nobody wired up.
func TestWriteAtIssuesNoPut(t *testing.T) {
	ctx := t.Context()
	r := newRig(t)

	for i := range 8 {
		if _, err := r.dev.WriteAt(pattern(byte(i), vhost.SectorSize), int64(i)*vhost.SectorSize); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if got := r.store.Puts(); got != 0 {
		t.Fatalf("the write path issued %d PUT(s); a normal WRITE must not PUT (§5.3, INV-18)", got)
	}
	if w := r.log.Watermarks(); w.Durable != 0 || w.Local != 8 {
		t.Fatalf("watermarks after 8 writes = %+v, want local 8 / durable 0", w)
	}

	if err := r.dev.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := r.store.Puts(); got == 0 {
		t.Fatal("FLUSH issued no PUT, so the 0 above says nothing about the write path")
	}
	if w := r.log.Watermarks(); w.Durable != w.Local {
		t.Fatalf("after a successful FLUSH durable = %d, local = %d", w.Durable, w.Local)
	}
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

// TestFlushWithoutAValidLeaseFailsAndDoesNotAck is decision (2) and the golden rule:
// Flush is the §14.4 ACK path, not an fsync. If the lease cannot be confirmed the
// guest must see an error — never a success — and durable must not move.
func TestFlushWithoutAValidLeaseFailsAndDoesNotAck(t *testing.T) {
	ctx := t.Context()

	tests := []struct {
		name string
		opts []rigOption
		// arm brings the rig to the state under test and returns the sentinel the
		// guest-facing FLUSH must carry.
		arm  func(t *testing.T, r *rig) error
		want error
	}{
		{
			name: "no lease checker at all fails closed",
			opts: []rigOption{withoutLease(), withRoot("wal-nolease")},
			arm:  func(*testing.T, *rig) error { return nil },
			want: wal.ErrNoLease,
		},
		{
			name: "an expired lease self-fences instead of ACKing",
			opts: []rigOption{withRoot("wal-expired")},
			arm: func(_ *testing.T, r *rig) error {
				r.clk.Advance(30 * time.Second)
				return nil
			},
			want: wal.ErrSelfFenced,
		},
		{
			name: "a failing object store is not a durable FLUSH",
			opts: []rigOption{withRoot("wal-s3down")},
			arm: func(_ *testing.T, r *rig) error {
				r.store.failEveryPut(errors.New("the object store is unreachable"))
				return nil
			},
			want: nil, // any error will do; what matters is that it is not success
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, tc.opts...)
			if _, err := r.dev.WriteAt(pattern(2, vhost.SectorSize), 0); err != nil {
				t.Fatalf("the write itself must succeed: %v", err)
			}
			if err := tc.arm(t, r); err != nil {
				t.Fatal(err)
			}

			err := r.dev.Flush(ctx)
			if err == nil {
				t.Fatal("FLUSH reported success; nothing may ACK durability the object store cannot produce")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("FLUSH error = %v, want one wrapping %v", err, tc.want)
			}
			if w := r.log.Watermarks(); w.Durable != 0 {
				t.Fatalf("a refused FLUSH advanced durable to %d", w.Durable)
			}
		})
	}
}

// TestASelfFencedDeviceKeepsRefusingTheFlush: fencing is not a transient fault. Once
// the log has self-fenced every later FLUSH fails immediately — the guest gets an
// error at once rather than an ACK, and rather than a wait.
func TestASelfFencedDeviceKeepsRefusingTheFlush(t *testing.T) {
	ctx := t.Context()
	r := newRig(t)
	if _, err := r.dev.WriteAt(pattern(3, vhost.SectorSize), 0); err != nil {
		t.Fatal(err)
	}
	r.clk.Advance(30 * time.Second)
	if err := r.dev.Flush(ctx); !errors.Is(err, wal.ErrSelfFenced) {
		t.Fatalf("first FLUSH after the lease expired: %v, want ErrSelfFenced", err)
	}
	// Even with the lease handed back, the log stays fenced: the Control Plane, not
	// the host, decides who writes (§12.2, §16).
	r.lease.Grant()
	for i := range 3 {
		if err := r.dev.Flush(ctx); !errors.Is(err, wal.ErrSelfFenced) {
			t.Fatalf("FLUSH %d on a fenced device: %v, want ErrSelfFenced", i, err)
		}
	}
	if w := r.log.Watermarks(); w.Durable != 0 {
		t.Fatalf("a fenced device advanced durable to %d", w.Durable)
	}
}

// TestEveryWALRefusalReachesTheGuestAsAnError is decision (4). virtio-blk's status
// byte has exactly three values — OK, IOERR, UNSUPP — so all three conditions below
// complete as VIRTIO_BLK_S_IOERR and the guest's block layer reports EIO; ENOSPC is
// not expressible on this wire (see the package doc). What this test pins is the part
// that *is* ours: each condition produces an error rather than a success, promptly,
// and carrying a sentinel an operator and the Agent can tell apart.
//
// The transport half of the claim — a non-nil Backend error becomes IOERR and never a
// dropped request — is TestBackendFailuresBecomeIOErrorsNotHangs in internal/vhost,
// and the whole path is exercised against a real guest in integration/vhost.
func TestEveryWALRefusalReachesTheGuestAsAnError(t *testing.T) {
	ctx := t.Context()

	tests := []struct {
		name string
		opts []rigOption
		// drive brings the device to the failure and returns the error the guest
		// would be shown.
		drive func(t *testing.T, r *rig) error
		want  error
		// alsoIs, when set, must match too: ErrDeviceFull is a classification laid
		// over the device's own error, not a replacement for it.
		alsoIs error
	}{
		{
			name: "backpressure: the unflushed backlog hit its bound (§5.7)",
			opts: []rigOption{withRoot("wal-bp"), withLimits(wal.Limits{MaxUnflushedBytes: 700})},
			drive: func(_ *testing.T, r *rig) error {
				// One 512-byte record (104-byte header + payload) fits; the second
				// does not.
				if _, err := r.dev.WriteAt(pattern(4, vhost.SectorSize), 0); err != nil {
					return fmt.Errorf("the first write should fit: %w", err)
				}
				_, err := r.dev.WriteAt(pattern(5, vhost.SectorSize), vhost.SectorSize)
				return err
			},
			want: wal.ErrBackpressure,
		},
		{
			name: "out of space: the local device is full",
			opts: []rigOption{withRoot("wal-enospc"), withENOSPC(1040)},
			drive: func(t *testing.T, r *rig) error {
				var err error
				for i := range 20 {
					if _, err = r.dev.WriteAt(make([]byte, 200), int64(i)*vhost.SectorSize); err != nil {
						break
					}
				}
				if err == nil {
					t.Fatal("the capped device never refused a write")
				}
				if got := r.log.Degraded(); got != wal.DegradedOutOfSpace {
					t.Fatalf("the log reports %q, want %q", got, wal.DegradedOutOfSpace)
				}
				return err
			},
			want: blockdev.ErrDeviceFull,
			// The classification is laid *over* the device's own error, not
			// substituted for it: whoever needs the underlying cause still has it.
			alsoIs: disk.ErrNoSpace,
		},
		{
			name: "self-fenced: this host has lost the authority to write",
			opts: []rigOption{withRoot("wal-fenced")},
			drive: func(_ *testing.T, r *rig) error {
				if _, err := r.dev.WriteAt(pattern(6, vhost.SectorSize), 0); err != nil {
					return err
				}
				r.clk.Advance(30 * time.Second)
				return r.dev.Flush(ctx)
			},
			want: wal.ErrSelfFenced,
		},
		{
			name: "outside the device: a sector that does not exist",
			opts: []rigOption{withRoot("wal-range")},
			drive: func(_ *testing.T, r *rig) error {
				_, err := r.dev.WriteAt(pattern(7, vhost.SectorSize), capacity)
				return err
			},
			want: vhost.ErrOutOfRange,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, tc.opts...)
			err := tc.drive(t, r)
			if err == nil {
				t.Fatal("the device reported success where the WAL said no")
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want one wrapping %v", err, tc.want)
			}
			if tc.alsoIs != nil && !errors.Is(err, tc.alsoIs) {
				t.Fatalf("error = %v, want one also wrapping %v", err, tc.alsoIs)
			}
			// An operator reads this string in a log line next to a guest's EIO.
			// "the device is full" and "this host is fenced" have different remedies
			// and must not arrive as the same sentence.
			if !strings.HasPrefix(err.Error(), "blockdev: ") {
				t.Fatalf("error %q does not name the layer that refused", err)
			}
		})
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

// TestLocalDurabilityModeStillNeedsNoPutOnTheWritePath: §14.8's `local` mode changes
// what a FLUSH waits for, never what a WRITE does.
func TestLocalDurabilityModeStillNeedsNoPutOnTheWritePath(t *testing.T) {
	ctx := t.Context()
	r := newRig(t, localOnly())
	r.log.SetDurabilityMode(wal.ModeLocal)

	if _, err := r.dev.WriteAt(pattern(11, vhost.SectorSize), 0); err != nil {
		t.Fatal(err)
	}
	if got := r.store.Puts(); got != 0 {
		t.Fatalf("%d PUT(s) on the write path in local mode", got)
	}
	if err := r.dev.Flush(ctx); err != nil {
		t.Fatalf("a local-mode FLUSH ACKs on fdatasync: %v", err)
	}
	if got := r.store.Puts(); got != 0 {
		t.Fatalf("a local-mode FLUSH issued %d PUT(s) synchronously", got)
	}
}

// TestConcurrentRequestsDoNotRaceTheLog: wal.Log is not safe for concurrent use, and
// vhost.Device serves one queue from one goroutine — but the Backend contract does
// not say so, and the device is what stands between the two. Run under -race.
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
