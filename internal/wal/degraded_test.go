package wal_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/lease"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
)

// TEST-GAPS (Open, wal-durability): "wal.Log has no out-of-space state".
//
// The operational sequence is the one `disk-fills-under-sustained-write-with-s3-down`
// already drives: S3 has been unreachable long enough that nothing closed the remote
// gap and no checkpoint authorised a truncation, so the WAL device fills. Everything
// observable about that is already asserted — backpressure first, no phantom
// sequence, sticky failure, clean replay. What is missing is the diagnosis: every
// WRITE after the device fills returns a raw device error that a caller cannot tell
// apart from a transient I/O fault, while the volume reports itself un-fenced and
// `durable_sequence` sits still. An operator paging on "writes are failing" has no
// signal saying *the device is full*, which is the one condition whose remedy
// (truncate, grow, restore S3) is different from every other I/O error's.
//
// The rule: a full device is a distinct, sticky, typed state, separate from fencing,
// and it is visible as a metric.

const (
	enospcPayload  = 200  // a 304-byte record: 104 bytes of header + payload
	enospcCapacity = 1000 // three records fit; the fourth tears
)

// fillTheDevice writes until the device refuses, returning the log and how many
// records it accepted.
func fillTheDevice(t *testing.T, l *wal.Log) int {
	t.Helper()
	for i := range 20 {
		if _, err := l.Write(uint64(i)*4096, make([]byte, enospcPayload), 0); err != nil {
			if i == 0 {
				t.Fatalf("the device refused the very first record: %v", err)
			}
			return i
		}
	}
	t.Fatal("the capped device never refused a write")
	return 0
}

func enospcLog(t *testing.T, name string) (*wal.Log, *sim.Disk) {
	t.Helper()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	f, err := d.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	d.InjectENOSPC(name, enospcCapacity)
	return wal.NewLog(f, clk, [16]byte{9}, 1, wal.Limits{MaxUnflushedBytes: 1 << 20}), d
}

// TestAFullDeviceIsAStickyTypedState is the finding itself: the caller can name the
// failure, it does not evaporate on the next call, and it clears only once the device
// has proven it can take bytes again.
func TestAFullDeviceIsAStickyTypedState(t *testing.T) {
	const name = "wal/full.wal"
	l, d := enospcLog(t, name)

	if got := l.Degraded(); got != wal.DegradedNone {
		t.Fatalf("a fresh log reports %q, want %q", got, wal.DegradedNone)
	}
	accepted := fillTheDevice(t, l)

	if got := l.Degraded(); got != wal.DegradedOutOfSpace {
		t.Fatalf("after the device filled the log reports %q, want %q", got, wal.DegradedOutOfSpace)
	}
	// Sticky: the device does not heal itself, so neither does the state. A caller
	// polling Degraded() between two failing writes must not read "healthy".
	for i := range 3 {
		if _, err := l.Write(uint64(1<<20)+uint64(i), make([]byte, enospcPayload), 0); err == nil {
			t.Fatalf("write %d succeeded on a full device", i)
		}
		if got := l.Degraded(); got != wal.DegradedOutOfSpace {
			t.Fatalf("after failing write %d the log reports %q, want %q", i, got, wal.DegradedOutOfSpace)
		}
	}

	// Space comes back (a checkpoint authorised a truncation, or an operator grew the
	// device). Only an append the device actually took proves that, so that is what
	// clears the state.
	d.ClearENOSPC(name)
	if got := l.Degraded(); got != wal.DegradedOutOfSpace {
		t.Fatalf("reclaiming space is not by itself proof; log reports %q", got)
	}
	if _, err := l.Write(1<<21, make([]byte, enospcPayload), 0); err != nil {
		t.Fatalf("write after space was reclaimed: %v", err)
	}
	if got := l.Degraded(); got != wal.DegradedNone {
		t.Fatalf("after a successful append the log still reports %q, want %q", got, wal.DegradedNone)
	}
	if l.Watermarks().Local != uint64(accepted+1) {
		t.Fatalf("local watermark = %d, want %d", l.Watermarks().Local, accepted+1)
	}
}

// TestOutOfSpaceDoesNotFenceAndFencingIsNotOutOfSpace pins the orthogonality the
// finding asks to be decided: fencing is about the lease (cluster-wide authority to
// write at all, §16), degradation is about the device (local, and recoverable
// without the Control Plane). Conflating them would either hand a volume to another
// host because a disk filled, or leave a full device looking healthy because the
// lease is fine.
func TestOutOfSpaceDoesNotFenceAndFencingIsNotOutOfSpace(t *testing.T) {
	l, _ := enospcLog(t, "wal/orthogonal.wal")
	fillTheDevice(t, l)
	if l.Fenced() {
		t.Fatal("a full device self-fenced the log; only a lease failure may do that (§16)")
	}

	// The mirror image: a self-fenced log on a healthy device is fenced, not degraded.
	ctx := context.Background()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	f, _ := d.Create("wal/fenced.wal")
	lm := lease.NewManager(clk, 10*time.Second)
	lm.Grant()
	vol := [16]byte{10}
	fl := wal.NewLog(f, clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	fl.EnableRemote(wal.NewBatcher(clk, vol, 1, 0, wal.DefaultBatchConfig()), wal.NewUploader(sim.NewObjectStore(), 5), lm)
	if _, err := fl.Write(0, []byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	clk.Advance(30 * time.Second) // the lease expires
	if err := fl.Flush(ctx); !errors.Is(err, wal.ErrSelfFenced) {
		t.Fatalf("flush after the lease expired: %v, want ErrSelfFenced", err)
	}
	if !fl.Fenced() {
		t.Fatal("the log did not self-fence")
	}
	if got := fl.Degraded(); got != wal.DegradedNone {
		t.Fatalf("a self-fenced log on a healthy device reports %q, want %q", got, wal.DegradedNone)
	}
}

// TestATransientAppendErrorIsNotOutOfSpace is the discrimination the finding names:
// "a caller cannot distinguish a full device from a transient I/O error". A state
// that latched on every append failure would be exactly as useless as none.
func TestATransientAppendErrorIsNotOutOfSpace(t *testing.T) {
	const name = "wal/torn.wal"
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	f, err := d.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	l := wal.NewLog(f, clk, [16]byte{11}, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	if _, err := l.Write(0, make([]byte, enospcPayload), 0); err != nil {
		t.Fatal(err)
	}
	d.InjectShortAppend(name, 40) // a torn write, not a full device
	if _, err := l.Write(4096, make([]byte, enospcPayload), 0); err == nil {
		t.Fatal("a short append must be reported to the caller")
	}
	if got := l.Degraded(); got != wal.DegradedNone {
		t.Fatalf("a torn append reported %q; only a full device is %q", got, wal.DegradedOutOfSpace)
	}
}

// TestBackpressureIsNotOutOfSpace: §5.7 backpressure is the WAL refusing a write the
// device would still have taken. Reporting the device as full there would make the
// gauge fire on every healthy volume that reached its configured bound.
func TestBackpressureIsNotOutOfSpace(t *testing.T) {
	l, _, _ := newLog(t, wal.Limits{MaxUnflushedBytes: 200})
	if _, err := l.Write(0, make([]byte, 32), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Write(64, make([]byte, 64), 0); !errors.Is(err, wal.ErrBackpressure) {
		t.Fatalf("want ErrBackpressure, got %v", err)
	}
	if got := l.Degraded(); got != wal.DegradedNone {
		t.Fatalf("backpressure reported %q, want %q", got, wal.DegradedNone)
	}
}

// TestOutOfSpaceIsRecordedAsAGauge: the finding asks for "a metric that says it".
// The value matters, not the name — a gauge wired backwards is worse than none.
func TestOutOfSpaceIsRecordedAsAGauge(t *testing.T) {
	ctx := context.Background()
	p, err := obs.NewTestProvider("wal-degraded")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(ctx) }()

	const name = "wal/metric.wal"
	l, d := enospcLog(t, name)
	l.SetRecorder(obs.NewRecorder(p.Metrics), "vol-9")

	fillTheDevice(t, l)
	values, err := p.GaugeValues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := values["wal_out_of_space"]; !ok || got != 1 {
		t.Fatalf("wal_out_of_space = %v (present=%t) on a full device, want 1", got, ok)
	}

	d.ClearENOSPC(name)
	if _, err := l.Write(1<<21, make([]byte, enospcPayload), 0); err != nil {
		t.Fatal(err)
	}
	values, err = p.GaugeValues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := values["wal_out_of_space"]; got != 0 {
		t.Fatalf("wal_out_of_space = %v after space was reclaimed, want 0", got)
	}
}

// TestOutOfSpaceClassifierIsInjectable: `internal/simio/disk` carries no typed
// no-space sentinel, so `wal` cannot `errors.Is` its way to the answer for both the
// real disk (syscall.ENOSPC, which depguard forbids outside simio) and the simulated
// one (sim.ErrNoSpace, which production code must not import). The default is a
// portable classifier over the message both share; a caller that *does* hold a typed
// error replaces it.
func TestOutOfSpaceClassifierIsInjectable(t *testing.T) {
	const name = "wal/classifier.wal"
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	d := sim.NewDisk()
	f, err := d.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	l := wal.NewLog(f, clk, [16]byte{12}, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	// A backend whose full-device error says nothing recognisable: the injected
	// classifier is the only thing that can name it.
	l.SetOutOfSpace(func(err error) bool { return errors.Is(err, sim.ErrShortWrite) })
	d.InjectShortAppend(name, 8)
	if _, err := l.Write(0, make([]byte, enospcPayload), 0); err == nil {
		t.Fatal("the short append must be reported")
	}
	if got := l.Degraded(); got != wal.DegradedOutOfSpace {
		t.Fatalf("the injected classifier was not consulted: log reports %q", got)
	}

	// And the default recognises both the simulated and the real device error.
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"the simulated device", sim.ErrNoSpace, true},
		{"a real ENOSPC as the kernel words it", errors.New("write /var/lib/spin/wal: no space left on device"), true},
		{"a torn write", sim.ErrShortWrite, false},
		{"nothing at all", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := wal.DefaultOutOfSpace(tc.err); got != tc.want {
				t.Fatalf("DefaultOutOfSpace(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

// TestNilClassifierRestoresTheDefault: SetOutOfSpace(nil) must restore
// DefaultOutOfSpace, not leave the log unable to recognise a full device at all.
func TestNilClassifierRestoresTheDefault(t *testing.T) {
	const name = "wal/nil-classifier.wal"
	l, _ := enospcLog(t, name)
	l.SetOutOfSpace(func(error) bool { return false })
	l.SetOutOfSpace(nil)
	fillTheDevice(t, l)
	if got := l.Degraded(); got != wal.DegradedOutOfSpace {
		t.Fatalf("after SetOutOfSpace(nil) the log reports %q, want %q", got, wal.DegradedOutOfSpace)
	}
}

// TestDefaultOutOfSpaceRecognisesBothDisks: the classifier is what turns a full device
// into a state an operator can act on, so it has to hold for the disk production runs
// on and the one every proof runs on. Identity, not wording — a message match was the
// weakest line in this package until disk.ErrNoSpace existed.
func TestDefaultOutOfSpaceRecognisesBothDisks(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil is not a full device", err: nil, want: false},
		{name: "the shared sentinel", err: disk.ErrNoSpace, want: true},
		{name: "the simulator's injected ENOSPC", err: sim.ErrNoSpace, want: true},
		{name: "wrapped by a caller", err: fmt.Errorf("append: %w", sim.ErrNoSpace), want: true},
		{name: "the operating system's error unwrapped", err: errors.New("write /wal: no space left on device"), want: true},
		{name: "an unrelated I/O error", err: errors.New("input/output error"), want: false},
		{name: "a torn write is not a full device", err: sim.ErrShortWrite, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := wal.DefaultOutOfSpace(tc.err); got != tc.want {
				t.Fatalf("DefaultOutOfSpace(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
