package cow

import (
	"bytes"
	"runtime"
	"testing"
)

// A read view that reports its size is only useful if the number is the memory it
// actually holds, and until this test the two could differ by three orders of magnitude.
//
// `removeRange` builds the remainders of a trimmed extent by re-slicing the original
// payload — `x.data[:s-x.start]` and `x.data[e-x.start:]`. A slice keeps its whole
// backing array alive, so an extent whose middle has been overwritten or discarded still
// pins every byte it was allocated with, while `liveBytes` (and therefore `Cost.Bytes`
// and the `read_view_bytes` gauge) counts only the bytes that survived. A guest that
// writes in megabytes and punches holes in kilobytes reports kilobytes and holds
// megabytes — which is exactly the shape that makes a bound on Cost.Bytes a bound on
// nothing.
//
// The assertion is on the process heap, not on Cost: Cost is the number under suspicion.
// The margin is wide on purpose (128 MiB written, 32 MiB allowed, ~64 KiB reported) so
// the test is about the retention and not about allocator noise.
func TestTrimmingAnExtentReleasesWhatItNoLongerHolds(t *testing.T) {
	const (
		extentSize = 4 << 20
		count      = 32 // 128 MiB written
		keep       = 512
	)

	payload := bytes.Repeat([]byte{0xAB}, extentSize)
	m := NewIntervalMap()

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for i := range count {
		m.Overwrite(uint64(i)*extentSize, payload)
	}
	// Punch the middle out of every extent: `keep` bytes survive at each end, and the
	// 4 MiB allocation behind them is what must not survive with them.
	for i := range count {
		m.Clear(uint64(i)*extentSize+keep, extentSize-2*keep)
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	held := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	runtime.KeepAlive(m)
	runtime.KeepAlive(payload)

	if got, want := m.Cost().Bytes, int64(count*2*keep); got != want {
		t.Fatalf("the view reports %d live bytes, want %d", got, want)
	}
	if held > 32<<20 {
		t.Fatalf("the view reports %d bytes and the heap holds %d: trimmed extents are still "+
			"pinning the payloads they were cut from", m.Cost().Bytes, held)
	}
}

// The bytes that survive a trim have to be the right ones. A copy that took the wrong
// window would satisfy the test above perfectly.
func TestATrimmedExtentStillReadsBack(t *testing.T) {
	m := NewIntervalMap()
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i)
	}
	m.Overwrite(0, payload)
	m.Clear(512, 3072) // leaves [0,512) and [3584,4096)

	buf := make([]byte, 4096)
	m.Read(0, buf)

	want := make([]byte, 4096)
	copy(want[:512], payload[:512])
	copy(want[3584:], payload[3584:])
	if !bytes.Equal(buf, want) {
		t.Fatalf("a trimmed extent read back wrong:\nhead %v\ntail %v", buf[:8], buf[3584:3592])
	}
}
