package cow

import (
	"bytes"
	"fmt"
	"testing"

	"pgregory.net/rapid"

	"github.com/spin-stack/storage/internal/obs"
)

// liveBytes is maintained incrementally (insert adds, removeRange subtracts each
// overlap) so that Cost is O(layers) instead of O(extents). An incremental counter is
// only worth having if it is *right*, and the only honest statement of right is that it
// equals the fold it replaced, after any sequence of operations — including the awkward
// ones: a write that splits an extent in two, a clear that trims one end, a write that
// exactly covers three older extents.
//
// This is the same shape as §25.2's serialize/replay property, applied to a counter
// rather than a format: arbitrary input, compare against the definition.
func TestTheMaintainedByteCountEqualsAFoldOverTheExtents(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		const size = 64
		m := NewIntervalMapOver(NewIntervalMap())

		n := rapid.IntRange(0, 40).Draw(t, "ops")
		for range n {
			off := rapid.Uint64Range(0, size-1).Draw(t, "off")
			length := rapid.Uint64Range(1, size-off).Draw(t, "len")
			if rapid.Bool().Draw(t, "write") {
				m.Overwrite(off, bytes.Repeat([]byte{7}, int(length)))
			} else {
				m.Clear(off, length)
			}

			var fold int64
			for _, e := range m.extents {
				fold += int64(len(e.data))
			}
			if m.liveBytes != fold {
				t.Fatalf("maintained %d live bytes, a fold over the extents says %d", m.liveBytes, fold)
			}
		}
	})
}

// The three numbers answer three different questions, and the volume that separates them
// is the snapshotted one: Freeze adds a layer and no bytes. A test that only ever built
// one map would pass with Cost hard-coding Layers: 1.
func TestCostFollowsTheWholeChain(t *testing.T) {
	base := NewIntervalMap()
	base.Overwrite(0, []byte("AAAA"))

	mid := NewIntervalMapOver(base)
	mid.Overwrite(4, []byte("BB"))

	top := NewIntervalMapOver(mid)
	top.Clear(0, 2)

	got := top.Cost()
	want := Cost{Bytes: 6, Extents: 2, Layers: 3, Cleared: 1}
	if got != want {
		t.Fatalf("Cost() = %+v, want %+v", got, want)
	}
}

const (
	blockSize = 4096
	// A volume snapshotted every few minutes for a couple of hours. Nothing in the
	// system bounds this number today, which is the finding below.
	snapshots = 64
)

// freeze is what wal.Log.Freeze does to the read view: seal the current map and keep
// writing into a fresh layer over it. Modelled here rather than driven through the WAL
// because this is the structure's cost, not the WAL's, and the WAL adds a file.
func freeze(m *IntervalMap) *IntervalMap { return NewIntervalMapOver(m) }

// reachable is the bytes a *reader* can still see: everything Ranges reports. It is
// O(extents) and deliberately not part of Cost — see the finding below for what it buys
// and why it is not bought at the flush cadence.
func reachable(m *IntervalMap) uint64 {
	var n uint64
	for _, r := range m.Ranges() {
		n += r.Length
	}
	return n
}

// **The finding this item exists to produce, and it is a defect, not a reassurance.**
//
// A volume that rewrites the same block between snapshots holds one copy of that block
// per snapshot, for ever. Freeze seals the layer and installs a new one over it; nothing
// ever collapses the chain, drops a layer whose every extent has been superseded, or
// releases a base once its image has been published. So the read view of a volume with
// 4 KiB of live data, snapshotted 64 times, holds 65 copies of it and a 65-deep read
// path — and `Read` walks every layer and scans every extent in each, so the guest's
// read latency grows linearly with the number of snapshots ever taken.
//
// The numbers this prints are the ones in track E's log. The assertions are exact
// because the point is to make the amplification a measured fact rather than a worry:
// when the chain learns to collapse, this test fails and its numbers are what the fix is
// compared against.
func TestASnapshottedVolumeHoldsOneCopyPerSnapshot(t *testing.T) {
	block := bytes.Repeat([]byte{0xAB}, blockSize)

	view := NewIntervalMap()
	view.Overwrite(0, block)
	for range snapshots {
		view = freeze(view)
		view.Overwrite(0, block) // the same block again: the guest's hot metadata block
	}

	c := view.Cost()
	live := reachable(view)
	t.Logf("after %d snapshots of a volume holding %d bytes of live data: "+
		"Bytes=%d Extents=%d Layers=%d (amplification %.0fx)",
		snapshots, live, c.Bytes, c.Extents, c.Layers, float64(c.Bytes)/float64(live))

	if live != blockSize {
		t.Fatalf("the volume holds %d reachable bytes, but only one block was ever written", live)
	}
	if want := int64(blockSize * (snapshots + 1)); c.Bytes != want {
		t.Fatalf("the chain holds %d bytes for one live block; expected the unbounded %d", c.Bytes, want)
	}
	if c.Layers != snapshots+1 {
		t.Fatalf("a read crosses %d layers after %d snapshots, expected %d", c.Layers, snapshots, snapshots+1)
	}
}

// The other half of the finding: layering itself is cheap. A volume that writes new
// blocks pays for its data and nothing else — no per-layer overhead, no copy of what it
// did not rewrite. So the gauge is not watching a structure that is doomed either way;
// it is separating the volume whose view is its working set from the volume whose view
// is its history.
func TestALayeredVolumeThatDoesNotRewriteCostsItsDataAndNoMore(t *testing.T) {
	block := bytes.Repeat([]byte{0xCD}, blockSize)

	view := NewIntervalMap()
	view.Overwrite(0, block)
	for i := range uint64(snapshots) {
		view = freeze(view)
		view.Overwrite((i+1)*blockSize, block)
	}

	c := view.Cost()
	live := reachable(view)
	t.Logf("after %d snapshots of a volume writing a new block each time: "+
		"Bytes=%d live=%d Extents=%d Layers=%d", snapshots, c.Bytes, live, c.Extents, c.Layers)

	if c.Bytes != int64(live) {
		t.Fatalf("the chain holds %d bytes for %d reachable: layering amplified a volume that never rewrote", c.Bytes, live)
	}
}

// A sparse image is the case the package doc promises costs nothing: memory is
// proportional to the working set, not to the volume. 4 MiB written into a 64 GiB
// volume must cost 4 MiB, and the offsets must not appear in the number at all.
func TestASparseImageCostsItsWrittenSetAndNotItsSize(t *testing.T) {
	const (
		volumeSize = 64 << 30
		written    = 1024
	)
	block := bytes.Repeat([]byte{0xEF}, blockSize)

	m := NewIntervalMap()
	for i := range uint64(written) {
		m.Overwrite(i*(volumeSize/written), block) // spread across the whole address space
	}

	c := m.Cost()
	t.Logf("%d blocks of %d bytes spread over a %d-byte volume: Bytes=%d Extents=%d Layers=%d",
		written, blockSize, volumeSize, c.Bytes, c.Extents, c.Layers)

	if want := int64(written * blockSize); c.Bytes != want {
		t.Fatalf("a sparse image of %d written bytes costs %d", want, c.Bytes)
	}
	if c.Extents != written {
		t.Fatalf("%d extents for %d separated writes", c.Extents, written)
	}
}

// Why `read_view_layers` is a series and not a curiosity: **depth is paid on every guest
// read, and it is linear.** Read paints the base first and lets each layer overwrite it,
// so a block that N layers have each rewritten is copied N times before the guest sees
// the newest one. The two shapes here are the two tests above, timed:
//
//   - "rewritten" — every layer holds the block being read (the snapshot-of-a-hot-block
//     case). Cost is a 4 KiB memcpy per layer, and at 256 layers a 4 KiB read spends
//     tens of microseconds of CPU, which is the same order as the NVMe it is meant to be
//     saving.
//   - "elsewhere" — every layer wrote somewhere else, so nothing is copied and the walk
//     is pure overhead. Still linear, just with a small constant.
//
// The absolute numbers belong to the machine that ran it; the slope belongs to the code.
// This is the repository's first benchmark and it earns its place by being the only
// executable form of the claim — the tests above assert the chain is 65 deep, and only
// this says what being 65 deep costs a guest. Run it with
// `go test ./internal/cow/ -run XXX -bench ReadAtDepth`.
func BenchmarkReadAtDepth(b *testing.B) {
	block := bytes.Repeat([]byte{1}, blockSize)
	shapes := map[string]func(d int) uint64{
		"rewritten": func(int) uint64 { return 0 },
		"elsewhere": func(d int) uint64 { return uint64(d+1) * blockSize },
	}
	for name, offsetOf := range shapes {
		for _, depth := range []int{1, 8, 64, 256} {
			view := NewIntervalMap()
			for d := range depth {
				if d > 0 {
					view = freeze(view)
				}
				view.Overwrite(offsetOf(d), block)
			}
			buf := make([]byte, blockSize)
			b.Run(fmt.Sprintf("%s/layers=%d", name, depth), func(b *testing.B) {
				for b.Loop() {
					view.Read(0, buf)
				}
			})
		}
	}
}

// The §26.2 series exist and carry the numbers a read view produces. The recording
// expression here is the one the WAL will hold (`internal/wal` is another track's file
// this wave), so what is proven is
// everything except the call site: that the three names are registered, that a Recorder
// therefore *keeps* the samples instead of dropping them, and that the values collected
// are the chain's and not the top layer's.
//
// The assertion is on the collected series, never on Cost's return value: obs.Recorder
// silently drops an unregistered name (that is what the fixed catalog is for), so a
// missing Catalog entry looks exactly like working code from the call site.
func TestTheReadViewCostReachesTheCatalogSeries(t *testing.T) {
	ctx := t.Context()
	p, err := obs.NewTestProvider("cow-read-view")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Shutdown(ctx) }()
	rec := obs.NewRecorder(p.Metrics)

	view := NewIntervalMap()
	view.Overwrite(0, bytes.Repeat([]byte{1}, blockSize))
	view = freeze(view)
	view.Overwrite(blockSize, bytes.Repeat([]byte{2}, blockSize))

	c := view.Cost()
	vol := obs.String("volume", "vol-1")
	rec.Gauge(ctx, "read_view_bytes", float64(c.Bytes), vol)
	rec.Gauge(ctx, "read_view_extents", float64(c.Extents), vol)
	rec.Gauge(ctx, "read_view_layers", float64(c.Layers), vol)

	values, err := p.GaugeValues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]float64{
		"read_view_bytes":   2 * blockSize,
		"read_view_extents": 2,
		"read_view_layers":  2,
	} {
		got, ok := values[name]
		if !ok {
			t.Errorf("nothing collected for %s; the catalog carries %v", name, values)
			continue
		}
		if got != want {
			t.Errorf("%s collected %v, want %v", name, got, want)
		}
	}
}
