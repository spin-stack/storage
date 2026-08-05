package agent_test

import (
	"crypto/rand"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// The four §26.2 metrics a Log owns reach an instrument only if somebody calls
// SetRecorder, and until 2026-08-04 nobody did: `wal.Log.SetRecorder` had no production
// caller at all, so the watermarks and `wal_out_of_space` could not be recorded even once
// a collector existed. An exporter would have shipped an empty series set and looked like
// working observability.
//
// The assertion is on the collected series rather than on the manager holding a non-nil
// Recorder, because a Recorder that reaches no Log is exactly the defect: the field would
// be set and the metrics still absent.
func TestAServedVolumeRecordsItsWALMetrics(t *testing.T) {
	p, err := obs.NewTestProvider("agent-wal-telemetry")
	if err != nil {
		t.Fatal(err)
	}
	f := newListenerFactory()
	m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin", SocketDir: "/run/spin",
		Budget: testBudget(),
	}, agent.VolumeManagerDeps{
		Clock:    sim.NewClock(time.Unix(1_700_000_000, 0).UTC()),
		Disk:     sim.NewDisk(),
		Listen:   f.listen,
		Mapper:   unusedMapper{},
		EventFD:  unusedEventFD,
		Store:    sim.NewObjectStore(),
		Rand:     rand.Reader,
		Recorder: obs.NewRecorder(p.Metrics),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Close(t.Context()) }()

	d := desiredVolume(t, 1)
	if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{d}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	// A guest write and a FLUSH: the watermarks move where they are recorded, which is
	// at the moments they change rather than from a background poller.
	writeOneBlock(t, m, d.GetVolumeId())
	dev, ok := m.Device(d.GetVolumeId())
	if !ok {
		t.Fatal("the volume is not being served")
	}
	if err := dev.Flush(t.Context()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	collected, err := p.CollectedMetrics(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// The read view's cost travels with the watermarks, on the durable step: cow.Cost
	// had no production caller and these three series had no producer when they landed,
	// which is the shape CLAUDE.md calls a liability — and the shape the chunk-addressing
	// spec spends a section condemning, in the same wave.
	for _, want := range []string{
		"wal_local_sequence", "wal_durable_sequence", "wal_unflushed_bytes",
		"read_view_bytes", "read_view_extents", "read_view_layers",
	} {
		if !collected[want] {
			t.Errorf("a served volume recorded no %s; collected: %v", want, collected)
		}
	}
}
