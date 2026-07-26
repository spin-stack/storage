package agent_test

import (
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/simio/sim"
)

func writeFile(t *testing.T, d *sim.Disk, name string, n int) {
	t.Helper()
	f, err := d.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Append(make([]byte, n)); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
}

// TestDiskUsageReportsTheDeviceNotAnEstimate is the gap this closes. The Agent used
// to sum the files under its own prefix and take the capacity from a flag, which
// reports the two numbers ADR-0013 needs least: it cannot see what anything else on
// the filesystem occupies, and it believes whoever started the process about how big
// the device is. Both are now the device's own answer.
//
// The file outside the Agent's prefix is the whole assertion: it is space the Agent
// cannot reclaim by any checkpoint or truncation, and a threshold that ignores it
// fires after the device is already full.
func TestDiskUsageReportsTheDeviceNotAnEstimate(t *testing.T) {
	d := sim.NewDisk()
	d.SetDeviceBudget(1 << 20)
	writeFile(t, d, "volumes/a/wal", 100)
	writeFile(t, d, "volumes/b/wal", 250)
	writeFile(t, d, "somebody-elses/file", 999)

	got, err := agent.NewDiskUsage(d).Usage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalBytes != 1<<20 {
		t.Errorf("TotalBytes = %d, want %d", got.TotalBytes, 1<<20)
	}
	if got.UsedBytes != 1349 {
		t.Errorf("UsedBytes = %d, want 1349 (everything on the device, not just ours)", got.UsedBytes)
	}
	if got.AvailBytes != 1<<20-1349 {
		t.Errorf("AvailBytes = %d, want %d", got.AvailBytes, 1<<20-1349)
	}
}

// TestDiskUsageFailureIsReported: a device that cannot be measured must fail the
// cycle, never report a zero that reads as an empty disk.
func TestDiskUsageFailureIsReported(t *testing.T) {
	if _, err := agent.NewDiskUsage(sim.NewDisk()).Usage(t.Context()); err == nil {
		t.Fatal("an unmeasurable device reported a usage")
	}
}

// TestVolumeSetIsOrderedAndCopied: every listing in this system is deterministic
// (INV-02), and a caller must not be able to mutate the set through the slice it
// was handed.
func TestVolumeSetIsOrderedAndCopied(t *testing.T) {
	s := agent.NewVolumeSet()
	s.Set(agent.VolumeStatus{VolumeID: "vol-c", RemoteGapBytes: 3})
	s.Set(agent.VolumeStatus{VolumeID: "vol-a", RemoteGapBytes: 1})
	s.Set(agent.VolumeStatus{VolumeID: "vol-b", RemoteGapBytes: 2})

	got, err := s.Volumes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, v := range got {
		ids = append(ids, v.VolumeID)
	}
	if strings.Join(ids, ",") != "vol-a,vol-b,vol-c" {
		t.Fatalf("not ordered by volume id: %v", ids)
	}

	got[0].VolumeID = "clobbered"
	again, err := s.Volumes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if again[0].VolumeID != "vol-a" {
		t.Fatal("Volumes handed out its own storage")
	}

	s.Remove("vol-b")
	again, _ = s.Volumes(t.Context())
	if len(again) != 2 {
		t.Fatalf("Remove left %d volumes, want 2", len(again))
	}
}

// TestConfigValidate: a misconfigured Agent must fail at startup, not report
// nonsense the Control Plane will act on.
func TestConfigValidate(t *testing.T) {
	valid := agent.Config{
		HostID:            "host-a",
		AgentVersion:      "0.1.0",
		MaxFormatVersion:  1,
		HeartbeatInterval: 5 * time.Second,
		RetryBackoff:      time.Second,
		LeaseTTL:          30 * time.Second,
		DeviceTotalBytes:  1 << 30,
	}
	tests := []struct {
		name string
		mut  func(*agent.Config)
		ok   bool
	}{
		{"valid", func(*agent.Config) {}, true},
		{"no host id", func(c *agent.Config) { c.HostID = "" }, false},
		{"no agent version", func(c *agent.Config) { c.AgentVersion = "" }, false},
		{"format version zero", func(c *agent.Config) { c.MaxFormatVersion = 0 }, false},
		{"no heartbeat interval", func(c *agent.Config) { c.HeartbeatInterval = 0 }, false},
		{"no retry backoff", func(c *agent.Config) { c.RetryBackoff = 0 }, false},
		{"backoff above the interval", func(c *agent.Config) { c.RetryBackoff = 10 * time.Second }, false},
		{"no lease ttl", func(c *agent.Config) { c.LeaseTTL = 0 }, false},
		{"lease ttl below the interval", func(c *agent.Config) { c.LeaseTTL = time.Second }, false},
		{"no device total", func(c *agent.Config) { c.DeviceTotalBytes = 0 }, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.mut(&cfg)
			err := cfg.Validate()
			if (err == nil) != tc.ok {
				t.Fatalf("Validate() = %v, want ok=%v", err, tc.ok)
			}
		})
	}
}

// TestNewRejectsMissingDependencies: every one of these is injected (INV-01), so a
// nil one is a wiring bug that must not survive construction.
func TestNewRejectsMissingDependencies(t *testing.T) {
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	full := agent.Deps{
		Clock:        clk,
		ControlPlane: newFakeCP(clk),
		Device:       fakeDevice{},
		Volumes:      agent.NewVolumeSet(),
	}
	tests := []struct {
		name string
		mut  func(*agent.Deps)
	}{
		{"no clock", func(d *agent.Deps) { d.Clock = nil }},
		{"no control plane", func(d *agent.Deps) { d.ControlPlane = nil }},
		{"no device", func(d *agent.Deps) { d.Device = nil }},
		{"no volume source", func(d *agent.Deps) { d.Volumes = nil }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deps := full
			tc.mut(&deps)
			if _, err := agent.New(testConfig(), deps); err == nil {
				t.Fatal("New accepted a missing dependency")
			}
		})
	}
	if _, err := agent.New(testConfig(), full); err != nil {
		t.Fatalf("New rejected a complete set of dependencies: %v", err)
	}
}
