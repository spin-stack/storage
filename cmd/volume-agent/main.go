// Command volume-agent runs the Volume Agent's reconciliation loop against a
// Control Plane (ADR-0018). It is deliberately thin: everything worth testing lives
// in internal/agent, and this file exists to build the *real* clock, disk, and RPC
// client and hand them over — main is the only place in the tree where a real
// implementation is constructed (INV-01).
//
// There is no data path yet. This binary heartbeats, learns what it should be
// serving, and reports what it observes; attaching a volume to a guest over
// vhost-user-blk arrives with the next increment.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spin-stack/storage/api/gen/spin/storage/v1/storagev1connect"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/simio/real"
)

// version is the build identity the Agent reports. Overridden at link time with
// -ldflags "-X main.version=...".
var version = "dev"

// maxFormatVersion is the highest on-disk/on-S3 format this build reads and writes.
// It is a constant of the binary, not configuration: it describes the code.
const maxFormatVersion = 1

func main() {
	if err := run(); err != nil {
		slog.Error("volume-agent exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		hostID       = flag.String("host-id", "", "fleet identity of this host (required)")
		cpURL        = flag.String("control-plane", "", "base URL of the Control Plane, e.g. http://cp:8080 (required)")
		dataDir      = flag.String("data-dir", "", "directory holding this Agent's WAL and checkpoints (required)")
		interval     = flag.Duration("heartbeat-interval", 5*time.Second, "reconciliation cadence")
		retryBackoff = flag.Duration("retry-backoff", time.Second, "delay after the first failed cycle; doubles up to the interval")
		leaseTTL     = flag.Duration("lease-ttl", 30*time.Second, "host lease TTL to expect from the Control Plane")
		httpTimeout  = flag.Duration("rpc-timeout", 10*time.Second, "per-request timeout for Control Plane calls")
	)
	flag.Parse()

	switch {
	case *hostID == "":
		return errors.New("-host-id is required")
	case *cpURL == "":
		return errors.New("-control-plane is required")
	case *dataDir == "":
		return errors.New("-data-dir is required")
	}

	cfg := agent.Config{
		HostID:            *hostID,
		AgentVersion:      version,
		MaxFormatVersion:  maxFormatVersion,
		HeartbeatInterval: *interval,
		RetryBackoff:      *retryBackoff,
		LeaseTTL:          *leaseTTL,
	}

	disk, err := real.NewDisk(*dataDir)
	if err != nil {
		return fmt.Errorf("opening the data directory: %w", err)
	}

	loop, err := agent.New(cfg, agent.Deps{
		Clock: real.NewClock(),
		ControlPlane: storagev1connect.NewControlPlaneServiceClient(
			&http.Client{Timeout: *httpTimeout}, *cpURL),
		// The device is measured, not declared: NewDiskUsage statfs's the
		// filesystem holding --data-dir, so the capacity ADR-0013's thresholds
		// divide by is the disk's own answer and includes what other tenants of
		// that filesystem occupy.
		Device:  agent.NewDiskUsage(disk),
		Volumes: agent.NewVolumeSet(),
		// No Recorder: obs has no production exporter yet (the OTLP wiring is a
		// deploy concern nobody has landed), and a nil Recorder is a working no-op.
		// Passing obs.NewTestProvider here would export the metrics to memory and
		// look like observability from the outside.
		Recorder: nil,
	})
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("volume-agent starting",
		"host_id", cfg.HostID, "version", version, "control_plane", *cpURL,
		"data_dir", *dataDir, "heartbeat_interval", cfg.HeartbeatInterval)

	if err := loop.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	slog.Info("volume-agent stopped")
	return nil
}
