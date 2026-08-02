// Command volume-agent runs the Volume Agent's reconciliation loop against a
// Control Plane (ADR-0018). It is deliberately thin: everything worth testing lives
// in internal/agent, and this file exists to build the *real* clock, disk, and RPC
// client and hand them over — main is the only place in the tree where a real
// implementation is constructed (INV-01).
//
// It now carries a data path: one runtime per volume the Control Plane lists for this
// host, each with its own WAL, block device and vhost-user socket a guest attaches to.
// What it does not carry yet is the remote half — remote mode needs a lease the Log can
// trust, and that arrives with the fencing increment.
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
	"path/filepath"
	"syscall"
	"time"

	"github.com/spin-stack/storage/api/gen/spin/storage/v1/storagev1connect"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/simio/real"
	"github.com/spin-stack/storage/internal/storecfg"
	"github.com/spin-stack/storage/internal/vhost/hostio"
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
		hostID       = flag.String("host-id", "", "fleet identity of this host: a UUIDv7 (required; mint one with `uuidgen` only if it is v7)")
		cpURL        = flag.String("control-plane", "", "base URL of the Control Plane, e.g. http://cp:8080 (required)")
		dataDir      = flag.String("data-dir", "", "directory holding this Agent's WAL and checkpoints (required)")
		socketDir    = flag.String("vhost-socket-dir", "", "directory this Agent binds one vhost-user socket per volume in (required)")
		interval     = flag.Duration("heartbeat-interval", 5*time.Second, "reconciliation cadence")
		retryBackoff = flag.Duration("retry-backoff", time.Second, "delay after the first failed cycle; doubles up to the interval")
		leaseTTL     = flag.Duration("lease-ttl", 30*time.Second, "host lease TTL to expect from the Control Plane")
		httpTimeout  = flag.Duration("rpc-timeout", 10*time.Second, "per-request timeout for Control Plane calls")
		kekFile      = flag.String("kek-file", "", "path to this host's 32-byte key-encryption key (§15.1). Without it the Agent runs unencrypted, which is dev mode only")
	)
	var storeFlags storecfg.Flags
	storeFlags.Register(flag.CommandLine)
	flag.Parse()

	switch {
	case *hostID == "":
		return errors.New("-host-id is required")
	case *cpURL == "":
		return errors.New("-control-plane is required")
	case *dataDir == "":
		return errors.New("-data-dir is required")
	case *socketDir == "":
		return errors.New("-vhost-socket-dir is required")
	}

	cfg := agent.Config{
		HostID:            *hostID,
		AgentVersion:      version,
		MaxFormatVersion:  maxFormatVersion,
		HeartbeatInterval: *interval,
		RetryBackoff:      *retryBackoff,
		LeaseTTL:          *leaseTTL,
	}

	// Validated before anything is opened: a typo'd flag should fail on the flag, not
	// behind a connection error from whichever of the disk and the object store
	// happened to be tried first.
	if err := cfg.Validate(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	disk, err := real.NewDisk(*dataDir)
	if err != nil {
		return fmt.Errorf("opening the data directory: %w", err)
	}

	// The object store is what FLUSH makes a write durable in (§14.4) and what a
	// restart recovers from (§5.8). The Agent has never had one — which is why it
	// could heartbeat and never upload a byte — so it is opened here, at startup,
	// rather than discovered to be missing on the first FLUSH.
	store, err := storeFlags.Open(ctx)
	if err != nil {
		return err
	}
	// §15: every payload of guest data is sealed with the volume's DEK before any
	// PUT. The KEK is read once, here — the Agent unwraps a DEK only at attach
	// (§15.1) and keeps it in memory. Without -kek-file there is no KMS and the Agent
	// runs in the clear, which is honest for dev and refused for anything else by the
	// operator who chose not to pass the flag.
	var kms crypto.KMS
	if *kekFile != "" {
		kekDisk, kerr := real.NewDisk(filepath.Dir(*kekFile))
		if kerr != nil {
			return fmt.Errorf("opening the directory holding the KEK: %w", kerr)
		}
		kek, kerr := crypto.LoadKEK(kekDisk, filepath.Base(*kekFile))
		if kerr != nil {
			return kerr
		}
		// The id is derived from the key, never configured: it is what the volume row
		// records and what the Agent compares against before unwrapping, and the
		// Control Plane derives it the same way from the same file (crypto.KEKID).
		kms = crypto.NewDevKMS(kek, crypto.KEKID(kek))
		slog.Info("key-encryption key loaded", "kek_id", crypto.KEKID(kek))
	} else {
		slog.Warn("no -kek-file: this Agent writes guest data unencrypted (§15 requires encryption outside dev)")
	}

	// One runtime per volume, each with its own WAL, block device and vhost-user
	// socket. This is what the Agent serves from — before it, the binary heartbeated
	// about an empty set forever.
	//
	// The lease is passed as a call through to the loop, not as the loop's
	// *lease.Manager: applyLease allocates a new manager whenever the Control Plane
	// changes the TTL, and a Log holding the old one would be gated by something nobody
	// renews — it would self-fence a perfectly healthy host and never recover. The
	// closure reads `loop` after it is assigned below; until then it answers false,
	// which is the safe direction (no lease, no durable ACK).
	var loop *agent.Loop
	volumes, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir:   *dataDir,
		SocketDir: *socketDir,
		// Without it no volume on this host ever runs a durability scheduler: a
		// checkpoint names the host publishing it (§12.3-12.4), and checkpointsEnabled
		// refuses to start one that cannot. The consequence is not subtle — `published`
		// stays 0 for the life of the process, not one byte of local WAL is ever
		// reclaimed, and the NVMe fills. It was missing until the e2e lane started the
		// real binary and read the line it logs about it.
		HostID: *hostID,
	}, agent.VolumeManagerDeps{
		Clock:   real.NewClock(),
		Disk:    disk,
		Listen:  hostio.Listen,
		Mapper:  hostio.NewMapper(),
		EventFD: hostio.NewEventFD,
		Store:   store,
		Lease:   func() bool { return loop != nil && loop.LeaseValid() },
		KMS:     kms,
		// Read through the loop for the same reason the lease is: the loop is assigned
		// below, and it owns the cache whose entries are evicted when a volume leaves
		// this host's desired state.
		Keys: func(ctx context.Context, volumeID string) (agent.VolumeKeys, error) {
			if loop == nil {
				return agent.VolumeKeys{}, errors.New("the Agent loop is not running yet")
			}
			return loop.VolumeKeys(ctx, volumeID)
		},
	})
	if err != nil {
		return err
	}
	defer func() {
		if err := volumes.Close(); err != nil {
			slog.Error("stopping the volume runtimes", "error", err)
		}
	}()

	loop, err = agent.New(cfg, agent.Deps{
		Clock: real.NewClock(),
		ControlPlane: storagev1connect.NewControlPlaneServiceClient(
			&http.Client{Timeout: *httpTimeout}, *cpURL),
		// The device is measured, not declared: NewDiskUsage statfs's the
		// filesystem holding --data-dir, so the capacity ADR-0013's thresholds
		// divide by is the disk's own answer and includes what other tenants of
		// that filesystem occupy.
		Device:  agent.NewDiskUsage(disk),
		Volumes: volumes,
		// No Recorder: obs has no production exporter yet (the OTLP wiring is a
		// deploy concern nobody has landed), and a nil Recorder is a working no-op.
		// Passing obs.NewTestProvider here would export the metrics to memory and
		// look like observability from the outside.
		Recorder: nil,
	})
	if err != nil {
		return err
	}

	slog.Info("volume-agent starting",
		"host_id", cfg.HostID, "version", version, "control_plane", *cpURL,
		"data_dir", *dataDir, "vhost_socket_dir", *socketDir,
		"heartbeat_interval", cfg.HeartbeatInterval)

	if err := loop.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	slog.Info("volume-agent stopped")
	return nil
}
