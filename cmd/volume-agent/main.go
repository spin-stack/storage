// Command volume-agent runs the Volume Agent's reconciliation loop against a
// Control Plane (ADR-0018). It is deliberately thin: everything worth testing lives
// in internal/agent, and this file exists to build the *real* clock, disk, and RPC
// client and hand them over — main is the only place in the tree where a real
// implementation is constructed (INV-01).
//
// # It carries no data path, and that is this build's whole shape
//
// It used to: one runtime per volume the Control Plane listed for this host, each with
// its own write-ahead log, block device and vhost-user socket a guest attached to. That
// engine is withdrawn. QEMU manages the local copy-on-write format through qcow2 from
// here on, and this system's job narrows to immutable commits, publication to object
// storage, and recovery — none of which this binary performs yet.
//
// So what runs is exactly the half that talks to the Control Plane: this host claims its
// data directory, reads its key, registers, heartbeats, holds a lease, learns which
// volumes it is supposed to be serving, and reports an empty set. It says so on the way
// up, on every cycle that has work it cannot do, and on the way down. An operator who
// starts it gets a host that appears in `-fleet-status` and serves nothing, which is the
// truth.
//
// **What Stage 1 adds**: a volume manager over qcow2 — create, attach, restart, detach,
// local persistence — driven by QMP, plugged into the loop at agent.VolumeReconciler,
// which is the seam left standing for it. Nothing remote in Stage 1: no object store
// flags here, because an Agent that took a bucket it never wrote to would be claiming a
// capability it does not have.
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
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/real"
)

// version is the build identity the Agent reports. Overridden at link time with
// -ldflags "-X main.version=...".
var version = "dev"

// maxFormatVersion is the highest on-disk/on-S3 format this build reads and writes.
// It is a constant of the binary, not configuration: it describes the code.
const maxFormatVersion = 1

// lockFile is this host's claim on its data directory, inside it. The name is part of
// the operator's world — it is what a human looks for to find out whether an Agent is
// holding a directory — so it is a constant here and not a path built at the call site.
const lockFile = "agent.lock"

func main() {
	if err := run(); err != nil {
		slog.Error("volume-agent exited", "error", err)
		os.Exit(1)
	}
}

func run() (err error) {
	var (
		hostID       = flag.String("host-id", "", "fleet identity of this host: a UUIDv7 (required; mint one with `uuidgen` only if it is v7)")
		cpURL        = flag.String("control-plane", "", "base URL of the Control Plane, e.g. http://cp:8080 (required)")
		dataDir      = flag.String("data-dir", "", "directory holding this Agent's local state, and the lock that keeps one Agent per host (required)")
		interval     = flag.Duration("heartbeat-interval", 5*time.Second, "reconciliation cadence")
		retryBackoff = flag.Duration("retry-backoff", time.Second, "delay after the first failed cycle; doubles up to the interval")
		leaseTTL     = flag.Duration("lease-ttl", 30*time.Second, "host lease TTL to expect from the Control Plane")
		httpTimeout  = flag.Duration("rpc-timeout", 10*time.Second, "per-request timeout for Control Plane calls")
		otlpEndpoint = flag.String("otlp-endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
			"OTLP/HTTP collector to export metrics to, e.g. http://collector:4318 (empty disables telemetry)")
		metricsListen = flag.String("metrics-listen", "",
			"host:port for the operator endpoint: GET /metrics (Prometheus text) and GET /healthz. Empty disables it, and then this process holds no listening socket at all")
		kekFile = flag.String("kek-file", "", "path to this host's 32-byte key-encryption key (§15.1). Without it the Agent holds no key material, which is dev mode only")
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

	// Validated before anything is opened: a typo'd flag should fail on the flag, not
	// behind a connection error from whichever dependency happened to be tried first.
	if err := cfg.Validate(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Telemetry, before anything that records: an exporter that arrives after the
	// components hold their Recorder would leave those components holding a no-op for
	// the life of the process. NewOTLPMetricExporter returns (nil, nil) for an empty
	// endpoint and NewProvider takes that nil, so an Agent with no collector needs no
	// conditional here and behaves exactly as it did before this existed.
	exporter, err := real.NewOTLPMetricExporter(ctx, *otlpEndpoint)
	if err != nil {
		return err
	}
	telemetry, err := obs.NewProvider("volume-agent", exporter)
	if err != nil {
		return err
	}
	defer func() {
		// WithoutCancel: the final flush must outlive the SIGTERM that started the
		// shutdown. Logged and never fatal: a collector that is down must not change
		// what this process's exit code says.
		if err := telemetry.Shutdown(context.WithoutCancel(ctx)); err != nil {
			slog.Error("flushing metrics", "error", err)
		}
	}()

	// Before the disk and the KEK are opened, and that is the point: those are the
	// startup steps that hang, and an Agent that answers /healthz while /metrics is
	// still empty is telling an operator exactly where it is stuck.
	if *metricsListen != "" {
		defer serveOperatorEndpoint(*metricsListen, telemetry)()
	}

	dataDisk, err := real.NewDisk(*dataDir)
	if err != nil {
		return fmt.Errorf("opening the data directory: %w", err)
	}

	// §10 opens with "un proceso por host". Two live Agents on one --data-dir is not a
	// hypothetical: whatever local format the volumes take, both incarnations open the
	// same files under it, and the second silently takes the guest from the first.
	//
	// It is taken in main, which is where the previous version deliberately did not put
	// it: the volume manager owned the data directory and took it there, so no step was
	// left to a caller that spin's runner would not inherit (ADR-0021). There is no
	// volume manager now, and a claim on the directory has to outlive one anyway — this
	// process holds it whether or not it is serving. The qcow2 manager should take it
	// back the moment it owns the directory's layout.
	unlock, err := dataDisk.Lock(lockFile)
	if err != nil {
		if errors.Is(err, disk.ErrLocked) {
			return fmt.Errorf("another Volume Agent is already using %s (§10: one Agent per host): %w", *dataDir, err)
		}
		return fmt.Errorf("claiming %s: %w", *dataDir, err)
	}
	// The kernel drops an flock when the process dies, so this defer is for the paths
	// that return rather than for a crash — nothing has to clean up after one.
	defer func() { err = errors.Join(err, unlock.Close()) }()

	// §15: guest data is sealed with the volume's DEK, wrapped under this KEK. Nothing
	// in this build unwraps one — there is no data path to seal for — so the key is read
	// and its id derived, and no KMS is built over it. Reading it is not ceremony: the
	// Control Plane wraps every volume's DEK under the KEK *it* read, an Agent that
	// reaches a different key from the same file has volumes it can never open, and the
	// two binaries disagreeing about how to parse the file is a defect this repository
	// has already shipped once. The id on this line is what makes them comparable.
	if *kekFile != "" {
		kekDisk, kerr := real.NewDisk(filepath.Dir(*kekFile))
		if kerr != nil {
			return fmt.Errorf("opening the directory holding the KEK: %w", kerr)
		}
		kek, kerr := crypto.LoadKEK(kekDisk, filepath.Base(*kekFile))
		if kerr != nil {
			return kerr
		}
		slog.Info("key-encryption key loaded", "kek_id", crypto.KEKID(kek))
	} else {
		slog.Warn("no -kek-file: this Agent holds no key material (§15 requires encryption outside dev)")
	}

	cp := storagev1connect.NewControlPlaneServiceClient(
		&http.Client{Timeout: *httpTimeout}, *cpURL)

	// An empty VolumeSource, and it is not a placeholder that will quietly start working:
	// it is a set nothing ever puts a volume into, because nothing in this build can
	// serve one. The loop hands the desired state to a VolumeReconciler when its source
	// happens to be one, and this is not one, so the desired state is recorded and acted
	// on by nobody. See the package doc for what fills this in.
	volumes := agent.NewVolumeSet()

	loop, err := agent.New(cfg, agent.Deps{
		Clock:        real.NewClock(),
		ControlPlane: cp,
		// The device is measured, not declared: NewDiskUsage statfs's the filesystem
		// holding --data-dir, so the capacity ADR-0013's thresholds divide by is the
		// disk's own answer and includes what other tenants of that filesystem occupy.
		Device:   agent.NewDiskUsage(dataDisk),
		Volumes:  volumes,
		Recorder: telemetry.Recorder(),
	})
	if err != nil {
		return err
	}

	slog.Info("volume-agent starting",
		"host_id", cfg.HostID, "version", version, "control_plane", *cpURL,
		"data_dir", *dataDir, "heartbeat_interval", cfg.HeartbeatInterval,
		"metrics_listen", *metricsListen)
	// Said once, at the top, in the log an operator is already reading. A process that
	// registers a host and then serves nothing looks like a bug from the outside, and
	// the difference between this and a bug is one line.
	slog.Warn("this Agent serves no volumes: the local block engine is withdrawn and the qcow2 volume manager is not built yet",
		"serves", "nothing", "reports", "an empty volume set", "next", "Stage 1: qcow2 create/attach/restart/detach, local only")

	if err := loop.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	// Printed here rather than from a deferred function, so "stopped" means the loop
	// returned rather than "was asked to stop". An operator greps for this line to tell
	// a clean shutdown from a disappearance.
	slog.Info("volume-agent stopped")
	return nil
}

// serveOperatorEndpoint starts the Agent's only listening socket and returns the
// function that stops it.
//
// **The Agent held no listening socket at all before this.** There was no /metrics, no
// /healthz and no admin port: every series it collected could reach a collector over
// OTLP or reach nobody, and nothing in this repository stood a collector up. So the
// operational answer to "what is this Agent doing" was "read its log". One read-only
// handler over what obs already collects is the whole fix, and it is deliberately not
// more than that — a pilot needs an answer from the process, not a platform.
//
// INV-01 and the socket: `cmd/` is where real implementations are constructed, and this
// is an http.Server bound in a main, the same shape cmd/control-plane already uses for
// its RPC listener. Nothing simulable is involved — the handler is a pure function of
// what the meter provider holds, it is on no data path, and no simulation drives it, so
// there is nothing here for simio to model.
//
// A bind that fails is loud and not fatal, which is the one judgement call in this
// function. The endpoint belongs to the operator: a port already in use must not end the
// process. What makes the failure detectable is the Error line — the thing that must
// never happen is a scrape that silently never worked.
func serveOperatorEndpoint(addr string, telemetry *obs.Provider) func() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		body, err := telemetry.Scrape(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", obs.ContentType)
		_, _ = w.Write(body)
	})
	// Liveness and nothing more: this process is up and its handler loop is answering.
	// It deliberately does not report on the Control Plane or any volume — a liveness
	// probe that goes red because a dependency is down restarts an Agent that may be
	// holding a guest's only copy of its session, which is the opposite of what anyone
	// wants. What the volumes are doing is /metrics' job.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			slog.Error("the operator endpoint is not serving: this Agent has no /metrics and no /healthz",
				"listen", addr, "error", err)
		}
	}()
	slog.Info("operator endpoint starting", "listen", addr, "metrics", "/metrics", "healthz", "/healthz")
	return func() { _ = srv.Close() }
}
