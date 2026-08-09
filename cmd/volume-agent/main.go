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
	"crypto/rand"
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
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/simio/clock"
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
		os.Exit(exitCode(err))
	}
}

// exitCode turns the teardown's outcome into the number a supervisor reads. It is the
// only thing that tells systemd whether restarting this unit is the fix or the harm.
//
//	0  every session this host was serving is in the object store
//	1  something is not: restart, and the next incarnation re-attaches at the same epoch
//	   (ADR-0024) with the local WAL intact and publishes it
//	2  another writer published over us: this host's copy is *older* than what is in the
//	   bucket, so do not restart — a restart would only try to lose that race again
//
// Abandonment is checked first, and the order is the decision. A teardown can end with
// one volume superseded and another abandoned by an impatient operator, and those two
// want opposite things from a supervisor. Restarting is safe for the superseded volume —
// it re-reads the manifest, finds itself behind, and refuses again — while *not*
// restarting leaves the abandoned session on a disk nothing will ever read. So the code
// that asks for a restart wins whenever both are true.
func exitCode(err error) int {
	switch {
	case errors.Is(err, agent.ErrPublishAbandoned):
		return 1
	case errors.Is(err, image.ErrSuperseded):
		return 2
	default:
		return 1
	}
}

func run() (err error) {
	var (
		hostID       = flag.String("host-id", "", "fleet identity of this host: a UUIDv7 (required; mint one with `uuidgen` only if it is v7)")
		cpURL        = flag.String("control-plane", "", "base URL of the Control Plane, e.g. http://cp:8080 (required)")
		dataDir      = flag.String("data-dir", "", "directory holding this Agent's WAL, and the lock that keeps one Agent per host (required)")
		socketDir    = flag.String("vhost-socket-dir", "", "directory this Agent binds one vhost-user socket per volume in (required)")
		interval     = flag.Duration("heartbeat-interval", 5*time.Second, "reconciliation cadence")
		retryBackoff = flag.Duration("retry-backoff", time.Second, "delay after the first failed cycle; doubles up to the interval")
		leaseTTL     = flag.Duration("lease-ttl", 30*time.Second, "host lease TTL to expect from the Control Plane")
		httpTimeout  = flag.Duration("rpc-timeout", 10*time.Second, "per-request timeout for Control Plane calls")
		otlpEndpoint = flag.String("otlp-endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
			"OTLP/HTTP collector to export metrics to, e.g. http://collector:4318 (empty disables telemetry)")
		grace = flag.Duration("shutdown-grace", 60*time.Second,
			"bound on ONE publish attempt at shutdown, not on the shutdown: an Agent that cannot publish keeps its data directory and retries until it can, or until a second signal")
		metricsListen = flag.String("metrics-listen", "",
			"host:port for the operator endpoint: GET /metrics (Prometheus text) and GET /healthz. Empty disables it, and then this process holds no listening socket at all")
		kekFile    = flag.String("kek-file", "", "path to this host's 32-byte key-encryption key (§15.1). Without it the Agent runs unencrypted, which is dev mode only")
		maxVolumes = flag.Int("max-volumes", agent.DefaultMaxVolumes,
			"how many volumes this host serves at once, and what its device budget is divided by: each volume's WAL is bounded by that share (ADR-0013 §1)")
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
		// shutdown, and this Agent's most interesting samples — the publish duration,
		// the holding line's attempts — are produced during it. Logged and never
		// fatal: a collector that is down must not change what the Agent's exit code
		// says about the guest's data.
		if err := telemetry.Shutdown(context.WithoutCancel(ctx)); err != nil {
			slog.Error("flushing metrics", "error", err)
		}
	}()

	// Before the disk, the object store and the KEK are opened, and that is the point:
	// those are the startup steps that hang, and an Agent that answers /healthz while
	// /metrics is still empty is telling an operator exactly where it is stuck.
	if *metricsListen != "" {
		defer serveOperatorEndpoint(*metricsListen, telemetry)()
	}

	disk, err := real.NewDisk(*dataDir)
	if err != nil {
		return fmt.Errorf("opening the data directory: %w", err)
	}

	// The device budget, before any volume can be served (ADR-0013 §1). It is measured
	// and divided here because there is no honest default: an Agent that started with
	// no budget would run with no write-path bound of any kind — which is what every
	// Agent this repository has ever run did, since nothing set wal.Limits — and the
	// first thing it would do about a filling device is take it to ENOSPC in the
	// middle of a guest's WRITE.
	//
	// The failure to *measure* is fatal for the same reason DiskUsage refuses to
	// smooth it into a zero: an unreadable device that looks empty reads as headroom
	// to every rule downstream.
	usage, err := agent.NewDiskUsage(disk).Usage(ctx)
	if err != nil {
		return err
	}
	budget, err := agent.NewBudget(usage, *maxVolumes)
	if err != nil {
		return err
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
		// "." and not *dataDir: the Disk above is already rooted at --data-dir, which
		// is what keeps the Agent from writing outside it, and DataDir is a path
		// inside that namespace. Passing the operator's absolute path here put every
		// WAL under <data-dir>/<data-dir>/wal/... — consistent, restart-safe, and
		// nowhere near where the operator was told to look.
		DataDir: ".",
		// The same directory, spelled for the human who has to find it. Without it the
		// line that says "I am holding this directory and will not let go" said
		// `data_dir=.`, which is true of every Agent that has ever run and useful to
		// nobody — see DataDirLabel.
		DataDirLabel: *dataDir,
		SocketDir:    *socketDir,
		// One attempt's bound, not the teardown's: see -shutdown-grace, and
		// The reviewed decision: there is no budget
		// after which this process gives a session up.
		ShutdownGrace: *grace,
		// Every Log this manager builds is bounded by its share of this (ADR-0013 §1).
		Budget: budget,
	}, agent.VolumeManagerDeps{
		Clock:   real.NewClock(),
		Disk:    disk,
		Listen:  hostio.Listen,
		Mapper:  hostio.NewMapper(),
		EventFD: hostio.NewEventFD,
		Store:   store,
		KMS:     kms,
		// §15: the image chunk nonces. Real randomness in the binary; the DST
		// harness injects a seeded reader so the same seed gives the same ciphertext.
		Rand: rand.Reader,
		// Every volume's Log gets this too — VolumeManager hands it down at the one
		// place a Log is built (see start).
		Recorder: telemetry.Recorder(),
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
	// The teardown is deferred so that every path out of run() — including the wiring
	// failures below — releases this host's claim on the data directory. Its error is
	// *joined into run's*, which is the whole point and used not to be: the old shape
	// logged it from a deferred function, and a deferred function cannot change the
	// process's exit status, so the Agent exited 0 whether or not the session it was
	// serving ever reached the bucket.
	defer func() {
		err = errors.Join(err, shutdown(volumes, loop))
		if err == nil {
			// Printed here, after the images are settled, so "stopped" means stopped
			// rather than "asked to stop". An operator greps for this line to tell a
			// clean shutdown from a disappearance.
			slog.Info("volume-agent stopped")
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
		Device:   agent.NewDiskUsage(disk),
		Volumes:  volumes,
		Recorder: telemetry.Recorder(),
	})
	if err != nil {
		return err
	}

	// The budget is printed with the rest of the wiring, and it is the line an operator
	// reads to find out why a guest is getting backpressure on a device that looks
	// half empty: the share, not the device, is what bounds one volume.
	slog.Info("volume-agent starting",
		"host_id", cfg.HostID, "version", version, "control_plane", *cpURL,
		"data_dir", *dataDir, "vhost_socket_dir", *socketDir,
		"heartbeat_interval", cfg.HeartbeatInterval,
		"device_bytes", budget.DeviceBytes, "guest_budget_bytes", budget.GuestBytes,
		"reserve_bytes", budget.ReserveBytes, "max_volumes", budget.MaxVolumes,
		"volume_share_bytes", budget.Share(), "metrics_listen", *metricsListen)

	// Started with the loop and stopped with it: ctx is what ends both. It reads the
	// devices the loop's VolumeManager owns, so it cannot start before that exists.
	go watchSpacePressure(ctx, real.NewClock(), volumes, telemetry.Recorder(),
		budget.Share(), cfg.HeartbeatInterval)

	if err := loop.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// serveOperatorEndpoint starts the Agent's only listening socket and returns the
// function that stops it.
//
// **The Agent held no listening socket at all before this.** There was no /metrics, no
// /healthz and no admin port: every series it collected could reach a collector over
// OTLP or reach nobody, and nothing in this repository stood a collector up. So the
// operational answer to "what is this Agent doing" was "read its log", and a guest
// taking I/O errors produced no line in it. One read-only handler over what obs already
// collects is the whole fix, and it is deliberately not more than that — a pilot needs
// an answer from the process, not a platform.
//
// INV-01 and the socket: `cmd/` is where real implementations are constructed, and this
// is an http.Server bound in a main, the same shape cmd/control-plane already uses for
// its RPC listener. Nothing simulable is involved — the handler is a pure function of
// what the meter provider holds, it is on no data path, and no simulation drives it, so
// there is nothing here for simio to model.
//
// A bind that fails is loud and not fatal, which is the one judgement call in this
// function. The endpoint belongs to the operator, not to the guest: a port already in
// use must not cost a tenant its session, and an Agent that exited here would be a new
// way to lose one. What makes the failure detectable is the Error line — the thing that
// must never happen is a scrape that silently never worked.
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
	// It deliberately does not report on the Control Plane, the object store or any
	// volume — a liveness probe that goes red because a dependency is down restarts an
	// Agent that is holding a guest's only copy of its session, which is the opposite
	// of what anyone wants. What the volumes are doing is /metrics' job.
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

// watchSpacePressure turns a volume that has begun refusing guest writes for want of
// space into something the host can see: one log line, and a gauge that stays up.
//
// The Agent had neither, and the hole was total. A guest that crossed its share got
// `I/O error, dev vda` and a failed fsync — the tenant saw the truth — while the
// Agent's log stayed at exactly its four start-up lines. Nothing in this process ever
// sees that refusal on its own: it is produced in blockdev on the guest's goroutine and
// handed straight back to a virtqueue that completes the request with IOERR.
//
// **Once per volume, not once per rejected write.** A guest under backpressure produces
// thousands of refusals a second, and a line each would bury the log at the moment it
// has to be readable. blockdev latches the fact instead of exposing a live predicate,
// which is also what lets this poll run at the heartbeat's cadence without missing the
// transition.
//
// Polled rather than called back, because a callback would have to be installed where
// the Device is built — inside internal/agent — and the two things this line needs are
// both here: the operator's log, and the device budget the share was divided out of.
// The Agent's own volume share is not knowledge blockdev has or should acquire.
func watchSpacePressure(ctx context.Context, clk clock.Clock, volumes *agent.VolumeManager,
	rec *obs.Recorder, share int64, every time.Duration) {
	said := map[string]bool{}
	for {
		if err := clk.Sleep(ctx, every); err != nil {
			return
		}
		vols, err := volumes.Volumes(ctx)
		if err != nil {
			continue
		}
		live := make(map[string]bool, len(vols))
		for _, v := range vols {
			live[v.VolumeID] = true
			dev, ok := volumes.Device(v.VolumeID)
			if !ok {
				continue
			}
			reason, refused := dev.RefusedForSpace()
			if !refused {
				rec.Gauge(ctx, "volume_backpressure", 0, obs.String("volume", v.VolumeID))
				continue
			}
			rec.Gauge(ctx, "volume_backpressure", 1, obs.String("volume", v.VolumeID))
			if said[v.VolumeID] {
				continue
			}
			said[v.VolumeID] = true
			slog.Warn("this volume is refusing guest writes for want of space; the guest is taking I/O errors and its fsync is failing",
				"volume_id", v.VolumeID, "reason", reason, "volume_share_bytes", share,
				"remedy", "stop the volume — publishing its image at stop is what reclaims the local WAL; nothing else does while it runs")
		}
		// A volume that left this host is forgotten, so that the same volume attaching
		// again — a new session, a new WAL, a new share — is reported again rather than
		// silently suppressed by what its predecessor did.
		for id := range said {
			if !live[id] {
				delete(said, id)
			}
		}
	}
}

// shutdown publishes every session this host was serving and does not come back until it
// has — holding the data-directory lock, and therefore this process's life, for as long
// as that takes.
//
// Two things make that legible instead of merely stubborn, and they are both here:
//
//   - **the second signal**, which is the operator's override. It is registered *now* and
//     not at start-up, because the first one is what got us here and a context registered
//     before it would already be cancelled. Catching it is safe: signal.Notify delivers to
//     every registered channel, and run's own handler is still installed (its stop() is
//     deferred earlier, so it runs after this), which is what keeps the next signal from
//     killing the process outright while it is holding data.
//   - **the heartbeat**, which keeps running while we hold. Without it the Control Plane
//     sees a host that stopped talking — indistinguishable from a crashed one — at exactly
//     the moment the interesting fact is that the host is alive and stuck. Not the whole
//     reconcile loop: reading the desired state again would start runtimes this teardown
//     has just stopped.
func shutdown(volumes *agent.VolumeManager, loop *agent.Loop) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if loop == nil {
		return volumes.Close(ctx) // a wiring failure: there are no runtimes and nobody to tell
	}
	sustainCtx, done := context.WithCancel(context.Background())
	sustained := make(chan struct{})
	go func() {
		defer close(sustained)
		loop.Sustain(sustainCtx)
	}()

	err := volumes.Close(ctx)
	done()
	<-sustained // joined rather than left running: it logs, and run() is about to return
	return err
}
