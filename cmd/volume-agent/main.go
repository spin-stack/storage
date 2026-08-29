// Command volume-agent runs the Volume Agent's reconciliation loop against a
// Control Plane (ADR-0018). It is deliberately thin: everything worth testing lives
// in internal/agent, and this file exists to build the *real* clock, disk, and RPC
// client and hand them over — main is the only place in the tree where a real
// implementation is constructed (INV-01).
//
// # What it serves, and what serves it
//
// The data path is QEMU's. This process prepares each volume's local qcow2 chain, hands the
// paths to whoever launches the VM, and speaks QMP to the QEMU that ends up there — it does
// not start one; internal/qcow carries the two-path contract. So: claim the data directory,
// read the key, register, heartbeat, hold a lease, learn which volumes to serve, prepare a
// chain for each, and report what it saw.
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
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/publisher"
	"github.com/spin-stack/storage/internal/qcow"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/real"
	"github.com/spin-stack/storage/internal/storecfg"
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

func run() (err error) {
	var (
		hostID       = flag.String("host-id", "", "fleet identity of this host: a UUIDv7 (required; mint one with `uuidgen` only if it is v7)")
		cpURL        = flag.String("control-plane", "", "base URL of the Control Plane, e.g. http://cp:8080 (required)")
		dataDir      = flag.String("data-dir", "", "directory holding this Agent's local state, and the lock that keeps one Agent per host (required)")
		interval     = flag.Duration("heartbeat-interval", 5*time.Second, "reconciliation cadence")
		retryBackoff = flag.Duration("retry-backoff", time.Second,
			"delay after the first failed cycle; doubles up to the interval. Defaults to the interval when that is shorter")
		leaseTTL     = flag.Duration("lease-ttl", 30*time.Second, "host lease TTL to expect from the Control Plane")
		httpTimeout  = flag.Duration("rpc-timeout", 10*time.Second, "per-request timeout for Control Plane calls")
		otlpEndpoint = flag.String("otlp-endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
			"OTLP/HTTP collector to export metrics to, e.g. http://collector:4318 (empty disables telemetry)")
		metricsListen = flag.String("metrics-listen", "",
			"host:port for the operator endpoint: GET /metrics (Prometheus text) and GET /healthz. Empty disables it, and then this process holds no listening socket at all")
		kekFile = flag.String("kek-file", "", "path to this host's 32-byte key-encryption key (§15.1). Without it the Agent holds no key material, which is dev mode only")
		qemuImg = flag.String("qemu-img", "",
			"path to the pinned qemu-img binary, which creates and inspects every qcow2 chain (required)")
		probeTimeout = flag.Duration("qemu-timeout", 5*time.Second,
			"how long one qemu-img run or one QMP exchange may take before the Agent gives up on it for this cycle")
		compactAt = flag.Int("compact-at-layers", qcow.DefaultCompaction.AtLayers,
			"collapse a volume's published prefix into a new immutable root once its chain is this many layers deep; 0 disables it. Past qcow.MaxLayers a chain cannot be rebuilt on another host, and this is what keeps a volume away from that")
		rotateAt = flag.Int64("rotate-at-bytes", qcow.DefaultRotateAtBytes,
			"seal a volume's tip and start a new layer once the tip occupies this many bytes; 0 disables rotation. The default is derived from what one commit costs — see qcow.DefaultRotateAtBytes, and `task measure:publish` to measure your own backend")
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
	case *qemuImg == "":
		// Required rather than defaulted to PATH: v6 pins QEMU to one version for CI
		// and production, and a chain created by whichever qemu-img a login shell found
		// is a chain nobody pinned.
		return errors.New("-qemu-img is required: name the pinned binary (task build:qemu puts it in _output/bin)")
	}

	cfg := agent.Config{
		HostID:            *hostID,
		AgentVersion:      version,
		MaxFormatVersion:  maxFormatVersion,
		HeartbeatInterval: *interval,
		RetryBackoff:      backoffFor(*retryBackoff, *interval),
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

	// §15: guest data is sealed with the volume's DEK, wrapped under this KEK; v6 §10 seals
	// every layer on its way to the object store, so a publishing Agent needs the key in the
	// clear for as long as the transfer takes. Reading it is not ceremony even on a host that
	// publishes nothing: the Control Plane wraps every DEK under the KEK *it* read, and the two
	// binaries disagreeing about how to parse the file is a defect already shipped once.
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
		kms = crypto.NewDevKMS(kek, crypto.KEKID(kek))
		slog.Info("key-encryption key loaded", "kek_id", crypto.KEKID(kek))
	} else {
		slog.Warn("no -kek-file: this Agent holds no key material (§15 requires encryption outside dev)")
	}

	// The publisher, if this host is configured to publish at all. Optional: an Agent with no
	// object store keeps every layer it seals on its own disk, and the line below says which it
	// is, because "this Agent publishes nothing" must not have to be inferred from the absence
	// of commits.
	//
	// The knot — the publisher needs the volume's key, which only agent.Loop can fetch; the
	// Loop needs the volume manager; the manager needs the publisher — is tied here in the
	// wiring, with a holder filled in once the Loop exists (see loopKeys).
	//
	// The volume manager is constructed before the Control Plane client because it is what
	// claims --data-dir: v6 §10 is one Agent per host, and two incarnations preparing chains
	// under the same paths would hand one qcow2 file to two QEMUs. The absolute path is
	// resolved first because this string is handed to other processes — qemu-img, and whoever
	// launches QEMU — whose working directory is not ours.
	root, err := filepath.Abs(*dataDir)
	if err != nil {
		return fmt.Errorf("resolving %s: %w", *dataDir, err)
	}
	keys := &loopKeys{}
	var (
		pub qcow.Publisher
		rec qcow.Recovery = recovery.Absent{}
		// wit is the second signal a lapsed lease is checked against, and it stays nil
		// when there is no object store. That is the honest answer rather than a
		// degraded one: with no second path an Agent cannot confirm that anybody took
		// its volumes, and the loop keeps serving instead of stopping guests on silence.
		wit agent.Witness
	)
	switch {
	case storeFlags.Bucket == "" && storeFlags.Dir == "":
		// No object store: this host seals layers and publishes none of them, and it also
		// cannot answer "does this volume have published commits". recovery.Absent is
		// what makes that a refusal rather than a blank disk — see the type.
		// And nothing rotates. Rotation exists to produce a layer to publish; with no
		// publisher it would seal one, stop at §11's second invariant — never rotate while
		// a sealed layer is unpublished — and leave the tip growing anyway. That is the
		// same disk with one more file in it and a chain one layer deeper, which is
		// strictly worse than not having rotated. An explicit -rotate-at-bytes is still
		// honoured, because a test measuring the rotation itself is a real caller.
		if !flagWasSet("rotate-at-bytes") {
			*rotateAt = 0
		}
		// And nothing collapses: a collapse publishes its root, so with no publisher it
		// would convert a whole volume's worth of bytes and have nowhere to put the result.
		if !flagWasSet("compact-at-layers") {
			*compactAt = 0
		}
		slog.Warn("no object store configured: this Agent publishes nothing and does not rotate, so nothing it holds survives losing this host, and it cannot serve a volume that has published commits elsewhere")
	case kms == nil:
		return errors.New("an object store is configured but -kek-file is not: a layer is sealed with the volume's DEK on the way out (v6 §10), and this Agent could not unwrap one")
	default:
		store, serr := storeFlags.Open(ctx)
		if serr != nil {
			return serr
		}
		paths := real.NewPaths()
		// The §28 numbers of the publish path are recorded only if the clock and the
		// recorder reach the two components that produce them: this is the wiring whose
		// absence left the metrics catalogue describing a system nobody was measuring.
		pub = publisher.New(store, kms, keys, paths).WithTelemetry(real.NewClock(), telemetry.Recorder())
		rec = recovery.New(root, *qemuImg, store, kms, keys, paths, real.NewRunner()).
			WithTelemetry(real.NewClock(), telemetry.Recorder())
		wit = descriptor.EpochWitness{Store: store}
	}

	volumes, err := qcow.New(ctx, qcow.Config{
		Root:          root,
		QemuImg:       *qemuImg,
		ProbeTimeout:  *probeTimeout,
		RotateAtBytes: *rotateAt,
		Compaction:    qcow.CompactionPolicy{AtLayers: *compactAt},
	}, qcow.Deps{
		Clock:     real.NewClock(),
		Disk:      dataDisk,
		Runner:    real.NewRunner(),
		Paths:     real.NewPaths(),
		Dialer:    real.NewUnixDialer(),
		Publisher: pub,
		Recovery:  rec,
	})
	if err != nil {
		return err
	}
	// The kernel drops an flock when the process dies, so this defer is for the paths
	// that return rather than for a crash — nothing has to clean up after one.
	defer func() { err = errors.Join(err, volumes.Close()) }()

	cp := storagev1connect.NewControlPlaneServiceClient(
		&http.Client{Timeout: *httpTimeout}, *cpURL)

	loop, err := agent.New(cfg, agent.Deps{
		Clock:        real.NewClock(),
		ControlPlane: cp,
		// The device is measured, not declared: NewDiskUsage statfs's the filesystem
		// holding --data-dir, so the capacity ADR-0013's thresholds divide by is the
		// disk's own answer and includes what other tenants of that filesystem occupy.
		Device:   agent.NewDiskUsage(dataDisk),
		Volumes:  volumes,
		Witness:  wit,
		Recorder: telemetry.Recorder(),
	})
	if err != nil {
		return err
	}
	keys.loop = loop

	slog.Info("volume-agent starting",
		"host_id", cfg.HostID, "version", version, "control_plane", *cpURL,
		"data_dir", root, "qemu_img", *qemuImg, "heartbeat_interval", cfg.HeartbeatInterval,
		"metrics_listen", *metricsListen)

	if err := loop.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	// Printed here rather than from a deferred function, so "stopped" means the loop
	// returned rather than "was asked to stop". An operator greps for this line to tell
	// a clean shutdown from a disappearance.
	slog.Info("volume-agent stopped")
	return nil
}

// backoffFor keeps the two flags in step so an operator does not have to. The Loop refuses a
// backoff longer than the interval, and that check is right; what was wrong is that the
// default backoff was a second, so `-heartbeat-interval 300ms` made the process refuse to
// start over a flag the operator never touched. An explicitly given backoff is left alone and
// still validated: silently shrinking a number somebody typed would be worse than a refusal.
func backoffFor(backoff, interval time.Duration) time.Duration {
	if flagWasSet("retry-backoff") || backoff <= interval {
		return backoff
	}
	return interval
}

// flagWasSet reports whether the operator typed this flag, which is not the same question
// as whether it holds a non-zero value: a default that a binary adjusts for itself must
// never adjust a number somebody chose.
func flagWasSet(name string) bool {
	var set bool
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = true
		}
	})
	return set
}

// loopKeys is the knot between the publisher and the loop, tied in the one place that already
// knows all three: the Control Plane is the only thing that knows a volume's DEK, agent.Loop
// is the only thing that talks to it, the Loop needs the volume manager, and the manager needs
// the publisher. It is read only from the reconcile cycle, which starts after loop.Run, so the
// assignment happens-before every read.
type loopKeys struct{ loop *agent.Loop }

func (k *loopKeys) VolumeKeys(ctx context.Context, volumeID string) (agent.VolumeKeys, error) {
	return k.loop.VolumeKeys(ctx, volumeID)
}

// serveOperatorEndpoint starts the Agent's only listening socket and returns the function
// that stops it. Before it there was no /metrics and no /healthz: every series the Agent
// collected could reach a collector over OTLP or reach nobody, and nothing here stands a
// collector up.
//
// INV-01: `cmd/` is where real implementations are constructed, and the handler is a pure
// function of what the meter provider holds, on no data path, so there is nothing for simio
// to model. A bind that fails is loud and not fatal — the endpoint belongs to the operator
// and a port already in use must not end the process; the Error line makes it detectable.
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
