// Command volume-agent runs the Volume Agent's reconciliation loop against a
// Control Plane (ADR-0018). It is deliberately thin: everything worth testing lives
// in internal/agent, and this file exists to build the *real* clock, disk, and RPC
// client and hand them over — main is the only place in the tree where a real
// implementation is constructed (INV-01).
//
// # What it serves, and what serves it
//
// The data path is QEMU's. This process prepares each volume's local qcow2 chain,
// hands the paths to whoever launches the VM, and speaks QMP to the QEMU that ends up
// there — it does not start one. internal/qcow's package comment carries the reasoning
// and the two-path contract; the short version is that spin's runner already runs the
// VMs on this host, and a second daemon supervising them is the responsibility this
// pivot exists to shed.
//
// So: this host claims its data directory (through the volume manager, which owns the
// layout inside it), reads its key, registers, heartbeats, holds a lease, learns which
// volumes it should be serving, prepares a chain for each, and reports what it saw.
//
// Nothing here talks to an object store, and there are no flags for one, because
// nothing in this build writes an object: commits, publication and recovery are the
// stages after this one, and a binary that accepted a bucket it never wrote to would be
// claiming a capability it does not have.
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
		rotateAt = flag.Int64("rotate-at-bytes", 0,
			"seal a volume's tip and start a new layer once the tip occupies this many bytes. 0 disables rotation, which is the default until v6 §11's threshold is fixed by measurement")
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

	// §15: guest data is sealed with the volume's DEK, wrapped under this KEK. A KMS is
	// built over it because there is now something that unwraps one: v6 §10 seals every
	// layer on its way to the object store, so an Agent that publishes needs the volume's
	// key in the clear for exactly as long as the transfer takes.
	//
	// Reading it is not ceremony even on a host that publishes nothing: the Control Plane
	// wraps every volume's DEK under the KEK *it* read, an Agent that reaches a different
	// key from the same file has volumes it can never open, and the two binaries
	// disagreeing about how to parse the file is a defect this repository has already
	// shipped once. The id on this line is what makes them comparable.
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

	// The publisher, if this host is configured to publish at all.
	//
	// Optional, and that is deliberate: an Agent with no object store keeps every layer
	// it seals on its own disk, which is what every lane before v6 §23.3 does and what a
	// single-machine trial does. It is not silent — the line below says which it is, and
	// "this Agent publishes nothing" is the sort of thing an operator must not have to
	// infer from the absence of commits.
	//
	// The knot: the publisher needs the volume's key, which only agent.Loop can fetch,
	// and the Loop needs the volume manager, which needs the publisher. It is resolved
	// here, in the wiring, by handing the publisher a holder that is filled in once the
	// Loop exists — rather than by giving any of the three a reason to know about the
	// other two.
	// The volume manager, and it is constructed here rather than after the Control Plane
	// client because it is what claims --data-dir: v6 §10 is one Agent per host, and two
	// incarnations preparing chains under the same paths would hand one qcow2 file to two
	// QEMUs. The claim used to be taken in this function, with a note saying the qcow2
	// manager should take it back the moment it owned the directory's layout. It does.
	//
	// The absolute path is resolved first. --data-dir is whatever an operator typed, and
	// this string is handed to *other* processes — qemu-img on a command line, and
	// whoever launches QEMU — whose working directory is not ours.
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
		slog.Warn("no object store configured: this Agent seals layers and publishes none of them, so nothing it holds survives losing this host, and it cannot serve a volume that has published commits elsewhere")
	case kms == nil:
		return errors.New("an object store is configured but -kek-file is not: a layer is sealed with the volume's DEK on the way out (v6 §10), and this Agent could not unwrap one")
	default:
		store, serr := storeFlags.Open(ctx)
		if serr != nil {
			return serr
		}
		paths := real.NewPaths()
		pub = publisher.New(store, kms, keys, paths)
		rec = recovery.New(root, *qemuImg, store, kms, keys, paths, real.NewRunner())
		wit = descriptor.EpochWitness{Store: store}
	}

	volumes, err := qcow.New(ctx, qcow.Config{
		Root:          root,
		QemuImg:       *qemuImg,
		ProbeTimeout:  *probeTimeout,
		RotateAtBytes: *rotateAt,
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

// backoffFor keeps the two flags in step so an operator does not have to.
//
// The Loop refuses a backoff longer than the interval — a retry that lands after the next
// cycle would have started is not a backoff — and that check is right. What was wrong is
// where it landed: the default backoff was a second, so `-heartbeat-interval 300ms`, which
// is an ordinary thing to want, made the process refuse to start over a flag the operator
// had never touched, naming that flag. A soak found it by moving the interval around.
//
// An explicitly given backoff is left alone and still validated. Silently shrinking a
// number somebody typed would be worse than the refusal: they asked for something, and if
// it cannot be had they should be told.
func backoffFor(backoff, interval time.Duration) time.Duration {
	explicit := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "retry-backoff" {
			explicit = true
		}
	})
	if explicit || backoff <= interval {
		return backoff
	}
	return interval
}

// loopKeys is the knot between the publisher and the loop, tied in one place.
//
// The Control Plane is the only thing that knows a volume's DEK and agent.Loop is the
// only thing that talks to it, so a publisher needs the Loop; the Loop needs the volume
// manager; the volume manager needs the publisher. Nothing about that is circular in
// *meaning* — it is three components each needing one verb from another — and this is
// where a wiring cycle belongs: in the main that already knows all three.
//
// It is read only from the reconcile cycle, which starts after loop.Run, so the
// assignment happens-before every read.
type loopKeys struct{ loop *agent.Loop }

func (k *loopKeys) VolumeKeys(ctx context.Context, volumeID string) (agent.VolumeKeys, error) {
	return k.loop.VolumeKeys(ctx, volumeID)
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
