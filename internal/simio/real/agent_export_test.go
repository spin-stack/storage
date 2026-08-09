package real_test

// A metric leaving a *process*, which is the one thing nothing here had ever observed.
//
// `TestOTLPExporterDeliversARecordedMetricToACollector` next door proves the exporter
// exports and `obs`'s own tests prove the catalog aggregates, both in-process. The Agent
// then builds the exporter from -otlp-endpoint and hands the Recorder down to every
// wal.Log. Every link is covered and **the chain was never run**: a `volume-agent` that
// built its Provider and dropped it, or wired a Recorder nothing reached, or exited
// without the flush, satisfies every one of those tests. That is exactly the seam
// CLAUDE.md's "build it thin, end to end" table is made of — `HostID` never set, the WAL
// under `<dir>/<dir>`, the queue loop that never restarted — each of them one field in a
// `main` with a well-tested package underneath.
//
// So this starts the real binary, with the flags an operator types, against a collector
// on a real socket, and asserts on what the collector decoded: a metric name, a value,
// and the label carrying the host id that was passed on the command line.
//
// **Why the test lives here.** Its honest home is `integration/e2e`, which already runs
// both binaries against a real Postgres and RustFS — and that file set belongs to another
// track this wave, so putting it there would mean editing files this lane does not own.
// Two consequences of landing it here instead, and both are arguments for it rather than
// against:
//
//   - it runs in `task test`, on every push, with no Docker and no containers. The e2e
//     lane is Docker-gated; this proof is not, because a fake Control Plane over httptest
//     and a filesystem object store are all an Agent with no volume needs;
//   - `internal/simio/real` is where the exporter's socket lives (INV-01), so the package
//     whose code opens the connection is the package whose test watches the bytes land.
//
// `internal/testinfra` has process-driving machinery and was rejected for a mechanical
// reason: it is behind `//go:build integration || e2e`, so importing it would tag this
// test out of `task test` and into a lane that needs Docker — trading the proof's reach
// for about forty lines of process handling. If this ever moves to `integration/e2e`, the
// move is a package rename and a swap of `startAgent` for `testinfra.Start`.

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/api/gen/spin/storage/v1/storagev1connect"
	"github.com/spin-stack/storage/internal/ids"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// The two series an Agent with no volume attached can produce. Everything else in
// obs.Catalog() is recorded by the WAL, the read view or a publish, and none of those
// exist until the Control Plane names a volume for this host — so a volume-less Agent is
// the smallest process that can prove the telemetry path at all, and these are the only
// two names it can prove it with.
const (
	leaseGauge     = "lease_remaining_seconds"
	failureCounter = "lease_renewal_failures_total"
)

// A successful heartbeat is the one thing an idle Agent does that reaches an instrument,
// and the value it records is the lease the Control Plane granted. That makes the
// assertion below non-circular in a way "the series exists" would not be: the Control
// Plane answers with a TTL nothing on the command line mentions, and the number the
// collector decodes has to be that one.
func TestARunningAgentDeliversItsLeaseGaugeToACollector(t *testing.T) {
	bin := buildVolumeAgent(t)

	metrics := &collector{}
	otlp := httptest.NewServer(metrics)
	defer otlp.Close()

	// 300s is deliberately nothing like the -lease-ttl flag passed below. The Agent's
	// window is the Control Plane's answer, never its own configuration (§12.2), so a
	// gauge reading ~7200 would mean the series is reporting what this host wished for
	// instead of what it was granted — and a test that only asserted "some positive
	// number arrived" would pass on it.
	cp := &controlPlane{leaseTTL: 300, reported: make(chan struct{})}
	procedure, handler := storagev1connect.NewControlPlaneServiceHandler(cp)
	mux := http.NewServeMux()
	mux.Handle(procedure, handler)
	cpsrv := httptest.NewServer(mux)
	defer cpsrv.Close()

	host := ids.New().String()
	agent := startAgent(t, bin,
		"-host-id", host,
		"-control-plane", cpsrv.URL,
		"-data-dir", t.TempDir(),
		"-vhost-socket-dir", shortSocketDir(t),
		"-object-store-dir", t.TempDir(),
		"-otlp-endpoint", otlp.URL,
		// One cycle, then an hour of silence. The Agent's first cycle runs immediately
		// and the next waits a heartbeat interval, so an interval longer than the test
		// makes the sample set deterministic: exactly one record, from exactly one
		// heartbeat. A short interval would make the decoded value a race against
		// however many cycles fitted before the signal.
		"-heartbeat-interval", "1h",
		// Must exceed the interval (agent.Config.Validate refuses otherwise), which is
		// why it cannot be the 300 above.
		"-lease-ttl", "2h",
	)

	// Waited on the *report*, not the heartbeat: report is the last RPC of a cycle and
	// the gauge is recorded inside the heartbeat, so the Control Plane seeing a report
	// means the sample is already in the SDK. Signalling on the heartbeat instead would
	// race the very line under test.
	waitFor(t, cp.reported, "the Agent never completed a reconciliation cycle", agent)

	// SIGTERM, and then the process's own exit status: the flush that carries this
	// sample happens in a deferred Shutdown after the volumes are settled, so a test
	// that killed the Agent would be asserting on nothing, and one that never waited
	// would race the POST.
	agent.stop(t)

	dp := lastPoint(t, metrics.gauges(), leaseGauge, metrics)
	// A window rather than an equality because the Agent anchors the lease to the
	// instant the request *left* and reads the remainder a round-trip later: the gauge
	// is 300 minus one loopback RPC. The window is tight enough to exclude both 7200
	// (the flag) and 0 (a lease the Agent never armed).
	if got := dp.GetAsDouble(); got <= 299 || got > 300 {
		t.Fatalf("the collector decoded %s = %v, want the Control Plane's 300s lease less one round trip; the Agent printed:\n%s",
			leaseGauge, got, agent.output())
	}
	// The label is what makes the series attributable. Asserting it against the id this
	// test put on the command line is what rules out a sample from anywhere else.
	if got := pointAttr(dp, "host"); got != host {
		t.Fatalf("the data point carried host=%q, want the -host-id this process was started with (%s)", got, host)
	}
	if got := metrics.resourceAttr("service.name"); got != "volume-agent" {
		t.Fatalf("the resource carried service.name=%q, want volume-agent — without it a collector cannot tell this binary's series from the Control Plane's", got)
	}
}

// The other half of the contract, and the case an operator actually needs: an Agent whose
// Control Plane is unreachable is the one whose telemetry has to arrive, because its logs
// are the only other evidence it is alive. It is also the harder path for the flush — the
// process spends its shutdown talking to something that is not there.
func TestAnAgentThatCannotReachItsControlPlaneStillDeliversTheFailureCounter(t *testing.T) {
	bin := buildVolumeAgent(t)

	metrics := &collector{}
	otlp := httptest.NewServer(metrics)
	defer otlp.Close()

	// A port that was bound and released, rather than a hardcoded one: it is free by
	// construction on this machine, where "nothing listens on 127.0.0.1:1" is only true
	// until a test runs as root.
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()

	host := ids.New().String()
	agent := startAgent(t, bin,
		"-host-id", host,
		"-control-plane", deadURL,
		"-data-dir", t.TempDir(),
		"-vhost-socket-dir", shortSocketDir(t),
		"-object-store-dir", t.TempDir(),
		"-otlp-endpoint", otlp.URL,
		// Both an hour, so the failed cycle is not retried before the signal and the
		// counter's value is exactly one rather than "however many the scheduler fitted".
		"-heartbeat-interval", "1h",
		"-retry-backoff", "1h",
		"-lease-ttl", "2h",
	)

	// The Agent logs this line after recording the counter and computing its backoff, so
	// the line implies the sample. It is also what an operator greps for, which is why
	// it is the thing waited on rather than a sleep.
	agent.waitForLine(t, "reconciliation cycle failed")
	agent.stop(t)

	dp := lastPoint(t, metrics.sums(), failureCounter, metrics)
	if got := dp.GetAsInt(); got != 1 {
		t.Fatalf("the collector decoded %s = %d, want 1 — one cycle ran and one heartbeat failed; the Agent printed:\n%s",
			failureCounter, got, agent.output())
	}
	if got := pointAttr(dp, "host"); got != host {
		t.Fatalf("the data point carried host=%q, want the -host-id this process was started with (%s)", got, host)
	}
}

// controlPlane is the smallest Control Plane an idle Agent will accept: it grants a
// lease, has no volumes for this host, and takes the report. Everything else on the
// service returns Unimplemented, which is the embedded handler's job — an Agent with no
// volume calls nothing else, and a stub that answered more would be describing a Control
// Plane this test does not have.
type controlPlane struct {
	storagev1connect.UnimplementedControlPlaneServiceHandler

	leaseTTL int32

	once     sync.Once
	reported chan struct{}
}

func (cp *controlPlane) Heartbeat(context.Context, *connect.Request[storagev1.HeartbeatRequest]) (*connect.Response[storagev1.HeartbeatResponse], error) {
	return connect.NewResponse(&storagev1.HeartbeatResponse{
		LeaseTtlSeconds: cp.leaseTTL,
		State:           storagev1.HostState_HOST_STATE_ACTIVE,
	}), nil
}

func (cp *controlPlane) GetDesiredState(context.Context, *connect.Request[storagev1.GetDesiredStateRequest]) (*connect.Response[storagev1.GetDesiredStateResponse], error) {
	return connect.NewResponse(&storagev1.GetDesiredStateResponse{}), nil
}

func (cp *controlPlane) ReportVolumeState(context.Context, *connect.Request[storagev1.ReportVolumeStateRequest]) (*connect.Response[storagev1.ReportVolumeStateResponse], error) {
	cp.once.Do(func() { close(cp.reported) })
	return connect.NewResponse(&storagev1.ReportVolumeStateResponse{}), nil
}

// gauges returns every Gauge data point the collector received, keyed by metric name —
// the sibling of sums() for the other half of the catalog. A Float64Gauge crosses OTLP as
// a Gauge with double points, and reading it back through the wire type is the point:
// nothing in this file may consult the SDK's own aggregation.
func (c *collector) gauges() map[string][]*metricspb.NumberDataPoint {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := map[string][]*metricspb.NumberDataPoint{}
	for _, req := range c.requests {
		for _, rm := range req.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, m := range sm.GetMetrics() {
					g := m.GetGauge()
					if g == nil {
						continue
					}
					out[m.GetName()] = append(out[m.GetName()], g.GetDataPoints()...)
				}
			}
		}
	}
	return out
}

// names lists what the collector decoded, for a failure message. "No such series" is
// useless on its own — the answer a reader needs is whether *nothing* arrived (the
// exporter never ran) or the wrong thing did (the wiring reaches a different name).
func (c *collector) names() []string {
	var out []string
	for name := range c.sums() {
		out = append(out, name)
	}
	for name := range c.gauges() {
		out = append(out, name)
	}
	return out
}

// lastPoint returns the final data point the collector holds for name. Last, because a
// gauge means its most recent value and a cumulative counter's latest export restates the
// total; either way the newest point is the one an operator would read.
func lastPoint(t *testing.T, points map[string][]*metricspb.NumberDataPoint, name string, c *collector) *metricspb.NumberDataPoint {
	t.Helper()
	dps := points[name]
	if len(dps) == 0 {
		t.Fatalf("the collector received no %s from the running Agent; it decoded %v", name, c.names())
	}
	return dps[len(dps)-1]
}

func pointAttr(dp *metricspb.NumberDataPoint, key string) string {
	for _, kv := range dp.GetAttributes() {
		if kv.GetKey() == key {
			return kv.GetValue().GetStringValue()
		}
	}
	return ""
}

// buildVolumeAgent compiles the binary under test. Built here rather than resolved out of
// _output/bin, which is what the e2e lane does: that path exists only after `task
// build:cmd`, so depending on it would make this proof skip on a developer's machine —
// and a proof that skips is how this repository shipped three documents claiming a lane
// no test performed.
func buildVolumeAgent(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "volume-agent")
	// The test's working directory is this package's, which is inside the module, so the
	// go tool resolves the import path without being told where the module root is.
	out, err := exec.Command("go", "build", "-o", bin, "github.com/spin-stack/storage/cmd/volume-agent").CombinedOutput()
	if err != nil {
		t.Fatalf("building cmd/volume-agent: %v\n%s", err, out)
	}
	return bin
}

// agentProcess is a started volume-agent and the lines it has printed. Forty lines of
// process handling rather than internal/testinfra's, for the build-tag reason at the top
// of this file.
type agentProcess struct {
	cmd     *exec.Cmd
	drained chan struct{}

	mu    sync.Mutex
	lines []string
	// printed is pinged (never blocked on) after each line, so a waiter wakes without
	// polling. Buffered at one: a ping dropped because the buffer is full is a ping the
	// waiter is about to rescan for anyway.
	printed chan struct{}
}

func startAgent(t *testing.T, bin string, args ...string) *agentProcess {
	t.Helper()

	cmd := exec.Command(bin, args...)
	// slog's default handler writes to stderr, so that is where every line this test
	// waits on appears. Stdout is left unattached: the Agent prints nothing there, and
	// attaching it would only invite an assertion on the wrong stream.
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("attaching to the Agent's stderr: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting %s: %v", bin, err)
	}

	p := &agentProcess{cmd: cmd, drained: make(chan struct{}), printed: make(chan struct{}, 1)}
	go func() {
		defer close(p.drained)
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			p.mu.Lock()
			p.lines = append(p.lines, scanner.Text())
			p.mu.Unlock()
			select {
			case p.printed <- struct{}{}:
			default:
			}
		}
	}()
	// A test that fails before stop() must not leave an Agent holding a data directory
	// and a socket for the rest of the run.
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	return p
}

// waitForLine blocks until the Agent printed a line containing want.
func (p *agentProcess) waitForLine(t *testing.T, want string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	for {
		p.mu.Lock()
		for _, line := range p.lines {
			if strings.Contains(line, want) {
				p.mu.Unlock()
				return
			}
		}
		p.mu.Unlock()

		select {
		case <-p.printed:
		case <-ctx.Done():
			t.Fatalf("the Agent never printed a line containing %q; it printed:\n%s", want, p.output())
		}
	}
}

// stop sends SIGTERM and asserts the Agent exited cleanly. The exit code is part of the
// claim: telemetry that is down must not change what the exit status says about the
// guest's data, so a flush that failed and a flush that never happened are both supposed
// to leave this at zero — which is why the metric itself, and not this, is the evidence.
func (p *agentProcess) stop(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signalling the Agent: %v", err)
	}
	// Drained before Wait: Wait closes the pipe, and the last lines the Agent prints are
	// the shutdown's — exactly the ones a failure message needs.
	<-p.drained
	if err := p.cmd.Wait(); err != nil {
		t.Fatalf("the Agent exited with %v; it printed:\n%s", err, p.output())
	}
}

func (p *agentProcess) output() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return strings.Join(p.lines, "\n")
}

// waitFor blocks on a channel the fake Control Plane closes, reporting what the Agent
// printed if it never arrives — an Agent that failed to start is otherwise a bare
// timeout with no cause in it.
func waitFor(t *testing.T, ch <-chan struct{}, what string, p *agentProcess) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatalf("%s; it printed:\n%s", what, p.output())
	}
}

// shortSocketDir is a socket directory the kernel can actually hold a bound socket in.
// t.TempDir() embeds the test's name, and these names are long enough that the path plus
// "/<volume-id>.sock" crosses sun_path's 108 bytes — so the Agent refuses it at start-up,
// which is the check cmd/volume-agent added after a bring-up spent an afternoon on an
// opaque "bind: invalid argument" retried every five seconds for ever. The old behaviour
// let these tests start an Agent whose sockets could never have bound; they did not
// notice because they assert on a metric and never serve a volume.
func shortSocketDir(t *testing.T) string {
	t.Helper()
	// usetesting is right in general and wrong here, and the exception is the whole
	// point of this helper: t.TempDir() embeds the test's name, which is what makes the
	// path too long for sun_path in the first place.
	dir, err := os.MkdirTemp("", "sk") //nolint:usetesting // t.TempDir() is the bug this works around
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}
