//go:build e2e

// Package e2e runs the deployment, not the library.
//
// Everything else in this repository drives Go types in-process — which is where the
// correctness proofs belong, and which is exactly why nothing had ever noticed the two
// things a deployment is actually made of: a `control-plane` and a `volume-agent` that
// were started with flags, found each other over a socket, and can be killed. The gap
// this closes is not a missing invariant; it is that no invariant was ever checked
// against the binaries.
//
// It is `-tags e2e` and not part of `task test` because it starts containers and
// processes and takes tens of seconds. `task ci:full` runs it.
package e2e

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/api/gen/spin/storage/v1/storagev1connect"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/testinfra"
)

const (
	startup = 60 * time.Second
	bucket  = "spin-e2e"
)

// deployment is the whole thing, running: Postgres, RustFS, a Control Plane process and
// an Agent process, plus the directories the Agent was given.
type deployment struct {
	dsn      string
	store    testinfra.ObjectStoreBackend
	cpURL    string
	hostID   string
	dataDir  string
	sockDir  string
	kekFile  string
	kekID    string
	cp       *testinfra.Process
	agentBin string
	agentEnv []string
}

// start brings up everything except the Agent — the Agent is started per test, because
// several of them are about what happens when it is killed and started again.
func start(t *testing.T) *deployment {
	t.Helper()

	cpBin := testinfra.Binary(t, "control-plane")
	agentBin := testinfra.Binary(t, "volume-agent")

	dsn := testinfra.Postgres(t)
	store := testinfra.RustFS(t, os.Getenv("RUSTFS_IMAGE"))

	// Credentials in the environment, never on the command line: storecfg takes them
	// from the SDK's default chain precisely so a secret does not land in `ps` output
	// or in a systemd unit (see internal/storecfg).
	env := []string{
		"AWS_ACCESS_KEY_ID=" + store.AccessKey,
		"AWS_SECRET_ACCESS_KEY=" + store.SecretKey,
		"AWS_REGION=" + store.Region,
	}
	dir := t.TempDir()
	kekFile := filepath.Join(dir, "kek")
	kek := bytes.Repeat([]byte{0x3F}, crypto.DEKSize)
	if err := os.WriteFile(kekFile, kek, 0o600); err != nil {
		t.Fatal(err)
	}

	d := &deployment{
		dsn: dsn, store: store,
		hostID:   ids.New().String(),
		dataDir:  filepath.Join(dir, "data"),
		sockDir:  shortSocketDir(t),
		kekFile:  kekFile,
		kekID:    crypto.KEKID([crypto.DEKSize]byte(kek)),
		agentBin: agentBin, agentEnv: env,
	}
	for _, p := range []string{d.dataDir, d.sockDir} {
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	port := freePort(t)
	d.cpURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	d.cp = testinfra.Start(t, testinfra.ProcessConfig{
		Name: "control-plane",
		Path: cpBin,
		// -s3-create-bucket only here: the bucket is created once, by the first
		// process that needs it, and every later process must find it rather than
		// invent one — which is why the flag is off by default (storecfg).
		Args: append([]string{
			"-listen", fmt.Sprintf("127.0.0.1:%d", port),
			"-database-url", dsn,
			"-holder-id", "cp-e2e",
			"-lease-ttl", "30s",
			"-s3-create-bucket",
		}, d.storeArgs()...),
		Env: env,
	})
	d.cp.WaitForLine(t, "control-plane elected", startup)
	return d
}

// shortSocketDir is a temporary directory for the vhost-user sockets that is *not*
// derived from the test's name.
//
// A Unix socket path is bounded by sun_path, 108 bytes on Linux, and bind(2) reports
// nothing more helpful than EINVAL when it overflows. t.TempDir() embeds the test's name,
// so `<tmp>/<TestName><digits>/001/run/<uuid>.sock` puts the test's own identifier in the
// path — and a volume id is 36 characters before the suffix. This lane was one long test
// name away from failing for a reason no error message would explain, and it took exactly
// one to find out: TestAGuestMakesTheDeploymentWriteADurableObject produced a 112-byte
// path.
//
// Named for the socket and not the test, so the length depends on nothing a future test
// can change.
func shortSocketDir(t *testing.T) string {
	t.Helper()
	// t.TempDir() is what usetesting wants and it is exactly what does not work here:
	// it derives the path from the test's name, which is the overflow.
	dir, err := os.MkdirTemp("", "spinsock") //nolint:usetesting // sun_path is 108 bytes; see above
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// storeArgs repeats the object-store flags for a second process.
func (d *deployment) storeArgs() []string {
	return []string{
		"-s3-bucket", bucket,
		"-s3-endpoint", d.store.Endpoint,
		"-s3-region", d.store.Region,
	}
}

// startAgent starts a volume-agent against this deployment.
func (d *deployment) startAgent(t *testing.T, name string) *testinfra.Process {
	t.Helper()
	p := testinfra.Start(t, testinfra.ProcessConfig{
		Name: name,
		Path: d.agentBin,
		Args: append([]string{
			"-host-id", d.hostID,
			"-control-plane", d.cpURL,
			"-data-dir", d.dataDir,
			"-vhost-socket-dir", d.sockDir,
			"-kek-file", d.kekFile,
			"-heartbeat-interval", "1s",
		}, d.storeArgs()...),
		Env: d.agentEnv,
	})
	p.WaitForLine(t, "key-encryption key loaded", startup)
	return p
}

// seedVolume runs `control-plane -seed-volume`, which provisions one volume and exits.
func (d *deployment) seedVolume(t *testing.T) {
	t.Helper()
	p := testinfra.Start(t, testinfra.ProcessConfig{
		Name: "seed",
		Path: testinfra.Binary(t, "control-plane"),
		Args: append([]string{
			"-database-url", d.dsn,
			"-holder-id", "cp-seed",
			"-seed-volume",
			"-seed-host", d.hostID,
			"-seed-size", "1073741824",
			"-kek-file", d.kekFile,
		}, d.storeArgs()...),
		Env: d.agentEnv,
	})
	if err := p.Wait(t, startup); err != nil {
		t.Fatalf("seeding a volume: %v", err)
	}
}

// waitForHost blocks until the Agent's heartbeat has registered this host. Nothing can
// be provisioned for a host the catalog does not know: volumes.primary_host_id is a
// foreign key, and the fleet learns a host exists by being told, not by configuration.
func (d *deployment) waitForHost(t *testing.T) {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), d.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	waitFor(t, startup, "the host to register itself", func() bool {
		var n int
		if err := pool.QueryRow(t.Context(),
			`SELECT count(*) FROM hosts WHERE host_id = $1`, d.hostID).Scan(&n); err != nil {
			return false
		}
		return n == 1
	})
}

func (d *deployment) cpClient() storagev1connect.ControlPlaneServiceClient {
	return storagev1connect.NewControlPlaneServiceClient(
		&http.Client{Timeout: 10 * time.Second}, d.cpURL)
}

// TestTheDeploymentServesAVolume is the slice, end to end, through the binaries: a
// Control Plane that was elected, a volume provisioned by the real provisioning path,
// an Agent that heartbeats, is handed that volume, and binds a socket for it.
//
// The assertion that matters is the *socket*: it is the only artefact that proves the
// Agent got as far as building a runtime. A heartbeat proves the process started; a
// desired-state answer proves the RPC works; only a bound socket proves a volume was
// opened, its keys unwrapped and its WAL created.
func TestTheDeploymentServesAVolume(t *testing.T) {
	d := start(t)
	// The Agent first: a volume references its primary host, and the host row is
	// created by the Agent's own heartbeat. Provisioning for a host the fleet has
	// never seen fails on the foreign key, which is the catalog saying the same thing.
	agent := d.startAgent(t, "volume-agent")
	d.waitForHost(t)
	d.seedVolume(t)

	volumeID := waitForServedVolume(t, d)
	sock := filepath.Join(d.sockDir, volumeID+".sock")
	waitFor(t, 30*time.Second, fmt.Sprintf("the socket for %s", volumeID), func() bool {
		_, err := os.Stat(sock)
		return err == nil
	})

	// And the Agent says what it built. Asserting on the log line rather than on a WAL
	// directory is deliberate: segments are created by the first append, and no guest
	// has written yet — a directory assertion would be waiting for something the
	// system correctly does not do.
	agent.WaitForLine(t, "serving volume", 30*time.Second)
	assertLogField(t, agent, "serving volume", "encrypted=true")

	// The durability scheduler must be running. It is refused when the Agent has no
	// host id, and the binary did not pass one until this lane read the line saying so
	// — every volume on every real Agent would have grown its WAL for ever.
	for _, line := range agent.Output() {
		if strings.Contains(line, "no durability scheduler") {
			t.Fatalf("this volume will never reclaim a byte: %s", line)
		}
	}
}

// assertLogField fails unless the process printed a line containing both needles. slog
// writes key=value, so this reads a structured field without parsing the format.
func assertLogField(t *testing.T, p *testinfra.Process, line, field string) {
	t.Helper()
	for _, l := range p.Output() {
		if strings.Contains(l, line) && strings.Contains(l, field) {
			return
		}
	}
	t.Fatalf("no %q line carried %q; got:\n%s", line, field, strings.Join(p.Output(), "\n"))
}

// TestASecondAgentRefusesTheSameDataDir closes DEV-0014 where it is observable: two
// real processes and a real flock(2).
//
// §10 opens with "un proceso por host" and nothing enforced it. Two live Agents on one
// --data-dir both resume the same segment files and both append to them, and
// hostio.Listen unlinks a stale socket before binding — so the second silently steals
// the guest from the first rather than failing to bind. The simulated lock in the DST
// harness would only be checking the simulation's own map; what makes this true in
// production is the kernel, and this is the only place that is exercised.
//
// The first Agent is left running on purpose. A lock that refused *after* the holder
// died would be the worse bug: ADR-0024 requires a kill -9'd Agent's replacement to
// start, which is what the restart test below covers.
func TestASecondAgentRefusesTheSameDataDir(t *testing.T) {
	d := start(t)
	first := d.startAgent(t, "agent-1")
	d.waitForHost(t)

	second := testinfra.Start(t, testinfra.ProcessConfig{
		Name: "agent-2",
		Path: d.agentBin,
		Args: append([]string{
			"-host-id", ids.New().String(),
			"-control-plane", d.cpURL,
			"-data-dir", d.dataDir, // the same one agent-1 holds
			"-vhost-socket-dir", d.sockDir,
			"-kek-file", d.kekFile,
		}, d.storeArgs()...),
		Env: d.agentEnv,
	})
	if err := second.Wait(t, 30*time.Second); err == nil {
		t.Fatal("a second Agent started against a data directory another one holds (§10, DEV-0014)")
	}
	// The message has to name the directory: the operator's next action is to find the
	// process holding it, and an errno would not help them do that.
	var said bool
	for _, line := range second.Output() {
		if strings.Contains(line, d.dataDir) && strings.Contains(line, "already using") {
			said = true
		}
	}
	if !said {
		t.Fatalf("the refusal did not name %s:\n%s", d.dataDir, strings.Join(second.Output(), "\n"))
	}

	// And the first is untouched — it still holds the directory and is still serving.
	d.seedVolume(t)
	first.WaitForLine(t, "serving volume", 60*time.Second)
}

// TestTheAgentWritesWhereItWasTold. The Disk is rooted at --data-dir and the manager's
// DataDir is a path inside *that* namespace, so passing the operator's absolute path in
// both places put every WAL under <data-dir>/<data-dir>/wal/... — consistent,
// restart-safe, and nowhere near where the operator was told to look. Nothing
// in-process could see it: every test gives the manager a Disk spanning a whole
// filesystem, where the two paths agree.
//
// The lock file is the witness because it is created at start-up, before any guest has
// written a byte: a WAL directory would only appear on the first append.
func TestTheAgentWritesWhereItWasTold(t *testing.T) {
	d := start(t)
	agent := d.startAgent(t, "volume-agent")
	agent.WaitForLine(t, "volume-agent starting", startup)

	waitFor(t, 30*time.Second, "the agent to claim its data directory", func() bool {
		_, err := os.Stat(filepath.Join(d.dataDir, "agent.lock"))
		return err == nil
	})
	// The doubled path, spelled out rather than globbed: <data-dir> holding a copy of
	// its own absolute path is the exact shape of the bug.
	doubled := filepath.Join(d.dataDir, strings.TrimPrefix(d.dataDir, string(filepath.Separator)))
	if _, err := os.Stat(doubled); err == nil {
		t.Fatalf("the Agent wrote under %s: --data-dir is being applied twice", doubled)
	}
}

// TestBothBinariesAgreeOnTheKEK is the regression this lane was built too late to
// prevent and exists to stop repeating.
//
// The Control Plane wraps a volume's DEK under the KEK it read; the Agent unwraps it
// under the KEK *it* read, and refuses the volume unless the ids match. Those were two
// separate readers with different rules until 2026-08-02 — a hex-encoded key file was a
// working Control Plane and a dead Agent — and the id was a flag on one side and a hash
// on the other. Nothing in-process could see it: it is only wrong once both binaries
// read the same file.
func TestBothBinariesAgreeOnTheKEK(t *testing.T) {
	d := start(t)
	agent := d.startAgent(t, "volume-agent")
	d.waitForHost(t)
	d.seedVolume(t)

	// What the Control Plane recorded, read straight from the catalog.
	pool, err := pgxpool.New(t.Context(), d.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var kekID string
	var keyID int64
	if err := pool.QueryRow(t.Context(),
		`SELECT kek_id, dek_key_id FROM volumes LIMIT 1`).Scan(&kekID, &keyID); err != nil {
		t.Fatal(err)
	}
	if kekID != d.kekID {
		t.Fatalf("the Control Plane wrapped under %q; the same file hashes to %q", kekID, d.kekID)
	}
	if keyID == 0 {
		t.Fatal("the catalog holds a DEK with no version (§15.1)")
	}

	// And the Agent opens it. It refuses a volume whose kek_id is not its own and a
	// DEK whose version does not authenticate, so a served volume *is* the assertion
	// that both binaries reached the same key from the same file.
	volumeID := waitForServedVolume(t, d)
	waitFor(t, 30*time.Second, "the volume's socket", func() bool {
		_, err := os.Stat(filepath.Join(d.sockDir, volumeID+".sock"))
		return err == nil
	})
	for _, line := range agent.Output() {
		if strings.Contains(line, "unwrapping the DEK") || strings.Contains(line, "is wrapped under KEK") {
			t.Fatalf("the Agent could not use the key the Control Plane wrapped: %s", line)
		}
	}
}

// TestAKilledAgentReAttachesAtTheSameEpoch is ADR-0024 as a deployment experiences it:
// SIGKILL, no cleanup, and a new process that finds a WAL on disk and a Control Plane
// still listing the volume at the epoch it was granted.
//
// The DST arm proves the numbering cannot collide. What only this lane can show is that
// the *process* comes back at all — that `Apply` resumes rather than refusing, and that
// nothing in the start-up path needs a clean shutdown to have happened.
func TestAKilledAgentReAttachesAtTheSameEpoch(t *testing.T) {
	d := start(t)
	first := d.startAgent(t, "agent-1")
	d.waitForHost(t)
	d.seedVolume(t)
	volumeID := waitForServedVolume(t, d)
	sock := filepath.Join(d.sockDir, volumeID+".sock")
	waitFor(t, 30*time.Second, "the first socket", func() bool {
		_, err := os.Stat(sock)
		return err == nil
	})
	epochBefore := volumeEpoch(t, d, volumeID)

	first.Kill(t)

	second := d.startAgent(t, "agent-2")
	second.WaitForLine(t, "key-encryption key loaded", startup)
	waitFor(t, 60*time.Second, "the volume served again", func() bool {
		for _, v := range servedVolumes(t, d) {
			if v.GetVolumeId() == volumeID {
				return true
			}
		}
		return false
	})
	if got := volumeEpoch(t, d, volumeID); got != epochBefore {
		t.Fatalf("the epoch moved across a restart: %d -> %d (ADR-0024 says it must not)", epochBefore, got)
	}
}

// --- helpers -------------------------------------------------------------------

func servedVolumes(t *testing.T, d *deployment) []*storagev1.DesiredVolume {
	t.Helper()
	resp, err := d.cpClient().GetDesiredState(t.Context(),
		connect.NewRequest(&storagev1.GetDesiredStateRequest{HostId: d.hostID}))
	if err != nil {
		return nil
	}
	return resp.Msg.GetVolumes()
}

func waitForServedVolume(t *testing.T, d *deployment) string {
	t.Helper()
	var id string
	waitFor(t, 30*time.Second, "a volume in the desired state", func() bool {
		vols := servedVolumes(t, d)
		if len(vols) == 0 {
			return false
		}
		id = vols[0].GetVolumeId()
		return true
	})
	return id
}

func volumeEpoch(t *testing.T, d *deployment, volumeID string) int64 {
	t.Helper()
	for _, v := range servedVolumes(t, d) {
		if v.GetVolumeId() == volumeID {
			return v.GetEpoch()
		}
	}
	t.Fatalf("volume %s is not in the desired state", volumeID)
	return 0
}

// waitFor polls until cond holds. Polling is right here and a sleep is not: the thing
// being waited on is another *process* reaching a state, and the only alternative to
// asking is guessing how long it takes.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// freePort asks the kernel for a port and closes it. There is a race between closing
// and the Control Plane binding, and it is the standard one: the alternative is a
// hardcoded port, which fails when two lanes run at once.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

// TestBothBinariesShutDownCleanly asks each binary to stop the way a supervisor does —
// SIGINT — and checks it actually stops. Until this test existed
// `testinfra.Process.Stop` had **no caller anywhere in the tree**: every lane either
// waited for a process that exits on its own or SIGKILLed one, so the entire clean
// shutdown path (`signal.NotifyContext`, the Control Plane's `-shutdown-grace`, the
// Agent's `defer volumes.Close()`) was code no test had ever asked to run.
//
// What it asserts is deliberately narrow, and narrower than the first version of it.
// That version also claimed that a second Agent claiming the same `--data-dir`
// afterwards proved `volumes.Close()` had run. It proves nothing: the kernel drops a
// flock when the process exits, however it exits, so the assertion passed with
// `volumes.Close()` deleted. Planting that bug is what found it. What is left is what a
// supervisor can actually tell apart:
//
//   - the process exits within the timeout after SIGINT (asserted inside Stop — a
//     daemon that ignores the signal hangs here, which is the bug the Control Plane
//     really had when SIGTERM was missing from its NotifyContext);
//   - it exits zero rather than crashing on the way down (also inside Stop);
//   - it prints its parting line, so an operator reading logs sees a shutdown rather
//     than a disappearance.
//
// Proven able to fail: dropping `os.Interrupt` from either binary's
// `signal.NotifyContext` makes Stop time out.
func TestBothBinariesShutDownCleanly(t *testing.T) {
	d := start(t)
	agent := d.startAgent(t, "agent-1")
	d.waitForHost(t)
	d.seedVolume(t)
	agent.WaitForLine(t, "serving volume", 60*time.Second)

	// Stopped while it is serving a volume, which is the state a supervisor restarts in
	// and the one where Close has runtimes to tear down.
	agent.Stop(t, 30*time.Second)
	assertSaid(t, agent, "volume-agent stopped")

	d.cp.Stop(t, 30*time.Second)
	assertSaid(t, d.cp, "control-plane stopped")
}

// assertSaid fails with the whole stream, because "the line is missing" and "the process
// printed a panic instead" need different fixes and the line alone cannot tell them apart.
func assertSaid(t *testing.T, p *testinfra.Process, want string) {
	t.Helper()
	for _, line := range p.Output() {
		if strings.Contains(line, want) {
			return
		}
	}
	t.Fatalf("%s never printed %q:\n%s", p.Name, want, strings.Join(p.Output(), "\n"))
}

// TestAGuestMakesTheDeploymentWriteADurableObject is the assertion this lane did not
// have: an artefact that exists only if the *binary* did its work.
//
// Everything else here stops at os.Stat(sock) and two log lines. That leaves the closure
// governing every durable ACK —
//
//	Lease: func() bool { return loop != nil && loop.LeaseValid() }   (cmd/volume-agent)
//
// — able to answer false for ever, or true for ever, with the whole lane still green. The
// QEMU lane did not cover it either: integration/vhost builds an agent.VolumeManager
// *in process*, with a filesystem-backed store and a lease it grants itself, so nothing
// anywhere exercised the binary's flags, its S3 credentials or its Control-Plane-driven
// lease on the data path.
//
// So a real Linux kernel drives the socket a real volume-agent bound, against the real
// Postgres and the real RustFS this deployment already starts, and the assertion is a key
// under wal/<volume-id>/ in the bucket. Nothing simulated is left in the path.
//
// A simulated front-end would have been cheaper and would run without QEMU. It was
// rejected: DEV-0018 is exactly the shape of bug a fake front-end hides — it configured
// the device once, every real boot configures it twice, and the queue loop stayed parked
// on the first kick.
func TestAGuestMakesTheDeploymentWriteADurableObject(t *testing.T) {
	// Skip before anything is built, and loudly: a lane whose inputs are missing has
	// not found a defect. CI builds them, so CI does not skip (ADR-0025).
	kernel, initramfs := testinfra.GuestImages(t)
	_, _ = testinfra.QEMUPaths(t)

	d := start(t)
	agent := d.startAgent(t, "volume-agent")
	d.waitForHost(t)
	d.seedVolume(t)

	volumeID := waitForServedVolume(t, d)
	sock := filepath.Join(d.sockDir, volumeID+".sock")
	waitFor(t, 30*time.Second, fmt.Sprintf("the socket for %s", volumeID), func() bool {
		_, err := os.Stat(sock)
		return err == nil
	})
	agent.WaitForLine(t, "serving volume", 30*time.Second)

	// Nothing may be in the bucket for this volume yet: a guest's WRITE is 0 PUTs
	// (§5.3, INV-18), and if objects appeared here the assertion below would prove
	// nothing about what the stop publishes.
	if keys := storeKeys(t, d, "image/"+volumeID+"/"); len(keys) != 0 {
		t.Fatalf("%d object(s) under image/%s/ before the guest ran: the assertion would be vacuous:\n%v",
			len(keys), volumeID, keys)
	}

	code, out := testinfra.RunLinuxGuest(t, t.Context(), sock, kernel, initramfs)
	switch {
	case strings.Contains(out, "GUESTINIT-FAIL"):
		t.Fatalf("the guest reported a failure:\n%s", testinfra.VerdictLines(out))
	case !strings.Contains(out, "GUESTINIT-PASS"):
		t.Fatalf("the guest never reported a verdict (exit %d):\n%s", code, testinfra.VerdictLines(out))
	}

	// The fsync ACKed locally and put nothing in the bucket — that is §14.8, and
	// asserting it here is what keeps the next assertion meaningful.
	if keys := storeKeys(t, d, "image/"+volumeID+"/"); len(keys) != 0 {
		t.Fatalf("a guest's fsync published %d object(s); §14.8 says the ACK is local:\n%v", len(keys), keys)
	}

	// Stopping is what publishes (ADR-0026), and this is the artefact V1's whole
	// durability contract produces: an image that exists only if the *binary* resolved
	// its credentials, sealed the chunks and CASed the manifest.
	agent.Stop(t, 30*time.Second)

	keys := storeKeys(t, d, "image/"+volumeID+"/")
	if len(keys) == 0 {
		t.Fatalf("the Agent stopped and the bucket holds nothing under image/%s/ — "+
			"the session's writes exist only on a host that has released them:\n%s",
			volumeID, testinfra.VerdictLines(out))
	}
	t.Logf("a real kernel's writes left %d object(s) under image/%s/: %v", len(keys), volumeID, keys)
}

// storeKeys lists what is actually in the bucket under a prefix. It asks the object store
// directly rather than the Agent, because the Agent reporting its own state is the claim
// under test, not the evidence for it.
func storeKeys(t *testing.T, d *deployment, prefix string) []string {
	t.Helper()
	out, err := d.store.Client().ListObjectsV2(t.Context(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		t.Fatalf("listing %s: %v", prefix, err)
	}
	keys := make([]string, 0, len(out.Contents))
	for _, o := range out.Contents {
		keys = append(keys, aws.ToString(o.Key))
	}
	return keys
}
