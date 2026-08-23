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
//
// This file is the shared harness — the deployment fixture and the helpers every
// scenario needs. It is separate from the scenarios because the scenarios arrive one at
// a time and the fixture is edited by nearly all of them: with one file, two increments
// adding two independent assertions are a three-way merge over 800 lines; with this
// split they are two new files and, at most, one appended helper here.
package e2e

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

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
	kekFile  string
	kekID    string
	cp       *testinfra.Process
	agentBin string
	agentEnv []string
	qemuImg  string
}

// start brings up everything except the Agent — the Agent is started per test, because
// what these scenarios are about is the process: a second one refused, one stopped.
func start(t *testing.T) *deployment {
	t.Helper()

	cpBin := testinfra.Binary(t, "control-plane")
	agentBin := testinfra.Binary(t, "volume-agent")
	// The Agent creates and inspects every qcow2 chain by running the pinned qemu-img
	// (v6 §7 forbids a parser of our own), and refuses to start without it — so this
	// lane needs the artefact `task build:qemu` produces. testinfra.Binary fails naming
	// that task rather than skipping: a lane that quietly declined to start the Agent
	// would be the gate reporting success for work it did not do.
	qemuImg := testinfra.Binary(t, "qemu-img")

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
		kekFile:  kekFile,
		kekID:    crypto.KEKID([crypto.DEKSize]byte(kek)),
		agentBin: agentBin, agentEnv: env, qemuImg: qemuImg,
	}
	if err := os.MkdirAll(d.dataDir, 0o755); err != nil {
		t.Fatal(err)
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
			// The device-pressure cordon, out of this lane's way and deliberately so.
			// This lane's Agents point --data-dir at the machine's temporary directory,
			// and the Agent measures the filesystem holding it *including other
			// tenants* — so on a CI runner, whose disk is 87% full, every host cordoned
			// itself on its first heartbeat and every placement failed with "no host
			// with capacity". That is the product behaving correctly and the lane
			// depending on a developer's roomy /tmp, which is why it had never failed
			// here. What this lane tests is the two binaries as processes; the band
			// itself is tested where it belongs, against synthetic heartbeats in
			// internal/cpserver.
			"-cordon-used-ratio", "1",
			"-uncordon-used-ratio", "0.99",
			"-s3-create-bucket",
		}, d.storeArgs()...),
		Env: env,
	})
	d.cp.WaitForLine(t, "control-plane elected", startup)
	return d
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
		// No object-store flags: this Agent takes none. Stage 1 is qcow2, local only,
		// and a binary that accepted a bucket it never writes to would be claiming a
		// capability it does not have.
		Args: []string{
			"-host-id", d.hostID,
			"-control-plane", d.cpURL,
			"-data-dir", d.dataDir,
			"-kek-file", d.kekFile,
			"-qemu-img", d.qemuImg,
			"-heartbeat-interval", "1s",
		},
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
