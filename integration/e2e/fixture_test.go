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
	"net/http"
	"os"
	"path/filepath"
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
			// The device-pressure cordon, out of this lane's way and deliberately so.
			// This lane's Agents point --data-dir at the machine's temporary directory,
			// and the Agent measures the filesystem holding it *including other
			// tenants* — so on a CI runner, whose disk is 87% full, every host cordoned
			// itself on its first heartbeat and every placement failed with "no host
			// with capacity". That is the product behaving correctly and the lane
			// depending on a developer's roomy /tmp, which is why it had never failed
			// here. What this lane tests is placement, cloning and the shutdown
			// publish; the band itself is tested where it belongs, against synthetic
			// heartbeats in internal/cpserver.
			"-cordon-used-ratio", "1",
			"-uncordon-used-ratio", "0.99",
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
// requestSnapshot records a snapshot request through the real binary and returns
// nothing: the id is not printed back on purpose, so the assertions below have to find
// the snapshot the way anything else would — by looking in the bucket.
func (d *deployment) requestSnapshot(t *testing.T, volumeID string) {
	t.Helper()
	p := testinfra.Start(t, testinfra.ProcessConfig{
		Name: "snapshot",
		Path: testinfra.Binary(t, "control-plane"),
		Args: append([]string{
			"-database-url", d.dsn,
			"-holder-id", "cp-snapshot",
			"-snapshot-volume", volumeID,
		}, d.storeArgs()...),
		Env: d.agentEnv,
	})
	if err := p.Wait(t, startup); err != nil {
		t.Fatalf("requesting a snapshot of %s: %v", volumeID, err)
	}
}

// placementArgs is the fill ceiling every *placing* one-shot needs in this lane, and it
// exists as one helper rather than as a literal at each call site because forgetting it
// is invisible until CI: a runner's disk is 87% used, placement.Admits refuses above 85%,
// and the command exits with "no host with capacity" — which reads like a fleet problem
// and is a lane that did not say what fleet it wanted.
//
// **It is the sibling of the cordon band the serving Control Plane is started with, and
// the two live in different places.** The band is a flag on the process that serves;
// this is a flag on whichever process runs the placing command. An operator who tunes one
// and not the other gets a fleet that never cordons and still refuses to place — which is
// exactly what happened here after the band was fixed and this was not. Recorded in
// TRACK-D.md as a shape worth changing: placement policy that lives in two processes'
// flags rather than in the catalog is policy nobody can read back.
func (d *deployment) placementArgs() []string {
	return []string{"-max-used-ratio", "1"}
}

// cloneSnapshot runs `control-plane -clone-snapshot`, which creates a volume from a
// published snapshot and exits. The host is not an argument: §20's placement order
// decides it, and that decision is the point of the command.
func (d *deployment) cloneSnapshot(t *testing.T, snapshotID string) {
	t.Helper()
	p := testinfra.Start(t, testinfra.ProcessConfig{
		Name: "clone",
		Path: testinfra.Binary(t, "control-plane"),
		Args: append([]string{
			"-database-url", d.dsn,
			"-holder-id", "cp-clone",
			"-clone-snapshot", snapshotID,
		}, append(d.placementArgs(), d.storeArgs()...)...),
		Env: d.agentEnv,
	})
	if err := p.Wait(t, startup); err != nil {
		t.Fatalf("cloning snapshot %s: %v", snapshotID, err)
	}
}

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

// volumeKeys is everything the bucket holds for one volume: its manifests under
// image/<volume>/ and its data under chunks/<volume>/.
//
// Two prefixes and not one, because the chunk store moved out of the volume's prefix when
// it became a lineage's. Every "nothing has been published yet" assertion in these lanes
// depends on covering both: a guest WRITE that PUT a chunk (INV-18 says it PUTs nothing)
// would otherwise land under a prefix no assertion looks at.
//
// chunks/<volume>/ is this volume's own lineage, which is right for every volume these
// lanes create — a clone's data is under its *parent's* id, and the one clone lane asserts
// on the manifest key rather than on a count.
func volumeKeys(t *testing.T, d *deployment, volumeID string) []string {
	t.Helper()
	return append(storeKeys(t, d, "image/"+volumeID+"/"), storeKeys(t, d, "chunks/"+volumeID+"/")...)
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
