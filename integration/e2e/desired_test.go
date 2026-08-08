//go:build e2e

package e2e

import (
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/testinfra"
)

// emptyDesiredLine is what the Agent prints, every cycle, while the Control Plane is
// telling it that it holds nothing and it is declining to believe that means "stop".
const emptyDesiredLine = "the control plane listed no volumes for this host"

// TestAnEmptyDesiredStateDoesNotTearDownTheHost is C8, at the seam.
//
// `VolumeManager.Apply` stopped every volume the desired state did not list, and under
// ADR-0026 stopping a volume is what publishes it — so a Control Plane that answered
// `GetDesiredState` with an empty list made this host upload every session it held and
// take every guest's device away, in one cycle, with nothing anywhere reporting an error.
// An empty list is not a rare shape: a Control Plane restarted against an empty database
// sends it, a `GetDesiredState` that returns no rows because of a bug sends it, and an
// Agent whose `-host-id` stopped matching after a config change is sent it too.
//
// **The Control Plane here is real and is not confused about anything except the list.**
// Everything the Agent asks goes to the process started by `start`, except
// `GetDesiredState`, which a reverse proxy answers with an empty response. So the volume
// is still this host's — `volumes.primary_host_id` is unchanged, the heartbeat is
// renewed, and the Agent's *reports* of the volume keep coming back ACCEPTED, which is
// what keeps the fencing path (`Loop.fence`) from being the thing under test. The only
// message saying "you hold nothing" is the empty list, and the assertion is that it
// costs the guest nothing.
//
// That the proxy is needed at all is the finding that shaped this test. With the real
// Control Plane there is no way to reach an empty desired state that does not also refuse
// the report: `ListVolumesByHost` filters on `volumes.primary_host_id` and
// `cpserver.applyReport` refuses on the same column, so a genuine detach empties the list
// *and* names the volume in the report. That is exactly why the guard in `Apply` is safe
// — see TestADetachedVolumeIsStillStoppedAndPublished, which is the other half of this
// pair and would go red if the guard had made a drain impossible.
func TestAnEmptyDesiredStateDoesNotTearDownTheHost(t *testing.T) {
	d := start(t)
	cp := newControlPlaneProxy(t, d.cpURL)
	agent := d.startAgentAgainst(t, "agent-1", cp.url)

	d.waitForHost(t)
	d.seedVolume(t)
	volumeID := waitForServedVolume(t, d)
	agent.waitForLine(t, "serving volume", startup)
	// The read view has to have landed while everything was healthy. A volume that never
	// resolved its base refuses to publish for an entirely different reason
	// (agent.ErrNoReadView), and without this wait the scenario could pass having proven
	// that rule instead of this one.
	agent.waitForLine(t, "read view recovered from the object store", startup)

	socket := filepath.Join(d.sockDir, volumeID+".sock")
	if _, err := os.Stat(socket); err != nil {
		t.Fatalf("the volume's vhost socket is not there before the test begins: %v", err)
	}
	if keys := volumeKeys(t, d, volumeID); len(keys) != 0 {
		t.Fatalf("%d object(s) for volume %s before the desired state went empty: the assertions below would be vacuous:\n%v",
			len(keys), volumeID, keys)
	}

	cp.listNothing()

	// Long enough to cover many cycles at -heartbeat-interval 1s, and the check is made
	// throughout rather than once at the end: with the guard removed the teardown lands
	// within a cycle or two, and catching it early is what makes the failure message
	// name the moment rather than the aftermath.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if keys := volumeKeys(t, d, volumeID); len(keys) != 0 {
			t.Fatalf("the Control Plane listed no volumes and the Agent stopped volume %s and published %v — an empty list is not a detach order, and this host is still the volume's writer",
				volumeID, keys)
		}
		if _, err := os.Stat(socket); err != nil {
			t.Fatalf("the guest's device is gone: %s no longer exists after the Control Plane listed nothing (%v)", socket, err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// It said so, repeatedly. A host in this state needs somebody to look at it, and a
	// line printed once at the transition is a line printed hours before whoever is
	// looking arrives.
	agent.waitForCount(t, emptyDesiredLine, 3, 30*time.Second)

	// And when the Control Plane starts answering properly again, the volume is *still
	// the same runtime*: nothing was restarted behind the guest's back, which is the
	// difference between "kept serving" and "torn down and rebuilt".
	cp.listEverything()
	// The guard is a guard and not a permanent refusal to reconcile: the moment the
	// Control Plane names the volume again, the line stops. Watched rather than slept
	// on — what is being observed is a count that stops moving, and 30 polls of 100ms
	// is three cycles at -heartbeat-interval 1s.
	last, stable := agent.count(emptyDesiredLine), 0
	waitFor(t, 30*time.Second, "the Agent to stop reporting an empty desired state", func() bool {
		n := agent.count(emptyDesiredLine)
		if n != last {
			last, stable = n, 0
			return false
		}
		stable++
		return stable >= 30
	})

	if n := agent.count("serving volume"); n != 1 {
		t.Fatalf("the Agent printed %q %d time(s), want 1: the volume was restarted rather than kept:\n%s",
			"serving volume", n, strings.Join(agent.output(), "\n"))
	}
	if keys := volumeKeys(t, d, volumeID); len(keys) != 0 {
		t.Fatalf("volume %s has published %v: the session was published while the volume never stopped being this host's", volumeID, keys)
	}
}

// TestADetachedVolumeIsStillStoppedAndPublished is the counterweight, and without it the
// guard above would be indistinguishable from "this Agent never lets go".
//
// A real detach clears `volumes.primary_host_id`, which empties this host's desired state
// — the message `Apply` now ignores — *and* makes the Control Plane refuse this host's
// report of that volume, which `Loop.fence` acts on by stopping the runtime and
// publishing its image. So a drain still arrives, in the same cycle it always did,
// through the door that names the volume instead of the one that names nothing.
//
// It is asserted on the bucket and on the socket, because those are what a detached
// volume's next host and its former guest actually observe.
func TestADetachedVolumeIsStillStoppedAndPublished(t *testing.T) {
	d := start(t)
	agent := d.startAgent(t, "agent-1")
	d.waitForHost(t)
	d.seedVolume(t)
	volumeID := waitForServedVolume(t, d)
	agent.WaitForLine(t, "serving volume", startup)
	agent.WaitForLine(t, "read view recovered from the object store", startup)

	socket := filepath.Join(d.sockDir, volumeID+".sock")
	if keys := volumeKeys(t, d, volumeID); len(keys) != 0 {
		t.Fatalf("%d object(s) for volume %s before the detach: the assertions below would be vacuous:\n%v", len(keys), volumeID, keys)
	}

	d.detachVolume(t, volumeID)

	waitFor(t, 60*time.Second, "the detached volume's image to reach the bucket", func() bool {
		return len(volumeKeys(t, d, volumeID)) != 0
	})
	waitFor(t, 60*time.Second, "the detached volume's socket to go away", func() bool {
		_, err := os.Stat(socket)
		return err != nil
	})
}

// detachVolume runs `control-plane -detach-volume`, which clears the volume's placement
// and exits. It is the operator's half of a drain, and the only thing in this repository
// that makes a host's desired state go empty for a reason that is not a bug.
func (d *deployment) detachVolume(t *testing.T, volumeID string) {
	t.Helper()
	p := testinfra.Start(t, testinfra.ProcessConfig{
		Name: "detach",
		Path: testinfra.Binary(t, "control-plane"),
		Args: append([]string{
			"-database-url", d.dsn,
			"-holder-id", "cp-detach",
			"-detach-volume", volumeID,
		}, d.storeArgs()...),
		Env: d.agentEnv,
	})
	if err := p.Wait(t, startup); err != nil {
		t.Fatalf("detaching %s: %v", volumeID, err)
	}
}

// startAgentAgainst starts an Agent whose Control Plane is reached at cpURL — a proxy
// this test can make lie — and which is otherwise the Agent every other scenario starts.
//
// It returns a heldAgent (see hold_test.go) rather than a testinfra.Process because this
// scenario reads the Agent's output while it keeps running and never signals it; the two
// should collapse into one when testinfra grows the same controls.
func (d *deployment) startAgentAgainst(t *testing.T, name, cpURL string) *heldAgent {
	t.Helper()
	a := startHeldAgent(t, name, d.agentBin, append([]string{
		"-host-id", d.hostID,
		"-control-plane", cpURL,
		"-data-dir", d.dataDir,
		"-vhost-socket-dir", d.sockDir,
		"-kek-file", d.kekFile,
		"-heartbeat-interval", "1s",
	}, d.storeArgs()...), d.agentEnv)
	a.waitForLine(t, "key-encryption key loaded", startup)
	return a
}

// --- a Control Plane that can be made to list nothing ----------------------------

// controlPlaneProxy forwards every RPC to the real Control Plane, except that while it is
// "listing nothing" it answers GetDesiredState itself with an empty response.
//
// It is a fault injector on a real deployment rather than a fake Control Plane, and the
// distinction is the point: the heartbeat, the key fetch and — critically — the volume
// report all reach the real process and get its real answers, so the Agent is not being
// told anything false about who owns the volume. The only thing it is being told is
// nothing, which is the message this scenario is about.
//
// An empty Connect unary response is an empty body: for the proto codec the response body
// *is* the serialized message, and a GetDesiredStateResponse with no volumes serializes
// to zero bytes. The request's own Content-Type is echoed back so the codec the client
// chose is the codec it is answered in.
type controlPlaneProxy struct {
	t   *testing.T
	url string

	mu    sync.Mutex
	empty bool
}

func newControlPlaneProxy(t *testing.T, target string) *controlPlaneProxy {
	t.Helper()
	u, err := url.Parse(target)
	if err != nil {
		t.Fatalf("parsing the Control Plane URL %q: %v", target, err)
	}
	p := &controlPlaneProxy{t: t}
	rp := httputil.NewSingleHostReverseProxy(u)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.listingNothing() && strings.HasSuffix(r.URL.Path, "/GetDesiredState") {
			w.Header().Set("Content-Type", r.Header.Get("Content-Type"))
			w.WriteHeader(http.StatusOK)
			return
		}
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	p.url = srv.URL
	t.Logf("control plane proxy: %s -> %s", p.url, target)
	return p
}

func (p *controlPlaneProxy) listingNothing() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.empty
}

func (p *controlPlaneProxy) listNothing()    { p.set(true) }
func (p *controlPlaneProxy) listEverything() { p.set(false) }

func (p *controlPlaneProxy) set(empty bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.empty = empty
	if empty {
		p.t.Log("control plane proxy: GetDesiredState now answers an empty list")
		return
	}
	p.t.Log("control plane proxy: GetDesiredState passes through again")
}
