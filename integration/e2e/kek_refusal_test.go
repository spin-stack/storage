//go:build e2e

package e2e

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spin-stack/storage/internal/testinfra"
)

// TestAKEKlessAgentPutsNoGuestPlaintextInTheBucket is the deployment-level half of the
// refusal, and it is here rather than only in internal/agent because the failure it is
// about is a *wiring* one: one flag missing from one process.
//
// What it reproduced before the fix, with the bytes: an operator restarts the Agent
// without -kek-file against a fleet whose volumes were provisioned with one. The binary
// prints a single WARN at start-up, serves the volume anyway (`encrypted=false`), the
// guest writes through the socket, and the image published at detach is the guest's
// plaintext under `chunks/<lineage>/…` — where the format says `<nonce:12><ct><tag:16>`
// and where §15.3's crypto-shredding guarantee says destroying the DEK makes the data
// unreadable. Nothing failed. Nothing was retried. The only signal was a line printed
// once, before the volume existed, about no volume in particular.
//
// The assertion is the bucket's bytes and the socket's absence, not the Agent's log:
// an Agent that says "refused" and serves the volume anyway satisfies any assertion on
// what it printed. The log line is checked too, but only as the operator's half —
// a refusal nobody can see is an outage with no cause.
func TestAKEKlessAgentPutsNoGuestPlaintextInTheBucket(t *testing.T) {
	// The guest is what puts plaintext anywhere, so without it this test cannot fail
	// for the reason it exists. Skipped before anything is built, loudly; CI builds the
	// inputs and so does not skip (ADR-0025).
	kernel, initramfs := testinfra.GuestImages(t)
	_, _ = testinfra.QEMUPaths(t)

	d := start(t)
	// The Control Plane still has the KEK — it is the Agent that was started without
	// one, which is the whole shape of the failure. So the volume seeded below is
	// provisioned *with* a KEK and its row says so.
	agent := d.startAgentWithoutKEK(t)
	d.waitForHost(t)
	d.seedVolume(t)

	volumeID := waitForServedVolume(t, d)

	// Wait for the Agent to have *decided* about this volume — it either refused it or
	// bound its socket — rather than for a duration. Both branches are then taken as
	// found, because the assertion that matters is the bucket's, and a test that fataled
	// on the log line here would never reach it in the world where the bug is back.
	sock := filepath.Join(d.sockDir, volumeID+".sock")
	refused := func() bool { return said(agent, volumeID, "holds no key-encryption key") }
	served := func() bool { _, err := os.Stat(sock); return err == nil }
	waitFor(t, 60*time.Second, "the Agent to refuse the volume or bind its socket",
		func() bool { return refused() || served() })

	// The guest's half. With the volume refused there is no socket and no guest; with
	// the refusal removed the socket is there, the guest writes 4 KiB blocks of a
	// pattern, and stopping the Agent publishes them.
	if served() {
		t.Errorf("volume %s has a vhost-user socket at %s: this Agent cannot seal a byte of what a guest writes through it",
			volumeID, sock)
		code, out := testinfra.RunLinuxGuest(t, sock, kernel, initramfs)
		t.Logf("a guest ran against it (exit %d):\n%s", code, testinfra.VerdictLines(out))
	}
	// The operator's half, and only that: a refusal nobody can see is an outage with no
	// cause. It names the volume, because "this host holds no KEK" says nothing about
	// which of the volumes placed on it are affected.
	if !refused() {
		t.Errorf("nothing the Agent printed names volume %s as refused for want of a KEK", volumeID)
	}

	// Stopping is what publishes (ADR-0026): whatever this session holds reaches the
	// object store here or nowhere.
	agent.Stop(t, 60*time.Second)

	// Every object in the bucket, not only this volume's prefixes: an image is chunks
	// under `chunks/<lineage>/` plus a manifest under `image/<volume>/`, and a scan that
	// picked a prefix could be made vacuous by the data moving.
	needle := guestPattern()[:512]
	keys := storeKeys(t, d, "")
	for _, key := range keys {
		body := objectBody(t, d, key)
		if bytes.Contains(body, needle) {
			t.Fatalf("%s carries the guest's plaintext (§15/§15.3): a KEK-less Agent served a volume the catalog "+
				"records as encrypted, and its image is readable by anyone who can read the bucket", key)
		}
	}
	t.Logf("%d object(s) in the bucket, none carrying the guest's pattern: %v", len(keys), keys)
}

// said reports whether one line of the process's output contains all of want. One line
// and not the whole stream: "the volume id appears somewhere and the reason appears
// somewhere" is satisfied by two unrelated lines, and the operator reads a line.
func said(p *testinfra.Process, want ...string) bool {
	for _, line := range p.Output() {
		all := true
		for _, w := range want {
			if !strings.Contains(line, w) {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

// startAgentWithoutKEK is `startAgent` minus the one flag this test is about. It is not a
// parameter on the fixture's helper because every other lane wants the opposite default:
// an Agent with no -kek-file is a misconfiguration, and it should have to be spelled out.
func (d *deployment) startAgentWithoutKEK(t *testing.T) *testinfra.Process {
	t.Helper()
	p := testinfra.Start(t, testinfra.ProcessConfig{
		Name: "volume-agent-no-kek",
		Path: d.agentBin,
		Args: append([]string{
			"-host-id", d.hostID,
			"-control-plane", d.cpURL,
			"-data-dir", d.dataDir,
			"-vhost-socket-dir", d.sockDir,
			"-heartbeat-interval", "1s",
		}, d.storeArgs()...),
		Env: d.agentEnv,
	})
	// The start-up WARN, which is the thing this whole test says is not enough: it is
	// waited on only to know the process reached its wiring, never as the refusal.
	p.WaitForLine(t, "no -kek-file", startup)
	return p
}

// guestPattern is what integration/guestinit writes: 4096 bytes of 'A'+i%23. It is
// duplicated here rather than shared because guestinit is a `main` package — the same
// reason testinfra spells out the guest's verdict strings.
func guestPattern() []byte {
	p := make([]byte, 4096)
	for i := range p {
		p[i] = byte('A' + (i % 23))
	}
	return p
}

// objectBody reads one object straight from the backend. The Agent's own report of what
// it published is the claim under test, so the evidence has to come from the bucket.
func objectBody(t *testing.T, d *deployment, key string) []byte {
	t.Helper()
	out, err := d.store.Client().GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("reading %s: %v", key, err)
	}
	defer func() { _ = out.Body.Close() }()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("reading %s: %v", key, err)
	}
	return body
}
