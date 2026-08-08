//go:build e2e

package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/testinfra"
)

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
	if keys := volumeKeys(t, d, volumeID); len(keys) != 0 {
		t.Fatalf("%d object(s) for volume %s before the guest ran: the assertion would be vacuous:\n%v",
			len(keys), volumeID, keys)
	}

	code, out := testinfra.RunLinuxGuest(t, sock, kernel, initramfs)
	switch {
	case strings.Contains(out, "GUESTINIT-FAIL"):
		t.Fatalf("the guest reported a failure:\n%s", testinfra.VerdictLines(out))
	case !strings.Contains(out, "GUESTINIT-PASS"):
		t.Fatalf("the guest never reported a verdict (exit %d):\n%s", code, testinfra.VerdictLines(out))
	}

	// The fsync ACKed locally and put nothing in the bucket — that is §14.8, and
	// asserting it here is what keeps the next assertion meaningful.
	if keys := volumeKeys(t, d, volumeID); len(keys) != 0 {
		t.Fatalf("a guest's fsync published %d object(s); §14.8 says the ACK is local:\n%v", len(keys), keys)
	}

	// Stopping is what publishes (ADR-0026), and this is the artefact V1's whole
	// durability contract produces: an image that exists only if the *binary* resolved
	// its credentials, sealed the chunks and CASed the manifest.
	agent.Stop(t, 30*time.Second)

	// Both prefixes, and the chunks are the half that carries the guest's bytes: since
	// the chunk store moved to the lineage they are under chunks/<lineage>/, which for a
	// volume that was created rather than cloned is its own id. A manifest naming no
	// chunk would satisfy a count under image/ alone.
	keys := storeKeys(t, d, "image/"+volumeID+"/")
	chunks := storeKeys(t, d, "chunks/"+volumeID+"/")
	if len(keys) == 0 || len(chunks) == 0 {
		t.Fatalf("the Agent stopped and the bucket holds %d object(s) under image/%s/ and %d under chunks/%s/ — "+
			"the session's writes exist only on a host that has released them:\n%s",
			len(keys), volumeID, len(chunks), volumeID, testinfra.VerdictLines(out))
	}
	t.Logf("a real kernel's writes left %v and %v", keys, chunks)
}
