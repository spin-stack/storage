//go:build e2e

package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/testinfra"
)

// The worst regression this deployment has shipped, and it was only reachable here.
//
// Two fixes that are individually correct — every placement grants a fresh epoch, and a
// volume that comes back below the sequence a guest was told was durable refuses to serve
// — together bricked a volume whose data was intact on the local disk:
//
//  1. a session is ACKed to a guest's fsync and not published (the Agent is killed, or a
//     detach lands in the seconds before its next poll). Its records are under
//     `<data-dir>/wal/<vol>/<epoch>/`;
//  2. the operator does the documented thing, `-attach-volume`, which grants a fresh
//     epoch, so the Agent opens `<epoch+1>/` — a directory that does not exist;
//  3. replay finds nothing, the durable floor fires, and the volume is refused;
//  4. every retry makes it worse. Another attach is another epoch. `-rebuild-metadata`
//     does not move `durable_sequence`. Renaming the directory by hand is refused,
//     because each segment header carries its own epoch.
//
// Nothing an operator could run reached those bytes, ever. Only this lane can state it:
// every step is a different *process* — the guest, the Agent, the attach command — and the
// epoch that breaks it is minted by the Control Plane, which no unit test has.
//
// The assertion is the returning guest reading its own bytes back. GUESTINIT-FAIL with
// "input/output error" is what this test reports against, and it is exactly what the lane
// printed before the fix.
func TestAVolumeAttachedAgainServesTheSessionItsHostWasKilledHolding(t *testing.T) {
	kernel, initramfs := testinfra.GuestImages(t)
	_, _ = testinfra.QEMUPaths(t)

	d := start(t)
	first := d.startAgent(t, "agent-1")
	d.waitForHost(t)
	d.seedVolume(t)
	volumeID := waitForServedVolume(t, d)
	sock := waitForVolumeSocket(t, d, volumeID)
	first.WaitForLine(t, "serving volume", startup)

	// A real kernel writes and fsyncs. That ACK is the promise the rest of this test is
	// about: the fleet has told this guest, in writing, that those blocks are durable.
	requireGuestPass(t, sock, kernel, initramfs)

	// And the fleet has written the promise down. Without this the durable floor has
	// nothing to fire on and the scenario is not the one that bricked.
	waitForCatalog(t, d, volumeID, "the catalog to record the ACK the guest's fsync returned on",
		func(pub, dur int64) bool { return dur > 0 && pub == 0 })

	// SIGKILL: no publish, no image. The session exists on this host's device and
	// nowhere else, which is the state a crashed Agent leaves.
	first.Kill(t)
	if keys := volumeKeys(t, d, volumeID); len(keys) != 0 {
		t.Fatalf("the killed Agent published %v; the scenario needs a session that is nowhere but the local WAL", keys)
	}
	held := walEpochs(t, d, volumeID)
	if len(held) != 1 {
		t.Fatalf("the killed Agent left %v under wal/%s/; the scenario needs exactly the one unpublished epoch", held, volumeID)
	}

	// What the operator does. Both halves through the real binary, because the fresh
	// epoch the second one mints is the whole mechanism.
	detachVolume(t, d, volumeID)
	attachVolume(t, d, volumeID)
	granted := volumeEpoch(t, d, volumeID)
	if fmt.Sprint(granted) == held[0] {
		t.Fatalf("the attach granted epoch %d, the same one the killed session held: this test would pass without the fix", granted)
	}

	second := d.startAgent(t, "agent-2")
	// Either outcome, so the guest below decides this test rather than the Agent's own
	// account of itself.
	waitForAnyLine(t, second, startup, refusedRollback, "read view recovered from the object store")
	// The refusal is checked here and not left to the socket wait below, because the two
	// report the same failure with very different words. A refused volume gets no socket at
	// all — its runtime is cancelled and its listener closed — so waiting for the socket
	// first would turn the bricked state this test is named after into "timed out waiting
	// for a file", naming neither the epoch nor the records that are sitting on the disk.
	if said(second, refusedRollback) {
		t.Fatalf("the volume was refused after being attached again at epoch %d: the session its host was "+
			"killed holding is under wal/%s/%s and nothing an operator can run reaches it",
			granted, volumeID, held[0])
	}
	waitForVolumeSocket(t, d, volumeID)

	// The tenant's half. verify mode writes nothing and reads back exactly the range the
	// first guest fsynced: "input/output error" is the volume refusing — the bricked state
	// — and a read-back mismatch would be it serving zeros.
	code, out := testinfra.RunLinuxGuest(t, sock, kernel, initramfs, "spin.mode=verify")
	if !strings.Contains(out, "GUESTINIT-PASS") {
		t.Fatalf("the guest could not read back what it fsynced before its host was killed (exit %d).\n"+
			"Those records are under wal/%s/%s on this host and the volume was re-attached at epoch %d:\n%s",
			code, volumeID, held[0], granted, testinfra.VerdictLines(out))
	}

	// And the other half of the same fix: the epoch that was taken up is gone. Nothing
	// used to delete one, so an A->B->A cycle left a sealed segment beside the live one and
	// every attach added another — for ever, charged against the guest budget the Agent
	// measures from that filesystem.
	after := walEpochs(t, d, volumeID)
	if len(after) != 1 || after[0] != fmt.Sprint(granted) {
		t.Fatalf("wal/%s/ holds %v after the attach at epoch %d; the epochs below it were drained and must be unlinked",
			volumeID, after, granted)
	}
}

// walEpochs is what the Agent still holds on its device for one volume, by epoch
// directory name. It reads the filesystem, because "the data directory keeps growing" is a
// claim about a device and no line the Agent prints can settle it.
func walEpochs(t *testing.T, d *deployment, volumeID string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(d.dataDir, "wal", volumeID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("reading the WAL directory of %s: %v", volumeID, err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

// attachVolume places the volume on this deployment's host through the real binary. It is
// the documented recovery step, and the fresh epoch it mints is what made the recovery
// impossible — so a test that reached the same state by editing the catalog would be
// testing something else.
func attachVolume(t *testing.T, d *deployment, volumeID string) {
	t.Helper()
	p := testinfra.Start(t, testinfra.ProcessConfig{
		Name: "attach",
		Path: testinfra.Binary(t, "control-plane"),
		Args: append([]string{
			"-database-url", d.dsn,
			"-holder-id", "cp-attach",
			"-attach-volume", volumeID,
			"-attach-host", d.hostID,
		}, append(d.placementArgs(), d.storeArgs()...)...),
		Env: d.agentEnv,
	})
	if err := p.Wait(t, startup); err != nil {
		t.Fatalf("attaching %s: %v", volumeID, err)
	}
}
