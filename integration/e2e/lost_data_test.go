//go:build e2e

package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/spin-stack/storage/internal/testinfra"
)

// The two ways a volume used to come back holding less than the fleet had promised, and
// the assertion is the same in both: a real guest, on the socket a real volume-agent
// bound, must get an I/O error rather than plausible-looking bytes.
//
// They are in one file because they are one hole with two faces. In both, the number that
// distinguishes "this volume is new" from "this volume's data is missing" was already in
// Postgres, and nothing on the attach path asked for it. Everything else about them
// differs — one is a manifest that disappeared, the other a local WAL that did — which is
// why they are two scenarios rather than a table.
//
// Neither can be seen anywhere but here. A unit test builds the VolumeManager itself and
// hands it whatever desired state it likes, so it cannot notice that the Control Plane
// never puts these numbers on the wire; and the failure mode is *the absence of an error*,
// which every assertion on `err` is blind to by construction.

const (
	// What the Agent prints when it refuses. Greped for verbatim, because "it refused"
	// and "it refused for this reason" are different claims and only the second one
	// distinguishes the fix from a volume that happened to fail for its own reasons.
	refusedNoImage  = "the catalog says this volume has published an image and the object store has none"
	refusedRollback = "this volume came back below the sequence a guest was already told was durable"
)

// TestAVolumeWhoseImageVanishedRefusesToComeUpBlank is face 1, reproduced end to end.
//
// A published image is deleted out from under a volume the catalog still describes
// correctly — a stray delete, a lifecycle expiry, a restore that missed one key. Before
// this was closed the Agent read `image.ErrNotPublished` as "this volume has never
// published, boot it empty", logged it at INFO with `durable_sequence=0`, and served a
// blank device. The tenant's guest then reads zeros for every block it ever wrote; and the
// *next* teardown publishes that blank view create-only, which is the moment the loss
// stops being recoverable.
//
// So the assertions are the two halves of that sentence, from outside the process: the
// guest gets an I/O error instead of zeros, and the bucket still has no manifest after the
// volume is taken away from this host — the teardown that used to overwrite it now
// refuses.
//
// The local WAL is left **intact**, deliberately. It still holds this volume's records, so
// the durable-sequence floor in the other test is satisfied and this scenario turns on
// exactly one fact: the catalog says this volume has published and the bucket disagrees.
func TestAVolumeWhoseImageVanishedRefusesToComeUpBlank(t *testing.T) {
	kernel, initramfs := testinfra.GuestImages(t)
	_, _ = testinfra.QEMUPaths(t)

	d := start(t)
	first := d.startAgent(t, "agent-1")
	d.waitForHost(t)
	d.seedVolume(t)
	volumeID := waitForServedVolume(t, d)
	sock := waitForVolumeSocket(t, d, volumeID)
	first.WaitForLine(t, "serving volume", startup)

	// Session 1: a real kernel writes and fsyncs, and the stop is what publishes.
	requireGuestPass(t, sock, kernel, initramfs)
	first.Stop(t, startup)
	manifest := "image/" + volumeID + "/manifest.json"
	requireKey(t, d, manifest, "the first session's stop")

	// Session 2 exists to put the number under test in the catalog. `published_sequence`
	// reaches the Control Plane from the Agent's report, and the report is built from the
	// log's watermarks — which learn the published point when the *next* attach installs
	// the image as its base. A volume that has published therefore says so from its
	// following session onwards, and this is that session.
	second := d.startAgent(t, "agent-2")
	waitForVolumeSocket(t, d, volumeID)
	second.WaitForLine(t, "read view recovered from the object store", startup)
	published := waitForCatalog(t, d, volumeID, "the catalog to record a published image",
		func(pub, dur int64) bool { return pub > 0 && dur >= pub })
	second.Stop(t, startup)

	// The accident. One key, and the catalog is not touched: this is the shape every
	// bucket-side mishap has — the volume's row is still perfectly correct.
	deleteKey(t, d, manifest)
	if keys := storeKeys(t, d, "image/"+volumeID+"/"); len(keys) != 0 {
		t.Fatalf("the manifest was supposed to be gone and %v is still there", keys)
	}

	// Session 3: the attach that used to come up blank.
	third := d.startAgent(t, "agent-3")
	waitForVolumeSocket(t, d, volumeID)
	// Either outcome, so the guest is what decides this test rather than the Agent's own
	// account of itself.
	waitForAnyLine(t, third, startup, refusedNoImage, "read view recovered from the object store")

	// What the tenant sees. Verify mode writes nothing and reads back the range session 1
	// fsynced: an I/O error is the volume refusing, and a read-back mismatch would be the
	// old behaviour handing it zeros.
	_, out := testinfra.RunLinuxGuest(t, sock, kernel, initramfs, "spin.mode=verify")
	requireGuestIOError(t, out)

	// The operator's half: which volume, and which of the two floors refused it.
	third.WaitForLine(t, refusedNoImage, startup)

	// And the half that made it permanent. Taking the volume away from this host runs the
	// same teardown a detach or a promotion runs, and that teardown publishes. Before this
	// change it published the blank view over the volume's own manifest key, create-only,
	// and the tenant's data was then unrecoverable from the bucket alone.
	detachVolume(t, d, volumeID)
	// Waited for on *either* outcome, deliberately: waiting only for the refusal would
	// make this a second assertion about the Agent's own log, and the claim being made
	// here is about the bucket. The teardown says one of these two things and then stops
	// touching the object store, so the check below runs on a settled bucket either way.
	waitForAnyLine(t, third, startup, "this volume's image could not be published", "volume image published")
	if keys := storeKeys(t, d, "image/"+volumeID+"/"); len(keys) != 0 {
		t.Fatalf("a volume that could not find its image published one anyway: %v\n"+
			"the catalog said published_sequence=%d, so this manifest describes a device the guest never wrote",
			keys, published)
	}
}

// TestAnAgentThatReplaysShortOfTheACKedSequenceRefusesToServe is face 2, reproduced end
// to end: the volume rolls back to its last publish and nothing anywhere reports an error.
//
// The shape is a cloud instance store, which is what a host's local WAL actually sits on.
// A guest fsyncs — so this fleet has told it, in writing, that those writes are durable —
// the Agent is SIGKILLed before it can publish, and the host comes back with its local
// disk empty. Replay finds nothing, the image from the previous session loads, and the
// volume serves happily at the older sequence.
//
// **The guest cannot tell.** That is the whole reason this needs a real one: the range a
// verify boot checks lives in the published image, so the read-back succeeds and returns
// the *old* bytes. Without the floor below, this test's guest reports GUESTINIT-PASS —
// the tenant reading superseded data with no I/O error is what "passes" here means.
func TestAnAgentThatReplaysShortOfTheACKedSequenceRefusesToServe(t *testing.T) {
	kernel, initramfs := testinfra.GuestImages(t)
	_, _ = testinfra.QEMUPaths(t)

	d := start(t)
	first := d.startAgent(t, "agent-1")
	d.waitForHost(t)
	d.seedVolume(t)
	volumeID := waitForServedVolume(t, d)
	sock := waitForVolumeSocket(t, d, volumeID)
	first.WaitForLine(t, "serving volume", startup)

	// Session 1: written, fsynced, and published by the stop. This is what the rollback
	// rolls *back to*, and it is why the guest below cannot see the loss.
	requireGuestPass(t, sock, kernel, initramfs)
	first.Stop(t, startup)
	requireKey(t, d, "image/"+volumeID+"/manifest.json", "the first session's stop")

	// Session 2: more writes, more fsyncs, no publish. The guest is told these are
	// durable and the Control Plane is told the same thing — that report is the promise
	// the third session has to keep.
	second := d.startAgent(t, "agent-2")
	waitForVolumeSocket(t, d, volumeID)
	second.WaitForLine(t, "read view recovered from the object store", startup)
	requireGuestPass(t, sock, kernel, initramfs)
	published, durable := int64(0), int64(0)
	waitForCatalog(t, d, volumeID, "the catalog to record an ACK above the published image",
		func(pub, dur int64) bool {
			published, durable = pub, dur
			return pub > 0 && dur > pub
		})

	// SIGKILL, so nothing is published: the second session exists only in the local WAL.
	second.Kill(t)
	// And then the instance store goes away with the host. Not a contrived deletion — it
	// is what a reboot of a cloud instance does to the disk the WAL lives on, and the
	// reason `durable` above is a promise this host can no longer keep on its own.
	if err := os.RemoveAll(filepath.Join(d.dataDir, "wal")); err != nil {
		t.Fatal(err)
	}

	third := d.startAgent(t, "agent-3")
	waitForVolumeSocket(t, d, volumeID)
	// Either outcome, so that the guest below is what decides this test. Waiting for the
	// refusal here instead would mean the assertion that matters — that a real kernel
	// cannot read the rolled-back volume — never runs when it would fail.
	waitForAnyLine(t, third, startup, refusedRollback, "read view recovered from the object store")

	_, out := testinfra.RunLinuxGuest(t, sock, kernel, initramfs, "spin.mode=verify")
	// GUESTINIT-PASS here is the defect, not a green test: it means the guest read the
	// image from session 1 back and could not tell that everything session 2 fsynced is
	// gone. That is the whole failure — old bytes, no error, nothing for a tenant to see.
	if strings.Contains(out, "GUESTINIT-PASS") {
		t.Fatalf("the volume rolled back from durable_sequence=%d to published_sequence=%d and a guest read it without an error:\n%s",
			durable, published, testinfra.VerdictLines(out))
	}
	requireGuestIOError(t, out)

	// And the operator's half: the log says which volume, what it came back with, and what
	// it was supposed to have.
	third.WaitForLine(t, refusedRollback, startup)
}

// --- helpers, local to this file ------------------------------------------------------

// requireGuestPass boots a write-and-verify guest and insists it succeeded. It is the
// setup half of both scenarios: the bytes the assertions are about have to have been
// written by a real kernel and acknowledged by a real fsync, or there is nothing to lose.
func requireGuestPass(t *testing.T, sock, kernel, initramfs string) {
	t.Helper()
	code, out := testinfra.RunLinuxGuest(t, sock, kernel, initramfs)
	switch {
	case strings.Contains(out, "GUESTINIT-FAIL"):
		t.Fatalf("the guest reported a failure:\n%s", testinfra.VerdictLines(out))
	case !strings.Contains(out, "GUESTINIT-PASS"):
		t.Fatalf("the guest never reported a verdict (exit %d):\n%s", code, testinfra.VerdictLines(out))
	}
}

// requireGuestIOError insists the guest failed *because the device refused*, not because
// it read the wrong bytes.
//
// The distinction is the entire point of both scenarios and it is checkable from the
// guest's own words: `readBack` reports "input/output error" when the backend returned an
// error and "read-back mismatch" when it cheerfully returned zeros. A test that accepted
// GUESTINIT-FAIL either way would pass against the behaviour being fixed.
func requireGuestIOError(t *testing.T, out string) {
	t.Helper()
	verdicts := testinfra.VerdictLines(out)
	if !strings.Contains(out, "GUESTINIT-FAIL") {
		t.Fatalf("the guest did not fail; the volume served it something:\n%s", verdicts)
	}
	if strings.Contains(out, "read-back mismatch") {
		t.Fatalf("the volume answered the guest with the wrong bytes instead of an error — "+
			"a mismatch is the guest noticing, and a tenant reading its own stale data would not:\n%s", verdicts)
	}
	if !strings.Contains(out, "input/output error") {
		t.Fatalf("the guest failed for some reason other than the device refusing to read:\n%s", verdicts)
	}
}

// waitForAnyLine blocks until the process has printed a line containing any of want.
//
// testinfra.Process.WaitForLine takes one string, and one string is wrong wherever the
// thing being waited for is "the step finished", not "the step succeeded": waiting only
// for the success line turns the next assertion into something that can never run in the
// failing case, and waiting only for the failure line makes the log the assertion.
func waitForAnyLine(t *testing.T, p *testinfra.Process, timeout time.Duration, want ...string) {
	t.Helper()
	waitFor(t, timeout, fmt.Sprintf("one of %q from %s", want, p.Name), func() bool {
		for _, line := range p.Output() {
			for _, w := range want {
				if strings.Contains(line, w) {
					return true
				}
			}
		}
		return false
	})
}

// waitForVolumeSocket blocks until the Agent has bound this volume's socket and returns
// its path.
func waitForVolumeSocket(t *testing.T, d *deployment, volumeID string) string {
	t.Helper()
	sock := filepath.Join(d.sockDir, volumeID+".sock")
	waitFor(t, 30*time.Second, fmt.Sprintf("the socket for %s", volumeID), func() bool {
		_, err := os.Stat(sock)
		return err == nil
	})
	return sock
}

// waitForCatalog polls the two watermarks the desired state now carries until they say
// what the scenario needs, and returns the published one.
//
// Straight out of Postgres rather than out of the desired state, on purpose: what the
// Control Plane puts on the wire is the thing under test, and reading the claim from the
// claimant is how an assertion becomes a tautology.
func waitForCatalog(t *testing.T, d *deployment, volumeID, what string, ok func(published, durable int64) bool) int64 {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), d.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	var published int64
	waitFor(t, startup, what, func() bool {
		var pub, dur int64
		if err := pool.QueryRow(t.Context(),
			`SELECT published_sequence, durable_sequence FROM volumes WHERE volume_id = $1`,
			volumeID).Scan(&pub, &dur); err != nil {
			return false
		}
		published = pub
		return ok(pub, dur)
	})
	return published
}

// requireKey insists one object is in the bucket, naming what was supposed to have put it
// there. Every later assertion is about that object being *gone*, so a scenario whose
// setup silently published nothing would prove nothing at all.
func requireKey(t *testing.T, d *deployment, key, who string) {
	t.Helper()
	for _, k := range storeKeys(t, d, key) {
		if k == key {
			return
		}
	}
	t.Fatalf("%s did not leave %s in the bucket", who, key)
}

// deleteKey removes one object, which is the accident both the operator and the bucket's
// own lifecycle rules can produce.
func deleteKey(t *testing.T, d *deployment, key string) {
	t.Helper()
	if _, err := d.store.Client().DeleteObject(t.Context(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}); err != nil {
		t.Fatalf("deleting %s: %v", key, err)
	}
}

// detachVolume clears the volume's placement through the real binary. It is how a test
// reaches the reconciliation teardown — the one a promotion or a drain runs — without
// stopping the Agent, which is what makes the "and then it published the blank one"
// assertion observable while the process is still there to be read.
func detachVolume(t *testing.T, d *deployment, volumeID string) {
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
