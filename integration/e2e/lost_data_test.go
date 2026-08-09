//go:build e2e

package e2e

import (
	"bytes"
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
// the assertion is the same in both: a volume whose data cannot be found serves the guest
// **nothing** — no device at all — and says so to the fleet.
//
// They are in one file because they are one hole with two faces. In both, the number that
// distinguishes "this volume is new" from "this volume's data is missing" was already in
// Postgres, and nothing on the attach path asked for it. Everything else about them
// differs — one is a manifest that disappeared, the other a local WAL that did — which is
// why they are two scenarios rather than a table.
//
// **Why the guest left.** Both scenarios used to boot a real kernel against the refused
// volume's socket and assert `input/output error`, and that was the strongest available
// observation while a refused volume still got a socket. It no longer is. The Agent now
// cancels the volume's context when it refuses, so the listener closes and the socket is
// unlinked, and the reason is exactly what those tests were pinning: what a tenant must
// not get is a plausible-looking device. A guest that cannot attach cannot be told
// anything false, which is a strictly stronger statement than a guest that attaches and
// is told EIO — and there is no variant satisfying both, because anything that stops the
// guest seeing a working device also stops QEMU attaching one.
//
// So what is asserted here is the absence of the socket, the refusal reaching the fleet
// (the catalog's column and the row `-fleet-status` prints), and the bucket: nothing
// published over the image that went missing, nothing published over the image the
// rollback would have replaced. A real guest still runs — in the *setup* of both, because
// the bytes these scenarios are about have to have been written by a real kernel and
// acknowledged by a real fsync, or there is nothing to lose.
//
// Neither can be seen anywhere but here. A unit test builds the VolumeManager itself and
// hands it whatever desired state it likes, so it cannot notice that the Control Plane
// never puts these numbers on the wire, nor that the refusal never reaches Postgres.

const (
	// What the Agent prints when it refuses. Greped for verbatim, because "it refused"
	// and "it refused for this reason" are different claims and only the second one
	// distinguishes the fix from a volume that happened to fail for its own reasons.
	refusedNoImage  = "the catalog says this volume has published an image and the object store has none"
	refusedRollback = "this volume came back below the sequence a guest was already told was durable"
)

// TestAVolumeWhoseImageVanishedGetsNoDevice is face 1, reproduced end to end.
//
// A published image is deleted out from under a volume the catalog still describes
// correctly — a stray delete, a lifecycle expiry, a restore that missed one key. Before
// this was closed the Agent read `image.ErrNotPublished` as "this volume has never
// published, boot it empty", logged it at INFO with `durable_sequence=0`, and served a
// blank device. The tenant's guest then reads zeros for every block it ever wrote; and the
// *next* teardown publishes that blank view create-only, which is the moment the loss
// stops being recoverable.
//
// So the assertions are the three halves of that sentence, from outside the process: the
// volume gets no vhost socket, so no guest can be handed anything at all; the fleet is
// told which volume and why; and the bucket still has no manifest after the volume is
// taken away from this host — the teardown that used to overwrite it now refuses.
//
// The local WAL is left **intact**, deliberately. It still holds this volume's records, so
// the durable-sequence floor in the other test is satisfied and this scenario turns on
// exactly one fact: the catalog says this volume has published and the bucket disagrees.
func TestAVolumeWhoseImageVanishedGetsNoDevice(t *testing.T) {
	kernel, initramfs := testinfra.GuestImages(t)
	_, _ = testinfra.QEMUPaths(t)

	d := start(t)
	first := d.startAgent(t, "agent-1")
	d.waitForHost(t)
	d.seedVolume(t)
	volumeID := waitForServedVolume(t, d)
	sock := waitForVolumeSocket(t, d, volumeID)
	first.WaitForLine(t, "serving volume", startup)

	// Session 1: a real kernel writes and fsyncs, and the stop is what publishes. This is
	// the only guest in this scenario and it is setup, not assertion — without it there is
	// no image for the accident below to remove.
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
	// The stopped Agent unlinked the socket on its way out, so the absence asserted below
	// starts from a known state rather than from whatever the previous session left. A
	// leftover file here would make `requireNoVolumeSocket` fail against a working fix.
	requireSocketGone(t, d, volumeID, "agent-2 stopped")

	// Session 3: the attach that used to come up blank.
	third := d.startAgent(t, "agent-3")

	// Waited for on the *fleet*, not on the Agent's log, and this is the ordering the
	// whole test turns on. The refusal reaches Postgres on a heartbeat, which is well
	// after the attach — so by the time it is visible here the Agent has long since
	// decided, and "the socket is not there" is a settled fact rather than a race with
	// `start` binding the listener. It is also the assertion the fleet needs in its own
	// right: a refusal that never leaves the host is an outage with no cause.
	detail := waitForRefusal(t, d, volumeID, "IMAGE_MISSING")
	if !strings.Contains(detail, "object store holds no image") {
		t.Errorf("the catalog's refusal_detail is %q; an operator reads this before touching the bucket", detail)
	}

	// What the tenant sees: nothing. No socket means QEMU's chardev has nothing to
	// connect to and the guest boots with no /dev/vda — which its own boot logic can act
	// on, unlike a 256 MiB disk that answers every read with EIO.
	requireNoVolumeSocket(t, d, volumeID, 10*time.Second)

	// The operator's half: which volume, and which of the two floors refused it.
	third.WaitForLine(t, refusedNoImage, startup)
	requireFleetStatusNotServing(t, d, volumeID, "IMAGE_MISSING")

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

// TestAnAgentThatReplaysShortOfTheACKedSequenceServesNoDevice is face 2, reproduced end
// to end: the volume rolls back to its last publish and nothing anywhere reports an error.
//
// The shape is a cloud instance store, which is what a host's local WAL actually sits on.
// A guest fsyncs — so this fleet has told it, in writing, that those writes are durable —
// the Agent is SIGKILLed before it can publish, and the host comes back with its local
// disk empty. Replay finds nothing, the image from the previous session loads, and the
// volume serves happily at the older sequence.
//
// **The guest cannot tell**, and that is why the device has to go rather than answer. The
// range a verify boot checks lives in the published image from session 1, so a rolled-back
// volume answers it *correctly* — with bytes that are two sessions old. There is no read a
// guest can make that distinguishes this from a healthy volume, which means there is no
// assertion about a guest's bytes that could be made here at all: the only honest signal is
// that the volume is not on the host's socket directory and the fleet says why.
//
// The bucket assertion is the counterpart of the other scenario's. There, nothing must be
// published over a manifest that vanished; here, the manifest is *present* and correct, and
// what must not happen is the short session being written over it — which would replace the
// volume's real image with one holding strictly less than the fleet promised.
func TestAnAgentThatReplaysShortOfTheACKedSequenceServesNoDevice(t *testing.T) {
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
	// rolls *back to*.
	requireGuestPass(t, sock, kernel, initramfs)
	first.Stop(t, startup)
	manifest := "image/" + volumeID + "/manifest.json"
	requireKey(t, d, manifest, "the first session's stop")

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
	// Read now, while the image is still the one session 1 published. It is the thing the
	// teardown of the refused session must not replace, and comparing bytes rather than an
	// ETag is deliberate: a backend that re-PUT identical content would keep neither.
	before := objectBody(t, d, manifest)

	// SIGKILL, so nothing is published: the second session exists only in the local WAL.
	second.Kill(t)
	// A SIGKILLed process never unlinks its sockets, so the file is still there and the
	// absence asserted below would be satisfied by nothing at all. Removed here, named,
	// rather than left for `requireNoVolumeSocket` to be confused by — and it is exactly
	// what a host that rebooted (which is what this scenario is) comes back without.
	if err := os.Remove(filepath.Join(d.sockDir, volumeID+".sock")); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	// And then the instance store goes away with the host. Not a contrived deletion — it
	// is what a reboot of a cloud instance does to the disk the WAL lives on, and the
	// reason `durable` above is a promise this host can no longer keep on its own.
	if err := os.RemoveAll(filepath.Join(d.dataDir, "wal")); err != nil {
		t.Fatal(err)
	}

	third := d.startAgent(t, "agent-3")

	// The fleet first, for the ordering reason in the scenario above: the refusal reaches
	// Postgres on a heartbeat, so once it is there the Agent's decision about this volume
	// is long since made and the socket's absence is settled rather than raced.
	detail := waitForRefusal(t, d, volumeID, "DURABILITY_LOST")
	if !strings.Contains(detail, "ACKed to a guest as durable") {
		t.Errorf("the catalog's refusal_detail is %q; it is the sentence that names the two sequences", detail)
	}

	// What the tenant gets: no device. There is no read that could have caught this — the
	// image answers session 1's range perfectly — so the absence of the socket is the whole
	// of what stands between a tenant and silently superseded data.
	requireNoVolumeSocket(t, d, volumeID, 10*time.Second)

	// And the operator's half: the log says which volume, what it came back with, and what
	// it was supposed to have, and `-fleet-status` names it without anyone reading a host's
	// log at all.
	third.WaitForLine(t, refusedRollback, startup)
	requireFleetStatusNotServing(t, d, volumeID, "DURABILITY_LOST")

	// The bucket. Taking the volume away runs the reconciliation teardown, which publishes
	// — and publishing this session would put the rolled-back view over the image that
	// still holds everything session 1 wrote.
	detachVolume(t, d, volumeID)
	waitForAnyLine(t, third, startup, "this volume's image could not be published", "volume image published")
	if after := objectBody(t, d, manifest); !bytes.Equal(before, after) {
		t.Fatalf("%s was rewritten by a host that came back at published_sequence=%d with durable_sequence=%d promised:\n"+
			"before: %s\nafter:  %s",
			manifest, published, durable, before, after)
	}
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

// requireNoVolumeSocket insists the volume has no vhost-user socket, and keeps insisting
// for `window`.
//
// A negative is only worth asserting when it is anchored, and the anchor is the caller's:
// both scenarios wait for the refusal to reach *Postgres* first, which takes a heartbeat,
// so the Agent has long since decided about this volume by the time this runs. Without
// that ordering this would race `start`, which binds the listener before the base is
// fetched — a window one object-store round trip wide, and the reason the check is a
// window rather than one `os.Stat`: a socket that reappears is a supervisor re-listening
// on a refused volume, which is the failure that would leave a tenant with a device again.
func requireNoVolumeSocket(t *testing.T, d *deployment, volumeID string, window time.Duration) {
	t.Helper()
	sock := filepath.Join(d.sockDir, volumeID+".sock")
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(sock); err == nil {
			t.Fatalf("%s exists: a volume this host refuses to serve still gets a device, so a guest attaches "+
				"a disk it can only take I/O errors from instead of finding no disk at all", sock)
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat %s: %v", sock, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// requireSocketGone is the same check once, as a precondition. It names who was supposed
// to have removed the file, because a leftover socket from an earlier session would make
// the window above fail against a perfectly good fix.
func requireSocketGone(t *testing.T, d *deployment, volumeID, who string) {
	t.Helper()
	sock := filepath.Join(d.sockDir, volumeID+".sock")
	if _, err := os.Stat(sock); err == nil {
		t.Fatalf("%s is still there after %s; this scenario's assertion is about the socket not appearing "+
			"and it cannot start from one that is already present", sock, who)
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat %s: %v", sock, err)
	}
}

// waitForRefusal blocks until the catalog carries this refusal for the volume and returns
// the sentence stored with it.
//
// This is the claim that "the refusal reaches the fleet", and it is read from Postgres
// rather than from the Agent's log for the reason waitForCatalog gives: the Agent
// reporting its own state is what is under test. It is also this lane's synchronisation
// point — it can only become true after a heartbeat carrying the refused report was
// accepted, which is strictly after the Agent decided not to serve the volume.
func waitForRefusal(t *testing.T, d *deployment, volumeID, want string) string {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), d.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	var detail string
	waitFor(t, startup, fmt.Sprintf("the catalog to record %s for volume %s", want, volumeID), func() bool {
		var refusal string
		if err := pool.QueryRow(t.Context(),
			`SELECT refusal, refusal_detail FROM volumes WHERE volume_id = $1`,
			volumeID).Scan(&refusal, &detail); err != nil {
			return false
		}
		return refusal == want
	})
	return detail
}

// requireFleetStatusNotServing runs the command an operator actually runs and insists the
// volume is in its NOT SERVED section with this reason.
//
// The catalog check above proves the column; this proves the one thing that column exists
// for. They are not the same assertion — a report that lands in a column no command prints
// is a refusal an operator still cannot see — and the whole failure being closed here is a
// volume that is down with no signal outside one host's log.
func requireFleetStatusNotServing(t *testing.T, d *deployment, volumeID, reason string) {
	t.Helper()
	p := testinfra.Start(t, testinfra.ProcessConfig{
		Name: "fleet-status",
		Path: testinfra.Binary(t, "control-plane"),
		Args: []string{"-database-url", d.dsn, "-fleet-status"},
		Env:  d.agentEnv,
	})
	if err := p.Wait(t, startup); err != nil {
		t.Fatalf("-fleet-status: %v", err)
	}
	// One line carrying both, not "the id appears and the reason appears": two unrelated
	// rows satisfy the second, and what an operator reads is a row.
	if !said(p, volumeID, reason) {
		t.Fatalf("no line of -fleet-status names volume %s as %s:\n%s",
			volumeID, reason, strings.Join(p.Output(), "\n"))
	}
	// And the STATE cell, which is what a scan of the table shows before anyone reads the
	// section below it. A volume left reading ACTIVE while its host refuses it is the
	// original silence with an extra section nobody scrolls to.
	if !said(p, volumeID, "NOT_SERVED") {
		t.Fatalf("the volumes table still renders %s as though it were being served:\n%s",
			volumeID, strings.Join(p.Output(), "\n"))
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
