//go:build e2e

package e2e

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/spin-stack/storage/internal/testinfra"
)

// TestAFlattenedCloneNoLongerNeedsItsParentsSnapshot is the deployment statement about
// FLATTEN: the real Control Plane binary, the real Agent, a real Postgres and a real
// object store, with the one thing an operator actually wants out of the command asserted
// as the thing that would otherwise be impossible — **the snapshot the clone descends from
// is deleted, and the clone is served anyway**.
//
// It exists because the flag is the whole point of the increment and a flag is exactly
// what a unit test cannot exercise. `internal/agent`'s arm says what the *bytes* are across
// a flatten, against a real VolumeManager and a real device; this one says the binary an
// operator types does it, in a deployment where nothing was constructed by the test.
//
// The observable is the pair at the end, and neither half is a field the code set: with
// `image/<parent>/snapshots/<snap>.json` gone, a second Agent prints the recovery line for
// the clone's volume id and exits 0. The recovery line is the one `fetchBase` reaches only
// once a read view exists, and exit 0 is the durability half — a clone whose view never
// resolved leaves its session in a local WAL and exits 1. Before the flatten that same
// deletion makes the clone unservable, which is what the plant confirms and what the whole
// operation is for.
//
// The parent's snapshot manifest is what is deleted, and deliberately not its chunks: a
// chunk belongs to the lineage rather than to the volume that wrote it, so deleting
// `chunks/<parent>/` would take the bytes of the parent's own image too, and the test would
// be asserting that the clone survived something no delete would ever do
// (DELETION-AND-RECLAIM-SPEC's order removes the snapshot manifests before the chunks).
//
// # What this lane cannot say, and the first run of it is how that was found
//
// **No guest boots here, so nothing in it holds a byte of data**: the Agent's own lines
// read `image_bytes=0` for the parent, the snapshot and the clone alike. An assertion that
// the flatten moved chunks into `chunks/<clone>/` is therefore unsatisfiable — there are no
// chunks anywhere — and it was written, run, and removed rather than weakened into
// something that would pass. What the flatten *carries* is asserted where bytes exist:
// `TestAFlattenedCloneIsServedFromItsOwnImage` in `internal/agent`, over a real
// VolumeManager, a real WAL and a real device, reading three offsets back after every
// object the parent owns is deleted. This lane says the deployment performs the operation;
// that one says what the operation preserves.
func TestAFlattenedCloneNoLongerNeedsItsParentsSnapshot(t *testing.T) {
	d := start(t)
	first := d.startAgent(t, "agent-1")
	d.waitForHost(t)
	d.seedVolume(t)

	parentID, snapshotID, cloneID := aCloneOfASnapshot(t, d, first)

	// The clone has to be served, and then stopped, before it has an image of its own —
	// which is the state a flatten rewrites. A flatten of a clone that never published is a
	// real case and it is the easy one; this is the one with something to preserve.
	waitFor(t, 60*time.Second, "the clone's socket", func() bool {
		_, err := os.Stat(filepath.Join(d.sockDir, cloneID+".sock"))
		return err == nil
	})
	waitForLineWith(t, first, 60*time.Second,
		"read view recovered from the object store", "volume_id="+cloneID)
	first.Stop(t, 60*time.Second)

	if keys := storeKeys(t, d, "image/"+cloneID+"/manifest.json"); len(keys) != 1 {
		t.Fatalf("the clone has no image of its own after its host stopped (%v)", keys)
	}

	// Detached first, which is the command's precondition: a flatten replaces the manifest a
	// serving host would compare against at its stop.
	d.detachVolume(t, cloneID)
	d.flattenVolume(t, cloneID)

	// The object its whole lineage hung on. After this the clone either reads out of its own
	// image or it does not read at all.
	deleteObject(t, d, "image/"+parentID+"/snapshots/"+snapshotID+".json")

	// Placed back on the same host by name, because placement is not this test's subject and
	// a policy that picked a different host would make the assertion below about scheduling.
	d.attachVolume(t, cloneID, d.hostID)
	second := d.startAgent(t, "agent-2")
	waitForLineWith(t, second, 60*time.Second,
		"read view recovered from the object store", "volume_id="+cloneID)
	second.Stop(t, 60*time.Second)
}

// flattenVolume runs `control-plane -flatten-volume`, the one-shot this increment adds.
func (d *deployment) flattenVolume(t *testing.T, volumeID string) {
	t.Helper()
	p := testinfra.Start(t, testinfra.ProcessConfig{
		Name: "flatten",
		Path: testinfra.Binary(t, "control-plane"),
		Args: append([]string{
			"-database-url", d.dsn,
			"-holder-id", "cp-flatten",
			"-flatten-volume", volumeID,
			"-kek-file", d.kekFile,
		}, d.storeArgs()...),
		Env: d.agentEnv,
	})
	if err := p.Wait(t, startup); err != nil {
		t.Fatalf("flattening %s: %v", volumeID, err)
	}
}

// attachVolume runs `control-plane -attach-volume`, the other half of a detach.
func (d *deployment) attachVolume(t *testing.T, volumeID, hostID string) {
	t.Helper()
	p := testinfra.Start(t, testinfra.ProcessConfig{
		Name: "attach",
		Path: testinfra.Binary(t, "control-plane"),
		Args: append([]string{
			"-database-url", d.dsn,
			"-holder-id", "cp-attach",
			"-attach-volume", volumeID,
			"-attach-host", hostID,
		}, append(d.placementArgs(), d.storeArgs()...)...),
		Env: d.agentEnv,
	})
	if err := p.Wait(t, startup); err != nil {
		t.Fatalf("attaching %s to %s: %v", volumeID, hostID, err)
	}
}

// deleteObject removes one key from the bucket, which in a versioned bucket is a delete
// marker (INV-14): the object stops answering, and nothing this repository runs can do more
// than that.
func deleteObject(t *testing.T, d *deployment, key string) {
	t.Helper()
	if _, err := d.store.Client().DeleteObject(t.Context(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}); err != nil {
		t.Fatalf("deleting %s: %v", key, err)
	}
	if keys := storeKeys(t, d, key); len(keys) != 0 {
		t.Fatalf("%s still answers a listing after being deleted: %v", key, keys)
	}
}
