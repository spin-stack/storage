//go:build e2e

package e2e

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/real"
	"github.com/spin-stack/storage/internal/testinfra"
)

// TestGCDeletesOrphansAndKeepsAHistory is v6 §24's fifteenth criterion, run the way an
// operator runs it: a real bucket with a real history in it, one object nothing names,
// and the shipped binary told to delete.
//
// It is here and not in a unit test because the two things that can go wrong are both
// outside the planner. One is the classification: `volumes/<id>/HEAD` and
// `descriptor.json` live under the same prefix as the manifests, and a rule about keys
// that is one encoding away from wrong deletes a volume's identity. The other is the
// backend: the object store's Delete is a *marker* on every implementation, and a bucket
// without versioning turns "reversible" into "gone" — RustFS answers here, which is what
// `task backend:conformance` stands in front of.
func TestGCDeletesOrphansAndKeepsAHistory(t *testing.T) {
	d := start(t)
	ctx := t.Context()
	store, err := real.NewS3Store(ctx, real.S3Config{
		Bucket: bucket, Endpoint: d.store.Endpoint, Region: d.store.Region,
		AccessKey: d.store.AccessKey, SecretKey: d.store.SecretKey,
	})
	if err != nil {
		t.Fatalf("opening the bucket this lane's processes share: %v", err)
	}

	// A history, published by the real writer: PUT layer, PUT manifest, CAS HEAD. The
	// volume is not in the catalog, which is deliberate — its HEAD is in the bucket, and
	// §20's roots are the union of the two precisely so a catalog outage cannot propose
	// deleting the fleet.
	vol := ids.New().String()
	enc, err := crypto.NewEncryption(crypto.DEK{Key: [crypto.DEKSize]byte{9}, KeyID: 1}, [16]byte(uuid.MustParse(vol)))
	if err != nil {
		t.Fatal(err)
	}
	var kept []string
	for _, payload := range []string{"one", "two"} {
		m, perr := commit.Publish(ctx, store, enc, strings.NewReader(payload), commit.Request{
			VolumeID: vol, CommitID: ids.New().String(), LayerID: ids.New().String(),
			Epoch: 1, VirtualSize: 1 << 30,
		})
		if perr != nil {
			t.Fatalf("publishing: %v", perr)
		}
		kept = append(kept, commit.ManifestKey(vol, m.CommitID), m.Layer.ObjectKey)
	}
	kept = append(kept, commit.HeadKey(vol))

	// And what an interrupted publish leaves: a layer in the bucket that no manifest names.
	orphan := commit.LayerKey(commit.Digest([]byte("nothing names me")))
	if _, err := store.Put(ctx, orphan, []byte("nothing names me"), objectstore.PutOptions{}); err != nil {
		t.Fatalf("putting an orphan: %v", err)
	}

	// -gc-grace 0 so the run is not a wait: what the grace period protects is an object
	// younger than one publish, and this lane has none in flight.
	p := testinfra.Start(t, testinfra.ProcessConfig{
		Name: "gc-delete",
		Path: testinfra.Binary(t, "control-plane"),
		Args: append([]string{
			"-database-url", d.dsn, "-gc-delete", "-gc-grace", "0",
		}, d.storeArgs()...),
		Env: d.agentEnv,
	})
	if err := p.Wait(t, startup); err != nil {
		t.Fatalf("-gc-delete: %v\n%s", err, strings.Join(p.Output(), "\n"))
	}
	out := strings.Join(p.Output(), "\n")
	if !strings.Contains(out, orphan) {
		t.Fatalf("the run does not name the orphan it was pointed at:\n%s", out)
	}

	if _, err := store.Get(ctx, orphan); err == nil {
		t.Fatal("the orphan still answers after -gc-delete")
	}
	for _, key := range kept {
		if _, err := store.Get(ctx, key); err != nil {
			t.Fatalf("%s belongs to a published history and is gone after -gc-delete: %v\n%s", key, err, out)
		}
	}

	// Reversible, on the backend rather than in the model (INV-14). This is the property
	// that decides how much the planner is allowed to be wrong, and it is a property of
	// the bucket's versioning, which no in-process test can observe.
	if err := store.Restore(ctx, orphan); err != nil {
		t.Fatalf("restoring what the GC deleted: %v", err)
	}
	body, err := store.Get(ctx, orphan)
	if err != nil || string(body) != "nothing names me" {
		t.Fatalf("the restored object came back as %q, %v", body, err)
	}
}
