//go:build e2e

package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestBothBinariesAgreeOnTheKEK is the regression this lane was built too late to
// prevent and exists to stop repeating.
//
// The Control Plane wraps a volume's DEK under the KEK it read; the Agent unwraps it
// under the KEK *it* read, and refuses the volume unless the ids match. Those were two
// separate readers with different rules until 2026-08-02 — a hex-encoded key file was a
// working Control Plane and a dead Agent — and the id was a flag on one side and a hash
// on the other. Nothing in-process could see it: it is only wrong once both binaries
// read the same file.
func TestBothBinariesAgreeOnTheKEK(t *testing.T) {
	d := start(t)
	agent := d.startAgent(t, "volume-agent")
	d.waitForHost(t)
	d.seedVolume(t)

	// What the Control Plane recorded, read straight from the catalog.
	pool, err := pgxpool.New(t.Context(), d.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var kekID string
	var keyID int64
	if err := pool.QueryRow(t.Context(),
		`SELECT kek_id, dek_key_id FROM volumes LIMIT 1`).Scan(&kekID, &keyID); err != nil {
		t.Fatal(err)
	}
	if kekID != d.kekID {
		t.Fatalf("the Control Plane wrapped under %q; the same file hashes to %q", kekID, d.kekID)
	}
	if keyID == 0 {
		t.Fatal("the catalog holds a DEK with no version (§15.1)")
	}

	// And the Agent opens it. It refuses a volume whose kek_id is not its own and a
	// DEK whose version does not authenticate, so a served volume *is* the assertion
	// that both binaries reached the same key from the same file.
	volumeID := waitForServedVolume(t, d)
	waitFor(t, 30*time.Second, "the volume's socket", func() bool {
		_, err := os.Stat(filepath.Join(d.sockDir, volumeID+".sock"))
		return err == nil
	})
	for _, line := range agent.Output() {
		if strings.Contains(line, "unwrapping the DEK") || strings.Contains(line, "is wrapped under KEK") {
			t.Fatalf("the Agent could not use the key the Control Plane wrapped: %s", line)
		}
	}
}
