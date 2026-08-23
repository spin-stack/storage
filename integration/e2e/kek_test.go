//go:build e2e

package e2e

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/spin-stack/storage/internal/testinfra"
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
//
// # What this used to assert, and why it does not any more
//
// It used to end at a served volume: the Agent refuses one whose kek_id is not its own,
// so a socket bound *was* the proof that both binaries reached the same key from the same
// file. There is no data path and no socket now — QEMU owns the local format from here
// on — so the evidence moves to the two ids themselves, and the assertion moves with it:
// the id the Control Plane wrote into the catalog when it wrapped the DEK, against the id
// the Agent printed when it loaded the key. Neither is computed by this test. They come
// from two processes that read one file, which is the whole of what went wrong.
//
// When the qcow2 volume manager lands, the served-volume assertion is the stronger one
// and should come back on top of this rather than instead of it: this one still fails on
// a machine where nothing can be served.
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

	// And what the Agent reached from the same file. The id is derived from the key's
	// bytes, never configured, so two binaries printing the same id is two binaries that
	// parsed the file identically — which is exactly what the hex-encoded-key incident
	// was not.
	if got := agentKEKID(t, agent); got != kekID {
		t.Fatalf("the Agent loaded KEK %q from %s; the Control Plane wrapped the volume's DEK under %q:\n%s",
			got, d.kekFile, kekID, strings.Join(agent.Output(), "\n"))
	}
}

// agentKEKID reads the id out of the line the Agent prints when it loads the key. The
// line and not a field on the process: an operator diagnosing "this Agent cannot open
// its volumes" reads exactly this, so a line that stopped carrying the id is the same
// regression as an id that was never derived.
func agentKEKID(t *testing.T, p *testinfra.Process) string {
	t.Helper()
	for _, line := range p.Output() {
		if !strings.Contains(line, "key-encryption key loaded") {
			continue
		}
		for _, tok := range strings.Fields(line) {
			if id, ok := strings.CutPrefix(tok, "kek_id="); ok {
				return strings.Trim(id, `"`)
			}
		}
		t.Fatalf("the Agent said it loaded a key and did not say which:\n%s", line)
	}
	t.Fatalf("the Agent never printed the key it loaded:\n%s", strings.Join(p.Output(), "\n"))
	return ""
}
