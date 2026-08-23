//go:build e2e

package e2e

import (
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/testinfra"
)

// TestFleetStatusReportsTheDeployment runs the one command an operator types when they do
// not yet know what is wrong, against a deployment that is actually running, and insists
// it names both halves of it.
//
// It is here rather than in `cmd/control-plane` because everything it depends on is a
// seam between processes and nothing else can reach it: the host row exists only because
// an Agent process heartbeated it into the catalog, the volume row exists only because a
// second Control Plane process seeded it, and the report is rendered by a third. A unit
// test hands the renderer rows it wrote itself, which is satisfied by a fleet report that
// reads a table nobody writes.
//
// The volume comes back with no host serving it, and that is the truth this build has to
// be able to print rather than a defect to route around: the Agent registers, holds a
// lease and reports an empty volume set, because the local block engine is withdrawn and
// the qcow2 volume manager is not built yet. An operator who runs this sees a live host
// and a volume nobody is serving, which is exactly the situation.
func TestFleetStatusReportsTheDeployment(t *testing.T) {
	d := start(t)
	d.startAgent(t, "volume-agent")
	d.waitForHost(t)
	d.seedVolume(t)

	p := testinfra.Start(t, testinfra.ProcessConfig{
		Name: "fleet-status",
		Path: testinfra.Binary(t, "control-plane"),
		// No -holder-id and no object-store flags: it takes no term and writes nothing,
		// and the catalog is what it reports. That exemption is the command's own
		// decision (see cmd/control-plane/main.go) and this is where it is exercised.
		Args: []string{"-database-url", d.dsn, "-fleet-status"},
		Env:  d.agentEnv,
	})
	if err := p.Wait(t, startup); err != nil {
		t.Fatalf("-fleet-status: %v", err)
	}
	out := strings.Join(p.Output(), "\n")

	// The host the Agent registered. Asserted by id rather than by a row count, because a
	// report that prints *a* host is satisfied by a renderer that invents one.
	if !strings.Contains(out, d.hostID) {
		t.Fatalf("-fleet-status does not name the host that has been heartbeating (%s):\n%s", d.hostID, out)
	}
	// And that it reached the fleet section as a live host rather than only appearing in
	// a volume's row: liveness is rendered against the lease TTL, which is the one thing
	// in this report that is computed rather than selected.
	if !strings.Contains(out, "ACTIVE") {
		t.Fatalf("the host is in the catalog and the report calls it nothing:\n%s", out)
	}
	// The seeded volume. Its id is not known to this test — seeding prints it and nothing
	// returns it — so the assertion is on the section: a deployment with one volume must
	// not render an empty volumes table.
	if !strings.Contains(out, "VOLUME") {
		t.Fatalf("-fleet-status printed no volumes table for a fleet with one volume:\n%s", out)
	}
}
