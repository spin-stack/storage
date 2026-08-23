//go:build e2e

package e2e

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestTheAgentPreparesAQcow2ChainForADesiredVolume is the seam between the two binaries
// and a third program: the Control Plane provisions a volume, the Agent learns about it
// from its own poll, and something appears on the filesystem that the pinned `qemu-img`
// agrees is a qcow2 of the size the catalog holds.
//
// Everything asserted here is outside the Agent: a file that exists, what another
// program says about it, and the path the Agent published in its log. That last one is
// the point of reading the path out of the log rather than recomputing it — the image
// path and the QMP socket are the contract with whoever launches the VM, so a test that
// derived them the way the code does would assert nothing about them.
func TestTheAgentPreparesAQcow2ChainForADesiredVolume(t *testing.T) {
	d := start(t)
	agent := d.startAgent(t, "agent")
	d.waitForHost(t)
	d.seedVolume(t)

	agent.WaitForLine(t, "volume ready", startup)
	image := field(t, agent.Output(), "volume ready", "image=")
	socket := field(t, agent.Output(), "volume ready", "qmp_socket=")

	if !strings.HasPrefix(image, d.dataDir) {
		t.Errorf("the image is at %q, outside the Agent's --data-dir %q", image, d.dataDir)
	}
	if got, want := filepath.Base(image), "current.qcow2"; got != want {
		t.Errorf("the active tip is named %q, want %q", got, want)
	}
	if got, want := filepath.Base(socket), "qmp.sock"; got != want {
		t.Errorf("the QMP socket is named %q, want %q", got, want)
	}
	// The socket is QEMU's to create, and no VM was launched here. Its absence is the
	// evidence that this Agent does not start one.
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Errorf("something created %s; the Agent is supposed to dial that socket, not serve it", socket)
	}

	// The pinned qemu-img, not a parser of ours (v6 §7).
	out, err := exec.CommandContext(t.Context(), d.qemuImg, "info", "--output=json", image).Output()
	if err != nil {
		t.Fatalf("qemu-img info %s: %v", image, err)
	}
	var info struct {
		Format      string `json:"format"`
		VirtualSize int64  `json:"virtual-size"`
	}
	if err := json.Unmarshal(out, &info); err != nil {
		t.Fatalf("decoding qemu-img info: %v", err)
	}
	if info.Format != "qcow2" {
		t.Errorf("qemu-img says %s is a %q image", image, info.Format)
	}
	// -seed-size in the fixture. The catalog's size and the image's must agree, or a
	// guest is handed a disk of a size nobody asked for.
	if want := int64(1073741824); info.VirtualSize != want {
		t.Errorf("virtual size is %d, want the catalog's %d", info.VirtualSize, want)
	}
}

// field pulls one `key=value` out of the first logged line containing marker.
func field(t *testing.T, lines []string, marker, key string) string {
	t.Helper()
	for _, line := range lines {
		if !strings.Contains(line, marker) {
			continue
		}
		for _, word := range strings.Fields(line) {
			if v, ok := strings.CutPrefix(word, key); ok {
				return v
			}
		}
	}
	t.Fatalf("no line containing %q carried %s", marker, key)
	return ""
}
