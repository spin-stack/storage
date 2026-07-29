package simulable_test

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"

	"github.com/spin-stack/storage/hack/analyzers/simulable"
)

// TestViolating asserts the analyzer flags every forbidden primitive in a
// package that is NOT exempt. Expected diagnostics are declared with `// want`
// comments in testdata/src/violating.
func TestViolating(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), simulable.Analyzer, "violating")
}

// TestCompliant asserts a package that only uses the simio interfaces produces
// zero diagnostics (no `// want` comments in the fixture).
func TestCompliant(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), simulable.Analyzer, "compliant")
}

// TestExempt asserts that code under an internal/simio path may use the real
// primitives without being flagged — that is where the real implementations
// legitimately live (ADR-0003).
func TestExempt(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), simulable.Analyzer, "exempt/internal/simio")
}

// TestExemptVHostHostIO asserts the second and only other exemption (ADR-0020):
// internal/vhost/hostio may open the Unix socket and mmap the front-end's memory
// that vhost-user is made of, because simio models neither.
func TestExemptVHostHostIO(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), simulable.Analyzer, "exempt/internal/vhost/hostio")
}

// TestExemptGuestInit asserts the third exemption (DEV-0013): integration/guestinit
// is PID 1 inside the guest VM, not host code, and it must be able to open a block
// device and mount /proc. Simulating those would make the one test that proves a real
// kernel issues FLUSH prove nothing at all.
func TestExemptGuestInit(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), simulable.Analyzer, "exempt/integration/guestinit")
}

// TestIntegrationItselfIsNotExempt is the narrowness proof for that third exemption.
// What earned it is "runs inside the guest", not "lives under integration/" — the
// host-side lane that drives QEMU is ordinary code and stays simulable.
func TestIntegrationItselfIsNotExempt(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), simulable.Analyzer, "notexempt/integration/vhost")
}

// TestVHostItselfIsNotExempt is the narrowness proof. An exemption that leaked
// to the parent package would make the whole vhost-user backend — protocol,
// virtqueue, request handling — unsimulable without anything failing, which is
// precisely the retrofit INV-01 says is impossible. Every call in the fixture
// must still be flagged.
func TestVHostItselfIsNotExempt(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), simulable.Analyzer, "notexempt/internal/vhost")
}
