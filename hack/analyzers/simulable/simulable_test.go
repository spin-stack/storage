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

// TestExemptTestInfra asserts the second exemption (DEV-0016): internal/testinfra is
// the build-tagged harness that starts containers and subprocesses for the lanes.
// There is no clock to inject into another process, and nothing here is linked into a
// binary this repository ships.
func TestExemptTestInfra(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), simulable.Analyzer, "exempt/internal/testinfra")
}

// TestTestInfraSiblingIsNotExempt is the narrowness proof for that fragment. Under a
// substring match, a package merely *named* like the exempt one inherits its
// exemption, and nothing anywhere goes red — the analyzer just quietly stops checking
// a package. Every call in the fixture must still be flagged.
func TestTestInfraSiblingIsNotExempt(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), simulable.Analyzer, "notexempt/internal/testinfradriver")
}

// TestHarnessExemptionIsPerFile is the narrowness proof for the per-file half of
// DEV-0016, and it asserts both directions at once because both live in one package:
// the build-tagged harness `fixture_test.go` produces no diagnostics, and its non-test
// neighbour `helper.go` — same directory, same package — is flagged on every line.
// A package-level exemption, or a rule that read "under integration/", would exempt
// both and this test is the only thing that would notice.
func TestHarnessExemptionIsPerFile(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), simulable.Analyzer, "harness/integration/e2e")
}
