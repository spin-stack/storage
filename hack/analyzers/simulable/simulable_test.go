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
