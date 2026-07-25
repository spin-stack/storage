// Command simulable runs the simulable analyzer standalone:
//
//	go run ./hack/analyzers/simulable/cmd/simulable ./...
//
// It is also wired into golangci-lint; this binary exists for local runs and as
// a fallback if the plugin path is unavailable (ADR-0003).
package main

import (
	"golang.org/x/tools/go/analysis/singlechecker"

	"github.com/spin-stack/storage/hack/analyzers/simulable"
)

func main() {
	singlechecker.Main(simulable.Analyzer)
}
