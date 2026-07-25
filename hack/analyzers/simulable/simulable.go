// Package simulable implements the static check mandated by the architecture
// document §25.1 (INV-01): production code must not call real time, network,
// disk, or S3 primitives directly. All such access goes through the simulable
// interfaces in internal/simio, whose only real implementations live under an
// internal/simio path. This analyzer flags direct use of the forbidden
// primitives anywhere outside those exempt packages.
//
// Resolution is type-based, not name-based: an aliased import of "time" is still
// caught, and a local method that happens to be named Now is not. See
// docs/plan/DECISIONS/ADR-0003-simulable-interfaces-lint.md.
package simulable

import (
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// Analyzer is the go/analysis entry point. It can be run standalone via
// hack/analyzers/simulable/cmd/simulable and as a golangci-lint plugin.
var Analyzer = &analysis.Analyzer{
	Name:     "simulable",
	Doc:      "flags direct time/net/disk/S3 primitives outside internal/simio (§25.1, INV-01)",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      run,
}

// forbidden maps a package path to the set of top-level functions within it that
// production code may not call directly. Keyed by the resolved package path so
// import aliases do not matter.
var forbidden = map[string]map[string]bool{
	"time": {
		"Now": true, "Since": true, "Sleep": true, "After": true,
		"Tick": true, "NewTimer": true, "NewTicker": true, "Until": true,
	},
	"net": {
		"Dial": true, "DialTimeout": true, "DialTCP": true, "DialUDP": true,
		"DialIP": true, "DialUnix": true, "Listen": true, "ListenTCP": true,
		"ListenUDP": true, "ListenUnix": true, "ListenPacket": true,
	},
	"os": {
		"Open": true, "Create": true, "OpenFile": true, "ReadFile": true,
		"WriteFile": true, "Remove": true, "RemoveAll": true, "Mkdir": true,
		"MkdirAll": true, "Rename": true, "Truncate": true,
	},
	"syscall": {
		"Read": true, "Write": true, "Open": true, "Close": true, "Fsync": true,
		"Fdatasync": true, "Connect": true, "Socket": true, "Bind": true,
	},
}

// exemptPathFragment marks packages allowed to touch the real primitives: the
// simio interface package and its real implementations both live under this path.
const exemptPathFragment = "internal/simio"

func run(pass *analysis.Pass) (any, error) {
	if strings.Contains(pass.Pkg.Path(), exemptPathFragment) {
		return nil, nil
	}

	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	nodeFilter := []ast.Node{(*ast.SelectorExpr)(nil)}

	insp.Preorder(nodeFilter, func(n ast.Node) {
		sel := n.(*ast.SelectorExpr)

		// Resolve what the selected identifier refers to. Function references
		// (called or taken as values) resolve to a *types.Func whose package and
		// name we can check regardless of the local import alias.
		obj := pass.TypesInfo.Uses[sel.Sel]
		fn, ok := obj.(*types.Func)
		if !ok || fn.Pkg() == nil {
			return
		}

		names, ok := forbidden[fn.Pkg().Path()]
		if !ok || !names[fn.Name()] {
			return
		}

		pass.Reportf(sel.Pos(), "direct use of %s.%s outside internal/simio: use a simio interface (§25.1, INV-01)",
			fn.Pkg().Path(), fn.Name())
	})

	return nil, nil
}
