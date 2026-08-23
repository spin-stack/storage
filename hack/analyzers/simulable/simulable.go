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

// exemptPathFragments marks the packages allowed to touch the real primitives.
//
//   - internal/simio is the sanctioned home of every simulable interface and of
//     its real implementations (ADR-0003).
//   - internal/testinfra is the build-tagged harness that starts containers and
//     subprocesses for the integration and e2e lanes (DEV-0016). Every file in it
//     carries `//go:build integration || e2e`, so none of it is linked into a
//     binary this repository ships. There is no clock to inject into another
//     *process*: a harness that waited on a simulated one would be measuring
//     nothing, and the whole point of the lanes is that the real world decides.
//
// Two fragments left this list on 2026-08-22 with the packages they named:
// internal/vhost/hostio — vhost-user's SOCK_STREAM socket, its SCM_RIGHTS
// descriptors and the mmap of the front-end's address space, none of which simio
// models — and integration/guestinit, PID 1 inside the guest and so on the far
// side of the interface INV-01 governs. Both belonged to the local block engine,
// which is withdrawn: QEMU manages the local copy-on-write format through qcow2
// now. An exemption for a path nothing occupies is a rule that can only ever
// widen by accident, so it goes with the path.
var exemptPathFragments = []string{
	"internal/simio",
	"internal/testinfra",
}

// hasPathSegments reports whether the slash-separated path p contains frag as a
// complete run of path segments.
//
// Plain strings.Contains would also match a longer sibling — "internal/testinfra" is
// a prefix of a hypothetical "internal/testinfradriver", and "integration" of
// "integrationhelpers" — so a package could inherit an exemption purely from how
// somebody named it. That is silent widening, the failure this analyzer exists to
// prevent, and it is invisible: nothing fails when a check stops checking. Anchoring
// at both ends costs a few lines and has a fixture.
func hasPathSegments(p, frag string) bool {
	for i := 0; ; {
		j := strings.Index(p[i:], frag)
		if j < 0 {
			return false
		}
		start, end := i+j, i+j+len(frag)
		if (start == 0 || p[start-1] == '/') && (end == len(p) || p[end] == '/') {
			return true
		}
		i = start + 1
	}
}

func run(pass *analysis.Pass) (any, error) {
	for _, frag := range exemptPathFragments {
		if hasPathSegments(pass.Pkg.Path(), frag) {
			return nil, nil
		}
	}

	// The harness exemption (DEV-0016) is per *file*, not per package, and it is the
	// only per-file rule there is — so the position lookup below is hoisted behind
	// this and never runs for the rest of the tree.
	//
	// Per-file is the entire point. The lanes' harnesses under integration/ start
	// QEMU, Postgres and our own binaries and then wait for them: there is no clock
	// to inject into another process, and a harness that waited on a simulated one
	// would be measuring nothing. But a lane's directory also holds ordinary
	// host-side code, and a package-level exemption would have carried it along —
	// so a non-test file in the very same directory stays flagged, and a fixture
	// holds both kinds of file side by side to prove it.
	//
	// It matches .golangci.yml's `(^|/)integration/.*_test\.go$` exactly, on
	// purpose: the two INV-01 layers still differ in two places (`cmd/` for depguard
	// only, and the golden fixtures), and every difference is one more thing a
	// reader has to hold in their head before widening either.
	underIntegration := hasPathSegments(pass.Pkg.Path(), "integration")

	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	nodeFilter := []ast.Node{(*ast.SelectorExpr)(nil)}

	insp.Preorder(nodeFilter, func(n ast.Node) {
		sel := n.(*ast.SelectorExpr)

		// The `_test.go` suffix, not a build tag, is what is checked: build
		// constraints are already resolved by the time the analyzer sees a file (an
		// untagged file is simply not in the package), so testing for one here would
		// test nothing. The suffix is also what keeps ordinary unit tests governed —
		// a time.Now() smuggled into a test beside simulable production code is
		// precisely how that code stops being simulable.
		if underIntegration && strings.HasSuffix(pass.Fset.Position(sel.Pos()).Filename, "_test.go") {
			return
		}

		// Resolve what the selected identifier refers to. Function references
		// (called or taken as values) resolve to a *types.Func whose package and
		// name we can check regardless of the local import alias.
		obj := pass.TypesInfo.Uses[sel.Sel]
		fn, ok := obj.(*types.Func)
		if !ok || fn.Pkg() == nil {
			return
		}

		// A *method* is not the package-level function of the same name, and the
		// distinction is load-bearing for exactly one pair: `time.After(d)` reads the
		// clock and hands back a channel, which is the thing INV-01 exists to keep out
		// of production code, while `(time.Time).After(u)` compares two instants that
		// were already obtained — pure arithmetic on values a simio clock produced.
		// Flagging the method made every timestamp comparison a violation, which pushes
		// a caller toward `!t.Before(u) && t != u` to satisfy a linter: worse code, same
		// semantics, no simulability gained. Caught when a fleet-status change compared
		// two heartbeats (2026-08-09).
		if sig, ok := fn.Type().(*types.Signature); ok && sig.Recv() != nil {
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
