// Package migrations embeds the Atlas migration files (ADR-0007) so tests and
// tooling can apply the real, versioned migrations rather than a shortcut. Atlas
// owns authoring (task db:migrate:diff) and production apply (task db:migrate); this
// embed is for programmatic apply in integration tests.
package migrations

import (
	"embed"
	"fmt"
	"io/fs"
	"sort"
)

//go:embed *.sql
var FS embed.FS

// Ordered returns the migration SQL bodies in lexicographic (== chronological,
// Atlas timestamp-prefixed) order.
func Ordered() ([]string, error) {
	entries, err := fs.ReadDir(FS, ".")
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	out := make([]string, 0, len(names))
	for _, n := range names {
		body, err := FS.ReadFile(n)
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", n, err)
		}
		out = append(out, string(body))
	}
	return out, nil
}
