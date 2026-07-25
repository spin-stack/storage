package db_test

import (
	"embed"
	"regexp"
	"strings"
	"testing"
)

// The .sql sources are embedded rather than read from disk: the test needs no file
// I/O at all, which keeps it inside INV-01 without an exemption.
//
//go:embed queries/*.sql
var querySources embed.FS

// TestEveryMutatingQueryIsTermGuarded is DEV-0005. §7 says every Control-Plane write
// validates the leader term, so a zombie CP affects 0 rows — but that was a
// convention, enforced only by whoever wrote the query remembering it, and
// operations.sql had been written without it. This reads the query files and fails on
// any INSERT/UPDATE/DELETE that does not consult control_plane_leader.
//
// Exemptions must be listed explicitly, with the reason, so adding one is a visible
// decision rather than an omission.
func TestEveryMutatingQueryIsTermGuarded(t *testing.T) {
	exempt := map[string]string{
		// Leadership acquisition is what *establishes* the term; it cannot be guarded
		// by the value it is about to set (§7 single-active election).
		"AcquireLeadership": "the query that elects the leader and writes the new term",
	}

	entries, err := querySources.ReadDir("queries")
	if err != nil {
		t.Fatal(err)
	}
	nameRe := regexp.MustCompile(`(?m)^--\s*name:\s*(\S+)`)
	mutRe := regexp.MustCompile(`(?is)\b(INSERT\s+INTO|UPDATE\s+\w+|DELETE\s+FROM)\b`)

	var unguarded []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		body, err := querySources.ReadFile("queries/" + e.Name())
		if err != nil {
			t.Fatal(err)
		}
		// Split the file into one chunk per named query.
		idx := nameRe.FindAllStringSubmatchIndex(string(body), -1)
		for i, loc := range idx {
			name := string(body[loc[2]:loc[3]])
			end := len(body)
			if i+1 < len(idx) {
				end = idx[i+1][0]
			}
			query := string(body[loc[0]:end])
			if !mutRe.MatchString(query) {
				continue // a read
			}
			if _, ok := exempt[name]; ok {
				continue
			}
			if !strings.Contains(query, "control_plane_leader") {
				unguarded = append(unguarded, e.Name()+":"+name)
			}
		}
	}
	if len(unguarded) > 0 {
		t.Fatalf("mutating queries without a leader-term guard (§7): %v", unguarded)
	}
}
