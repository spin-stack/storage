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

// TestCommittedBytesIsDerivedInOnePlace is ADR-0017's rule about its own rule. The
// committed-capacity derivation was copied into four queries because the schema tool
// of the day could not express a view (ADR-0019 removed that constraint), and four
// copies of an accounting rule is four places for a placement decision to be taken
// against a different definition of "full".
//
// The marker is jsonb_array_elements: the in-flight half of the sum is the only
// thing in this project that unnests an operation's plan, so a query file that
// mentions it is a query file carrying its own copy of the derivation.
func TestCommittedBytesIsDerivedInOnePlace(t *testing.T) {
	entries, err := querySources.ReadDir("queries")
	if err != nil {
		t.Fatal(err)
	}
	var inlined []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		body, err := querySources.ReadFile("queries/" + e.Name())
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(body), "jsonb_array_elements") {
			inlined = append(inlined, e.Name())
		}
	}
	if len(inlined) > 0 {
		t.Fatalf("queries carrying their own copy of the committed-bytes derivation "+
			"instead of reading host_committed_bytes: %v", inlined)
	}

	// And the readers do read it: a derivation that lives in a view nobody selects
	// from is not a rule that lives once, it is a rule that has been deleted.
	readers := map[string]int{"hosts.sql": 2, "volumes.sql": 1, "operations.sql": 1}
	for file, want := range readers {
		body, err := querySources.ReadFile("queries/" + file)
		if err != nil {
			t.Fatal(err)
		}
		// Comments are stripped first: a file that only *mentions* the view in the
		// prose explaining why it reads it would otherwise count as reading it.
		var sql strings.Builder
		for line := range strings.Lines(string(body)) {
			if !strings.HasPrefix(strings.TrimSpace(line), "--") {
				sql.WriteString(line)
			}
		}
		if got := strings.Count(sql.String(), "host_committed_bytes"); got != want {
			t.Fatalf("%s reads host_committed_bytes %d times, want %d", file, got, want)
		}
	}
}
