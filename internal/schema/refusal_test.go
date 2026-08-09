package schema_test

import (
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/schema"
)

// TestTheRefusalColumnStoresExactlyTheRefusalsGoKnows is the price of choosing a closed
// vocabulary over a free string, collected in one place.
//
// A refusal is spelled three times — the Go type, the wire enum, and this column's CHECK
// — and the argument for that is that a new refusal is a new code path in this repository
// and so the three move in one commit. This is what makes that true rather than hoped
// for: the two directions of drift have opposite and equally bad failures, and neither
// shows up until an incident. A value in Go that the column refuses turns the first
// report of a *new* refusal into a failing RPC — the Agent shouting about a volume it
// cannot serve, and the Control Plane dropping the message. A value in the column that Go
// no longer has is a row nothing can parse, so `volumeFromRow` fails and every read of
// that volume — including `-fleet-status`, the thing an operator ran to find out what is
// wrong — returns an error instead of the fleet.
//
// It reads the declared state rather than a live database, so it runs in the unit lane
// where a schema edit is made, not in the Docker-gated one where it would be found later.
func TestTheRefusalColumnStoresExactlyTheRefusalsGoKnows(t *testing.T) {
	// The CHECK list as written, across however many lines gofmt-for-SQL leaves it on.
	re := regexp.MustCompile(`(?s)CHECK\s*\(refusal IN \((.*?)\)\)`)
	m := re.FindStringSubmatch(schema.SQL)
	if m == nil {
		t.Fatal("schema.sql has no CHECK on volumes.refusal: the column takes any string, " +
			"and the vocabulary is then whatever an Agent happens to send")
	}
	var stored []string
	for _, raw := range strings.Split(m[1], ",") {
		stored = append(stored, strings.Trim(strings.TrimSpace(raw), "'"))
	}

	want := lifecycle.RefusalNames()
	slices.Sort(stored)
	slices.Sort(want)
	if !slices.Equal(stored, want) {
		t.Fatalf("volumes.refusal accepts %v and internal/lifecycle declares %v", stored, want)
	}
}
