#!/usr/bin/env bash
# Every path, ADR and package symbol this tree's prose points at must resolve.
#
# WHAT THIS IS FOR. The other two ratchets check that a *finding* still matches: a term
# still fires on a comment, a symbol is still unreachable. Neither checks the thing the
# entries are made of — the sentence explaining why. A reason that cites a package deleted
# six weeks ago is a reason nobody can evaluate, and it reads as authoritative because it
# names something specific.
#
# It found six on the day it was written, each surviving a comment sweep that read every
# line: `deadcode-allow.txt` citing internal/wal, this repository's own comment-rot.sh
# citing wal.StrictOrder, CLAUDE.md and two ADRs citing ADR-0010 and ADR-0014, and
# metadata.go arguing that V1 has no resize because internal/vhost does not offer a
# protocol feature. Every one of them was prose a human had recently reviewed.
#
# WHY IT NEEDS NO VOCABULARY, unlike comment-rot.sh. That file lists the mechanisms this
# tree withdrew, by hand, and it was blind to `wal` for a month — the largest of them —
# because nobody added the word. This derives what is gone from what does not resolve, so a
# package deleted tomorrow is covered tomorrow.
#
# Three kinds of reference, and each is exact rather than heuristic:
#
#   1. a repository path (internal/…, cmd/…, hack/…, docs/…, integration/…, migrations/…);
#   2. an ADR id, which must have a file under docs/plan/DECISIONS;
#   3. `pkg.Symbol` where `internal/pkg` is one of the paths that did NOT resolve — a
#      reference to a type or function of a package that is gone. Scoped that way on
#      purpose: matching every `x.Y` would drown in json.Unmarshal and t.Fatalf.
#
# NAMING SOMETHING TO SAY IT IS GONE IS LEGITIMATE, and it is most of what the withdrawn-
# mechanism vocabulary is made of ("internal/checkpoint, 215 lines, deleted"). Those live
# in hack/refs-allow.txt with a reason, and the ratchet turns both ways: a reference in
# neither place fails, and an allowlist line that matches nothing fails too, so an entry
# cannot outlive the sentence it excuses.
set -uo pipefail
cd "$(dirname "$0")/.."

ALLOW=${ALLOW:-hack/refs-allow.txt}
test -f "$ALLOW" || { echo "no list at $ALLOW — it is read on every run" >&2; exit 1; }

# The allowlist is excluded: it is a list of references by construction, so every line in
# it would report itself. This script is NOT excluded — its examples are things that are
# genuinely gone, they are in the allowlist like any other deliberate mention, and keeping
# it in scope is what puts `internal/wal` and `internal/vhost` in the withdrawn-package set
# that the third rule is derived from.
files=$(git ls-files '*.go' '*.md' '*.txt' '*.sh' '*.sql' '*.proto' '*.yml' |
	grep -v '^api/gen/' | grep -v '^hack/refs-allow.txt$')
work=$(mktemp -d); trap 'rm -rf "$work"' EXIT

# --- 1. repository paths ----------------------------------------------------------------
# A trailing glob is resolved as one: CLAUDE.md names `internal/simio/real/s3*.go`, which is
# a set of files and not a missing one.
# Segments carry no dot, so `internal/lifecycle.HostState` yields the package and not a
# path that was never written; an optional lowercase extension closes the last one. A ref
# holding `.*` is a regex from a config file, not a path.
# Not preceded by a path character, so `github.com/bufbuild/buf/cmd/buf` is one module path
# and not a `cmd/buf` this repository is missing. A ref followed by a backslash is a regex
# from a config file, as is one holding `.*`.
grep -onE '(^|[^A-Za-z0-9_/.-])(internal|cmd|hack|docs|integration|migrations|deploy)(/[A-Za-z0-9_*-]+)+(\.(go|sh|py|sql|md|txt|yml|json|proto))?' $files 2>/dev/null |
	grep -vE '\.\*' | sed -E 's|^([^:]+):([0-9]+):[^A-Za-z0-9_]?|\1:\2:|' >"$work/paths"
: >"$work/found"
while IFS=: read -r file _ ref; do
	case $ref in
	*'*'*)
		# A glob names a set: `internal/simio/real/s3*.go` is several files, not a
		# missing one. nullglob makes "matched nothing" observable as an empty list.
		# shellcheck disable=SC2086
		shopt -s globstar nullglob
		set -- $ref
		shopt -u globstar nullglob
		[ "$#" -gt 0 ] && continue
		;;
	*)
		test -e "$ref" && continue
		;;
	esac
	printf '%s\t%s\n' "$file" "$ref" >>"$work/found"
done <"$work/paths"

# --- 2. ADR ids -------------------------------------------------------------------------
grep -onE '\bADR-[0-9]{4}' $files 2>/dev/null | while IFS=: read -r file _ ref; do
	compgen -G "docs/plan/DECISIONS/$ref-*.md" >/dev/null 2>&1 || printf '%s\t%s\n' "$file" "$ref"
done >>"$work/found"

# --- 3. symbols of packages that are gone -----------------------------------------------
# The set comes from step 1, so nothing is hand-maintained: a package that disappears puts
# its own name here on the next run.
gone=$(awk -F'\t' '$2 ~ /^internal\/[a-z0-9_]+$/ {sub(/^internal\//,"",$2); print $2}' "$work/found" | sort -u)
if [ -n "$gone" ]; then
	pat=$(echo "$gone" | paste -sd'|' -)
	grep -onE "\\b($pat)\\.[A-Z][A-Za-z0-9_]*" $files 2>/dev/null |
		while IFS=: read -r file _ ref; do printf '%s\t%s\n' "$file" "$ref"; done >>"$work/found"
fi

sort -u "$work/found" -o "$work/found"

# --- the allowlist, and both directions of the ratchet ----------------------------------
grep -vE '^\s*(#|$)' "$ALLOW" | sed 's/\s*#.*//' | sed 's/[[:space:]]*$//' | sort -u >"$work/allow"
awk -F'\t' '{print $1"\t"$2}' "$work/found" | sort -u >"$work/keys"

comm -23 "$work/keys" "$work/allow" >"$work/unexplained"
comm -13 "$work/keys" "$work/allow" >"$work/stale"

printf 'scanned: %s files    references that do not resolve: %s    explained (%s): %s\n' \
	"$(echo "$files" | wc -l)" "$(wc -l <"$work/keys")" "$ALLOW" "$(wc -l <"$work/allow")"

bad=0
if [ -s "$work/stale" ]; then
	bad=1
	cat >&2 <<-'MSG'

		stale entries in the allowlist — nothing matches these any more, so the sentence was
		rewritten or the thing came back. Remove them: a list that outlives its findings is
		the hand-maintained list this replaces.
	MSG
	sed 's/\t/  ->  /; s|^|  |' "$work/stale" >&2
fi
if [ -s "$work/unexplained" ]; then
	bad=1
	cat >&2 <<-'MSG'

		this prose points at something that is not there. A reader cannot tell a reference
		that is merely stale from one that is load-bearing, and both read as authoritative:

	MSG
	sed 's/\t/  ->  /; s|^|  |' "$work/unexplained" >&2
	cat >&2 <<-'MSG'

		fix the sentence (the default — CLAUDE.md: a doc-code divergence is fixed in the
		increment that finds it), or add it to hack/refs-allow.txt WITH THE REASON if naming
		the thing is the point, which is what saying "this is gone" requires.
	MSG
fi
[ "$bad" = 0 ] || exit 1
echo "OK: every path, ADR and withdrawn-package symbol this tree names resolves or is explained."
