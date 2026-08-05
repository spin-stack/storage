#!/usr/bin/env bash
# Which functions a shipped binary links but never calls — computed, not remembered.
#
# CLAUDE.md's rule is that "a component with no caller is a liability, not progress", and
# `CloneCrossHost` was deleted for exactly it. Until now the enforcement was a hand-written
# list in STATUS.md ("Components with no production caller"), and in two consecutive waves
# it was wrong: it went stale inside one increment, and the wave after it added an OTLP
# exporter and a provider that nobody added to the list. A list a human maintains about
# code a human is changing is the same defect as a checker that cannot fire — it reports
# success by not being updated.
#
# ROOTS ARE THE BINARIES, AND ONLY THE BINARIES: ./cmd/... plus integration/guestinit,
# which is PID 1 inside the guest and is as much a binary this repository builds and boots
# as the other two. `deadcode -test` was rejected outright: it makes every test's own
# subject reachable, so it answers "is this called by anything at all", which is never the
# question. CLAUDE.md's question is narrower and is the one that found CloneCrossHost —
# does a *binary* reach it.
#
# TWO BLIND SPOTS, both real, and they are why this is a floor rather than a proof:
#
#   1. Reflection. RTA conservatively marks every method of a type that reaches `reflect`
#      as live. `wal.TruncateLocal` — STATUS.md's flagship entry, called by the DST harness
#      and by nothing else — is therefore NOT reported here; `deadcode -whylive` answers
#      "reachable only through reflection". A symbol absent from this report is not
#      evidence that something calls it.
#   2. Packages no binary imports at all are not in the program, so no function in them can
#      be reported. That is where STATUS.md's other entry (`metadata.BumpVolumeEpoch`, in
#      metadata/sim and metadata/pg) lives. The package pass below closes exactly that gap,
#      at package granularity: it names the packages, not the symbols.
#
# EXIT POLICY. Non-zero when there is a finding nobody has explained, when an allowlist
# entry has gone stale (the symbol was deleted, or something now calls it), and when an
# entry carries no reason. The last two are the anti-rot property that the hand-written
# list never had: this allowlist cannot quietly become the place findings go to be
# forgotten, because an entry that stops matching fails the check that reads it.
#
# The result is valid for one GOOS/GOARCH/build-tag configuration — linux/amd64, no tags,
# which is what CI runs and what the binaries ship as.
set -euo pipefail

# Byte ordering, not the developer's locale: `sort` in a UTF-8 locale folds punctuation
# while `comm` compares bytes, so the two disagree about a list containing `.` and `/` and
# comm reports "input is not in sorted order" — or worse, silently misses a line.
export LC_ALL=C

MODULE=github.com/spin-stack/storage
DEADCODE=${DEADCODE:-.tools/bin/deadcode}
ALLOW=${ALLOW:-hack/deadcode-allow.txt}
ROOTS=(./cmd/... ./integration/guestinit)

test -x "$DEADCODE" || {
	echo "no deadcode binary at $DEADCODE — run: task tools:deadcode" >&2
	exit 1
}
test -f "$ALLOW" || {
	echo "no allowlist at $ALLOW" >&2
	exit 1
}

# deadcode answers about the program it managed to load, and it exits 0 having loaded a
# broken one: a package with type errors is simply absent, so a tree that does not compile
# produces a *shorter* report. That is the direction that hides findings, which makes the
# build a precondition rather than a convenience.
go build "${ROOTS[@]}" >/dev/null || {
	echo >&2
	echo "the roots do not build, so deadcode would report on a program that is missing packages." >&2
	echo "fix the build first; a short report here means 'not analysed', not 'nothing dead'." >&2
	exit 1
}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Symbols, module-relative and one per line. The name (package + func), never the
# file:line: a line number changes whenever anything above it does, so an allowlist keyed
# by position would go stale on edits that have nothing to do with it.
"$DEADCODE" -filter "$MODULE" -f='{{range .Funcs}}{{$.Path}}.{{.Name}}{{"\n"}}{{end}}' "${ROOTS[@]}" |
	sed "s|^$MODULE/||" |
	sort -u >"$work/found"

# The package pass (blind spot 2): packages in the module that no binary imports. Coarse
# on purpose — at this granularity the answer is "nothing links this at all", which is a
# different and blunter statement than "this function is unreachable".
comm -23 \
	<(go list ./... | sed "s|^$MODULE/\{0,1\}||" | sort -u) \
	<(go list -deps "${ROOTS[@]}" | grep "^$MODULE" | sed "s|^$MODULE/\{0,1\}||" | sort -u) |
	sed 's|^|package |' >>"$work/found"
sort -o "$work/found" "$work/found"

# The allowlist. An entry is `<symbol-or-package>  # why it is not a liability`, and the
# reason is mandatory: an allowlist of bare names is a list of things somebody once
# decided, with the decision missing, which is the state this whole task exists to leave.
awk '
	/^[[:space:]]*(#.*)?$/ { next }
	{
		i = index($0, "#")
		if (i == 0) { print "NOREASON\t" $0; next }
		sym = substr($0, 1, i - 1); reason = substr($0, i + 1)
		gsub(/^[ \t]+|[ \t]+$/, "", sym); gsub(/^[ \t]+|[ \t]+$/, "", reason)
		if (sym == "") next
		if (reason == "") { print "NOREASON\t" sym; next }
		print "ALLOW\t" sym
	}
' "$ALLOW" >"$work/parsed"

grep '^NOREASON' "$work/parsed" | cut -f2- | sort -u >"$work/noreason" || true
grep '^ALLOW' "$work/parsed" | cut -f2- | sort -u >"$work/allowed" || true

comm -23 "$work/found" "$work/allowed" >"$work/unexplained"
comm -13 "$work/found" "$work/allowed" >"$work/stale"

n_found=$(wc -l <"$work/found")
n_allowed=$(wc -l <"$work/allowed")
n_unexplained=$(wc -l <"$work/unexplained")
n_stale=$(wc -l <"$work/stale")
n_noreason=$(wc -l <"$work/noreason")

echo "roots: ${ROOTS[*]}"
echo "reported: $n_found    explained by $ALLOW: $n_allowed    unexplained: $n_unexplained"

rc=0

if [ "$n_noreason" -gt 0 ]; then
	echo
	echo "allowlist entries with no reason (add '# why', or delete the entry):"
	sed 's|^|  |' "$work/noreason"
	rc=1
fi

if [ "$n_stale" -gt 0 ]; then
	echo
	echo "stale allowlist entries — deadcode no longer reports these, so they were deleted"
	echo "or something now calls them. Remove them; an allowlist that outlives its findings"
	echo "is the hand-written list this replaced:"
	sed 's|^|  |' "$work/stale"
	rc=1
fi

if [ "$n_unexplained" -gt 0 ]; then
	echo
	echo "no binary reaches these, and nothing says why that is acceptable:"
	sed 's|^|  |' "$work/unexplained"
	echo
	echo "each one is a decision: delete it (CLAUDE.md's default), or add it to $ALLOW"
	echo "with the reason beside it."
	rc=1
fi

[ "$rc" -eq 0 ] && echo "OK: every unreachable symbol has a recorded reason, and every reason still applies."
exit "$rc"
