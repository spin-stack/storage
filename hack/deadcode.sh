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
# ROOTS ARE THE BINARIES, AND ONLY THE BINARIES: ./cmd/... . It used to be ./cmd/... plus
# integration/guestinit, which was PID 1 inside the guest and as much a binary this
# repository built and booted as the other two; it went with the local block engine it
# booted against. `deadcode -test` was rejected outright: it makes every test's own
# subject reachable, so it answers "is this called by anything at all", which is never the
# question. CLAUDE.md's question is narrower and is the one that found CloneCrossHost —
# does a *binary* reach it.
#
# TWO BLIND SPOTS, both real, and they are why this is a floor rather than a proof:
#
#   1. Reflection. RTA conservatively marks every method of a type that reaches `reflect`
#      as live, so a symbol whose only caller is the DST harness can be absent from this
#      report entirely — `deadcode -whylive` answers "reachable only through reflection".
#      A symbol absent from this report is not evidence that something calls it.
#   2. Packages no binary imports at all are not in the program, so no function in them can
#      be reported. That is where STATUS.md's other entry (`metadata.BumpVolumeEpoch`, in
#      metadata/sim and metadata/pg) lives. The package pass below closes exactly that gap,
#      at package granularity: it names the packages, not the symbols.
#
# TWO LISTS, AND THE GATE. hack/deadcode-allow.txt says "unreachable, and that is correct
# forever"; hack/deadcode-pending.txt says "unreachable, and nobody has finished deleting
# it yet". Both are read the same way and both are enforced the same way — what differs is
# what an entry claims, and keeping them apart is what let this become a blocking step in
# `task ci` without turning the allowlist into the place findings go to be forgotten.
#
# EXIT POLICY. Non-zero when there is a finding neither list mentions, when an entry in
# either list has gone stale (the symbol was deleted, or something now calls it), when an
# entry carries no reason, and when the two lists claim the same symbol. Together those
# make the pending list a ratchet that only turns one way: the set of unexplained findings
# cannot grow, and an entry cannot outlive the code it names. A pinned *count* would have
# neither property — deleting one finding would buy the right to add another, and the one
# ratchet in this repository that pinned a bare integer (`wantBehavioural`) is the one
# that causes merge conflicts.
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
PENDING=${PENDING:-hack/deadcode-pending.txt}
ROOTS=(./cmd/...)

test -x "$DEADCODE" || {
	echo "no deadcode binary at $DEADCODE — run: task tools:deadcode" >&2
	exit 1
}
for list in "$ALLOW" "$PENDING"; do
	test -f "$list" || {
		# A missing list would otherwise make every one of its entries "unexplained",
		# which is loud — but a missing *pending* list on a tree that has emptied it is
		# indistinguishable from that, so say which file and why it must exist.
		echo "no list at $list — both lists are read on every run; an empty one is a file with only comments" >&2
		exit 1
	}
done

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

# The lists. An entry is `<symbol-or-package>  # why`, and the reason is mandatory: a list
# of bare names is a list of things somebody once decided, with the decision missing, which
# is the state this whole task exists to leave. Both files parse identically — one reader,
# so a rule cannot apply to one list and quietly not to the other.
parse_list() { # <file> <entries-out> <noreason-out>
	awk '
		/^[[:space:]]*(#.*)?$/ { next }
		{
			i = index($0, "#")
			if (i == 0) { print "NOREASON\t" $0; next }
			sym = substr($0, 1, i - 1); reason = substr($0, i + 1)
			gsub(/^[ \t]+|[ \t]+$/, "", sym); gsub(/^[ \t]+|[ \t]+$/, "", reason)
			if (sym == "") next
			if (reason == "") { print "NOREASON\t" sym; next }
			print "ENTRY\t" sym
		}
	' "$1" >"$work/parsed"
	grep '^ENTRY' "$work/parsed" | cut -f2- | sort -u >"$2" || true
	grep '^NOREASON' "$work/parsed" | cut -f2- | sort -u >"$3" || true
}

parse_list "$ALLOW" "$work/allowed" "$work/noreason-allow"
parse_list "$PENDING" "$work/pending" "$work/noreason-pending"
sed "s|^|$ALLOW: |" "$work/noreason-allow" >"$work/noreason"
sed "s|^|$PENDING: |" "$work/noreason-pending" >>"$work/noreason"

# Explained = named by either list. Unexplained is what fails the gate, and it is the set
# that may never grow.
sort -u "$work/allowed" "$work/pending" >"$work/explained"
comm -23 "$work/found" "$work/explained" >"$work/unexplained"

# Stale is computed per list rather than against the union, because the two have different
# remedies: a stale allowlist entry means an assumption stopped holding, a stale pending
# entry means somebody finished the deletion and left the line behind.
comm -13 "$work/found" "$work/allowed" >"$work/stale-allow"
comm -13 "$work/found" "$work/pending" >"$work/stale-pending"

# A symbol in both lists claims two opposite things — "correct forever" and "must be
# deleted". It is neither stale nor unexplained, so nothing else here would catch it, and
# it is exactly what happens when a pending entry is promoted to the allowlist and the old
# line is not removed.
comm -12 "$work/allowed" "$work/pending" >"$work/both"

n_found=$(wc -l <"$work/found")
n_allowed=$(wc -l <"$work/allowed")
n_pending=$(wc -l <"$work/pending")
n_unexplained=$(wc -l <"$work/unexplained")
n_stale_allow=$(wc -l <"$work/stale-allow")
n_stale_pending=$(wc -l <"$work/stale-pending")
n_noreason=$(wc -l <"$work/noreason")
n_both=$(wc -l <"$work/both")

echo "roots: ${ROOTS[*]}"
echo "reported: $n_found    explained by $ALLOW: $n_allowed    owed a deletion in $PENDING: $n_pending    unexplained: $n_unexplained"

rc=0

if [ "$n_noreason" -gt 0 ]; then
	echo
	echo "entries with no reason (add '# why', or delete the entry):"
	sed 's|^|  |' "$work/noreason"
	rc=1
fi

if [ "$n_both" -gt 0 ]; then
	echo
	echo "these are in BOTH lists, which claims they are permanently fine and must be"
	echo "deleted at the same time. Keep the line that is true and remove the other:"
	sed 's|^|  |' "$work/both"
	rc=1
fi

if [ "$n_stale_allow" -gt 0 ]; then
	echo
	echo "stale entries in $ALLOW — deadcode no longer reports these, so they were deleted"
	echo "or something now calls them. Remove them; an allowlist that outlives its findings"
	echo "is the hand-written list this replaced:"
	sed 's|^|  |' "$work/stale-allow"
	rc=1
fi

if [ "$n_stale_pending" -gt 0 ]; then
	echo
	echo "these are in $PENDING and are no longer reported — the deletion happened."
	echo "Remove the lines, in the commit that removed the code: the pending list is a"
	echo "ratchet, and an entry that outlives its finding is how it would stop shrinking:"
	sed 's|^|  |' "$work/stale-pending"
	rc=1
fi

if [ "$n_unexplained" -gt 0 ]; then
	echo
	echo "no binary reaches these, and nothing says why that is acceptable:"
	sed 's|^|  |' "$work/unexplained"
	echo
	echo "each one is a decision, and this step is blocking so that it gets made now:"
	echo "  delete it (CLAUDE.md's default: a component with no caller is a liability),"
	echo "  or add it to $ALLOW if a test/DST/lane is its only legitimate caller,"
	echo "  or add it to $PENDING if it is a deletion somebody has to finish."
	echo "Every one of them takes the reason on the same line."
	rc=1
fi

if [ "$rc" -eq 0 ]; then
	echo "OK: every unreachable symbol has a recorded reason, and every reason still applies."
	# The debt is printed on a *green* run, which is the only kind of run anybody has after
	# this becomes a gate. A list that is only shown when the build is red is a list that
	# stops being read the moment it starts working.
	if [ "$n_pending" -gt 0 ]; then
		echo
		echo "still owed a deletion ($PENDING) — the ratchet is not at zero:"
		sed 's|^|  |' "$work/pending"
	else
		echo "$PENDING is empty: nothing unreachable is waiting on a decision."
	fi
fi
exit "$rc"
