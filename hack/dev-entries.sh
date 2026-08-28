#!/usr/bin/env bash
# Which doc↔code divergences are open right now, and who owes the decision — computed
# from STATUS.md, pinned as a set.
#
# CLAUDE.md's gate ends with "No open DEV entry that this increment introduced", and
# nothing read a DEV entry until this script.
#
# Rejected: list and never fail (easy to never run); fail whenever any entry is open (red
# on the day it lands, because entries wait on decisions a human has not made). What is
# left is hack/deadcode.sh's ratchet over the open SET: an unpinned open entry is red, and
# a pin that outlives its entry is red.
#
# Only heading lines naming a DEV-NNNN id are parsed, so prose cannot move this check.
# Resolved means the id itself is struck (## ~~DEV-0019~~ — …); a heading struck somewhere
# else is reported as ambiguous rather than guessed at.
set -euo pipefail

cd "$(dirname "$0")/.."

# `comm` compares bytes; `sort` in a UTF-8 locale folds punctuation. The ids here are
# ASCII, but the lists are sorted and diffed with the same tools hack/deadcode.sh had to
# pin for exactly this reason.
export LC_ALL=C

STATUS=${STATUS:-docs/plan/STATUS.md}
PIN=${PIN:-hack/dev-entries-open.txt}

test -f "$STATUS" || {
	echo "no STATUS.md at $STATUS — this check reads the file CLAUDE.md calls the only one" >&2
	echo "that tracks state; if it moved, this check has been reporting on nothing." >&2
	exit 1
}
test -f "$PIN" || {
	# A missing pin file would make every open entry "unpinned", which is loud. An empty
	# one is a different and legitimate state (nothing is open), so say which file.
	echo "no pin at $PIN — it is read on every run; an empty set is a file with only comments" >&2
	exit 1
}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Headings only. The id is the FIRST `DEV-NNNN` on the line: a heading names its own entry
# before it mentions another one. Any heading level, so an entry demoted from `##` to
# `###` keeps being seen rather than silently leaving the open set.
awk '
	/^#+[ \t]/ {
		if (match($0, /DEV-[0-9][0-9][0-9][0-9]/)) {
			id = substr($0, RSTART, RLENGTH)
			state = "OPEN"
			if (match($0, "~~[^~]*" id "[^~]*~~")) {
				state = "RESOLVED"
			} else if (index($0, "~~") > 0) {
				state = "AMBIGUOUS"
			}
			print state "\t" id "\t" NR "\t" $0
		}
	}
' "$STATUS" >"$work/headings"

# An enumeration that came back empty is the failure mode this repository keeps finding:
# `go test -run` matching no tests, a glob matching no workflows, a lane skipping itself.
# Zero headings does not mean zero open entries — it means the convention changed or the
# file moved, and this check stopped checking.
if [ ! -s "$work/headings" ]; then
	echo "no DEV-NNNN heading of any kind in $STATUS." >&2
	echo "That is not 'nothing is open': entries are '## DEV-NNNN — …' headings and resolved" >&2
	echo "ones keep their heading with the id struck (~~DEV-NNNN~~), so a file with none has" >&2
	echo "changed convention or moved, and this check is answering about the wrong text." >&2
	exit 1
fi

# Selected with awk on the tab-separated first field rather than `grep -P '^OPEN\t'`:
# `grep -P` is a build-time option (PCRE) and this has to run on whatever grep the CI
# image ships, while awk's field splitting needs no such thing.
field() { awk -F'\t' -v want="$1" -v col="$2" '$1 == want { print $col }' "$work/headings"; }
field OPEN 2 | sort -u >"$work/open"
field RESOLVED 2 | sort -u >"$work/resolved"
awk -F'\t' '$1 == "AMBIGUOUS" { sub(/^[^\t]*\t/, ""); print }' "$work/headings" | sort >"$work/ambiguous"
field AMBIGUOUS 2 | sort -u >"$work/ambiguous-ids"

# An id with an open heading AND a resolved one claims both at once. It is neither
# unpinned nor stale, so nothing else below would catch it, and it is exactly what a
# second entry for the same id looks like when a lane strikes one copy.
comm -12 "$work/open" "$work/resolved" >"$work/contradictory"

# The pin. Format and parser are hack/deadcode-allow.txt's, deliberately: `<id>  # why`,
# reason mandatory. A list of bare ids is a list of things somebody once decided with the
# decision missing, which is the state this whole target exists to leave.
awk '
	/^[[:space:]]*(#.*)?$/ { next }
	{
		i = index($0, "#")
		if (i == 0) { print "NOREASON\t" $0; next }
		id = substr($0, 1, i - 1); reason = substr($0, i + 1)
		gsub(/^[ \t]+|[ \t]+$/, "", id); gsub(/^[ \t]+|[ \t]+$/, "", reason)
		if (id == "") next
		if (reason == "") { print "NOREASON\t" id; next }
		if (id !~ /^DEV-[0-9][0-9][0-9][0-9]$/) { print "MALFORMED\t" id; next }
		print "ENTRY\t" id "\t" reason
	}
' "$PIN" >"$work/pin-parsed"

pinfield() { awk -F'\t' -v want="$1" -v col="$2" '$1 == want { print $col }' "$work/pin-parsed"; }
pinfield ENTRY 2 | sort >"$work/pinned-all"
sort -u "$work/pinned-all" >"$work/pinned"
pinfield NOREASON 2 | sort -u >"$work/noreason"
pinfield MALFORMED 2 | sort -u >"$work/malformed"
# Two lines for one id are two reasons for one decision; the second is the one nobody
# reads. uniq -d over the un-deduplicated list is the only place this can be seen.
uniq -d "$work/pinned-all" >"$work/dup-pin"

# The two directions of the ratchet.
comm -23 "$work/open" "$work/pinned" >"$work/unpinned" # open, and nothing pinned it: it cannot grow
comm -13 "$work/open" "$work/pinned" >"$work/stale"    # pinned, and no longer open: it cannot outlive its entry

echo "status: $STATUS    pin: $PIN"
printf 'entries: %s open, %s resolved    pinned: %s    unpinned: %s\n' \
	"$(wc -l <"$work/open")" "$(wc -l <"$work/resolved")" \
	"$(wc -l <"$work/pinned")" "$(wc -l <"$work/unpinned")"

rc=0

if [ -s "$work/noreason" ]; then
	echo
	echo "pinned with no reason (add '# who owns it, and what closing it takes'):"
	sed 's|^|  |' "$work/noreason"
	rc=1
fi

if [ -s "$work/malformed" ]; then
	echo
	echo "these are not DEV ids — the pin names entries by id, one per line, nothing else:"
	sed 's|^|  |' "$work/malformed"
	rc=1
fi

if [ -s "$work/dup-pin" ]; then
	echo
	echo "pinned twice, so one of the two reasons is the one nobody reads. Keep one line:"
	sed 's|^|  |' "$work/dup-pin"
	rc=1
fi

if [ -s "$work/ambiguous" ]; then
	echo
	echo "these headings are struck somewhere other than the id, so whether the entry is"
	echo "open cannot be read off the heading. Strike the id itself (~~DEV-NNNN~~) if it is"
	echo "resolved, or move the strike off the heading if it is not:"
	sed 's|^|  |' "$work/ambiguous"
	rc=1
fi

if [ -s "$work/contradictory" ]; then
	echo
	echo "these ids have BOTH an open heading and a struck one — the entry claims to be"
	echo "resolved and not resolved at once. Delete the heading that is not true:"
	sed 's|^|  |' "$work/contradictory"
	rc=1
fi

if [ -s "$work/unpinned" ]; then
	echo
	echo "OPEN AND UNPINNED — the gate's fourth line is about exactly these:"
	while read -r id; do
		awk -F'\t' -v id="$id" -v f="$STATUS" \
			'$1 == "OPEN" && $2 == id { print "  " f ":" $3 "\t" $4 }' "$work/headings"
	done <"$work/unpinned"
	echo
	echo "CLAUDE.md: \"No open DEV entry that this increment introduced.\" Shipping with one"
	echo "open is allowed and is a decision somebody makes, so make it here — add the id to"
	echo "$PIN with who owns it and what closing it takes, in the commit that"
	echo "opened it. Or close the entry: strike the id in its heading and say what closed it."
	rc=1
fi

if [ -s "$work/stale" ]; then
	echo
	echo "pinned as open, and no longer open in $STATUS:"
	while read -r id; do
		if grep -q "^$id\$" "$work/resolved"; then
			echo "  $id — resolved; remove the line, in the commit that resolved it"
		elif grep -q "^$id\$" "$work/ambiguous-ids"; then
			# Without this branch the message below would say "no heading at all" about an
			# entry whose heading is right there, which sends the reader looking for a
			# deletion that did not happen. The ambiguity is reported above; this says
			# only that the pin cannot be judged until it is settled.
			echo "  $id — its heading is the ambiguous one above; settle the strike first"
		else
			echo "  $id — no heading at all; it was renumbered or deleted, and the pin outlived it"
		fi
	done <"$work/stale"
	echo
	echo "The pin is a ratchet and this is the direction it turns: an entry that outlives"
	echo "its finding is how the set would stop shrinking."
	rc=1
fi

if [ "$rc" -eq 0 ]; then
	if [ -s "$work/open" ]; then
		# Printed on a GREEN run, which after this becomes a gate is the only kind of run
		# anybody has. A list shown only when the build is red is a list that stops being
		# read the moment the mechanism starts working — hack/deadcode.sh prints its
		# pending set for the same reason.
		echo
		echo "OPEN DEV ENTRIES — every one of them is a doc↔code divergence this tree ships with:"
		while read -r id; do
			awk -F'\t' -v id="$id" '$1 == "OPEN" && $2 == id { print "  " $4 }' "$work/headings"
			awk -F'\t' -v id="$id" '$1 == "ENTRY" && $2 == id { print "      pinned: " $3 }' "$work/pin-parsed"
		done <"$work/open"
		echo
		echo "OK: every open entry is pinned with a reason, and every pinned entry is still open."
	else
		echo
		echo "OK: no open DEV entry. $PIN is empty, and the gate's fourth line is satisfied"
		echo "by the tree rather than by a decision somebody made."
	fi
fi
exit "$rc"
