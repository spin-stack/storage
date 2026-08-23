#!/usr/bin/env bash
# Which production comments still describe a mechanism this tree withdrew — computed,
# not remembered.
#
# This repository deleted ~10.000 lines of Markdown because documents drifted from the
# code, and moved the reasoning into comments at the line that makes each decision. That
# was right, and CLAUDE.md now tells every reader that "the decision lives in the code, at
# the line that makes it". The rot moved with it, and inside a comment it is strictly
# worse than it was in a document: a comment has no banner, no date and no owner, and the
# project's own instructions say to trust it.
#
# The review that produced this check found, among others:
#
#   - wal.Log.Flush's doc comment describes uploading and verifying every covering object
#     and VERIFYING THE LEASE before the ACK. Three functions below, durableStep does an
#     fdatasync and advances a watermark. Nothing uploads. Nothing consults a lease.
#   - cmd/volume-agent says the object store "is what FLUSH makes a write durable in".
#     It has not been since ADR-0026.
#
# WHAT A FINDING IS. A **(symbol, term) pair**: one vocabulary term, inside the comments
# attached to or contained by one declaration — `internal/wal/log.go:Log.Flush:remote`.
# Three alternatives were rejected:
#
#   - **file:line**, the obvious key, is what hack/deadcode.sh refuses for its own list and
#     for the same reason: a line number moves whenever anything above it does, so the list
#     would go stale on edits that have nothing to do with it.
#   - **the whole file** collapses log.go's eight mentions of `checkpoint` into one
#     decision, so allow-listing the honest one silences the lying one beside it.
#   - **the line's text**, hashed, is stable against edits above and against nothing else:
#     rewording a legitimate comment would fail the gate with a message about a hash.
#
# The symbol is what a human acts on ("is what Log.Flush says about `remote` still true?"),
# it survives edits above it, and it is the same unit CLAUDE.md uses when it says the
# decision lives at the line that makes it. It moves when the symbol is renamed — which is
# the one edit where re-reading the comment is worth the cost.
#
# A WORD IS NOT A MECHANISM, and a gate that cannot tell them apart is a gate that gets
# turned off in a week. `remote` appears in `nvme_remote_backlog_bytes`, a live column;
# `standby` in `standby_host_id`; `gc` in Go's own garbage collector. Two rules:
#
#   1. Comment text only — never code, never a string literal. A URL in `"http://…"` is
#      not a comment and neither is an identifier.
#   2. Identifier-aware word boundaries: a match must not touch `[A-Za-z0-9_]` on either
#      side, so `nvme_remote_backlog_bytes`, `StandbyHostID` and `gcInterval` do not match
#      while `remote`, `warm standby` and `GC` in prose do.
#
# Rule 2 is not asserted by reading it. **Every term in the table below ships with a string
# it must match and a live identifier it must not**, and this script re-proves all of them
# before it looks at a single file (see "the vocabulary" and `selftest`). A term whose
# pattern is mistyped matches nothing and would otherwise report success for ever — the
# same defect as `task dst`'s `-run` regex that selected no tests.
#
# TWO LISTS, AND THE GATE. hack/comment-rot-allow.txt says "this mention is legitimate";
# hack/comment-rot-pending.txt says "this comment is a lie somebody still owes a
# correction". Both are hack/deadcode-allow.txt's format and are read by one parser. The
# split is the point: several comments name a withdrawn mechanism precisely to say it is
# gone, and those are correct for ever; the ones that describe it in the present tense are
# debt, and putting them in the allowlist would cost that file its meaning.
#
# EXIT POLICY, identical in shape to hack/deadcode.sh: non-zero when a finding is in
# neither list, when an entry in either list no longer matches a finding (the comment was
# fixed, the symbol renamed, the term dropped), when an entry carries no reason, and when
# both lists claim the same key. That makes it a ratchet that turns one way and fails in
# both directions: the set of unexplained mentions cannot grow, and a list entry cannot
# outlive its finding.
#
# REJECTED: an in-comment escape (`//commentrot:ok`). It is cheaper for the author, which
# is exactly the problem — the decision would live in the file being defended, invisible
# to anyone auditing the set, and the count could grow without a second file ever being
# edited. The cost of one line in a list, in a commit somebody reviews, is the mechanism.
#
# The result is valid for one GOOS/GOARCH/build-tag configuration — linux/amd64, no tags,
# what CI runs and what the binaries ship as.
set -euo pipefail

cd "$(dirname "$0")/.."

# Byte ordering, not the developer's locale: `sort` in a UTF-8 locale folds punctuation
# while `comm` compares bytes, and these keys are full of `.`, `/` and `:`. The same
# reason hack/deadcode.sh pins it, plus one more — the `§` in a term is two bytes, and a
# byte locale is the only one where awk's regex over it is predictable.
export LC_ALL=C

MODULE=github.com/spin-stack/storage
ALLOW=${ALLOW:-hack/comment-rot-allow.txt}
PENDING=${PENDING:-hack/comment-rot-pending.txt}

# ROOTS ARE THE BINARIES, AND THE SCOPE IS WHAT THEY LINK. hack/deadcode.sh's roots,
# deliberately: ./cmd/... . integration/guestinit was the third until 2026-08-22, PID 1
# inside the guest and as much a binary this repository built as the other two; it went
# with the local block engine it booted against. "Production comments" then needs no
# hand-maintained exclusion list — the harness (internal/dst,
# internal/simio/sim, the *test packages) is out because no binary links it, and a package
# that a binary starts linking is in from that moment, with no edit here.
ROOTS=(./cmd/...)

# --- the vocabulary -----------------------------------------------------------------
#
# Five columns, tab-separated: the term's name (what a key says, so the lists read as
# English and not as regex), the pattern (lowercase — every line is folded before
# matching), a string the pattern MUST match, a live identifier it MUST NOT, and what was
# withdrawn. A term earns its place by naming a mechanism with **no implementation in this
# tree**; the evidence is in the last column and was checked against the code, not against
# a document.
#
# Terms that were considered and left out, because the word outlived the mechanism and a
# term that fires on live prose is how a gate gets deleted:
#
#   - `upload`  — a volume is uploaded to the object store when it stops (image.Publish).
#                 Only `uploader`, the withdrawn per-FLUSH component, is a mechanism.
#   - `lease`   — the lease is live; it is the ACK *gate* that went, so the term is
#                 `lease-gated` and the sentences that are left are found through `remote`.
#   - `materialize` — the package went; the verb did not. A clone still assembles its
#                 ancestry, and blockdev's "materialized base image" is that, not the
#                 deleted cross-host mover.
#   - `recovery`, `fencing wait`, `epoch`, `self-fence` — all still implemented. Only
#                 §16's `SELF_FENCED` state itself is gone.
#   - `INV-06`/`INV-07`/`INV-08`/`INV-13` — an invariant id is a different axis. INV-13's
#                 truncation floor is still enforced by wal.StrictOrder, so the id is not
#                 evidence of anything either way.
vocabulary() {
	cat <<-'TERMS'
		remote	remote	a FLUSH in remote mode	nvme_remote_backlog_bytes	the `remote` durability mode and the WAL→S3 chain behind it (ADR-0026 §1); V1 has one ACK contract and it is fdatasync
		uploader	uploader	the uploader retries	uploaderState	the per-FLUSH WAL uploader; nothing in the tree PUTs on the write path
		batcher	batcher	harmless in the batcher	batcherQueue	the batch of records an upload drained; the WAL has no pending-batch list
		checkpoint	checkpoint(s|ed|ing|er|ers)?	truncate after a checkpoint	checkpointID	internal/checkpoint, 215 lines, deleted with the remote chain; nothing truncates the local WAL mid-session
		objectize	objectiz(e|es|ed|ing|ation)	not-yet-objectized extents	objectizer_count	mid-session objectization of WAL extents into S3 objects
		compaction	compaction	objectization and compaction	compaction_bytes	compaction of the objects that chain produced
		gc	gc	the GC's grace period	gcInterval	internal/gc, 381 lines, deleted; nothing sweeps the bucket, so a mention in the present tense sends a reader looking for a sweeper
		warm-standby	warm standby	acting as warm standby	warm standbyHostID	the pre-warmed second host; placement still prefers a cached host, but nothing keeps one warm
		lease-gated	lease-gated	the lease-gated ACK	lease-gatedly	the rule that a FLUSH ACK required a lease valid on the monotonic clock (INV-06)
		lease-valid	(in)?valid lease|lease is (still )?(in)?valid	the lease is still valid	the lease is validated on arrival	the same rule written the other way round — "a FLUSH is ACKed only while the lease is valid". The lease itself is live and decides who serves, so this term finds sentences, not the mechanism, and the live ones are in the allowlist.
		self-fenced	self_fenced	§16 SELF_FENCED	self_fenced_at	the §16 state a log entered when a durable step found the lease invalid. Losing the lease still stops the device — that is live — but there is no such state to transition to.
		14.4	§14\.4	the §14.4 ACK	§14.42	the six-step remote ACK chain (close the batch, fdatasync, upload, verify, advance, ACK). ADR-0026 keeps it in the design document as the V2 path and removed the code.
	TERMS
}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

vocabulary >"$work/terms"

# --- the vocabulary proves it can fire, before anything is scanned -------------------
#
# A pattern that matches nothing reports success for ever, and this repository has paid
# for that shape three times (a `-run` regex selecting no tests, a glob matching no
# workflows, a checker that could not fire). The positive fixture is the proof the pattern
# still fires; the negative fixture is the proof of the rule this whole check rests on —
# that `remote` in `nvme_remote_backlog_bytes` is a word and not a mechanism. Both run on
# the same code path the scan uses — literally the same function text, spliced into both
# awk programs from MATCHES below, because a self-test that proves a *copy* of the rule is
# a self-test that stops meaning anything the first time somebody edits one of the two.
#
# THE RULE ITSELF: fold the line, then require that the match touch no [a-z0-9_] on either
# side. That is what separates `remote` from `nvme_remote_backlog_bytes`, and it is one
# line so that it can be read.
MATCHES='function matches(text, pat) { return match(tolower(text), "(^|[^a-z0-9_])(" pat ")([^a-z0-9_]|$)") > 0 }'

selftest() {
	awk -F'\t' "$MATCHES"'
		NF != 5{ printf "malformed vocabulary row (want 5 tab-separated fields, got %d): %s\n", NF, $0; bad++; next }
		seen[$1]++ > 0 { printf "two terms are both named %s, so one of their keys is unreachable\n", $1; bad++ }
		{
			if (!matches($3, $2)) { printf "%s: /%s/ does not match its own example %s — the pattern is mistyped, and a pattern that cannot fire reports success for ever\n", $1, $2, $3; bad++ }
			if (matches($4, $2))  { printf "%s: /%s/ fires on %s, which is an identifier this tree uses. A word is not a mechanism; fix the pattern or the boundary rule\n", $1, $2, $4; bad++ }
			if ($5 == "")         { printf "%s says nothing about what was withdrawn\n", $1; bad++ }
		}
		END { exit(bad > 0) }
	' "$work/terms"
}
if ! selftest >"$work/selftest"; then
	echo "the vocabulary does not hold up, so nothing was scanned:" >&2
	sed 's|^|  |' "$work/selftest" >&2
	exit 1
fi

for list in "$ALLOW" "$PENDING"; do
	test -f "$list" || {
		# A missing list makes every one of its entries "unexplained", which is loud —
		# but a missing *pending* list on a tree that has emptied it looks the same, so
		# name the file and why it has to exist.
		echo "no list at $list — both are read on every run; an empty one is a file with only comments" >&2
		exit 1
	}
done

# --- what gets scanned ----------------------------------------------------------------
#
# GoFiles, so `_test.go` is out by construction rather than by a pattern somebody has to
# keep right, and so is any file this GOOS/build-tag configuration excludes — the set the
# shipped binaries are actually built from.
go list -deps -f \
	'{{if .Module}}{{if eq .Module.Path "'"$MODULE"'"}}{{$d := .Dir}}{{range .GoFiles}}{{$d}}/{{.}}{{"\n"}}{{end}}{{end}}{{end}}' \
	"${ROOTS[@]}" |
	sed "s|^$PWD/||" >"$work/all-files" || {
	echo >&2
	echo "the roots do not load, so this would scan a subset of the tree and call it clean." >&2
	echo "fix the build first; a short file list here means 'not analysed', not 'no rot'." >&2
	exit 1
}

# Generated files carry the header and no human wrote their comments; sqlc's output is six
# of the files a binary links. `grep -L` lists the files that do NOT match.
if [ -s "$work/all-files" ]; then
	# shellcheck disable=SC2046 # the list is go list output: no spaces in these paths
	grep -L '^// Code generated .* DO NOT EDIT\.$' $(cat "$work/all-files") | sort >"$work/files"
else
	: >"$work/files"
fi

n_files=$(wc -l <"$work/files")
if [ "$n_files" -eq 0 ]; then
	echo "no files to scan. That is not 'no rot' — it means the roots, the module filter or" >&2
	echo "the generated-file rule stopped selecting anything, and this check is answering" >&2
	echo "about an empty tree." >&2
	exit 1
fi

# --- the scan --------------------------------------------------------------------------
#
# One awk pass per file set. It does three things a `grep` cannot: it knows which bytes are
# a comment (a URL inside a string literal is not), it knows which declaration a comment
# belongs to, and it applies the boundary rule from `matches()` — the same function the
# self-test above just proved.
awk -v terms="$work/terms" "$MATCHES"'
	BEGIN {
		FS = "\t"
		while ((getline line < terms) > 0) {
			n = split(line, f, "\t")
			if (n >= 2 && f[1] != "") { names[++np] = f[1]; pats[np] = f[2] }
		}
		close(terms)
	}

	# ---- per-file state ----
	# The flush happens before `fname` moves: a file whose last line is a comment leaves a
	# buffered block, and reporting it under the *next* file’s name is a finding pointing
	# at a line that says something else — which is how internal/lifecycle.go’s trailing
	# note was first reported against internal/lineage/delete.go.
	FNR == 1 {
		flushblock("")
		fname = FILENAME
		top = "package"; ingroup = 0; inblock = 0; nbuf = 0
	}

	{
		split_line($0)                            # sets `comment` and `code`

		if (code ~ /^[ \t]*$/ && comment != "") { # a comment-only line: buffer it
			buf[++nbuf] = comment; bufln[nbuf] = FNR
			next
		}
		if (code ~ /^[ \t]*$/) {                  # blank: a buffered block is detached
			flushblock("")
			next
		}
		flushblock(code)                          # code: the buffer was its doc comment
		settop(code)
		if (comment != "") {                      # a trailing comment belongs to the code
			buf[++nbuf] = comment; bufln[nbuf] = FNR
			flushblock("")
		}
	}

	END { flushblock("") }

	# ---- which bytes are a comment, and which are code ----
	# One pass, setting the two globals `comment` and `code`, because they are two halves
	# of one decision: whether the `//` at column 30 opens a comment depends on whether the
	# quote at column 12 opened a string, and two independent walkers would have to agree
	# about that. Walked byte by byte rather than matched with a regex — `"http://x"` and
	# `"// not a comment"` are code, and a regex for "// outside a string" is not one
	# anybody should have to read.
	function split_line(line,   i, n, c, two, p) {
		comment = ""; code = ""
		n = length(line); i = 1
		if (inblock) {                                  # this line opened inside /* … */
			p = index(line, "*/")
			if (p == 0) { comment = line; return }
			inblock = 0
			comment = substr(line, 1, p + 1)
			i = p + 2
		}
		while (i <= n) {
			two = substr(line, i, 2); c = substr(line, i, 1)
			if (two == "//") { comment = comment " " substr(line, i); return }
			if (two == "/*") {
				p = index(substr(line, i + 2), "*/")
				if (p == 0) { inblock = 1; comment = comment " " substr(line, i); return }
				comment = comment " " substr(line, i, p + 3); i = i + p + 3; continue
			}
			if (c == "\"" || c == "`" || c == "'"'"'") {
				p = skipstring(line, i, c, n)
				code = code substr(line, i, p - i); i = p; continue
			}
			code = code c; i++
		}
	}
	function skipstring(line, i, q, n,   d) {
		i++
		while (i <= n) {
			d = substr(line, i, 1)
			if (d == "\\" && q != "`") { i += 2; continue }
			if (d == q) { return i + 1 }
			i++
		}
		return i
	}

	# ---- which declaration a comment belongs to ----
	# Top-level declarations start at column 0 in gofmt-ed code, which is what makes this
	# reliable without a Go parser. A doc comment takes the declaration BELOW it; anything
	# else takes the declaration above.
	function settop(code,   m, recv, meth) {
		if (code !~ /^[A-Za-z]/) {
			if (code ~ /^[)}]/) { ingroup = 0 }
			return
		}
		ingroup = 0
		if (match(code, /^func[ \t]+\([^)]*\)[ \t]*[A-Za-z0-9_]+/)) {
			m = substr(code, RSTART, RLENGTH)
			sub(/^func[ \t]+\(/, "", m)
			recv = m; sub(/\).*$/, "", recv)
			sub(/^[A-Za-z0-9_]+[ \t]+/, "", recv); gsub(/[*\[\]]/, "", recv)
			meth = m; sub(/^[^)]*\)[ \t]*/, "", meth)
			top = recv "." meth
			return
		}
		if (match(code, /^func[ \t]+[A-Za-z0-9_]+/))            { top = trimkw(code, "func"); return }
		if (match(code, /^type[ \t]+[A-Za-z0-9_]+/))            { top = trimkw(code, "type"); ingroup = (code ~ /[({][ \t]*$/); return }
		if (match(code, /^(var|const)[ \t]+[A-Za-z0-9_]+/))     { top = trimkw(code, "(var|const)"); ingroup = (code ~ /\([ \t]*$/); return }
		if (match(code, /^(var|const)[ \t]*\(/))                { top = (code ~ /^var/ ? "var" : "const"); ingroup = 1; return }
		if (match(code, /^package[ \t]+[A-Za-z0-9_]+/))         { top = "package"; return }
		if (code ~ /^import/)                                   { top = "import"; ingroup = (code ~ /\([ \t]*$/); return }
	}
	function trimkw(code, kw,   m) {
		match(code, "^" kw "[ \t]+[A-Za-z0-9_]+")
		m = substr(code, RSTART, RLENGTH)
		sub("^" kw "[ \t]+", "", m)
		return m
	}
	# The member a struct field or a grouped var/const comment sits above. Only inside a
	# group: in a function body the next line is as likely to be `x := 1`, and an anchor
	# named after a local variable would move every time somebody renames one.
	function anchorfor(code,   m, savetop, savegroup) {
		if (code == "") { return top }
		if (code ~ /^[A-Za-z]/) {                    # a top-level declaration below us
			savetop = top; savegroup = ingroup
			settop(code)
			m = top
			top = savetop; ingroup = savegroup
			return m
		}
		if (ingroup && match(code, /^[ \t]+[A-Za-z_][A-Za-z0-9_]*/)) {
			m = substr(code, RSTART, RLENGTH); gsub(/^[ \t]+/, "", m)
			return top "." m
		}
		return top
	}

	function flushblock(next_code,   i, j, anchor, key, text) {
		if (nbuf == 0) { return }
		anchor = anchorfor(next_code)
		for (j = 1; j <= np; j++) {
			hits = 0
			for (i = 1; i <= nbuf; i++) {
				if (!matches(buf[i], pats[j])) { continue }
				text = buf[i]; gsub(/^[ \t\/*]+/, "", text); gsub(/[ \t]+$/, "", text)
				if (hits == 0) { key = fname ":" anchor ":" names[j]; lines = bufln[i]; sample = text }
				else           { lines = lines "," bufln[i] }
				hits++
			}
			if (hits > 0) { print key "\t" lines "\t" sample }
		}
		nbuf = 0
	}
' $(cat "$work/files") | sort -u >"$work/found-full"

cut -f1 "$work/found-full" | sort -u >"$work/found"

# --- the lists --------------------------------------------------------------------------
#
# `<key>  # why`, reason mandatory. hack/deadcode-allow.txt's format and its parser, so a
# rule cannot apply to one list and quietly not to the other.
parse_list() { # <file> <entries-out> <noreason-out> <duplicate-out>
	awk '
		/^[[:space:]]*(#.*)?$/ { next }
		{
			i = index($0, "#")
			if (i == 0) { print "NOREASON\t" $0; next }
			key = substr($0, 1, i - 1); reason = substr($0, i + 1)
			gsub(/^[ \t]+|[ \t]+$/, "", key); gsub(/^[ \t]+|[ \t]+$/, "", reason)
			if (key == "") next
			if (reason == "") { print "NOREASON\t" key; next }
			print "ENTRY\t" key
		}
	' "$1" >"$work/parsed"
	awk -F'\t' '$1 == "ENTRY" { print $2 }' "$work/parsed" | sort >"$work/entries-all"
	sort -u "$work/entries-all" >"$2"
	awk -F'\t' '$1 == "NOREASON" { print $2 }' "$work/parsed" | sort -u >"$3"
	# Two lines for one key are two reasons for one decision, and the second is the one
	# nobody reads. uniq -d over the un-deduplicated list is the only place it can be
	# seen — hack/dev-entries.sh checks its pin the same way.
	uniq -d "$work/entries-all" >"$4"
}

parse_list "$ALLOW" "$work/allowed" "$work/noreason-allow" "$work/dup-allow"
parse_list "$PENDING" "$work/pending" "$work/noreason-pending" "$work/dup-pending"
sed "s|^|$ALLOW: |" "$work/noreason-allow" >"$work/noreason"
sed "s|^|$PENDING: |" "$work/noreason-pending" >>"$work/noreason"
sed "s|^|$ALLOW: |" "$work/dup-allow" >"$work/dup"
sed "s|^|$PENDING: |" "$work/dup-pending" >>"$work/dup"

sort -u "$work/allowed" "$work/pending" >"$work/explained"
comm -23 "$work/found" "$work/explained" >"$work/unexplained"
comm -13 "$work/found" "$work/allowed" >"$work/stale-allow"
comm -13 "$work/found" "$work/pending" >"$work/stale-pending"
# One key claiming both "legitimate for ever" and "a lie somebody owes a fix" is neither
# stale nor unexplained, so nothing else here would catch it. It is what a promotion from
# pending to allowed looks like when the old line survives.
comm -12 "$work/allowed" "$work/pending" >"$work/both"

show() { # <keys-file> — each key with where it is and what it says
	while read -r key; do
		awk -F'\t' -v k="$key" '$1 == k { printf "  %s\n      line %s: %s\n", $1, $2, $3 }' "$work/found-full"
	done <"$1"
}

printf 'scanned: %s files linked by %s    mentions: %s    legitimate (%s): %s    owed a correction (%s): %s\n' \
	"$n_files" "${ROOTS[*]}" "$(wc -l <"$work/found")" \
	"$ALLOW" "$(wc -l <"$work/allowed")" "$PENDING" "$(wc -l <"$work/pending")"

rc=0

if [ -s "$work/noreason" ]; then
	echo
	echo "entries with no reason (add '# why', or delete the entry):"
	sed 's|^|  |' "$work/noreason"
	rc=1
fi

if [ -s "$work/dup" ]; then
	echo
	echo "listed twice, so one of the two reasons is the one nobody reads. Keep one line:"
	sed 's|^|  |' "$work/dup"
	rc=1
fi

if [ -s "$work/both" ]; then
	echo
	echo "these are in BOTH lists, which claims the comment is legitimate and is a lie at the"
	echo "same time. Keep the line that is true and remove the other:"
	sed 's|^|  |' "$work/both"
	rc=1
fi

if [ -s "$work/stale-allow" ]; then
	echo
	echo "stale entries in $ALLOW — no comment matches these any more, so the"
	echo "comment was rewritten, the symbol was renamed or the term was dropped. Remove them:"
	echo "an allowlist that outlives its findings is the hand-written list this replaced."
	sed 's|^|  |' "$work/stale-allow"
	rc=1
fi

if [ -s "$work/stale-pending" ]; then
	echo
	echo "these are in $PENDING and are no longer found — the comment was"
	echo "corrected. Remove the lines, in the commit that corrected it: the pending list is a"
	echo "ratchet, and an entry that outlives its finding is how it would stop shrinking:"
	sed 's|^|  |' "$work/stale-pending"
	rc=1
fi

if [ -s "$work/unexplained" ]; then
	echo
	echo "these production comments name a mechanism this tree withdrew, and nothing says"
	echo "whether they are still true:"
	echo
	show "$work/unexplained"
	echo
	echo "each one is a decision, and this step is blocking so that it gets made now:"
	echo "  fix the comment (the default — CLAUDE.md: a doc↔code divergence is fixed in the"
	echo "    increment that finds it),"
	echo "  or add it to $ALLOW if the mention is legitimate — naming the"
	echo "    mechanism to say it is gone is legitimate, and several entries there do,"
	echo "  or add it to $PENDING if it is a correction somebody has to"
	echo "    finish, with who owns it."
	echo "Every one of them takes the reason on the same line."
	rc=1
fi

if [ "$rc" -eq 0 ]; then
	echo "OK: every withdrawn-mechanism mention has a recorded reason, and every reason still applies."
	# Printed on a GREEN run, which once this is a gate is the only kind of run anybody
	# has. hack/deadcode.sh prints its pending set for the same reason: a list shown only
	# when the build is red stops being read the moment the mechanism starts working.
	if [ -s "$work/pending" ]; then
		echo
		echo "still lying, and owed a correction ($PENDING) — the ratchet is not at zero:"
		show "$work/pending"
	else
		echo "$PENDING is empty: no production comment describes a withdrawn mechanism as current."
	fi
fi
exit "$rc"
