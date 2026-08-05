#!/usr/bin/env bash
# Every `task <name>` a GitHub workflow invokes resolves against this Taskfile.
#
# Why this exists at all: no workflow in this repository has ever executed (STATUS.md),
# so the only thing that has ever checked `.github/workflows/*.yml` is somebody reading
# it. A renamed or deleted target is then a red run on the day CI first runs — or, worse,
# on the day the guest lane first matters — and the message is `task: Task "x" does not
# exist`, forty seconds into a job that was supposed to be proving something else.
#
# It is a static check on purpose. Running the workflows locally (act, or a self-hosted
# runner) was rejected: it needs Docker, a registry credential and the published QEMU
# image, which is precisely the set of things that are missing and that make the guest
# jobs unrunnable here. What *can* be settled without any of that is whether the names
# line up, and that is the failure this check has actually seen — `guest:proofs` was a
# target whose own description claimed CI ran it, and no workflow named it.
#
# `task --summary <name>` is the resolver rather than a parse of the Taskfile's YAML:
# it is the Taskfile's own answer to "is this a target", it costs one process, and it
# exits non-zero for a name that does not exist (200) *and* for an internal-only one
# (202) — which a workflow must not call either.
set -euo pipefail

cd "$(dirname "$0")/.."

# The same binary that invoked this script, not whatever `task` is on PATH: a Taskfile
# read by one version of go-task and resolved by another is a difference this check
# would report as a missing target. `task` is the fallback for running the script by
# hand.
TASK_EXE=${TASK_EXE:-task}

shopt -s nullglob
workflows=(.github/workflows/*.yml .github/workflows/*.yaml)
# A glob that matches nothing would make every loop below a no-op that exits 0 — the
# same shape as `go test -run` selecting no tests, which is the defect wave 0 removed
# from `task dst`. If the directory moves, this check must go red, not quiet.
if [ ${#workflows[@]} -eq 0 ]; then
  echo "no workflows under .github/workflows — this check would prove nothing" >&2
  exit 1
fi

# Deliberately permissive: any `task <name>` in the file, including the ones inside
# comments and inside the `echo` lines that tell a human what to run. A name in a
# comment that no longer resolves is stale advice, which is worth the same red as a
# stale invocation. The cost of being permissive is that prose of the form
# "this task takes a while" would be read as a target named `takes`; that fires loudly,
# and the fix is to write the sentence differently or backtick the name. The opposite
# error — a regex anchored to `run:` that quietly skips an invocation written as
# `$(task ...)` or continued onto the next line — is silent, and silent is the whole
# problem this file is about.
mapfile -t names < <(grep -ohE '\btask [a-z][a-z0-9:._-]*' "${workflows[@]}" | sed -E 's/^task //' | sort -u)

if [ ${#names[@]} -eq 0 ]; then
  echo "no 'task <name>' invocations found in ${workflows[*]} — the extraction is broken," >&2
  echo "or the workflows stopped calling the Taskfile; either way this check is not checking." >&2
  exit 1
fi

missing=()
for name in "${names[@]}"; do
  if "$TASK_EXE" --summary "$name" >/dev/null 2>&1; then
    echo "  ok  $name"
  else
    echo "  MISSING  $name"
    missing+=("$name")
  fi
done

if [ ${#missing[@]} -ne 0 ]; then
  echo >&2
  echo "these names appear in .github/workflows and are not targets of this Taskfile:" >&2
  for name in "${missing[@]}"; do echo "  task $name" >&2; done
  echo >&2
  echo "a workflow invoking one of them fails at the moment CI runs, which for the guest" >&2
  echo "jobs is the moment the proofs matter. Rename the reference or restore the target." >&2
  echo "(An internal: true target fails here too — a workflow may not call one.)" >&2
  exit 1
fi

echo "OK: ${#names[@]} task names in ${#workflows[@]} workflow files, all resolve"
