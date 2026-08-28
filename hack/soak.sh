#!/usr/bin/env bash
# Run everything this repository can run, over and over, until something breaks.
#
# It exists because of the table at the top of CLAUDE.md: every defect in it was at a
# seam, and every one was found by *running* the real thing rather than by testing a part
# of it more deeply. A gate answers "did this change break something a test already
# knows about". This answers a different question — "does any of it break when it is run
# a hundred times, with the parameters moved and the processes killed at moments nobody
# chose" — and that question needs hours, not a merge.
#
# What each round does:
#
#   1. the DST set across SOAK_SEEDS extra seeds, which is cheap and deterministic, so a
#      failure names a number anyone can re-run;
#   2. the unit and property suites under -race, twice, because rapid draws different
#      cases each run and the race detector needs the schedule to vary;
#   3. the three demonstrations with their parameters moved — the rotation threshold, the
#      churn, the reconcile interval — because every one of those changes which code
#      paths race;
#   4. a demonstration with the Agent SIGKILLed while it is publishing, which is the
#      boundary v6 §15 has a row for and nothing else exercises.
#
# Nothing is fixed here and nothing is committed. A failing round is captured whole —
# logs, seed, parameters, scratch directory — and the soak carries on, because the second
# failure is often the one that explains the first.
#
# It runs in a git worktree pinned to a commit, so the tree it is testing does not move
# while somebody edits the checkout it came from.
set -uo pipefail

cd "$(dirname "$0")/.."
ROOT=$PWD

# Under _output and not /tmp. A soak runs for hours and its findings are the only thing it
# produces; a /tmp cleaner took a run's worth of them once, which is a way of doing the
# work and throwing the answer away. _output is gitignored and is where every other
# artefact of this repository lives.
SOAK_DIR=${SOAK_DIR:-$ROOT/_output/soak-$(date -u +%Y%m%d-%H%M%S)}
SOAK_REF=${SOAK_REF:-HEAD}
SOAK_SEEDS=${SOAK_SEEDS:-200}
SOAK_ROUNDS=${SOAK_ROUNDS:-0} # 0 = until stopped
SOAK_DEADLINE=${SOAK_DEADLINE:-0} # unix seconds; 0 = none
export SOAK_SEEDS

TREE=$SOAK_DIR/tree
FINDINGS=$SOAK_DIR/findings
mkdir -p "$FINDINGS"

say() { printf '\n[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

# --- the tree under test ------------------------------------------------------------
#
# A worktree, because the point is to soak one commit for hours while the checkout it
# came from is being edited. _output is not rebuilt: QEMU is tens of minutes and the
# guest kernel is pinned, so the artefacts are linked from the primary checkout and only
# the two Go binaries are built here — those are what a change would alter.
if [ ! -d "$TREE" ]; then
  git worktree add --detach "$TREE" "$SOAK_REF" >/dev/null || exit 1
  mkdir -p "$TREE/_output/bin"
  for d in share guest lib; do ln -sfn "$ROOT/_output/$d" "$TREE/_output/$d"; done
  for f in "$ROOT"/_output/bin/qemu*; do ln -sfn "$f" "$TREE/_output/bin/$(basename "$f")"; done
fi
cd "$TREE"
COMMIT=$(git rev-parse --short HEAD)
go build -o "$TREE/_output/bin/" ./cmd/... || exit 1

say "soaking $COMMIT in $TREE; findings in $FINDINGS"

# --- capture ------------------------------------------------------------------------
#
# A finding is a directory, not a line: whoever reads it needs the command, the output,
# and the scratch the run left behind. Nothing is summarised here — summarising is what
# throws away the field that turns out to matter.
fail_count=0
capture() {
  local name=$1 log=$2 scratch=${3:-}
  fail_count=$((fail_count + 1))
  local out="$FINDINGS/$(printf '%03d' $fail_count)-$name"
  mkdir -p "$out"
  cp "$log" "$out/output.log" 2>/dev/null || true
  printf 'commit=%s\nround=%d\nname=%s\nwhen=%s\n' "$COMMIT" "$round" "$name" "$(date -u +%FT%TZ)" >"$out/what"
  [ -n "$scratch" ] && [ -d "$scratch" ] && cp -a "$scratch" "$out/scratch" 2>/dev/null
  say "FINDING $fail_count: $name -> $out"
}

run() {
  local name=$1; shift
  local log; log=$(mktemp)
  say "$name"
  if "$@" >"$log" 2>&1; then
    tail -1 "$log"
    rm -f "$log"
    return 0
  fi
  tail -20 "$log"
  capture "$name" "$log"
  rm -f "$log"
  return 1
}

# --- a demonstration with the Agent killed while it publishes -----------------------
#
# v6 §15 has a row for a crash after the snapshot: there may be a sealed layer and a new
# active tip with no commit published, and the host is supposed to come back and publish
# *that* rather than rotate again. Nothing else in this repository puts a process in that
# state, because it lasts for as long as one upload.
kill_mid_publish() {
  local scratch; scratch=$(mktemp -d /tmp/soak-kill-XXXXXX)
  local log; log=$(mktemp)
  say "stage3 with the Agent killed while it publishes"
  DEMO_DIR=$scratch COMMITS_WANTED=2 setsid bash hack/stage3-demo.sh >"$log" 2>&1 &
  local demo=$!
  # Wait for the first commit, then kill the Agent it came from.
  for _ in $(seq 600); do
    grep -q "committed:" "$scratch/logs/agent1.log" 2>/dev/null && break
    kill -0 $demo 2>/dev/null || break
    sleep 0.1
  done
  pkill -9 -f "volume-agent -host-id" 2>/dev/null
  # And then end the demonstration rather than waiting for it to time out. It is going
  # to fail — its Agent is gone — and what is being read is not its verdict but what the
  # bucket holds, which is already whatever it is going to be.
  sleep 2
  kill -TERM -"$demo" 2>/dev/null
  wait $demo 2>/dev/null
  # The demonstration is *expected* to fail: its Agent is gone. What is being read is
  # what the bucket holds — a HEAD that names a commit whose manifest and layer are both
  # there, whatever moment the process died at.
  local vol store
  store=$scratch/store
  vol=$(ls "$store/volumes" 2>/dev/null | head -1)
  if [ -z "$vol" ]; then
    tail -20 "$log"; capture "kill-mid-publish-no-volume" "$log" "$scratch"; rm -rf "$scratch"; rm -f "$log"; return 1
  fi
  if ! python3 "$ROOT/hack/soak-check-bucket.py" "$store" "$vol" >>"$log" 2>&1; then
    tail -20 "$log"; capture "kill-mid-publish-broken-chain" "$log" "$scratch"; rm -rf "$scratch"; rm -f "$log"; return 1
  fi
  tail -1 "$log"
  rm -rf "$scratch"; rm -f "$log"
  return 0
}

round=0
trap 'say "stopping after $round rounds, $fail_count findings"; exit 0' INT TERM
while :; do
  round=$((round + 1))
  [ "$SOAK_ROUNDS" -gt 0 ] && [ "$round" -gt "$SOAK_ROUNDS" ] && break
  [ "$SOAK_DEADLINE" -gt 0 ] && [ "$(date +%s)" -ge "$SOAK_DEADLINE" ] && break
  say "=== round $round (findings so far: $fail_count) ==="

  run "dst-${SOAK_SEEDS}-seeds" go test -race -count=1 ./internal/dst/
  run "unit-race" go test -race -count=2 ./internal/... ./cmd/...

  # Parameters moved every round. Rotation threshold and churn decide how often the
  # publish path runs at all; the reconcile interval decides how much a guest writes
  # between two looks, which is the one number §11 says nothing can bound.
  #
  # The threshold is drawn *below* the churn, and the two were drawn independently until
  # a round picked ROTATE_AT=11 MiB against CHURN=10 MiB and both rotation demos timed out
  # after 300 s. That is not a defect and the run reported it as one: `spin.churn=<MiB>`
  # rewrites one region in a loop, so within a layer each of its clusters is allocated
  # once and the tip stops growing at roughly the churn size. Measured on that very run —
  # the finding kept the scratch — the one layer ended at 11,337,728 bytes against a
  # threshold of 11,534,336: short by 192 KiB, for ever. The demos then wait for three
  # rotations that are physically impossible.
  #
  # An unattended runner that reports its own parameter choice as a finding is worse than
  # one that reports nothing: it teaches whoever reads it to skip the findings directory,
  # which is where the real ones will be. Same reason ADR-0025 refused to fail the gate
  # for a missing artefact — a red that means "you held it wrong" trains people past red.
  export CHURN=$(( RANDOM % 48 + 8 ))
  export ROTATE_AT=$(( (RANDOM % (CHURN - 4) + 1) * 1048576 ))
  export HEARTBEAT="$(( RANDOM % 700 + 200 ))ms"
  say "round $round parameters: ROTATE_AT=$ROTATE_AT CHURN=$CHURN HEARTBEAT=$HEARTBEAT"

  for stage in stage1 stage2 stage3; do
    scratch=$(mktemp -d "/tmp/soak-$stage-XXXXXX")
    log=$(mktemp)
    say "demo $stage"
    if DEMO_DIR=$scratch bash "hack/$stage-demo.sh" >"$log" 2>&1; then
      tail -1 "$log"; rm -rf "$scratch"
    else
      tail -25 "$log"; capture "demo-$stage" "$log" "$scratch"; rm -rf "$scratch"
    fi
    rm -f "$log"
  done

  kill_mid_publish
done
say "done: $round rounds, $fail_count findings in $FINDINGS"
