#!/usr/bin/env bash
# Start-up refusals the Volume Agent owes an operator, driven against the real binary
# (task agent:verify).
#
# One check lives here today, and it is here rather than in a unit test because the whole
# defect is in the seam between an operator's flag and the kernel: nothing that constructs
# a VolumeManager itself ever passes a directory long enough to matter.
#
#   -vhost-socket-dir is joined with "<volume-id>.sock", and a filesystem Unix socket path
#   is bounded by sockaddr_un's sun_path — 108 bytes including the NUL, so bind(2) takes
#   107. Over that, bind returns EINVAL, the Agent's reconciliation loop retries the cycle
#   every five seconds for ever, the Control Plane goes on showing the volume placed, and
#   nothing anywhere says "your path is too long". Measured before the fix: a 164-byte
#   directory produced `WARN reconciliation cycle failed error="... bind: invalid
#   argument" retry_in=5s` and no exit, for as long as it was left running.
#
# What is asserted is what the outside sees — the exit code, the process still being
# there, the bytes on stderr, the absence of any socket — never a field the code set.
#
# The limit itself is asserted against the *kernel* (step 0) rather than restated from a
# header, because a constant this script and cmd/volume-agent both merely believe is a
# constant that can be wrong in both places at once.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

# The arithmetic the Agent refuses by, restated independently here so that an edit to
# either side has to change both — and checked against a real bind() below.
MAX_UNIX_PATH=107  # sun_path is 108 bytes; one of them is the NUL
SOCKET_NAME_LEN=42 # "/" + a 36-character UUID + ".sock"
MAX_DIR=$((MAX_UNIX_PATH - SOCKET_NAME_LEN))

# A UUIDv7 (the version nibble is the 7 opening the third group). The Agent refuses
# anything else, and this script is not about that refusal.
HOST_ID=0198f0b0-1c2d-7000-8000-00000000000a
# Volume-id-shaped, and the same 36 characters long, so the probe binds exactly the path
# a volume would.
VOLUME_ID=0198f0b0-1c2d-7000-8000-00000000000b

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

tmp=$(mktemp -d /tmp/spin-agent-startup.XXXXXX)
trap 'rm -rf "$tmp"' EXIT

echo "==> building the real Agent"
bin=$tmp/volume-agent
(cd "$ROOT" && go build -o "$bin" ./cmd/volume-agent)

# dir_of_length <total> — an existing directory whose path is exactly <total> bytes.
dir_of_length() {
  local want=$1 d pad
  # Two statements, not one `local`: bash expands every word of a command before it
  # performs any of its assignments, so `local want=$1 pad=$((want-1))` reads an unset
  # `want` and dies under `set -u`.
  pad=$((want - ${#tmp} - 1))
  [ "$pad" -ge 1 ] || fail "this script's scratch directory ($tmp) is already ${#tmp} bytes; it cannot build a $want-byte path"
  d=$tmp/$(printf 'x%.0s' $(seq 1 "$pad"))
  [ ${#d} -eq "$want" ] || fail "built a ${#d}-byte path when $want was wanted"
  mkdir -p "$d"
  printf '%s' "$d"
}

# --- 0. the limit is the kernel's, not this file's ------------------------------------
#
# Binds the exact path the longest permitted socket directory would produce, and the one
# a single byte longer. If these two disagree with MAX_UNIX_PATH the constant in
# cmd/volume-agent is wrong in the same way, and every assertion below is measuring the
# wrong boundary.
echo "==> checking the kernel's Unix-socket path limit"
probe=$tmp/probe
cat >"$tmp/probe.go" <<'EOF'
// Binds one Unix socket and reports. The one thing in this lane that asks the kernel.
package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	ln, err := net.Listen("unix", os.Args[1])
	if err != nil {
		fmt.Printf("len=%d FAILED: %v\n", len(os.Args[1]), err)
		os.Exit(1)
	}
	_ = ln.Close()
	fmt.Printf("len=%d bound\n", len(os.Args[1]))
}
EOF
(cd "$ROOT" && go build -o "$probe" "$tmp/probe.go")

ok_sock=$(dir_of_length "$MAX_DIR")/$VOLUME_ID.sock
[ ${#ok_sock} -eq "$MAX_UNIX_PATH" ] || fail "a ${MAX_DIR}-byte directory yields a ${#ok_sock}-byte socket path, not $MAX_UNIX_PATH: SOCKET_NAME_LEN is wrong"
"$probe" "$ok_sock" || fail "the kernel refused a ${MAX_UNIX_PATH}-byte socket path that cmd/volume-agent permits: the limit is lower than $MAX_UNIX_PATH"
rm -f "$ok_sock"

too_long_sock=$(dir_of_length $((MAX_DIR + 1)))/$VOLUME_ID.sock
if "$probe" "$too_long_sock"; then
  fail "the kernel accepted a ${#too_long_sock}-byte socket path that cmd/volume-agent refuses: the limit is higher than $MAX_UNIX_PATH and the Agent is refusing directories that work"
fi

# run_agent <label> <socket-dir> <seconds> <awaited> — start the Agent and wait for it to
# either exit or print <awaited> ("exit" waits only for the exit). Writes $tmp/<label>.log;
# sets $exit_status, or "running" if it was still up when it printed the line or ran out
# of budget (in which case it is killed).
run_agent() {
  local label=$1 sockdir=$2 budget=$3 awaited=$4 data=$tmp/$1-data store=$tmp/$1-store log=$tmp/$1.log
  mkdir -p "$sockdir" "$data" "$store"
  "$bin" \
    -host-id "$HOST_ID" \
    -control-plane http://127.0.0.1:1 \
    -data-dir "$data" \
    -vhost-socket-dir "$sockdir" \
    -object-store-dir "$store" \
    >"$log" 2>&1 &
  local pid=$! i
  for ((i = 0; i < budget * 10; i++)); do
    # The process first: a run that exits after printing the line has still exited, and
    # reading the log first would report it as running.
    if ! kill -0 "$pid" 2>/dev/null; then
      wait "$pid" && exit_status=0 || exit_status=$?
      return 0
    fi
    if [ "$awaited" != exit ] && grep -q "$awaited" "$log" 2>/dev/null; then
      break
    fi
    sleep 0.1
  done
  exit_status=running
  kill -KILL "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

# refuses <label> <socket-dir> — the Agent must exit non-zero, promptly, having bound
# nothing, and must name the three numbers there is an action for.
refuses() {
  local label=$1 dir=$2 n
  echo "==> starting the Agent with a ${#dir}-byte -vhost-socket-dir"
  run_agent "$label" "$dir" 20 exit
  case $exit_status in
  running) fail "the Agent did not exit: a socket directory nothing can bind is being retried, not refused. Its log:
$(cat "$tmp/$label.log")" ;;
  0) fail "the Agent exited 0 with a socket directory nothing can bind. Its log:
$(cat "$tmp/$label.log")" ;;
  esac
  # What was given, what the kernel allows, and the longest directory that would work. A
  # refusal that says only "invalid argument" is the failure this check exists to end, so
  # the message is asserted and not just the exit status.
  for n in "${#dir}" "$MAX_UNIX_PATH" "$MAX_DIR"; do
    grep -q -- "$n" "$tmp/$label.log" ||
      fail "the refusal never names $n, so an operator cannot act on it. What it printed:
$(cat "$tmp/$label.log")"
  done
  [ "$(find "$dir" -name '*.sock' 2>/dev/null | wc -l)" -eq 0 ] ||
    fail "the Agent bound a socket under a directory it refused"
  echo "    refused, exit status $exit_status"
}

# starts <label> <socket-dir> — the Agent must come up. With no Control Plane reachable,
# the line it prints once every dependency is open is the last observable thing before it
# starts retrying, so that line is the assertion.
starts() {
  local label=$1 dir=$2
  echo "==> starting the Agent with a ${#dir}-byte -vhost-socket-dir"
  run_agent "$label" "$dir" 20 "volume-agent starting"
  [ "$exit_status" = running ] ||
    fail "the Agent exited (status $exit_status) with a ${#dir}-byte socket directory, which is inside the ${MAX_DIR}-byte limit. Its log:
$(cat "$tmp/$label.log")"
  grep -q "volume-agent starting" "$tmp/$label.log" ||
    fail "the Agent never reached start-up with a workable socket directory. Its log:
$(cat "$tmp/$label.log")"
  echo "    started"
}

# --- 1. the incident: a directory no volume's socket can ever fit in ------------------
#
# 164 bytes, the length actually hit walking the documented bring-up by hand.
refuses long "$(dir_of_length 164)"

# --- 2. the boundary, in both directions ----------------------------------------------
#
# One byte over the arithmetic must be refused and the arithmetic itself must be
# permitted. Without this pair the check above passes on a limit that is merely somewhere
# between 65 and 164 — including one so tight it refuses directories that work.
refuses boundary "$(dir_of_length $((MAX_DIR + 1)))"
starts atlimit "$(dir_of_length "$MAX_DIR")"

# --- 3. an ordinary directory still starts --------------------------------------------
starts short "$tmp/sock"

echo "OK: the Agent refuses a -vhost-socket-dir it can never bind (naming the byte counts) and starts on one it can"
