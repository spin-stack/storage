#!/usr/bin/env bash
# The scaffolding every stage demonstration stands on, and nothing a stage is about.
#
# It brings up what the demonstrations have in common — a database, a key, a Control
# Plane and a Volume Agent as real processes, a registered host — and gives them the
# three helpers that make a shell script assert on another process: `say`, `die` and
# `waitfor`. Each stage script sources this and then does only its own steps.
#
# A caller sets, before sourcing:
#   DEMO_NAME    a short slug; names the database and the scratch directory
#   DEMO_DONE    the closing line, printed only when the whole script succeeded
#   AGENT_FLAGS  extra flags for the Volume Agent (optional)
#   HEARTBEAT    the Agent's reconcile interval (optional, default 1s)
#
# and gets back: DIR, DSN, PORT, HOST_ID, SIZE, the binary paths, start_agent, and a
# running stack with the host registered.
: "${DEMO_NAME:?the sourcing script must set DEMO_NAME}"
: "${DEMO_DONE:=the demonstration ran}"
HEARTBEAT=${HEARTBEAT:-1s}

set -euo pipefail

cd "$(dirname "$0")/.."
ROOT=$PWD
OUT=$ROOT/_output

# The pinned artefacts. Every one of them has a task that produces it, named in the
# error, because "missing file" is not something a reader can act on.
# Two binaries, and which one runs is the same decision as which accelerator: the
# production build has no TCG compiled in at all, so it is not a binary a machine without
# KVM can run slowly — it is one that will not start. See ACCEL below.
# Every demo passes `-vga none` as well as `-display none`. The second only says not to
# open a window; the device is still created, and this machine's QEMU ships no
# vgabios-stdvga.bin for it — it has no display adapter, on purpose. Without the flag a
# guest dies at start-up with `failed to find romfile "vgabios-stdvga.bin"`, which reads
# like a missing firmware file rather than a device nobody wanted.
QEMU=$OUT/bin/qemu-system-x86_64
QEMU_TCG=$OUT/bin/qemu-system-x86_64-tcg
QEMU_IMG=$OUT/bin/qemu-img
KERNEL=$OUT/guest/vmlinux
INITRAMFS=$OUT/guest/initramfs.cpio.gz
CP=$OUT/bin/control-plane
AGENT=$OUT/bin/volume-agent

# The development Postgres `task db:dev:up` starts. A database of its own per run, so
# two runs cannot see each other's fleet.
PGC=${PGC:-spin-storage-devdb}
DB=${DB:-${DEMO_NAME}_$$}

DIR=${DEMO_DIR:-$(mktemp -d /tmp/${DEMO_NAME}-XXXXXX)}
SIZE=${SIZE:-268435456}
# Emulation is allowed here and nowhere else. What a demo demonstrates is a Linux guest
# reaching a qcow2 through virtio, and that is true at either speed; refusing to run
# without KVM would make the one command a human runs unrunnable on a developer outside
# the `kvm` group and on every hosted CI runner. A host serving tenants is the opposite
# case, which is why the binary it runs has no TCG in it at all (Dockerfile.qemu).
#
# Chosen here rather than left to QEMU's `kvm:tcg` fallback list, which would pick the
# same thing and say nothing. The silence is the problem: a machine that should have KVM
# and does not — a developer outside the group, a runner that lost nested virt — runs
# emulated at a tenth of the speed, and every timing the run prints is a measurement of
# something else. Several numbers in this repository's comments came out of these demos.
if [ -z "${ACCEL:-}" ]; then
  if [ -r /dev/kvm ] && [ -w /dev/kvm ]; then
    ACCEL=kvm
  else
    ACCEL=tcg
  fi
fi
# And the binary follows the accelerator, because only one of the two carries TCG.
if [ "$ACCEL" = tcg ]; then
  QEMU=$QEMU_TCG
fi

say() { printf '\n=== %s\n' "$*"; }
die() { printf '\nFAILED: %s\n' "$*" >&2; exit 1; }

need() { test -x "$1" || test -f "$1" || die "missing $1 — run: $2"; }
need "$QEMU"      "task machine"
need "$QEMU_IMG"  "task machine"
need "$KERNEL"    "task machine"
need "$INITRAMFS" "task guest:build"
need "$CP"        "task build:cmd"
need "$AGENT"     "task build:cmd"
docker exec "$PGC" true 2>/dev/null || die "no Postgres container named $PGC — run: task db:dev:up"

# --- cleanup ------------------------------------------------------------------------
#
# Everything this run started, killed in the reverse order, and the database dropped.
# A demonstration that leaves a Control Plane bound to a port is a demonstration that
# cannot be run twice.
PIDS=()
cleanup() {
  local status=$?
  for pid in "${PIDS[@]:-}"; do
    [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  docker exec "$PGC" psql -U cp -d postgres -qc "DROP DATABASE IF EXISTS $DB (FORCE)" >/dev/null 2>&1 || true
  if [ "$status" -eq 0 ]; then
    printf '\n=== %s\n' "$DEMO_DONE"
    printf '    scratch: %s\n' "$DIR"
  else
    printf '\n    scratch kept for inspection: %s\n' "$DIR" >&2
  fi
  return $status
}
# INT and TERM as well as EXIT, so a demonstration that is stopped from outside still
# takes its processes and its database with it. A soak kills these on purpose, and one
# that leaked a Control Plane and a Postgres database per round would run out of both.
trap cleanup EXIT INT TERM

# waituntil greps a growing file until the pattern appears at least n times. It is what
# a demonstration waits on when the thing it is about is a repeated event rather than a
# single line: waiting on "the guest finished writing" and then counting is a race that
# resolves in favour of passing on a fast disk.
waituntil() {
  local file=$1 want=$2 n=$3 secs=${4:-60} i=0
  while [ $i -lt $((secs * 10)) ]; do
    [ "$(grep -cF -- "$want" "$file" 2>/dev/null || true)" -ge "$n" ] && return 0
    sleep 0.1
    i=$((i + 1))
  done
  echo "--- last 40 lines of $file ---" >&2
  tail -40 "$file" >&2
  die "timed out after ${secs}s waiting for $n x '$want' in $file"
}

# waitfor greps a growing file until the line appears, or gives up. Polling a file
# another *process* is writing is the only alternative to guessing how long it takes.
waitfor() {
  local file=$1 want=$2 secs=${3:-60} i=0
  while [ $i -lt $((secs * 10)) ]; do
    grep -qF -- "$want" "$file" 2>/dev/null && return 0
    sleep 0.1
    i=$((i + 1))
  done
  echo "--- last 40 lines of $file ---" >&2
  tail -40 "$file" >&2
  die "timed out after ${secs}s waiting for '$want' in $file"
}

mkdir -p "$DIR"/{agent,logs}

say "0. a database and a key"
docker exec "$PGC" psql -U cp -d postgres -qc "CREATE DATABASE $DB" >/dev/null
docker exec -i "$PGC" psql -U cp -d "$DB" -q -v ON_ERROR_STOP=1 <internal/schema/schema.sql >/dev/null
head -c 32 /dev/urandom >"$DIR/kek"
DSN="postgres://cp:cp@127.0.0.1:${DEVDB_PORT:-55432}/$DB?sslmode=disable"
# A free port, asked of the kernel rather than hardcoded, so two runs can overlap.
PORT=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')
# INV-22: every id in this system is a UUIDv7, and the Agent refuses anything else on
# the flag rather than heartbeating into a row that will never match.
HOST_ID=$(python3 - <<'PY'
import os, time
ms = int(time.time() * 1000)
b = bytearray(os.urandom(16))
b[0:6] = ms.to_bytes(6, "big")
b[6] = (b[6] & 0x0F) | 0x70
b[8] = (b[8] & 0x3F) | 0x80
h = b.hex()
print(f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}")
PY
)
# uuidv7 mints another id, for a stage that needs a second host in the fleet. Same rule
# as HOST_ID above: INV-22, and the binaries refuse anything else on the flag.
uuidv7() {
  python3 -c "
import os, time
ms = int(time.time() * 1000)
b = bytearray(os.urandom(16))
b[0:6] = ms.to_bytes(6, 'big')
b[6] = (b[6] & 0x0F) | 0x70
b[8] = (b[8] & 0x3F) | 0x80
h = b.hex()
print(f'{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}')
"
}
echo "database=$DB  host_id=$HOST_ID  data_dir=$DIR/agent"

say "1. the two binaries, as processes"
"$CP" -database-url "$DSN" -object-store-dir "$DIR/store" -kek-file "$DIR/kek" \
  -listen "127.0.0.1:$PORT" -holder-id "cp-$DEMO_NAME" \
  -cordon-used-ratio 1 -uncordon-used-ratio 0.99 >"$DIR/logs/cp.log" 2>&1 &
PIDS+=($!)
waitfor "$DIR/logs/cp.log" "control-plane elected"

# AGENT_FLAGS_TEMPLATE lets a caller name a path under the scratch directory, which it
# cannot know before this file chose one. @DIR@ is the only substitution and it is done
# once, here.
if [ -n "${AGENT_FLAGS_TEMPLATE:-}" ]; then
  AGENT_FLAGS=${AGENT_FLAGS_TEMPLATE//@DIR@/$DIR}
fi

start_agent() {
  "$AGENT" -host-id "$HOST_ID" -control-plane "http://127.0.0.1:$PORT" \
    -data-dir "$DIR/agent" -kek-file "$DIR/kek" -qemu-img "$QEMU_IMG" \
    -heartbeat-interval "$HEARTBEAT" ${AGENT_FLAGS:-} >>"$1" 2>&1 &
  AGENT_PID=$!
  PIDS+=("$AGENT_PID")
}
start_agent "$DIR/logs/agent1.log"
waitfor "$DIR/logs/agent1.log" "key-encryption key loaded"
grep -m1 "volume-agent starting" "$DIR/logs/agent1.log"
# The Agent runs qemu-img once at start-up and refuses to start if it cannot. Printed
# here because "which qemu-img is this host handing its chains to" is the question an
# operator asks first when a chain comes out wrong.
grep -m1 "qemu-img is usable" "$DIR/logs/agent1.log"

# The hosts row is a foreign key, so nothing can be provisioned for a host the catalog
# has not been told about. The Agent tells it by heartbeating.
until docker exec "$PGC" psql -U cp -d "$DB" -tAc \
  "SELECT count(*) FROM hosts WHERE host_id='$HOST_ID'" | grep -q '^1$'; do sleep 0.2; done
echo "the host registered itself"
if [ "$ACCEL" = tcg ]; then
  echo "    NOTE: emulated (no writable /dev/kvm, or ACCEL=tcg was asked for). Every duration"
  echo "    this run prints is emulation, not a measurement of anything a tenant would see."
else
  echo "    accelerator: $ACCEL"
fi

