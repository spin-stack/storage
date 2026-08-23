#!/usr/bin/env bash
# Stage 1, end to end, with the real binaries and a real Linux guest.
#
# What it demonstrates, in order, and each step asserts on something outside the
# process — a line a binary printed, a file on disk, a verdict from inside a kernel:
#
#   1. a Control Plane and a Volume Agent find each other, and the host registers;
#   2. a volume is provisioned, and the Agent prepares its qcow2 chain — the file did
#      not exist before this and `qemu-img` says it is a qcow2 of the catalog's size;
#   3. a Linux guest boots off exactly that file, writes, fsyncs, and stays up;
#   4. while it is up, the Agent says over QMP that a VM has the image open — which is
#      how this system knows a volume is being used, since it does not launch the VM;
#   5. the Agent is SIGKILLed and restarted **under the running guest**, and comes back
#      serving the same volume without touching the image (an offline tool here would be
#      refused by QEMU's own write lock, and the volume with it);
#   6. the guest is told to stop and powers itself off;
#   7. a second guest boots off the same chain and finds the bytes — local persistence,
#      which is the whole of Stage 1's promise.
#
# Nothing is published to an object store. There is no commit, no HEAD and no manifest;
# those are the stages after this one.
#
# The two paths that are the contract with whoever launches the VM — the active image
# and the QMP socket — are read out of the Agent's own log rather than recomputed here,
# so that the demonstration fails if the Agent and this script ever disagree about them.
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT=$PWD
OUT=$ROOT/_output

# The pinned artefacts. Every one of them has a task that produces it, named in the
# error, because "missing file" is not something a reader can act on.
QEMU=$OUT/bin/qemu-system-x86_64
QEMU_IMG=$OUT/bin/qemu-img
KERNEL=$OUT/guest/vmlinux
INITRAMFS=$OUT/guest/initramfs.cpio.gz
CP=$OUT/bin/control-plane
AGENT=$OUT/bin/volume-agent

# The development Postgres `task db:dev:up` starts. A database of its own per run, so
# two runs cannot see each other's fleet.
PGC=${PGC:-spin-storage-devdb}
DB=${DB:-stage1_$$}

DIR=${DEMO_DIR:-$(mktemp -d /tmp/stage1-XXXXXX)}
SIZE=${SIZE:-268435456}
# `kvm:tcg` and not `kvm`: QEMU takes KVM where the caller can open /dev/kvm and falls
# back to emulation where it cannot, which is a developer not in the `kvm` group and every
# hosted CI runner. What Stage 1 demonstrates is a Linux guest reaching a qcow2 through
# virtio, and that is true at either speed; refusing to run without KVM would make the one
# command a human runs unrunnable on most of the machines that would run it.
ACCEL=${ACCEL:-kvm:tcg}

say() { printf '\n=== %s\n' "$*"; }
die() { printf '\nFAILED: %s\n' "$*" >&2; exit 1; }

need() { test -x "$1" || test -f "$1" || die "missing $1 — run: $2"; }
need "$QEMU"      "task build:qemu"
need "$QEMU_IMG"  "task build:qemu"
need "$KERNEL"    "task fetch:kernel"
need "$INITRAMFS" "task build:guest"
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
    printf '\n=== the whole of Stage 1 ran: created, attached, restarted under a running guest, detached, and read back\n'
    printf '    scratch: %s\n' "$DIR"
  else
    printf '\n    scratch kept for inspection: %s\n' "$DIR" >&2
  fi
  return $status
}
trap cleanup EXIT

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
echo "database=$DB  host_id=$HOST_ID  data_dir=$DIR/agent"

say "1. the two binaries, as processes"
"$CP" -database-url "$DSN" -object-store-dir "$DIR/store" -kek-file "$DIR/kek" \
  -listen "127.0.0.1:$PORT" -holder-id cp-stage1 \
  -cordon-used-ratio 1 -uncordon-used-ratio 0.99 >"$DIR/logs/cp.log" 2>&1 &
PIDS+=($!)
waitfor "$DIR/logs/cp.log" "control-plane elected"

start_agent() {
  "$AGENT" -host-id "$HOST_ID" -control-plane "http://127.0.0.1:$PORT" \
    -data-dir "$DIR/agent" -kek-file "$DIR/kek" -qemu-img "$QEMU_IMG" \
    -heartbeat-interval 1s >>"$1" 2>&1 &
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

say "2. a volume, and the chain the Agent prepares for it"
"$CP" -database-url "$DSN" -object-store-dir "$DIR/store" -kek-file "$DIR/kek" \
  -holder-id cp-seed -seed-volume -seed-host "$HOST_ID" -seed-size "$SIZE" >"$DIR/logs/seed.log" 2>&1
waitfor "$DIR/logs/agent1.log" "volume ready"
grep -m1 "volume ready" "$DIR/logs/agent1.log"

# Read out of the Agent's log, not recomputed: the two paths are an interface, and a
# demonstration that recomputed them could not see the Agent change one.
IMAGE=$(grep -m1 -o 'image=[^ ]*' "$DIR/logs/agent1.log" | head -1 | cut -d= -f2)
SOCK=$(grep -m1 -o 'qmp_socket=[^ ]*' "$DIR/logs/agent1.log" | head -1 | cut -d= -f2)
test -f "$IMAGE" || die "the Agent said the image is at $IMAGE and nothing is there"
"$QEMU_IMG" info "$IMAGE" | sed 's/^/    /'

say "3. a Linux guest boots off that file, writes and holds"
# The contract, and the whole of it: the active image as the disk, a QMP socket where
# the Agent looks. Nothing else here is the Agent's business.
mkfifo "$DIR/ctl"
# Opened read-write, and that is not a stylistic choice: opening a FIFO write-only
# blocks until a reader arrives, and the reader here is a QEMU this script has not
# started yet. Read-write never blocks, and it keeps a writer open so the guest's
# console does not see EOF the moment the stop word has been sent.
exec 9<>"$DIR/ctl"
"$QEMU" -machine "q35,accel=$ACCEL" -m 512 -smp 1 -display none -monitor none -no-reboot \
  -L "$OUT/share/spin-stack/qemu" \
  -kernel "$KERNEL" -initrd "$INITRAMFS" \
  -append "console=ttyS0 panic=1 spin.mode=hold" \
  -drive "file=$IMAGE,format=qcow2,if=virtio,cache=writeback" \
  -qmp "unix:$SOCK,server=on,wait=off" \
  -serial stdio <"$DIR/ctl" >"$DIR/logs/guest1.log" 2>&1 &
PIDS+=($!)
GUEST1=$!
waitfor "$DIR/logs/guest1.log" "GUESTINIT-HELD" 90
echo "the guest wrote its pattern and fsync returned; it is holding the disk open"

say "4. the Agent sees the VM over QMP"
waitfor "$DIR/logs/agent1.log" "a VM has attached to this volume" 30
grep -m1 "a VM has attached" "$DIR/logs/agent1.log"

say "5. the Agent is killed and restarted while the guest keeps the image open"
kill -9 "$AGENT_PID"
wait "$AGENT_PID" 2>/dev/null || true
start_agent "$DIR/logs/agent2.log"
waitfor "$DIR/logs/agent2.log" "volume ready" 30
grep -m1 "volume ready" "$DIR/logs/agent2.log"
# attached=true on the *first* line about this volume is the assertion: the new Agent
# asked QEMU before it asked the filesystem, so no offline tool was ever pointed at an
# image a guest is writing.
grep -q "volume ready.*attached=true" "$DIR/logs/agent2.log" ||
  die "the restarted Agent did not see the running guest; it would have run qemu-img over a live image"
grep -q "refusing a volume" "$DIR/logs/agent2.log" &&
  die "the restarted Agent refused the volume its own guest is using"
echo "the restarted Agent re-attached to the live chain and refused nothing"

say "6. the guest is told to stop, and powers itself off"
echo "GUESTCTL-STOP" >&9
waitfor "$DIR/logs/guest1.log" "GUESTINIT-PASS" 60
wait "$GUEST1" 2>/dev/null || true
grep -m1 "GUESTINIT-PASS" "$DIR/logs/guest1.log"

say "7. a second boot, reading only"
"$QEMU" -machine "q35,accel=$ACCEL" -m 512 -smp 1 -display none -monitor none -no-reboot \
  -L "$OUT/share/spin-stack/qemu" \
  -kernel "$KERNEL" -initrd "$INITRAMFS" \
  -append "console=ttyS0 panic=1 spin.mode=verify" \
  -drive "file=$IMAGE,format=qcow2,if=virtio,cache=writeback" \
  -serial stdio </dev/null >"$DIR/logs/guest2.log" 2>&1
grep -q "GUESTINIT-PASS" "$DIR/logs/guest2.log" ||
  { tail -20 "$DIR/logs/guest2.log" >&2; die "the second boot did not find what the first one wrote"; }
grep -m1 "GUESTINIT-PASS" "$DIR/logs/guest2.log"
echo "a boot that wrote nothing read back everything the first boot fsynced"
