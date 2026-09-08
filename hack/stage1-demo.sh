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
# so that the demonstration fails if the Agent and this script ever disagree about them.set -euo pipefail

DEMO_NAME=stage1
DEMO_DONE="the whole of Stage 1 ran: created, attached, restarted under a running guest, detached, and read back"
# shellcheck source=hack/demo-lib.sh
source "$(dirname "$0")/demo-lib.sh"

say "2. a volume, and the chain the Agent prepares for it"
"$CP" -database-url "$DSN" -object-store-dir "$DIR/store" -kek-file "$DIR/kek" \
  -holder-id cp-seed -seed-volume -seed-host "$HOST_ID" -seed-size "$SIZE" >"$DIR/logs/seed.log" 2>&1
waitfor "$DIR/logs/agent1.log" "volume ready"
grep -m1 "volume ready" "$DIR/logs/agent1.log"

# Read out of the Agent's log, not recomputed: the two paths are an interface, and a
# demonstration that recomputed them could not see the Agent change one.
POINTER=$(grep -m1 -o 'pointer=[^ ]*' "$DIR/logs/agent1.log" | head -1 | cut -d= -f2)
SOCK=$(grep -m1 -o 'qmp_socket=[^ ]*' "$DIR/logs/agent1.log" | head -1 | cut -d= -f2)
# And the image comes from the pointer, the way whoever launches the VM gets it. The
# Agent's log says the same thing, which is asserted rather than assumed.
IMAGE=$(cat "$POINTER")
LOGGED=$(grep -m1 -o 'image=[^ ]*' "$DIR/logs/agent1.log" | head -1 | cut -d= -f2)
test "$IMAGE" = "$LOGGED" || die "active/current names $IMAGE and the Agent logged $LOGGED"
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
"$QEMU" -machine "q35,accel=$ACCEL" -m 512 -smp 1 -display none -vga none -monitor none -no-reboot \
  -net none \
  -L "$OUT/qemu" \
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
"$QEMU" -machine "q35,accel=$ACCEL" -m 512 -smp 1 -display none -vga none -monitor none -no-reboot \
  -net none \
  -L "$OUT/qemu" \
  -kernel "$KERNEL" -initrd "$INITRAMFS" \
  -append "console=ttyS0 panic=1 spin.mode=verify" \
  -drive "file=$IMAGE,format=qcow2,if=virtio,cache=writeback" \
  -serial stdio </dev/null >"$DIR/logs/guest2.log" 2>&1
grep -q "GUESTINIT-PASS" "$DIR/logs/guest2.log" ||
  { tail -20 "$DIR/logs/guest2.log" >&2; die "the second boot did not find what the first one wrote"; }
grep -m1 "GUESTINIT-PASS" "$DIR/logs/guest2.log"
echo "a boot that wrote nothing read back everything the first boot fsynced"
