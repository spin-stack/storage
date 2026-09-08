#!/usr/bin/env bash
# Stage 2, end to end: a volume's tip is sealed and replaced **while a Linux guest is
# writing to it**, and the guest neither notices nor loses anything.
#
# What it demonstrates, in order:
#
# This is also the lane that launches the guest the *modern* way — `-blockdev` with named
# nodes and an explicit virtio-blk-pci — rather than `-drive ...,if=virtio`. The two shapes
# leave the Agent different things to name the disk with: a generated drive id and an
# anonymous node, or a real node and no drive id at all. Rotation refused the second until
# it was taught to look, which is the shape any libvirt-derived runner produces. Stages 1,
# 3 and 4 keep `-drive`, so both are proven by something that actually boots.
#
#   1. a volume is provisioned and a guest boots off its first layer;
#   2. the guest writes continuously (spin.churn) while the Agent watches the tip grow;
#   3. every time the tip crosses -rotate-at-bytes the Agent creates a new layer over
#      it, moves `active/current`, and tells QEMU to switch — with the VM writing
#      throughout. Each rotation prints the pause the guest actually saw;
#   4. the chain is several layers deep and `qemu-img check` says it is sound;
#   5. the guest is told to stop, powers itself off, and a second boot — which writes
#      nothing — reads back through the whole chain what the first one fsynced.
#
# The last step is the one that matters. Everything before it could be true of a system
# that rotated correctly and lost the guest's writes; a verify boot that finds the
# pattern is the chain having been rebuilt from layers that were sealed under load.
#
# Nothing is published to an object store. The measured pauses printed at the end are
# v6 §23.2's exit criterion — they are what fixes §11's defaults.
set -euo pipefail

DEMO_NAME=stage2
DEMO_DONE="the whole of Stage 2 ran: a guest wrote through several rotations and read every byte back"
# The tip is sealed once it occupies this much. Small, because the point is to make the
# trigger fire several times inside one demonstration, not to pick a production value —
# picking that is what the numbers this prints are for.
ROTATE_AT=${ROTATE_AT:-4194304}
# The size of the region the guest rewrites, in MiB. It writes until told to stop, so
# this is not how much it writes — it is how much of the device the writing touches.
CHURN=${CHURN:-32}
# How many rotations to wait for before stopping the guest. Two is the smallest number
# that shows a layer being sealed *over another sealed layer*, which is where a chain
# stops being a special case of one file.
ROTATIONS_WANTED=${ROTATIONS_WANTED:-3}
# Faster than Stage 1's: the trigger is evaluated once per reconcile cycle, so the cycle
# is the ceiling on how many rotations a churn of this size can produce. The backoff goes
# with it — the Agent refuses to start with one longer than its own interval.
HEARTBEAT=${HEARTBEAT:-300ms}
AGENT_FLAGS="-rotate-at-bytes $ROTATE_AT -retry-backoff $HEARTBEAT"
# shellcheck source=hack/demo-lib.sh
source "$(dirname "$0")/demo-lib.sh"

say "2. a volume, and the first layer of its chain"
"$CP" -database-url "$DSN" -object-store-dir "$DIR/store" -kek-file "$DIR/kek" \
  -holder-id cp-seed -seed-volume -seed-host "$HOST_ID" -seed-size "$SIZE" >"$DIR/logs/seed.log" 2>&1
waitfor "$DIR/logs/agent1.log" "volume ready"
POINTER=$(grep -m1 -o 'pointer=[^ ]*' "$DIR/logs/agent1.log" | head -1 | cut -d= -f2)
SOCK=$(grep -m1 -o 'qmp_socket=[^ ]*' "$DIR/logs/agent1.log" | head -1 | cut -d= -f2)
FIRST=$(cat "$POINTER")
echo "the chain starts at one layer: $FIRST"

say "3. a Linux guest boots off it and starts writing, and does not stop"
mkfifo "$DIR/ctl"
# Read-write, so opening does not block on a QEMU that has not started yet and the
# guest's console does not see EOF the moment the stop word has been sent.
exec 9<>"$DIR/ctl"
"$QEMU" -machine "q35,accel=$ACCEL" -m 512 -smp 1 -display none -vga none -monitor none -no-reboot \
  -net none \
  -L "$OUT/qemu" \
  -kernel "$KERNEL" -initrd "$INITRAMFS" \
  -append "console=ttyS0 panic=1 spin.mode=hold spin.churn=$CHURN" \
  -blockdev "driver=file,filename=$FIRST,node-name=vol-file,discard=unmap" \
  -blockdev "driver=qcow2,node-name=vol,file=vol-file,discard=unmap" \
  -device virtio-blk-pci,drive=vol,id=virtio-disk0,disable-legacy=on \
  -qmp "unix:$SOCK,server=on,wait=off" \
  -serial stdio <"$DIR/ctl" >"$DIR/logs/guest1.log" 2>&1 &
PIDS+=($!)
GUEST1=$!
waitfor "$DIR/logs/guest1.log" "GUESTINIT-HELD" 120
echo "the guest wrote its pattern, fsync returned, and it is now churning"

say "4. the Agent seals the tip under the running guest, again and again"
# Waiting on the rotations and not on the writing. The guest keeps going until it is
# told to stop, so this is the demonstration asserting on the event it is about rather
# than on a guess about how much data causes it.
waituntil "$DIR/logs/agent1.log" "rotated:" "$ROTATIONS_WANTED" 300
grep "rotated:" "$DIR/logs/agent1.log" | sed 's/^/    /'
ROTATIONS=$(grep -c "rotated:" "$DIR/logs/agent1.log" || true)
grep -q "could not rotate" "$DIR/logs/agent1.log" &&
  die "a rotation failed under the running guest"
# The guest is still the same VM, still holding the disk, and was never refused.
grep -q "refusing a volume" "$DIR/logs/agent1.log" &&
  die "the volume was refused while it was being rotated"
echo "$ROTATIONS rotations, and the guest never stopped"

say "5. the chain QEMU is writing to now"
TIP=$(cat "$POINTER")
[ "$TIP" != "$FIRST" ] || die "active/current still names the layer the guest booted from"
# Asked of QEMU rather than of the filesystem: the tip is open and no offline tool may
# touch it (v6 §5). This is the same question the Agent asks every cycle.
python3 - "$SOCK" "$TIP" <<'PY'
import json, socket, sys
sock, tip = sys.argv[1], sys.argv[2]
s = socket.socket(socket.AF_UNIX); s.connect(sock); f = s.makefile("rw")
def cmd(c):
    f.write(json.dumps({"execute": c}) + "\n"); f.flush()
    while True:
        r = json.loads(f.readline())
        if "event" not in r: return r
f.readline(); cmd("qmp_capabilities")
# The entry with a medium in it: with no -nodefaults there is a CD-ROM tray first, and
# it has no `inserted` at all.
disks = [b for b in cmd("query-block")["return"] if b.get("inserted")]
got = disks[0]["inserted"]["file"]
print(f"    QEMU says it is writing to {got}")
if got != tip:
    sys.exit(f"QEMU is writing to {got} and active/current names {tip}")
PY
LAYERS=$(ls "$(dirname "$TIP")" | wc -l)
echo "    $LAYERS layers on disk"

say "6. the guest is told to stop, and powers itself off"
echo "GUESTCTL-STOP" >&9
waitfor "$DIR/logs/guest1.log" "GUESTINIT-CHURNED" 120
echo "    the guest wrote $(grep -m1 -o 'GUESTINIT-CHURNED [0-9]*' "$DIR/logs/guest1.log" | awk '{print $2}') MiB while that happened"
waitfor "$DIR/logs/guest1.log" "GUESTINIT-PASS" 120
wait "$GUEST1" 2>/dev/null || true
grep -m1 "GUESTINIT-PASS" "$DIR/logs/guest1.log"

say "7. the chain, offline, now that nothing holds it"
"$QEMU_IMG" info --backing-chain "$TIP" | grep -E "^image|^backing file:" | sed 's/^/    /'
# Every layer, not just the tip: a rotation that produced a sound tip over a damaged
# layer is exactly what a backing chain hides until somebody reads through it.
for layer in "$(dirname "$TIP")"/*.qcow2; do
  "$QEMU_IMG" check "$layer" >"$DIR/logs/check.log" 2>&1 ||
    { cat "$DIR/logs/check.log" >&2; die "qemu-img check failed on $layer"; }
done
echo "    qemu-img check: all $LAYERS layers sound"

say "8. a second boot, reading only, through the whole chain"
"$QEMU" -machine "q35,accel=$ACCEL" -m 512 -smp 1 -display none -vga none -monitor none -no-reboot \
  -net none \
  -L "$OUT/qemu" \
  -kernel "$KERNEL" -initrd "$INITRAMFS" \
  -append "console=ttyS0 panic=1 spin.mode=verify" \
  -drive "file=$TIP,format=qcow2,if=virtio,cache=writeback" \
  -serial stdio </dev/null >"$DIR/logs/guest2.log" 2>&1
grep -q "GUESTINIT-PASS" "$DIR/logs/guest2.log" ||
  { tail -20 "$DIR/logs/guest2.log" >&2; die "the bytes did not survive being rotated over"; }
grep -m1 "GUESTINIT-PASS" "$DIR/logs/guest2.log"
echo "a boot that wrote nothing read back everything the first boot fsynced, through $LAYERS layers"

say "the numbers this stage exists to produce (v6 §23.2)"
grep -o "pause_ms=[0-9.]*" "$DIR/logs/agent1.log" | cut -d= -f2 |
  python3 -c '
import sys
v = sorted(float(x) for x in sys.stdin)
if not v: sys.exit("no rotation printed a pause")
print(f"    external-snapshot pause over {len(v)} rotations: min {min(v):.2f}ms  median {v[len(v)//2]:.2f}ms  max {max(v):.2f}ms")'
grep -o "sealed_bytes=[0-9]*" "$DIR/logs/agent1.log" | cut -d= -f2 |
  python3 -c '
import sys
v = [int(x) for x in sys.stdin]
print(f"    sealed layer size: min {min(v)/2**20:.1f}MiB  max {max(v)/2**20:.1f}MiB  (trigger was {'"$ROTATE_AT"'/2**20:.1f}MiB)")'
