#!/usr/bin/env bash
# Stage 6, end to end: a clone reads what its parent wrote.
#
#   1. a volume is provisioned and a guest writes a pattern to it, then holds;
#   2. an operator asks for a snapshot; the Agent seals, publishes, and names the commit;
#   3. the parent's guest is stopped;
#   4. an operator clones that snapshot with the real control-plane binary;
#   5. the Agent prepares the clone's chain — the parent's layers, fetched from the bucket
#      and opened with the parent's key binding, with a fresh tip on top;
#   6. a second guest boots the clone and reads back the pattern the FIRST guest wrote.
#
# Step 6 is the whole demonstration and nothing short of it will do. Until this ran, a
# clone was a volume the Control Plane created, the Agent served as an empty chain, and a
# guest booted blank — the copy advertised and not delivered, with no error anywhere. That
# is DEV-0007's shape, reintroduced by the v6 pivot rather than by a bug.
set -euo pipefail

DEMO_NAME=stage6
DEMO_DONE="the whole of Stage 6 ran: a clone was built from its parent's commit and its guest read the parent's bytes"
ROTATE_AT=${ROTATE_AT:-4194304}
HEARTBEAT=${HEARTBEAT:-300ms}
AGENT_FLAGS_TEMPLATE="-rotate-at-bytes $ROTATE_AT -retry-backoff $HEARTBEAT -object-store-dir @DIR@/store"
# shellcheck source=hack/demo-lib.sh
source "$(dirname "$0")/demo-lib.sh"

psql() { docker exec "$PGC" psql -U cp -d "$DB" -tAqc "$1"; }
CPFLAGS=(-database-url "$DSN" -object-store-dir "$DIR/store" -kek-file "$DIR/kek")

say "2. a volume, and a guest that writes a pattern to it"
"$CP" "${CPFLAGS[@]}" -holder-id cp-seed -seed-volume -seed-host "$HOST_ID" -seed-size "$SIZE" \
  >"$DIR/logs/seed.log" 2>&1
waitfor "$DIR/logs/agent1.log" "volume ready"
POINTER=$(grep -m1 -o 'pointer=[^ ]*' "$DIR/logs/agent1.log" | head -1 | cut -d= -f2)
SOCK=$(grep -m1 -o 'qmp_socket=[^ ]*' "$DIR/logs/agent1.log" | head -1 | cut -d= -f2)
PARENT=$(grep -m1 -o 'volume_id=[^ ]*' "$DIR/logs/agent1.log" | head -1 | cut -d= -f2)
echo "parent volume $PARENT"

mkfifo "$DIR/ctl"
exec 9<>"$DIR/ctl"
"$QEMU" -machine "q35,accel=$ACCEL" -m 512 -smp 1 -display none -monitor none -no-reboot \
  -L "$OUT/share/spin-stack/qemu" \
  -kernel "$KERNEL" -initrd "$INITRAMFS" \
  -append "console=ttyS0 panic=1 spin.mode=hold spin.churn=8" \
  -drive "file=$(cat "$POINTER"),format=qcow2,if=virtio,cache=writeback" \
  -qmp "unix:$SOCK,server=on,wait=off" \
  -serial stdio <"$DIR/ctl" >"$DIR/logs/guest1.log" 2>&1 &
PIDS+=($!)
GUEST1=$!
waitfor "$DIR/logs/guest1.log" "GUESTINIT-HELD" 120
echo "the parent's guest wrote its pattern and fsynced it"

say "3. a snapshot of the parent, taken under the running guest"
"$CP" "${CPFLAGS[@]}" -holder-id cp-snap -snapshot-volume "$PARENT" >"$DIR/logs/snapshot.log" 2>&1
SNAP=$(grep -m1 -o 'snapshot_id=[^ ]*' "$DIR/logs/snapshot.log" | head -1 | cut -d= -f2)
test -n "$SNAP" || die "no snapshot was recorded: $(cat "$DIR/logs/snapshot.log")"
waitfor "$DIR/logs/agent1.log" "snapshot taken" 90
for _ in $(seq 60); do
  COMMIT=$(psql "SELECT COALESCE(commit_id::text,'') FROM snapshots WHERE snapshot_id = '$SNAP'")
  [ -n "$COMMIT" ] && break
  sleep 0.5
done
test -n "$COMMIT" || die "the snapshot never named a commit"
echo "snapshot $SNAP names commit $COMMIT"

say "4. the parent's guest stops"
echo "GUESTCTL-STOP" >&9
wait "$GUEST1" 2>/dev/null || true
echo "the parent is quiet; everything it wrote is in the bucket"

say "5. the clone, created with the real binary"
"$CP" "${CPFLAGS[@]}" -holder-id cp-clone -clone-snapshot "$SNAP" >"$DIR/logs/clone.log" 2>&1
CLONE=$(grep -m1 -o 'volume_id=[^ ]*' "$DIR/logs/clone.log" | head -1 | cut -d= -f2)
test -n "$CLONE" || die "no clone was created: $(cat "$DIR/logs/clone.log")"
echo "clone $CLONE, from snapshot $SNAP"
# The catalog records the lineage, and the Agent is *told* it — it cannot look a parent up.
test "$(psql "SELECT parent_snapshot_id FROM volumes WHERE volume_id = '$CLONE'")" = "$SNAP" ||
  die "the clone's row does not name its parent snapshot"

say "6. the Agent builds the clone's chain from the parent's commit"
waituntil "$DIR/logs/agent1.log" "volume ready" 2 120
CPOINTER=$(grep "volume_id=$CLONE" "$DIR/logs/agent1.log" | grep -m1 -o 'pointer=[^ ]*' | cut -d= -f2)
test -n "$CPOINTER" || die "the Agent never reported the clone as ready"
CIMAGE=$(cat "$CPOINTER")
echo "the clone's tip is $(basename "$CIMAGE")"
# Its backing chain must reach the parent's layers. A clone whose chain is one layer deep
# is the empty-chain failure with a different name.
DEPTH=$("$QEMU_IMG" info --output=json --backing-chain "$CIMAGE" |
  python3 -c 'import json,sys; print(len(json.load(sys.stdin)))')
test "$DEPTH" -ge 2 || die "the clone's chain is $DEPTH layer(s) deep: it was born empty, not cloned"
echo "    its chain is $DEPTH layers deep, so the parent's data is under it"

say "7. a second guest boots the clone and reads the FIRST guest's pattern"
CSOCK=$(grep "volume_id=$CLONE" "$DIR/logs/agent1.log" | grep -m1 -o 'qmp_socket=[^ ]*' | cut -d= -f2)
"$QEMU" -machine "q35,accel=$ACCEL" -m 512 -smp 1 -display none -monitor none -no-reboot \
  -L "$OUT/share/spin-stack/qemu" \
  -kernel "$KERNEL" -initrd "$INITRAMFS" \
  -append "console=ttyS0 panic=1 spin.mode=verify" \
  -drive "file=$CIMAGE,format=qcow2,if=virtio,cache=writeback" \
  -qmp "unix:$CSOCK,server=on,wait=off" \
  -serial stdio </dev/null >"$DIR/logs/guest2.log" 2>&1 &
PIDS+=($!)
waitfor "$DIR/logs/guest2.log" "GUESTINIT-" 120
grep -q "GUESTINIT-PASS" "$DIR/logs/guest2.log" ||
  die "the clone's guest did not read its parent's pattern: $(grep -m1 'GUESTINIT' "$DIR/logs/guest2.log")"
echo "GUESTINIT-PASS — the clone read back what a different volume's guest wrote"
