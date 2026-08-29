#!/usr/bin/env bash
# Stage 6, end to end: a clone reads what its parent wrote.
#
#   1. a volume is provisioned and a guest writes a pattern to it, then holds;
#   2. an operator asks for a snapshot; the Agent seals, publishes, and names the commit;
#   3. the parent's guest is stopped;
#   4. an operator clones that snapshot with the real control-plane binary;
#   5. the Agent prepares the clone's chain — the parent's layers, fetched from the bucket
#      and opened with the parent's key binding, with a fresh tip on top;
#   6. a second guest boots the clone and reads back the pattern the FIRST guest wrote;
#   7. that clone is snapshotted in turn and cloned again, and a guest boots the clone of
#      the clone and reads back the pattern the FIRST guest wrote — two generations down.
#
# Step 6 is the whole demonstration of a clone and nothing short of it will do. Until it
# ran, a clone was a volume the Control Plane created, the Agent served as an empty chain,
# and a guest booted blank — the copy advertised and not delivered, with no error anywhere.
# That is DEV-0007's shape, reintroduced by the v6 pivot rather than by a bug.
#
# Step 7 is the same sentence about a lineage, and it is the only thing that demonstrates
# one: a chain that resolves is not a chain that carries the right bytes. Each generation's
# guest writes in a slot of its own (`spin.slot`), so the last guest verifying slot 0 is
# reading bytes only the ORIGINAL volume's guest ever wrote. A rebuild that stopped at the
# nearest ancestor hands it a chain with zeros there and reports success.
set -euo pipefail

DEMO_NAME=stage6
DEMO_DONE="the whole of Stage 6 ran: a clone read its parent's bytes, and a clone of that clone read the original volume's"
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
  -append "console=ttyS0 panic=1 spin.mode=hold spin.churn=8 spin.slot=0" \
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
# -max-used-ratio, because a clone is *placed* and every other stage seeds its volume onto
# a named host. ADR-0013's ceiling is 85% of the device holding --data-dir, and a hosted CI
# runner sits above that before this demo starts — so the default refuses the clone and the
# failure reads as "the clone is broken" when what happened is the policy working. This
# stage is about the chain a clone builds, not about admission; the ceiling has its own
# tests, and placement refusing a full host is one of them.
"$CP" "${CPFLAGS[@]}" -holder-id cp-clone -max-used-ratio ${CLONE_MAX_USED:-0.99} -clone-snapshot "$SNAP"   >"$DIR/logs/clone.log" 2>&1 ||
  die "the clone was refused: $(tail -3 "$DIR/logs/clone.log")"
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
# In the foreground and with no QMP socket, unlike the writing guests: a verify boot reads,
# reports and powers itself off, so waiting for the process is waiting for the answer — and
# the socket stays free for the guest in step 8, which needs the Agent to reach it.
CSOCK=$(grep "volume_id=$CLONE" "$DIR/logs/agent1.log" | grep -m1 -o 'qmp_socket=[^ ]*' | cut -d= -f2)
"$QEMU" -machine "q35,accel=$ACCEL" -m 512 -smp 1 -display none -monitor none -no-reboot \
  -L "$OUT/share/spin-stack/qemu" \
  -kernel "$KERNEL" -initrd "$INITRAMFS" \
  -append "console=ttyS0 panic=1 spin.mode=verify spin.slot=0" \
  -drive "file=$CIMAGE,format=qcow2,if=virtio,cache=writeback" \
  -serial stdio </dev/null >"$DIR/logs/guest2.log" 2>&1
grep -q "GUESTINIT-PASS" "$DIR/logs/guest2.log" ||
  die "the clone's guest did not read its parent's pattern: $(grep -m1 'GUESTINIT' "$DIR/logs/guest2.log")"
echo "GUESTINIT-PASS — the clone read back what a different volume's guest wrote"

say "8. a guest on the clone, writing a slot of its own so the clone has a commit to name"
# The clone must publish a commit before it can be snapshotted, and only a running QEMU
# can seal a tip — the Agent rotates through QMP. This guest writes slot 1, which is
# nowhere near slot 0: what the last guest of all reads back has to be attributable to the
# ORIGINAL volume's guest, and two guests writing identical bytes at one offset would make
# a chain missing a whole generation pass.
CIMAGE=$(cat "$CPOINTER")
mkfifo "$DIR/ctl2"
exec 8<>"$DIR/ctl2"
"$QEMU" -machine "q35,accel=$ACCEL" -m 512 -smp 1 -display none -monitor none -no-reboot \
  -L "$OUT/share/spin-stack/qemu" \
  -kernel "$KERNEL" -initrd "$INITRAMFS" \
  -append "console=ttyS0 panic=1 spin.mode=hold spin.slot=1" \
  -drive "file=$CIMAGE,format=qcow2,if=virtio,cache=writeback" \
  -qmp "unix:$CSOCK,server=on,wait=off" \
  -serial stdio <"$DIR/ctl2" >"$DIR/logs/guest3.log" 2>&1 &
PIDS+=($!)
GUEST3=$!
waitfor "$DIR/logs/guest3.log" "GUESTINIT-HELD" 120
echo "the clone's guest wrote slot 1 and fsynced it; slot 0 is still only in the parent's layers"

say "9. a snapshot of the clone, and the clone's guest stops"
"$CP" "${CPFLAGS[@]}" -holder-id cp-snap2 -snapshot-volume "$CLONE" >"$DIR/logs/snapshot2.log" 2>&1
SNAP2=$(grep -m1 -o 'snapshot_id=[^ ]*' "$DIR/logs/snapshot2.log" | head -1 | cut -d= -f2)
test -n "$SNAP2" || die "no snapshot of the clone was recorded: $(cat "$DIR/logs/snapshot2.log")"
waituntil "$DIR/logs/agent1.log" "snapshot taken" 2 90
for _ in $(seq 60); do
  COMMIT2=$(psql "SELECT COALESCE(commit_id::text,'') FROM snapshots WHERE snapshot_id = '$SNAP2'")
  [ -n "$COMMIT2" ] && break
  sleep 0.5
done
test -n "$COMMIT2" || die "the clone's snapshot never named a commit"
echo "snapshot $SNAP2 of the clone names commit $COMMIT2"
echo "GUESTCTL-STOP" >&8
wait "$GUEST3" 2>/dev/null || true

say "10. the clone of the clone"
"$CP" "${CPFLAGS[@]}" -holder-id cp-clone2 -max-used-ratio ${CLONE_MAX_USED:-0.99} -clone-snapshot "$SNAP2" \
  >"$DIR/logs/clone2.log" 2>&1 ||
  die "the clone of a clone was refused: $(tail -3 "$DIR/logs/clone2.log")"
GRAND=$(grep -m1 -o 'volume_id=[^ ]*' "$DIR/logs/clone2.log" | head -1 | cut -d= -f2)
test -n "$GRAND" || die "no clone of the clone was created: $(cat "$DIR/logs/clone2.log")"
test "$(psql "SELECT chain_depth FROM volumes WHERE volume_id = '$GRAND'")" = "2" ||
  die "the clone of the clone is not at depth 2 in the catalog"
echo "volume $GRAND, at depth 2, from snapshot $SNAP2"

say "11. the Agent builds a chain out of both generations"
waituntil "$DIR/logs/agent1.log" "volume ready" 3 180
GPOINTER=$(grep "volume_id=$GRAND" "$DIR/logs/agent1.log" | grep -m1 -o 'pointer=[^ ]*' | cut -d= -f2)
test -n "$GPOINTER" || die "the Agent never reported the clone of the clone as ready"
GIMAGE=$(cat "$GPOINTER")
GDEPTH=$("$QEMU_IMG" info --output=json --backing-chain "$GIMAGE" |
  python3 -c 'import json,sys; print(len(json.load(sys.stdin)))')
# One tip, at least one layer from the middle generation, at least one from the original.
test "$GDEPTH" -ge 3 || die "the grandchild's chain is $GDEPTH layer(s) deep: a generation is missing"
echo "    its chain is $GDEPTH layers deep"

say "12. a guest boots the clone of the clone and reads the ORIGINAL guest's slot"
"$QEMU" -machine "q35,accel=$ACCEL" -m 512 -smp 1 -display none -monitor none -no-reboot \
  -L "$OUT/share/spin-stack/qemu" \
  -kernel "$KERNEL" -initrd "$INITRAMFS" \
  -append "console=ttyS0 panic=1 spin.mode=verify spin.slot=0" \
  -drive "file=$GIMAGE,format=qcow2,if=virtio,cache=writeback" \
  -serial stdio </dev/null >"$DIR/logs/guest4.log" 2>&1
grep -q "GUESTINIT-PASS" "$DIR/logs/guest4.log" ||
  die "the grandchild's guest did not read the original volume's slot: $(grep -m1 'GUESTINIT' "$DIR/logs/guest4.log")"
echo "GUESTINIT-PASS — a clone of a clone read back what the first volume's guest wrote"

say "13. eight more clones of the same snapshot, and the disk does not move"
# The number that decided the local layout. A clone reuses its ancestors' layers, and for
# a long time "reuse" meant *copy*: layers lived under `volumes/<id>/layers/`, so every
# clone needed its own rebased file — `qemu-img rebase -u` rewrites a layer's header to
# name its parent — and a hundred clones of a volume with a 20 GiB published history cost
# 2 TB of local disk. The duplication was never about the bytes; the object store has
# always held one object per layer however many volumes descend from it.
#
# One directory for the whole host makes the backing path the same for everyone, so the
# header is written once and every clone shares the file. Counted in files and in bytes,
# because the bytes are what ran out.
LAYERS_DIR="$DIR/agent/layers"
FILES_BEFORE=$(ls "$LAYERS_DIR"/*.qcow2 | wc -l)
BYTES_BEFORE=$(du -sb "$LAYERS_DIR" | cut -f1)
for i in $(seq 1 8); do
  "$CP" "${CPFLAGS[@]}" -holder-id "cp-clone-$i" -max-used-ratio ${CLONE_MAX_USED:-0.99} \
    -clone-snapshot "$SNAP" >"$DIR/logs/clone-$i.log" 2>&1 ||
    die "clone $i was refused: $(tail -3 "$DIR/logs/clone-$i.log")"
done
waituntil "$DIR/logs/agent1.log" "volume ready" 11 300
FILES_AFTER=$(ls "$LAYERS_DIR"/*.qcow2 | wc -l)
BYTES_AFTER=$(du -sb "$LAYERS_DIR" | cut -f1)
# Eight clones, eight new tips — an empty qcow2 each — and not one copied ancestor. The
# check is on the *growth*: eight tips are the honest cost of eight volumes, and anything
# beyond that is a history duplicated.
GREW=$((FILES_AFTER - FILES_BEFORE))
[ "$GREW" -le 8 ] ||
  die "8 clones added $GREW layer files: their ancestors are being copied"
echo "    $FILES_BEFORE layer files before, $FILES_AFTER after: 8 new tips and no copied history"
echo "    $(numfmt --to=iec $BYTES_BEFORE) before, $(numfmt --to=iec $BYTES_AFTER) after"
