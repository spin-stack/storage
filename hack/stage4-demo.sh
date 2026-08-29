#!/usr/bin/env bash
# Stage 4, end to end: the host is destroyed and the volume comes back.
#
# This is the half of v6 §26's cycle that the first three stages could not show —
# RUN → COMMIT → **DESTROY HOST → RECOVER** → RUN — and it is the one that says whether
# any of the rest was worth doing. Everything before it demonstrates that bytes reach an
# object store. This demonstrates that the object store is enough.
#
#   1. a guest writes, the Agent seals and publishes; the guest stops cleanly;
#   2. what the bucket holds is read with `cat` and remembered;
#   3. **the host is destroyed**: the Agent is killed and its entire data directory is
#      deleted — every layer, the pointer, state.json, the lock. Nothing local survives;
#   4. an Agent starts under the same host id with an empty data directory, which is what
#      a rebuilt machine looks like to the catalog, and rebuilds the volume from the
#      bucket alone;
#   5. a guest boots on it and reads back what a guest on a host that no longer exists
#      wrote — through layers this machine downloaded and re-linked, not layers it wrote.
#
# What is lost is what was never published: the tip the guest was writing to when the host
# died. That is the promise, exactly — "se recupera hasta el último commit publicado" — and
# step 5 asserts it in the only way that means anything, by reading the bytes.
set -euo pipefail

DEMO_NAME=stage4
DEMO_DONE="the whole of Stage 4 ran: a host was destroyed and its volume came back from the object store alone"
ROTATE_AT=${ROTATE_AT:-4194304}
CHURN=${CHURN:-32}
COMMITS_WANTED=${COMMITS_WANTED:-2}
HEARTBEAT=${HEARTBEAT:-300ms}
AGENT_FLAGS_TEMPLATE="-rotate-at-bytes $ROTATE_AT -retry-backoff $HEARTBEAT -object-store-dir @DIR@/store"
# shellcheck source=hack/demo-lib.sh
source "$(dirname "$0")/demo-lib.sh"

unframe() { tail -n +2 "$1"; }

say "2. a volume, and a guest that writes to it"
"$CP" -database-url "$DSN" -object-store-dir "$DIR/store" -kek-file "$DIR/kek" \
  -holder-id cp-seed -seed-volume -seed-host "$HOST_ID" -seed-size "$SIZE" >"$DIR/logs/seed.log" 2>&1
waitfor "$DIR/logs/agent1.log" "volume ready"
POINTER=$(grep -m1 -o 'pointer=[^ ]*' "$DIR/logs/agent1.log" | head -1 | cut -d= -f2)
SOCK=$(grep -m1 -o 'qmp_socket=[^ ]*' "$DIR/logs/agent1.log" | head -1 | cut -d= -f2)
VOLUME=$(grep -m1 -o 'volume_id=[^ ]*' "$DIR/logs/agent1.log" | head -1 | cut -d= -f2)
FIRST=$(cat "$POINTER")

mkfifo "$DIR/ctl"
exec 9<>"$DIR/ctl"
"$QEMU" -machine "q35,accel=$ACCEL" -m 512 -smp 1 -display none -monitor none -no-reboot \
  -L "$OUT/share/spin-stack/qemu" \
  -kernel "$KERNEL" -initrd "$INITRAMFS" \
  -append "console=ttyS0 panic=1 spin.mode=hold spin.churn=$CHURN" \
  -drive "file=$FIRST,format=qcow2,if=virtio,cache=writeback" \
  -qmp "unix:$SOCK,server=on,wait=off" \
  -serial stdio <"$DIR/ctl" >"$DIR/logs/guest1.log" 2>&1 &
PIDS+=($!)
GUEST1=$!
waitfor "$DIR/logs/guest1.log" "GUESTINIT-HELD" 120
echo "the guest wrote its pattern and is churning"

say "3. the Agent publishes what it seals"
waituntil "$DIR/logs/agent1.log" "committed:" "$COMMITS_WANTED" 300
grep "committed:" "$DIR/logs/agent1.log" | sed 's/^/    /'
echo "GUESTCTL-STOP" >&9
waitfor "$DIR/logs/guest1.log" "GUESTINIT-PASS" 120
wait "$GUEST1" 2>/dev/null || true
echo "    the guest stopped cleanly"

say "4. what the object store holds, before the host is destroyed"
HEAD_OBJ="$DIR/store/volumes/$VOLUME/HEAD"
test -f "$HEAD_OBJ" || die "no HEAD: nothing was ever published, so there is nothing to recover"
COMMIT=$(unframe "$HEAD_OBJ" | python3 -c 'import json,sys; print(json.load(sys.stdin)["commit_id"])')
DEPTH=0
c=$COMMIT
while [ -n "$c" ]; do
  M="$DIR/store/volumes/$VOLUME/commits/$c.json"
  test -f "$M" || die "the chain from HEAD reaches $c and there is no manifest for it"
  c=$(unframe "$M" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("parent_commit_id",""))')
  DEPTH=$((DEPTH + 1))
done
echo "    HEAD names $COMMIT; the published history is $DEPTH commits deep"
LOCAL_BEFORE=$(ls "$DIR/agent/volumes/$VOLUME/layers" | wc -l)
echo "    this host holds $LOCAL_BEFORE local layers, and is about to hold none"

say "5. the host is destroyed"
# Not a graceful stop. The Agent is killed and everything it kept is deleted: the layers,
# the pointer, state.json, the data-directory lock. What is left is the catalog row and
# the bucket, which is exactly what survives a machine that does not come back.
kill -9 "$AGENT_PID" 2>/dev/null || true
wait "$AGENT_PID" 2>/dev/null || true
rm -rf "$DIR/agent"
test ! -d "$DIR/agent" || die "the data directory is still there"
echo "    the Agent is gone and $DIR/agent no longer exists"

say "6. the same machine, with the volume's HEAD also gone"
# The failure this step exists for is silent: the object store answering "no HEAD" means
# either "this volume is new" or "this volume's HEAD is gone", and a host with no local
# record of the volume cannot tell them apart. Guessing "new" creates a blank qcow2 and
# the guest boots an empty disk with no I/O error anywhere. The catalog is the only party
# that knows, and it is asked here with everything else destroyed — which is the state a
# lifecycle rule, a bad restore or a wrong bucket leaves.
mv "$DIR/store/volumes/$VOLUME/HEAD" "$DIR/head.saved"
mkdir -p "$DIR/agent"
start_agent "$DIR/logs/agent-blank.log"
waitfor "$DIR/logs/agent-blank.log" "refusing a volume" 300
grep -m1 "refusing a volume" "$DIR/logs/agent-blank.log" | sed 's/^/    /'
grep -q "will not create it empty over a history that exists" "$DIR/logs/agent-blank.log" ||
  die "the host refused the volume for some other reason than the missing HEAD"
test ! -e "$DIR/agent/volumes/$VOLUME/active/current" ||
  die "a blank chain was prepared for a volume whose history is in the catalog"
echo "    no chain was created: a guest would have booted this as an empty disk"
kill -9 "$AGENT_PID" 2>/dev/null || true
wait "$AGENT_PID" 2>/dev/null || true
rm -rf "$DIR/agent"
mv "$DIR/head.saved" "$DIR/store/volumes/$VOLUME/HEAD"

say "7. a rebuilt machine, same host id, empty disk"
# The same host id on purpose: to the catalog this is the machine coming back, which is
# what makes the volume still placed here. What it does not have is a single byte of it.
mkdir -p "$DIR/agent"
start_agent "$DIR/logs/agent2.log"
waitfor "$DIR/logs/agent2.log" "volume ready" 300
grep -m1 "volume ready" "$DIR/logs/agent2.log"
grep -q "refusing a volume" "$DIR/logs/agent2.log" &&
  { grep -m1 "refusing a volume" "$DIR/logs/agent2.log" >&2; die "the rebuilt host refused the volume instead of recovering it"; }

RECOVERED=$(cat "$DIR/agent/volumes/$VOLUME/active/current")
test -f "$RECOVERED" || die "the pointer names $RECOVERED and there is nothing there"
say "8. the chain this machine rebuilt out of the bucket"
"$QEMU_IMG" info --backing-chain "$RECOVERED" | grep -E "^image|^backing file:" | sed 's/^/    /'
# One layer per published commit, plus the new empty tip the guest will write to.
GOT=$("$QEMU_IMG" info --backing-chain "$RECOVERED" | grep -c "^image:")
[ "$GOT" -eq "$((DEPTH + 1))" ] ||
  die "the rebuilt chain is $GOT images deep and the published history is $DEPTH commits plus a new tip"
echo "    $GOT images: $DEPTH downloaded layers and one new tip"
for layer in "$DIR/agent/volumes/$VOLUME"/layers/*.qcow2; do
  "$QEMU_IMG" check "$layer" >"$DIR/logs/check.log" 2>&1 ||
    { cat "$DIR/logs/check.log" >&2; die "qemu-img check failed on the downloaded $layer"; }
done
echo "    qemu-img check: every downloaded layer is sound"

say "9. a guest reads back what a guest on a host that no longer exists wrote"
"$QEMU" -machine "q35,accel=$ACCEL" -m 512 -smp 1 -display none -monitor none -no-reboot \
  -L "$OUT/share/spin-stack/qemu" \
  -kernel "$KERNEL" -initrd "$INITRAMFS" \
  -append "console=ttyS0 panic=1 spin.mode=verify" \
  -drive "file=$RECOVERED,format=qcow2,if=virtio,cache=writeback" \
  -serial stdio </dev/null >"$DIR/logs/guest2.log" 2>&1
grep -q "GUESTINIT-PASS" "$DIR/logs/guest2.log" ||
  { tail -20 "$DIR/logs/guest2.log" >&2; die "the recovered volume does not hold what the first guest wrote"; }
grep -m1 "GUESTINIT-PASS" "$DIR/logs/guest2.log"
echo "    the bytes came from an object store, through a machine that had never seen them"

say "10. the disk comes back when the fleet moves the volume somewhere else"
# A released volume keeps its layers on purpose: it leaves the desired state for reasons
# that reverse, and getting it back should be free. What makes them reclaimable is not
# time passing but a fact — `volumes/<id>/epoch` naming a grant this host does not hold.
# From that moment this chain is a fork that can never be published, so the files are
# holding a disk for a history nothing will accept.
LAYERS="$DIR/agent/volumes/$VOLUME/layers"
BEFORE=$(ls "$LAYERS" | wc -l)
[ "$BEFORE" -gt 0 ] || die "there are no layers to reclaim"

# A second host, so there is somewhere to move the volume to. It registers itself by
# heartbeating — the fleet learns a host exists by being told — and is then stopped: this
# step is about the *first* host's disk, and nothing has to run for the epoch to move.
OTHER=$(uuidv7)
mkdir -p "$DIR/agent-b"
"$AGENT" -host-id "$OTHER" -control-plane "http://127.0.0.1:$PORT" \
  -data-dir "$DIR/agent-b" -kek-file "$DIR/kek" -qemu-img "$QEMU_IMG" \
  -heartbeat-interval "$HEARTBEAT" >"$DIR/logs/agent-b.log" 2>&1 &
OTHER_PID=$!
# Registered when the fleet says so, asked with the same command an operator would use:
# a host row exists because a host heartbeated it into the catalog, and nothing can be
# placed on a host the catalog does not know (volumes.primary_host_id is a foreign key).
for i in $(seq 1 100); do
  "$CP" -database-url "$DSN" -fleet-status >"$DIR/logs/fleet-b.txt" 2>&1 || true
  grep -qF "$OTHER" "$DIR/logs/fleet-b.txt" && break
  [ "$i" = 100 ] && die "the second host never registered: $(tail -8 "$DIR/logs/fleet-b.txt")"
  sleep 0.2
done
"$CP" -database-url "$DSN" -object-store-dir "$DIR/store" -holder-id cp-move \
  -detach-volume "$VOLUME" >"$DIR/logs/move.log" 2>&1 ||
  die "detach failed: $(tail -3 "$DIR/logs/move.log")"
"$CP" -database-url "$DSN" -object-store-dir "$DIR/store" -holder-id cp-move \
  -max-used-ratio 0.99 -attach-volume "$VOLUME" -attach-host "$OTHER" >>"$DIR/logs/move.log" 2>&1 ||
  die "attach to the second host failed: $(tail -3 "$DIR/logs/move.log")"
kill -9 "$OTHER_PID" 2>/dev/null || true
wait "$OTHER_PID" 2>/dev/null || true
echo "    volume $VOLUME now belongs to host $OTHER"

waitfor "$DIR/logs/agent2.log" "reclaimed the local disk" 120
grep -m1 "reclaimed the local disk" "$DIR/logs/agent2.log" | sed 's/^/    /'
AFTER=$(ls "$LAYERS" 2>/dev/null | wc -l)
[ "$AFTER" -eq 0 ] || die "$AFTER layers are still on the first host after the volume moved"
echo "    $BEFORE layers freed on the host that no longer holds it"
