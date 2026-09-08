#!/usr/bin/env bash
# Stage 5, end to end: a snapshot is a name for a commit.
#
#   1. a volume is provisioned and a guest boots off its first layer;
#   2. the guest writes, and the Agent publishes what it seals;
#   3. an operator asks for a snapshot with the real control-plane binary, while the
#      guest is still running and writing;
#   4. the Agent seals the tip *because it was asked*, publishes it, and reports the
#      commit its history now ends at;
#   5. the catalog says PUBLISHED and names that commit — and the commit is read out of
#      the bucket with `cat`, so the row and the object agree without either being asked
#      to describe the other.
#
# What this proves that a unit test cannot: the snapshot request travels the only way an
# Agent is ever told anything — as desired state it converges on — and the answer comes
# back on the volume's ordinary report. Both halves were fully written before this and
# neither had a counterpart: the Control Plane could record a request no host would ever
# act on, and did.
set -euo pipefail

DEMO_NAME=stage5
DEMO_DONE="the whole of Stage 5 ran: a snapshot was asked for under a running guest, and it names a commit that is in the bucket"
ROTATE_AT=${ROTATE_AT:-4194304}
CHURN=${CHURN:-32}
HEARTBEAT=${HEARTBEAT:-300ms}
AGENT_FLAGS_TEMPLATE="-rotate-at-bytes $ROTATE_AT -retry-backoff $HEARTBEAT -object-store-dir @DIR@/store"
# shellcheck source=hack/demo-lib.sh
source "$(dirname "$0")/demo-lib.sh"

psql() { docker exec "$PGC" psql -U cp -d "$DB" -tAqc "$1"; }

say "2. a volume, and the first layer of its chain"
"$CP" -database-url "$DSN" -object-store-dir "$DIR/store" -kek-file "$DIR/kek" \
  -holder-id cp-seed -seed-volume -seed-host "$HOST_ID" -seed-size "$SIZE" >"$DIR/logs/seed.log" 2>&1
waitfor "$DIR/logs/agent1.log" "volume ready"
POINTER=$(grep -m1 -o 'pointer=[^ ]*' "$DIR/logs/agent1.log" | head -1 | cut -d= -f2)
SOCK=$(grep -m1 -o 'qmp_socket=[^ ]*' "$DIR/logs/agent1.log" | head -1 | cut -d= -f2)
VOLUME=$(grep -m1 -o 'volume_id=[^ ]*' "$DIR/logs/agent1.log" | head -1 | cut -d= -f2)
FIRST=$(cat "$POINTER")
echo "volume $VOLUME, first layer $(basename "$FIRST")"

say "3. a Linux guest boots off it and starts writing"
mkfifo "$DIR/ctl"
exec 9<>"$DIR/ctl"
"$QEMU" -machine "q35,accel=$ACCEL" -m 512 -smp 1 -display none -vga none -monitor none -no-reboot \
  -net none \
  -L "$OUT/qemu" \
  -kernel "$KERNEL" -initrd "$INITRAMFS" \
  -append "console=ttyS0 panic=1 spin.mode=hold spin.churn=$CHURN" \
  -drive "file=$FIRST,format=qcow2,if=virtio,cache=writeback" \
  -qmp "unix:$SOCK,server=on,wait=off" \
  -serial stdio <"$DIR/ctl" >"$DIR/logs/guest1.log" 2>&1 &
PIDS+=($!)
waitfor "$DIR/logs/guest1.log" "GUESTINIT-HELD" 120
echo "the guest is writing"

say "4. an operator asks for a snapshot, with the guest still running"
"$CP" -database-url "$DSN" -object-store-dir "$DIR/store" -kek-file "$DIR/kek" \
  -holder-id cp-snap -snapshot-volume "$VOLUME" >"$DIR/logs/snapshot.log" 2>&1
SNAP=$(grep -m1 -o 'snapshot_id=[^ ]*' "$DIR/logs/snapshot.log" | head -1 | cut -d= -f2)
test -n "$SNAP" || die "the control plane recorded no snapshot: $(cat "$DIR/logs/snapshot.log")"
echo "snapshot $SNAP requested"
# The row exists and names nothing yet. This is the state the whole feature used to stop
# in — CREATING for ever, because no host read the request.
STATE=$(psql "SELECT state FROM snapshots WHERE snapshot_id = '$SNAP'")
test "$STATE" = "CREATING" || die "a fresh snapshot is $STATE, want CREATING"

say "5. the Agent seals the tip because it was asked, and reports the commit"
waitfor "$DIR/logs/agent1.log" "sealing the tip for a snapshot" 60
waitfor "$DIR/logs/agent1.log" "snapshot taken" 60
grep -m1 "snapshot taken" "$DIR/logs/agent1.log" | sed 's/^/    /'

say "6. the catalog names a commit, and the bucket has it"
for _ in $(seq 60); do
  STATE=$(psql "SELECT state FROM snapshots WHERE snapshot_id = '$SNAP'")
  [ "$STATE" = "PUBLISHED" ] && break
  sleep 0.5
done
test "$STATE" = "PUBLISHED" || die "the snapshot is $STATE after the host reported it"
COMMIT=$(psql "SELECT commit_id FROM snapshots WHERE snapshot_id = '$SNAP'")
test -n "$COMMIT" || die "a PUBLISHED snapshot names no commit"
echo "    the catalog says: PUBLISHED at commit $COMMIT"

# And the object is there, read with cat. The key is *derived* from the two ids — nothing
# in the catalog stores it — which is the property the withdrawn manifest_key column gave
# up when it became a string the Agent sent.
MANIFEST="$DIR/store/volumes/$VOLUME/commits/$COMMIT.json"
test -f "$MANIFEST" || die "the catalog names commit $COMMIT and the bucket has no $MANIFEST"
echo "    the bucket has: $(tail -n +2 "$MANIFEST")"

# The snapshot must name a commit that is actually in this volume's published history —
# a commit id that exists but is not reachable from HEAD would be a snapshot of nothing.
HEAD_COMMIT=$(tail -n +2 "$DIR/store/volumes/$VOLUME/HEAD" |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["commit_id"])')
found=no
c=$HEAD_COMMIT
while [ -n "$c" ] && [ "$c" != "null" ]; do
  [ "$c" = "$COMMIT" ] && { found=yes; break; }
  c=$(tail -n +2 "$DIR/store/volumes/$VOLUME/commits/$c.json" |
    python3 -c 'import json,sys; print(json.load(sys.stdin).get("parent_commit_id") or "")')
done
test "$found" = yes || die "the snapshot names $COMMIT, which is not on the chain from HEAD"
echo "    and it is on the chain from HEAD"
