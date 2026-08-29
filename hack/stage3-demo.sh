#!/usr/bin/env bash
# Stage 3, end to end: what a guest writes ends up in an object store, and what is in the
# object store is enough to rebuild the volume without this host.
#
#   1. a volume is provisioned and a guest boots off its first layer;
#   2. the guest writes continuously; the Agent seals the tip and **publishes** each
#      sealed layer — upload, immutable manifest, compare-and-set on HEAD;
#   3. the bucket is read back with nothing but `cat`: HEAD names a commit, the commit
#      names its parent and a layer, and the chain from HEAD is unbroken;
#   4. every layer object is checked for the guest's own bytes — they are sealed with the
#      volume's DEK on the way out (v6 §10), so finding them would be the whole promise
#      broken;
#   5. the guest stops and a second boot reads back what it wrote, locally.
set -euo pipefail

DEMO_NAME=stage3
DEMO_DONE="the whole of Stage 3 ran: a guest wrote, every sealed layer was published, and HEAD's chain is complete and sealed"
ROTATE_AT=${ROTATE_AT:-4194304}
CHURN=${CHURN:-32}
COMMITS_WANTED=${COMMITS_WANTED:-3}
HEARTBEAT=${HEARTBEAT:-300ms}
# The Agent publishes into the same object store the Control Plane was started with, and
# needs the same KEK: a layer is sealed with the volume's DEK, which is wrapped under it.
# $DIR is chosen by demo-lib, so the store path is filled in after it is sourced — which
# is why this is a template rather than the string itself.
AGENT_FLAGS_TEMPLATE="-rotate-at-bytes $ROTATE_AT -retry-backoff $HEARTBEAT -object-store-dir @DIR@/store"
# shellcheck source=hack/demo-lib.sh
source "$(dirname "$0")/demo-lib.sh"

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
echo "the guest is writing"

say "4. the Agent seals each tip and publishes it"
waituntil "$DIR/logs/agent1.log" "committed:" "$COMMITS_WANTED" 300
grep "committed:" "$DIR/logs/agent1.log" | sed 's/^/    /'
grep -q "could not publish" "$DIR/logs/agent1.log" &&
  die "a publish failed"
grep -q "PUBLISH_FENCED\|refusing a volume" "$DIR/logs/agent1.log" &&
  die "the volume was refused while it was publishing"

say "5. what is in the object store, read with cat"
# Every structural object is `<sha256 of the payload>\n<json>` (internal/framed), so the
# payload is everything after the first line. No tool of ours is needed to read a bucket,
# which is the point of the format.
unframe() { tail -n +2 "$1"; }
HEAD_OBJ="$DIR/store/volumes/$VOLUME/HEAD"
test -f "$HEAD_OBJ" || die "no HEAD at $HEAD_OBJ"
echo "    HEAD: $(unframe "$HEAD_OBJ")"
COMMIT=$(unframe "$HEAD_OBJ" | python3 -c 'import json,sys; print(json.load(sys.stdin)["commit_id"])')

# Walk the chain from HEAD to the first commit, checking every layer it names is there
# and is the size and digest the manifest recorded.
depth=0
while [ -n "$COMMIT" ]; do
  M="$DIR/store/volumes/$VOLUME/commits/$COMMIT.json"
  test -f "$M" || die "the chain from HEAD reaches commit $COMMIT and there is no manifest for it"
  # Split on a pipe, not on whitespace and not on a tab. The first commit has no parent,
  # so the line starts with an empty field — and `read` discards leading IFS *whitespace*,
  # tabs included, which shifts every other field along. It read back as a layer named by
  # its own size.
  IFS='|' read -r PARENT KEY SIZE SHA < <(unframe "$M" | python3 -c '
import json,sys
m = json.load(sys.stdin)
print("|".join([m.get("parent_commit_id",""), m["layer"]["object_key"], str(m["layer"]["size"]), m["layer"]["sha256"]]))')
  L="$DIR/store/$KEY"
  test -f "$L" || die "commit $COMMIT names layer $KEY and it is not in the bucket"
  got=$(stat -c%s "$L")
  [ "$got" = "$SIZE" ] || die "layer $KEY is $got bytes, the manifest says $SIZE"
  gotsha=$(sha256sum "$L" | cut -d' ' -f1)
  [ "$gotsha" = "$SHA" ] || die "layer $KEY hashes to $gotsha, the manifest says $SHA"
  depth=$((depth + 1))
  echo "    commit $COMMIT -> layer $(basename "$KEY") ($SIZE bytes, digest ok)"
  COMMIT=$PARENT
done
[ "$depth" -ge "$COMMITS_WANTED" ] || die "the chain from HEAD is $depth commits, want at least $COMMITS_WANTED"
echo "    $depth commits, every layer present and matching its digest"

say "6. nothing left this host in the clear (v6 §10)"
# The guest writes a known pattern; a layer object containing it would mean the volume's
# data went to the object store unsealed. Checked against every layer, not a sample.
NEEDLE=$(python3 -c 'print("".join(chr(ord("A") + (i % 23)) for i in range(64)))')
for L in "$DIR"/store/layers/sha256/*/*/*; do
  grep -qF "$NEEDLE" "$L" && die "$(basename "$L") contains the guest's plaintext"
  # And the churn's pattern, which is a different one and covers far more of the device.
  grep -qF "abcdefghijklmnopqrstuvwxyzabcdefghijklmnop" "$L" && die "$(basename "$L") contains the guest's churn in the clear"
done
echo "    $(ls "$DIR"/store/layers/sha256/*/*/* | wc -l) layer objects, none carrying the guest's bytes"

say "7. the fleet can answer what the RPO is (v6 §11, §21)"
# The number this system exists to keep small, asked of the Control Plane rather than
# scraped off the host: every commit above happened on the Agent, and until it reached the
# catalog nobody could ask about a volume by name.
"$CP" -database-url "$DSN" -object-store-dir "$DIR/store" -fleet-status >"$DIR/logs/fleet.txt" 2>&1 ||
  die "-fleet-status failed: $(tail -5 "$DIR/logs/fleet.txt")"
RPO=$(awk -v v="$VOLUME" '$1 == v {print $7}' "$DIR/logs/fleet.txt")
UNPUB=$(awk -v v="$VOLUME" '$1 == v {print $8}' "$DIR/logs/fleet.txt")
[ -n "$RPO" ] && [ "$RPO" != "-" ] ||
  die "the catalog has no RPO for $VOLUME after $depth commits: $(grep "$VOLUME" "$DIR/logs/fleet.txt")"
echo "    the catalog says volume $VOLUME is $RPO behind the object store, with $UNPUB unpublished"

say "8. the guest is told to stop, and powers itself off"
echo "GUESTCTL-STOP" >&9
waitfor "$DIR/logs/guest1.log" "GUESTINIT-CHURNED" 120
echo "    the guest wrote $(grep -m1 -o 'GUESTINIT-CHURNED [0-9]*' "$DIR/logs/guest1.log" | awk '{print $2}') MiB while that happened"
waitfor "$DIR/logs/guest1.log" "GUESTINIT-PASS" 120
wait "$GUEST1" 2>/dev/null || true
grep -m1 "GUESTINIT-PASS" "$DIR/logs/guest1.log"

say "9. a second boot, reading only"
TIP=$(cat "$POINTER")
"$QEMU" -machine "q35,accel=$ACCEL" -m 512 -smp 1 -display none -monitor none -no-reboot \
  -L "$OUT/share/spin-stack/qemu" \
  -kernel "$KERNEL" -initrd "$INITRAMFS" \
  -append "console=ttyS0 panic=1 spin.mode=verify" \
  -drive "file=$TIP,format=qcow2,if=virtio,cache=writeback" \
  -serial stdio </dev/null >"$DIR/logs/guest2.log" 2>&1
grep -q "GUESTINIT-PASS" "$DIR/logs/guest2.log" ||
  { tail -20 "$DIR/logs/guest2.log" >&2; die "the bytes did not survive"; }
grep -m1 "GUESTINIT-PASS" "$DIR/logs/guest2.log"
