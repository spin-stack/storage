#!/usr/bin/env bash
# The kernel the QEMU guest lane boots: acquire it, and prove it is the one we pinned.
#
# storage does not build a kernel — it consumes spinbox's artefact (ADR-0021). What this
# script adds is that the artefact stops being *a path on one developer's machine*: it is
# pinned by content hash and can come from a registry, so the lane is runnable somewhere
# other than a laptop with a sibling checkout (ADR-0022).
#
# Everything lands at one canonical path ($KERNEL, default _output/guest/vmlinux). No task
# and no test resolves `../spinbox/...` any more; that path is only ever a *source* to
# copy from, and one of several.
#
#   fetch    put the pinned kernel at $KERNEL, from the first source that has it
#   verify   assert what has to hold about it before a guest is booted with it
#   pin      print the sha256 of a file, for bumping GUEST_KERNEL_SHA256
set -euo pipefail

KERNEL=${KERNEL:-_output/guest/vmlinux}
# The pin. Empty disables the check — for bisecting a kernel change, never for CI.
KERNEL_SHA256=${KERNEL_SHA256:-}
KERNEL_VERSION=${KERNEL_VERSION:-}
# Source 1: a sibling spinbox checkout that has already built one.
SPINBOX_KERNEL=${SPINBOX_KERNEL:-}
# Source 2: the mirrored image (ADR-0022). Pin it by digest, not by tag.
KERNEL_IMAGE=${KERNEL_IMAGE:-}

sha256() { sha256sum "$1" | cut -d' ' -f1; }

# matches_pin succeeds when the file is there and is the artefact we pinned. With no pin
# declared, presence is all there is to check.
matches_pin() {
  local f=$1
  [ -f "$f" ] || return 1
  [ -n "$KERNEL_SHA256" ] || return 0
  [ "$(sha256 "$f")" = "$KERNEL_SHA256" ]
}

# --- fetch ------------------------------------------------------------------------

# from_local copies a kernel an adjacent spinbox checkout has already built. This is the
# zero-network path and the one that works today; the registry below is what makes the
# lane runnable where no such checkout exists.
from_local() {
  [ -n "$SPINBOX_KERNEL" ] && [ -f "$SPINBOX_KERNEL" ] || return 1
  if ! matches_pin "$SPINBOX_KERNEL"; then
    echo "  $SPINBOX_KERNEL is $(sha256 "$SPINBOX_KERNEL"), not the pinned $KERNEL_SHA256"
    return 1
  fi
  cp -f "$SPINBOX_KERNEL" "$KERNEL"
  echo "  copied from $SPINBOX_KERNEL"
}

# from_image pulls the mirror. buildx with a local output is the same mechanism
# `task build:qemu` uses to get binaries out of an image, so there is one way in this
# repository to turn a published artefact into a file in _output/.
from_image() {
  [ -n "$KERNEL_IMAGE" ] || return 1
  command -v docker >/dev/null || { echo "  no docker, cannot pull $KERNEL_IMAGE"; return 1; }
  local tmp
  tmp=$(mktemp -d)
  # shellcheck disable=SC2064  # $tmp must expand now, not at trap time
  trap "rm -rf '$tmp'" RETURN
  docker buildx build \
    --file Dockerfile.guest-kernel \
    --target extract \
    --platform linux/amd64 \
    --build-arg "GUEST_KERNEL_REF=$KERNEL_IMAGE" \
    --output "type=local,dest=$tmp" \
    "$tmp" >/dev/null || { echo "  pulling $KERNEL_IMAGE failed"; return 1; }
  [ -f "$tmp/vmlinux" ] || { echo "  $KERNEL_IMAGE has no /vmlinux"; return 1; }
  if ! matches_pin "$tmp/vmlinux"; then
    echo "  $KERNEL_IMAGE carries $(sha256 "$tmp/vmlinux"), not the pinned $KERNEL_SHA256"
    return 1
  fi
  mv "$tmp/vmlinux" "$KERNEL"
  echo "  pulled from $KERNEL_IMAGE"
}

cmd_fetch() {
  mkdir -p "$(dirname "$KERNEL")"

  if matches_pin "$KERNEL"; then
    echo "kernel already at $KERNEL (${KERNEL_VERSION:-unpinned version})"
    return 0
  fi
  # A file that is there but wrong is the case worth being loud about: it is how a lane
  # certifies one kernel and reports on another.
  if [ -f "$KERNEL" ]; then
    echo "$KERNEL does not match the pin — replacing it"
  fi

  echo "fetching the guest kernel ${KERNEL_VERSION:+$KERNEL_VERSION }(pin ${KERNEL_SHA256:-none})..."
  if from_local || from_image; then
    echo "✓ $KERNEL"
    return 0
  fi

  cat >&2 <<EOF

no guest kernel could be obtained, and storage does not build one (ADR-0021).

Sources tried, in order:
  1. a sibling spinbox checkout — SPINBOX_KERNEL=${SPINBOX_KERNEL:-<unset>}
  2. the mirrored image        — GUEST_KERNEL_IMAGE=${KERNEL_IMAGE:-<unset>}

Any one of these fixes it:
  cd ../spinbox && task build:kernel     # then re-run; note it may exit non-zero and
                                         # still emit the artefact (cache permissions)
  task fetch:kernel GUEST_KERNEL_IMAGE=ghcr.io/<owner>/<repo>/guest-kernel:<version>
  task guest:kernel:push GUEST_KERNEL_IMAGE=...   # publish one you already have

If the kernel legitimately changed, re-pin it rather than clearing the pin:
  task guest:kernel:pin SPINBOX_KERNEL=<path>
EOF
  return 1
}

# --- verify -----------------------------------------------------------------------

# ikconfig prints the .config the kernel carries (CONFIG_IKCONFIG=y embeds it gzipped
# after the IKCFG_ST marker). Reading it from the vmlinux rather than from a config file
# next to it is the point: the artefact travels alone, and this is the only statement
# about it that cannot go stale.
ikconfig() {
  local off
  off=$(grep -abo -m1 'IKCFG_ST' "$KERNEL" 2>/dev/null | cut -d: -f1) || return 1
  [ -n "$off" ] || return 1
  # The gzip stream starts right after the 8-byte marker and is followed by the rest of
  # the kernel image, so gzip decompresses the config and *then* exits non-zero on the
  # trailing bytes. Under `set -o pipefail` that failure is the whole pipeline's, which
  # is why the status is discarded here and the caller judges by the output instead.
  tail -c "+$((off + 9))" "$KERNEL" | { gzip -dc 2>/dev/null || true; }
}

# Each of these turns a boot-time symptom into a named failure here. Without VIRTIO_BLK
# there is no /dev/vda and the guest reports a missing device, which reads as a backend
# bug; without PVH, QEMU has no entry point and fails opening a ROM; without
# BLK_DEV_INITRD the initramfs is ignored and PID 1 never runs; without the 8250 console
# the verdict the host greps for is never printed.
readonly REQUIRED_CONFIG=(
  CONFIG_VIRTIO_BLK
  CONFIG_PVH
  CONFIG_BLK_DEV_INITRD
  CONFIG_SERIAL_8250_CONSOLE
)

cmd_verify() {
  local fail=0

  test -f "$KERNEL" || { echo "no kernel at $KERNEL — run: task fetch:kernel" >&2; return 1; }

  if [ -n "$KERNEL_SHA256" ]; then
    local got
    got=$(sha256 "$KERNEL")
    if [ "$got" != "$KERNEL_SHA256" ]; then
      echo "$KERNEL is not the pinned kernel:" >&2
      echo "  pinned $KERNEL_SHA256" >&2
      echo "  found  $got" >&2
      echo "re-fetch it (task fetch:kernel) or re-pin deliberately (task guest:kernel:pin)" >&2
      return 1
    fi
  fi

  # The lane boots a PVH ELF, not a bzImage. QEMU's failure for the wrong one is a
  # rom-open error that reads like a backend bug, which is why it is checked here.
  head -c4 "$KERNEL" | grep -q 'ELF' || {
    echo "$KERNEL is not an ELF: the lane boots a PVH kernel, not a bzImage" >&2; return 1; }
  if command -v readelf >/dev/null; then
    readelf -n "$KERNEL" 2>/dev/null | grep -q 'Xen' || {
      echo "$KERNEL has no Xen PVH note: QEMU has no entry point for it (needs CONFIG_PVH=y)" >&2
      return 1; }
  fi

  local config checked
  config=$(ikconfig || true)
  if [ -n "$config" ]; then
    local opt
    for opt in "${REQUIRED_CONFIG[@]}"; do
      if ! grep -q "^${opt}=y$" <<<"$config"; then
        echo "$KERNEL was built without ${opt}=y" >&2
        fail=1
      fi
    done
    [ "$fail" -eq 0 ] || return 1
    checked="${REQUIRED_CONFIG[*]} present"
  else
    # Legal, just unverifiable: nothing says a kernel must carry its config.
    echo "warning: $KERNEL has no embedded config (CONFIG_IKCONFIG=n) — ${REQUIRED_CONFIG[*]} unchecked" >&2
    checked="config unchecked"
  fi

  # Just the version, not the full banner: the banner runs to the build host and
  # timestamp, and the first copy of it in the image is a truncated format string.
  local version
  version=$(strings -a "$KERNEL" 2>/dev/null | grep -m1 -o 'Linux version [^ ]*' || true)
  echo "OK: ${version:-$KERNEL} — PVH ELF, $checked"
}

# --- pin --------------------------------------------------------------------------

cmd_pin() {
  local f=${1:-${SPINBOX_KERNEL:-$KERNEL}}
  test -f "$f" || { echo "no such file: $f" >&2; return 1; }
  echo "$f"
  echo "  sha256: $(sha256 "$f")"
  local version
  version=$(strings -a "$f" 2>/dev/null | grep -m1 '^Linux version ' || true)
  [ -n "$version" ] && echo "  $version"
  echo
  echo "put that hash in Taskfile.yml as GUEST_KERNEL_SHA256, and record why it moved."
}

case "${1:-}" in
  fetch) cmd_fetch ;;
  verify) cmd_verify ;;
  pin) shift; cmd_pin "$@" ;;
  *) echo "usage: $0 {fetch|verify|pin [file]}" >&2; exit 2 ;;
esac
