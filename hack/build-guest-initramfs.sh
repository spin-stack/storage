#!/usr/bin/env bash
# Build the initramfs the QEMU integration lane boots: a single static Go binary as
# /init, and the mount points it needs.
#
# There is nothing else in it — no shell, no libc, no busybox. The guest's whole job is
# to open /dev/vda, write, fsync and read back, and every byte that is not that is a byte
# whose failure could be mistaken for ours.
#
# The kernel is NOT built here. It comes from spinbox (ADR-0021): storage consumes the
# built artefact, never the code, so a kernel change costs a path and not an import.
set -euo pipefail

OUT_DIR=${OUT_DIR:-_output/guest}
STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE"' EXIT

command -v cpio >/dev/null || { echo "cpio is required to build an initramfs (apt-get install cpio)"; exit 1; }

echo "building the guest init (static, linux/amd64)..."
# CGO off is what makes it static: an initramfs has no dynamic loader, so a binary
# linked against libc would fail with a message the kernel prints before any console
# the test can read.
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
  -trimpath -ldflags='-s -w' \
  -o "$STAGE/init" ./integration/guestinit

# devtmpfs, proc and sysfs are mounted by init itself; the kernel only needs the
# directories to exist.
mkdir -p "$STAGE"/{dev,proc,sys}

mkdir -p "$OUT_DIR"
( cd "$STAGE" && find . -print0 | cpio --null --create --format=newc --quiet ) \
  | gzip -9 > "$OUT_DIR/initramfs.cpio.gz"

echo "✓ $OUT_DIR/initramfs.cpio.gz ($(du -h "$OUT_DIR/initramfs.cpio.gz" | cut -f1))"
