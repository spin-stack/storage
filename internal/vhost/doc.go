// Package vhost serves a virtio-blk device to a vhost-user front-end — QEMU
// 11.0.2 — over a Unix socket (§3, §16, roadmap §30.3). It is the transport the
// guest's block I/O arrives on: the front-end shares its guest memory and a
// virtqueue, and this package parses the READ/WRITE/FLUSH requests out of that
// ring and completes them against a Backend.
//
// # Shape
//
// Everything that decides anything is pure. Message framing, feature
// negotiation, guest-address translation, split-virtqueue walking and virtio-blk
// request handling all operate on ordinary byte slices and small interfaces
// (Conn, Mapper, EventFD), so a simulated front-end drives the whole protocol in
// unit tests — no VM, no sockets, no shared memory. The real Unix socket, the
// SCM_RIGHTS file-descriptor passing, the mmap of the front-end's memory regions
// and the kick/call eventfds live behind those interfaces in
// internal/vhost/hostio.
//
// # INV-01
//
// §25.1/INV-01 says production code reaches the real world only through
// internal/simio. This package is the documented exception, and it is narrow:
// the exception is internal/vhost/hostio, not internal/vhost. vhost-user is not
// expressible over simio/network — that interface carries length-delimited
// messages between named endpoints, while vhost-user requires a SOCK_STREAM Unix
// socket whose messages carry file descriptors over SCM_RIGHTS, and whose whole
// point is mapping the peer's memory into this process. simio has no notion of
// either, and inventing one would model a fiction. See
// docs/plan/DECISIONS/ADR-0020-vhost-user-host-io.md.
package vhost
