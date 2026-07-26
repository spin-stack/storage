// Package hostio is the real-world half of the vhost-user backend: the Unix
// socket that carries the protocol, the SCM_RIGHTS descriptor passing that
// carries the guest's memory and doorbells, the mmap that maps that memory into
// this process, and the eventfds the front-end and the backend nudge each other
// with.
//
// # Why this package exists (INV-01)
//
// §25.1/INV-01 routes every real-world primitive through internal/simio. This
// package is the single documented exception, and it is a package rather than a
// set of scattered calls precisely so the exception has an address:
// internal/vhost is pure and testable against a simulated front-end, and
// everything that touches the kernel is here, behind the Conn, Listener, Mapper
// and EventFD interfaces that package defines.
//
// simio cannot express vhost-user. simio/network is a length-delimited message
// transport between named endpoints; vhost-user needs a SOCK_STREAM Unix socket
// whose messages carry file descriptors, and its central act is mapping the
// peer's address space into this process. Neither descriptor passing nor mmap
// has any counterpart in simio, and adding one would be modelling a fiction:
// there is nothing to simulate about "the guest's RAM is now our RAM" that is
// not simply the mapping itself. What *is* simulable — the protocol, the
// negotiation, the ring, the request handling — lives next door in
// internal/vhost and is simulated there.
//
// See docs/plan/DECISIONS/ADR-0020-vhost-user-host-io.md.
package hostio
