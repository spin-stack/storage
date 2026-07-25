// Package simio is the root of the simulable I/O interfaces mandated by §25.1
// (INV-01). The interfaces live in the subpackages clock, disk, network, and
// objectstore; their thin real passthrough implementations live in
// internal/simio/real and their deterministic simulated implementations in
// internal/simio/sim. Everything under this path is exempt from the simulable
// analyzer — it is the one place allowed to touch real time, sockets, disk, and
// object-store primitives (ADR-0003).
package simio
