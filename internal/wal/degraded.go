package wal

import (
	"context"
	"errors"
	"strings"

	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/simio/disk"
)

// Degradation is why the local WAL device cannot be written to. It is a vocabulary
// in the style of internal/lifecycle — a named string type rather than a loose bool
// — because "degraded" is not one condition: the remedy for a full device (truncate
// after a checkpoint, grow the device, restore the object store so the remote gap can
// close) is not the remedy for anything else, and a caller that can only see a bool
// cannot pick one.
//
// Degradation is deliberately NOT a lifecycle machine. It has no operator-driven
// transitions and no illegal moves to reject: it is a latch over what the device just
// did, set by a failed append and cleared by a successful one.
type Degradation string

const (
	// DegradedNone is a log whose device is taking writes.
	DegradedNone Degradation = "NONE"
	// DegradedOutOfSpace is a log whose last append was refused for want of space on
	// the device (ENOSPC). It is sticky: a full device does not heal itself, so the
	// state does not clear until an append proves the device took bytes again.
	DegradedOutOfSpace Degradation = "OUT_OF_SPACE"
)

// String renders the degradation for traces and logs.
func (d Degradation) String() string {
	if d == "" {
		return string(DegradedNone)
	}
	return string(d)
}

// OutOfSpaceFunc classifies a disk error as "the device has no room left".
//
// It stays injectable even though disk.ErrNoSpace now makes the default portable: a
// deployment whose filesystem reports exhaustion its own way (a quota, a thin-provisioned
// volume returning EIO) can supply the rule without a change here. The default is
// correct for both disks in this tree.
type OutOfSpaceFunc func(error) bool

// noSpaceMessage is the wording both worlds share: syscall.ENOSPC.Error() is exactly
// this string, and sim.ErrNoSpace embeds it verbatim for that reason.
const noSpaceMessage = "no space left on device"

// DefaultOutOfSpace reports whether err is the device saying it is full. It compares
// by identity against disk.ErrNoSpace, which both the real disk (wrapping
// syscall.ENOSPC) and the simulator wrap, and keeps a message match as a fallback for
// a disk that returns the operating system's error unwrapped. It is used only to
// *label* a failure that has already been reported to the caller — a false negative
// loses a diagnostic, never a write, and a false positive raises a gauge, never an
// ACK.
func DefaultOutOfSpace(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, disk.ErrNoSpace) {
		return true
	}
	// A disk implementation that predates the sentinel, or one that returns the
	// operating system's error unwrapped. Kept as a fallback rather than as the rule:
	// it costs nothing, and dropping it would silently stop recognising a full device
	// on any such disk instead of failing loudly.
	return strings.Contains(err.Error(), noSpaceMessage)
}

// SetOutOfSpace replaces the classifier that decides whether a failed append means
// the device is full. Passing nil restores DefaultOutOfSpace.
func (l *Log) SetOutOfSpace(f OutOfSpaceFunc) {
	if f == nil {
		f = DefaultOutOfSpace
	}
	l.outOfSpace = f
}

// Degraded reports what, if anything, the local device is refusing to do.
//
// It is orthogonal to Fenced(), and the two must not be conflated. Fenced() is about
// the lease: this host has lost the authority to write for this volume at all, the
// Control Plane decides it, and no local action clears it (§12.2, §16). Degraded() is
// about the device under this one log: nothing cluster-wide has changed, the volume
// is still this host's, and a truncation or a bigger device fixes it. In particular
// ENOSPC never self-fences — handing a volume to another host because a disk filled
// would turn a local, recoverable condition into a failover.
func (l *Log) Degraded() Degradation {
	if l.degraded == "" {
		return DegradedNone
	}
	return l.degraded
}

// noteAppendResult latches or clears the device state around one append. It is called
// on every append, accepted or refused, which is what makes the state sticky without
// a separate probe: only an append the device took can clear it.
//
// Sync errors are deliberately not classified here. A filesystem with delayed
// allocation can report ENOSPC at fdatasync instead of at write; the simulated disk
// charges allocation at append time and does not model that variant, so treating it
// would be an untested branch. It is recorded as a known limitation rather than
// guessed at.
//
// The gauge is published only on a transition, so a healthy volume pays nothing per
// WRITE for a value that has not moved.
func (l *Log) noteAppendResult(err error) {
	was := l.Degraded()
	switch {
	case err == nil:
		l.degraded = DegradedNone
	case l.outOfSpace != nil && l.outOfSpace(err):
		l.degraded = DegradedOutOfSpace
	}
	// Any other error leaves the state as it was: a transient fault neither proves
	// the device is full nor proves it has room.
	if l.Degraded() != was {
		// The append path carries no context — Write is the guest's data path, not
		// an RPC — so a transition is recorded against a background context.
		l.recordDegraded(context.Background())
	}
}

// recordDegraded publishes the device state as `wal_out_of_space` (§26.2). It is a
// gauge and not a counter because what an operator needs to alert on is that the
// device is full *right now*; the count of times it filled says nothing about whether
// anyone can write.
//
// A nil recorder is a no-op, which is what the DST harness and the unit tests run
// with.
func (l *Log) recordDegraded(ctx context.Context) {
	if l.rec == nil {
		return
	}
	full := 0.0
	if l.Degraded() == DegradedOutOfSpace {
		full = 1
	}
	l.rec.Gauge(ctx, "wal_out_of_space", full, obs.String("volume", l.volLabel))
}
