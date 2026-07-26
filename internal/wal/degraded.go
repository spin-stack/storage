package wal

import (
	"context"
	"strings"

	"github.com/spin-stack/storage/internal/obs"
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
// It is injected for the same reason the clock and the disk are (§25.1, INV-01):
// `internal/simio/disk` — the interface the WAL depends on — declares no typed
// no-space sentinel, so there is nothing the WAL can compare against that holds for
// both implementations. The real disk returns an *os.PathError wrapping
// syscall.ENOSPC, and `syscall` is denied outside internal/simio; the simulated disk
// returns sim.ErrNoSpace, and production code must not import the simulator. Until
// `disk` grows a shared sentinel (a simio-owned change), classification is a policy
// the caller can supply and DefaultOutOfSpace is the portable fallback.
type OutOfSpaceFunc func(error) bool

// noSpaceMessage is the wording both worlds share: syscall.ENOSPC.Error() is exactly
// this string, and sim.ErrNoSpace embeds it verbatim for that reason.
const noSpaceMessage = "no space left on device"

// DefaultOutOfSpace reports whether err is the device saying it is full. It matches
// on the message rather than on identity, which is a compromise and is stated as one:
// the alternative is a WAL that can never recognise a full device on either the real
// or the simulated disk. It is used only to *label* a failure that has already been
// reported to the caller — a false negative loses a diagnostic, never a write, and a
// false positive raises a gauge, never an ACK.
func DefaultOutOfSpace(err error) bool {
	return err != nil && strings.Contains(err.Error(), noSpaceMessage)
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
