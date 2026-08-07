package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
)

// setCordon takes a host out of the placement rotation, or puts it back, on an
// operator's authority.
//
// It exists because that authority had no way to be exercised. ADR-0013 §5 splits
// cordon into two actors — the pressure loop, which cordons a host whose device passes
// 70% used, and a human, who cordons one for a reason no measurement can see — and the
// whole of lifecycle.CordonReason, its overwrite table, lifecycle.ErrCordonHeld and the
// third predicate of the SetHostState statement exist to keep the loop from clearing
// what the human set. Every one of those was reachable only from a test:
// `grep -rn CordonOperator --include='*.go' . | grep -v _test.go` named the constant's
// own declaration, the overwrite table, and the store contract, and no binary. An
// operator about to reboot a host, or watching one with a flapping NIC, had exactly two
// options — leave it taking new volumes, or hand-write an UPDATE against the catalog
// that skips the term guard and the transition table.
//
// The reason is not a flag. CordonReason answers "who is asking", not "why", and there
// is exactly one human actor; a free-text note would be a second column that must agree
// with the first (the type's own comment rejects that), and this binary is a test
// harness whose operator surface is the sibling `spin` project's (ADR-0021). What an
// incident needs from here is the *state*, which -fleet-status prints in the REASON
// column as OPERATOR — distinguishable from DEVICE_PRESSURE, which is the only
// distinction any decision in this system makes.
//
// Uncordoning is `state = ACTIVE`, which stores CordonNone (the store derives that:
// a reason belongs to a cordon). It is written on the operator's authority so that it
// also clears a DEVICE_PRESSURE cordon — the case an operator hits when a host is
// wedged between the 65% and 70% band and they have decided it is fine. That is not a
// way to overrule the loop for long: the next heartbeat above 70% cordons it again,
// because CordonPressure may overwrite CordonNone. Saying so is the point — an operator
// who uncordons a full device should see it come straight back rather than believe the
// ceiling has been lifted.
func setCordon(ctx context.Context, md metadata.Store, term int64, hostID string, state lifecycle.HostState) error {
	if err := md.SetHostState(ctx, term, hostID, state, lifecycle.CordonOperator); err != nil {
		return fmt.Errorf("setting host %s to %s: %w", hostID, state, err)
	}
	// The volumes the host already holds are named in the line, because "cordoned" is
	// the answer to a question it does not answer: a cordoned host keeps serving
	// everything it has (lifecycle.HostState.Serving), and an operator who reads
	// "cordoned" and then reboots the machine has stopped those volumes, not protected
	// them. Detaching them is a separate decision and a separate command.
	vols, err := md.ListVolumesByHost(ctx, hostID)
	if err != nil {
		return fmt.Errorf("listing what host %s still serves: %w", hostID, err)
	}
	ids := make([]string, 0, len(vols))
	for _, v := range vols {
		ids = append(ids, v.VolumeID)
	}
	slog.Info("host fleet state changed by the operator",
		"host_id", hostID, "state", state,
		"still_serving", len(ids), "volume_ids", ids)
	return nil
}
