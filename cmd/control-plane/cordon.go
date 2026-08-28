package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
)

// setCordon takes a host out of the placement rotation, or puts it back, on an operator's
// authority — which until this existed had no way to be exercised: lifecycle.CordonOperator,
// the overwrite table, ErrCordonHeld and the third predicate of SetHostState were reachable
// only from a test, so an operator about to reboot a host could either leave it taking new
// volumes or hand-write an UPDATE that skips the term guard and the transition table.
//
// The reason is not a flag: CordonReason answers "who is asking", not "why", and there is
// exactly one human actor; a free-text note would be a second column that must agree with
// the first. What an incident needs is the state, which -fleet-status prints as OPERATOR.
//
// Uncordoning is `state = ACTIVE`, written on the operator's authority so it also clears a
// DEVICE_PRESSURE cordon. That is not a way to overrule the loop for long — the next
// heartbeat above 70% cordons the host again — and saying so is the point.
func setCordon(ctx context.Context, md metadata.Store, term int64, hostID string, state lifecycle.HostState) error {
	if err := md.SetHostState(ctx, term, hostID, state, lifecycle.CordonOperator); err != nil {
		return fmt.Errorf("setting host %s to %s: %w", hostID, state, err)
	}
	// The volumes the host already holds are named, because a cordoned host keeps serving
	// everything it has (lifecycle.HostState.Serving): an operator who reads "cordoned" and then
	// reboots the machine has stopped those volumes, not protected them.
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
