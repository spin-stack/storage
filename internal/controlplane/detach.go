package controlplane

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
)

// ErrStillServing means the volume's host has not been observed to have stopped serving
// it, so taking it away now could put a second writer on it.
var ErrStillServing = errors.New("controlplane: the volume's host is still serving it")

// Detach takes a volume away from its host, and is the first half of moving one: `Place`
// refuses a volume that is placed elsewhere, so this is what a `-attach-volume` on another
// host has to go through.
//
// The rule it enforces is the one the operator's instructions already carried — "detached,
// *observed to have stopped*, and then placed" — which until now was a sentence in a log
// line and nothing else. An operator reacting to a partition is exactly who runs this, and
// exactly when the incumbent is least able to say what it is doing, so the observation has
// to be something the catalog holds rather than something a human remembers to do.
//
// Three ways to have observed it, any one of which is enough:
//
//   - The volume is not placed anywhere. Nothing to take it from.
//   - Its host's last report says it is not serving it — any refusal, LEASE_LOST and
//     ISOLATED included. That is the host itself saying it stopped.
//   - Its host has not been heard from for the dwell. Silence is not proof, and it is not
//     treated as any: what makes this safe is the isolation response on the other side,
//     which pauses that host's guests after its own grace. The inequality between the two
//     is why they are one design — see agent.Loop.isolationGrace.
//
// Otherwise it refuses, and `-force` is the operator saying they have observed it some
// other way. There is no fourth automatic answer to add here: the honest one is the
// promoter, which would drive the volume through FENCING_WAIT and its durable dwell, and
// which does not exist.
func Detach(ctx context.Context, md metadata.Store, term int64, volumeID string, dwell time.Duration, now time.Time, force bool) error {
	vol, err := md.GetVolume(ctx, volumeID)
	if err != nil {
		return err
	}
	if !force && vol.PrimaryHostID != "" {
		if err := observedStopped(ctx, md, vol, dwell, now); err != nil {
			return err
		}
	}
	return md.SetVolumePrimaryHost(ctx, term, volumeID, "")
}

// observedStopped is the check itself, separated so its refusal can say which of the three
// answers was missing — an operator who cannot tell why cannot decide whether to force.
func observedStopped(ctx context.Context, md metadata.Store, vol metadata.Volume, dwell time.Duration, now time.Time) error {
	if vol.Progress.Refusal != lifecycle.RefusalNone {
		return nil
	}
	host, err := md.GetHost(ctx, vol.PrimaryHostID)
	if err != nil {
		// A volume placed on a host the catalog does not have is a broken row, not a
		// running writer. Reported rather than allowed: the operator is one -force away,
		// and guessing here is guessing about a guest.
		return fmt.Errorf("%w: volume %s names host %s, which the catalog does not have: %w",
			ErrStillServing, vol.VolumeID, vol.PrimaryHostID, err)
	}
	if silence := now.Sub(host.LastHeartbeat); silence >= dwell {
		return nil
	}
	return fmt.Errorf("%w: volume %s is served by host %s, which was heard from %s ago and reports no refusal; wait %s for the dwell, or pass -force if you have observed the guest stop",
		ErrStillServing, vol.VolumeID, vol.PrimaryHostID,
		now.Sub(host.LastHeartbeat).Truncate(time.Second), dwell)
}
