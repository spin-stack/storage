package recovery

import (
	"context"
	"errors"
	"fmt"

	"github.com/spin-stack/storage/internal/epoch"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// VerifyPublisher answers the question a publication has to ask before it claims a
// namespace only one host may write: *is this epoch mine?* — not merely "is the number
// still the one I was given?".
//
// The number cannot answer it. volumes.primary_host_id in PostgreSQL and the S3 epoch
// object are the two durable records of one promotion, written by two different steps
// of §12.3 (§12.4). A promoter that died between them, or one that was overtaken,
// leaves them naming different hosts at the same epoch; both hosts then pass a
// number-only epoch.Verify and both publish into wal/<vol>/<epoch>/ and
// checkpoints/<vol>/<epoch>/. Nothing downstream can tell the result apart from one
// writer's history — that is what INV-10 and INV-21 exist to name, and by then it is
// already unrecoverable.
//
// Three answers are deliberately *not* refusals, and each of them would be a worse
// failure than the hole if it were:
//
//   - No epoch object. §12.4 makes the object belt-and-suspenders over the monotonic
//     lease (§12.2) and the Control-Plane term (§7); a backend without conditional
//     writes has none, and the protocol is still safe. Refusing here would turn an
//     optional fence into a precondition for publishing anything.
//   - An epoch granted to nobody (HolderID empty). Init writes exactly that for a
//     volume that has just been created or rebuilt (§22.5) — it has no writer yet, so
//     there is nobody to fence. Refusing would stop that volume's first checkpoint,
//     and a volume that cannot checkpoint can never truncate its local WAL (INV-13):
//     the host's NVMe fills and stalls every co-tenant on the disk. Closing this case
//     properly is a Control-Plane change — grant epoch 1 to the host the volume is
//     attached to — not a publisher one.
//   - A read failure is *not* in that list. It is returned, because "I could not
//     establish who holds this epoch" is not "it is mine".
//
// It is one read, not two. epoch.VerifyHolder is the same rule with the two cases above
// closed, and calling it after a triage read would decide the number and the holder
// from two different versions of an object a promotion can CAS at any moment.
func VerifyPublisher(ctx context.Context, store objectstore.Store, volumeID string, ep uint64, hostID string) error {
	r, _, err := epoch.NewStore(store).CurrentRecord(ctx, volumeID)
	switch {
	case errors.Is(err, objectstore.ErrNotFound):
		return nil
	case err != nil:
		return fmt.Errorf("recovery: cannot establish who holds epoch %d of %s: %w", ep, volumeID, err)
	}
	if r.Epoch != ep {
		return fmt.Errorf("%w: %s is at epoch %d, this publisher holds %d",
			epoch.ErrEpochChanged, volumeID, r.Epoch, ep)
	}
	if r.HolderID == "" {
		return nil
	}
	if r.HolderID != hostID {
		return fmt.Errorf("%w: epoch %d of %s was granted to %q, this publisher is %q",
			epoch.ErrNotHolder, ep, volumeID, r.HolderID, hostID)
	}
	return nil
}
