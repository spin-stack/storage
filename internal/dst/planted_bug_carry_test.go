package dst

import (
	"testing"
)

// The carry's whole safety argument is an *order*: append every kept record under the
// granted epoch, fdatasync, and only then unlink the directories they came from. Nothing
// else in this tree watches that order. The unit tests drive the carry on a device that
// keeps its promises, so the sync is invisible there — remove the call and every one of
// them still passes.
//
// Planted by the device, not by the code: a drive with a volatile write cache that
// reports fdatasync complete without persisting anything. That is an operational reality
// (a consumer SSD, a virtio-blk configured cache=unsafe, a filesystem mounted nobarrier),
// it is the fault sim.Disk already models as InjectSyncLoss, and it turns "append,
// fdatasync, unlink" back into "append, unlink" without a line of internal/wal changing.
//
// What it buys is the failure the whole file exists downstream of, made worse: the
// records the guest was ACKed on are unlinked from the epoch that held them, the copies
// under the granted epoch were only ever in the page cache, and the host dies. The
// volume comes back holding nothing at all — no error at any point, and the bytes that
// were on the disk before the rescue are not on it after.
//
// The checker sees it as an absence, which is why it has to be a checker: every scan is
// individually consistent, every digest that *is* there matches, and the only statement
// that is false is one about the whole run — a sequence that was promised is on no epoch
// of the device once the carry has had its last chance to finish.
func TestPlantedBugCarriedRecordsAreUnlinkedBeforeTheyAreDurable(t *testing.T) {
	requirePasses(t, 51, NewCarriedRecordChecker(), scenarioCarryForwardCrashPoints)
	plantedBug(t, 51, NewCarriedRecordChecker(), "acked-records-cross-epochs-intact", func(s *Sim) error {
		return carryForwardCrashPoints(s, fdatasyncIgnored)
	})
}
