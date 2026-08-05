//go:build integration || e2e

package testinfra

import (
	"os"
	"testing"
)

// Whether a lane whose input is not on disk skips or fails.
//
// Every lane in this repository that needs something `task build:*` produces used to
// answer "skip", at three sites, unconditionally — and that is the whole reason
// `task ci:full` could report success on a machine that had never booted a guest. A gate
// is read by an exit code, and `t.Skip` exits 0. The guest-backed lanes carry the headline
// proof of every recent wave (a guest's fsync puts zero objects in the bucket, a snapshot
// taken under a live guest is one point and not a smear, an Agent that cannot publish
// keeps its data directory), and the gate said "ok" having run none of them.
//
// The fix is not "always fail", because both answers are right in different places:
//
//   - On a laptop with no QEMU, a red build that means "you did not run task build:qemu"
//     is a red build people learn to ignore, and the developer has not broken anything.
//   - In the merge gate, the *claim* is that these proofs ran. A skip there is that claim
//     being false, with nothing outside the process able to tell.
//
// So the decision moves to the caller who makes the claim: the gate sets
// SPIN_REQUIRE_PROOFS, and inside it a missing input is a failure that names the input,
// the task that produces it, and the honest target to run instead.
//
// Rejected: keying off CI=true (GitHub sets it). It makes the behaviour depend on where
// the process happens to run rather than on what the caller is asserting, so a developer
// could never reproduce the gate's answer, and a lane invoked from some future workflow
// that is *not* the gate would start hard-failing for no stated reason.
const requireProofsEnv = "SPIN_REQUIRE_PROOFS"

// missingInput reports that a lane's input is not on disk — as a skip, or, under the
// merge gate, as a failure. what describes the input; produce is the command that makes
// one. Neither returns to the caller: both t.Skipf and t.Fatalf end the test.
func missingInput(t *testing.T, what, produce string) {
	t.Helper()
	if os.Getenv(requireProofsEnv) == "" {
		t.Skipf("%s — run: %s", what, produce)
	}
	t.Fatalf("%s\n"+
		"run: %s\n\n"+
		"%s is set, so this lane may not skip itself: the gate that set it (task ci:full, "+
		"or CI's guest jobs) reports success only if these proofs actually ran. If this "+
		"machine genuinely cannot build QEMU, run `task ci:noguest` — the same gate minus "+
		"the guest-backed proofs, which says so in its name and in its last line of output.",
		what, produce, requireProofsEnv)
}
