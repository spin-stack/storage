package dst

import (
	"testing"
)

// **No volume is ever both refused and serving a socket.** The fleet is told nobody is
// serving this volume; a guest can still connect to it and take an I/O error on every
// sector. Two statements about one volume, both believed, and nothing failing.
//
// Planted by the socket, not by the Agent: a listener whose Close does not take the path
// with it. That is one configuration away in production — `hostio.Listen` relies on Go
// unlinking the address of a listener *it* created, and a listener adopted from a passed
// descriptor never unlinks — and the outcome is the state this whole increment removed:
// a socket file a guest connects to and is refused by, forever, with the catalog
// correctly saying the volume is not being served.
//
// It is the one fault in this file that reaches the conjunction. The other two produce
// the opposite silence — a volume that keeps its device and is reported *healthy* — and
// are caught by the two checkers that already watch for that.
func TestPlantedBugARefusedVolumeKeepsItsSocket(t *testing.T) {
	requirePasses(t, 67, NewRefusedVolumeChecker(), scenarioARefusedVolumeHasNoSocket)
	plantedBug(t, 67, NewRefusedVolumeChecker(), "refused-volume-has-no-device", func(s *Sim) error {
		return aRefusedVolumeHasNoSocket(s, theSocketOutlivesItsListener, "image-missing")
	})
}

// A host that can no longer confirm it owns a volume must take the guest's device away,
// and the socket is where that is true or false — not `VolumeManager.Device`, which a
// refused volume deliberately still answers so that it can still be *reported*.
//
// Planted by the wiring, not by the code: a reconciler whose Fence records nothing and
// tears nothing down. That is DEV-0012 exactly — `Loop.report` computed the fenced list
// into a field and the data path never read it — and it is the same category as the
// plaintext-WAL and unwired-scheduler proofs: an omission, which is how this class of
// bug actually reaches production. It is worth planting here rather than only at the
// Control Plane's refusal, because the expired lease is the one fence with no external
// trigger: nothing outside the process knows the host stopped being able to renew, so an
// Agent that acted on nothing would look healthy from every direction but the guest's.
func TestPlantedBugAHostWithNoLeaseKeepsServing(t *testing.T) {
	requirePasses(t, 61, NewFencedVolumeChecker(), scenarioARefusedVolumeHasNoSocket)
	plantedBug(t, 61, NewFencedVolumeChecker(), "fenced-volume-not-served", func(s *Sim) error {
		return aRefusedVolumeHasNoSocket(s, theFenceIsNotActedOn, "lease-lost")
	})
}

// The other half of the same arm's argument: refusing is only right because serving
// would be wrong, and what "wrong" means is bytes.
//
// Planted by a Control Plane that lists a volume without the watermarks it holds for it.
// That is the message `GetDesiredState` really sent until those two fields were added:
// the catalog calls them informative (§5.8), which was read as "not worth sending", and
// the Agent was then asked to tell "this volume is new" from "this volume's image is
// gone" using the only authority that cannot distinguish them — the bucket. It finds no
// manifest, installs an empty base, and the guest reads zeros over every range the fleet
// says the volume published, with no error anywhere.
//
// The checker is the one that already watches the bytes a guest receives, because that
// is the only place "refused" and "served correctly" can be told apart from outside.
func TestPlantedBugAVolumeWithNoImageServesZeros(t *testing.T) {
	requirePasses(t, 63, NewDurableRangeChecker(), scenarioARefusedVolumeHasNoSocket)
	plantedBug(t, 63, NewDurableRangeChecker(), "durable-range-survives-restart", func(s *Sim) error {
		return aRefusedVolumeHasNoSocket(s, catalogForgetsWhatItPromised, "image-missing")
	})
}
