package agent_test

import (
	"bytes"
	"testing"

	"github.com/spin-stack/storage/internal/simio/sim"
)

// A range erased in the middle of a lineage reads as zeros through the whole chain, and
// goes on reading as zeros after the volume has stopped and started again.
//
// **This is the failure that decided whether publishing could stop flattening at all.**
// While a manifest was the volume's whole flattened view, an erasure was expressed as
// absence and absence read as zeros. In a chain of deltas absence means "ask the layer
// below", so the same manifest would hand the guest the *grandparent's* older bytes at
// every offset the parent freed. That is §14.6 exactly: not a lost write but data
// resurrection, on blocks a filesystem has already returned to its free list and may have
// handed to something else.
//
// The assertion is the bytes the guest reads back through the device, and the second half
// — after a stop and a restart — is not a repetition of the first. The first says the
// composition at attach is right; the second says the clone's *own* published manifest,
// laid back over that composition, did not undo it. Two different objects, two different
// ways to lose the erasure.
//
// # No guest can issue a DISCARD, and that is why the erasure is an ancestor's
//
// VIRTIO_BLK_F_DISCARD is not offered (internal/vhost/features.go: the bit is a wire
// negotiation with its own configuration-space fields), so nothing a device can be driven
// to do reaches wal.Log.Discard, which has no production caller. What is reachable through
// the real VolumeManager is the *other* side of the same format question: an ancestor
// whose published snapshot carries an erasure, which is what a snapshot of a volume that
// had discarded would contain. So the middle ancestor here erases a block its own parent
// wrote, through the production publisher, and everything below is the Agent.
//
// internal/image's TestADiscardedRangeIsNotResurrectedByTheAncestryUnderneathIt is the
// half this cannot reach: the volume under test doing the discarding itself.
func TestARangeErasedInTheMiddleOfALineageStaysErased(t *testing.T) {
	const (
		kept   = int64(0)                 // the oldest wrote it and nothing erased it
		erased = int64(4 * testBlockSize) // the oldest wrote it, the middle erased it
		ownOK  = int64(8 * testBlockSize) // the clone writes here, so its own image is real
	)
	store := sim.NewObjectStore()
	kms, dek, wrapped := lineageKeys(t)

	oldest := publishAncestor(t, store, dek, ancestorSpec{
		writes: map[int64]byte{kept: 0xA1, erased: 0xA1},
	})
	middle := publishAncestor(t, store, dek, ancestorSpec{
		parent:   oldest,
		discards: []int64{erased},
	})

	v := desiredVolume(t, 1)
	v.ParentSnapshotId, v.ParentVolumeId = middle.snapshot, middle.volume

	first := lineageManager(t, store, kms, wrapped, dek.KeyID, "/var/lib/spin-erased-1")
	dev := serveClone(t, first, v)
	readBlock(t, dev, kept, 0xA1, "a range the oldest ancestor wrote and nobody erased")
	assertZeros(t, dev, erased, "the middle ancestor erased this range; its parent's bytes must not show through")
	writeBlock(t, dev, ownOK, 0xC2)
	if err := first.Close(t.Context()); err != nil {
		t.Fatalf("closing the clone's first Agent: %v", err)
	}

	// The restart, on a fresh directory: everything comes back out of the bucket, and the
	// clone now has an image of its own to lay over the ancestry.
	second := lineageManager(t, store, kms, wrapped, dek.KeyID, "/var/lib/spin-erased-2")
	again := serveClone(t, second, v)
	readBlock(t, again, kept, 0xA1, "a restarted clone still reads what the oldest ancestor wrote")
	readBlock(t, again, ownOK, 0xC2, "a restarted clone reads what it wrote itself")
	assertZeros(t, again, erased, "after a restart the erased range reads as the oldest ancestor's bytes again: the erasure was resurrected")
}

// assertZeros is readBlock's opposite, and it is spelled out rather than expressed as
// readBlock(..., 0x00, ...) so a failure names what came back instead of it — which for
// this file is always somebody's resurrected data.
func assertZeros(t *testing.T, dev interface {
	ReadAt([]byte, int64) (int, error)
}, off int64, why string,
) {
	t.Helper()
	got := make([]byte, testBlockSize)
	if _, err := dev.ReadAt(got, off); err != nil {
		t.Fatalf("ReadAt %d: %v — %s", off, err, why)
	}
	if !bytes.Equal(got, make([]byte, testBlockSize)) {
		t.Fatalf("offset %d reads %#x, want zeros — %s", off, got[:8], why)
	}
}
