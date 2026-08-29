package dst

import (
	"bytes"
	"context"
	"fmt"
	"slices"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/qcow"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// reconcileScenarios is the half of the reconciler a simulation can reach.
//
// **Half, and the split is where qemu-img is.** What the reconciler *does* — open a
// chain, ask a live QEMU which file it has open, snapshot a new overlay over the tip —
// is another process's work, and a fake for it here would be a second implementation of
// qemu-img: a model of a program, agreeing with the scenario and with nothing else. That
// half is tested against the real binary, in internal/qcow's adversary lane
// (adversary_reconcile, adversary_crash-points, adversary_qcow-restart) and end to end by
// `task demo:stage2`.
//
// What is left when that is taken away is the decision, and it is the part that has been
// wrong before: which layers this host has sealed and not published, derived from the
// record on disk and the tip QEMU has open (qcow.State.ObserveTip and State.SealedBelow).
// It is pure over data this package can hold, and its inputs arrive through I/O this
// package can break — a disk that acknowledges an fsync it does not honour, and then
// loses power.
func reconcileScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "no-sealed-layer-is-chained-past", Run: reconcileScenario(false)},
	}
}

// reconcileRoot is the simulated Agent's data directory. Absolute, because qcow.Config
// refuses a relative one and every path in a state record is built from it.
const reconcileRoot = "/agent"

// reconcileVirtualSize is the guest-visible size of the scenario's volume.
const reconcileVirtualSize = 1 << 30

// reconcileScenario is one volume, four layers and a power failure.
//
// The Agent starts with no object store, which is what an Agent started without one does
// today: it serves the guest and rotates, and every sealed layer stays on the host. Two
// rotations later the power fails, and the operator configures the store on the way back
// up. So the host comes up owing two layers at once and having been told about neither —
// the case the derivation is for, and the case the publishing order is for. Nothing may be
// lost by the crash: the tip is re-observed on the way back up, and every layer under it
// that is in no commit is still owed, oldest first.
//
// Then the derivation's other edge, which the same crash reaches: a layer recorded
// *above* the tip, because the pointer moves before QEMU is told to switch. Nothing may
// leave the host across that cycle — not the recorded layer, which no guest ever wrote
// into, and not the tip, which one still is.
//
// volatileCache is the fault. With it, every state record written between the volume
// coming up and the power failure is acknowledged and not persisted, which is what a
// device with a volatile write cache and no flush does; the power failure then takes them
// all. Deliberately not from the very first write: a state.json that was never durable at
// all comes back zero-length, and ReadState refuses that outright (internal/qcow's state
// table covers it). What this reaches is the dangerous shape instead — a record that is
// framed, digest-sound, believable, and one power failure out of date.
func reconcileScenario(volatileCache bool) Scenario {
	return func(s *Sim) error {
		ctx := context.Background()
		volumeID := ids.NewAt(1<<40, s.Rand).String()
		enc, err := volumeKey(s, volumeID)
		if err != nil {
			return err
		}
		h := &reconcileHost{
			s: s, paths: simPaths{disk: s.Disk}, store: &recordingStore{Store: s.Store, sim: s},
			enc: enc, volumeID: volumeID,
		}
		layers := make([]string, 4)
		for i := range layers {
			layers[i] = ids.NewAt(int64(1<<40+100+i), s.Rand).String()
		}

		tip := layers[0]
		if err := h.cycle(ctx, tip); err != nil {
			return err
		}

		h.volatile = volatileCache
		if volatileCache {
			s.Emit(Event{Kind: EventFault, Msg: "the disk acknowledges an fsync it does not honour"})
		}

		h.rotate(tip, layers[1])
		tip = layers[1]
		if err := h.cycle(ctx, tip); err != nil {
			return err
		}

		h.rotate(tip, layers[2])
		tip = layers[2]
		if err := h.record(tip); err != nil {
			return err
		}
		s.Disk.Crash()
		s.Emit(Event{Kind: EventFault, Msg: "the host lost power with two sealed layers on it and no object store to send them to"})

		h.publisher = true
		s.Notef("the object store is configured; this host owes it everything it has sealed")

		// One layer per cycle, so two cycles at the same tip: the second is what shows
		// the walk did not stop at the layer the first one published.
		for range 2 {
			if err := h.cycle(ctx, tip); err != nil {
				return err
			}
		}
		// The other half of the derivation, and one a crash reaches just as easily: the
		// pointer moves before QEMU is told to switch, so a cycle can find a layer
		// recorded *above* the tip the guest is still writing into. Neither is this
		// host's to publish — the recorded one holds no guest writes at all, the tip is
		// still being written — so the chain must not grow across this cycle. Stated as
		// a difference and not as a list of layers, because the run carrying the fault
		// is two layers behind here and has to reach its own violation.
		before, err := h.publishedLayers(ctx)
		if err != nil {
			return err
		}
		if err := h.record(layers[3]); err != nil {
			return err
		}
		if err := h.cycle(ctx, tip); err != nil {
			return err
		}
		after, err := h.publishedLayers(ctx)
		if err != nil {
			return err
		}
		if !slices.Equal(before, after) {
			return fmt.Errorf("the tip was recorded ahead of the guest's and the chain grew from %v to %v; %s is the file QEMU has open and %s is one it never opened",
				before, after, tip, layers[3])
		}

		h.rotate(tip, layers[3])
		tip = layers[3]
		if err := h.cycle(ctx, tip); err != nil {
			return err
		}

		// The bucket, read the way a recovery reads it. Three of the four layers were
		// sealed; the fourth is the tip the guest is still writing into and is nobody's
		// to publish.
		got, err := h.publishedLayers(ctx)
		if err != nil {
			return err
		}
		if want := layers[:3]; !slices.Equal(got, want) {
			return fmt.Errorf("the chain from HEAD carries layers %v; the guest's writes are in %v", got, want)
		}
		return nil
	}
}

// reconcileHost is one Agent's reconciliation cycle for one volume, with the two steps
// that need a live QEMU left out: the tip is whatever the caller says QEMU has open, and
// the layer's bytes stand in for a qcow2 file.
//
// Everything about *deciding* is the production code's: the state record is read and
// written by qcow.ReadState and qcow.WriteState through the simulated disk, the tip is
// recorded by qcow.State.ObserveTip, and which layer is owed comes from
// qcow.State.SealedBelow.
type reconcileHost struct {
	s        *Sim
	paths    simPaths
	store    objectstore.Store
	enc      *crypto.Encryption
	volumeID string
	// commits counts the commit ids minted in this run, so each is distinct. A restart
	// that has lost the record of what a sealed layer was promised under mints a fresh
	// id for it, which is a duplicate commit for one layer and is the documented cost.
	commits int
	// publisher is whether this Agent has an object store at all. Without one it owes
	// nothing and rotates freely, which is what makes the layers pile up.
	publisher bool
	// volatile makes the next state write report a successful fsync without persisting.
	volatile bool
}

// cycle is one pass of the reconciliation loop: record the tip, then publish the oldest
// layer this host owes. Two steps, because the crash between them is the one the
// derivation is written to survive.
func (h *reconcileHost) cycle(ctx context.Context, tip string) error {
	if err := h.record(tip); err != nil {
		return err
	}
	if !h.publisher {
		return nil
	}
	return h.publish(ctx, tip)
}

// rotate is QEMU switching the guest onto a new tip, which is what makes the old one a
// complete, read-only layer. The scenario states it; the reconciler is not told.
func (h *reconcileHost) rotate(sealed, next string) {
	h.s.Emit(Event{
		Kind: EventSeal, LayerID: sealed,
		Msg: fmt.Sprintf("layer=%s sealed, the guest is now writing into %s", sealed, next),
	})
}

func (h *reconcileHost) record(tip string) error {
	st, err := qcow.ReadState(h.paths, reconcileRoot, h.volumeID)
	if err != nil {
		return err
	}
	if !st.ObserveTip(tip) {
		return nil
	}
	return h.writeState(st)
}

func (h *reconcileHost) publish(ctx context.Context, tip string) error {
	st, err := qcow.ReadState(h.paths, reconcileRoot, h.volumeID)
	if err != nil {
		return err
	}
	owed := st.SealedBelow(tip)
	if len(owed) == 0 {
		return nil
	}
	// The oldest first: a commit whose parent is not in the history yet is a hole
	// spliced into the chain.
	layerID := owed[0]
	h.commits++
	m, err := publishLayer(ctx, h.s, h.store, h.enc, "host-a", commit.Request{
		VolumeID: h.volumeID, CommitID: ids.NewAt(int64(1<<40+h.commits), h.s.Rand).String(),
		LayerID: layerID, Epoch: 1, VirtualSize: reconcileVirtualSize,
	}, bytes.Repeat([]byte(layerID+" "), 64))
	if err != nil {
		return fmt.Errorf("publishing layer %s: %w", layerID, err)
	}
	// Which commit this host's copy of the layer came from — the half of recordCommit
	// that decides anything. The trim that bounds the list is left out: the walk stops
	// at the first published layer either way, so nothing here depends on it.
	st.Commits = append(st.Commits, qcow.CommitLayer{CommitID: m.CommitID, LayerID: layerID})
	return h.writeState(st)
}

func (h *reconcileHost) writeState(st qcow.State) error {
	if h.volatile {
		// One-shot, and WriteAtomic syncs once, so this is exactly the write ahead.
		h.s.Disk.InjectSyncLoss(qcow.StateFile(reconcileRoot, h.volumeID))
	}
	return qcow.WriteState(h.paths, reconcileRoot, h.volumeID, st)
}

// publishedLayers walks the chain from HEAD and returns the layers it names, oldest
// first: what a host that has never seen this volume would find in the bucket.
func (h *reconcileHost) publishedLayers(ctx context.Context) ([]string, error) {
	head, _, err := commit.ReadHead(ctx, h.s.Store, h.volumeID)
	if err != nil {
		return nil, fmt.Errorf("reading HEAD: %w", err)
	}
	var out []string
	for id := head.CommitID; id != ""; {
		m, err := commit.ReadManifest(ctx, h.s.Store, h.volumeID, id)
		if err != nil {
			return nil, fmt.Errorf("the chain from HEAD reaches %s, which is not readable: %w", id, err)
		}
		out = append([]string{m.Layer.LayerID}, out...)
		id = m.ParentCommitID
	}
	return out, nil
}
