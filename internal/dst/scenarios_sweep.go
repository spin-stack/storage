package dst

import (
	"fmt"

	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/qcow"
)

// sweepScenarios drives the one rule in this system that deletes files.
//
// **Half, and the split is the same one the reconciler's scenarios describe.** What the
// sweep decides — which layer files nothing on this host reads through — is pure over the
// records and the pointers, and both arrive through I/O this package can break. What it
// cannot reach is the live QEMU: `Manager.Apply` asks a running guest which image it has
// open and refuses a volume it cannot account for, and a fake for that here would be a
// model of qemu-img agreeing with the scenario. That half is qcow's.
//
// The fault is a disk that acknowledges an fsync it does not honour. It is aimed here
// rather than anywhere else because the sweep's keep-set comes from the records, so a
// record that comes back *believable and out of date* is the input that makes the rule
// delete the wrong file — and a record like that passes every check the code makes on it:
// it is framed, its digest is sound, and it names a coherent chain from a minute ago.
func sweepScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "no-live-layer-is-swept", Run: sweepScenario(false)},
	}
}

// sweepScenario is two volumes on one host, one of them a clone reading through the
// other's published layers, and a power failure that takes the newer records with it.
//
// The clone is the shape that makes a short keep-set catastrophic rather than merely
// wasteful. Layers live in one directory for the whole host (ADR-0027), so a volume whose
// record came back one power failure out of date does not lose *its* files: it loses the
// files of whoever else is reading them.
func sweepScenario(volatileCache bool) Scenario {
	return func(s *Sim) error { return runSweepScenario(s, volatileCache) }
}

func runSweepScenario(s *Sim, volatileCache bool) error {
	h := &sweepHost{s: s, paths: simPaths{disk: s.Disk}}
	parent := ids.NewAt(1<<40, s.Rand).String()
	clone := ids.NewAt(1<<40+1, s.Rand).String()

	// The parent's published history: three layers a guest has written and a tip it is
	// writing into now.
	base := make([]string, 4)
	for i := range base {
		base[i] = ids.NewAt(int64(1<<40+10+i), s.Rand).String()
	}
	for _, id := range base {
		if err := h.create(id); err != nil {
			return err
		}
	}
	if err := h.serve(parent, base, base[len(base)-1]); err != nil {
		return err
	}

	// The clone reads through the parent's first three layers and writes into its own.
	cloneTip := ids.NewAt(1<<40+20, s.Rand).String()
	if err := h.create(cloneTip); err != nil {
		return err
	}
	if err := h.serve(clone, append(append([]string{}, base[:3]...), cloneTip), cloneTip); err != nil {
		return err
	}

	// Nothing is garbage yet, and the sweep must agree.
	if err := h.sweep(); err != nil {
		return err
	}

	// What the rule is *for*: a volume that released its claim. Reclaim rewrites its
	// record to name nothing (on a grant confirmed elsewhere), and from that moment its
	// layers are read by nobody — so a run where the sweep collects nothing has not
	// exercised the rule at all. The clone still reads three of them, and those must stay:
	// that is the whole reason releasing a claim and freeing a file are two separate acts.
	released := ids.NewAt(1<<40+2, s.Rand).String()
	ownLayer := ids.NewAt(int64(1<<40+15), s.Rand).String()
	if err := h.create(ownLayer); err != nil {
		return err
	}
	if err := h.serve(released, append(append([]string{}, base[:3]...), ownLayer), ownLayer); err != nil {
		return err
	}
	if err := h.release(released); err != nil {
		return err
	}
	if err := h.sweep(); err != nil {
		return err
	}
	if there, err := h.paths.Exists(qcow.LayerImage(sweepRoot, ownLayer)); err != nil || there {
		return fmt.Errorf("volume %s released its claim and its own layer %s is still on disk (%v)", released, ownLayer, err)
	}
	for _, id := range base[:3] {
		if there, err := h.paths.Exists(qcow.LayerImage(sweepRoot, id)); err != nil || !there {
			return fmt.Errorf("layer %s went with a volume that released it, and the clone reads through it (%v)", id, err)
		}
	}

	// The fault. From here the disk acknowledges without persisting, so everything the
	// parent records about the layers it rotates onto is lost by the power failure — and
	// what comes back is not an empty record, which the code refuses outright, but the
	// coherent one from before.
	if volatileCache {
		s.Emit(Event{Kind: EventFault, Msg: "the disk acknowledges an fsync it does not honour"})
		h.volatile = true
		// And the guest's QEMU stops answering. One fault alone is survivable and the
		// unplanted run says so: a stale record is caught by asking the running guest,
		// and an unreachable guest leaves records this host still trusts. Together they
		// take the answer away and corrupt what is left.
		s.Emit(Event{Kind: EventFault, Msg: "the QMP socket stops answering"})
		h.qmpDown = true
	}

	grown := ids.NewAt(1<<40+40, s.Rand).String()
	if err := h.create(grown); err != nil {
		return err
	}
	if err := h.serve(parent, append(append([]string{}, base...), grown), grown); err != nil {
		return err
	}
	s.Disk.Crash()
	s.Emit(Event{Kind: EventFault, Msg: "the host lost power"})
	h.volatile = false

	// The guest did not lose power with the host in this scenario's world — QEMU is
	// holding the same files it was holding a moment ago. Restating both chains says so,
	// and it is what the checker measures the next sweep against.
	if err := h.chain(parent, append(append([]string{}, base...), grown)); err != nil {
		return err
	}
	if err := h.chain(clone, append(append([]string{}, base[:3]...), cloneTip)); err != nil {
		return err
	}
	return h.sweep()
}

const sweepRoot = "/agent"

// sweepHost is one Agent's layers directory, its per-volume records and its pointers.
type sweepHost struct {
	s     *Sim
	paths simPaths
	// live is what QEMU has open for each volume this host holds, empty when no guest is
	// attached. It is stated by the scenario the way a Seal event is — noticing it is the
	// Manager's job, against a running QEMU this package deliberately does not model — and
	// it is the one thing that can prove a record is not older than what is running.
	live map[string]qcow.LiveImage
	// qmpDown makes the next answer about a guest an absence rather than an answer, which
	// is what a QMP socket that will not respond leaves the Manager holding.
	qmpDown  bool
	volatile bool
}

// create is a layer file appearing on disk, which is `qemu-img create` in production.
func (h *sweepHost) create(layerID string) error {
	return h.paths.WriteAtomic(qcow.LayerImage(sweepRoot, layerID), []byte("layer "+layerID))
}

// serve records that a volume's chain reads through these layers and points at that tip,
// in the order production writes them: the record first, so a pointer this host cannot
// account for is a fault and never a window (qcow.recordThenPoint).
func (h *sweepHost) serve(volumeID string, chain []string, tip string) error {
	st, err := qcow.ReadState(h.paths, sweepRoot, volumeID)
	if err != nil {
		return err
	}
	for _, id := range chain {
		st.ObserveTip(id)
	}
	if h.volatile {
		h.s.Disk.InjectSyncLoss(qcow.StateFile(sweepRoot, volumeID))
	}
	if err := qcow.WriteState(h.paths, sweepRoot, volumeID, st); err != nil {
		return err
	}
	// The pointer goes the same way when the fault is on, which is the shape that
	// matters: a record and a pointer that reverted *together* are consistent with each
	// other and a minute out of date, so nothing comparing the two can tell.
	if h.volatile {
		h.s.Disk.InjectSyncLoss(qcow.ActivePointer(sweepRoot, volumeID))
	}
	if err := qcow.SyncPointer(h.paths, sweepRoot, volumeID, qcow.LayerImage(sweepRoot, tip)); err != nil {
		return err
	}
	if h.live == nil {
		h.live = map[string]qcow.LiveImage{}
	}
	h.live[volumeID] = qcow.LiveImage{Path: qcow.LayerImage(sweepRoot, tip), Known: !h.qmpDown}
	return h.chain(volumeID, chain)
}

// chain states what one volume's guest reads through. It is the scenario's ledger and the
// only thing the checker measures a removal against — deliberately not the record, which
// is what the fault corrupts.
func (h *sweepHost) chain(volumeID string, layers []string) error {
	h.s.Emit(Event{Kind: EventChain, VolumeID: volumeID, Layers: layers,
		Msg: fmt.Sprintf("volume=%s reads through %v", volumeID, layers)})
	return nil
}

// sweep runs the production rule and reports what it removed.
//
// A refusal is not a scenario failure. The rule is allowed — required, even — to decline
// when it cannot enumerate what a volume holds, and after a power failure that is the
// answer it should reach; what it is never allowed to do is delete a file a guest is
// reading. So the error is noted and the run continues, and the checker judges the
// removals.
func (h *sweepHost) sweep() error {
	removed, err := qcow.Sweep(h.paths, sweepRoot, h.live)
	if err != nil {
		h.s.Notef("the sweep declined: %v", err)
		return nil
	}
	for _, path := range removed {
		h.s.Emit(Event{Kind: EventSweep, LayerID: qcow.LayerIDOfImage(path),
			Msg: "removed " + path})
	}
	return nil
}

// release is what reclaim does when the object store confirms the fleet granted a volume
// elsewhere: the record stops naming anything, and the files become collectable by the one
// rule that collects any file.
func (h *sweepHost) release(volumeID string) error {
	st, err := qcow.ReadState(h.paths, sweepRoot, volumeID)
	if err != nil {
		return err
	}
	if err := qcow.WriteState(h.paths, sweepRoot, volumeID, qcow.State{
		FormatVersion: st.FormatVersion, VolumeID: volumeID,
	}); err != nil {
		return err
	}
	if err := h.paths.Remove(qcow.ActivePointer(sweepRoot, volumeID)); err != nil {
		return err
	}
	// The guest went with the volume: a host that released a claim is not serving it.
	h.live[volumeID] = qcow.LiveImage{Known: true}
	return h.chain(volumeID, nil)
}
