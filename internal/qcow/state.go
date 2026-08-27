package qcow

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/spin-stack/storage/internal/framed"
)

// stateName is the volume's durable record of what this host knows about its own
// published history. It sits beside `active/` rather than under `layers/` because it is
// about the volume and not about any one layer, and because everything else in this
// volume's layout is already spelled by this package — a second package computing a
// path under the data directory is the `--data-dir applied twice` defect waiting to
// happen again.
const stateName = "state.json"

// StateFile is where one volume's state record lives.
func StateFile(root, volumeID string) string {
	return filepath.Join(VolumeDir(root, volumeID), stateName)
}

// ErrBadState means the state file cannot be believed: it disagrees with its own
// digest, it does not decode, it was written by a format this binary does not know, or
// it describes another volume.
//
// It is one sentinel over four causes because the caller's move is the same for all of
// them and must not be "carry on with the zero value". A State this host cannot read is
// a host that does not know whether it owes the object store a sealed layer and does not
// know which layers it already holds; acting on a blank one republishes a layer under a
// second commit id and re-downloads a chain that is already on the disk. The specific
// cause is wrapped alongside, so an operator still gets `framed.ErrCorrupt` or
// `framed.ErrFormatTooNew` in the message and a test can branch on either.
//
// Rejected: aliasing framed.ErrCorrupt the way descriptor.ErrCorruptDescriptor does.
// That covers a flipped bit and not a state file restored under the wrong volume, whose
// digest is intact and whose contents are somebody else's — and the second is the one
// that makes this host skip a download it needed.
var ErrBadState = errors.New("qcow: this volume's state.json cannot be believed")

// State is what this host durably knows about a volume's published history. It is
// derived state — the object store is the authority, and a lost state file costs work
// and never correctness — but it is the only place two facts can live at all:
//
//   - which commit a local layer file came from, and
//   - which commit id a sealed-but-unpublished layer was already promised under.
//
// It holds no guest data, only structural metadata, so it stays in the clear (v6 §15.3,
// the same call descriptor.json makes).
//
// # Two writers, one file
//
// Manager.rotate records a pending commit and Manager.publish clears it; recovery
// appends to Commits as it fetches. All of them run under Manager.mu — Apply holds it
// across ensure, which is what opens a chain and what a restore runs inside — so this
// type carries no lock of its own. A second process writing this file is not a case
// this design has: one Agent owns a data directory, enforced by the lock New takes.
type State struct {
	// FormatVersion is framed.FormatVersion at the time the file was written. First, so
	// that a human catting the file sees it before anything else.
	FormatVersion int    `json:"format_version"`
	VolumeID      string `json:"volume_id"`
	// Commits is every published commit whose layer this host holds, oldest first in
	// the order it was learned. It is what lets a same-host recovery skip a download,
	// and it has to be durable to do that: `qemu-img rebase -u` rewrites a layer's
	// header, so a layer that has been repointed at a local parent no longer hashes to
	// the object it was fetched from and can never again be checked against its
	// manifest. This record is then the only thing that can vouch for the file.
	Commits []CommitLayer `json:"commits,omitempty"`
	// Pending is the sealed layer this host owes the object store, nil when it owes
	// none. At most one ever exists (v6 §11: nothing rotates while a sealed layer is
	// unpublished), which is why it is one value and not a queue.
	//
	// It is an optimisation and not the authority. What a layer *is* — sealed, tip, or
	// published — is derived from Layers, Commits and the tip QEMU has open; this record
	// only carries the commit id the layer was already promised under, so that a retry
	// after a restart is the same commit rather than a second one. A lost Pending costs a
	// duplicate id and never a lost layer; see sealedBelow.
	Pending *PendingCommit `json:"pending,omitempty"`
	// Layers is every layer this host has observed as this volume's tip, newest first.
	//
	// It exists so that "sealed and not published" can be *derived* rather than recorded
	// at the moment of sealing. A layer is sealed exactly when it is no longer the tip,
	// and the tip is observed — from the QEMU that has it open, or from `active/current`
	// — so the sealed set is (this list) − (the tip) − (Commits), and no record has to
	// survive the microseconds between blockdev-snapshot-sync returning and a write to
	// this file.
	//
	// Recording the *new* tip is what makes that safe, and it is the opposite of
	// recording the sealed one: the entry is written on an ordinary cycle, after the
	// switch, by the code that observes which file QEMU actually has open. A crash that
	// loses the entry loses nothing — the next cycle observes the same tip and writes it
	// again — while a record of what was sealed, written before the switch, would name a
	// file QEMU is still writing into.
	//
	// It is trimmed at the newest published layer (see trimLayers): everything under a
	// published commit is published, so nothing older is ever a question.
	Layers []string `json:"layers,omitempty"`
	// Fenced, when set, is this host's own record that it stopped being this volume's
	// writer while it believed it was one — HEAD moved under it, or its lease lapsed.
	//
	// It is durable because the statement is about the volume and not about a process:
	// PUBLISH_FENCED lived in a field of an in-memory struct, so an OOM, a deploy or a
	// SIGKILL turned "another host owns this volume" into "this volume looks fine", and
	// the restarted Agent resumed the guest's disk and published again. Nothing in the
	// fleet corrects that — the refusal is a column nobody reconciles — so the fact has
	// to outlive the process that learned it.
	Fenced *Fencing `json:"fenced,omitempty"`
}

// Fencing is the moment this host stopped being a volume's writer.
type Fencing struct {
	// Epoch is the epoch this host held when it happened. It is what clears the record:
	// a *higher* epoch is the fleet granting the volume to this host again, and nothing
	// else — not a restart, not the socket coming back, not the object store answering.
	Epoch int64 `json:"epoch"`
	// Refusal is the storagev1.VolumeRefusal value as a number, and Detail is what an
	// operator reads. The enum is stored as an int rather than imported here because
	// this file is the on-disk format and a proto name that is renamed later must not
	// change how an existing state.json decodes.
	Refusal int32  `json:"refusal"`
	Detail  string `json:"detail,omitempty"`
}

// CommitLayer is one published commit and the local file that belongs to it.
type CommitLayer struct {
	CommitID string `json:"commit_id"`
	LayerID  string `json:"layer_id"`
}

// PendingCommit is a layer that was sealed here and has not been published.
//
// Recording it is what stops a restart between sealing and publishing from minting a
// second commit id for the same bytes: the restarted Agent republishes under the id
// written here, commit.Publish recognises its own retry (a byte-identical manifest under
// a create-only PUT), and the history gets one entry instead of two.
//
// A window remains, between blockdev-snapshot-sync returning and this file landing.
// Writing the record *before* the QMP switch was considered and rejected: the layer is
// still the live tip at that moment, so a restart inside that window would publish a
// file QEMU is writing into — a corrupt commit, where what is kept is a duplicate entry
// in a history. The window stays on purpose, and it is microseconds wide.
//
// It carries no path. The file is LayerImage(root, volumeID, LayerID), and a stored path
// is a second spelling of the layout that can disagree with the first.
type PendingCommit struct {
	CommitID string `json:"commit_id"`
	LayerID  string `json:"layer_id"`
	// Epoch is the fencing token this host held when it sealed the layer, carried so
	// that a publish after a restart states the epoch the bytes were written under and
	// not the one this process happens to hold now.
	Epoch int64 `json:"epoch"`
	// PlainBytes is the sealed file's length; VirtualSize is the guest-visible size of
	// the volume the commit reconstructs.
	PlainBytes  int64 `json:"plain_bytes"`
	VirtualSize int64 `json:"virtual_size"`
}

// MarshalState returns the bytes to store: the JSON, framed by internal/framed — a
// digest line over the bytes as stored, then the payload.
//
// framed is reused rather than a second integrity primitive written here, for the reason
// that package states: two copies of a rule drift. It also carries why the digest is
// over the bytes as stored and not a field inside the JSON — Go's decoder matches field
// names case-insensitively, so a flipped bit in a key re-marshals to the same digest.
func MarshalState(s State) ([]byte, error) {
	// Stamped over whatever the caller put here rather than trusted: a State built by
	// hand carries a zero, and the whole point of the field is that it cannot be absent.
	// `s` is this function's own copy, so the caller's value is untouched.
	s.FormatVersion = framed.FormatVersion
	body, err := json.Marshal(s)
	if err != nil {
		return nil, fmt.Errorf("qcow: encoding the state of volume %s: %w", s.VolumeID, err)
	}
	return framed.Frame(body), nil
}

// UnmarshalState checks the digest, decodes, checks the version, and only then lets a
// caller read a field.
//
// The order is the whole of it. json.Unmarshal silently discards fields it does not
// know, so a state file from a newer format decodes without complaint into whatever
// subset this binary understands — a pending commit whose new field this build never
// heard of, published anyway. The version check is what turns that into a refusal.
func UnmarshalState(volumeID string, body []byte) (State, error) {
	payload, err := framed.Unframe(body)
	if err != nil {
		return State{}, fmt.Errorf("%w: %s: %w", ErrBadState, stateName, err)
	}
	var s State
	if err := json.Unmarshal(payload, &s); err != nil {
		return State{}, fmt.Errorf("%w: decoding %s of volume %s: %w", ErrBadState, stateName, volumeID, err)
	}
	if err := framed.CheckVersion(s.FormatVersion); err != nil {
		return State{}, fmt.Errorf("%w: %s of volume %s: %w", ErrBadState, stateName, volumeID, err)
	}
	// The file must describe the volume it was asked for. The digest proves the bytes
	// are the bytes that were written and says nothing about *where*: a state file
	// copied or restored under another volume's directory passes it intact, and every
	// commit it names is then credited to this volume — which is one download skipped
	// for a layer this host does not have, and a chain that resolves to a hole.
	// descriptor.Read and commit.ReadManifest make the same check for the same reason.
	if s.VolumeID != volumeID {
		return State{}, fmt.Errorf("%w: the %s under volume %s describes volume %s",
			ErrBadState, stateName, volumeID, s.VolumeID)
	}
	return s, nil
}

// ReadState loads a volume's state record. A volume with no file yet — every volume
// before its first rotation — reads as the zero State and no error, because "this host
// holds nothing and owes nothing" is exactly true of it.
//
// A filesystem that could not be examined is an error and never an absent file: "I could
// not look" and "there is nothing there" lead to opposite decisions about a sealed layer
// somebody's guest wrote, and collapsing them would silently take the wrong one.
func ReadState(p Paths, root, volumeID string) (State, error) {
	file := StateFile(root, volumeID)
	exists, err := p.Exists(file)
	if err != nil {
		return State{}, fmt.Errorf("qcow: looking for %s: %w", file, err)
	}
	if !exists {
		return State{}, nil
	}
	body, err := p.ReadFile(file)
	if err != nil {
		return State{}, fmt.Errorf("qcow: reading %s: %w", file, err)
	}
	s, err := UnmarshalState(volumeID, body)
	if err != nil {
		return State{}, fmt.Errorf("%s: %w", file, err)
	}
	return s, nil
}

// WriteState replaces the volume's state record in one step.
//
// Atomic because of what reads it: this host, after a crash, deciding whether it owes
// the object store a layer. A half-written record read as a whole one is a sealed layer
// nobody publishes or a commit id nobody can reproduce, and both are silent. Paths
// already fsyncs the file and its directory before the rename, which is the same recipe
// the pointer gets and for the same reason.
func WriteState(p Paths, root, volumeID string, s State) error {
	// Stamped from the directory the file is going into, not trusted from the value.
	// The identity check on the way back in is only worth something if the two can
	// never be written apart in the first place.
	s.VolumeID = volumeID
	body, err := MarshalState(s)
	if err != nil {
		return err
	}
	file := StateFile(root, volumeID)
	// Same as writePointer: WriteAtomic creates its temporary file in the target
	// directory, so a missing directory surfaces as a failure to write the state rather
	// than as the missing directory it is.
	if err := p.MkdirAll(filepath.Dir(file)); err != nil {
		return fmt.Errorf("qcow: making the directory for %s: %w", file, err)
	}
	if err := p.WriteAtomic(file, body); err != nil {
		return fmt.Errorf("qcow: writing %s: %w", file, err)
	}
	return nil
}

// recordTip notes that `layerID` is the volume's tip, and reports whether that was news.
//
// The caller writes the file only when it was, because this runs once per volume per
// reconciliation cycle and the fact changes once per rotation: an fsync of the file and
// of its directory per heartbeat, for a line that is already there, is real I/O bought
// for nothing. The same reasoning SyncPointer carries.
func (s *State) recordTip(layerID string) bool {
	for _, id := range s.Layers {
		if id == layerID {
			return false
		}
	}
	s.Layers = append([]string{layerID}, s.Layers...)
	return true
}

// sealedBelow is every layer this host has sealed and not published, oldest first.
//
// It is derived and not looked up, which is the whole point: a layer is sealed exactly
// when it stops being the tip, and `tip` is an observation — what QEMU has open, or what
// `active/current` names — so a crash between the QMP switch and any write to this file
// cannot hide a complete layer full of the guest's writes. It used to: the sealed layer
// was known only from Pending, the next commit chained past it, and the published history
// got a hole in it that nothing anywhere reported.
//
// The walk stops at the first published layer because everything under one is published
// too — a commit's manifest covers its whole backing chain — so the list is short (v6 §11
// keeps it at one, except after a crash or a spell with no publisher).
//
// Layers above the tip are skipped rather than sealed. That is the other window Rotate
// opens on purpose: the pointer moves before QEMU is told to switch, so a new layer can
// be recorded that the guest never moved to. QEMU's answer decides which file is live,
// and this never returns it.
func (s State) sealedBelow(tip string) []string {
	published := make(map[string]bool, len(s.Commits))
	for _, c := range s.Commits {
		published[c.LayerID] = true
	}
	var out []string
	below := false
	for _, id := range s.Layers {
		switch {
		case id == tip:
			below = true
		case !below:
			continue
		case published[id]:
			return reversed(out)
		default:
			out = append(out, id)
		}
	}
	return reversed(out)
}

// trimLayers drops everything under a published layer, which bounds Layers: the newest
// published commit is a floor nothing older can ever be a question about again.
func (s *State) trimLayers(published string) {
	for i, id := range s.Layers {
		if id == published {
			s.Layers = s.Layers[:i+1]
			return
		}
	}
}

// reversed returns the list oldest-first. Layers is stored newest-first because that is
// the end that changes, and publishing goes the other way: a commit's parent must already
// be in the history when it lands.
func reversed(in []string) []string {
	out := make([]string, 0, len(in))
	for i := len(in) - 1; i >= 0; i-- {
		out = append(out, in[i])
	}
	return out
}
