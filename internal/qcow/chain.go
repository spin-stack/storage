// Package qcow owns a volume's local qcow2 chain: where it lives, how it is created,
// how an existing one is opened and checked, and which file is the tip QEMU writes to.
//
// # This process does not launch QEMU, and that is the design
//
// QEMU is the data path (v6 §4): it reads and writes the active image directly, keeps
// qcow2's semantics, and performs the guest's flushes. The Agent "prepares filesystems
// and directories, creates and opens qcow2 chains, **controls QEMU over QMP**" — v6 §4
// again, and §7 lists what that control is: flush the block devices, take the external
// snapshot, switch to the new tip, confirm it is being used. Not one of them starts a
// virtual machine. ADR-0021 says who does: spin's `cmd/runner`, "the long-lived per-host
// daemon that runs the QEMU VMs", which this system integrates into.
//
// So the Agent would have to acquire a second responsibility — VM supervision, with the
// process lifetime, the crash policy and the console that come with it — to launch QEMU
// itself, and it would be the second daemon on the host doing it. The pivot to qcow2
// exists to shed responsibilities, not to trade one for another.
//
// # The contract with whoever does launch it
//
// Two paths, and nothing else. Whoever boots the VM must give QEMU:
//
//   - the image at ActiveImage(root, volumeID) as the disk it writes to, and
//   - a QMP socket at QMPSocket(root, volumeID), server side.
//
// Both are derived from the volume's directory, so the launcher needs to know the data
// directory and the volume id and can compute the rest. That is the whole interface;
// it is small on purpose, because it spans two repositories.
//
// # The rule about offline tools
//
// v6 §5: the file QEMU is using is never processed with offline tools that could modify
// it, and §7 allows `create`, `info`, `check`, `convert` and `rebase` on images that are
// not in use. This package obeys something stricter and simpler to check: it runs
// `qemu-img` against a volume's active image **only while opening the chain**, before
// any VM of ours can be attached to it, and never again for as long as the volume is
// held. After that, every question about the live image goes to QEMU over QMP.
package qcow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/spin-stack/storage/internal/commit"
)

// ErrChainMismatch means the image on disk is not the one the catalog describes — a
// different virtual size, a different format, a header that says it is corrupt. It is a
// refusal and never a repair: the bytes belong to somebody's volume, and this system
// does not yet know whose.
var ErrChainMismatch = errors.New("qcow: the local image does not match the volume it is supposed to be")

// ErrChainMissing means this volume has published commits and this host holds none of
// them, and the rebuild that would have fetched them did not finish. It is a refusal and
// never a create.
//
// Creating a fresh empty layer for a volume that already has one is how a guest is handed
// a blank disk — the sentence readPointer already carries one branch below, where it was
// not true of the branch above it: a volume with no pointer was assumed to be a volume
// with no history, and nothing consulted the object store to find out.
var ErrChainMissing = errors.New("qcow: this volume has published commits and this host has no copy of them")

// ErrStaleChain means this host holds a chain the published history has moved past. It is
// separate from ErrChainMissing because the operator's next move is different and is not
// guessable: missing means look at the bucket, stale means two hosts have both served this
// volume and somebody has to decide which of the two histories is the one to keep.
var ErrStaleChain = errors.New("qcow: this host's chain is behind the published history")

// Recovery rebuilds a volume's published chain on this host.
//
// It is an interface here and not a bool the caller computes, because the question — may
// this volume be born empty? — must be impossible to skip. A bool is a thing a caller
// forgets to set, and the caller forgetting is the whole defect this guard closes.
//
// The bucket is the authority and not the catalog. `published_sequence` and
// `durable_sequence` on DesiredVolume are documented as exactly this fact and nothing in
// this system can advance either: a commit manifest has no sequence, and the only writer
// of those columns is the Agent's own report, which reports zero. A guard built on a
// column that is structurally zero for every volume in the fleet always answers "born
// empty" — the defect with a wire field in front of it.
type Recovery interface {
	// Restore rebuilds volumeID's published chain locally. A wrapped commit.ErrNoHead
	// means the volume has never published and an empty chain is correct; every other
	// error means refuse. commit.ErrNoHead is reused rather than a sentinel of our own
	// because the condition *is* "this volume has no HEAD".
	Restore(ctx context.Context, volumeID string, sizeBytes int64) (Restored, error)
	// Current is the commit the object store says is this volume's newest, wrapping
	// commit.ErrNoHead when it has never published. It is Restore's question without
	// Restore's work, and it is asked on every open of a chain that is already here:
	// a local chain is only current if the published history has not moved past it.
	Current(ctx context.Context, volumeID string) (string, error)
}

// Restored is what a rebuild leaves behind for Open to build the new tip over.
//
// It lives in this package rather than in the one that produces it because that package
// imports this one — it writes files where LayerImage says they go — and the reverse
// would be a cycle.
type Restored struct {
	// Base is the topmost restored layer; the new tip is created as an overlay over it.
	Base string
	// VirtualSize comes from the head commit and not from the catalog row. Where the two
	// disagree the commit is the one that describes the bytes.
	VirtualSize int64
	// HeadCommitID is the commit the chain was rebuilt to.
	HeadCommitID string
}

// ErrImageBusy means `qemu-img` could not open an image because a VM holds its write
// lock. It is information and not a fault: it is the strongest evidence available that a
// guest is running on this volume, arrived at from the one angle that cannot lie about it.
//
// It exists because the two ways of learning that disagree for a moment. QEMU is asked
// over QMP first, and every dial failure is indistinguishable from "no VM is attached" —
// so a socket that was momentarily unavailable sends this package down the offline branch
// and straight into the lock. Reading that as a broken image refuses a volume whose guest
// is, at that moment, writing to it. The volume is refused for this cycle, because
// nothing here can confirm *which* layer the VM has open, and retried on the next one —
// which is all the recovery this needs, since the socket is answering again by then.
var ErrImageBusy = errors.New("qcow: a VM has this image open, so no offline tool may look at it")

// writeLockRefusal is what the pinned qemu-img 11.1.1 prints when another process holds
// the image. Matching on it is matching on another program's message, which is fragile in
// exactly one direction: a wording change makes a busy image read as a broken one again,
// which is the behaviour this replaced and not something worse.
const writeLockRefusal = `Failed to get shared "write" lock`

// ErrForeignImage means a QEMU is attached at this volume's QMP socket with a different
// file open. Somebody is running a VM against an image this Agent did not prepare, at
// the socket path that is supposed to be this volume's, so neither party can be told
// which one is wrong from here.
var ErrForeignImage = errors.New("qcow: the QEMU at this volume's QMP socket has a different image open")

// The layout under the Agent's data directory.
//
//	volumes/<volume-id>/
//	├── layers/<layer-id>.qcow2   every tip this volume has ever had
//	├── active/current            one line: the absolute path of the tip
//	└── qmp.sock
//
// # A layer file is never renamed, never reused, and never means a second thing
//
// This is the one rule the layout exists to keep, and it was not free: v6 §5 drew the
// tree with a fixed `active/current.qcow2` and sealed layers moved to `sealed/<id>.qcow2`,
// which is what a person would draw. Rotating that tree means a path — the one QEMU was
// launched with — coming to mean a different file, and measuring it against a real QEMU
// showed what that costs:
//
//   - QEMU remembers the *string* it opened a node with, for ever. After a rotation that
//     reuses `active/current.qcow2`, the node holding the sealed layer still calls itself
//     `active/current.qcow2` — which now names the live tip. Anything that re-resolved it
//     would open the tip as its own backing.
//   - `query-block` stops answering with a path at all. It cannot render the graph as one
//     filename any more, so `file` comes back as `json:{"backing": ...}` — and this
//     Agent's one safety check on an attached VM is "is the file QEMU has open ours".
//     Rotation would have broken it, silently, in the direction of refusing good volumes.
//
// With id-named layers both problems are absent rather than handled: every path QEMU ever
// sees is a real file that will still be that file tomorrow, and `query-block` answers
// with it. §5's tree was corrected to this one.
//
// `active/current` is therefore not the image — it is a *pointer* to it, and it is the
// contract with whoever launches the VM: read the line, hand that path to QEMU. It is
// derived state, repaired from what QEMU says (Open), never trusted over it.
const (
	volumesDir  = "volumes"
	layersDir   = "layers"
	activeDir   = "active"
	pointerName = "current"
	layerSuffix = ".qcow2"
	// socketName is the QMP endpoint. It sits at the volume's root rather than under
	// `layers/` because it belongs to the *session*, not to any one tip: rotation
	// replaces the tip while the VM keeps running, and the socket must not move with it.
	socketName = "qmp.sock"
)

// VolumeDir is where everything belonging to one volume lives.
func VolumeDir(root, volumeID string) string {
	return filepath.Join(root, volumesDir, volumeID)
}

// LayersDir holds every layer of one volume, tip and sealed alike. Which one is the tip
// is not encoded in the directory, because that is the fact that changes.
func LayersDir(root, volumeID string) string {
	return filepath.Join(VolumeDir(root, volumeID), layersDir)
}

// LayerImage is one layer's file.
func LayerImage(root, volumeID, layerID string) string {
	return filepath.Join(LayersDir(root, volumeID), layerID+layerSuffix)
}

// LayerIDOfImage recovers a layer's id from its path. The id is in the filename because
// that is the only place a layer carries its own identity — the file is a qcow2 and has
// nowhere else to put one — and it is needed by anything that meets a layer without
// having been the thing that created it: a publish after a restart, a sweep, a recovery.
func LayerIDOfImage(path string) string {
	return strings.TrimSuffix(filepath.Base(path), layerSuffix)
}

// ActivePointer is the file naming the qcow2 QEMU should be launched against. Half the
// contract with whoever launches the VM; it holds one absolute path and no newline.
func ActivePointer(root, volumeID string) string {
	return filepath.Join(VolumeDir(root, volumeID), activeDir, pointerName)
}

// QMPSocket is where the Agent expects to find QEMU's QMP endpoint for this volume. The
// other half of the contract: QEMU creates it (`-qmp unix:<path>,server=on,wait=off`)
// and the Agent dials it.
func QMPSocket(root, volumeID string) string {
	return filepath.Join(VolumeDir(root, volumeID), socketName)
}

// Runner runs an external program and returns its standard output. `qemu-img` is
// reached through it, because v6 §7 forbids a qcow2 parser of our own — so every
// question about an offline image is another process's answer.
type Runner interface {
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
}

// Paths is the little of the filesystem a chain needs by absolute path: the
// directories it lives in, and whether the image is already there.
type Paths interface {
	MkdirAll(dir string) error
	Exists(path string) (bool, error)
	// Size is the space the file occupies, which for a qcow2 grows as the guest
	// allocates clusters. It is what the rotation threshold is measured against.
	Size(path string) (int64, error)
	ReadFile(path string) ([]byte, error)
	// WriteAtomic replaces the file's contents in one step, so a reader sees the old
	// path or the new one and never a truncated line. The pointer is read by another
	// process, at a moment this one does not choose.
	WriteAtomic(path string, data []byte) error
}

// Chain is one volume's local qcow2 chain: a stack of layers, the newest of which the
// guest writes to and all the others of which are complete and read-only.
//
// What this type is for is naming which file is the tip. Everything else about the chain
// — how deep it is, what backs what — lives in the qcow2 headers, which is the one place
// that cannot disagree with itself, and is read back with `qemu-img` when somebody needs
// it rather than tracked here.
type Chain struct {
	// Active is the tip QEMU writes to.
	Active string
	// Forked says this chain replaced one this host held before it lost the volume, so
	// everything the caller remembers about the old one is about a history that is not
	// this volume's any more. clearFork drops that on disk; this is what tells the
	// Manager to drop it in memory too — without it the same process went on to publish
	// the fork's sealed layer onto the rebuilt history, and only a restart made it stop.
	Forked bool
	// SizeBytes is the virtual size the image reports, read back from qemu-img rather
	// than remembered from what was asked for.
	SizeBytes int64
}

// imageInfo is the part of `qemu-img info --output=json` this package reads.
type imageInfo struct {
	Format      string `json:"format"`
	VirtualSize int64  `json:"virtual-size"`
	Filename    string `json:"filename"`
	// FullBackingFilename is the backing path as the header records it, resolved to an
	// absolute one. It is how a freshly created overlay is checked before QEMU is told
	// to write into it — see Rotate for why nothing later would catch it being wrong.
	FullBackingFilename string `json:"full-backing-filename"`
	Specific            struct {
		Data struct {
			Corrupt bool `json:"corrupt"`
		} `json:"data"`
	} `json:"format-specific"`
}

// OpenRequest is what Open needs to know about one volume.
type OpenRequest struct {
	Root     string
	VolumeID string
	// SizeBytes is the virtual size the catalog says this volume has.
	SizeBytes int64
	// LiveImage is the layer a running QEMU already has open, empty when none does.
	// It is an answer from QEMU, not a path this package composed.
	LiveImage string
	// NewLayerID names the layer this Open may have to create: the volume's first, or
	// the new tip over a chain that was just rebuilt from the object store.
	NewLayerID string
	// Recovery answers whether this volume has published commits, and puts them on this
	// disk when it has. It is required: an optional guard is not a guard.
	Recovery Recovery
}

// Open returns the chain for one volume, creating the first layer the first time.
//
// # QEMU's answer outranks the pointer
//
// A live image says a QEMU has that file open — the Agent restarted while the guest kept
// running — and it is taken as the tip even when `active/current` says otherwise, with
// the pointer repaired to match. The disagreement is a real state and not a corruption:
// a rotation writes the pointer before it tells QEMU to switch, so a crash in between
// leaves the pointer one layer ahead of the guest. Believing the pointer there would hand
// the next boot a file that is missing every write the guest has made since.
//
// An image in use is checked by nobody: `qemu-img` takes a lock and would fail, and
// forcing past that lock is the one thing v6 §5 forbids outright. QEMU opened the file,
// which is a stronger statement about it than any check made from here.
func Open(ctx context.Context, r Runner, p Paths, qemuImg string, req OpenRequest) (*Chain, error) {
	if req.Recovery == nil {
		// Before anything else, including the size check: a caller that did not wire a
		// recovery cannot tell a new volume from one whose data is in the object store,
		// and the only thing it could do for the second is hand a guest a blank disk.
		return nil, fmt.Errorf("%w: no recovery was wired, so volume %s cannot be asked about", ErrChainMissing, req.VolumeID)
	}
	if req.SizeBytes <= 0 {
		return nil, fmt.Errorf("%w: volume %s has a size of %d bytes", ErrChainMismatch, req.VolumeID, req.SizeBytes)
	}
	pointer := ActivePointer(req.Root, req.VolumeID)
	if req.LiveImage != "" {
		// A guest being attached is not a reason to skip the question. This branch
		// returned the live image straight away, on the argument that QEMU's answer
		// outranks the pointer — which it does, about *which file* is the tip, and says
		// nothing about whether that chain is still this volume's history. A host whose
		// guest never stopped is exactly the shape a fence leaves behind, so the branch
		// that skipped the check was the branch the check exists for.
		//
		// Nothing is rebuilt here and nothing is repaired: an image a guest holds cannot
		// be replaced under it. What a stale chain gets is a refusal, which is what stops
		// the layers being sealed and published into a history that has no room for them.
		if err := checkNotStale(ctx, p, req, req.LiveImage); err != nil {
			return nil, err
		}
		if err := syncPointer(p, pointer, req.LiveImage); err != nil {
			return nil, err
		}
		return &Chain{Active: req.LiveImage, SizeBytes: req.SizeBytes}, nil
	}

	if err := p.MkdirAll(LayersDir(req.Root, req.VolumeID)); err != nil {
		return nil, fmt.Errorf("qcow: making the layer directory for volume %s: %w", req.VolumeID, err)
	}
	image, err := readPointer(p, req.Root, req.VolumeID, pointer)
	if err != nil {
		return nil, err
	}
	local, err := ReadState(p, req.Root, req.VolumeID)
	if err != nil {
		return nil, fmt.Errorf("%w: volume %s: %w", ErrChainMissing, req.VolumeID, err)
	}
	// A chain this host gave up is not this volume's history any more, and the pointer
	// cannot say so: it is one line of text that survived the host losing the volume.
	// Between the loss and this grant another host held the volume, served it and
	// published — that is what a lease lapsing or a HEAD moving under us means — so the
	// bucket decides what the chain is, and the local layers are a fork of it.
	//
	// This is not the same question checkNotStale asks. That one compares HEAD with what
	// this host published and can only refuse; this one applies when the object store has
	// no opinion this host can be measured against, and it *repairs* — the volume has
	// been granted back at a higher epoch and a guest is waiting for a disk.
	if image != "" && local.Fenced != nil {
		chain, keepLocal, err := regrant(ctx, r, p, qemuImg, req, pointer, image, local)
		if err != nil {
			return nil, err
		}
		if !keepLocal {
			return chain, nil
		}
	}
	if image == "" {
		return born(ctx, r, p, qemuImg, req, pointer, local)
	}

	// A local chain is not the same thing as the current one. This host keeps its layers
	// when a volume leaves its desired state — deliberately, so that getting it back is
	// cheap — and in between, another host may have served it and published commits. The
	// chain on this disk is then a fork of the volume's history: complete, openable,
	// exactly what `qemu-img` would call sound, and missing everything the other host
	// wrote. Serving it hands the guest an older disk with no error anywhere, and the
	// first rotation seals a layer over it that the published history has no room for.
	if err := checkNotStale(ctx, p, req, image); err != nil {
		return nil, err
	}

	// The whole chain and not just the tip. `qemu-img info` exits 0 on an image whose
	// backing file is gone — format, virtual size and the corrupt flag all pass — so
	// this Agent would log "volume ready" and the failure would land on whoever
	// launches QEMU, as `Could not open backing file`. `--backing-chain` opens every
	// layer and exits 1 (both measured against the pinned 11.1.1).
	//
	// Rotate keeps plain `inspect` and must: the overlay it checks is backed by a tip a
	// live QEMU holds the write lock on, so a walk would fail on the lock and break
	// every rotation. This branch is the one place the image is known to be offline —
	// LiveImage == "" returned above — which is exactly where the walk is safe.
	chain, err := inspectChain(ctx, r, qemuImg, image)
	if err != nil {
		return nil, err
	}
	info := chain[0]
	for _, layer := range chain {
		if layer.Specific.Data.Corrupt {
			// The corrupt bit in the qcow2 header, which QEMU sets when it finds an
			// inconsistency it could not resolve. `qemu-img check` is deliberately not
			// run on every open: it walks every refcount in the image, so it costs time
			// proportional to the volume on every attach, and the header already carries
			// the verdict of whoever last had a reason to look. Checked on every layer
			// and not only the tip, because a guest reads through all of them.
			return nil, fmt.Errorf("%w: %s has the qcow2 corrupt flag set", ErrChainMismatch, layer.Filename)
		}
	}
	switch {
	case info.Format != "qcow2":
		return nil, fmt.Errorf("%w: %s is a %s image, not qcow2", ErrChainMismatch, image, info.Format)
	case info.VirtualSize != req.SizeBytes:
		// Not resized to match. The catalog and the image disagree about how big the
		// guest's disk is, and growing it here would hand a guest a device that changed
		// size behind its back on the strength of a row this Agent cannot verify.
		return nil, fmt.Errorf("%w: %s is %d bytes and the catalog says %d",
			ErrChainMismatch, image, info.VirtualSize, req.SizeBytes)
	}
	return &Chain{Active: image, SizeBytes: info.VirtualSize}, nil
}

// checkNotStale refuses a local chain the published history has moved past.
//
// The comparison is against this host's own record and not against the chain's depth,
// because depth cannot tell the two apart: a host that published three commits and a host
// that published one and was overtaken twice both hold layers. What distinguishes them is
// which commits *this host* put there, which is what state.json says.
//
// A volume that has never published is not stale, and neither is one whose HEAD names a
// commit this host published itself. Everything else is refused rather than rebuilt: a
// rebuild would silently discard whatever the guest wrote here since the fork, and the
// operator's next question — which of the two histories is the real one — is not one this
// Agent can answer alone.
func checkNotStale(ctx context.Context, p Paths, req OpenRequest, image string) error {
	head, err := req.Recovery.Current(ctx, req.VolumeID)
	switch {
	case errors.Is(err, commit.ErrNoHead):
		return nil
	case err != nil:
		return fmt.Errorf("%w: volume %s has a local chain and the object store could not say whether it is current: %w",
			ErrChainMissing, req.VolumeID, err)
	}
	local, err := ReadState(p, req.Root, req.VolumeID)
	if err != nil {
		return fmt.Errorf("%w: volume %s: %w", ErrChainMissing, req.VolumeID, err)
	}
	for _, c := range local.Commits {
		if c.CommitID == head {
			return nil
		}
	}
	return fmt.Errorf("%w: volume %s has a local chain at %s, and the published history is at commit %s, which this host did not write",
		ErrStaleChain, req.VolumeID, image, head)
}

// born is the branch for a volume with no local chain, and the one that used to lose
// data: it ran `qemu-img create` unconditionally, so a volume with published commits
// placed on a host that has never seen it got an empty qcow2 and no error anywhere.
//
// The bucket is asked first, and there are exactly three answers. No HEAD is the
// ordinary case — a volume created a second ago — and costs one request that never
// happens again, in a branch that was about to fork a process anyway. A rebuild that
// finished is a chain on this disk to overlay. Anything else is a refusal, including an
// unreachable bucket: objectstore separates "the key is not there" from "I could not
// look" precisely so that this line does not read the second as the first.
func born(ctx context.Context, r Runner, p Paths, qemuImg string, req OpenRequest, pointer string, local State) (*Chain, error) {
	restored, err := req.Recovery.Restore(ctx, req.VolumeID, req.SizeBytes)
	switch {
	case errors.Is(err, commit.ErrNoHead):
		// This host's own record outranks the bucket's answer, in exactly one direction
		// and only here. An Agent that published commits for this volume and is now
		// started against no object store — or against the wrong one, a typo, a staging
		// value, a bucket restored without its HEADs — asks a Recovery that answers "this
		// volume has never published", and would create a blank disk over a history it
		// knows about, because it wrote the file that says so. The same goes for a host
		// that was fenced: it held a chain a moment ago, so "never published" is an answer
		// about the store this binary was pointed at and not about the volume.
		//
		// The other direction is not symmetric: a state file that records nothing proves
		// nothing, because a host that has never seen the volume has no state file at all.
		// So this refuses and never permits.
		//
		// It is asked *after* Restore and not before, which is the order the earlier
		// version had wrong: a host that holds commits and a store that can rebuild the
		// chain is not a conflict at all, and refusing before the question was put made a
		// re-granted host with a published history permanently unserveable.
		if len(local.Commits) > 0 {
			return nil, fmt.Errorf("%w: volume %s has %d commits recorded on this host and no local chain, and the object store does not know them",
				ErrChainMissing, req.VolumeID, len(local.Commits))
		}
		if local.Fenced != nil {
			return nil, fmt.Errorf("%w: volume %s stopped being this host's at epoch %d and its local chain was set aside, and the object store says the volume has never published",
				ErrChainMissing, req.VolumeID, local.Fenced.Epoch)
		}
		image := LayerImage(req.Root, req.VolumeID, req.NewLayerID)
		if _, err := r.Run(ctx, qemuImg, "create", "-f", "qcow2", image, fmt.Sprint(req.SizeBytes)); err != nil {
			return nil, fmt.Errorf("qcow: creating %s: %w", image, err)
		}
		if err := writePointer(p, pointer, image); err != nil {
			return nil, err
		}
		return &Chain{Active: image, SizeBytes: req.SizeBytes}, nil
	case err != nil:
		return nil, fmt.Errorf("%w: volume %s: %w", ErrChainMissing, req.VolumeID, err)
	}

	return overlay(ctx, r, p, qemuImg, req, pointer, restored)
}

// regrant decides what a host that lost this volume and has been granted it back serves.
//
// The local pointer cannot answer it: it is one line of text that survived the host
// losing the volume, and between the loss and this grant another host may have held the
// volume, served it and published — which is what a lease lapsing or a HEAD moving under
// us means. So the object store is asked, with the question that can also act on the
// answer.
//
// Restore and not Current, and the difference is the whole decision. Current asks whether
// the local chain is behind, which can only end in a refusal; Restore asks the store to
// put the volume's history on this disk, and a host with a guest waiting needs the second.
// Its ErrNoHead is the case that keeps the local chain: nothing was ever published, by
// this host or by whoever had the volume in between, so the layers here are the only
// history this volume has and replacing them with an empty image is the blank-disk defect
// with a fence in front of it.
//
// keepLocal true means the caller carries on with the chain that is already here.
func regrant(ctx context.Context, r Runner, p Paths, qemuImg string, req OpenRequest, pointer, image string, local State) (chain *Chain, keepLocal bool, err error) {
	restored, err := req.Recovery.Restore(ctx, req.VolumeID, req.SizeBytes)
	switch {
	case errors.Is(err, commit.ErrNoHead):
		slog.Info("this volume was granted back to this host and the object store holds no history for it, so the local chain is the only one there is",
			"volume_id", req.VolumeID, "tip", image, "fenced_at_epoch", local.Fenced.Epoch)
		if err := clearFence(p, req.Root, req.VolumeID); err != nil {
			return nil, false, err
		}
		return nil, true, nil
	case err != nil:
		return nil, false, fmt.Errorf("%w: volume %s was granted back to this host and its published history could not be rebuilt: %w",
			ErrChainMissing, req.VolumeID, err)
	}
	slog.Warn("this host was not this volume's writer and has been granted it again: the local chain is a fork of the published history, and the object store's copy is the one served",
		"volume_id", req.VolumeID, "local_tip", image, "head_commit_id", restored.HeadCommitID,
		"fenced_at_epoch", local.Fenced.Epoch, "why", local.Fenced.Detail)
	chain, err = overlay(ctx, r, p, qemuImg, req, pointer, restored)
	if err != nil {
		return nil, false, err
	}
	if err := clearFork(p, req.Root, req.VolumeID); err != nil {
		return nil, false, err
	}
	chain.Forked = true
	return chain, false, nil
}

// overlay puts this host's new tip on top of a chain the object store just rebuilt.
func overlay(ctx context.Context, r Runner, p Paths, qemuImg string, req OpenRequest, pointer string, restored Restored) (*Chain, error) {
	// An overlay over the rebuilt chain, never a flatten. An overlay is O(1) — measured,
	// 0.00 s and 193 KiB over a four-layer gigabyte — and a flatten is a second full pass
	// over every byte the recovery just wrote, at twice the peak disk. §19 already owns
	// flattening, as compaction, outside the recovery path.
	//
	// It is not published either: v6 §11's "an idle volume does not commit" means this
	// layer becomes a commit only when a rotation seals it, and commit.Publish reads HEAD
	// itself, so its parent is automatically the commit the chain was rebuilt to.
	tip := LayerImage(req.Root, req.VolumeID, req.NewLayerID)
	if _, err := r.Run(ctx, qemuImg, "create", "-f", "qcow2", "-b", restored.Base, "-F", "qcow2",
		"-u", tip, fmt.Sprint(restored.VirtualSize)); err != nil {
		return nil, fmt.Errorf("qcow: creating the new tip %s over the recovered chain %s: %w", tip, restored.Base, err)
	}
	// The same two assertions Rotate makes, for the same reason: qemu-img accepts a
	// wrong-but-existing backing in silence, and a guest then runs perfectly on somebody
	// else's history until it stops. Plain `inspect` and not the walk — recovery has
	// already walked the layers under this one, offline, and asserted that the chain
	// resolves to as many layers as the history has commits.
	info, err := inspect(ctx, r, qemuImg, tip)
	if err != nil {
		return nil, err
	}
	switch {
	case info.VirtualSize != restored.VirtualSize:
		return nil, fmt.Errorf("%w: the new tip %s is %d bytes and the head commit says %d",
			ErrChainMismatch, tip, info.VirtualSize, restored.VirtualSize)
	case filepath.Clean(info.FullBackingFilename) != filepath.Clean(restored.Base):
		return nil, fmt.Errorf("%w: the new tip %s is backed by %q, not by the recovered chain %q",
			ErrChainMismatch, tip, info.FullBackingFilename, restored.Base)
	}
	if err := writePointer(p, pointer, tip); err != nil {
		return nil, err
	}
	return &Chain{Active: tip, SizeBytes: restored.VirtualSize}, nil
}

// clearFence forgets that this host was fenced, for the volume it has been granted back
// and whose local chain it is keeping. Only the fence: the layer list and the pending
// commit describe the chain that is still being served.
func clearFence(p Paths, root, volumeID string) error {
	st, err := ReadState(p, root, volumeID)
	if err != nil {
		return fmt.Errorf("%w: volume %s: %w", ErrChainMissing, volumeID, err)
	}
	st.Fenced = nil
	return WriteState(p, root, volumeID, st)
}

// clearFork forgets what this host knew about the chain a re-derived volume replaced.
//
// Three facts go, and each of them would be a lie about the new chain: the fence, because
// the volume is this host's again and the record is what would refuse it next cycle; the
// layer list, because those layers are a fork the tip no longer sits on; and the pending
// commit, because publishing it now would put a layer into the history whose bytes the
// chain being served does not contain. That last one is a sealed layer being dropped, and
// it is logged with its ids for exactly that reason — the alternative is a commit that
// reconstructs to something no guest ever saw.
//
// Commits is kept. Those are commits whose layers this host still holds, the restore just
// added to them, and they are what lets the next rebuild skip a download.
//
// The state is read again rather than reused: the restore that ran a moment ago appended
// to this same file.
func clearFork(p Paths, root, volumeID string) error {
	st, err := ReadState(p, root, volumeID)
	if err != nil {
		return fmt.Errorf("%w: volume %s: %w", ErrChainMissing, volumeID, err)
	}
	if st.Pending != nil {
		slog.Warn("dropping a sealed layer that was never published: it belongs to the chain this host held before it lost the volume, and the published history has moved on without it",
			"volume_id", volumeID, "commit_id", st.Pending.CommitID, "layer_id", st.Pending.LayerID,
			"layer", LayerImage(root, volumeID, st.Pending.LayerID))
	}
	st.Fenced, st.Layers, st.Pending = nil, nil, nil
	return WriteState(p, root, volumeID, st)
}

// Rotate seals the tip and puts a new empty layer on top of it, with the guest writing
// throughout. It is v6 §23.2, and after it returns the previous tip is complete: nothing
// will ever write to that file again, which is what makes it something a commit can
// upload.
//
// # The order is the design, and every other order loses data
//
//  1. Create the new layer, with its backing path written by us.
//  2. Read it back and check that path, before QEMU has it.
//  3. Point `active/current` at it.
//  4. Tell QEMU to switch (blockdev-snapshot-sync, mode=existing).
//
// Step 3 comes before step 4 and not after. Whichever way round they go there is a window
// where a crash leaves the two disagreeing, and the question is only what a VM relaunched
// in that window opens. Pointer first, it opens the new layer: an empty overlay over
// everything the guest wrote, which is correct. Pointer last, it opens the layer that is
// *already the backing of the live tip* and writes into it — a second writer under a file
// QEMU is reading through, which corrupts the chain with no error anywhere. The Agent
// converges out of the first window on its next cycle (Open); there is no converging out
// of the second.
//
// Step 2 is not defensive. QEMU does not check that an overlay's recorded backing is the
// node it attaches — that is exactly what lets step 1 name a file by a path QEMU never
// used — so a wrong backing path is invisible for the entire life of the VM and wrong on
// the next boot, when a guest gets somebody else's disk or a shorter one. The moment it
// can still be caught is before step 4.
//
// The new layer is created with `-u`: qemu-img is told not to open the backing file. It
// cannot, because QEMU holds its write lock, and this is the one place where "unsafe"
// means "does not consult a file that is already known to be busy".
func (c *Chain) Rotate(ctx context.Context, r Runner, p Paths, qemuImg, root, volumeID, layerID string, switchTo func(newTip string) error) (sealed string, err error) {
	next := LayerImage(root, volumeID, layerID)
	exists, err := p.Exists(next)
	if err != nil {
		return "", fmt.Errorf("qcow: looking for %s: %w", next, err)
	}
	if exists {
		// A layer id that has been used before. Overwriting it would silently discard
		// whatever a previous rotation left there, and every id this Agent generates is
		// a v7 UUID, so this cannot happen by chance — only by a caller reusing one.
		return "", fmt.Errorf("%w: layer %s already exists", ErrChainMismatch, next)
	}
	if _, err := r.Run(ctx, qemuImg, "create", "-f", "qcow2", "-b", c.Active, "-F", "qcow2",
		"-u", next, fmt.Sprint(c.SizeBytes)); err != nil {
		return "", fmt.Errorf("qcow: creating the next layer %s over %s: %w", next, c.Active, err)
	}
	info, err := inspect(ctx, r, qemuImg, next)
	if err != nil {
		return "", err
	}
	switch {
	case info.VirtualSize != c.SizeBytes:
		return "", fmt.Errorf("%w: the new layer %s is %d bytes and the chain is %d",
			ErrChainMismatch, next, info.VirtualSize, c.SizeBytes)
	case filepath.Clean(info.FullBackingFilename) != filepath.Clean(c.Active):
		return "", fmt.Errorf("%w: the new layer %s is backed by %q, not by the tip %q",
			ErrChainMismatch, next, info.FullBackingFilename, c.Active)
	}
	if err := writePointer(p, ActivePointer(root, volumeID), next); err != nil {
		return "", err
	}
	if err := switchTo(next); err != nil {
		return "", err
	}
	sealed, c.Active = c.Active, next
	return sealed, nil
}

// readPointer returns the tip `active/current` names, or "" if the volume has none yet.
func readPointer(p Paths, root, volumeID, pointer string) (string, error) {
	exists, err := p.Exists(pointer)
	if err != nil {
		return "", fmt.Errorf("qcow: looking for %s: %w", pointer, err)
	}
	if !exists {
		return "", nil
	}
	body, err := p.ReadFile(pointer)
	if err != nil {
		return "", fmt.Errorf("qcow: reading %s: %w", pointer, err)
	}
	image := strings.TrimSpace(string(body))
	// A layer of *this* volume, not any layer. `active/current` is the one local file
	// with no identity of its own — a qcow2 carries its own header, a manifest names its
	// volume, and this is one line of text that a data directory restored from a backup
	// of another volume, or copied when a volume was re-placed, leaves pointing at
	// somebody else's tip. Booting it is another tenant's disk served under this
	// volume's name, and every layer this host then seals over it is theirs too.
	// Cleaned before the prefix is compared, and that is the whole of it: a pointer
	// naming `<this volume>/layers/../../<other volume>/layers/x.qcow2` starts with the
	// right prefix as a *string* and resolves to somebody else's tip. Every path here is
	// handed to another process, which resolves it, so the comparison has to resolve it
	// too.
	image = filepath.Clean(image)
	if !strings.HasPrefix(image, filepath.Clean(LayersDir(root, volumeID))+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: %s names %q, which is not a layer of volume %s",
			ErrChainMismatch, pointer, image, volumeID)
	}
	if !filepath.IsAbs(image) {
		// Including the empty string, which is what a pointer truncated by a crash
		// looks like. Refused rather than treated as "no volume yet": creating a fresh
		// empty layer for a volume that has one is how a guest is handed a blank disk.
		return "", fmt.Errorf("%w: %s names %q, which is not an absolute path", ErrChainMismatch, pointer, image)
	}
	return image, nil
}

// SyncPointer makes `active/current` name the layer a guest is actually writing to,
// leaving the file alone when it already does.
//
// It is called on every cycle a VM is attached, and reading before writing is not an
// optimisation: writing is fsync of the file and of its directory, and doing that per
// volume per heartbeat for a fact that changes once a rotation would be real I/O bought
// for nothing.
//
// What it converges is the window Rotate opens deliberately. The pointer moves before
// QEMU is told to switch, so a snapshot that fails — or one whose answer never came
// back, which is the case nobody can tell apart from it — leaves the pointer one layer
// ahead of the guest. It is left ahead rather than put back, because "the command
// failed" and "the answer was lost after it took effect" look the same from here and
// only one of those is safe to undo. QEMU is asked next cycle and it settles it.
func SyncPointer(p Paths, root, volumeID, image string) error {
	return syncPointer(p, ActivePointer(root, volumeID), image)
}

func syncPointer(p Paths, pointer, image string) error {
	exists, err := p.Exists(pointer)
	if err != nil {
		return fmt.Errorf("qcow: looking for %s: %w", pointer, err)
	}
	if exists {
		body, err := p.ReadFile(pointer)
		if err != nil {
			return fmt.Errorf("qcow: reading %s: %w", pointer, err)
		}
		if strings.TrimSpace(string(body)) == image {
			return nil
		}
	}
	return writePointer(p, pointer, image)
}

// writePointer publishes which layer the VM is to be launched against.
func writePointer(p Paths, pointer, image string) error {
	if err := p.MkdirAll(filepath.Dir(pointer)); err != nil {
		return fmt.Errorf("qcow: making the directory for %s: %w", pointer, err)
	}
	if err := p.WriteAtomic(pointer, []byte(image)); err != nil {
		return fmt.Errorf("qcow: writing %s: %w", pointer, err)
	}
	return nil
}

// inspectChain runs `qemu-img info --backing-chain`, which answers with an array — the
// image first, then every layer under it — and fails outright when a link is missing.
func inspectChain(ctx context.Context, r Runner, qemuImg, image string) ([]imageInfo, error) {
	out, err := r.Run(ctx, qemuImg, "info", "--output=json", "--backing-chain", image)
	if err != nil {
		if strings.Contains(err.Error(), writeLockRefusal) {
			return nil, fmt.Errorf("%w: %s, so the chain under it cannot be walked from here; the VM's QMP socket did not answer this cycle: %w",
				ErrImageBusy, image, err)
		}
		return nil, fmt.Errorf("qcow: walking the backing chain of %s (a layer under it is missing): %w", image, err)
	}
	var chain []imageInfo
	if err := json.Unmarshal(out, &chain); err != nil {
		return nil, fmt.Errorf("qcow: decoding qemu-img info for the chain of %s: %w", image, err)
	}
	if len(chain) == 0 {
		return nil, fmt.Errorf("%w: qemu-img walked no layers at all for %s", ErrChainMismatch, image)
	}
	return chain, nil
}

// inspect runs `qemu-img info` on an offline image.
func inspect(ctx context.Context, r Runner, qemuImg, image string) (imageInfo, error) {
	out, err := r.Run(ctx, qemuImg, "info", "--output=json", image)
	if err != nil {
		// The likeliest failure here is not a broken image: it is `Failed to get shared
		// "write" lock`, meaning a QEMU has the file open that this Agent could not
		// reach over QMP. Saying so is the difference between an operator checking the
		// socket path they launched the VM with and an operator running a repair on a
		// perfectly good image.
		if strings.Contains(err.Error(), writeLockRefusal) {
			return imageInfo{}, fmt.Errorf("%w: %s; the VM's QMP socket did not answer this cycle: %w", ErrImageBusy, image, err)
		}
		return imageInfo{}, fmt.Errorf("qcow: inspecting %s: %w", image, err)
	}
	var info imageInfo
	if err := json.Unmarshal(out, &info); err != nil {
		return imageInfo{}, fmt.Errorf("qcow: decoding qemu-img info for %s: %w", image, err)
	}
	return info, nil
}
