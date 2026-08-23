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
	"path/filepath"
	"strings"
)

// ErrChainMismatch means the image on disk is not the one the catalog describes — a
// different virtual size, a different format, a header that says it is corrupt. It is a
// refusal and never a repair: the bytes belong to somebody's volume, and this system
// does not yet know whose.
var ErrChainMismatch = errors.New("qcow: the local image does not match the volume it is supposed to be")

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
	// NewLayerID names the volume's first layer, used only when it has none yet.
	NewLayerID string
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
	if req.SizeBytes <= 0 {
		return nil, fmt.Errorf("%w: volume %s has a size of %d bytes", ErrChainMismatch, req.VolumeID, req.SizeBytes)
	}
	pointer := ActivePointer(req.Root, req.VolumeID)
	if req.LiveImage != "" {
		if err := syncPointer(p, pointer, req.LiveImage); err != nil {
			return nil, err
		}
		return &Chain{Active: req.LiveImage, SizeBytes: req.SizeBytes}, nil
	}

	if err := p.MkdirAll(LayersDir(req.Root, req.VolumeID)); err != nil {
		return nil, fmt.Errorf("qcow: making the layer directory for volume %s: %w", req.VolumeID, err)
	}
	image, err := readPointer(p, pointer)
	if err != nil {
		return nil, err
	}
	if image == "" {
		image = LayerImage(req.Root, req.VolumeID, req.NewLayerID)
		if _, err := r.Run(ctx, qemuImg, "create", "-f", "qcow2", image, fmt.Sprint(req.SizeBytes)); err != nil {
			return nil, fmt.Errorf("qcow: creating %s: %w", image, err)
		}
		if err := writePointer(p, pointer, image); err != nil {
			return nil, err
		}
		return &Chain{Active: image, SizeBytes: req.SizeBytes}, nil
	}

	info, err := inspect(ctx, r, qemuImg, image)
	if err != nil {
		return nil, err
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
	case info.Specific.Data.Corrupt:
		// The corrupt bit in the qcow2 header, which QEMU sets when it finds an
		// inconsistency it could not resolve. `qemu-img check` is deliberately not run
		// on every open: it walks every refcount in the image, so it costs time
		// proportional to the volume on every attach, and the header already carries
		// the verdict of whoever last had a reason to look.
		return nil, fmt.Errorf("%w: %s has the qcow2 corrupt flag set", ErrChainMismatch, image)
	}
	return &Chain{Active: image, SizeBytes: info.VirtualSize}, nil
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
func readPointer(p Paths, pointer string) (string, error) {
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

// inspect runs `qemu-img info` on an offline image.
func inspect(ctx context.Context, r Runner, qemuImg, image string) (imageInfo, error) {
	out, err := r.Run(ctx, qemuImg, "info", "--output=json", image)
	if err != nil {
		// The likeliest failure here is not a broken image: it is `Failed to get shared
		// "write" lock`, meaning a QEMU has the file open that this Agent could not
		// reach over QMP. Saying so is the difference between an operator checking the
		// socket path they launched the VM with and an operator running a repair on a
		// perfectly good image.
		return imageInfo{}, fmt.Errorf("qcow: inspecting %s (a lock failure here means a VM has it open and its QMP socket is not where this Agent looks): %w", image, err)
	}
	var info imageInfo
	if err := json.Unmarshal(out, &info); err != nil {
		return imageInfo{}, fmt.Errorf("qcow: decoding qemu-img info for %s: %w", image, err)
	}
	return info, nil
}
