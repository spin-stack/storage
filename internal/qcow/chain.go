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

// The layout under the Agent's data directory (v6 §5).
//
// `sealed/` and `cache/` are in §5's tree and are deliberately not created here.
// Nothing seals a layer or downloads one yet; a directory that exists and stays empty
// for two stages is a reader being told a mechanism is running.
const (
	volumesDir  = "volumes"
	activeDir   = "active"
	activeImage = "current.qcow2"
	// socketName is the QMP endpoint. It sits at the volume's root rather than under
	// `active/` because it belongs to the *session*, not to any one tip: Stage 2 rotates
	// the file under `active/` while the VM keeps running, and the socket must not move
	// with it.
	socketName = "qmp.sock"
)

// VolumeDir is where everything belonging to one volume lives.
func VolumeDir(root, volumeID string) string {
	return filepath.Join(root, volumesDir, volumeID)
}

// ActiveImage is the qcow2 file QEMU writes to. Half the contract with whoever launches
// the VM.
func ActiveImage(root, volumeID string) string {
	return filepath.Join(VolumeDir(root, volumeID), activeDir, activeImage)
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
}

// Chain is one volume's local qcow2 chain.
//
// Stage 1 has exactly one link in it — the active tip, with no backing file — and the
// type is still a chain rather than a path because the next stage adds the second: an
// external snapshot seals the tip and QEMU starts writing to a new one on top of it.
// What this type is for is naming which file is the tip, and that question only becomes
// interesting once there is more than one.
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
	Specific    struct {
		Data struct {
			Corrupt bool `json:"corrupt"`
		} `json:"data"`
	} `json:"format-specific"`
}

// Open returns the chain for one volume, creating the image the first time.
//
// `live` says a QEMU already has the active image open — the Agent restarted while the
// guest kept running — and it changes what may be done, not what is reported. An image
// in use is checked by nobody: `qemu-img` takes a lock and would fail, and forcing past
// that lock is the one thing v6 §5 forbids outright. QEMU opened the file, which is a
// stronger statement about it than any check made from here.
func Open(ctx context.Context, r Runner, p Paths, qemuImg, root, volumeID string, sizeBytes int64, live bool) (*Chain, error) {
	if sizeBytes <= 0 {
		return nil, fmt.Errorf("%w: volume %s has a size of %d bytes", ErrChainMismatch, volumeID, sizeBytes)
	}
	image := ActiveImage(root, volumeID)
	if live {
		return &Chain{Active: image, SizeBytes: sizeBytes}, nil
	}

	if err := p.MkdirAll(filepath.Dir(image)); err != nil {
		return nil, fmt.Errorf("qcow: making the directory for volume %s: %w", volumeID, err)
	}
	exists, err := p.Exists(image)
	if err != nil {
		return nil, fmt.Errorf("qcow: looking for %s: %w", image, err)
	}
	if !exists {
		if _, err := r.Run(ctx, qemuImg, "create", "-f", "qcow2", image, fmt.Sprint(sizeBytes)); err != nil {
			return nil, fmt.Errorf("qcow: creating %s: %w", image, err)
		}
		return &Chain{Active: image, SizeBytes: sizeBytes}, nil
	}

	info, err := inspect(ctx, r, qemuImg, image)
	if err != nil {
		return nil, err
	}
	switch {
	case info.Format != "qcow2":
		return nil, fmt.Errorf("%w: %s is a %s image, not qcow2", ErrChainMismatch, image, info.Format)
	case info.VirtualSize != sizeBytes:
		// Not resized to match. The catalog and the image disagree about how big the
		// guest's disk is, and growing it here would hand a guest a device that changed
		// size behind its back on the strength of a row this Agent cannot verify.
		return nil, fmt.Errorf("%w: %s is %d bytes and the catalog says %d",
			ErrChainMismatch, image, info.VirtualSize, sizeBytes)
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
