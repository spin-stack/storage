// Package qmp is a QEMU Machine Protocol client, cut down to the one question this
// system asks today: which image file does the QEMU at this socket actually have open?
//
// # Why there is a client here at all
//
// The Agent does not run QEMU (see internal/qcow): it prepares a volume's chain and has no
// other way to know whether anything is using it. QMP is the only channel that answers, and
// v6 §7 makes it the mandatory one — flush, snapshot, switch to the new tip, and confirm QEMU
// is using it.
//
// Not a general QMP library: no event subscription, no command registry, no reconnection.
package qmp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// ErrNoEndpoint means nothing was listening at the socket path. It is the ordinary
// state of a volume whose chain is ready and whose VM has not been launched, so callers
// branch on it rather than treating it as a failure.
var ErrNoEndpoint = errors.New("qmp: no endpoint at the socket")

// Dialer opens a byte stream to a QMP socket. internal/simio/real.UnixDialer is the
// production implementation; it binds the stream's lifetime to ctx, which is what gives
// every read below a deadline without this package touching a clock (INV-01).
type Dialer interface {
	Dial(ctx context.Context, path string) (io.ReadWriteCloser, error)
}

// Client is one QMP session. It is not safe for concurrent use: a QMP connection is a
// single request/response stream, and interleaving two exchanges on it would pair each
// answer with the wrong question.
type Client struct {
	conn io.ReadWriteCloser
	dec  *json.Decoder
	enc  *json.Encoder
}

// BlockDevice is one entry of `query-block`: a block backend and the file behind it.
type BlockDevice struct {
	// Device is the backend's legacy name ("drive0"), empty for a modern -blockdev.
	Device string
	// QDev is the guest device's qdev path, which is what a -blockdev-configured disk
	// is identified by. Either of the two may be empty; both being empty is a device
	// with no name at all, which QEMU allows.
	QDev string
	// File is the path QEMU has open. This is the field the whole package exists for.
	File string
	// Format is the driver QEMU opened it with ("qcow2", "raw", ...). A volume whose
	// image is being read as raw is a volume whose backing chain is invisible to it,
	// which is worth being able to notice.
	Format string
	// NodeName is the block graph node holding the image, empty when QEMU generated an
	// anonymous one. A VM launched with `-drive file=...,if=virtio` has an anonymous
	// node — `#block126` — which QMP refuses as input, and a VM launched with
	// `-blockdev node-name=...` has a real one and *no* Device. Neither field is
	// reliably present, which is why Target picks.
	NodeName string
}

// Target is how a command names the block node it acts on. Exactly one of the two is
// set.
//
// It exists because there is no single way to name a disk that works for both kinds of
// VM, and this Agent does not launch the VM (ADR-0021) so it does not get to choose.
// A `-drive` disk has a generated drive id and an anonymous node; a `-blockdev` disk has
// a named node and no drive id. Rotation broke on the second — `deviceFor` refused a
// disk with no drive id — which is a VM shaped the way any libvirt-derived runner shapes
// one.
type Target struct {
	Device   string
	NodeName string
}

// Args renders the target as the arguments a block command names it with.
func (t Target) Args() map[string]string {
	if t.Device != "" {
		return map[string]string{"device": t.Device}
	}
	return map[string]string{"node-name": t.NodeName}
}

// Named reports whether the target is addressable at all.
func (t Target) Named() bool { return t.Device != "" || t.NodeName != "" }

// String is what an error message says about it.
func (t Target) String() string {
	if t.Device != "" {
		return "drive " + t.Device
	}
	return "node " + t.NodeName
}

// Dial connects to the QMP socket at path and completes the capabilities negotiation,
// after which the connection accepts commands.
//
// The negotiation is not optional and is not deferred: QEMU answers every command with
// `CommandNotFound` until `qmp_capabilities` has been executed, so a client that skipped
// it would fail on its first real question with an error about the wrong thing.
func Dial(ctx context.Context, d Dialer, path string) (*Client, error) {
	conn, err := d.Dial(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("%w %q: %w", ErrNoEndpoint, path, err)
	}
	c := &Client{
		conn: conn,
		dec:  json.NewDecoder(bufio.NewReader(conn)),
		enc:  json.NewEncoder(conn),
	}
	// The greeting arrives unprompted, before anything is sent. Reading it is what
	// makes the next decode line up with the answer to our own command.
	var greeting struct {
		QMP *json.RawMessage `json:"QMP"`
	}
	if err := c.dec.Decode(&greeting); err != nil {
		return nil, errors.Join(fmt.Errorf("qmp: reading the greeting from %q: %w", path, err), conn.Close())
	}
	if greeting.QMP == nil {
		return nil, errors.Join(
			fmt.Errorf("qmp: %q answered without a QMP greeting; it is not a QMP socket", path), conn.Close())
	}
	if _, err := c.execute("qmp_capabilities"); err != nil {
		return nil, errors.Join(fmt.Errorf("qmp: negotiating capabilities with %q: %w", path, err), conn.Close())
	}
	return c, nil
}

// Close ends the session.
func (c *Client) Close() error { return c.conn.Close() }

// BlockDevices reports what `query-block` says QEMU has open.
//
// It takes no context, deliberately: the connection this runs on was created with one
// and dies with it, so a second deadline here would be a second answer to the same
// question. Cancel the context the client was dialled with.
func (c *Client) BlockDevices() ([]BlockDevice, error) {
	raw, err := c.execute("query-block")
	if err != nil {
		return nil, err
	}
	var devices []struct {
		Device   string `json:"device"`
		QDev     string `json:"qdev"`
		Inserted *struct {
			File     string `json:"file"`
			Driver   string `json:"drv"`
			NodeName string `json:"node-name"`
		} `json:"inserted"`
	}
	if err := json.Unmarshal(raw, &devices); err != nil {
		return nil, fmt.Errorf("qmp: decoding the query-block answer: %w", err)
	}
	out := make([]BlockDevice, 0, len(devices))
	for _, d := range devices {
		// A backend with no medium inserted — an empty CD-ROM tray is the usual one —
		// has no file, and reporting it as a device with an empty path would make it
		// indistinguishable from a device whose file we failed to read.
		if d.Inserted == nil {
			continue
		}
		out = append(out, BlockDevice{
			Device: d.Device, QDev: d.QDev,
			File: d.Inserted.File, Format: d.Inserted.Driver,
			NodeName: nameOrAnonymous(d.Inserted.NodeName),
		})
	}
	return out, nil
}

// reply is one line from QEMU. Exactly one of the three fields is set: an answer, an
// error, or an asynchronous event that has nothing to do with the command in flight.
type reply struct {
	Return json.RawMessage `json:"return"`
	Error  *struct {
		Class string `json:"class"`
		Desc  string `json:"desc"`
	} `json:"error"`
	Event string `json:"event"`
}

// Snapshot points the device at an overlay that already exists, freezing what it was
// writing to: after it returns, `file` is where the guest's writes land and the previous
// file is complete and read-only. It is the whole of v6 §23.2's rotation.
//
// `mode: existing` — QEMU does not create the overlay, the caller does. The alternative,
// `absolute-paths`, has QEMU create it and record its backing as *the path this QEMU was
// launched with*, which is only the right answer as long as that path never comes to mean
// a different file. It was measured against a real QEMU: with `absolute-paths` and a stable
// contract path, the header of the new tip names the file that is about to become the new
// tip — a chain that points at itself. Handing QEMU an overlay whose backing we wrote
// ourselves is what makes the recorded path the one that is true offline.
//
// QEMU does not check that the overlay's recorded backing is the node it attaches. That is
// what makes this work, and it is also why the caller verifies the file it created before
// getting here: a wrong backing path is invisible for the life of the VM and wrong on the
// next boot.
// The overlay is named only when the source is, and that is QEMU's rule rather than a
// preference: addressed by node-name, blockdev-snapshot-sync answers "New overlay
// node-name missing" without one. Measured against the pinned QEMU.
func (c *Client) Snapshot(t Target, overlayNode, file string) error {
	args := t.Args()
	args["snapshot-file"] = file
	args["format"] = "qcow2"
	args["mode"] = "existing"
	if t.Device == "" {
		args["snapshot-node-name"] = overlayNode
	}
	_, err := c.executeWith("blockdev-snapshot-sync", args)
	return err
}

// NamedNodes is every node in the block graph that has a name, which is what a fresh
// overlay name has to avoid colliding with.
//
// It is a separate call from BlockDevices because the two answer different questions:
// query-block lists what a *guest device* has open, and the graph underneath it —
// backing files, filters, the file node under a qcow2 — is only in this one. A name
// already taken by a backing node is as unusable as one taken by a tip.
func (c *Client) NamedNodes() ([]string, error) {
	raw, err := c.execute("query-named-block-nodes")
	if err != nil {
		return nil, err
	}
	var nodes []struct {
		Inserted *struct {
			NodeName string `json:"node-name"`
		} `json:"inserted"`
		NodeName string `json:"node-name"`
	}
	if err := json.Unmarshal(raw, &nodes); err != nil {
		return nil, fmt.Errorf("qmp: decoding the query-named-block-nodes answer: %w", err)
	}
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if n.NodeName != "" {
			out = append(out, n.NodeName)
		}
		if n.Inserted != nil && n.Inserted.NodeName != "" {
			out = append(out, n.Inserted.NodeName)
		}
	}
	return out, nil
}

// Stop pauses the guest. It is what fencing does to a VM that is still writing.
//
// # Why the disk is not made read-only instead
//
// Read-only is what you would want: a fenced volume the guest can still read, mounted
// read-only, with the machine alive to say so. It was measured against the pinned QEMU
// and it cannot be had from this side.
//
//   - With the drive QEMU creates for itself (`-drive file=...,if=virtio`, which is how a
//     VM is launched here) the node is anonymous — `#block140` — and QMP refuses an
//     anonymous node as input: "Cannot change the option 'node-name'". There is also no
//     `read-only` property on virtio-blk-pci to set.
//   - Launched with named nodes instead (`-blockdev node-name=vol0,...`), blockdev-reopen
//     is reachable and still refuses: "Read-only block node 'vol0' cannot support
//     read-write users". The guest's virtio driver holds it read-write, and nothing on
//     the host can take that away from a running kernel.
//
// So the choice is between stopping the guest and leaving it writing to a volume this
// host no longer owns. A fenced host whose guest goes on writing is a host that has been
// told it is not the writer and is writing — every byte after that lands in a chain the
// published history has no room for, and the guest is being told those writes succeeded.
// Pausing is the smallest true thing this Agent can do: it does not kill the VM, whose
// lifetime belongs to whoever launched it (ADR-0021), and it ends the lie.
func (c *Client) Stop() error {
	_, err := c.execute("stop")
	return err
}

// execute sends one command and returns the raw `return` value. Events are skipped rather
// than delivered: QEMU interleaves them with command answers on the same stream — a
// `JOB_STATUS_CHANGE` can arrive between the request and its reply — so the loop reads until
// something is an answer or an error.
func (c *Client) execute(command string) (json.RawMessage, error) {
	return c.executeWith(command, nil)
}

// executeWith is execute with an `arguments` object. Nil arguments are left off the wire
// rather than sent as an empty object, because QEMU rejects `arguments` on a command that
// takes none.
func (c *Client) executeWith(command string, args map[string]string) (json.RawMessage, error) {
	req := map[string]any{"execute": command}
	if args != nil {
		req["arguments"] = args
	}
	if err := c.enc.Encode(req); err != nil {
		return nil, fmt.Errorf("qmp: sending %s: %w", command, err)
	}
	for {
		var r reply
		if err := c.dec.Decode(&r); err != nil {
			return nil, fmt.Errorf("qmp: reading the answer to %s: %w", command, err)
		}
		switch {
		case r.Error != nil:
			return nil, fmt.Errorf("qmp: %s failed: %s: %s", command, r.Error.Class, r.Error.Desc)
		case r.Return != nil:
			return r.Return, nil
		case r.Event != "":
			continue
		default:
			return nil, fmt.Errorf("qmp: %s got a line that is neither an answer, an error nor an event", command)
		}
	}
}

// nameOrAnonymous drops QEMU's generated node names.
//
// A node QEMU named for itself is called `#block126`, and QMP refuses a name beginning
// with `#` as *input* — so carrying it would produce a Target that looks addressable and
// is not. Empty is the honest answer: this node has no name anything can use.
func nameOrAnonymous(name string) string {
	if strings.HasPrefix(name, "#") {
		return ""
	}
	return name
}
