//go:build integration

// Package vhost_test is the QEMU lane: the only place the vhost-user backend
// meets the front-end it was written for.
//
// internal/vhost is proven against a simulated front-end written beside it, and
// that proves self-consistency and nothing else — a decoder tested against its
// own encoder always agrees with itself. RISK-10 marked QEMU 11.0.2's
// vhost-user-blk behaviour Unverified for exactly this reason. What runs here is
// the real binary (_output/bin/qemu-system-x86_64, the version task qemu:verify
// pins), booting a real guest off a device this process serves.
package vhost_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/vhost"
	"github.com/spin-stack/storage/internal/vhost/hostio"
)

const (
	// deviceSize is the raw file served to the guest. Small: nothing here reads
	// more than a handful of sectors, and TCG is slow enough already.
	deviceSize = 16 << 20

	// patternLBA holds the bytes the guest reads; resultLBA is where its INT 13h
	// write puts them. Both are fixed in testdata/bootsector.S.
	patternLBA = 1
	resultLBA  = 64

	// guestExitOK and guestExitFailed are what QEMU's isa-debug-exit reports for
	// the boot sector's two paths: it writes 0x20 or 0x21, and QEMU exits with
	// (value << 1) | 1. Neither value is small on purpose — QEMU's *own* failure
	// exit status is 1, which is exactly what writing 0 would encode to, and the
	// negative control has to be able to tell "the guest succeeded" from "the
	// guest never ran".
	guestExitOK     = 0x41
	guestExitFailed = 0x43

	// bootTimeout bounds the guest. Under TCG (no KVM in CI) SeaBIOS plus this
	// boot sector is well under a second; the bound is here so a backend that
	// never completes a request fails instead of hanging the lane.
	bootTimeout = 90 * time.Second

	// refusalTimeout bounds the negative control. The working lane reaches the
	// guest in under a second, so this is generous by more than an order of
	// magnitude: if the guest is going to run at all, it has run by now.
	refusalTimeout = 15 * time.Second
)

// qemuPaths locates the pinned QEMU and its firmware. The binary and the BIOS
// blobs are what `task build:qemu` extracts into _output; the lane skips rather
// than fails when they are absent, because a developer who has not built QEMU
// has not broken anything.
func qemuPaths(t *testing.T) (bin, bios string) {
	t.Helper()
	root := os.Getenv("QEMU_OUTPUT_DIR")
	if root == "" {
		root = filepath.Join("..", "..", "_output")
	}
	bin = filepath.Join(root, "bin", "qemu-system-x86_64")
	bios = filepath.Join(root, "share", "spin-stack", "qemu")
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("no QEMU at %s — run: task build:qemu", bin)
	}
	if _, err := os.Stat(filepath.Join(bios, "bios-256k.bin")); err != nil {
		t.Skipf("no firmware at %s — run: task build:qemu", bios)
	}
	return bin, bios
}

// bootSector is the guest, all 512 bytes of it. See testdata/bootsector.S.
func bootSector(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "bootsector.bin"))
	if err != nil {
		t.Fatalf("reading the boot sector: %v", err)
	}
	if len(b) != vhost.SectorSize {
		t.Fatalf("the boot sector is %d bytes, want %d", len(b), vhost.SectorSize)
	}
	if b[510] != 0x55 || b[511] != 0xaa {
		t.Fatal("the boot sector has no MBR signature; SeaBIOS will not boot it")
	}
	return b
}

// trace records what the front-end actually said, so the assertions are about
// the handshake QEMU performed rather than about it not crashing.
type trace struct {
	mu       sync.Mutex
	requests []vhost.Request
	payloads map[vhost.Request][]byte
	ioErrors []error

	// Readiness is a *state*, and it is latched from two sides because neither
	// alone is enough. This guest passes through it far too quickly to poll —
	// SeaBIOS boots, does three sector operations and powers the machine off,
	// and the session then correctly tears the device down, so a poller can find
	// "not ready" both before and after a device that served the whole workload
	// in between. But the message hook cannot see the last transition either: it
	// runs *before* a message is dispatched, so a guest that hangs after
	// SET_VRING_ENABLE sends nothing more and the hook never fires again.
	// Together they cover both.
	ready     func() bool
	everReady bool
}

func newTrace() *trace { return &trace{payloads: map[vhost.Request][]byte{}} }

func (tr *trace) onRequest(m vhost.Message) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.requests = append(tr.requests, m.Request)
	// The last of each wins: QEMU sends SET_FEATURES twice (once while probing,
	// once for real) and the second is the negotiated one.
	tr.payloads[m.Request] = bytes.Clone(m.Payload)
	// The hook runs before the message is dispatched, so this observes the
	// effect of the *previous* one. SET_VRING_ENABLE is not the last message
	// QEMU sends — GET_VRING_BASE follows when the guest shuts down — so a
	// device that ever went live is always seen going live.
	if !tr.everReady && tr.ready != nil && tr.ready() {
		tr.everReady = true
	}
}

// sample latches readiness if the device is live right now.
func (tr *trace) sample() {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if !tr.everReady && tr.ready != nil && tr.ready() {
		tr.everReady = true
	}
}

// wasReady reports whether the device ever reached the state where the guest
// could be served.
func (tr *trace) wasReady() bool {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.everReady
}

func (tr *trace) onError(_ uint32, err error) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.ioErrors = append(tr.ioErrors, err)
}

func (tr *trace) snapshot() ([]vhost.Request, map[vhost.Request][]byte, []error) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]vhost.Request(nil), tr.requests...), clonePayloads(tr.payloads), append([]error(nil), tr.ioErrors...)
}

func clonePayloads(m map[vhost.Request][]byte) map[vhost.Request][]byte {
	out := make(map[vhost.Request][]byte, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func (tr *trace) saw(r vhost.Request) bool {
	reqs, _, _ := tr.snapshot()
	for _, got := range reqs {
		if got == r {
			return true
		}
	}
	return false
}

// lane is one backend serving one QEMU.
type lane struct {
	dev   *hostio.RawFile
	srv   *vhost.Server
	trace *trace
	sock  string
	path  string
	serve chan error
}

// start seeds a raw device file, serves it on a Unix socket, and returns before
// any front-end has connected.
func start(t *testing.T, ctx context.Context, seed func([]byte)) *lane {
	t.Helper()

	// A short directory: sun_path is 108 bytes and t.TempDir() spends most of
	// them on the test's name.
	dir, err := os.MkdirTemp("", "vhostlane") //nolint:usetesting // see above
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	path := filepath.Join(dir, "device.raw")
	dev, err := hostio.CreateRawFile(path, deviceSize)
	if err != nil {
		t.Fatalf("CreateRawFile: %v", err)
	}
	t.Cleanup(func() { _ = dev.Close() })

	img := make([]byte, deviceSize)
	seed(img)
	if _, err := dev.WriteAt(img, 0); err != nil {
		t.Fatalf("seeding the device: %v", err)
	}
	if err := dev.Flush(ctx); err != nil {
		t.Fatalf("flushing the seed: %v", err)
	}

	sock := filepath.Join(dir, "vhost.sock")
	ln, err := hostio.Listen(sock)
	if err != nil {
		t.Fatalf("Listen(%s): %v", sock, err)
	}

	tr := newTrace()
	srv, err := vhost.NewServer(ln, vhost.Config{
		Backend:   dev,
		Mapper:    hostio.NewMapper(),
		Serial:    "spin-vhost-0",
		OnRequest: tr.onRequest,
		OnError:   tr.onError,
	}, hostio.NewEventFD)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	tr.mu.Lock()
	tr.ready = func() bool { d := srv.Device(); return d != nil && d.Ready() }
	tr.mu.Unlock()

	l := &lane{dev: dev, srv: srv, trace: tr, sock: sock, path: path, serve: make(chan error, 1)}
	go func() { l.serve <- srv.Serve(ctx) }()
	return l
}

// runQEMU boots the guest against the lane's socket and returns QEMU's exit
// status and its output.
func runQEMU(t *testing.T, ctx context.Context, l *lane) (int, string) {
	t.Helper()
	bin, bios := qemuPaths(t)

	args := []string{
		"-L", bios,
		// accel=kvm:tcg, in that order: KVM where the machine allows it (CI
		// runners and developer boxes usually do not expose /dev/kvm), TCG
		// otherwise. The backend cannot tell the difference; the wall clock can.
		"-machine", "q35,accel=kvm:tcg,memory-backend=mem",
		"-m", "256M",
		// vhost-user requires the front-end's RAM to be shareable: the whole
		// protocol is this process mapping it. Without share=on QEMU refuses to
		// realize the device.
		"-object", "memory-backend-memfd,id=mem,size=256M,share=on",
		"-chardev", "socket,id=vhostchr0,path=" + l.sock,
		"-device", "vhost-user-blk-pci,chardev=vhostchr0,num-queues=1,bootindex=0",
		// How the guest reports its verdict: out 0xf4 makes QEMU exit with
		// (value << 1) | 1.
		"-device", "isa-debug-exit,iobase=0xf4,iosize=0x01",
		// -nodefaults is not cosmetic. The default q35 machine wants an e1000e
		// and a VGA adapter, whose option ROMs are not among the firmware blobs
		// `task build:qemu` extracts, and QEMU refuses to start without them.
		"-nodefaults",
		"-display", "none",
		"-no-reboot",
	}

	cctx, cancel := context.WithTimeout(ctx, bootTimeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, bin, args...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out

	polling := make(chan struct{})
	go func() {
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			l.trace.sample()
			select {
			case <-polling:
				return
			case <-tick.C:
			}
		}
	}()
	err := cmd.Run()
	close(polling)

	if !l.trace.wasReady() {
		requests, _, _ := l.trace.snapshot()
		t.Fatalf("the device never reached the state where a guest could be served.\nrequests: %v\nqemu output:\n%s",
			requests, out.String())
	}
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return 0, out.String()
	case errors.As(err, &exitErr):
		return exitErr.ExitCode(), out.String()
	default:
		t.Fatalf("QEMU: %v\n%s", err, out.String())
		return -1, ""
	}
}

// TestQEMUBootsAGuestOffTheBackend is Increment 3.1's exit criterion, and the
// [Verify] step RISK-10 asks for.
//
// Everything asserted here happened over a real vhost-user socket:
//
//   - the handshake QEMU 11.0.2 actually performs, message by message;
//   - a READ the guest did not ask for — SeaBIOS reading LBA 0 to find the boot
//     signature, which is the only reason the guest runs at all;
//   - a READ and a WRITE the guest did ask for, whose bytes are checked on the
//     backend side, so a device that completed the requests without moving the
//     data would fail.
func TestQEMUBootsAGuestOffTheBackend(t *testing.T) {
	ctx := t.Context()
	boot := bootSector(t)
	want := bytes.Repeat([]byte("spin-stack/vhost"), vhost.SectorSize/16)

	l := start(t, ctx, func(img []byte) {
		copy(img, boot)
		copy(img[patternLBA*vhost.SectorSize:], want)
	})

	code, output := runQEMU(t, ctx, l)
	if code != guestExitOK {
		t.Fatalf("guest exited %d, want %d (the boot sector's success path).\nrequests: %v\nqemu output:\n%s",
			code, guestExitOK, l.trace.requests, output)
	}

	got := make([]byte, vhost.SectorSize)
	if _, err := l.dev.ReadAt(got, resultLBA*vhost.SectorSize); err != nil {
		t.Fatalf("reading back LBA %d: %v", resultLBA, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("LBA %d holds %q…, want the pattern the guest read from LBA %d",
			resultLBA, got[:32], patternLBA)
	}
	if _, _, ioErrors := l.trace.snapshot(); len(ioErrors) != 0 {
		t.Fatalf("the backend completed %d request(s) as I/O errors: %v", len(ioErrors), ioErrors)
	}
}

// TestQEMUPerformsTheHandshakeWeImplemented pins the sequence and the values.
// The simulated front-end in internal/vhost was written from the specification;
// this is the only thing that says the specification was read correctly.
func TestQEMUPerformsTheHandshakeWeImplemented(t *testing.T) {
	ctx := t.Context()
	boot := bootSector(t)
	l := start(t, ctx, func(img []byte) { copy(img, boot) })
	if code, output := runQEMU(t, ctx, l); code != guestExitOK {
		t.Fatalf("guest exited %d, want %d.\nqemu output:\n%s", code, guestExitOK, output)
	}

	requests, payloads, _ := l.trace.snapshot()

	t.Run("every message needed to bring a queue up arrives", func(t *testing.T) {
		// Not an exact sequence: QEMU repeats GET_FEATURES, SET_FEATURES and
		// SET_VRING_CALL during probing, and pinning the repetitions would make
		// this fail on a QEMU that probes differently while serving guests
		// perfectly well. What must be present is every message without which
		// the queue cannot go live.
		required := []vhost.Request{
			vhost.ReqGetFeatures, vhost.ReqSetFeatures,
			vhost.ReqGetProtocolFeatures, vhost.ReqSetProtocolFeatures,
			vhost.ReqSetOwner, vhost.ReqGetConfig,
			vhost.ReqSetMemTable, vhost.ReqSetVringNum, vhost.ReqSetVringBase,
			vhost.ReqSetVringAddr, vhost.ReqSetVringKick, vhost.ReqSetVringCall,
			vhost.ReqSetVringEnable,
		}
		for _, r := range required {
			if !l.trace.saw(r) {
				t.Errorf("QEMU never sent %s; observed %v", r, requests)
			}
		}
	})

	t.Run("nothing this backend refuses is ever asked for", func(t *testing.T) {
		// Each of these is refused by dispatch, fatally. If QEMU sends one
		// anyway the connection dies mid-handshake, so this asserts the feature
		// sets we advertise really do keep it away from them.
		for _, r := range []vhost.Request{
			vhost.ReqGetInflightFd, vhost.ReqSetInflightFd,
			vhost.ReqSetLogBase, vhost.ReqSetLogFd,
		} {
			if l.trace.saw(r) {
				t.Errorf("QEMU sent %s, which this backend refuses; observed %v", r, requests)
			}
		}
	})

	t.Run("the negotiated features are a subset of what we offer", func(t *testing.T) {
		p, ok := payloads[vhost.ReqSetFeatures]
		if !ok || len(p) < 8 {
			t.Fatalf("no SET_FEATURES payload recorded")
		}
		got := binary.LittleEndian.Uint64(p)
		if unknown := got &^ vhost.DeviceFeatures; unknown != 0 {
			t.Fatalf("QEMU set features %#x, including %#x this backend never offered", got, unknown)
		}
		// VIRTIO_F_VERSION_1 (bit 32) and VHOST_USER_F_PROTOCOL_FEATURES (bit
		// 30) are the two the rest of the implementation depends on: modern
		// ring layout, and SET_VRING_ENABLE being sent at all.
		for _, b := range []struct {
			bit  uint
			name string
		}{{32, "VIRTIO_F_VERSION_1"}, {30, "VHOST_USER_F_PROTOCOL_FEATURES"}} {
			if got&(1<<b.bit) == 0 {
				t.Errorf("QEMU did not negotiate %s (features %#x)", b.name, got)
			}
		}
	})

	t.Run("the queue depth QEMU sets is one this backend serves", func(t *testing.T) {
		p, ok := payloads[vhost.ReqSetVringNum]
		if !ok || len(p) < 8 {
			t.Fatalf("no SET_VRING_NUM payload recorded")
		}
		index := binary.LittleEndian.Uint32(p[0:4])
		num := binary.LittleEndian.Uint32(p[4:8])
		if index != 0 {
			t.Errorf("SET_VRING_NUM for queue %d; this backend serves one queue (§30.3)", index)
		}
		// QEMU's vhost-user-blk `queue-size` property defaults to 128, which is
		// the depth §30.3 fixes. This asserts the two still agree: a default
		// change would otherwise show up as a connection that dies at
		// SET_VRING_NUM with no explanation.
		if num != vhost.MaxQueueSize {
			t.Errorf("SET_VRING_NUM %d, want %d — QEMU's queue-size default has moved", num, vhost.MaxQueueSize)
		}
	})

	t.Run("GET_CONFIG asks for a config space we can answer", func(t *testing.T) {
		p, ok := payloads[vhost.ReqGetConfig]
		if !ok || len(p) < 12 {
			t.Fatalf("no GET_CONFIG payload recorded")
		}
		offset := binary.LittleEndian.Uint32(p[0:4])
		size := binary.LittleEndian.Uint32(p[4:8])
		// QEMU asks for the size of its own struct virtio_blk_config, which
		// grows between releases. Recording the number QEMU 11.0.2 asks for is
		// the point: the backend fills 60 bytes and zero-fills the rest, and
		// the bound it enforces is the protocol's 256.
		t.Logf("QEMU 11.0.2 asked for %d bytes of config space at offset %d", size, offset)
		if offset != 0 {
			t.Errorf("GET_CONFIG at offset %d, want 0", offset)
		}
		if size == 0 || offset+size > vhost.MaxConfigSize {
			t.Errorf("GET_CONFIG asks for %d bytes at %d, past the %d-byte maximum this backend enforces",
				size, offset, vhost.MaxConfigSize)
		}
	})

	t.Run("the session ends cleanly when the guest goes away", func(t *testing.T) {
		// GET_VRING_BASE is the front-end taking the ring back. Seeing it means
		// QEMU shut the device down in order rather than dropping the socket,
		// which is what Increment 3.2's reconnection will have to resume from.
		if !l.trace.saw(vhost.ReqGetVringBase) {
			t.Errorf("QEMU never sent GET_VRING_BASE; observed %v", requests)
		}
	})
}

// TestQEMUNeverRunsAGuestOnABackendThatCannotSpeak is the negative control.
// Without one, every assertion above is also satisfied by a lane that quietly
// does not run: if QEMU realized the device regardless of what the backend
// said, "the guest booted" would be evidence about SeaBIOS and nothing else.
//
// The backend here accepts the connection and hangs up. The observable is that
// the guest never runs — QEMU is killed by this test's own deadline, having
// never reached the boot sector, where the same setup with a working backend
// reaches it and exits in well under a second.
//
// It is deliberately *not* asserted that QEMU gives up. It does not:
//
//	Failed to write msg. Wrote -1 instead of 12.
//	Reconnecting after error: vhost_backend_init failed: Protocol error
//
// QEMU 11.0.2's vhost-user-blk reconnects on its own after a backend error, with
// no `reconnect=` on the chardev — measured here, and the thing Increment 3.2 is
// built on top of. See docs/plan/RISKS.md, RISK-10.
func TestQEMUNeverRunsAGuestOnABackendThatCannotSpeak(t *testing.T) {
	ctx := t.Context()
	bin, bios := qemuPaths(t)

	dir, err := os.MkdirTemp("", "vhostlane") //nolint:usetesting // sun_path is 108 bytes
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "vhost.sock")

	ln, err := hostio.Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	// Hang up on every connection, including the ones QEMU makes when it
	// reconnects.
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	cctx, cancel := context.WithTimeout(ctx, refusalTimeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, bin,
		"-L", bios,
		"-machine", "q35,accel=kvm:tcg,memory-backend=mem",
		"-m", "256M",
		"-object", "memory-backend-memfd,id=mem,size=256M,share=on",
		"-chardev", "socket,id=vhostchr0,path="+sock,
		"-device", "vhost-user-blk-pci,chardev=vhostchr0,num-queues=1,bootindex=0",
		"-device", "isa-debug-exit,iobase=0xf4,iosize=0x01",
		"-nodefaults", "-display", "none", "-no-reboot",
	)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err = cmd.Run()

	if cctx.Err() == nil {
		// QEMU exited by itself. That is fine only if the guest never ran: the
		// boot sector's two exit statuses are the ones that would mean it did.
		var exitErr *exec.ExitError
		code := 0
		if errors.As(err, &exitErr) {
			code = exitErr.ExitCode()
		}
		if code == guestExitOK || code == guestExitFailed {
			t.Fatalf("the guest ran (exit %d) on a backend that never completed a handshake:\n%s", code, out.String())
		}
		t.Logf("QEMU gave up on its own: %v\n%s", err, strings.TrimSpace(out.String()))
		return
	}
	t.Logf("QEMU never reached the guest and was still retrying after %s, as expected:\n%s",
		refusalTimeout, strings.TrimSpace(out.String()))
}
