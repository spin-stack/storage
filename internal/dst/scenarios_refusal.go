package dst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/api/gen/spin/storage/v1/storagev1connect"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/cpserver"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/vhost"
)

// A volume that fails closed has two halves, and a test that checks one of them passes
// for an implementation that got the other one badly wrong.
//
//  1. **No device.** A volume this host will not serve publishes no socket. Until the
//     socket went with the refusal, a guest attached an ordinary 1 MiB /dev/vda and took
//     a hard I/O error on every sector: a disk that exists and cannot be read, which is
//     the least actionable signal this system can emit.
//  2. **The refusal still reaches the fleet.** A volume with no runtime must still be
//     reported, or it vanishes from the wire — and an absence there is indistinguishable
//     from a volume nobody ever placed on this host, which is the silence the refusal
//     vocabulary was added to end. A scenario that only checked the socket would pass
//     for an implementation that took the report down with the device.
//
// The two are asserted together, for every way this Agent can refuse: the catalog says
// the volume published an image and the bucket holds none; the volume comes back below
// the sequence a guest's fsync already returned on; the object store will not answer for
// the manifest at all; the volume is wrapped under a KEK this host was not started with;
// and the host lease lapses, so this host can no longer confirm it owns anything.
//
// Those five are the whole vocabulary a host can report bar ATTACH_FAILED, which is the
// catch-all for everything local — a socket that will not bind, a WAL that will not
// open, the -max-volumes ceiling — and reaches the report through the same map as
// NO_KEY, which this scenario already drives.
//
// **Why the whole spine and not the VolumeManager alone.** The report is the half that
// can only fail at a seam: the manager's `Volumes` answer, `Loop.report`'s mapping,
// `cpserver.applyRefusal`'s host-and-epoch predicate, and the catalog write are four
// pieces, each already covered on its own, and the failure being pinned is a volume that
// is silent in the fleet. So the scenario drives a real `agent.Loop` against a real
// `cpserver.Server` over a real `metasim` catalog, and reads the answer out of the volume
// row — the same place `-fleet-status` reads it.
//
// The one thing not modelled is the HTTP transport: the Connect handler is called
// in-process, because a socket is exactly what INV-01 keeps out of a simulation, and
// nothing on this path is about a wire format. `internal/agent`'s spine tests cover that
// hop over a real transport.

func refusalScenarios() []MandatoryScenario {
	return []MandatoryScenario{
		{Name: "a-refused-volume-has-no-socket-and-is-still-reported", Run: scenarioARefusedVolumeHasNoSocket},
	}
}

func refusalCheckers() []Checker { return []Checker{NewRefusedVolumeChecker()} }

// RefusedVolumeChecker enforces the conjunction the scenario is built around: **no
// volume is ever both refused and serving a socket** (§16).
//
// It is a checker and not only an assertion because a scenario sees the moments it looks
// at, and this property has to hold at every one of them. The failure it exists for is
// not the line that takes the socket away — that is one call, and the scenario drives it
// five ways — it is the *next* refusal somebody adds. Every refusal in this Agent
// travels through one of two doors (`Volume.refuse`, or a runtime that never started and
// is recorded in the manager's refusals map), and a third door that recorded a reason
// while leaving the runtime up would report a volume the fleet reads as "not being
// served" while a guest keeps a device for it: two statements about one volume, both
// believed, and no error anywhere.
//
// The refusal it reads is the *catalog's*, after the report crossed the wire, so the
// event states the two facts a fleet operator can check — what the fleet was told, and
// what the host still has bound — rather than two fields of one process.
type RefusedVolumeChecker struct{ violation error }

// NewRefusedVolumeChecker returns a fresh checker.
func NewRefusedVolumeChecker() *RefusedVolumeChecker { return &RefusedVolumeChecker{} }

func (c *RefusedVolumeChecker) Name() string { return "refused-volume-has-no-device" }

func (c *RefusedVolumeChecker) Observe(e Event) {
	if e.Kind != EventRefusal || c.violation != nil {
		return
	}
	if e.Refusal != string(lifecycle.RefusalNone) && e.SocketBound {
		c.violation = fmt.Errorf("volume %s is refused with %s at step %d and its socket is still bound: "+
			"the fleet is told nobody is serving it while a guest attaches a device that errors on every read (§16)",
			e.Key, e.Refusal, e.Step)
	}
}

func (c *RefusedVolumeChecker) Check() error { return c.violation }

const (
	// refusalLeaseTTL is the host lease the simulated Control Plane grants, and the
	// interval the lease arm advances past. It exceeds the heartbeat interval because
	// agent.Config refuses a lease that would lapse during normal operation.
	refusalLeaseTTL   = 30 * time.Second
	refusalHeartbeat  = 5 * time.Second
	refusalVolumeSize = 1 << 20
	// refusalDeviceBytes sizes the simulated NVMe, which the heartbeat reports and the
	// Control Plane's cordon band divides. Large enough that nothing here is cordoned
	// for fill, which is a different scenario's subject.
	refusalDeviceBytes = 1 << 30
)

// refusalPlant selects a fault injected into the world around the Agent. Never into the
// Agent: every one of these is something an operator's fleet can genuinely be, which is
// what makes the checkers' proofs behavioural rather than a hand-written event.
type refusalPlant int

const (
	noRefusalPlant refusalPlant = iota
	// catalogForgetsWhatItPromised: a Control Plane that lists a volume without the two
	// watermarks it holds for it. That is not hypothetical, it is what
	// `GetDesiredState` sent until the fields were added: the catalog calls them
	// informative (§5.8), "informative" was read as "not worth sending", and the Agent
	// was left deciding whether a missing image means "new volume" or "your data is
	// gone" with the one authority that cannot tell those apart — the bucket.
	catalogForgetsWhatItPromised
	// theFenceIsNotActedOn: a reconciler that records a fence and tears nothing down.
	// The DEV-0012 shape exactly, in the one place it is still reachable: the Agent's
	// own answer to a lease it can no longer renew.
	theFenceIsNotActedOn
	// theSocketOutlivesItsListener: a listener whose Close does not take the socket path
	// with it. Production's `hostio.Listen` relies on Go unlinking the path when the
	// listener it created is closed — a listener *adopted* from a passed descriptor
	// (socket activation, a path another process made) does not unlink at all, and Go
	// will not do it for one it did not bind. The guest then finds a socket file,
	// connects, and is refused: a device that exists and can never work, which is
	// exactly the state taking the socket away exists to remove.
	//
	// It is the fault that proves the *scenario's* socket assertion rather than a
	// checker's — see the proof in planted_bug_refusal_test.go.
	theSocketOutlivesItsListener
)

// allRefusalArms runs every arm; the planted runs name one, so a failing trace is about
// the arm the fault was aimed at.
const allRefusalArms = ""

// refusalArm is one way this Agent decides it will not serve a volume the fleet placed
// on it. Adding a way it can refuse should be one struct literal here.
type refusalArm struct {
	// name is the arm, and the suffix of every directory and socket path it uses, so two
	// arms in one simulation cannot collide on the shared Disk.
	name string
	// vol is the catalog row the fleet holds for the volume. Everything that makes the
	// arm refuse is in it: what the catalog remembers being told, and what it says the
	// volume was wrapped with.
	vol func(volumeID, hostID string) metadata.Volume
	// want is the word the catalog must hold once the refusal has crossed the wire.
	want lifecycle.Refusal
	// fault is what goes wrong in the world around the Agent before it is asked to
	// serve the volume, for the arms whose cause is not in the catalog row.
	fault func(s *Sim, volumeID string) error
	// settle drives the arm from the first reconcile cycle to the moment the refusal is
	// decided. It is the arm's only asynchrony, and every one of them ends on a
	// happens-before rather than on a delay (INV-02).
	settle func(s *Sim, f *refusalFleet, volumeID string) error
	// listens is how many times the volume's socket path may ever have been bound. One
	// for a volume that started and then failed closed — bound once, closed, and never
	// re-opened — and zero for one whose runtime never opened at all.
	listens int
	// cycleError is what a reconcile cycle is expected to fail with, empty when the
	// cycle must succeed. A volume that cannot start fails Apply, and that failure is
	// carried past the report deliberately: the report is the only thing that can tell
	// the fleet the volume exists and is not being served.
	cycleError string
	// selfFenced marks the arm where this host gives a volume up on its own — the
	// expired lease. Its socket is what the fenced-volume checker reads, which is a
	// stronger statement than the manager's own answer about whether a device exists:
	// the socket is the thing a guest attaches to.
	selfFenced bool
}

func refusalArms() []refusalArm {
	return []refusalArm{
		{
			name: "image-missing",
			vol: func(volumeID, hostID string) metadata.Volume {
				v := refusalVolume(volumeID, hostID)
				// The catalog remembers a session this volume published, and the bucket
				// holds no image for it: a stray delete, a lifecycle expiry, a restore
				// that missed one key. Serving it would hand the guest zeros for
				// everything it ever wrote and then publish the blank session over the
				// manifest that was supposed to hold it.
				v.LocalSequence, v.DurableSequence, v.PublishedSequence = 128, 128, 128
				return v
			},
			want:    lifecycle.RefusalImageMissing,
			settle:  refusesWhenItsBaseResolves,
			listens: 1,
		},
		{
			name: "durability-lost",
			vol: func(volumeID, hostID string) metadata.Volume {
				v := refusalVolume(volumeID, hostID)
				// Nothing was ever published, and a guest's fsync was answered on
				// sequence 200. The host comes back on an empty data directory — a
				// reboot onto a fresh instance store — so replay reaches 0, which is
				// below what the fleet already promised.
				v.LocalSequence, v.DurableSequence = 200, 200
				return v
			},
			want:    lifecycle.RefusalDurabilityLost,
			settle:  refusesWhenItsBaseResolves,
			listens: 1,
		},
		{
			name: "no-read-view",
			vol: func(volumeID, hostID string) metadata.Volume {
				v := refusalVolume(volumeID, hostID)
				v.LocalSequence, v.DurableSequence, v.PublishedSequence = 64, 64, 64
				return v
			},
			// The catch-all, and the one an operator sees most: the manifest is *there*
			// and the store will not answer for it. "Not found" and "the store said no"
			// are different evidence and only one of them is a missing object, so they
			// are different words in the report — this arm is the second.
			want: lifecycle.RefusalNoReadView,
			fault: func(s *Sim, volumeID string) error {
				u, err := ids.Parse(volumeID)
				if err != nil {
					return err
				}
				// Aimed at the manifest rather than at the whole store, so the fault
				// cannot move to some other object as the boot path grows a call.
				s.Store.InjectThrottleKey(image.ManifestKey([16]byte(u)), 1)
				s.Emit(Event{Kind: EventFault, Msg: "the object store refuses the volume's manifest"})
				return nil
			},
			settle:  refusesWhenItsBaseResolves,
			listens: 1,
		},
		{
			name: "no-key",
			vol: func(volumeID, hostID string) metadata.Volume {
				v := refusalVolume(volumeID, hostID)
				// The fleet wrapped this volume under a KEK, and this Agent was started
				// without one (no -kek-file). It may not decide on its own that an
				// encrypted volume is now a plaintext one.
				v.KEKID, v.DEKWrapped, v.DEKKeyID = "kek-dst", []byte("wrapped-dek"), 4
				return v
			},
			want:       lifecycle.RefusalNoKey,
			settle:     neverOpensAtAll,
			listens:    0,
			cycleError: "no key-encryption key",
		},
		{
			name:       "lease-lost",
			vol:        refusalVolume,
			want:       lifecycle.RefusalLeaseLost,
			settle:     theLeaseLapses,
			listens:    1,
			selfFenced: true,
		},
	}
}

// refusalVolume is a healthy row: an active volume placed on this host, provisioned
// without a KEK (the §15 dev mode the whole harness runs as), with nothing published.
func refusalVolume(volumeID, hostID string) metadata.Volume {
	return metadata.Volume{
		VolumeID: volumeID, SizeBytes: refusalVolumeSize, BlockSize: 512,
		CurrentEpoch: 1, State: lifecycle.VolumeActive, PrimaryHostID: hostID,
		// Not encryption: DEKKeyID is the DEK's version, and zero is the WAL's
		// plaintext marker, which the catalog refuses. A row with a version and no
		// wrapped key is a volume nobody wrapped.
		DEKKeyID: 1,
	}
}

func scenarioARefusedVolumeHasNoSocket(s *Sim) error {
	return aRefusedVolumeHasNoSocket(s, noRefusalPlant, allRefusalArms)
}

func aRefusedVolumeHasNoSocket(s *Sim, plant refusalPlant, only string) error {
	// The heartbeat reports the device, so the simulated NVMe has to have a size before
	// the first cycle: an unsized sim.Disk refuses to answer Usage at all.
	s.Disk.SetDeviceBudget(refusalDeviceBytes)
	for _, arm := range refusalArms() {
		if only != allRefusalArms && arm.name != only {
			continue
		}
		if err := runRefusalArm(s, arm, plant); err != nil {
			return fmt.Errorf("%s: %w", arm.name, err)
		}
	}
	return nil
}

func runRefusalArm(s *Sim, arm refusalArm, plant refusalPlant) error {
	ctx := context.Background()
	f, err := newRefusalFleet(s, arm.name, plant)
	if err != nil {
		return err
	}
	// A refused volume's teardown returns ErrNoReadView — the publish it will never make
	// — which is the correct answer and not this scenario's subject.
	defer func() { _ = f.mgr.Close(context.Background()) }()

	volumeID := ids.NewAt(simEpoch*1000, s.Rand).String()
	if err := f.md.CreateVolume(ctx, f.term, arm.vol(volumeID, f.host), nil); err != nil {
		return fmt.Errorf("placing the volume on the host: %w", err)
	}
	if arm.fault != nil {
		if err := arm.fault(s, volumeID); err != nil {
			return fmt.Errorf("injecting the arm's fault: %w", err)
		}
	}

	// Cycle one: heartbeat, read the desired state, serve it, report what was observed.
	if err := f.cycle(ctx, arm); err != nil {
		return fmt.Errorf("the first cycle: %w", err)
	}
	if err := arm.settle(s, f, volumeID); err != nil {
		return err
	}
	// Cycle two is what carries the refusal off the host. The first cycle reports the
	// set it observed after Apply, and three of these five arms decide *after* that:
	// the base fetch is lazy on purpose, so the volume is already being served when it
	// discovers it cannot be.
	if err := f.cycle(ctx, arm); err != nil {
		return fmt.Errorf("the second cycle: %w", err)
	}

	socket := f.socket(volumeID)
	bound := f.sockets.bound(socket)
	v, err := f.md.GetVolume(ctx, volumeID)
	if err != nil {
		return fmt.Errorf("reading the volume back from the catalog: %w", err)
	}
	s.Emit(Event{Kind: EventRefusal, Key: volumeID, Refusal: string(v.Refusal), SocketBound: bound})
	if arm.selfFenced {
		// The socket, not m.Device: what a guest attaches to is the socket, and a host
		// that has given a volume up while still holding one is a guest being served by
		// a writer that cannot confirm it owns the volume.
		s.Emit(Event{Kind: EventVolumeServe, Key: volumeID, ServedAfterFence: bound})
	}

	if bound {
		return fmt.Errorf("volume %s refuses to serve and %s is still bound: a guest attaches a %d-byte device that errors on every read, which is the one signal it cannot act on",
			volumeID, socket, refusalVolumeSize)
	}
	if got := f.sockets.listens(socket); got != arm.listens {
		return fmt.Errorf("%s was bound %d times, want %d: a refused volume must not have its socket re-opened",
			socket, got, arm.listens)
	}
	if v.Refusal != arm.want {
		return fmt.Errorf("the fleet holds refusal %q for volume %s; this host refused it with %q — a volume nobody is serving that the catalog reads as healthy is the silence this field exists to end",
			v.Refusal, volumeID, arm.want)
	}
	if v.RefusalDetail == "" {
		return fmt.Errorf("volume %s is refused with %q and no sentence behind it: the enum is what an alert keys on, the detail is what an operator reads",
			volumeID, v.Refusal)
	}
	s.Notef("volume %s: no socket, and the fleet was told %q", volumeID, v.Refusal)
	return nil
}

// refusesWhenItsBaseResolves drives the two arms that fail closed after the volume is
// already being served.
//
// **The read is the wait.** A volume whose base has not resolved parks every read, and
// `Volume.refuse` is what unparks them — with an error, on the goroutine that then
// cancels the serve context, with no branch in between. Waiting for the socket instead
// of driving the read would depend on a goroutine's timing (INV-02); waiting on a delay
// would be a magic timeout.
func refusesWhenItsBaseResolves(s *Sim, f *refusalFleet, volumeID string) error {
	dev, ok := f.mgr.Device(volumeID)
	if !ok {
		return errors.New("the volume the Control Plane listed was never opened at all")
	}
	buf := make([]byte, 512)
	_, readErr := dev.ReadAt(buf, 0)
	zeros := readErr == nil && bytes.Equal(buf, make([]byte, len(buf)))
	// Emitted whichever way it went. An answered read here is the failure this arm is
	// about — zeros where the fleet says the volume published, which is a wrong answer
	// no guest can detect — and it is the shape the checker that already watches guest
	// bytes reads.
	s.Emit(Event{Kind: EventDurableRead, Key: volumeID,
		ZerosAfterRestart: zeros, ForeignBytesAfterRestart: readErr == nil && !zeros})
	if readErr == nil {
		return fmt.Errorf("volume %s answered a read of a range the fleet says it holds: %x…", volumeID, buf[:8])
	}
	// The refusal has happened; the listener close is the next thing on that goroutine.
	// This blocks on a channel production code closes rather than on a deadline: a
	// regression that stopped taking the socket away would hang here instead of failing,
	// which is the honest trade — the alternative is a timeout, and a timeout in a
	// simulation is a stop signal.
	f.sockets.awaitReleased(f.socket(volumeID))
	return nil
}

// neverOpensAtAll is the no-key arm: the runtime fails before a socket is ever bound, so
// there is nothing asynchronous to wait for and nothing to close.
func neverOpensAtAll(_ *Sim, f *refusalFleet, volumeID string) error {
	if _, ok := f.mgr.Device(volumeID); ok {
		return errors.New("a volume this Agent cannot open has a device")
	}
	return nil
}

// theLeaseLapses is the fourth way, and the only one where the volume was serving
// perfectly well: nothing renews the host lease — the Control Plane is unreachable, or
// this host is partitioned from it — and the Agent's own monotonic clock is the only
// thing left that can act on it.
func theLeaseLapses(s *Sim, f *refusalFleet, volumeID string) error {
	socket := f.socket(volumeID)
	if !f.sockets.bound(socket) {
		return fmt.Errorf("the volume the fleet placed on this host never bound %s", socket)
	}
	s.Emit(Event{Kind: EventFault, Msg: "nothing renews the host lease"})
	s.Tick(refusalLeaseTTL + time.Second)
	return nil
}

// refusalFleet is one arm's world: a catalog, the Control Plane over it, and the Agent —
// loop and volume manager — that answers to it.
type refusalFleet struct {
	md      *metasim.Store
	term    int64
	host    string
	mgr     *agent.VolumeManager
	loop    *agent.Loop
	sockets *socketRegistry
	sockDir string
}

func (f *refusalFleet) socket(volumeID string) string {
	return path.Join(f.sockDir, volumeID+".sock")
}

// cycle runs one reconciliation and holds it to what the arm expects. An arm that names
// a cycleError requires the cycle to fail with it: a volume that cannot start is the
// whole reason the Agent carries an Apply failure past the report instead of returning
// at it, and a cycle that silently started succeeding would mean the volume opened.
func (f *refusalFleet) cycle(ctx context.Context, arm refusalArm) error {
	err := f.loop.Reconcile(ctx)
	switch {
	case err == nil && arm.cycleError != "":
		return fmt.Errorf("the cycle was expected to fail with %q and succeeded", arm.cycleError)
	case err != nil && arm.cycleError == "":
		return err
	case err != nil && !strings.Contains(err.Error(), arm.cycleError):
		return fmt.Errorf("the cycle failed with %v, want %q", err, arm.cycleError)
	}
	return nil
}

func newRefusalFleet(s *Sim, name string, plant refusalPlant) (*refusalFleet, error) {
	ctx := context.Background()
	md := metasim.New(s.Clock.Wall)
	term, err := md.AcquireLeadership(ctx, "cp-"+name)
	if err != nil {
		return nil, fmt.Errorf("electing a control plane: %w", err)
	}
	// The handler is the client: *cpserver.Server implements both sides of the generated
	// interface, so the Agent's calls land in the real handler with no transport between
	// them (INV-01: a socket is what a simulation may not have).
	var cp storagev1connect.ControlPlaneServiceClient = cpserver.New(
		md, func() int64 { return term }, refusalLeaseTTL, cpserver.DefaultBand())
	if plant == catalogForgetsWhatItPromised {
		cp = forgetfulControlPlane{cp}
	}

	f := &refusalFleet{
		md: md, term: term,
		host:    ids.NewAt(simEpoch*1000, s.Rand).String(),
		sockets: newSocketRegistry(plant == theSocketOutlivesItsListener),
		sockDir: "/run/spin-" + name,
	}

	// The Agent asks the Control Plane for the volume's key material, which is what
	// makes the no-key arm the wiring it really is rather than a fabricated error: a
	// host with no KEK against a volume the fleet wrapped. The loop is assigned below —
	// nothing calls this until the first Apply.
	keys := func(ctx context.Context, volumeID string) (agent.VolumeKeys, error) {
		return f.loop.VolumeKeys(ctx, volumeID)
	}
	mgr, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin-" + name, SocketDir: f.sockDir, Budget: scenarioBudget(1),
	}, agent.VolumeManagerDeps{
		Clock:   s.Clock,
		Disk:    s.Disk,
		Listen:  f.sockets.listen,
		Mapper:  simMapper{},
		EventFD: simEventFD,
		Store:   s.Store,
		Rand:    s.Rand,
		Keys:    keys,
	})
	if err != nil {
		return nil, fmt.Errorf("building the volume manager: %w", err)
	}
	f.mgr = mgr

	var vols agent.VolumeSource = mgr
	if plant == theFenceIsNotActedOn {
		vols = deafReconciler{mgr}
	}
	loop, err := agent.New(agent.Config{
		HostID: f.host, AgentVersion: "dst", MaxFormatVersion: 3,
		HeartbeatInterval: refusalHeartbeat, RetryBackoff: refusalHeartbeat,
		LeaseTTL: refusalLeaseTTL,
	}, agent.Deps{
		Clock:        s.Clock,
		ControlPlane: cp,
		Device:       hostDevice{s.Disk},
		Volumes:      vols,
	})
	if err != nil {
		return nil, fmt.Errorf("building the agent loop: %w", err)
	}
	f.loop = loop
	return f, nil
}

// hostDevice is the NVMe the heartbeat measures. sim.Disk answers Usage without a
// context because nothing in it can block; the Agent's interface takes one.
type hostDevice struct{ d *sim.Disk }

func (h hostDevice) Usage(context.Context) (disk.Usage, error) { return h.d.Usage() }

// deafReconciler is a VolumeManager whose Fence records nothing and tears nothing down.
//
// It is the DEV-0012 shape in the one place it is still reachable: the Agent deciding,
// on its own monotonic clock, that it can no longer confirm it owns a volume. Every
// other fence in this system arrives as a refused report; this one has no external
// trigger at all, so an implementation that computed it and acted on nothing would look
// exactly like a healthy host from every direction except the guest's.
type deafReconciler struct{ *agent.VolumeManager }

func (deafReconciler) Fence(context.Context, []string, storagev1.VolumeRefusal, string) error {
	return nil
}

// forgetfulControlPlane lists a host's volumes without the watermarks the catalog holds
// for them. See catalogForgetsWhatItPromised: this is the message that shipped, and the
// Agent it produces cannot tell a volume that never wrote anything from one whose data
// is missing.
type forgetfulControlPlane struct {
	storagev1connect.ControlPlaneServiceClient
}

func (c forgetfulControlPlane) GetDesiredState(ctx context.Context,
	req *connect.Request[storagev1.GetDesiredStateRequest],
) (*connect.Response[storagev1.GetDesiredStateResponse], error) {
	resp, err := c.ControlPlaneServiceClient.GetDesiredState(ctx, req)
	if err != nil {
		return nil, err
	}
	for _, v := range resp.Msg.GetVolumes() {
		v.PublishedSequence, v.DurableSequence = 0, 0
	}
	return resp, nil
}

// socketRegistry is the vhost-user socket directory, as much of it as a simulation can
// legitimately hold: which paths are bound right now, and how many times each has been
// bound since the host came up.
//
// It exists because "the volume has no device" is not a field the Agent sets — it is the
// absence of a bound socket, which is what QEMU connects to and what a guest's kernel
// enumerates. Asserting on `VolumeManager.Device` instead would satisfy an
// implementation that kept its listener: the Volume stays in the served map after it
// refuses, deliberately, so that it can still be reported.
type socketRegistry struct {
	mu sync.Mutex
	st map[string]*socketState
	// keepsThePath is theSocketOutlivesItsListener: closing the listener no longer
	// unbinds the path a guest connects to.
	keepsThePath bool
}

type socketState struct {
	// binds counts every successful Listen on the path, so a re-listen after a refusal
	// is visible even if the socket is closed again before anything looks.
	binds int
	// ln is the listener holding the path, nil when nothing does.
	ln *socketListener
}

func newSocketRegistry(keepsThePath bool) *socketRegistry {
	return &socketRegistry{st: map[string]*socketState{}, keepsThePath: keepsThePath}
}

func (r *socketRegistry) listen(socket string) (vhost.Listener, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.st[socket]
	if st == nil {
		st = &socketState{}
		r.st[socket] = st
	}
	if st.ln != nil {
		// Two runtimes for one volume, which the manager's own teardown order exists to
		// prevent. A real bind would take the path from the first one silently
		// (hostio.Listen unlinks a stale socket), so this refuses instead of modelling
		// the theft.
		return nil, fmt.Errorf("dst: %s is already bound", socket)
	}
	ln := &socketListener{reg: r, socket: socket, closed: make(chan struct{})}
	st.ln, st.binds = ln, st.binds+1
	return ln, nil
}

func (r *socketRegistry) release(ln *socketListener) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.keepsThePath {
		return
	}
	if st := r.st[ln.socket]; st != nil && st.ln == ln {
		st.ln = nil
	}
}

func (r *socketRegistry) bound(socket string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.st[socket]
	return st != nil && st.ln != nil
}

func (r *socketRegistry) listens(socket string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if st := r.st[socket]; st != nil {
		return st.binds
	}
	return 0
}

// awaitReleased blocks until nothing holds socket. See refusesWhenItsBaseResolves for
// why this is a wait on a channel and not a deadline.
func (r *socketRegistry) awaitReleased(socket string) {
	r.mu.Lock()
	var ln *socketListener
	if st := r.st[socket]; st != nil {
		ln = st.ln
	}
	r.mu.Unlock()
	if ln == nil {
		return
	}
	<-ln.closed
}

// socketListener is one bound socket. Accept blocks until Close, which is the whole
// contract vhost.Server.Serve relies on to be cancellable and the only part of a socket
// a simulation may model (INV-01).
//
// Close is idempotent through a sync.Once because two callers reach it — vhost's own
// context.AfterFunc and the supervisor — and a select-on-closed is not a guard against
// that at all.
type socketListener struct {
	reg    *socketRegistry
	socket string
	closed chan struct{}
	once   sync.Once
}

func (l *socketListener) Accept() (vhost.Conn, error) {
	<-l.closed
	return nil, errors.New("dst: the volume's socket is gone")
}

func (l *socketListener) Close() error {
	l.once.Do(func() {
		l.reg.release(l)
		close(l.closed)
	})
	return nil
}
