package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"sort"
	"sync"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/blockdev"
	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/lineage"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/vhost"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// Volume is one volume's data path on this host: the WAL it appends to, the block
// device a guest is served from, and the vhost-user server behind its socket. It owns
// them, starts them together and stops them together.
//
// It is deliberately not a method-set on Loop, and nothing here knows what a heartbeat
// or a Control Plane is (ADR-0021). spin's runner already owns the QEMU process and the
// host's lifecycle; when storage lands there it must be able to take this type and the
// manager below without taking the reconciliation loop with them.
//
// With an object store and a lease it runs in remote mode: a FLUSH is the §14.4 ACK
// path, and it returns only once every covering object is verified and the lease is
// still valid on the monotonic clock (INV-06, INV-07). Without a store it is local-only
// — writes are taken, reads are served, and a FLUSH ACKs on fdatasync alone (§14.8)
// rather than claiming a durability nothing backs.
type Volume struct {
	id    string
	epoch int64
	// root is what wal.SegmentDir is given; the segments themselves land in
	// <root>/<volume-id>/<epoch>, and the epoch is in that path because a promoted
	// writer must never append into the segments of the epoch it replaced.
	root   string
	socket string

	log *wal.Log
	dev *blockdev.Device
	// enc is this volume's DEK bound to its id, or nil when the Agent has no KMS.
	// The Log already holds it for the write path; it is kept here because the *read*
	// path needs it too and rebuilding the base happens on a goroutine that has no
	// other way to reach it. Passing a literal nil there is the bug this field exists
	// to make hard to write — see fetchBase.
	enc *wal.Encryption
	// vol is the volume id as the object store keys use it.
	vol [16]byte
	// lineage is the root of the clone chain this volume belongs to — itself, unless it
	// descends from something. It is where its *chunks* live and what their AAD binds
	// (image.Ident), because the whole chain shares one DEK.
	//
	// Resolved by fetchBase, from the walk it already does, before it reads a byte:
	// a clone cannot find one chunk of its own image without it. Written there before
	// baseDone closes and read only after a wait on it — like inheritedBytes below, and
	// for the same reason the two need no lock.
	lineage [16]byte
	// imageETag is the manifest this volume booted from, and what its own publish CASes
	// against (ADR-0026). Empty means there was none — a first boot — and publishing
	// with an empty ETag is create-only, so a first boot racing another still produces
	// one image and one refusal.
	imageETag string
	// store, rnd, clk and rec are kept here because publishing happens as the runtime
	// tears down, which is after the manager has stopped tracking it.
	store objectstore.Store
	rnd   io.Reader
	clk   clock.Clock
	rec   *obs.Recorder

	cancel context.CancelFunc
	done   chan struct{}
	// baseCancel releases the base fetch's context. It is deliberately *not* v.cancel:
	// stop() cancels the serve context and then waits on baseDone, so a fetch running
	// under the serve context is cancelled by the very teardown that is about to depend
	// on its result — see start, where the two contexts are separated, and stop, which
	// calls this only once publish has consumed what the fetch produced. nil when this
	// Agent has no object store.
	baseCancel context.CancelFunc
	// baseDone is closed once fetchBase has resolved the read view, whether it installed
	// a base or failed one. stop() waits on it, and the reason is not tidiness: the base
	// is everything the volume held before this session, so publishing before it lands
	// writes an image with that data missing — and the publish CASes over the previous
	// manifest, so it would replace the volume's own history with a partial view of it.
	// nil when this Agent has no object store and there is no base to wait for.
	baseDone chan struct{}
	// baseFailed records that fetchBase could not resolve the view. A volume that never
	// got its base must not publish at all: its view is not a subset of the truth, it is
	// a different thing.
	baseFailed bool
	// inherited is the ancestry this volume's own layers sit over: the composed snapshots
	// of every volume it descends from, or nil for a volume that was created rather than
	// cloned. It is what publish must *not* write down — `image.Publish` takes it and
	// serialises only the layers above it (cow.DeltaOver) — and it is held here rather
	// than passed along because a publish happens as the runtime tears down, long after
	// the walk that produced it.
	//
	// Written by fetchBase before it closes baseDone, read only after a wait on it, like
	// lineage above, so the two need no lock.
	inherited *cow.IntervalMap
	// inheritedBytes is how much of this volume's read view comes from its ancestors
	// rather than from an image of its own, and inheritedFrom names the snapshot it was
	// cloned from. Both are zero for a volume that descends from nothing.
	//
	// They exist to be logged, and what they mean changed when publishing stopped
	// flattening: this is no longer a bill the clone settles at its first stop but a
	// permanent property of it. The dataset stays in the ancestors' snapshots, this
	// volume's manifest never names it, and every attach reads through it — which is
	// exactly the cost the depth ceiling of §20.1 bounds.
	//
	// Written by fetchBase before it closes baseDone, read only after a wait on it, so
	// the two do not need a lock.
	inheritedBytes int64
	inheritedFrom  string

	// snapMu guards the snapshot bookkeeping below. It is its own lock because a
	// snapshot's upload outlives the reconcile cycle that started it, and the manager's
	// lock is held across starts and stops.
	snapMu sync.Mutex
	// pending is the snapshot the Control Plane currently asks for, empty when it asks
	// for none. It is what Status reports about, so the answer the Agent sends is always
	// about the request it was last given rather than about something it did once.
	pending string
	// snaps is what this session knows about each snapshot it was asked for. An entry
	// with done=false is an upload in flight; the map is what stops a request that
	// repeats every few seconds from starting a second one.
	snaps map[string]*snapState
	// snapWG counts the uploads in flight, so a volume being torn down finishes the
	// snapshots it started. Not tidiness: the upload calls Freeze on the log this
	// teardown is about to close, so without the wait a snapshot that was seconds from
	// done is reported FAILED and the catalog records a failure that did not happen.
	snapWG sync.WaitGroup
}

// snapState is one snapshot's outcome on this host.
type snapState struct {
	done     bool
	sequence uint64
	err      error
}

// Status is what the Agent reports about this volume: the watermarks the log actually
// holds, qualified by the epoch they were produced under (§12.3).
func (v *Volume) Status() VolumeStatus {
	w := v.log.Watermarks()
	st := VolumeStatus{
		VolumeID:          v.id,
		Epoch:             v.epoch,
		LocalSequence:     int64(w.Local),
		DurableSequence:   int64(w.Durable),
		PublishedSequence: int64(w.Published),
	}
	// Only a *finished* snapshot is reported, and only the one currently asked for. An
	// upload still in flight says nothing: the Control Plane's row stays CREATING, the
	// request arrives again next cycle, and the entry below is what makes that a no-op
	// rather than a second upload.
	v.snapMu.Lock()
	defer v.snapMu.Unlock()
	if st.SnapshotID = v.pending; st.SnapshotID == "" {
		return st
	}
	switch snap := v.snaps[st.SnapshotID]; {
	case snap == nil, !snap.done:
		st.SnapshotID = "" // nothing to say yet
	case snap.err != nil:
		st.SnapshotError = snap.err.Error()
	default:
		st.SnapshotSequence = int64(snap.sequence)
	}
	return st
}

// quiesce stops serving and waits for everything that could still append to the log.
// After it returns nothing can write to this volume, which is exactly publish's
// precondition — it is what makes ViewAtRest's view a point rather than a smear.
//
// It is separate from release (below) because between the two the image has to reach the
// object store, and that can take a very long time: see VolumeManager.Close, which
// quiesces every volume, then retries their publishes for as long as it takes. It is
// idempotent, so a second call during a teardown that was already under way is free.
func (v *Volume) quiesce() {
	v.cancel()
	<-v.done
	// Before the image and before the log closes: an in-flight snapshot is holding a
	// frozen view of this log and is the only thing that can finish it.
	v.snapWG.Wait()
}

// release gives up the volume's local resources: the base fetch's context and the log.
//
// It is called only once this volume's session is settled — published, or explicitly
// given up on — because closing the log is what ends this host's ability to publish it
// at all. Nothing here deletes a segment: the records stay on disk exactly as they were,
// which is what makes "restart and it republishes" true (ADR-0024, and the reason
// a test pins it).
func (v *Volume) release() error {
	if v.baseCancel != nil {
		// Nothing is waiting on the fetch any more, so whatever it is still doing is
		// work nobody will read. The goroutine has already ended in every path that got
		// here — the publish waits on baseDone — so this is releasing the context's
		// resources rather than stopping anything (the shutdown-publish decision).
		v.baseCancel()
	}
	return v.log.Close()
}

// ErrNoReadView is a volume that never resolved its base: publishing it would write down
// a view that is not a subset of the truth but a different thing, so it is refused.
//
// It is a sentinel because the shutdown path has to tell it apart from a store that is
// merely unreachable. Both mean "this session is not in the object store", but only the
// second is worth retrying — the fetch that failed is over, and this Volume will never
// attempt another one. Holding the data directory for it would be a wait with no event
// that can end it; a restart is what re-fetches the base and republishes.
var ErrNoReadView = errors.New("agent: the volume's read view never resolved, so its image would be missing everything it held before this session")

// ident is the pair image keys everything by: this volume, and the lineage whose chunk
// store holds its bytes.
//
// Every caller of it is downstream of a wait on baseDone — publish, snapshot — because
// fetchBase is what resolves the lineage of a clone. A caller that skipped that wait
// would publish a clone's chunks under the clone's own id, where the next reader of that
// manifest will not look for them, and nothing would report a failure.
func (v *Volume) ident() image.Ident {
	return image.Ident{Volume: v.vol, Lineage: v.lineage}
}

// publish writes the volume's state to the object store. It is the whole durability
// contract of V1 (ADR-0026): nothing else leaves the host, and what this writes is what
// the next boot — or a clone — reads.
//
// It runs after quiesce, because that is the first moment nothing can append.
//
// It returns its error instead of logging it, which it used to do. The difference is the
// whole of the shutdown-publish decision: a caller that is told the publish failed can retry it,
// hold the data directory while it does, and exit non-zero if it never succeeds. A caller
// that reads slog cannot do any of those things, and the process exited 0 with the
// session in nobody's bucket.
func (v *Volume) publish(ctx context.Context) error {
	if v.store == nil {
		return nil // local-only Agent: the local WAL is all there is, by design
	}
	if v.baseDone != nil {
		// The fetch runs on its own goroutine and is not covered by v.done, which waits
		// for the serve loop. Publishing without waiting is how a race becomes data loss:
		// the view would be missing everything the base holds, and the CAS would install
		// that over the manifest the base came from. Whoever bounded that wait has
		// already done so (see VolumeManager.awaitBase); this is the guard that makes
		// publish correct on its own rather than by convention.
		<-v.baseDone
	}
	if v.baseFailed {
		return fmt.Errorf("%w: volume %s", ErrNoReadView, v.id)
	}
	view, seq := v.log.ViewAtRest()
	v.sayWhatThisImageCosts(view)
	// Under ADR-0026 this is the *only* moment anything leaves the host, so its duration
	// is the cost of a whole session rather than one step among many — and it is what an
	// operator watching a slow shutdown needs (§26.2). Recorded for a failed publish too:
	// how long it took to fail is the more interesting number.
	start := v.clk.Now()
	defer func() {
		v.rec.Observe(ctx, "image_publish_duration_seconds", v.clk.Now().Sub(start).Seconds(),
			obs.String("volume", v.id))
	}()
	etag, err := image.Publish(ctx, v.store, v.rnd, v.enc, v.ident(), view, v.inherited, seq, v.imageETag)
	if err != nil {
		return fmt.Errorf("agent: volume %s: publishing its image at sequence %d: %w", v.id, seq, err)
	}
	v.imageETag = etag
	slog.Info("volume image published", "volume_id", v.id, "sequence", seq)
	return nil
}

// sayWhatThisImageCosts prints the size of the image about to be written, and how much
// this volume reads out of its ancestors and is not writing down.
//
// **Before the upload, not after, and that is the whole point.** The operator's question
// is asked while a stop is hanging, and a line printed when the publish finishes cannot
// answer a question about why it has not finished. Nothing else in the process says this:
// the durability histogram is recorded on the way out, and `chain_depth` has no producer.
//
// What the two numbers mean has now changed twice, and it is worth being exact because an
// operator sizing a stop reads them. They were "the whole flattened view" and "how much of
// it came from a parent", when a publish wrote down everything a clone could read. Moving
// the chunk store to the lineage made the second stop being a *transfer* — the untouched
// chunks were already there — while the manifest still named them. Now the manifest is a
// delta, so `image_bytes` is what this volume itself states and `inherited_bytes` is what
// it leaves to its ancestry: a clone of a 40 GiB golden image that wrote one sector prints
// a few kilobytes against 40 GiB inherited, and the second number never falls, because it
// is what every attach of this volume will keep reading through.
//
// It is printed for a snapshot too, for the same reason and with the same meaning.
//
// Summing the delta is O(extents), the fold `cow.Cost` deliberately avoids. That is right
// here and wrong there: Cost is recorded at the WAL's flush cadence, on every guest fsync,
// and this runs once per stop, immediately before an upload of exactly these ranges. The
// alternative — reporting Cost().Bytes — would double-count every byte a layer overwrote
// in the one below it.
func (v *Volume) sayWhatThisImageCosts(view *cow.IntervalMap) {
	own, err := view.DeltaOver(v.inherited)
	if err != nil {
		// The publish that follows fails on this same error, so this is not the report of
		// it — it is the line that would otherwise be missing from the one place an
		// operator is looking while the stop hangs.
		slog.Error("the volume's image cannot be sized: its view does not sit over the ancestry it was given",
			"volume_id", v.id, "error", err)
		return
	}
	var n int64
	for _, r := range own.Data {
		n += int64(r.Length)
	}
	if v.inheritedBytes == 0 {
		slog.Info("publishing the volume's image", "volume_id", v.id, "image_bytes", n)
		return
	}
	slog.Info("publishing the volume's image, which states what this volume wrote; the dataset it inherited stays in its ancestors' snapshots and is read through them at every attach",
		"volume_id", v.id, "image_bytes", n, "discarded_ranges", len(own.Discarded),
		"inherited_bytes", v.inheritedBytes, "parent_snapshot_id", v.inheritedFrom,
		"lineage_root", format.UUIDString(v.lineage))
}

// liveBytes is how many bytes a view answers with — the sum of the ranges it reports,
// flattened over its whole base chain.
//
// Its one caller measures an **ancestry**, and flattened is the right sense there: what a
// clone reads through its ancestors is the composed chain, not any one link of it, and
// summing the links would count a range an ancestor wrote and a nearer one rewrote twice.
// It is deliberately not what a publish moves any more — that is the delta, which
// sayWhatThisImageCosts takes separately.
func liveBytes(view *cow.IntervalMap) int64 {
	var n int64
	for _, r := range view.Ranges() {
		n += int64(r.Length)
	}
	return n
}

// ListenFunc opens the vhost-user socket for one volume. It is injected because a Unix
// socket is a kernel object (INV-01): production passes hostio.Listen, and a test passes
// something it can close.
type ListenFunc func(socket string) (vhost.Listener, error)

// KeysFunc fetches one volume's wrapped key material. Production passes
// Loop.VolumeKeys, which asks the Control Plane once and caches the answer; it is a
// function rather than the Loop because ADR-0021 keeps this type from knowing what a
// Control Plane is, and
// because ADR-0021 keeps this type from knowing what a Control Plane is.
type KeysFunc func(ctx context.Context, volumeID string) (VolumeKeys, error)

// ErrNoKEK is a volume the catalog records as encrypted, on an Agent that holds no
// key-encryption key. It is a sentinel because "this host cannot open this volume" and
// "this host was started wrong" want different answers from whoever is watching: the
// first is a placement or a key-distribution problem, the second is one missing flag on
// one process, and the fix is to restart it with -kek-file.
var ErrNoKEK = errors.New("agent: this Agent holds no key-encryption key (no -kek-file) and the volume was provisioned with one")

// encryptionFor unwraps this volume's DEK and binds it to the volume (§15.1). It
// returns nil, nil only for a volume the catalog says was provisioned without a KEK —
// the dev/local mode of §15 — and an error for every other failure, because the
// alternative to encrypting is not "encrypt later", it is writing this guest's data
// into the bucket in the clear.
//
// **The key material is fetched before the KMS is consulted, and that order is the
// guard.** It used to be the other way round: an Agent with no KMS returned nil here
// without ever asking what the volume was provisioned with, so a volume whose row
// carries a kek_id was served unencrypted — `encrypted=false` on the serving line, the
// guest's writes in the WAL in cleartext, and at detach an image of raw guest plaintext
// published under keys that say `<nonce:12><ct><tag:16>`. Nothing failed, nothing was
// retried, and §15.3's crypto-shredding guarantee was gone for that volume: destroying
// the DEK leaves those objects readable. The only thing against it was one WARN at
// start-up, which is process-wide, printed once, and says nothing about any volume.
//
// A KEK-less Agent may still serve volumes provisioned without one, which is what the
// dev/local mode is and what the DST harness and the QEMU lane run as. What it may not
// do is decide, on its own, that a volume the fleet encrypted is now a plaintext volume.
func (m *VolumeManager) encryptionFor(ctx context.Context, id string, vol [16]byte) (*wal.Encryption, error) {
	if m.deps.Keys == nil {
		// Neither a KMS nor a source of key material: there is nothing to ask and
		// nothing to ask about. NewVolumeManager refuses a KMS without Keys, and
		// `cmd/volume-agent` wires Keys whether or not it was given a -kek-file, so the
		// only callers that land here are in-process ones (the DST harness,
		// integration/vhost) whose volumes have no catalog row behind them at all.
		return nil, nil //nolint:nilnil // no keys at all is a mode, not a failure: see VolumeManagerDeps.KMS
	}
	keys, err := m.deps.Keys(ctx, id)
	if err != nil {
		// Fail closed, on this host's *only* way of learning whether the volume is
		// encrypted. Not knowing is not the same as "not encrypted", and the direction
		// of the mistake is not symmetric: refusing an unencrypted volume costs an
		// attach that the next cycle retries, serving an encrypted one in the clear
		// costs the guarantee outright and reports nothing.
		return nil, fmt.Errorf("agent: volume %s: reading key material: %w", id, err)
	}
	if m.deps.KMS == nil {
		// The dev/local mode is exactly this: a volume the catalog says nobody wrapped.
		// kek_id and dek_wrapped are checked together because either one, on its own,
		// is a row that says "this volume's bytes are sealed" — and this Agent could
		// not open them nor produce them.
		if keys.KEKID == "" && len(keys.DEKWrapped) == 0 {
			return nil, nil //nolint:nilnil // provisioned without a KEK: the §15 dev mode, and it stays
		}
		// Per volume and by name, because that is what the start-up WARN could never
		// be. Returned rather than logged here: `Loop.Run` prints every failed cycle,
		// so this reaches the operator once per cycle, with the volume and the KEK in
		// it, for as long as the host is wired this way.
		return nil, fmt.Errorf("%w: volume %s is wrapped under KEK %q, so this host can neither read its image nor seal what a guest writes; it is not served",
			ErrNoKEK, id, keys.KEKID)
	}
	if keys.KEKID != m.deps.KMS.KEKID() {
		// Not a §15.1 rotation — that is the *DEK* rotating under one KEK. This is the
		// volume having been wrapped by a KEK this host does not hold, and unwrapping
		// would fail on the AEAD anyway. Saying which key is missing turns an opaque
		// authentication failure into an operational instruction.
		return nil, fmt.Errorf("agent: volume %s is wrapped under KEK %q; this host holds %q",
			id, keys.KEKID, m.deps.KMS.KEKID())
	}
	dek, err := m.deps.KMS.UnwrapDEK(keys.DEKWrapped, keys.DEKKeyID)
	if err != nil {
		return nil, fmt.Errorf("agent: volume %s: unwrapping the DEK (version %d): %w", id, keys.DEKKeyID, err)
	}
	enc, err := wal.NewEncryption(dek, vol)
	if err != nil {
		return nil, fmt.Errorf("agent: volume %s: %w", id, err)
	}
	return enc, nil
}

// VolumeManagerConfig is where this host keeps things.
type VolumeManagerConfig struct {
	// DataDir holds the WALs: <data-dir>/wal/<volume-id>/<epoch>.
	//
	// It is a path *inside the injected Disk's namespace*, not a host path, and the
	// difference has bitten once already: production roots its real.Disk at the
	// operator's --data-dir, so passing the same absolute path here produced
	// <data-dir>/<data-dir>/wal/... — every byte the Agent wrote was one level below
	// where its operator was told to look. A rooted Disk wants "." here; a Disk
	// spanning a whole filesystem (every test, and the DST harness) wants the path.
	DataDir string
	// DataDirLabel is that same directory spelled the way the operator spelled it, and it
	// exists for one reason: in production DataDir is the string ".", so every message
	// naming it named nothing. The Agent's real Disk is rooted at --data-dir precisely so
	// the process cannot write outside it, which makes "." the correct value for the field
	// above and a useless one for a human — and the message that matters most is the one
	// that says "I am holding this directory and will not let go", whose whole purpose is
	// to tell an operator which directory, on which host, is stuck.
	//
	// Found by running the binaries: the holding line printed `data_dir=.`. Nothing
	// in-process could see it, because every test hands the manager a Disk spanning a
	// whole filesystem, where the two spellings agree — which is the same blind spot that
	// hid --data-dir being applied twice.
	//
	// Empty means "use DataDir", so a test or the DST harness needs no second string.
	DataDirLabel string
	// SocketDir holds one vhost-user socket per volume: <socket-dir>/<volume-id>.sock.
	SocketDir string
	// Budget is this device divided among the volumes this host serves (ADR-0013 §1).
	// It is required: an Agent whose logs have no MaxLocalBytes has no write-path
	// bound at all, and the first thing it does about a filling device is take it to
	// ENOSPC — which arrives as a partial append, in the middle of a guest's WRITE,
	// for every volume on the host at once. That was the state of the production
	// binary until this existed, and it was invisible because every test set the
	// limits it wanted itself.
	//
	// It lives here rather than in `main` for the reason the data-directory lock does:
	// a step left to `main` is a step spin's runner does not inherit (ADR-0021), and
	// this repository has already shipped an Agent that never set HostID because one
	// field in one binary was forgotten.
	Budget Budget
	// ShutdownGrace bounds **one** publish attempt, and one wait for a read view, during
	// a teardown. It is not a budget after which data is abandoned: when an attempt is
	// cut short Close retries it, and nothing in this type ever gives a session up on a
	// timer (the shutdown-publish decision, "REVIEWED AND DECIDED"). What it exists for is the
	// one failure a retry cannot survive — a PUT that neither succeeds nor fails, which
	// without a bound is a teardown that hangs with nothing printed and no way in.
	//
	// The zero value is legal and means unbounded, which is what every in-process caller
	// wants: a test and the DST harness have no operator behind them, and a timer armed
	// on the simulated clock would be one more thing the harness has to advance past to
	// reach the behaviour it is actually driving. `cmd/volume-agent` sets it from
	// -shutdown-grace, whose default (60s) mirrors cmd/control-plane's.
	ShutdownGrace time.Duration
}

// dataDir is the directory this Agent claims, spelled for a human. See DataDirLabel.
func (c VolumeManagerConfig) dataDir() string {
	if c.DataDirLabel != "" {
		return c.DataDirLabel
	}
	return c.DataDir
}

// VolumeManagerDeps are the injected collaborators (INV-01).
type VolumeManagerDeps struct {
	Clock  clock.Clock
	Disk   disk.Disk
	Listen ListenFunc
	// Mapper turns the front-end's memory-region descriptors into host memory, and
	// EventFD adapts the kick/call descriptors it sends. Both are kernel objects, and
	// both live behind the ADR-0020 exemption in internal/vhost/hostio. Neither is
	// touched until a front-end connects, but both are required here: vhost.NewServer
	// refuses a Config without them, and a manager that discovers that in its serve
	// goroutine has already told Apply the volume started.
	Mapper  vhost.Mapper
	EventFD vhost.EventFDFunc
	// Store is where FLUSH makes a write durable (§14.4). Nil is local-only mode: the
	// device serves and takes writes, and no FLUSH ever claims remote durability.
	Store objectstore.Store
	// KMS unwraps a volume's DEK, and Keys is where the wrapped one comes from.
	//
	// An Agent with no KMS runs unencrypted, which is the dev/local mode (§6.2) the DST
	// harness and the QEMU lane use — but only for volumes the catalog says were
	// provisioned without a KEK. Keys is consulted whether or not there is a KMS, and it
	// is what decides that (see encryptionFor): "this host was started without a key" is
	// not a licence to reclassify an encrypted volume as a plaintext one. This is why
	// `cmd/volume-agent` wires Keys even when it was given no -kek-file.
	//
	// An Agent *with* a KMS encrypts every volume it serves or serves none of them —
	// see start. §15.1 puts the unwrap at attach and nowhere else: one KMS call
	// outside the data path, and the DEK lives in memory only.
	KMS  crypto.KMS
	Keys KeysFunc
	// Rand is where the image's chunk nonces come from (§15, image.Publish). It is
	// injected rather than reached for because INV-01 keeps randomness out of the data
	// path's dependencies and because DST needs the same seed to produce the same
	// ciphertext (INV-02). Production passes crypto/rand.Reader; nil means the volume
	// cannot publish an encrypted image, which is refused at construction rather than
	// discovered at the first stop.
	Rand io.Reader
	// Recorder is where the §26.2 metrics this manager owns are written. Nil is a
	// working no-op, which is what production passes today — `cmd/volume-agent` has no
	// exporter to send them to, and wiring one is a deploy concern nobody has landed.
	// The metrics are recorded anyway because §19 names two of them as mandatory and
	// because the alternative is discovering, during the first incident, that the code
	// to record them was never written.
	Recorder *obs.Recorder
}

// VolumeManager owns the live runtimes and is the Agent's VolumeSource. Apply is the
// whole of the reconciliation: it diffs the desired state against what is running.
type VolumeManager struct {
	cfg  VolumeManagerConfig
	deps VolumeManagerDeps

	mu      sync.Mutex
	volumes map[string]*Volume
	// fencedEpoch is the highest epoch this host has been fenced out of, per volume.
	// Without it a fenced volume would come straight back: the Control Plane refuses
	// the *report* while GetDesiredState may still list the volume for this host, so
	// the next Apply would find no runtime and start one — serving a volume this host
	// has just been told it does not own. Only a higher epoch clears it, because a
	// higher epoch is the Control Plane granting the volume again.
	fencedEpoch map[string]int64
	closed      bool
	// lock is this host's claim on DataDir (DEV-0014). Held for the manager's life
	// and released by Close.
	lock io.Closer
}

// NewVolumeManager validates the wiring and returns a manager with nothing running.
func NewVolumeManager(cfg VolumeManagerConfig, deps VolumeManagerDeps) (*VolumeManager, error) {
	switch {
	case cfg.DataDir == "":
		return nil, errors.New("agent: a data directory is required to hold the WALs")
	case cfg.SocketDir == "":
		return nil, errors.New("agent: a socket directory is required to serve volumes")
	case cfg.Budget.Share() <= 0:
		// Refused here, at start-up, rather than discovered when the device fills.
		// A zero budget is not "unbounded by choice", it is a wiring omission: the
		// binary measures its device (NewDiskUsage) and divides it (NewBudget), and
		// both of those fail loudly on a device that cannot be measured. See
		// VolumeManagerConfig.Budget.
		return nil, fmt.Errorf("agent: a device budget is required to serve volumes (ADR-0013 §1): %d bytes for %d volumes leaves no share",
			cfg.Budget.GuestBytes, cfg.Budget.MaxVolumes)
	case deps.Clock == nil:
		return nil, errors.New("agent: a clock must be injected (INV-01)")
	case deps.Disk == nil:
		return nil, errors.New("agent: a disk must be injected (INV-01)")
	case deps.Listen == nil:
		return nil, errors.New("agent: a listen function must be injected (INV-01)")
	case deps.Mapper == nil:
		return nil, errors.New("agent: a memory mapper must be injected (ADR-0020)")
	case deps.EventFD == nil:
		return nil, errors.New("agent: an EventFD adapter must be injected (ADR-0020)")
	case deps.Store != nil && deps.KMS != nil && deps.Rand == nil:
		// An Agent that can encrypt and cannot draw a nonce would seal every image chunk
		// with whatever a nil reader gives — which is nothing, so Publish would fail at
		// the first stop, in the teardown path, where the data is already unreachable.
		// Refused here instead (§15, image.Publish).
		return nil, errors.New("agent: a random source must be injected when a KMS is (§15: image chunk nonces)")
	case deps.KMS != nil && deps.Keys == nil:
		// A KMS with nowhere to get wrapped keys from would unwrap nothing and every
		// volume would fail to start — at attach, one at a time, looking like a
		// Control Plane problem. It is a wiring problem, and it is visible here.
		return nil, errors.New("agent: a KMS needs a source of wrapped volume keys (§15.1)")
	}
	// §10 opens with "un proceso por host", and until now nothing enforced it. Two
	// Agents against one data directory both resume the same segment files and both
	// append to them, and `hostio.Listen` unlinks a stale socket before binding — so
	// the second silently steals the guest from the first rather than failing to bind.
	// The lock is taken here rather than in `main` because this type owns DataDir, and
	// because a step left to `main` is a step spin's runner will not inherit (ADR-0021)
	// — which is exactly how HostID went missing until an e2e lane read the log line
	// about it.
	lock, err := deps.Disk.Lock(path.Join(cfg.DataDir, lockFile))
	if err != nil {
		if errors.Is(err, disk.ErrLocked) {
			return nil, fmt.Errorf("agent: another Volume Agent is already using %s (§10: one Agent per host): %w",
				cfg.dataDir(), err)
		}
		return nil, fmt.Errorf("agent: claiming %s: %w", cfg.dataDir(), err)
	}
	return &VolumeManager{
		cfg: cfg, deps: deps, lock: lock,
		volumes:     map[string]*Volume{},
		fencedEpoch: map[string]int64{},
	}, nil
}

// lockFile is what this Agent claims inside its data directory. Its *contents* are
// never read: a pid in it would be a liveness check the kernel already performs, with
// the classic race (read pid, process dies, pid is reused) this deliberately avoids.
const lockFile = "agent.lock"

// Apply makes the running set match desired: start what is new, stop what left, and
// replace what was promoted to a new epoch. With one exception, argued where it is
// implemented below: an *empty* desired state stops nothing, because it is the one
// message a confused Control Plane and a fully drained host both send.
//
// Failures are collected rather than returned at the first one. A volume whose socket
// is taken must not stop the others from being served — the loop retries the whole
// desired state on its next cycle, and Apply is idempotent for everything that already
// started.
func (m *VolumeManager) Apply(ctx context.Context, desired []*storagev1.DesiredVolume) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return errors.New("agent: the volume manager is closed")
	}
	m.mu.Unlock()

	live := make(map[string]bool, len(desired))
	var errs []error

	for _, d := range desired {
		id := d.GetVolumeId()
		if err := validateDesired(d); err != nil {
			errs = append(errs, err)
			continue
		}
		live[id] = true

		m.mu.Lock()
		existing, running := m.volumes[id]
		fencedAt, wasFenced := m.fencedEpoch[id]
		m.mu.Unlock()

		if wasFenced && d.GetEpoch() <= fencedAt {
			// Fenced out of this epoch and the Control Plane has not granted a newer
			// one. Silently, because the desired state repeats every few seconds and
			// this is the steady state until the volume is either re-granted or
			// dropped from the list.
			continue
		}

		if running {
			if existing.epoch == d.GetEpoch() {
				// Already serving exactly this — which is the *common* case, and the one
				// a snapshot request arrives in. Checking it only on the paths that
				// start a runtime would mean a volume can be snapshotted at the moment
				// it is attached and never again.
				m.ensureSnapshot(existing, d.GetPendingSnapshotId())
				continue
			}
			// Promoted. The old runtime is torn down before the new one opens, because
			// both would otherwise want the same socket.
			if err := m.remove(ctx, id); err != nil {
				errs = append(errs, fmt.Errorf("agent: replacing volume %s at epoch %d: %w", id, d.GetEpoch(), err))
				continue
			}
		}

		v, err := m.start(ctx, d)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		m.mu.Lock()
		m.volumes[id] = v
		m.mu.Unlock()
		m.ensureSnapshot(v, d.GetPendingSnapshotId())
	}

	// Whatever the Control Plane no longer lists for this host: promoted away,
	// detached, or fenced. In every case this host has stopped being its writer.
	m.mu.Lock()
	var gone []string
	for id := range m.volumes {
		if !live[id] {
			gone = append(gone, id)
		}
	}
	// The fencing memory exists to outlast a desired state that **still lists** a volume
	// whose report the Control Plane refused (DEV-0012): without it the very next Apply
	// finds no runtime and starts one, serving a volume this host was just told it lost.
	// Once the Control Plane has stopped listing the volume at all, there is nothing left
	// for the memory to outlast — and keeping it is not free.
	//
	// It became not-free with the guard below. A detach used to reach this host as an
	// empty desired state and stop the volume here, setting no fencing memory; now the
	// empty list stops nothing and the *report* is what is refused, so the same detach
	// goes through Fence and does set it. `controlplane.Place` re-places a volume without
	// bumping its epoch, so `-detach-volume X` followed by `-attach-volume X` naming this
	// same host would land at the epoch this host is remembered as fenced out of, and be
	// skipped forever — a guest with a device that never comes back and no error anywhere.
	// Forgetting on "no longer listed" is what keeps DEV-0012's rule (the Control Plane
	// keeps listing it, so the entry survives) without that.
	for id := range m.fencedEpoch {
		if !live[id] {
			delete(m.fencedEpoch, id)
		}
	}
	m.mu.Unlock()
	// INV-02: this is the order the volumes are stopped, published and named in, and a
	// map's is a different one every run.
	sort.Strings(gone)

	// **An absence inside a list is information; an empty list is not.** Stopping a
	// volume is what publishes it (ADR-0026), so obeying an empty desired state means a
	// host uploads every session it holds and takes every guest's device away at once —
	// and an empty list is exactly what a Control Plane produces when it comes up
	// against an empty database, when the query behind GetDesiredState returns no rows
	// for a reason that has nothing to do with this host, or when a config change leaves
	// this Agent asking about a host id nobody ever placed anything on. None of those is
	// distinguishable from "you have been detached from everything" by looking at the
	// message, so the message alone stops nothing.
	//
	// A list that names *something* is a different kind of statement: it is proof the
	// Control Plane knows this host and is deciding volume by volume, so a volume
	// missing from it is a decision about that volume. That case is unchanged — a
	// partial list still stops what it leaves out.
	//
	// The test is len(live) and not len(desired), because a desired state whose every
	// entry this host could not parse (no id, not a UUID, a negative epoch — see
	// validateDesired) is a message that named nothing usable either, and treating it as
	// a full inventory would tear the host down on the strength of what it just refused.
	//
	// **This does not make a real drain impossible, and that is checkable rather than
	// hopeful.** `cpserver.GetDesiredState` lists on `volumes.primary_host_id` and
	// `cpserver.applyReport` refuses on that same column, so every genuine detach,
	// promotion or deletion that empties this host's desired state also refuses this
	// host's *report* of those volumes — and `Loop.fence` then calls Fence, which is the
	// same teardown, in the same cycle. The empty list was never the only signal for a
	// legitimate teardown; it was the one that carries no name.
	//
	// **Where the rule is wrong, and what happens then.** Two cases, and neither is
	// hypothetical enough to leave unwritten. (1) If the desired state ever grows a
	// filter the report path does not share — a volume state, a host in maintenance — a
	// volume dropped by it keeps being served here until something names it. That is
	// bounded rather than permanent: the fleet cannot hand that volume to anyone else
	// without changing `primary_host_id`, which is precisely what makes the next report
	// refuse it. (2) A Control Plane whose catalog is *gone* refuses every report too,
	// so Fence tears this host down anyway and this guard buys nothing at all. It closes
	// the door where an absence is an order; it deliberately does not teach the Agent to
	// second-guess a refusal that names a volume, because that refusal is the only thing
	// standing between two hosts serving one volume.
	if len(live) == 0 && len(gone) > 0 {
		// Every cycle, not once: an Agent whose Control Plane has stopped naming its
		// volumes is in a state somebody has to fix, and it ends the moment the Control
		// Plane says anything at all. A line on the transition would be one line, hours
		// before whoever is looking arrives — the same reasoning as the holding line in
		// publishHeld.
		slog.Warn("the control plane listed no volumes for this host; the ones already being served are kept, because an empty desired state names nothing",
			"serving", len(gone), "volume_ids", gone, "listed", len(desired))
		return errors.Join(errs...)
	}
	for _, id := range gone {
		if err := m.remove(ctx, id); err != nil {
			errs = append(errs, fmt.Errorf("agent: stopping volume %s: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

// validateDesired refuses what this host cannot serve, on the volume rather than at the
// first guest request — by which time a guest has a device it cannot use.
func validateDesired(d *storagev1.DesiredVolume) error {
	id := d.GetVolumeId()
	if id == "" {
		return errors.New("agent: the Control Plane listed a volume with no id")
	}
	if _, err := ids.Parse(id); err != nil {
		// The WAL carries the volume id as 16 raw bytes and the socket path is built
		// from it; a value that is not a UUID has neither spelling.
		return fmt.Errorf("agent: volume id %q is not a UUID (INV-22): %w", id, err)
	}
	if d.GetEpoch() < 0 {
		return fmt.Errorf("agent: volume %s was listed at epoch %d", id, d.GetEpoch())
	}
	return nil
}

// start builds one volume's runtime and puts its serve loop under supervision. It
// unwinds everything it opened if any step fails: a log left open on a volume nothing
// serves would hold the WAL and be invisible to Volumes.
func (m *VolumeManager) start(ctx context.Context, d *storagev1.DesiredVolume) (*Volume, error) {
	id := d.GetVolumeId()
	u, err := ids.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("agent: volume id %q: %w", id, err)
	}

	// The fan-out the budget was divided by is also the number of volumes this host
	// serves, and it has to be refused here or the division means nothing: every log
	// gets a share of GuestBytes/MaxVolumes, so serving MaxVolumes+1 of them is a host
	// whose volumes can hold more than its budget between them — the exact failure
	// ADR-0013 §1 exists for, arrived at through the only door left.
	//
	// Refusing an attach is a local, defensive power the Agent already has (ADR-0013
	// §5); moving volumes is the Control Plane's, and this does not do that. The
	// volume stays in the desired state and Apply retries it every cycle, so a
	// detach elsewhere lets it in without anyone intervening — and the Control Plane
	// sees the failure in the report, which is where an operator finds out that this
	// host's -max-volumes disagrees with what the fleet placed on it.
	m.mu.Lock()
	running := len(m.volumes)
	m.mu.Unlock()
	if running >= m.cfg.Budget.MaxVolumes {
		return nil, fmt.Errorf("agent: volume %s: this host already serves %d volumes, the fan-out its %d-byte device budget was divided by (-max-volumes)",
			id, running, m.cfg.Budget.GuestBytes)
	}

	// The root is <data-dir>/wal and nothing more: wal.SegmentDir appends the volume
	// and the epoch itself, so passing an already-namespaced path produced
	// <data-dir>/wal/<id>/<epoch>/<id>/<epoch>. It went unnoticed because sim.Disk.List
	// matches by prefix, so the test asserting the convention passed on the doubled
	// path — see TestSocketAndWALPathsArePerVolumeAndEpoch, which now asserts the
	// directory exactly.
	root := path.Join(m.cfg.DataDir, "wal")

	// Resume, or start fresh. Which one is decided by the disk, not by configuration:
	// a directory that already holds segments belongs to a previous run of this Agent,
	// and building a fresh log over it would leave every one of those records unread —
	// including ones a guest was told were durable. wal refuses that outright, so the
	// volume would be unusable rather than wrong, but unusable is not the goal.
	dir := wal.SegmentDir(root, [16]byte(u), uint64(d.GetEpoch()))
	existing, err := m.deps.Disk.List(dir)
	if err != nil {
		return nil, fmt.Errorf("agent: volume %s: looking for an existing WAL in %s: %w", id, dir, err)
	}

	// §15: every payload of guest data is sealed with the volume's DEK before any PUT.
	// The unwrap happens here, at attach, and nowhere else (§15.1) — one KMS call
	// outside the data path, and what it returns never leaves memory.
	//
	// It fails the volume rather than degrading it. An Agent that fell back to
	// plaintext would write cleartext into a bucket under a name that says otherwise,
	// and §15.3's crypto-shredding guarantee cannot survive that: the objects would
	// still be readable after the DEK was destroyed.
	enc, err := m.encryptionFor(ctx, id, [16]byte(u))
	if err != nil {
		return nil, err
	}

	var log *wal.Log
	// Every volume with an object store behind it needs its read view built from that
	// store. There is no case where the local segments are the whole truth:
	//
	//   - *resuming* — its own objects hold what truncation reclaimed;
	//   - a *clone* — its parent's objects hold everything it has not written itself;
	//   - and the one that cost the most to find: a volume **promoted to this host**
	//     (§12.3, a drain or a failover) has no local segments and no parent, and
	//     everything it owns was written by a previous epoch on another machine.
	//     Keying this on "resuming, or a clone" made a promoted destination serve
	//     **zeros for its predecessor's whole volume** — no error, no complaint, and
	//     INV-09's guarantee intact in the object store the Agent never asked.
	//
	// A genuinely new volume recovers an empty view, which is the right answer for it,
	// and costs one LIST. That is the price of not having to decide which of the four
	// cases this is from the outside.
	resuming := len(existing) > 0
	needsBase := m.deps.Store != nil
	if needsBase {
		// The durable point is not passed here: it lives in the object store, and
		// fetching it now would mean a round trip before the volume could be served.
		// It arrives with the base (see baseFetch below), which is the only moment it
		// is known. Until then the log reports durable = 0 — an understatement, which
		// is the safe direction for every rule that reads it.
		log, err = wal.ResumeAwaitingBase(m.deps.Disk, root, m.deps.Clock,
			[16]byte(u), uint64(d.GetEpoch()), m.cfg.Budget.Limits(), enc)
		if err != nil {
			return nil, fmt.Errorf("agent: volume %s: resuming the WAL in %s: %w", id, dir, err)
		}
	} else {
		log = wal.NewLog(m.deps.Disk, root, m.deps.Clock, [16]byte(u), uint64(d.GetEpoch()), m.cfg.Budget.Limits())
		if enc != nil {
			log.EnableEncryption(enc)
		}
	}

	// The four §26.2 metrics this log owns — the watermarks, the unflushed bytes, and
	// wal_out_of_space — reach an instrument only through here. It is wired at the one
	// place a Log is built for a real volume: SetRecorder had no production caller at
	// all until now, so those series could not be recorded even once a collector
	// existed. The label is the volume id rather than a counter, because that is what
	// an operator has when they arrive with a volume that is misbehaving.
	log.SetRecorder(m.deps.Recorder, id)

	// Nothing enables a remote path any more (ADR-0026 increment 4.5). A FLUSH is
	// fdatasync and an ACK; the volume reaches the object store when it stops, as one
	// image, and that is the whole of what leaves the host.

	dev, err := blockdev.New(log, d.GetSizeBytes())
	if err != nil {
		_ = log.Close()
		return nil, fmt.Errorf("agent: volume %s: %w", id, err)
	}

	socket := path.Join(m.cfg.SocketDir, id+".sock")
	v := &Volume{
		id: id, epoch: d.GetEpoch(), root: root, socket: socket,
		log: log, dev: dev, enc: enc,
		vol: [16]byte(u), store: m.deps.Store, rnd: m.deps.Rand,
		// Its own lineage until the walk in fetchBase says otherwise. The zero value
		// would be a chunk prefix no reader ever looks under, so the field is never
		// allowed to hold it, not even for the moment before the walk runs.
		lineage: [16]byte(u),
		clk:     m.deps.Clock, rec: m.deps.Recorder,
		done:  make(chan struct{}),
		snaps: map[string]*snapState{},
	}

	// One listener is opened here so a socket that cannot be bound fails Apply rather
	// than disappearing into a goroutine. The supervisor opens the later ones.
	ln, err := m.deps.Listen(socket)
	if err != nil {
		_ = log.Close()
		return nil, fmt.Errorf("agent: volume %s: opening %s: %w", id, socket, err)
	}

	serveCtx, cancel := context.WithCancel(ctx)
	v.cancel = cancel
	go m.supervise(serveCtx, v, ln, d.GetBlockSize())
	if needsBase {
		v.baseDone = make(chan struct{})
		// **Not the serve context, and not a child of ctx either** (the shutdown-publish decision
		// §5). Both are cancelled by the thing that then waits for this fetch's result:
		// stop() cancels the serve context and *then* blocks on baseDone, and ctx here is
		// the Agent loop's context, which SIGTERM cancels before Close() runs at all. So
		// a volume stopped while its base was still loading cancelled its own read, set
		// baseFailed, and publish() then correctly refused to write an image missing
		// everything the volume held before this session — the whole session dropped,
		// silently, and reachable by nothing worse than a volume attached shortly before a
		// restart, a large image, or a slow store.
		//
		// context.WithoutCancel keeps the values (a trace span, the deadline-free lineage)
		// and drops only the cancellation, which is the one thing about the parent that is
		// wrong for this work. Rejected: context.Background(), which would also discard the
		// values, and re-deriving from the parent with a longer deadline, which cannot
		// help — the parent is cancelled, not expired.
		//
		// What bounds it, then: today, only the store's own timeouts and the operator's
		// stop timeout, because the shutdown deadline this should hang off does not exist
		// yet (-shutdown-grace, the next increment of the same spec). That is deliberate
		// and it is the safe direction of the two: a stop that waits too long is visible
		// and recoverable, a stop that publishes an image with a hole in it is neither.
		baseCtx, baseCancel := context.WithCancel(context.WithoutCancel(ctx))
		v.baseCancel = baseCancel
		go func() {
			defer close(v.baseDone)
			m.fetchBase(baseCtx, v, [16]byte(u), uint64(d.GetEpoch()), d)
		}()
	}
	// One line per volume this host starts serving. Everything else the manager logs
	// is an exception, so an Agent that came up correctly said nothing at all about
	// the volumes it opened — which is the state an operator most needs confirmed, and
	// the only evidence available to anything watching from outside the process.
	slog.Info("serving volume",
		"volume_id", id, "epoch", d.GetEpoch(), "socket", socket,
		"resumed", resuming, "encrypted", enc != nil)

	return v, nil
}

// fetchBase rebuilds the read view from the object store and hands it to the log. This
// is the lazy half of installing a base: the volume is already being served,
// and only its *reads* are waiting on this.
//
// It owes the log exactly one InstallBase or FailBase on every path, which is why there
// is no early return that skips both. A log that gets neither parks every read for the
// life of the process.
func (m *VolumeManager) fetchBase(ctx context.Context, v *Volume, volumeID [16]byte, epoch uint64, d *storagev1.DesiredVolume) {
	if m.deps.Store == nil {
		// Local-only mode has no object store to recover from, so the local segments
		// are all there is and they have already been replayed. Nothing to wait for.
		v.baseFailed = true
		v.log.FailBase(errors.New("this Agent has no object store: the read view is whatever the local WAL holds"))
		return
	}

	// The volume's own state is one image, published when it last stopped (ADR-0026).
	// This replaced replaying a chain of WAL objects: there is no chain, and no
	// contiguous prefix to establish — the manifest resolves or it does not.
	//
	// **An image is a delta over the ancestry; it does not supersede it.** That is the
	// sentence this function turned on 2026-08-08 (the chain-depth decision step 3), and the
	// previous one read the other way round: a publish flattened, so a clone's own image
	// held its parent's bytes from its first stop onwards and the parent link stopped
	// describing the read path the moment the manifest existed. Now `image.uploadChunks`
	// walks `view.DeltaOver(ancestry)` and writes down only what this volume's own layers
	// state, so the read view is the ancestry with the image laid over it — in *every*
	// session, not only the one before the first stop.
	//
	// The order below is therefore forced: the chain, then the ancestry it names, then the
	// image over it. A clone cannot find one byte of its *own* image without the first
	// (its chunks live under the lineage root, image.ChunksPrefix), and cannot answer a
	// read of anything it did not write without the second.
	//
	// **What made the old arrangement safe was flattening, and what makes this one safe is
	// the tombstone.** A manifest used to state a DISCARD as *absence*, so sliding a parent
	// underneath a loaded image uncovered every range the guest had erased — §14.6's
	// failure, and the reason `image.Load` returned an unlayered map that `cow.SetBase`
	// refused. Absence in a delta means "ask the layer below" by design, and the erasure is
	// carried explicitly as `Manifest.Discarded`, replayed by the loader as a tombstone. The
	// protection did not go away; it moved into the format, where it also survives a
	// restart, which the refusal never did.
	//
	// **What this costs at attach**, plainly, because step 4's ceiling is chosen against it:
	// a clone now pays, per link of its lineage, one descriptor GET (small, cleartext) plus
	// one snapshot manifest GET plus the chunks that manifest names — and it pays it on
	// every attach, not only on its first. A volume that descends from nothing pays nothing
	// at all: parentChain returns on the empty parent link before it touches the store, and
	// that is nearly every volume.
	//
	// Rejected: keeping the image self-contained by flattening at publish. That is the
	// status quo this replaces, and it costs a copy of the whole inherited dataset at the
	// clone's first stop — measured, in internal/image's clone-cost tests — for a volume
	// that may have written one sector.
	//
	// Rejected: recording the lineage root in the volume's own descriptor and reading that
	// instead, one GET whatever the depth. It duplicates a fact the chain already states —
	// and a duplicate that can disagree with the chain is a way for a volume's chunks to be
	// written under a prefix its own ancestry says is the wrong one. Rejected too: carrying
	// it in DesiredVolume, for the reason in parentChain — the bucket is the authority a
	// rebuild trusts, and ADR-0021 is not the obstacle here, the second copy is.
	chain, err := m.parentChain(ctx, v, d)
	if err != nil {
		// Fail closed. A volume whose lineage cannot be resolved is one whose chunk
		// store cannot be named, so there is no view to serve and no prefix to publish
		// into — proceeding would write this session under the wrong root.
		slog.Error("the volume's lineage could not be resolved; its reads will fail",
			"volume_id", v.id, "parent_snapshot_id", d.GetParentSnapshotId(), "error", err)
		v.baseFailed = true
		v.log.FailBase(err)
		return
	}

	// The lineage root: the volume at the top of the chain, or this one when the chain is
	// empty. It is where this volume's chunks live and what their AAD binds, and it has to
	// be settled before anything is loaded — a clone cannot find one byte of its own image
	// without it.
	v.lineage = lineage.Root(volumeID, chain)

	// The ancestry: every snapshot this volume descends from, composed into one view, or
	// nil for a volume that was created rather than cloned. It is loaded in every session
	// now — it used to be loaded only in the one before a clone's first stop, because
	// after that the flattened image already held it.
	ancestry, err := m.parentView(ctx, v, chain)
	if err != nil {
		// Fail closed, the same rule as a base that cannot be recovered and for the same
		// reason: an empty view where data belongs is a wrong answer a guest cannot
		// detect. It is stricter than it was, and deliberately: a clone whose ancestry is
		// unreadable used to serve happily from its own flattened image, and now that
		// image states only what the clone itself wrote.
		slog.Error("the volume's ancestry could not be materialized; its reads will fail",
			"volume_id", v.id, "parent_snapshot_id", d.GetParentSnapshotId(), "error", err)
		v.baseFailed = true
		v.log.FailBase(err)
		return
	}
	// What this volume's own layers sit over, and what its publish must not write down.
	// Held on the Volume because the publish happens after teardown, when the chain that
	// produced it is long gone (see publish, which passes it to image.Publish).
	v.inherited = ancestry
	if ancestry != nil {
		// What this volume reads out of its ancestors rather than out of an image of its
		// own — the size of the dataset it is riding on, which is now a permanent property
		// of a clone rather than a bill it settles at its first stop.
		//
		// It is a fold over Ranges(), not Cost().Bytes: Cost sums each layer, so a range an
		// ancestor wrote and a nearer one rewrote would count once per layer and a
		// depth-3 lineage would report three times the dataset. The fold is O(extents) —
		// the walk Cost exists to avoid — and that is right here, because this runs once at
		// attach rather than at every guest fsync.
		v.inheritedBytes, v.inheritedFrom = liveBytes(ancestry), d.GetParentSnapshotId()
	}

	// v.enc, not nil: the chunks are sealed under this volume's DEK, and loading them
	// without it would fold ciphertext into the read view (DEV-0019).
	base, man, etag, err := image.Load(ctx, m.deps.Store, v.enc, v.ident(), ancestry)
	switch {
	case errors.Is(err, image.ErrNotPublished):
		// A volume that has never stopped cleanly has no image, which is the first boot
		// and must work. Its base is the ancestry outright — an empty delta over it is the
		// same view — or an empty map for a volume that descends from nothing.
		base, man = ancestry, image.Manifest{}
		if base == nil {
			base = cow.NewIntervalMap()
		}
	case err != nil:
		// Refuse loudly rather than serve zeros. An empty view where data belongs is
		// indistinguishable from a fresh volume, and a guest cannot tell them apart.
		slog.Error("the volume's image could not be loaded; its reads will fail",
			"volume_id", v.id, "epoch", v.epoch, "error", err)
		v.baseFailed = true
		v.log.FailBase(err)
		return
	}
	// The ETag this volume CASes against when it publishes in turn. Carrying it is what
	// makes the fence work: a host that never loaded the manifest publishes with an empty
	// ETag, which is create-only, and loses to the one that did.
	v.imageETag = etag
	durable := man.Sequence
	if err := v.log.InstallBase(base, durable); err != nil {
		slog.Error("the recovered read view could not be installed",
			"volume_id", v.id, "epoch", v.epoch, "error", err)
		v.baseFailed = true
		v.log.FailBase(err)
		return
	}
	// reclaimed_local_bytes is what installing that base gave back to the device: the
	// previous session's segments, whose records this image already holds (wal.InstallBase).
	// It belongs on this line because it is the only place the number can be attributed —
	// an operator watching a host's data directory shrink at start-up otherwise has
	// nothing saying which volume it was, or that it was deliberate.
	//
	// ancestors is how many links this attach walked, and it is on this line because it is
	// the number an operator needs when an attach is slow: every link is a descriptor, a
	// snapshot manifest and the chunks it names, and a read then crosses all of them. It is
	// not the catalog's `chain_depth` — nothing here knows what a Control Plane is
	// (ADR-0021) — it is what this host actually walked, which is the number that can
	// disagree with the catalog and the one worth printing.
	slog.Info("read view recovered from the object store",
		"volume_id", v.id, "epoch", v.epoch, "durable_sequence", durable,
		"reclaimed_local_bytes", v.log.ReclaimedBytes(),
		"cloned_from", d.GetParentSnapshotId(), "ancestors", len(chain),
		"inherited_bytes", v.inheritedBytes)
}

// parentView composes the read view a clone inherits: every snapshot in the lineage it
// descends from, layered oldest first — or nil for a volume that was created rather than
// cloned (§20).
//
// Its one caller reaches it on **every** attach of a clone, because a clone's own image
// is a delta over exactly this view (fetchBase). It used to run once in a clone's whole
// life, on the boot before its first stop, and that was right only while publishing
// flattened.
//
// The composition itself is `lineage.Compose`, and it is there rather than here because
// the operator's FLATTEN composes the same chain in order to write it down
// (`lineage.Flatten`). A second implementation of "layer the ancestry" would be two
// answers to what a clone reads, and only one of them would be the one its guest sees.
// A nil bottom is what a reader wants: the foot of a chain is a link like any other, laid
// over nothing.
func (m *VolumeManager) parentView(ctx context.Context, v *Volume, chain []lineage.Ancestor) (*cow.IntervalMap, error) {
	if len(chain) == 0 {
		return nil, nil //nolint:nilnil // no parent is a shape, not a failure
	}
	view, err := lineage.Compose(ctx, m.deps.Store, v.enc, v.lineage, chain, nil)
	if err != nil {
		return nil, fmt.Errorf("agent: volume %s: %w", v.id, err)
	}
	return view, nil
}

// parentChain returns this volume's ancestry, nearest first, or nil when it descends from
// nothing.
//
// # The desired state says whether to look; the bucket says what is found
//
// The link used to come from `DesiredVolume` outright, with only the links *above* it read
// from the ancestors' descriptors. That was two authorities for one fact, and FLATTEN is
// what made the difference between them observable: a flattened volume's descriptor names
// no parent while the catalog still does, because `volumes.parent_snapshot_id` is
// write-once by construction (`CreateVolume`'s COALESCE) and no catalog write can clear it.
// An Agent that believed the desired state would walk to an ancestry the volume no longer
// reads through — and, worse, resolve a lineage root that is no longer its own, so it would
// look for its *own* chunks under its ex-parent's prefix and find none.
//
// So the desired state's link is the trigger and `lineage.Walk` is the answer. ADR-0021 is
// untouched: the Agent is still told which volumes to serve and still looks nothing up
// against a Control Plane — it reads an object out of the store it already reads its data
// from, which is the same authority `-rebuild-metadata` trusts when the database is the
// thing that was lost (§22.5, INV-20).
//
// A volume the desired state gives no parent walks nothing at all and touches the store
// once less, which is nearly every volume; the extra GET is a clone's, and a clone already
// pays three per link.
func (m *VolumeManager) parentChain(ctx context.Context, v *Volume, d *storagev1.DesiredVolume) ([]lineage.Ancestor, error) {
	if d.GetParentSnapshotId() == "" {
		return nil, nil
	}
	chain, err := lineage.Walk(ctx, m.deps.Store, v.id)
	if err != nil {
		return nil, err
	}
	if len(chain) == 0 {
		// The flattened case, and it is worth a line rather than silence: an operator who
		// ran FLATTEN and has not yet corrected the catalog needs to see that the two
		// disagree, and that the bucket is what the Agent obeyed.
		slog.Info("the catalog says this volume descends from a snapshot and its descriptor says it descends from nothing; it has been flattened, and its own image is what answers every read",
			"volume_id", v.id, "parent_snapshot_id", d.GetParentSnapshotId())
	}
	return chain, nil
}

// supervise runs the vhost server and restarts it when it returns.
//
// vhost.Server.Serve returns on any session error, and a guest reconnects to the socket
// — so without this the first protocol error would end the volume's service for the
// lifetime of the process, silently. Each attempt gets a fresh listener because the
// previous one is closed by whatever ended the session (including Serve's own
// cancellation path, which closes the listener to unblock Accept).
func (m *VolumeManager) supervise(ctx context.Context, v *Volume, first vhost.Listener, blockSize int32) {
	defer close(v.done)

	ln := first
	for {
		if ctx.Err() != nil {
			_ = ln.Close()
			return
		}

		srv, err := vhost.NewServer(ln, vhost.Config{
			Backend:   v.dev,
			Mapper:    m.deps.Mapper,
			Serial:    v.id,
			BlockSize: uint32(blockSize),
		}, m.deps.EventFD)
		if err != nil {
			// A configuration the device refuses will be refused again identically;
			// retrying it is a busy loop that logs forever.
			slog.Error("volume cannot be served", "volume_id", v.id, "epoch", v.epoch, "error", err)
			_ = ln.Close()
			return
		}

		err = srv.Serve(ctx)
		_ = ln.Close()
		if ctx.Err() != nil {
			return
		}
		slog.Warn("vhost session ended; re-listening",
			"volume_id", v.id, "epoch", v.epoch, "socket", v.socket, "error", err)

		ln, err = m.deps.Listen(v.socket)
		if err != nil {
			// Nothing left to serve on. The volume stays in the desired state, so the
			// next Apply that finds no runtime for it starts one again.
			slog.Error("cannot re-open the volume's socket",
				"volume_id", v.id, "socket", v.socket, "error", err)
			return
		}
	}
}

// Fence stops serving the named volumes, because the Control Plane has refused their
// reports: this host is not their writer anymore — the epoch moved on, the primary
// changed, or the volume is unknown to the fleet (§12.3, §16). This is the trigger that
// stops a guest's I/O — the Control Plane's view, not this host's lease clock; the log's
// own self-fencing stops only the durable path, deliberately (see wal.Log's `fenced`).
//
// **It stops reads as well as writes, and it takes the socket with it.** That is the
// safe side of a choice with no comfortable option. A read of already-written bytes
// breaks no durability rule, but it is a stale read handed to a guest whose volume now
// has a different writer somewhere else, and the guest has no way to tell. The cost is
// paid by that guest: QEMU reconnects on its own, finds nothing listening, and its I/O
// stalls rather than being answered by a host with no authority to answer it.
//
// A volume re-granted to this host at a higher epoch starts a fresh runtime on the next
// Apply, under the new epoch's WAL root, and the guest's pending reconnect succeeds.
func (m *VolumeManager) Fence(ctx context.Context, volumeIDs []string) error {
	var errs []error
	for _, id := range volumeIDs {
		m.mu.Lock()
		v, running := m.volumes[id]
		if running && v.epoch > m.fencedEpoch[id] {
			m.fencedEpoch[id] = v.epoch
		}
		m.mu.Unlock()
		if !running {
			continue
		}
		slog.Warn("volume fenced; tearing its runtime down",
			"volume_id", id, "epoch", v.epoch)
		if err := m.remove(ctx, id); err != nil {
			errs = append(errs, fmt.Errorf("agent: fencing volume %s: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

// remove stops one runtime and drops it. It is safe to call for a volume that is not
// running.
//
// This is the *reconciliation* teardown — a promotion, a fence, a volume the Control
// Plane stopped listing — and it makes exactly one publish attempt. It deliberately does
// **not** hold the way Close does, and does not return the publish's error either:
//
//   - it runs on the reconcile goroutine, so a retry loop here is a heartbeat not sent,
//     a lease not renewed, and every *other* volume on this host fenced for the sake of
//     one that has already left it;
//   - and in every case that reaches here the volume has moved on. A fenced or promoted
//     volume belongs to another host now, which is the same reason ErrSuperseded is not
//     retried: refusing to release buys nothing, because nobody is coming back for this
//     directory's copy.
//
// Returning the error instead of logging it would stop the promotion it is part of —
// Apply skips starting the new epoch's runtime when remove fails — so a store having a
// bad minute would leave the host serving neither epoch.
func (m *VolumeManager) remove(ctx context.Context, id string) error {
	m.mu.Lock()
	v, ok := m.volumes[id]
	if ok {
		delete(m.volumes, id)
	}
	m.mu.Unlock()
	if !ok {
		return nil
	}
	v.quiesce()
	// WithoutCancel: ctx here is the reconcile cycle's, and this publish must outlive
	// the cycle rather than be cancelled by it — a cancelled publish is the silent data
	// loss the whole spec is about (same rule as the base fetch's context, see start).
	m.awaitBase(context.WithoutCancel(ctx), v)
	if err := m.publishAttempt(context.WithoutCancel(ctx), v); err != nil {
		slog.Error("this volume's image could not be published; this session's writes are only in this host's local WAL, and this host is no longer the volume's writer",
			"volume_id", id, "epoch", v.epoch, "data_dir", m.cfg.dataDir(), "error", err)
	}
	return v.release()
}

// Volumes implements VolumeSource over the live runtimes, ordered by volume id
// (INV-02). This is what makes a heartbeat honest: before it, the binary reported an
// empty set forever — every heartbeat said remote_backlog=0 and carried zero reports.
func (m *VolumeManager) Volumes(context.Context) ([]VolumeStatus, error) {
	m.mu.Lock()
	out := make([]VolumeStatus, 0, len(m.volumes))
	for _, v := range m.volumes {
		out = append(out, v.Status())
	}
	m.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].VolumeID < out[j].VolumeID })
	return out, nil
}

// Device returns the block device serving one volume, if it is running.
func (m *VolumeManager) Device(volumeID string) (*blockdev.Device, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.volumes[volumeID]
	if !ok {
		return nil, false
	}
	return v.dev, true
}

// Close stops every runtime, publishes every session it was serving, and **does not
// return until it has** — holding this host's claim on the data directory for as long as
// that takes. It is idempotent.
//
// That is the decision in the shutdown-publish decision "REVIEWED AND DECIDED", and its
// mechanism is this function's shape: a flock is released by the kernel when the process
// exits, so "refuse to release the lock" can only mean "do not exit", which can only mean
// "do not return from here". Everything else follows — the retries, the holding line, and
// the fact that a store outage stops a rolling restart fleet-wide rather than silently
// costing a session per host.
//
// ctx is the operator's override and nothing else: cancelling it abandons whatever has
// not published, names it, and returns ErrPublishAbandoned. `cmd/volume-agent` wires it
// to the *second* signal. Abandoning is safe — the records are on disk and the next
// incarnation re-attaches at the same epoch (ADR-0024) and republishes — which is why
// the escape can exist at all; what is not safe is abandoning silently.
func (m *VolumeManager) Close(ctx context.Context) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	held := make([]*Volume, 0, len(m.volumes))
	for _, v := range m.volumes {
		held = append(held, v)
	}
	m.mu.Unlock()
	// INV-02: the order volumes are published in is the order an operator reads about
	// them, and a map's is a different one every run.
	sort.Slice(held, func(i, j int) bool { return held[i].id < held[j].id })

	// Every volume stops serving *first*, before any of them publishes. A guest whose
	// host is going away should lose its device at the moment the host decided to go,
	// not after some other volume's upload — and until this loop has run, a volume later
	// in the list is still taking writes that its own ViewAtRest would then have to
	// include.
	for _, v := range held {
		v.quiesce()
	}

	errs := m.publishHeld(ctx, held)
	// Released last, after every runtime is down and every image is settled: while this
	// Agent still has a session that only exists here, it still owns the directory.
	if m.lock != nil {
		if err := m.lock.Close(); err != nil {
			errs = append(errs, fmt.Errorf("agent: releasing %s: %w", m.cfg.dataDir(), err))
		}
	}
	return errors.Join(errs...)
}

// ErrPublishAbandoned means an operator told this Agent to stop waiting while sessions
// were still unpublished. The data is in the local WAL and a restart republishes it,
// which is what makes the process's non-zero exit an instruction rather than an epitaph.
var ErrPublishAbandoned = errors.New("agent: the shutdown publish was abandoned; these sessions exist only in this host's local WAL")

// publishRetryInitial and publishRetryMax bound the wait between rounds. The cap is what
// makes the holding line a heartbeat rather than a countdown that goes quiet: at 30s an
// operator watching a stuck host gets a line every half minute for as long as it lasts,
// and the store's own timeouts already dominate the cost of an attempt.
const (
	publishRetryInitial = 2 * time.Second
	publishRetryMax     = 30 * time.Second
)

// publishHeld publishes every quiesced volume, retrying until each one is either in the
// object store or is a failure retrying cannot fix.
//
// **A round tries every volume that is still unpublished, one at a time, and then waits.**
// The two alternatives were rejected for different reasons. Publishing them concurrently
// multiplies the bandwidth a stopping host takes from the ones still serving, and there
// is no io-class scheduler left to bound it (deleted in ADR-0026 4.5); worse, the
// per-volume lines interleave, and "which volume is stuck" is the first question an
// incident asks. Publishing them one *to completion* before starting the next — which is
// what a naive loop over stop() does — strands every other volume's image behind the
// slowest one, so a failure specific to volume A means volume B is never even attempted
// and the operator hears nothing about it.
func (m *VolumeManager) publishHeld(ctx context.Context, held []*Volume) []error {
	var errs []error
	remaining := held
	backoff := publishRetryInitial
	since := m.deps.Clock.Now()

	for attempt := 1; ; attempt++ {
		var stuck []*Volume
		for _, v := range remaining {
			if v.store != nil {
				// What makes a hang attributable to a volume rather than to "the Agent is
				// stuck". A local-only Agent says nothing, because it publishes nothing.
				slog.Info("volume image publish started",
					"volume_id", v.id, "attempt", attempt, "volumes_remaining", len(remaining))
			}
			m.awaitBase(ctx, v)
			err := m.publishAttempt(ctx, v)
			switch {
			case err == nil:
				errs = append(errs, m.drop(v))
			case errors.Is(err, image.ErrSuperseded), errors.Is(err, ErrNoReadView):
				// The two failures a retry cannot survive, and they are opposites.
				// ErrSuperseded: another writer published over us, so retrying would
				// replace a newer image with an older one — the single thing INV-10
				// exists to prevent — and holding this directory buys nothing, because
				// the volume is being served by a host that does not care what is in it.
				// ErrNoReadView: the fetch that would have made the image complete is
				// already over, and nothing in this process will attempt another.
				slog.Error("this volume's image will not be published by this Agent; its session stays in the local WAL",
					"volume_id", v.id, "epoch", v.epoch, "data_dir", m.cfg.dataDir(), "error", err)
				errs = append(errs, err, m.drop(v))
			default:
				slog.Error("volume image publish failed",
					"volume_id", v.id, "attempt", attempt, "retry_in", backoff, "error", err)
				stuck = append(stuck, v)
			}
		}
		remaining = stuck
		if len(remaining) == 0 {
			return errs
		}

		// A process that is deliberately refusing to die must say so on a schedule. One
		// that says it once and goes quiet is indistinguishable from one that hung, and
		// the whole reason the owner chose holding over exiting is that a stuck host
		// should be *legible* — this line and the heartbeat are the two things that make
		// it so.
		slog.Warn("agent is holding unpublished data and will not release its data directory",
			"volumes", len(remaining), "volume_ids", volumeIDs(remaining),
			"attempts", attempt, "oldest_wait", m.deps.Clock.Now().Sub(since),
			"data_dir", m.cfg.dataDir())

		if err := m.deps.Clock.Sleep(ctx, backoff); err != nil {
			// The operator's override (a second signal), or a caller that gave up on us.
			// Named, one line per volume, because "which sessions did I just agree to
			// leave behind" is the only question this moment is about.
			for _, v := range remaining {
				w := v.log.Watermarks()
				slog.Error("abandoning this volume's unpublished session on request; it stays in this host's local WAL and a restart republishes it",
					"volume_id", v.id, "epoch", v.epoch, "local_sequence", w.Local,
					"data_dir", m.cfg.dataDir(),
					// Relative to data_dir, because that is how this process addresses its
					// own disk: everything it opens is inside the Disk rooted there.
					"wal", wal.SegmentDir(v.root, v.vol, uint64(v.epoch)))
				errs = append(errs, fmt.Errorf("%w: volume %s at sequence %d", ErrPublishAbandoned, v.id, w.Local), m.drop(v))
			}
			return errs
		}
		if backoff *= 2; backoff > publishRetryMax {
			backoff = publishRetryMax
		}
	}
}

// drop takes a settled volume out of the served set and releases it. Out of the map
// first: until it leaves, Volumes still reports it — which is deliberate while it is
// being held (see Volumes) and wrong the instant its log is closed.
func (m *VolumeManager) drop(v *Volume) error {
	m.mu.Lock()
	delete(m.volumes, v.id)
	m.mu.Unlock()
	return v.release()
}

func volumeIDs(vs []*Volume) []string {
	out := make([]string, 0, len(vs))
	for _, v := range vs {
		out = append(out, v.id)
	}
	return out
}

// awaitBase waits for the volume's read view to resolve, bounded by ShutdownGrace.
//
// The wait itself is not optional: publishing before the base lands writes an image
// missing everything the volume held before this session, over the manifest that base
// came from. What is new here is the bound. A fetch against a store that answers nothing
// — not refusing, just never replying — would otherwise make the teardown wait forever
// with nothing printed, which is the one shape "hold and retry" cannot tell apart from
// "hung". When the grace runs out the fetch is cancelled, fetchBase records a failed view,
// and the volume is reported as one this Agent will not publish (ErrNoReadView) rather
// than published incomplete.
func (m *VolumeManager) awaitBase(ctx context.Context, v *Volume) {
	if v.baseDone == nil {
		return
	}
	select {
	case <-v.baseDone:
		return
	default:
	}
	slog.Info("waiting for this volume's read view before publishing its image",
		"volume_id", v.id, "grace", m.cfg.ShutdownGrace)

	var expired <-chan clock.Instant
	if m.cfg.ShutdownGrace > 0 {
		t := m.deps.Clock.NewTimer(m.cfg.ShutdownGrace)
		defer t.Stop()
		expired = t.C()
	}
	select {
	case <-v.baseDone:
	case <-expired:
		v.baseCancel()
		<-v.baseDone // it owes the log a FailBase; waiting is what makes baseFailed true
	case <-ctx.Done():
		v.baseCancel()
		<-v.baseDone
	}
}

// publishAttempt makes one bounded attempt to put a volume's image in the object store.
//
// The bound is armed on the *injected* clock rather than with context.WithTimeout, which
// reads the real one: a deadline the DST harness cannot advance is a deadline that either
// never fires in simulation or fires by wall-clock accident, and INV-01 exists precisely
// so time is one of the things a scenario drives. A zero ShutdownGrace arms nothing,
// which is what leaves every in-process caller's behaviour exactly as it was.
func (m *VolumeManager) publishAttempt(ctx context.Context, v *Volume) error {
	if m.cfg.ShutdownGrace <= 0 {
		return v.publish(ctx)
	}
	actx, cancel := context.WithCancel(ctx)
	defer cancel()
	t := m.deps.Clock.NewTimer(m.cfg.ShutdownGrace)
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-t.C():
			cancel()
		case <-done:
			t.Stop()
		}
	}()
	return v.publish(actx)
}

var _ VolumeSource = (*VolumeManager)(nil)

// ensureSnapshot starts the snapshot the Control Plane asks for, at most once.
//
// The request repeats every few seconds until the catalog row leaves CREATING, so the
// map is what makes the second, third and hundredth arrival free. The upload runs on its
// own goroutine because the caller is the reconcile loop: an Agent that blocked there
// for the length of an upload would miss the heartbeat that renews its lease, and be
// fenced for doing what it was told.
//
// A snapshot id that is no longer asked for is dropped, which is the only thing that
// keeps this map from growing for the life of the process.
func (m *VolumeManager) ensureSnapshot(v *Volume, snapshotID string) {
	v.snapMu.Lock()
	defer v.snapMu.Unlock()
	v.pending = snapshotID
	for id := range v.snaps {
		if id != snapshotID {
			delete(v.snaps, id)
		}
	}
	if snapshotID == "" {
		return
	}
	if _, started := v.snaps[snapshotID]; started {
		return
	}
	if m.deps.Store == nil {
		// Nothing to publish into. Reported as a failure rather than left silent: a
		// snapshot nobody can take is an operator's problem, and a row stuck in
		// CREATING is how it stays invisible.
		v.snaps[snapshotID] = &snapState{done: true, err: errors.New("agent: this host has no object store to snapshot into")}
		return
	}
	v.snaps[snapshotID] = &snapState{}
	v.snapWG.Add(1)
	go func() {
		defer v.snapWG.Done()
		// Not the reconcile context: it is scoped to one cycle, and this outlives it.
		seq, err := m.snapshot(context.Background(), v, snapshotID)
		v.snapMu.Lock()
		defer v.snapMu.Unlock()
		if snap := v.snaps[snapshotID]; snap != nil {
			snap.done, snap.sequence, snap.err = true, seq, err
		}
	}()
}

// snapshot freezes a running volume under a name and publishes the frozen copy.
//
// It is §19 end to end and it does not stop the guest: Freeze captures the sequence and
// swaps the read view under the volume's lock, the guest carries on writing into a fresh
// layer, and the upload happens afterwards against a map nothing can mutate. §2's "pausa
// de I/O por snapshot ~0" is that swap.
//
// The snapshot does *not* become the volume's image. They are different things with
// different lives: the image is where this volume resumes, the snapshot is a named point
// others descend from. Coupling them would make taking a snapshot change what a restart
// reads, which is not something anybody asked for.
func (m *VolumeManager) snapshot(ctx context.Context, v *Volume, snapshotID string) (uint64, error) {
	if v.baseDone != nil {
		// Same hazard publish() waits for, and worse here: a view frozen before the base
		// lands is missing everything the volume held before this session, and it would
		// be written down under a name other volumes clone from.
		<-v.baseDone
	}
	if v.baseFailed {
		return 0, fmt.Errorf("agent: volume %s never resolved its read view, so a snapshot of it would be missing everything it held before this session", v.id)
	}

	// §19's two mandatory metrics (§26.2). The pause is what the guest experiences —
	// Freeze holds the volume's lock — and it is measured around *Freeze alone*, not
	// around the whole operation: an earlier version of this code measured a function
	// that captured and uploaded, and reported a pause of zero because the simulated
	// clock only advances when something works. A pause metric that cannot distinguish
	// the capture from the upload is the metric an incident needs and does not have.
	vol := obs.String("volume", v.id)
	pauseStart := m.deps.Clock.Now()
	frozen, seq, err := v.log.Freeze()
	m.deps.Recorder.Observe(ctx, "snapshot_pause_duration_seconds", m.deps.Clock.Now().Sub(pauseStart).Seconds(), vol)
	if err != nil {
		return 0, fmt.Errorf("agent: volume %s: freezing at a sequence: %w", v.id, err)
	}
	v.sayWhatThisImageCosts(frozen)
	publishStart := m.deps.Clock.Now()
	defer func() {
		m.deps.Recorder.Observe(ctx, "snapshot_publish_duration_seconds", m.deps.Clock.Now().Sub(publishStart).Seconds(), vol)
	}()
	if _, err := image.PublishSnapshot(ctx, m.deps.Store, v.rnd, v.enc, v.ident(), frozen, v.inherited, seq, snapshotID); err != nil {
		if !errors.Is(err, image.ErrSnapshotExists) {
			return 0, fmt.Errorf("agent: volume %s: publishing snapshot %s: %w", v.id, snapshotID, err)
		}
		// Already published — this host restarted, or a previous incarnation got there.
		// The manifest is immutable (INV-16), so the sequence to report is *its* one,
		// not the one just frozen: reporting the later number would put a point in the
		// catalog that no copy corresponds to.
		_, man, lerr := image.LoadSnapshot(ctx, m.deps.Store, v.enc, v.ident(), snapshotID)
		if lerr != nil {
			return 0, fmt.Errorf("agent: volume %s: snapshot %s exists but could not be read: %w", v.id, snapshotID, lerr)
		}
		seq = man.Sequence
	}
	slog.Info("snapshot published", "volume_id", v.id, "snapshot_id", snapshotID, "sequence", seq)
	return seq, nil
}

// Snapshot takes one snapshot and waits for it, for a caller that holds the volume id
// rather than the runtime. Production does not use it — the Control Plane asks through
// desired state, and ensureSnapshot is what answers — so it exists for the lanes that
// drive a snapshot directly and need the published fact before they assert on it.
func (m *VolumeManager) Snapshot(ctx context.Context, volumeID, snapshotID string) (uint64, error) {
	m.mu.Lock()
	v, ok := m.volumes[volumeID]
	m.mu.Unlock()
	if !ok {
		return 0, fmt.Errorf("agent: volume %s is not being served here", volumeID)
	}
	if m.deps.Store == nil {
		return 0, fmt.Errorf("agent: volume %s has no object store to snapshot into", volumeID)
	}
	return m.snapshot(ctx, v, snapshotID)
}
