// Command control-plane serves the Agent-facing RPC surface (ADR-0018) over the
// Control Plane libraries that already exist: metadata/pg for the authority and
// controlplane.Elector for the term.
//
// It is the minimum viable server — enough for an Agent to have something to talk
// to — and like every main in this tree it is thin: the behaviour lives in
// internal/cpserver, this file builds the real Postgres pool, the real object store,
// and the HTTP listener.
//
// The term is taken through the Elector, never through metadata.Store's
// AcquireLeadership directly (ADR-0011): a term that no object in the bucket
// witnesses is a term a restored database can hand out twice, and two processes
// holding the same term pass every guard this system has.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"path/filepath"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/cpserver"
	"github.com/spin-stack/storage/internal/lifecycle"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/metadata/pg"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/placement"
	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/real"
	"github.com/spin-stack/storage/internal/storecfg"
)

// version is the build identity. Overridden at link time with -ldflags.
var version = "dev"

func main() {
	if err := run(); err != nil {
		slog.Error("control-plane exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		listen      = flag.String("listen", ":8080", "address to serve the Connect API on")
		databaseDSN = flag.String("database-url", os.Getenv("DATABASE_URL"),
			"PostgreSQL connection string (required; defaults to $DATABASE_URL)")
		// No default, and that is the answer rather than an omission. This value is what
		// the leadership row records as its holder, so two processes sharing one identity
		// each read a leader row bearing their own name and each conclude they are still
		// leading — the split-brain the Elector exists to make impossible, reintroduced by
		// a convenience. Any default that could be computed here (a hostname, a constant)
		// is exactly the kind two processes collide on. Saying "required" in the help is
		// the whole fix: it costs one word and it makes -h the place an operator finds
		// out, instead of a process that starts and dies.
		holderID = flag.String("holder-id", "",
			"identity of this Control Plane process, distinct per process (required, except with -fleet-status)")
		leaseTTL = flag.Duration("lease-ttl", 30*time.Second, "host lease TTL granted on heartbeat")

		// The Elector needs an object store to witness the term (ADR-0011). Either
		// a bucket or, for a single-machine dev run, a directory.

		shutdownGrace = flag.Duration("shutdown-grace", 10*time.Second, "how long to let in-flight requests finish")

		// seed-volume: provision one volume and exit. It is not an admin API — that
		// arrives with volctl — but without it GetDesiredState answers every Agent
		// with an empty list forever, so nothing downstream of this binary can be
		// exercised at all.
		seedVolume = flag.Bool("seed-volume", false, "provision one volume and exit, instead of serving")
		seedHost   = flag.String("seed-host", "", "with -seed-volume: the host that will serve it (a UUIDv7)")
		seedSize   = flag.Int64("seed-size", 1<<30, "with -seed-volume: capacity in bytes (a whole number of 512-byte sectors)")
		seedBlock  = flag.Int("seed-block-size", 4096, "with -seed-volume: logical block size")
		// One command needs it now. -flatten-volume and -delete-volume were the other
		// two, and both were rewrites of a chunked image this system no longer produces
		// — they went with it. Seeding still needs the key, because a volume's DEK is
		// wrapped under it before the row is written and an Agent that cannot unwrap it
		// has a volume it can never open.
		kekFile = flag.String("kek-file", "",
			"file holding the 32-byte key-encryption key (required for -seed-volume)")

		// snapshot-volume: record a snapshot request and exit, the same shape as
		// -seed-volume and for the same reason. The snapshot itself is taken by the
		// Agent that serves the volume — this only writes the row it converges on.
		snapshotVolume = flag.String("snapshot-volume", "", "record a snapshot request for this volume and exit, instead of serving")

		// clone-snapshot: create a volume from a published snapshot and exit. The host
		// is not a flag on purpose — §20's placement order decides it, and the whole
		// point of that order is that an operator naming a host would override the one
		// decision that makes a clone boot quickly.
		cloneSnapshot = flag.String("clone-snapshot", "", "create a volume from this snapshot and exit, instead of serving")
		deleteVolume  = flag.String("delete-volume", "", "crypto-shred this volume and exit: its wrapped DEK is deleted, which is what makes leaving its layers safe")

		// rebuild-metadata: reconstruct the catalog from the bucket and exit (§22.5,
		// INV-20). It is why descriptors are written at all — without it a lost
		// PostgreSQL is unrecoverable even though every byte of every volume is intact.
		rebuildMetadata = flag.Bool("rebuild-metadata", false, "rebuild the volume and snapshot catalog from the object store, and exit")
		oversubscribe   = flag.Float64("max-oversubscription", 1.0,
			"with -clone-snapshot / -attach-volume: committed/total ceiling a host may reach (§28.2)")
		// The second ceiling is the measured one (ADR-0013 §3): what the host's last
		// heartbeat said its device holds, which is what actually runs out. It is a
		// separate flag rather than a share of the one above because the two are not
		// the same quantity — promises are oversubscribed on purpose and bytes are
		// not — and because the right value depends on the deployment: a dedicated
		// NVMe per host tolerates a higher fill than a filesystem shared with logs
		// and images, whose other tenants no truncation of ours can reclaim.
		// The device-pressure cordon band (ADR-0013 §3). Flags rather than constants
		// because the first CI run this repository ever had failed every placement in
		// the e2e lane: a GitHub runner's disk is 87% full, the Agent measures the
		// filesystem holding --data-dir including other tenants, so every host cordoned
		// itself on its first heartbeat. The product was right and the lane had been
		// relying on a developer's roomy /tmp; see cpserver.Band.
		cordonRatio = flag.Float64("cordon-used-ratio", cpserver.DefaultBand().Cordon,
			"used ratio at which the Control Plane stops placing new volumes on a host (ADR-0013 §3)")
		uncordonRatio = flag.Float64("uncordon-used-ratio", cpserver.DefaultBand().Uncordon,
			"used ratio at which a host cordoned for pressure is placed on again; must be below -cordon-used-ratio, and the gap is the hysteresis")

		maxUsedRatio = flag.Float64("max-used-ratio", placement.DefaultMaxUsedRatio,
			"with -clone-snapshot / -attach-volume: used/total a host may already measure and still receive a volume (ADR-0013)")

		// fleet-status: the only read-only one-shot here, and the only fleet-wide read
		// this repository has that is not psql. fleet.go carries the reasoning.
		fleetStatus = flag.Bool("fleet-status", false, "print the fleet's hosts, volumes and unfinished snapshots, and exit")

		// detach-volume / attach-volume: the two halves of a volume's placement, the
		// same one-shot shape as the flags above. They exist because primary_host_id
		// was write-once — CreateVolume set it and its converging upsert protected it —
		// so an attach was permanent, and the volumes -rebuild-metadata restores with
		// no host (it says so on the way out) could never be given one.
		//
		// They are two flags rather than one because the store refuses a straight
		// hand-over (metadata.ErrAlreadyPlaced carries the reason: a host learns it has
		// lost a volume only on its next poll, so a single write would have two Agents
		// serving it). Detaching is what makes the release safe — the Agent's teardown
		// publishes the session's image before it drops the socket — and an operator
		// re-placing the volume has to wait for that to have happened.
		//
		// -attach-host is optional, and its absence is the case the rebuild leaves
		// behind: a catalog restored from the bucket records no placement at all, so an
		// operator with forty volumes has forty hosts to invent. Without it the
		// placement order decides, exactly as -clone-snapshot's does, under the same two
		// ceiling flags above. controlplane.Place carries the reasoning for both halves.
		detachVolume = flag.String("detach-volume", "", "clear this volume's placement and exit, instead of serving")
		attachVolume = flag.String("attach-volume", "", "place this volume and exit, instead of serving")
		attachHost   = flag.String("attach-host", "",
			"with -attach-volume: the host that will serve it (a UUIDv7); empty asks placement to choose")

		// cordon-host / uncordon-host: the human half of ADR-0013 §5. The automatic half
		// has run since wave 3 — a host past 70% used cordons itself on its next heartbeat
		// — and the half a human drives had no command at all, so the authority split the
		// cordon_reason column exists to enforce could only ever be exercised by the loop.
		// cordon.go carries the reasoning.
		cordonHost   = flag.String("cordon-host", "", "stop placing new volumes on this host and exit, instead of serving")
		uncordonHost = flag.String("uncordon-host", "", "let this host take new volumes again and exit, instead of serving")

		otlpEndpoint = flag.String("otlp-endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
			"OTLP/HTTP collector to export metrics to, e.g. http://collector:4318 (empty disables telemetry)")
	)
	var storeFlags storecfg.Flags
	storeFlags.Register(flag.CommandLine)
	flag.Parse()

	switch {
	// -fleet-status is exempt from both. It identifies nobody, because it takes no
	// term and writes nothing, and demanding an identity for a read is friction in
	// front of the one command an operator runs when they do not yet know what is
	// wrong. The DSN it still needs: the catalog is what it reports.
	case *holderID == "" && !*fleetStatus:
		return errors.New("-holder-id is required")
	case *databaseDSN == "":
		return errors.New("-database-url (or $DATABASE_URL) is required")
	}

	// Validated on the flags rather than at the first heartbeat: an inverted or
	// zero-width band is a typo whose symptom is a host changing state on every
	// heartbeat, which is a confusing thing to debug from the other end.
	band := cpserver.Band{Cordon: *cordonRatio, Uncordon: *uncordonRatio}
	if err := band.Validate(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Telemetry, before anything that records — the same wiring cmd/volume-agent has, for
	// the same reason: a component that takes its Recorder before the exporter exists
	// holds a no-op for the life of the process.
	//
	// This binary is mostly one-shots, and that decides what a series from it can mean
	// rather than disqualifying it. `chain_depth` changes only when the Control Plane
	// changes it (controlplane.Clone says why), so recording at the change and flushing at
	// exit is a complete record of a value that is not allowed to move in between — where
	// a poller would need a loop this process does not have. NewOTLPMetricExporter returns
	// (nil, nil) for an empty endpoint and NewProvider takes that nil, so an operator who
	// has no collector runs exactly the command they ran before.
	exporter, err := real.NewOTLPMetricExporter(ctx, *otlpEndpoint)
	if err != nil {
		return err
	}
	telemetry, err := obs.NewProvider("control-plane", exporter)
	if err != nil {
		return err
	}
	defer func() {
		// WithoutCancel: a one-shot's only sample is recorded microseconds before this
		// runs, and the serving path's shutdown starts with the SIGTERM that cancelled
		// ctx. Logged and never fatal — a collector that is down must not change what a
		// clone's exit code says about whether the volume exists.
		if serr := telemetry.Shutdown(context.WithoutCancel(ctx)); serr != nil {
			slog.Error("flushing metrics", "error", serr)
		}
	}()

	pool, err := pgxpool.New(ctx, *databaseDSN)
	if err != nil {
		return fmt.Errorf("connecting to PostgreSQL: %w", err)
	}
	defer pool.Close()

	md := pg.New(pool)

	// Before the object store is opened, deliberately: a report of the catalog reads
	// no object, and storeFlags.Open refuses the empty case, so leaving it below would
	// make an operator name a bucket this command never touches — during an incident,
	// possibly the very bucket that is unreachable.
	if *fleetStatus {
		// The lease TTL is what the report renders liveness against, and it is this
		// flag rather than a second one: a threshold an operator can set differently
		// on the reading side from the serving side is a threshold that would let the
		// report call a host dead that the Control Plane is still leasing to.
		return fleetReport(ctx, md, os.Stdout, *leaseTTL)
	}

	// storeFlags.Open refuses the empty case: the object store is the recovery
	// authority (§5.8) and every path below needs it.
	store, err := storeFlags.Open(ctx)
	if err != nil {
		return err
	}

	// Seeding is an admin command, not a leader taking over, and the difference is not
	// cosmetic: AcquireLeadership increments the term unconditionally — for the same
	// holder id too — so a seed run against a live deployment would leave the *serving*
	// Control Plane holding a stale term, with every write it makes from that moment on
	// refused as ErrStaleTerm until someone restarts it. Provisioning a volume must not
	// take down the fleet's Control Plane. It borrows the current term instead, and
	// fails if there is nobody to borrow from.
	if *seedVolume {
		leader, lerr := md.GetLeader(ctx)
		if lerr != nil {
			return fmt.Errorf("-seed-volume needs a Control Plane to be leading (start one first): %w", lerr)
		}
		slog.Info("seeding under the current term", "holder_id", leader.HolderID, "term", leader.Term)
		// No -seed-local-durability: ADR-0026 left one ACK contract, so the flag
		// selected between a mode that exists and a mode that does not. Keeping it as
		// a no-op would be worse than removing it — an operator who passes it is told
		// nothing, and believes they changed what a FLUSH means.
		return seed(ctx, md, store, *kekFile, controlplane.VolumeSpec{
			SizeBytes: *seedSize,
			BlockSize: int32(*seedBlock),
			HostID:    *seedHost,
		}, leader.Term)
	}

	if *snapshotVolume != "" {
		leader, lerr := md.GetLeader(ctx)
		if lerr != nil {
			return fmt.Errorf("-snapshot-volume needs a Control Plane to be leading (start one first): %w", lerr)
		}
		// Both ids are generated here rather than taken as flags: the snapshot id is
		// what the request is named by, and the request id is §18's idempotency key, so
		// a retry of *this command* is a new request while a retry of the write is not.
		snap, serr := controlplane.RequestSnapshot(ctx, md, leader.Term, *snapshotVolume, ids.New().String(), ids.New().String())
		if serr != nil {
			return serr
		}
		slog.Info("snapshot requested",
			"snapshot_id", snap.SnapshotID, "volume_id", snap.VolumeID, "epoch", snap.Epoch)
		return nil
	}

	if *rebuildMetadata {
		// Under the *current* term, like the other admin commands: a rebuild is not a
		// leader taking over, and incrementing the term would leave the serving Control
		// Plane's writes refused as stale.
		leader, lerr := md.GetLeader(ctx)
		if lerr != nil {
			return fmt.Errorf("-rebuild-metadata needs a Control Plane to be leading (start one first): %w", lerr)
		}
		// The KEK, and it is not optional here. A rebuild runs precisely when the catalog
		// is gone, so whatever it copies out of the bucket becomes the *only* record of a
		// volume's key material. A poisoned `dek_wrapped` recorded by a rebuild is
		// permanent in a way the same object sitting in a bucket is not, which is why
		// this path is the one that must unwrap every descriptor before believing it.
		if *kekFile == "" {
			return errors.New("-rebuild-metadata needs -kek-file: it verifies every descriptor's wrapped DEK before recording it, and cannot do that without the key that wraps them")
		}
		kek, kerr := readKEK(*kekFile)
		if kerr != nil {
			return kerr
		}
		sum, rerr := controlplane.RebuildMetadata(ctx, md, store,
			crypto.NewDevKMS(kek, crypto.KEKID(kek)), leader.Term)
		if rerr != nil {
			return rerr
		}
		// Placement is not in any object, so nothing comes back with a host. Said out
		// loud because an operator reading "rebuilt 40 volumes" would otherwise expect
		// the fleet to start serving them.
		slog.Info("catalog rebuilt from the object store; no volume has a primary host — place them to resume serving",
			"volumes", sum.Volumes)
		return nil
	}

	if *detachVolume != "" || *attachVolume != "" {
		if *detachVolume != "" && *attachVolume != "" {
			return errors.New("-detach-volume and -attach-volume are two separate runs: a volume moves host by being detached, observed to have stopped, and then placed")
		}
		// Under the current term, like every other admin command here: a placement
		// change is not a leader taking over, and AcquireLeadership would leave the
		// serving Control Plane's writes refused as stale.
		leader, lerr := md.GetLeader(ctx)
		if lerr != nil {
			return fmt.Errorf("changing a volume's placement needs a Control Plane to be leading (start one first): %w", lerr)
		}
		if *attachVolume != "" {
			// The policy the operator declared, the same one -clone-snapshot places
			// against: whichever host it picks has to be one the fleet would have picked
			// itself, or the two paths that add bytes to a device disagree about what a
			// full device is.
			placed, aerr := controlplane.Place(ctx, md, store,
				placement.Policy{MaxOversubscription: *oversubscribe, MaxUsedRatio: *maxUsedRatio},
				leader.Term, *attachVolume, *attachHost)
			if aerr != nil {
				return aerr
			}
			// host_state is logged because a named host is honoured without an admission
			// check (controlplane.Place says why): an operator who has just placed a
			// volume onto a CORDONED or DRAINING host must be able to see that in the
			// line that says it worked.
			//
			// epoch, because it is the WAL directory the host will open
			// (<data-dir>/wal/<volume-id>/<epoch>) and the proof that this attach is not
			// resuming a session from before the volume was somewhere else. An operator
			// comparing it with the Agent's "serving volume" line can see the two agree.
			slog.Info("volume placed", "volume_id", *attachVolume,
				"host_id", placed.Host.HostID, "host_state", placed.Host.State,
				"epoch", placed.Epoch, "chosen_by", chooser(*attachHost))
			return nil
		}
		if err := md.SetVolumePrimaryHost(ctx, leader.Term, *detachVolume, ""); err != nil {
			return err
		}
		// The Agent finds out on its next GetDesiredState poll, and its teardown is
		// what puts this session's bytes in the object store. Said out loud because
		// an operator who reads "detached" and immediately re-places the volume has
		// re-created the window this command exists to avoid.
		slog.Info("volume detached; its host stops serving it on its next poll, and publishes the session's image as it does",
			"volume_id", *detachVolume)
		return nil
	}

	if *cordonHost != "" || *uncordonHost != "" {
		if *cordonHost != "" && *uncordonHost != "" {
			return errors.New("-cordon-host and -uncordon-host are two separate runs: pass the one you mean")
		}
		// Under the current term, like every other admin command here: taking a host out
		// of the rotation is not a leader taking over, and AcquireLeadership would leave
		// the serving Control Plane's writes refused as stale.
		leader, lerr := md.GetLeader(ctx)
		if lerr != nil {
			return fmt.Errorf("changing a host's fleet state needs a Control Plane to be leading (start one first): %w", lerr)
		}
		host, state := *cordonHost, lifecycle.HostCordoned
		if host == "" {
			host, state = *uncordonHost, lifecycle.HostActive
		}
		return setCordon(ctx, md, leader.Term, host, state)
	}

	if *deleteVolume != "" {
		leader, lerr := md.GetLeader(ctx)
		if lerr != nil {
			return fmt.Errorf("-delete-volume needs a Control Plane to be leading (start one first): %w", lerr)
		}
		// The KEK, because the descendant check unwraps before it believes a parent link:
		// `parent_volume_id` is an unauthenticated field, and a forged descriptor claiming
		// descent would otherwise block a real volume's shred for ever.
		if *kekFile == "" {
			return errors.New("-delete-volume needs -kek-file: it verifies which descriptors this fleet wrapped before deciding what descends from the volume")
		}
		dkek, kerr := readKEK(*kekFile)
		if kerr != nil {
			return kerr
		}
		shred, derr := controlplane.DeleteVolume(ctx, md, store,
			crypto.NewDevKMS(dkek, crypto.KEKID(dkek)), leader.Term, *deleteVolume)
		if derr != nil {
			return derr
		}
		// Which of the two happened, said out loud. An operator who ran this to destroy
		// data must not have to infer whether it was destroyed.
		//
		// Even the shred is qualified: objectstore.Delete is a reversible marker by design
		// (INV-14), so the bytes stay until the bucket's lifecycle policy expires the
		// non-current versions, and a deployment that has not configured one has not
		// destroyed anything yet.
		if shred.KeyDestroyed {
			slog.Info("volume crypto-shredded: it held the last wrap of its key, which is now unreachable through this interface — its layers are noise once the bucket expires the descriptor's non-current versions",
				"volume_id", *deleteVolume)
			return nil
		}
		slog.Warn("volume removed, NOT shredded: it shares its key with a live relative, so every layer it published stays readable. Deleting the last member of the lineage is what destroys the key",
			"volume_id", *deleteVolume, "key_shared_with", shred.SharedWith)
		return nil
	}

	if *cloneSnapshot != "" {
		leader, lerr := md.GetLeader(ctx)
		if lerr != nil {
			return fmt.Errorf("-clone-snapshot needs a Control Plane to be leading (start one first): %w", lerr)
		}
		// A clone re-wraps the shared DEK under its own volume id rather than copying its
		// parent's ciphertext, so this path needs the KEK where it did not before. What a
		// clone shares with its parent is the key *bytes* — which is what lets it read the
		// parent's layers — and not the wrap: custody and access are two mechanisms, and
		// only the first is what a swapped descriptor attacks.
		if *kekFile == "" {
			return errors.New("-clone-snapshot needs -kek-file: a clone re-wraps the volume's key under its own id")
		}
		ckek, kerr := readKEK(*kekFile)
		if kerr != nil {
			return kerr
		}
		vol, cerr := controlplane.Clone(ctx, md, store,
			crypto.NewDevKMS(ckek, crypto.KEKID(ckek)), rand.Reader,
			placement.Policy{MaxOversubscription: *oversubscribe, MaxUsedRatio: *maxUsedRatio},
			telemetry.Recorder(), leader.Term, *cloneSnapshot, ids.New().String())
		if cerr != nil {
			return cerr
		}
		slog.Info("volume cloned",
			"volume_id", vol.VolumeID, "host_id", vol.PrimaryHostID,
			"parent_snapshot_id", vol.ParentSnapshotID, "chain_depth", vol.ChainDepth)
		return nil
	}

	// Fail closed on a missing store: without a witness outside PostgreSQL the Elector
	// cannot prove a term has never been issued, and issuing one anyway is the failure
	// ADR-0011 exists for.
	term, err := controlplane.NewElector(md, store).Acquire(ctx, *holderID)
	if err != nil {
		return fmt.Errorf("acquiring a Control Plane term: %w", err)
	}
	slog.Info("control-plane elected", "holder_id", *holderID, "term", term, "version", version)

	// The term is fixed for the life of the process: it is never re-acquired, because
	// AcquireLeadership increments unconditionally and a process that re-elected
	// itself every few seconds would leave every admin one-shot in this file — each of
	// which reads GetLeader and then writes under that term — failing at random.
	// Losing the term ends the process instead; renewLeadership below is what notices,
	// on the same write that keeps this process's liveness stamp fresh.
	srv := &http.Server{
		Addr:              *listen,
		Handler:           cpserver.Handler(cpserver.New(md, func() int64 { return term }, *leaseTTL, band)),
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Two senders, so two slots: neither goroutine may block on a channel nobody is
	// going to read again once the other has won the select.
	errCh := make(chan error, 2)
	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("serving: %w", err)
			return
		}
		errCh <- nil
	}()
	// §7 says a Control Plane that has been superseded "detects the condition and
	// terminates itself". Nothing detected it: a second Control Plane took the term
	// and the first kept listening, kept accepting connections, and failed every
	// mutation with ErrStaleTerm for ever — which an Agent cannot tell from a Control
	// Plane that is merely refusing this one write. The guard is what makes the socket
	// go away, which is the signal an Agent (and a load balancer) already understands.
	go func() {
		errCh <- renewLeadership(ctx, md, real.NewClock(), term, *holderID, leaderRenewInterval(*leaseTTL))
	}()
	slog.Info("control-plane serving", "listen", *listen, "lease_ttl", *leaseTTL,
		"leader_renew_interval", leaderRenewInterval(*leaseTTL))

	select {
	case err := <-errCh:
		if err == nil {
			return nil
		}
		// Close rather than Shutdown: a process that has lost the term must stop
		// answering now, and draining in-flight requests would only let it spend the
		// grace period returning ErrStaleTerm to an Agent that is waiting on it.
		_ = srv.Close()
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), *shutdownGrace)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutting down: %w", err)
		}
		slog.Info("control-plane stopped")
		return nil
	}
}

// errTermLost ends a Control Plane that another one has superseded. It is a sentinel
// so the exit is one identifiable thing in a log and not a string somebody greps.
var errTermLost = errors.New("this Control Plane no longer holds its term")

// leaderRenewInterval is how often a serving Control Plane renews its leadership. It is
// derived from -lease-ttl rather than being a flag of its own: the lease is already this
// system's unit of "how long a fact about the fleet may be believed", and it is what
// -fleet-status compares the leader's stamp against. A third of it means a healthy
// leader's renewed_at is never older than TTL/3 — three chances to renew before anything
// reading the catalog is entitled to call this process gone — and it bounds a superseded
// process's remaining life at a third of the same window. The one-second floor is a
// busy-loop guard: the renewal is one indexed UPDATE, but a sub-second lease is a typo,
// and hammering the catalog is not the way to find out.
func leaderRenewInterval(leaseTTL time.Duration) time.Duration {
	if d := leaseTTL / 3; d > time.Second {
		return d
	}
	return time.Second
}

// renewLeadership keeps this Control Plane's leadership fresh, and ends the process when
// it turns out no longer to hold it.
//
// One term-guarded UPDATE does both jobs, which is why it is a write and not the read
// this used to be. While it succeeds it is the only durable evidence that this process
// is alive: control_plane_leader.renewed_at was stamped by the election and never touched
// again, so a Control Plane dead for five minutes and one that started five minutes ago
// printed the identical line in -fleet-status. When it fails with ErrStaleTerm it is §7's
// "detects the condition and terminates itself" — the superseded process finds out on a
// write it makes anyway, every few seconds, instead of on whichever Agent mutation
// happens to arrive first.
//
// It is a renewal and not a re-election: AcquireLeadership increments the term
// unconditionally, and every admin one-shot in this file reads GetLeader and then writes
// under that term, so a self-renewing leader would fail them at random.
//
// A catalog it cannot reach is not a term it has lost: those failures are logged and
// retried, because a Control Plane cut off from PostgreSQL already writes nothing at all,
// and killing it on a connection blip would turn a database hiccup into a fleet-wide
// outage. Only ErrStaleTerm ends the process, because only ErrStaleTerm is the catalog
// stating that somebody else is the leader.
//
// Returns nil when ctx ends — that is a normal shutdown, not a lost term.
func renewLeadership(ctx context.Context, md metadata.Store, clk clock.Clock, term int64, holderID string, every time.Duration) error {
	for {
		if err := clk.Sleep(ctx, every); err != nil {
			return nil
		}
		err := md.RenewLeadership(ctx, term, holderID)
		switch {
		case err == nil:
			continue
		case ctx.Err() != nil:
			return nil
		case !errors.Is(err, metadata.ErrStaleTerm):
			slog.Warn("could not renew this Control Plane's leadership; continuing to serve",
				"holder_id", holderID, "term", term, "error", err)
			continue
		}
		// Superseded. The leader row is read once more, best effort, purely to name the
		// successor: the exit is already decided, and an operator needs to know they are
		// looking at a deliberate takeover rather than a crash.
		leader, gerr := md.GetLeader(ctx)
		if gerr != nil {
			slog.Error("superseded by another Control Plane; exiting so a stale process stops serving",
				"holder_id", holderID, "term", term, "leader_read_error", gerr)
			return fmt.Errorf("%w: this process holds term %d", errTermLost, term)
		}
		slog.Error("superseded by another Control Plane; exiting so a stale process stops serving",
			"holder_id", holderID, "term", term,
			"leader_holder_id", leader.HolderID, "leader_term", leader.Term)
		return fmt.Errorf("%w: %s holds term %d, this process holds term %d",
			errTermLost, leader.HolderID, leader.Term, term)
	}
}

// chooser names who decided the host, for the line that reports a placement. It is one
// word in a log rather than nothing because the two are answerable to different people:
// a volume the policy placed can be re-placed by re-running the command, and a volume an
// operator placed by hand is where it is because somebody meant it.
func chooser(attachHost string) string {
	if attachHost == "" {
		return "placement"
	}
	return "operator"
}

// seed provisions one volume and reports what it made. It runs after the election, so
// it holds a real term and a stale process is refused by the same guard every other
// mutation goes through (§7).
func seed(ctx context.Context, md metadata.Store, store objectstore.Store, kekFile string, spec controlplane.VolumeSpec, term int64) error {
	if kekFile == "" {
		return errors.New("-kek-file is required with -seed-volume: the DEK is wrapped under it, and the Agent must be given the same one")
	}
	kek, err := readKEK(kekFile)
	if err != nil {
		return err
	}
	// crypto/rand, passed explicitly: the Provisioner takes its randomness as a
	// parameter so a DST run is reproducible, which means production has to say out
	// loud that it wants the real thing.
	p := controlplane.NewProvisioner(md, store, crypto.NewDevKMS(kek, crypto.KEKID(kek)), rand.Reader)
	vol, err := p.Provision(ctx, term, spec)
	if err != nil {
		return err
	}
	slog.Info("volume provisioned",
		"volume_id", vol.VolumeID, "host_id", spec.HostID,
		"size_bytes", spec.SizeBytes, "dek_key_id", vol.KeyID)
	return nil
}

// readKEK loads the 32-byte key-encryption key. It is a file rather than a flag
// because a key on a command line is in `ps`, in shell history and in the unit file.
//
// It reads through simio/disk rather than os (INV-01): every file this tree opens goes
// through the injected interface, and a key file is not an exception worth carving.
// readKEK opens the directory holding the KEK and reads it through simio (INV-01).
// The parsing rules live in internal/crypto so this binary and the Agent cannot
// disagree about what a key file is.
func readKEK(path string) ([crypto.DEKSize]byte, error) {
	d, err := real.NewDisk(filepath.Dir(path))
	if err != nil {
		return [crypto.DEKSize]byte{}, fmt.Errorf("opening the KEK's directory: %w", err)
	}
	return crypto.LoadKEK(d, filepath.Base(path))
}
