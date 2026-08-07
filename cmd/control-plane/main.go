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
	"github.com/spin-stack/storage/internal/placement"
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
			"PostgreSQL connection string (default $DATABASE_URL)")
		holderID = flag.String("holder-id", "", "identity of this Control Plane process (required)")
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
		kekFile    = flag.String("kek-file", "", "file holding the 32-byte key-encryption key (required for -seed-volume)")

		// snapshot-volume: record a snapshot request and exit, the same shape as
		// -seed-volume and for the same reason. The snapshot itself is taken by the
		// Agent that serves the volume — this only writes the row it converges on.
		snapshotVolume = flag.String("snapshot-volume", "", "record a snapshot request for this volume and exit, instead of serving")

		// clone-snapshot: create a volume from a published snapshot and exit. The host
		// is not a flag on purpose — §20's placement order decides it, and the whole
		// point of that order is that an operator naming a host would override the one
		// decision that makes a clone boot quickly.
		cloneSnapshot = flag.String("clone-snapshot", "", "create a volume from this snapshot and exit, instead of serving")

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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
		return fleetReport(ctx, md, os.Stdout)
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
		sum, rerr := controlplane.RebuildMetadata(ctx, md, store, leader.Term)
		if rerr != nil {
			return rerr
		}
		// Placement is not in any object, so nothing comes back with a host. Said out
		// loud because an operator reading "rebuilt 40 volumes" would otherwise expect
		// the fleet to start serving them.
		slog.Info("catalog rebuilt from the object store; no volume has a primary host — place them to resume serving",
			"volumes", sum.Volumes, "snapshots", sum.Snapshots)
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
			host, aerr := controlplane.Place(ctx, md,
				placement.Policy{MaxOversubscription: *oversubscribe, MaxUsedRatio: *maxUsedRatio},
				leader.Term, *attachVolume, *attachHost)
			if aerr != nil {
				return aerr
			}
			// host_state is logged because a named host is honoured without an admission
			// check (controlplane.Place says why): an operator who has just placed a
			// volume onto a CORDONED or DRAINING host must be able to see that in the
			// line that says it worked.
			slog.Info("volume placed", "volume_id", *attachVolume,
				"host_id", host.HostID, "host_state", host.State, "chosen_by", chooser(*attachHost))
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

	if *cloneSnapshot != "" {
		leader, lerr := md.GetLeader(ctx)
		if lerr != nil {
			return fmt.Errorf("-clone-snapshot needs a Control Plane to be leading (start one first): %w", lerr)
		}
		vol, cerr := controlplane.Clone(ctx, md, store,
			placement.Policy{MaxOversubscription: *oversubscribe, MaxUsedRatio: *maxUsedRatio},
			leader.Term, *cloneSnapshot, ids.New().String())
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

	// The term is fixed for the life of the process. A process that loses it does
	// not "renew" into a new one: every write it attempts fails with ErrStaleTerm,
	// which the handler answers as Aborted, and an operator restarts it.
	srv := &http.Server{
		Addr:              *listen,
		Handler:           cpserver.Handler(cpserver.New(md, func() int64 { return term }, *leaseTTL)),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	slog.Info("control-plane serving", "listen", *listen, "lease_ttl", *leaseTTL)

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serving: %w", err)
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
