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

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lifecycle"
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
		seedLocal  = flag.Bool("seed-local-durability", false, "with -seed-volume: ACK FLUSH on fdatasync instead of on a verified object (§14.8)")
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
		oversubscribe   = flag.Float64("max-oversubscription", 1.0, "with -clone-snapshot: committed/total ceiling a host may reach (§28.2)")
	)
	var storeFlags storecfg.Flags
	storeFlags.Register(flag.CommandLine)
	flag.Parse()

	switch {
	case *holderID == "":
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

	// storeFlags.Open refuses the empty case: the object store is the recovery
	// authority (§5.8) and both paths below need it.
	store, err := storeFlags.Open(ctx)
	if err != nil {
		return err
	}

	md := pg.New(pool)

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
		durability := lifecycle.DurabilityRemote
		if *seedLocal {
			durability = lifecycle.DurabilityLocal
		}
		return seed(ctx, md, store, *kekFile, controlplane.VolumeSpec{
			SizeBytes:  *seedSize,
			BlockSize:  int32(*seedBlock),
			HostID:     *seedHost,
			Durability: durability,
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

	if *cloneSnapshot != "" {
		leader, lerr := md.GetLeader(ctx)
		if lerr != nil {
			return fmt.Errorf("-clone-snapshot needs a Control Plane to be leading (start one first): %w", lerr)
		}
		vol, cerr := controlplane.Clone(ctx, md, store, placement.Policy{MaxOversubscription: *oversubscribe},
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
		"size_bytes", spec.SizeBytes, "durability", spec.Durability, "dek_key_id", vol.KeyID)
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
