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
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/cpserver"
	"github.com/spin-stack/storage/internal/metadata/pg"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/real"
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
		s3Bucket   = flag.String("s3-bucket", "", "bucket holding this deployment's objects")
		s3Endpoint = flag.String("s3-endpoint", "", "S3-compatible endpoint (empty means AWS)")
		s3Region   = flag.String("s3-region", "", "region (required by SigV4 even where ignored)")
		storeDir   = flag.String("object-store-dir", "", "filesystem object store, for a dev run without S3")

		shutdownGrace = flag.Duration("shutdown-grace", 10*time.Second, "how long to let in-flight requests finish")
	)
	flag.Parse()

	switch {
	case *holderID == "":
		return errors.New("-holder-id is required")
	case *databaseDSN == "":
		return errors.New("-database-url (or $DATABASE_URL) is required")
	case *s3Bucket == "" && *storeDir == "":
		// Fail closed: without a witness the Elector cannot prove a term has never
		// been issued, and issuing one anyway is the failure ADR-0011 exists for.
		return errors.New("one of -s3-bucket or -object-store-dir is required: a term needs a witness (ADR-0011)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	pool, err := pgxpool.New(ctx, *databaseDSN)
	if err != nil {
		return fmt.Errorf("connecting to PostgreSQL: %w", err)
	}
	defer pool.Close()

	store, err := openObjectStore(ctx, *s3Bucket, *s3Endpoint, *s3Region, *storeDir)
	if err != nil {
		return err
	}

	md := pg.New(pool)
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

// openObjectStore builds the term witness: S3 when a bucket is named, a directory
// otherwise. Both come from internal/simio/real — the only place a real backend is
// constructed (INV-01).
func openObjectStore(ctx context.Context, bucket, endpoint, region, dir string) (objectstore.Store, error) {
	if bucket == "" {
		store, err := real.NewObjectStore(dir)
		if err != nil {
			return nil, fmt.Errorf("opening the filesystem object store %q: %w", dir, err)
		}
		return store, nil
	}
	store, err := real.NewS3Store(ctx, real.S3Config{
		Bucket:   bucket,
		Endpoint: endpoint,
		Region:   region,
	})
	if err != nil {
		return nil, fmt.Errorf("opening the S3 object store %q: %w", bucket, err)
	}
	return store, nil
}
