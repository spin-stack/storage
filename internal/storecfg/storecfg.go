// Package storecfg turns command-line flags into an object store, so both binaries
// name the same knobs and resolve them the same way.
//
// It exists because they did not: the Control Plane grew its own flags and opener while the
// Agent had none, and two definitions of "how do I reach the object store" show up as a host
// talking to a different bucket than the Control Plane it reports to.
package storecfg

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/real"
)

// Flags is the object-store configuration surface of a binary.
type Flags struct {
	Bucket       string
	Endpoint     string
	Region       string
	Dir          string
	CreateBucket bool
	Timeout      time.Duration
}

// Register declares the flags on fs. The names match what cmd/control-plane already
// documented, so existing invocations keep working.
func (f *Flags) Register(fs *flag.FlagSet) {
	fs.StringVar(&f.Bucket, "s3-bucket", "", "object-store bucket (mutually exclusive with -object-store-dir)")
	fs.StringVar(&f.Endpoint, "s3-endpoint", "", "S3-compatible endpoint, e.g. http://rustfs:9000 (empty means AWS)")
	fs.StringVar(&f.Region, "s3-region", "", "region for SigV4 (default us-east-1; most compatible backends ignore it)")
	fs.StringVar(&f.Dir, "object-store-dir", "", "filesystem object store, for a single-machine deployment (mutually exclusive with -s3-bucket)")
	fs.BoolVar(&f.CreateBucket, "s3-create-bucket", false,
		"create the bucket and enable versioning if absent; off by default so a typo'd bucket name fails instead of inventing an empty deployment")
	fs.DurationVar(&f.Timeout, "s3-request-timeout", 30*time.Second, "bound on a single object-store request")
}

// Credentials are deliberately not flags. They come from the SDK's default chain — an
// instance role in AWS, AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY for a local backend —
// because a secret on a command line is readable by every process on the host and lands
// in shell history, systemd unit files and `ps` output.

// Open builds the store. Exactly one of -s3-bucket and -object-store-dir must be set:
// the object store is the recovery authority (§5.8), and guessing which one an operator
// meant is not a thing to be clever about.
func (f *Flags) Open(ctx context.Context) (objectstore.Store, error) {
	switch {
	case f.Bucket == "" && f.Dir == "":
		return nil, errors.New("one of -s3-bucket or -object-store-dir is required")
	case f.Bucket != "" && f.Dir != "":
		return nil, errors.New("-s3-bucket and -object-store-dir are mutually exclusive")
	}

	if f.Bucket == "" {
		store, err := real.NewObjectStore(f.Dir)
		if err != nil {
			return nil, fmt.Errorf("opening the filesystem object store %q: %w", f.Dir, err)
		}
		return store, nil
	}

	cfg := real.S3Config{
		Bucket:         f.Bucket,
		Endpoint:       f.Endpoint,
		Region:         f.Region,
		RequestTimeout: f.Timeout,
	}
	open := real.NewS3Store
	if f.CreateBucket {
		open = real.EnsureBucket
	}
	store, err := open(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("opening the S3 object store %q: %w", f.Bucket, err)
	}
	return store, nil
}
