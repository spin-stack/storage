package real

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// S3Config is the whole configuration surface of the object store. Every SDK knob
// that matters lives here and nowhere else: callers construct one S3Store and pass
// the objectstore.Store interface around, so no package outside this file needs to
// know the SDK exists (INV-01), and there is no second place where someone can pick
// a different path-style or checksum setting by accident.
type S3Config struct {
	// Bucket holds every key of this deployment.
	Bucket string
	// Endpoint is the S3-compatible endpoint (http://host:port). Empty means AWS.
	Endpoint string
	// Region is required by SigV4 even on backends that ignore it.
	Region string
	// AccessKey/SecretKey are static credentials; empty means the SDK's default
	// credential chain (instance role, env, config file).
	AccessKey, SecretKey string
	// UsePathStyle addresses buckets as /bucket/key. Required for MinIO/RustFS and
	// anything else without per-bucket DNS; defaulted on when Endpoint is set.
	UsePathStyle bool
	// ChecksumWhenRequired stops the SDK from adding CRC trailers to every request.
	// The conformance suite proves the pinned backend accepts the default
	// (when-supported) mode; this is the escape hatch for a backend version that
	// chokes on aws-chunked trailers, so nobody has to go hunting for the knob.
	ChecksumWhenRequired bool
	// RequestTimeout bounds a single HTTP request. Zero means the SDK default.
	RequestTimeout time.Duration
}

// S3Store is the S3-backed objectstore.Store: the one place the AWS SDK is used.
// It maps S3's conditional-write vocabulary onto the two sentinel errors the
// protocol reasons about (§12.4, §14.5) and paginates LIST internally, because the
// recovery scan (§22.1) must never see a truncated prefix.
type S3Store struct {
	client *s3.Client
	bucket string
}

var _ objectstore.Store = (*S3Store)(nil)

// ErrBucketNotVersioned means the bucket does not have versioning Enabled, so a
// Delete would destroy the object instead of placing a reversible delete marker.
// INV-14 ("a GC mistake costs a restore, not the data") is false on such a bucket.
var ErrBucketNotVersioned = errors.New("simio/real: bucket versioning is not Enabled")

// bucketVersioningAPI is the one call the constructor needs, named here so the check
// is unit-testable without a backend.
type bucketVersioningAPI interface {
	GetBucketVersioning(context.Context, *s3.GetBucketVersioningInput, ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error)
}

// requireVersioning fails unless the bucket has versioning Enabled — the precondition of
// INV-14. A bucket whose versioning tooling forgot, or an operator Suspended, is
// indistinguishable from a correct one at every other layer until the first delete, which
// is why this is asked once, in the constructor.
func requireVersioning(ctx context.Context, api bucketVersioningAPI, bucket string) error {
	out, err := api.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	if err != nil {
		// Cannot establish it (missing bucket, no permission, backend error) is the
		// same answer as "not versioned": we may not place a delete marker here. The
		// verdict is the same; the sentence is not. This runs inside NewS3Store, so
		// it is the first error a misconfigured deployment sees, and leading with
		// "versioning is not Enabled" when the truth is "your credentials were
		// refused" costs an operator the hour it takes to stop reading bucket
		// policies. Lead with what happened, keep the sentinel for the caller.
		return fmt.Errorf("could not determine versioning for %s (refusing it: %w): %w", bucket, ErrBucketNotVersioned, err)
	}
	if out.Status != types.BucketVersioningStatusEnabled {
		return fmt.Errorf("%w: %s has versioning status %q", ErrBucketNotVersioned, bucket, out.Status)
	}
	return nil
}

// resolveCredentials answers what will sign this store's requests. Static credentials
// win when configured; otherwise the SDK's default chain is loaded explicitly, because
// s3.New resolves nothing on its own and an Options with no Credentials provider sends
// every request unsigned. Both realistic deployments go through the chain.
func resolveCredentials(ctx context.Context, cfg S3Config) (aws.CredentialsProvider, error) {
	if cfg.AccessKey != "" || cfg.SecretKey != "" {
		return credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""), nil
	}
	loaded, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("simio/real: loading the default AWS credential chain: %w", err)
	}
	return loaded.Credentials, nil
}

// NewS3Store builds the client from cfg and refuses a bucket the GC could not undo a
// delete on. It is the only constructor that touches s3.Options.
func NewS3Store(ctx context.Context, cfg S3Config) (*S3Store, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("simio/real: S3Config.Bucket is required")
	}
	opts := s3.Options{
		Region:       regionOrDefault(cfg.Region),
		UsePathStyle: cfg.UsePathStyle || cfg.Endpoint != "",
	}
	if cfg.Endpoint != "" {
		opts.BaseEndpoint = aws.String(cfg.Endpoint)
	}
	creds, err := resolveCredentials(ctx, cfg)
	if err != nil {
		return nil, err
	}
	opts.Credentials = creds
	if cfg.ChecksumWhenRequired {
		opts.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		opts.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	}
	if cfg.RequestTimeout > 0 {
		opts.HTTPClient = awshttp.NewBuildableClient().WithTimeout(cfg.RequestTimeout)
	}
	store := &S3Store{client: s3.New(opts), bucket: cfg.Bucket}
	if err := requireVersioning(ctx, store.client, cfg.Bucket); err != nil {
		return nil, err
	}
	return store, nil
}

// Client exposes the underlying client for the conformance suite and for the §24
// subsystem work (hedged GETs, retry budget). Production paths use the interface.
func (s *S3Store) Client() *s3.Client { return s.client }

// Bucket is the bucket every key lives in.
func (s *S3Store) Bucket() string { return s.bucket }

// translate maps the S3 error vocabulary onto the interface's sentinels. The
// mappings are not guesses: each is pinned by a case in the conformance suite.
func translate(err error) error {
	if err == nil {
		return nil
	}
	var api smithy.APIError
	if !errors.As(err, &api) {
		return err
	}
	switch api.ErrorCode() {
	case "PreconditionFailed", "ConditionalRequestConflict":
		// AWS answers a *concurrent* conditional write with 409
		// ConditionalRequestConflict rather than 412 — same race, different code.
		// Leaving it unmapped means the promoter that lost gets an opaque error and
		// the "I was fenced" branch of §12.4 is never taken (INV-10).
		return fmt.Errorf("%w: %s", objectstore.ErrPreconditionFailed, api.ErrorMessage())
	case "NoSuchKey", "NotFound":
		// An If-Match against a key that does not exist answers NoSuchKey on the
		// backends we certify, not 412 — the caller reads that as "not initialised
		// yet", which is what epoch.CompareAndAdvance needs.
		return fmt.Errorf("%w: %s", objectstore.ErrNotFound, api.ErrorMessage())
	case "NoSuchBucket":
		// Deliberately *not* ErrNotFound. Recovery reads a missing key as "nothing
		// was written yet"; a misconfigured bucket reading the same way declares an
		// empty durable prefix for a volume whose data is intact (§22.1).
		return fmt.Errorf("%w: %s", objectstore.ErrBucketNotFound, api.ErrorMessage())
	}
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) {
		switch respErr.HTTPStatusCode() {
		case http.StatusPreconditionFailed, http.StatusConflict:
			return fmt.Errorf("%w: %s", objectstore.ErrPreconditionFailed, api.ErrorCode())
		}
	}
	// Everything else — SlowDown, ServiceUnavailable, RequestTimeout, InternalError,
	// AccessDenied — stays opaque on purpose: a sentinel is for a condition a caller
	// *branches* on, and nothing branches on "this attempt failed". Naming them would
	// invite a branch on a distinction nobody makes.
	return err
}

func (s *S3Store) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	return s.PutStream(ctx, key, bytes.NewReader(data), int64(len(data)), opts)
}

// PutStream sends the body as it is produced. ContentLength is set explicitly: without it
// the SDK buffers a non-seekable body to discover its length, which is the allocation
// this method exists to avoid, and the reader is bounded to the same number so a body
// that runs long cannot append to the object.
func (s *S3Store) PutStream(ctx context.Context, key string, body io.Reader, size int64, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	in := &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          io.LimitReader(body, size),
		ContentLength: aws.Int64(size),
	}
	switch {
	case opts.IfNoneMatch:
		in.IfNoneMatch = aws.String("*")
	case opts.IfMatch != "":
		in.IfMatch = aws.String(opts.IfMatch)
	}
	// The payload is sent unsigned, and that is what makes a stream possible at all:
	// SigV4 computes a SHA-256 over the whole body before the first byte goes out, which
	// means rewinding it, and a body that can be rewound is a body that is already in
	// memory. Against a real backend the SDK does not fall back — it fails the request
	// with "failed to seek body to start, request stream is not seekable", which is how
	// this was found: green in every local lane and red in the one that talks to RustFS.
	//
	// Nothing is given up. The bytes are covered by the SHA-256 the commit protocol takes
	// over what the store actually consumed, and compares against the digest that names
	// the key before the manifest is written — a stronger check than the transport's,
	// because it survives the object sitting in the bucket.
	//
	// What it does cost is the SDK's own retries: it cannot replay a body it cannot
	// rewind, so a connection lost mid-upload fails the publish. The commit protocol
	// retries the whole thing next cycle from the same SealedLayer under the same commit
	// id, so that costs a cycle rather than a commit.
	// One attempt, and it has to be said rather than left to the default. The SDK cannot
	// replay a body it cannot rewind, so its retry does not retry: it fails with "failed
	// to rewind transport stream for retry", and that error arrives *instead of* the one
	// the server actually sent. A loser of the create-only race then reports a transport
	// problem where it should report ErrPreconditionFailed — measured here as roughly one
	// conformance run in three.
	//
	// Nothing is lost by saying so. The retry could never have worked, and the commit
	// protocol retries the whole publish next cycle from the same SealedLayer under the
	// same commit id.
	out, err := s.client.PutObject(ctx, in,
		s3.WithAPIOptions(v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware),
		func(o *s3.Options) { o.RetryMaxAttempts = 1 })
	if err != nil {
		return objectstore.PutResult{}, translate(err)
	}
	return objectstore.PutResult{ETag: aws.ToString(out.ETag)}, nil
}

func (s *S3Store) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key),
	})
	if err != nil {
		return nil, translate(err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("simio/real: read %s: %w", key, err)
	}
	return data, nil
}

func (s *S3Store) Head(ctx context.Context, key string) (objectstore.ObjectInfo, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key),
	})
	if err != nil {
		return objectstore.ObjectInfo{}, translate(err)
	}
	return objectstore.ObjectInfo{
		Key:          key,
		Size:         aws.ToInt64(out.ContentLength),
		ETag:         aws.ToString(out.ETag),
		LastModified: aws.ToTime(out.LastModified),
	}, nil
}

// List returns every object under prefix, following continuation tokens. A page is
// capped (1000 keys on S3 and on the certified backend), and a partial listing
// would make recovery declare a short durable prefix — so this never returns one
// page and calls it a day.
func (s *S3Store) List(ctx context.Context, prefix string) ([]objectstore.ObjectInfo, error) {
	var out []objectstore.ObjectInfo
	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket), Prefix: aws.String(prefix),
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, translate(err)
		}
		for _, o := range page.Contents {
			out = append(out, objectstore.ObjectInfo{
				Key:          aws.ToString(o.Key),
				Size:         aws.ToInt64(o.Size),
				ETag:         aws.ToString(o.ETag),
				LastModified: aws.ToTime(o.LastModified),
			})
		}
	}
	return out, nil
}

// Delete removes a key. On the versioned buckets this system requires (§10, §21.3)
// the backend turns this into a delete marker, so the object stays recoverable and
// the GC cannot destroy data; the interface deliberately has no permanent-delete
// operation (INV-14).
func (s *S3Store) Delete(ctx context.Context, key string) error {
	if _, err := s.Head(ctx, key); err != nil {
		// S3 answers 204 for a missing key; the interface contract (and the sim)
		// report ErrNotFound, so keep the two implementations identical.
		return err
	}
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key),
	})
	return translate(err)
}

// Restore removes the delete marker a Delete placed, so the version underneath becomes
// current again (§21.3) — the operator action INV-14 rests on.
//
// It refuses in the one case that would be silently wrong: if the latest version is not a
// delete marker, something wrote the key after the Delete, and the marked version is not
// what removing a marker would surface.
func (s *S3Store) Restore(ctx context.Context, key string) error {
	var (
		latestMarker *string
		everMarked   bool
		pager        = s3.NewListObjectVersionsPaginator(s.client, &s3.ListObjectVersionsInput{
			Bucket: aws.String(s.bucket), Prefix: aws.String(key),
		})
	)
	for pager.HasMorePages() && latestMarker == nil {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return translate(err)
		}
		for _, dm := range page.DeleteMarkers {
			if aws.ToString(dm.Key) != key {
				continue // the prefix can match longer keys
			}
			everMarked = true
			if aws.ToBool(dm.IsLatest) {
				latestMarker = dm.VersionId
				break
			}
		}
	}
	switch {
	case latestMarker != nil:
		_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(s.bucket), Key: aws.String(key), VersionId: latestMarker,
		})
		return translate(err)
	case everMarked:
		// It was marked, and something has written the key since: the marked version
		// is no longer what removing a marker would surface.
		return fmt.Errorf("%w: %s", objectstore.ErrRestoreSuperseded, key)
	default:
		return fmt.Errorf("%w: %s carries no delete marker", objectstore.ErrNotFound, key)
	}
}
