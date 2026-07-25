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
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

// NewS3Store builds the client from cfg. It is the only constructor that touches
// s3.Options.
func NewS3Store(cfg S3Config) (*S3Store, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("simio/real: S3Config.Bucket is required")
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	opts := s3.Options{
		Region:       region,
		UsePathStyle: cfg.UsePathStyle || cfg.Endpoint != "",
	}
	if cfg.Endpoint != "" {
		opts.BaseEndpoint = aws.String(cfg.Endpoint)
	}
	if cfg.AccessKey != "" || cfg.SecretKey != "" {
		opts.Credentials = credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")
	}
	if cfg.ChecksumWhenRequired {
		opts.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		opts.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	}
	if cfg.RequestTimeout > 0 {
		opts.HTTPClient = awshttp.NewBuildableClient().WithTimeout(cfg.RequestTimeout)
	}
	return &S3Store{client: s3.New(opts), bucket: cfg.Bucket}, nil
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
	case "PreconditionFailed":
		return fmt.Errorf("%w: %s", objectstore.ErrPreconditionFailed, api.ErrorMessage())
	case "NoSuchKey", "NotFound", "NoSuchBucket":
		// An If-Match against a key that does not exist answers NoSuchKey on the
		// backends we certify, not 412 — the caller reads that as "not initialised
		// yet", which is what epoch.CompareAndAdvance needs.
		return fmt.Errorf("%w: %s", objectstore.ErrNotFound, api.ErrorMessage())
	}
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusPreconditionFailed {
		return fmt.Errorf("%w: %s", objectstore.ErrPreconditionFailed, api.ErrorCode())
	}
	return err
}

func (s *S3Store) Put(ctx context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	in := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}
	switch {
	case opts.IfNoneMatch:
		in.IfNoneMatch = aws.String("*")
	case opts.IfMatch != "":
		in.IfMatch = aws.String(opts.IfMatch)
	}
	out, err := s.client.PutObject(ctx, in)
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
		Key:  key,
		Size: aws.ToInt64(out.ContentLength),
		ETag: aws.ToString(out.ETag),
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
				Key:  aws.ToString(o.Key),
				Size: aws.ToInt64(o.Size),
				ETag: aws.ToString(o.ETag),
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
