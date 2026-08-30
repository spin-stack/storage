package real

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// EnsureBucket creates the bucket if it is absent and turns versioning on, then returns
// a store over it.
//
// It exists because NewS3Store fails closed on a bucket that is not versioned (INV-14),
// and nothing in the tree could produce one: only test helpers ever called CreateBucket,
// so a fresh deployment could not get past the constructor. That is a chicken-and-egg
// bug, not a policy.
//
// It is deliberately NOT what NewS3Store does on its own. Creating a bucket is not a
// side effect a store constructor should have: on a typo'd -s3-bucket the silent-create
// behaviour invents an empty deployment and reports success, which for a system whose
// object store is the recovery authority (§5.8) is the worst available answer. A caller
// asks for this explicitly, once, at bootstrap.
func EnsureBucket(ctx context.Context, cfg S3Config) (*S3Store, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("simio/real: S3Config.Bucket is required")
	}
	creds, err := resolveCredentials(ctx, cfg)
	if err != nil {
		return nil, err
	}
	opts := s3.Options{
		Region:       regionOrDefault(cfg.Region),
		UsePathStyle: cfg.UsePathStyle || cfg.Endpoint != "",
		Credentials:  creds,
	}
	if cfg.Endpoint != "" {
		opts.BaseEndpoint = aws.String(cfg.Endpoint)
	}
	client := s3.New(opts)

	in := &s3.CreateBucketInput{Bucket: aws.String(cfg.Bucket)}
	// CreateBucket carries the region twice — in the endpoint it is sent to and in a
	// location constraint in the body — and AWS requires them to agree: without the
	// constraint it answers 400 IllegalLocationConstraintException in every region but
	// us-east-1, whose constraint is the empty one and must be left out instead. So this
	// is not a nicety; without it `-s3-create-bucket` works in one region and nowhere
	// else, on the one run a deployment cannot skip.
	if r := opts.Region; r != "us-east-1" {
		in.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(r),
		}
	}
	if _, err := client.CreateBucket(ctx, in); err != nil {
		// Already ours is the normal case on every run after the first. Anything else
		// — including "someone else owns this name" — is reported, because silently
		// continuing would then version and write into a bucket we do not control.
		if !bucketAlreadyOurs(err) {
			return nil, fmt.Errorf("creating bucket %s: %w", cfg.Bucket, err)
		}
	}
	if _, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(cfg.Bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	}); err != nil {
		return nil, fmt.Errorf("enabling versioning on %s (INV-14 requires it): %w", cfg.Bucket, err)
	}
	return NewS3Store(ctx, cfg)
}

// bucketAlreadyOurs reports whether the CreateBucket failure means "it is already there
// and it is yours" — the two codes S3 and its compatibles use for that.
func bucketAlreadyOurs(err error) bool {
	var api smithy.APIError
	if !errors.As(err, &api) {
		return false
	}
	switch api.ErrorCode() {
	case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
		return true
	}
	return false
}

func regionOrDefault(region string) string {
	if region == "" {
		return "us-east-1"
	}
	return region
}
