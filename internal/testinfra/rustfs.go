//go:build integration

// Package testinfra starts the real dependencies an integration test needs, as
// containers, so every scenario is reproducible on a laptop and in CI with no
// shared state and no manual setup (§6.2 local mode). It is integration-only: the
// build tag keeps it out of the unit/DST lane, which stays in-process.
package testinfra

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// DefaultRustFSImage is the backend the §6.1 conformance suite certifies. It is
// pinned by digest: a re-tagged `latest` must not silently change what was
// certified. Override with RUSTFS_IMAGE (the Taskfile passes the pinned value).
const DefaultRustFSImage = "rustfs/rustfs:latest@sha256:84ce557a0245a06a9aae5516f55ee0f007fca78d41df356f419306fdc0cb168c"

// ObjectStoreBackend is a running S3-compatible backend.
type ObjectStoreBackend struct {
	Endpoint  string // http://host:port
	AccessKey string
	SecretKey string
	Region    string
}

// Client returns an S3 client aimed at this backend. Path-style addressing is
// required: a container has no DNS for bucket-name virtual hosts.
func (b ObjectStoreBackend) Client() *s3.Client { return b.ClientWith() }

// ClientWith is Client with extra option overrides — used by the conformance suite
// to exercise the SDK knobs that matter against non-AWS backends (checksum mode).
func (b ObjectStoreBackend) ClientWith(opts ...func(*s3.Options)) *s3.Client {
	base := s3.Options{
		BaseEndpoint: aws.String(b.Endpoint),
		Region:       b.Region,
		UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider(
			b.AccessKey, b.SecretKey, ""),
	}
	return s3.New(base, opts...)
}

// RustFS starts a single-node RustFS and returns it. Single-node is a development
// backend only (§6.1 forbids it in production); what it is for here is running the
// conformance suite and every S3-shaped scenario deterministically.
func RustFS(t *testing.T, image string) ObjectStoreBackend {
	t.Helper()
	if image == "" {
		image = DefaultRustFSImage
	}
	ctx := context.Background()

	const (
		accessKey = "rustfsadmin"
		secretKey = "rustfsadmin"
	)
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"RUSTFS_ACCESS_KEY": accessKey,
			"RUSTFS_SECRET_KEY": secretKey,
			"RUSTFS_VOLUMES":    "/data",
		},
		// The S3 endpoint answers 403 to an unsigned request as soon as it is
		// serving, which is a precise readiness signal (a 200 never comes).
		WaitingFor: wait.ForHTTP("/").
			WithPort("9000/tcp").
			WithStatusCodeMatcher(func(status int) bool { return status == 403 || status == 200 }).
			WithStartupTimeout(90 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start rustfs: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatal(err)
	}
	return ObjectStoreBackend{
		Endpoint:  fmt.Sprintf("http://%s:%s", host, port.Port()),
		AccessKey: accessKey,
		SecretKey: secretKey,
		Region:    "us-east-1",
	}
}
