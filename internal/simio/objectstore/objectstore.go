// Package objectstore is the simulable object-store interface (§25.1, INV-01).
// It models the S3 semantics the design depends on: create-only PUT via
// If-None-Match (§14.5), strong read-after-write for GET/HEAD, eventually
// consistent LIST, ETags for CAS (§12.4), and reversible deletes (delete markers,
// §21.3). Production code depends on Store, never on an S3 SDK directly.
package objectstore

import (
	"context"
	"errors"
	"time"
)

// Sentinel errors mirror the S3 conditions the protocol reasons about.
var (
	// ErrNotFound is a 404 on GET/HEAD.
	ErrNotFound = errors.New("simio/objectstore: not found")
	// ErrPreconditionFailed is a 412: If-None-Match:* on an existing key, or a
	// failed If-Match CAS.
	ErrPreconditionFailed = errors.New("simio/objectstore: precondition failed")
)

// PutOptions carries conditional-write semantics.
type PutOptions struct {
	// IfNoneMatch requires the key to not already exist (create-only, §14.5).
	IfNoneMatch bool
	// IfMatch, when non-empty, requires the current ETag to equal it (CAS, §12.4).
	IfMatch string
}

// PutResult reports the stored object's ETag.
type PutResult struct {
	ETag string
}

// ObjectInfo describes a stored object. LastModified is what the GC's grace period
// is measured against (§21.3): an object written moments ago may belong to a manifest
// that is still being published.
type ObjectInfo struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
}

// Store is the object-store surface used by the Agent and Control Plane.
type Store interface {
	// Put stores data at key subject to opts. With IfNoneMatch it returns
	// ErrPreconditionFailed if the key exists; with IfMatch it CASes on ETag.
	Put(ctx context.Context, key string, data []byte, opts PutOptions) (PutResult, error)
	// Get returns the object bytes (strong read-after-write).
	Get(ctx context.Context, key string) ([]byte, error)
	// Head returns object metadata without the body.
	Head(ctx context.Context, key string) (ObjectInfo, error)
	// List returns objects whose key has the prefix, sorted by key. LIST may be
	// eventually consistent with respect to recent Puts.
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
	// Delete places a reversible delete marker over the key. Every implementation
	// keeps the bytes: permanent removal belongs to the bucket lifecycle, and this
	// interface deliberately cannot reach it (§5.11, §21.3, INV-14). A marked object
	// stops answering Get/Head/List, so callers see it as gone.
	Delete(ctx context.Context, key string) error
}
