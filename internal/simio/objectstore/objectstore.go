// Package objectstore is the simulable object-store interface (§25.1, INV-01).
// It models the S3 semantics the design depends on: create-only PUT via
// If-None-Match (§14.5), strong read-after-write for GET/HEAD, eventually
// consistent LIST, ETags for CAS (§12.4), and reversible deletes (delete markers,
// §21.3). Production code depends on Store, never on an S3 SDK directly.
package objectstore

import (
	"context"
	"errors"
	"io"
	"time"
)

// Sentinel errors mirror the S3 conditions the protocol reasons about.
var (
	// ErrNotFound is a 404 on GET/HEAD.
	ErrNotFound = errors.New("simio/objectstore: not found")
	// ErrPreconditionFailed is a 412: If-None-Match:* on an existing key, or a
	// failed If-Match CAS.
	ErrPreconditionFailed = errors.New("simio/objectstore: precondition failed")
	// ErrRestoreSuperseded means the key was written again after Delete marked it, so
	// the marked version is no longer the one a Restore would surface. Undoing the
	// delete is refused rather than approximated: returning the newer bytes would
	// hand an operator a volume rebuilt from content that was never what was marked.
	ErrRestoreSuperseded = errors.New("simio/objectstore: the marked version was superseded by a later write")
	// ErrBucketNotFound is the container itself being absent or unreachable — a
	// misconfiguration, never "this object is not there". Recovery reads a missing
	// key as "nothing was written yet"; reading a missing *bucket* the same way
	// would declare an empty durable prefix for a volume whose data is intact.
	ErrBucketNotFound = errors.New("simio/objectstore: bucket not found")
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

// ObjectInfo describes a stored object. LastModified is on the interface because every
// backend answers a listing with it; nothing in this tree reads it yet.
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
	// PutStream is Put for an object nobody wants a second copy of in memory: it stores
	// exactly size bytes read from body, under the same conditional-write semantics. A
	// sealed layer is the only such object here, and holding one was an OOM waiting for
	// a volume an order of magnitude past the rotation threshold.
	//
	// size is authoritative because that is what S3 needs before the first byte goes out
	// (Content-Length) — an implementation reads no further than it, and a body that ends
	// early is an error and stores nothing. The caller knows the exact number: for a
	// layer it is measured by the pass that computed the digest.
	PutStream(ctx context.Context, key string, body io.Reader, size int64, opts PutOptions) (PutResult, error)
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
	// stops answering Get/Head/List, so callers see it as gone. Deleting a key that
	// is already marked, or was never there, is ErrNotFound.
	Delete(ctx context.Context, key string) error
	// Restore removes the delete marker (INV-14: "a GC mistake costs a restore, not the
	// data"). It is on the interface, not an extra some implementations offer, because
	// an implementation that cannot reverse a mark makes every reachability bug
	// permanent. ErrNotFound if there is no marker; ErrRestoreSuperseded if the key was
	// written again after being marked.
	Restore(ctx context.Context, key string) error
}
