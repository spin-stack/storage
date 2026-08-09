package real

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// ObjectStore is a filesystem-backed object store used for local/dev mode (§6.2). The
// production S3-SDK-backed store is the §24 subsystem, and it lives next door in s3.go
// behind this same objectstore.Store contract. This one satisfies that
// contract without a network, which is what lets the DST harness and the whole contract
// suite run in-process.
//
// Two properties are not incidental, because the protocol is built on them:
//
//   - a conditional write is atomic. Create-only (§14.5, INV-21) and the If-Match
//     CAS (§12.4, INV-10) decide which of several concurrent writers wins; a
//     check-then-write implementation lets all of them win, silently, and two
//     promoters believing they hold the fence is split brain. Writers serialise on
//     mu, and create-only publishes with link(2), whose EEXIST *is* the exclusion —
//     so it holds even for two processes sharing the directory;
//   - an object is never partially visible. Bodies are staged in a temp file,
//     fsynced, and then linked or renamed into place — both atomic — so a reader
//     sees the old object or the new one, never a prefix of either. A torn WAL
//     object is not a read error: recovery's integrity check reads it as the end of
//     the durable prefix and everything past it is gone.
type ObjectStore struct {
	root string
	// mu serialises writers. Readers do not take it: rename gives them atomicity.
	mu sync.Mutex
}

// NewObjectStore returns a store rooted at dir (created if absent).
func NewObjectStore(dir string) (*ObjectStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &ObjectStore{root: dir}, nil
}

func (s *ObjectStore) path(key string) string {
	return filepath.Join(s.root, filepath.FromSlash(key))
}

func etagOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Sidecars this store keeps next to an object. None of them is ever an object: they
// are filtered out of List and are not reachable through any key, because a stray key
// is an orphan to the GC and a corrupt WAL object to recovery.
const (
	// markerSuffix is this store's delete marker: an empty sidecar file next to the
	// object. The bytes are never removed — permanent deletion is the lifecycle's job
	// on a real bucket, and this implementation must not offer what the interface
	// forbids (§21.3, INV-14).
	markerSuffix = ".deleted"
	// supersededSuffix records that a marked object was written again, so the marked
	// version is no longer the one a restore would surface (§21.3, finding 7).
	supersededSuffix = ".superseded"
	// tmpSuffix is the staging name for an in-progress body.
	tmpSuffix = ".tmp"
)

func isSidecar(key string) bool {
	return strings.HasSuffix(key, markerSuffix) ||
		strings.HasSuffix(key, supersededSuffix) ||
		strings.HasSuffix(key, tmpSuffix)
}

func (s *ObjectStore) Put(_ context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	if isSidecar(key) {
		return objectstore.PutResult{}, fmt.Errorf("simio/real: %q collides with the store's own bookkeeping", key)
	}
	// The conditional check and the write are one critical section. Splitting them
	// is what let every concurrent create-only writer win.
	s.mu.Lock()
	defer s.mu.Unlock()

	p := s.path(key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return objectstore.PutResult{}, err
	}
	// A delete marker is the latest version, so to a conditional write the object
	// does not exist — the same way S3 behaves on a versioned bucket.
	marked := s.marked(key)
	if opts.IfMatch != "" {
		cur, err := os.ReadFile(p)
		if err != nil || marked || etagOf(cur) != opts.IfMatch {
			return objectstore.PutResult{}, objectstore.ErrPreconditionFailed
		}
	}

	// Stage the whole body first, so publishing it is a single atomic step and no
	// reader can ever observe a prefix of it.
	tmp, err := stage(p, data)
	if err != nil {
		return objectstore.PutResult{}, err
	}
	defer func() { _ = os.Remove(tmp) }()

	switch {
	case opts.IfNoneMatch && !marked:
		// Link *is* the exclusion, not a check preceding one: it fails with EEXIST
		// if the key already exists, atomically, so two concurrent creators cannot
		// both win — and neither can two processes sharing the directory.
		if err := os.Link(tmp, p); err != nil {
			if errors.Is(err, os.ErrExist) {
				return objectstore.PutResult{}, objectstore.ErrPreconditionFailed
			}
			return objectstore.PutResult{}, err
		}
	default:
		if err := os.Rename(tmp, p); err != nil {
			return objectstore.PutResult{}, err
		}
	}
	if err := syncDir(filepath.Dir(p)); err != nil {
		return objectstore.PutResult{}, err
	}
	if marked {
		// The marker is cleared only now that the new bytes are in place: doing it
		// first means a crash, an ENOSPC or an EIO in that window serves the object
		// an operator retired. And the marked version is no longer what a restore
		// would surface, so record that instead of losing the fact (finding 7).
		if err := os.Rename(p+markerSuffix, p+supersededSuffix); err != nil {
			return objectstore.PutResult{}, err
		}
	}
	return objectstore.PutResult{ETag: etagOf(data)}, nil
}

// stage writes data to a temp file beside p and fsyncs it, returning its path. The
// caller publishes it with link (create-only) or rename (overwrite); both are atomic,
// so a reader sees the old object or the new one and never a torn one.
func stage(p string, data []byte) (string, error) {
	dir, base := filepath.Split(p)
	f, err := os.CreateTemp(dir, base+".*"+tmpSuffix)
	if err != nil {
		return "", err
	}
	tmp := f.Name()
	fail := func(err error) (string, error) {
		_ = f.Close()
		_ = os.Remove(tmp)
		return "", err
	}
	if _, err := f.Write(data); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	if err := os.Chmod(tmp, 0o644); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	return tmp, nil
}

// syncDir fsyncs a directory so a rename or link into it survives a crash.
func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (s *ObjectStore) Get(_ context.Context, key string) ([]byte, error) {
	if s.marked(key) {
		return nil, objectstore.ErrNotFound
	}
	data, err := os.ReadFile(s.path(key))
	if os.IsNotExist(err) {
		return nil, objectstore.ErrNotFound
	}
	return data, err
}

func (s *ObjectStore) Head(_ context.Context, key string) (objectstore.ObjectInfo, error) {
	if s.marked(key) {
		return objectstore.ObjectInfo{}, objectstore.ErrNotFound
	}
	data, err := os.ReadFile(s.path(key))
	if os.IsNotExist(err) {
		return objectstore.ObjectInfo{}, objectstore.ErrNotFound
	}
	if err != nil {
		return objectstore.ObjectInfo{}, err
	}
	info, err := os.Stat(s.path(key))
	if err != nil {
		return objectstore.ObjectInfo{}, err
	}
	return objectstore.ObjectInfo{
		Key: key, Size: int64(len(data)), ETag: etagOf(data), LastModified: info.ModTime(),
	}, nil
}

func (s *ObjectStore) List(_ context.Context, prefix string) ([]objectstore.ObjectInfo, error) {
	var out []objectstore.ObjectInfo
	err := filepath.WalkDir(s.root, func(p string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, rerr := filepath.Rel(s.root, p)
		if rerr != nil {
			return rerr
		}
		key := filepath.ToSlash(rel)
		if isSidecar(key) {
			return nil // bookkeeping is not an object
		}
		if !strings.HasPrefix(key, prefix) || s.marked(key) {
			return nil
		}
		data, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		info, ierr := entry.Info()
		if ierr != nil {
			return ierr
		}
		out = append(out, objectstore.ObjectInfo{
			Key: key, Size: int64(len(data)), ETag: etagOf(data), LastModified: info.ModTime(),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (s *ObjectStore) marked(key string) bool {
	_, err := os.Stat(s.path(key) + markerSuffix)
	return err == nil
}

// Delete places a delete marker over the object; the data stays for Restore.
func (s *ObjectStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := os.Stat(s.path(key)); err != nil {
		if os.IsNotExist(err) {
			return objectstore.ErrNotFound
		}
		return err
	}
	if s.marked(key) {
		return objectstore.ErrNotFound
	}
	// The marker supersedes any record of an older one: this object is what a
	// restore would now bring back.
	_ = os.Remove(s.path(key) + supersededSuffix)
	f, err := os.Create(s.path(key) + markerSuffix)
	if err != nil {
		return err
	}
	return f.Close()
}

// Restore removes the delete marker: the operator step behind "un-GC this" (INV-14).
func (s *ObjectStore) Restore(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := os.Stat(s.path(key) + supersededSuffix); err == nil {
		return fmt.Errorf("%w: %s", objectstore.ErrRestoreSuperseded, key)
	}
	err := os.Remove(s.path(key) + markerSuffix)
	if os.IsNotExist(err) {
		return objectstore.ErrNotFound
	}
	return err
}
