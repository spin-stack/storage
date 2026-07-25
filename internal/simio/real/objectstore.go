package real

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// ObjectStore is a filesystem-backed object store used for local/dev mode
// (§6.2). The production S3-SDK-backed store is the Track D S3 subsystem (§24);
// see docs/plan/DECISIONS/ADR-0004-objectstore-real-staging.md. This
// implementation satisfies the same Store contract without a network.
type ObjectStore struct {
	root string
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

func (s *ObjectStore) Put(_ context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	p := s.path(key)
	// A delete marker is the latest version, so to a conditional write the object
	// does not exist — the same way S3 behaves on a versioned bucket. The marker is
	// only cleared once the write is going ahead.
	marked := s.marked(key)
	if opts.IfNoneMatch {
		if _, err := os.Stat(p); err == nil && !marked {
			return objectstore.PutResult{}, objectstore.ErrPreconditionFailed
		}
	}
	if opts.IfMatch != "" {
		cur, err := os.ReadFile(p)
		if err != nil || marked || etagOf(cur) != opts.IfMatch {
			return objectstore.PutResult{}, objectstore.ErrPreconditionFailed
		}
	}
	if marked {
		if err := os.Remove(p + markerSuffix); err != nil {
			return objectstore.PutResult{}, err
		}
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return objectstore.PutResult{}, err
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		return objectstore.PutResult{}, err
	}
	return objectstore.PutResult{ETag: etagOf(data)}, nil
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
		if strings.HasSuffix(key, markerSuffix) {
			return nil // the marker itself is not an object
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

// markerSuffix is this store's delete marker: an empty sidecar file next to the
// object. The bytes are never removed — permanent deletion is the lifecycle's job on
// a real bucket, and this implementation must not offer what the interface forbids
// (§21.3, INV-14).
const markerSuffix = ".deleted"

func (s *ObjectStore) marked(key string) bool {
	_, err := os.Stat(s.path(key) + markerSuffix)
	return err == nil
}

// Delete places a delete marker over the object; the data stays for Restore.
func (s *ObjectStore) Delete(_ context.Context, key string) error {
	if _, err := os.Stat(s.path(key)); err != nil {
		if os.IsNotExist(err) {
			return objectstore.ErrNotFound
		}
		return err
	}
	if s.marked(key) {
		return objectstore.ErrNotFound
	}
	f, err := os.Create(s.path(key) + markerSuffix)
	if err != nil {
		return err
	}
	return f.Close()
}

// Restore removes the delete marker.
func (s *ObjectStore) Restore(_ context.Context, key string) error {
	err := os.Remove(s.path(key) + markerSuffix)
	if os.IsNotExist(err) {
		return objectstore.ErrNotFound
	}
	return err
}
