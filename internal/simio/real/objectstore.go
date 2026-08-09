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
	"syscall"

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
//     promoters believing they hold the fence is split brain. Create-only publishes
//     with link(2), whose EEXIST *is* the exclusion; If-Match cannot be expressed as
//     one syscall, so its read-compare-publish runs under an flock on the directory
//     the key lives in (see lockKeysIn). Both hold between *processes*, which is the
//     only thing that matters: this store is what `-object-store-dir` selects, i.e.
//     the single-machine deployment, where two Agents publishing one volume's
//     manifest are two processes on one filesystem. An in-process mutex here excluded
//     nothing and hid everything — under it, four processes CASing from the same
//     prevETag all won, and so did four processes after an operator deleted the lock
//     file the exclusion used to live in;
//   - an object is never partially visible. Bodies are staged in a temp file,
//     fsynced, and then linked or renamed into place — both atomic — so a reader
//     sees the old object or the new one, never a prefix of either. A torn WAL
//     object is not a read error: recovery's integrity check reads it as the end of
//     the durable prefix and everything past it is gone.
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

// Root is the directory holding every object. It exists so a *second process* can
// open the same store — which is the only way to test a conditional write's
// exclusion, since an in-process lock makes any check-then-act look atomic.
func (s *ObjectStore) Root() string { return s.root }

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
	// lockSuffix is a reserved name, and the only one here that names no file this
	// store writes. It used to: the per-key `<key>.lock` whose flock serialised the
	// mutations of one key, until an operator deleting it turned out to dissolve the
	// exclusion (see lockKeysIn). The name stays reserved because `*.lock` is what a
	// stale-lock sweep deletes — an object stored under one would be swept away with
	// no error and no way back.
	lockSuffix = ".lock"
)

// isSidecar reports whether a key names one of this store's own files, or the one
// name it reserves. Put refuses such a key and every read path treats it as absent: a
// key that is indistinguishable from a delete marker silently hides the object it
// sits next to, and an object at `x.lock` is one an operator's sweep deletes.
func isSidecar(key string) bool {
	return strings.HasSuffix(key, markerSuffix) ||
		strings.HasSuffix(key, supersededSuffix) ||
		strings.HasSuffix(key, tmpSuffix) ||
		strings.HasSuffix(key, lockSuffix)
}

// lockKeysIn takes the exclusive lock that serialises mutations of every key in one
// directory. It is held across processes, because the writers this store has to
// exclude are processes: on a single-machine deployment two Agents share
// `-object-store-dir`, and the CAS on a volume's manifest is the whole of V1's
// fencing.
//
// **The lock is the directory, not a `<key>.lock` sidecar** — which is what it was,
// and what an operator could switch off. A POSIX lock lives on an *inode*, so it is
// an exclusion only for as long as the path still resolves to the inode it was taken
// on. Delete the sidecar under a holder and the next writer opens the same name, gets
// a brand-new inode, locks that, and is inside the read-compare-publish alongside the
// holder: two CAS winners from one prevETag, both told they published, no error
// anywhere and nothing to recover from afterwards. Measured against the sidecar
// version: with a holder in place, `rm <key>.lock` let a second CAS complete in 6ms
// instead of blocking. The action that does it is a routine one — `find -name
// '*.lock' -delete`, an rsync or a backup restore that skips sidecars, a tidy-up
// script — which is the whole problem: nothing about it looks like it touches
// fencing.
//
// A directory closes that, because there is no file named for the lock to delete, and
// the thing that would replace it holds the objects too: `rm -rf` on a volume's
// directory is not a cleanup anybody performs by accident, and it takes the manifest
// with it. Rejected: keeping the per-key sidecar and verifying its inode before
// publishing, which keeps a *routine* command as the trigger and still leaves the
// window between the check and the rename. Rejected: one store-wide lock file, which
// is a sidecar again — same unlink, same hole — and serialises the whole store rather
// than one directory.
//
// The cost is granularity: two keys in one directory now serialise. Only the publish
// does — staging the body and its fsync happen outside the lock — so what a second
// writer waits for is a read-compare, a rename, and the directory fsync it was going
// to queue behind anyway.
//
// flock(2), for the crash case. The kernel drops the lock when the fd is closed,
// including when the holder is SIGKILLed or panics mid-publish, so a dead writer
// cannot wedge a key — there is no lease, no timeout, and nothing to reap. Rejected:
// an O_CREAT|O_EXCL lock file, which is the same atomic exclusion but leaves the key
// locked forever when its holder dies, and the usual repair (break a lock older than
// T) reintroduces precisely the race it was meant to remove — two writers both
// deciding the lock is stale, both proceeding, both winning. A lock whose
// correctness depends on a timeout is not an exclusion (§25.1 says the same thing
// about sleeps).
func (s *ObjectStore) lockKeysIn(dir string) (*dirLock, error) {
	for range lockAttempts {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
		f, err := os.Open(dir)
		if os.IsNotExist(err) {
			continue // removed between the mkdir and the open; make it again
		}
		if err != nil {
			return nil, fmt.Errorf("simio/real: opening %q to lock it: %w", dir, err)
		}
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("simio/real: locking %q: %w", dir, err)
		}
		held, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		l := &dirLock{dir: dir, f: f, held: held}
		if err := l.stillExcludes(); err != nil {
			// We hold a directory the path no longer names, so we are excluding
			// nobody. Take whatever replaced it rather than publish under a lock
			// that means nothing.
			l.release()
			continue
		}
		return l, nil
	}
	return nil, fmt.Errorf("simio/real: %q was replaced under every one of %d attempts to lock it", dir, lockAttempts)
}

// lockAttempts bounds re-acquiring after the directory was replaced underneath us.
// There is no sleep and no timeout in it: each attempt is a fresh mkdir + open +
// flock, and giving up is an error naming the directory. Something recreating a
// directory faster than a writer can lock it is not a state to wait out.
const lockAttempts = 8

// dirLock is a held exclusion over every key in one directory.
type dirLock struct {
	dir  string
	f    *os.File
	held os.FileInfo
}

// stillExcludes reports that the directory this lock is held on is still the one the
// keys resolve through. It is checked when the lock is taken and again immediately
// before anything is published: a lock on an inode the path no longer names excludes
// nobody, and publishing under one is how two writers both win with no error. That
// makes the remaining case — somebody replaces a directory full of objects
// mid-publish — a loud failure for at least one of the writers instead of a silent
// double publish.
func (l *dirLock) stillExcludes() error {
	current, err := os.Stat(l.dir)
	if err != nil {
		return fmt.Errorf("simio/real: the locked directory %q went away mid-write: %w", l.dir, err)
	}
	if !os.SameFile(l.held, current) {
		return fmt.Errorf("simio/real: %q was replaced while it was locked, so this write is not excluding anybody: it was refused rather than published", l.dir)
	}
	return nil
}

// release drops the lock. Closing the fd is what releases it; that is also what makes
// the crash case self-healing, so there is deliberately no separate LOCK_UN to forget.
func (l *dirLock) release() { _ = l.f.Close() }

func (s *ObjectStore) Put(_ context.Context, key string, data []byte, opts objectstore.PutOptions) (objectstore.PutResult, error) {
	if isSidecar(key) {
		return objectstore.PutResult{}, fmt.Errorf("simio/real: %q collides with the store's own bookkeeping", key)
	}
	p := s.path(key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return objectstore.PutResult{}, err
	}

	// Stage the whole body first, so publishing it is a single atomic step and no
	// reader can ever observe a prefix of it. Staging happens outside the lock: the
	// temp name is unique, and an fsync of a multi-megabyte WAL object is the one
	// part of a Put that must not serialise the whole key.
	tmp, err := stage(p, data)
	if err != nil {
		return objectstore.PutResult{}, err
	}
	defer func() { _ = os.Remove(tmp) }()

	// From here to the publish is one critical section, and it has to be one across
	// processes: reading the current ETag, comparing it, and renaming over the key
	// is a check preceding an act, and every writer that reads before any of them
	// renames sees the ETag it was told to expect. Under an in-process mutex, four
	// processes CASing one manifest from the same prevETag all returned nil.
	lock, err := s.lockKeysIn(filepath.Dir(p))
	if err != nil {
		return objectstore.PutResult{}, err
	}
	defer lock.release()

	// A delete marker is the latest version, so to a conditional write the object
	// does not exist — the same way S3 behaves on a versioned bucket.
	marked := s.marked(key)
	if opts.IfMatch != "" {
		cur, err := os.ReadFile(p)
		if err != nil || marked || etagOf(cur) != opts.IfMatch {
			return objectstore.PutResult{}, objectstore.ErrPreconditionFailed
		}
	}

	// Nothing is published under a lock that stopped being one: if the directory was
	// replaced between taking it and here, this writer is not excluding the next one,
	// and the caller hears about it instead of both of them succeeding.
	if err := lock.stillExcludes(); err != nil {
		return objectstore.PutResult{}, err
	}

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
	if isSidecar(key) || s.marked(key) {
		return nil, objectstore.ErrNotFound
	}
	data, err := os.ReadFile(s.path(key))
	if os.IsNotExist(err) {
		return nil, objectstore.ErrNotFound
	}
	return data, err
}

func (s *ObjectStore) Head(_ context.Context, key string) (objectstore.ObjectInfo, error) {
	if isSidecar(key) || s.marked(key) {
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
	if isSidecar(key) {
		return objectstore.ErrNotFound
	}
	// The same lock a Put takes: a mark placed between another process's ETag
	// comparison and its rename would otherwise publish over a retired object.
	lock, err := s.lockKeysIn(filepath.Dir(s.path(key)))
	if err != nil {
		return err
	}
	defer lock.release()
	if _, err := os.Stat(s.path(key)); err != nil {
		if os.IsNotExist(err) {
			return objectstore.ErrNotFound
		}
		return err
	}
	if s.marked(key) {
		return objectstore.ErrNotFound
	}
	if err := lock.stillExcludes(); err != nil {
		return err
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
	if isSidecar(key) {
		return objectstore.ErrNotFound
	}
	lock, err := s.lockKeysIn(filepath.Dir(s.path(key)))
	if err != nil {
		return err
	}
	defer lock.release()
	if _, err := os.Stat(s.path(key) + supersededSuffix); err == nil {
		return fmt.Errorf("%w: %s", objectstore.ErrRestoreSuperseded, key)
	}
	if err := lock.stillExcludes(); err != nil {
		return err
	}
	err = os.Remove(s.path(key) + markerSuffix)
	if os.IsNotExist(err) {
		return objectstore.ErrNotFound
	}
	return err
}
