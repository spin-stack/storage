package real

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/spin-stack/storage/internal/simio/disk"
)

// Disk is the production disk backed by a directory on a real filesystem.
type Disk struct {
	root string
}

// NewDisk returns a Disk rooted at dir (created if absent).
func NewDisk(dir string) (*Disk, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Disk{root: dir}, nil
}

func (d *Disk) path(name string) string { return filepath.Join(d.root, filepath.FromSlash(name)) }

// Create makes the file and then makes its *name* durable, which fdatasync does not: a
// crash between the two loses the whole file, records the WAL already ACKed included.
// Every directory MkdirAll had to create is in the same position, so the sync walks from
// the file's parent up to the shallowest directory that did not exist before — in the
// steady state, one fsync of a cached directory inode.
func (d *Disk) Create(name string) (disk.File, error) {
	p := d.path(name)
	dir := filepath.Dir(p)
	top := shallowestMissing(dir, d.root)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syncDirs(dir, top); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return &realFile{f: f}, nil
}

// shallowestMissing returns the highest ancestor of dir (at or below root) that does
// not exist yet, or "" when the whole chain is already there. Its own parent is what
// has to be fsynced for its name to survive.
func shallowestMissing(dir, root string) string {
	missing := ""
	for p := dir; strings.HasPrefix(p, root) && p != root; p = filepath.Dir(p) {
		if _, err := os.Stat(p); err != nil {
			missing = p
			continue
		}
		break
	}
	return missing
}

// syncDirs fsyncs dir and, when top is non-empty, every ancestor up to and including
// top's parent — the directories whose entries were created by this call.
func syncDirs(dir, top string) error {
	stop := dir
	if top != "" {
		stop = filepath.Dir(top)
	}
	for p := dir; ; p = filepath.Dir(p) {
		if err := fsyncDir(p); err != nil {
			return err
		}
		if p == stop {
			return nil
		}
	}
}

// fsyncDir flushes a directory's own entries. A directory has to be opened read-only
// for this; on Linux fsync of that descriptor is what persists the names in it.
func fsyncDir(p string) error {
	f, err := os.Open(p)
	if err != nil {
		return fmt.Errorf("simio/real: open %q to fsync it: %w", p, err)
	}
	if err := f.Sync(); err != nil {
		return errors.Join(fmt.Errorf("simio/real: fsync %q: %w", p, err), f.Close())
	}
	return f.Close()
}

func (d *Disk) Open(name string) (disk.File, error) {
	f, err := os.OpenFile(d.path(name), os.O_RDWR, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, disk.ErrNotExist
		}
		return nil, err
	}
	return &realFile{f: f}, nil
}

func (d *Disk) Remove(name string) error { return os.Remove(d.path(name)) }

func (d *Disk) Rename(oldName, newName string) error {
	np := d.path(newName)
	if err := os.MkdirAll(filepath.Dir(np), 0o755); err != nil {
		return err
	}
	err := os.Rename(d.path(oldName), np)
	if os.IsNotExist(err) {
		return disk.ErrNotExist
	}
	return err
}

func (d *Disk) Exists(name string) (bool, error) {
	_, err := os.Stat(d.path(name))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (d *Disk) List(prefix string) ([]string, error) {
	var names []string
	err := filepath.WalkDir(d.root, func(p string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, rerr := filepath.Rel(d.root, p)
		if rerr != nil {
			return rerr
		}
		name := filepath.ToSlash(rel)
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

// Usage answers ADR-0013's question with a statfs of the filesystem holding this Disk's
// root. Summing our own files — what the Agent did before this existed — misses every
// byte another tenant of the same filesystem occupies, and those are the bytes nothing we
// do to our own files gives back. syscall is denied outside internal/simio; the
// uint64→int64 conversions are safe, a signed byte count overflowing at 8 EiB and Bfree
// never being above Blocks.
func (d *Disk) Usage() (disk.Usage, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(d.root, &st); err != nil {
		return disk.Usage{}, fmt.Errorf("simio/real: statfs %q: %w", d.root, err)
	}
	// Bsize is the block size the counts below are expressed in. It is int64 on
	// Linux and uint32 on Darwin; the conversion is what keeps this one file.
	bsize := int64(st.Bsize)
	return disk.Usage{
		TotalBytes: int64(st.Blocks) * bsize,
		UsedBytes:  int64(st.Blocks-st.Bfree) * bsize,
		AvailBytes: int64(st.Bavail) * bsize,
	}, nil
}

type realFile struct {
	f *os.File
}

func (r *realFile) Append(p []byte) (int, error) {
	if _, err := r.f.Seek(0, io.SeekEnd); err != nil {
		return 0, noSpace(err)
	}
	n, err := r.f.Write(p)
	return n, noSpace(err)
}

// noSpace wraps ENOSPC in disk.ErrNoSpace so a caller can recognise a full device by
// identity without importing syscall. The original error is preserved in the chain.
func noSpace(err error) error {
	if err == nil || !errors.Is(err, syscall.ENOSPC) {
		return err
	}
	return fmt.Errorf("%w: %w", disk.ErrNoSpace, err)
}

func (r *realFile) ReadAt(p []byte, off int64) (int, error) { return r.f.ReadAt(p, off) }
func (r *realFile) Truncate(size int64) error               { return r.f.Truncate(size) }
func (r *realFile) Sync() error                             { return r.f.Sync() }

func (r *realFile) Size() (int64, error) {
	st, err := r.f.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func (r *realFile) Close() error { return r.f.Close() }

// Lock takes an exclusive, non-blocking flock on name inside this Disk's root
// (DEV-0014). The lock belongs to the open file description, which is why the returned
// Closer keeps the *os.File alive: closing it releases the lock, and letting it be
// garbage-collected would release it while the caller still believed it held the
// directory. The kernel drops it when the process dies, so a crash needs no cleanup.
func (d *Disk) Lock(name string) (io.Closer, error) {
	path := d.path(name)
	// The parent, like Create makes: a lock is taken before anything else exists, so
	// requiring the directory first would mean every caller creating it by hand — and
	// the sim, where a name needs no parent at all, would silently disagree.
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("simio/real: creating the directory for %q: %w", path, err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("simio/real: opening the lock file %q: %w", path, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", disk.ErrLocked, path)
		}
		return nil, fmt.Errorf("simio/real: locking %q: %w", path, err)
	}
	return &fileLock{f: f}, nil
}

// fileLock makes Close idempotent. *os.File returns ErrClosed on a second Close and the
// simulated lock returns nil, and a contract the two implementations answer differently
// is a contract neither can be relied on for — a caller with a `defer unlock()` and an
// explicit release on the happy path is ordinary, and must not have to care which Disk
// it was handed.
type fileLock struct {
	f      *os.File
	closed bool
}

func (l *fileLock) Close() error {
	if l.closed {
		return nil
	}
	l.closed = true
	// Closing the descriptor is what releases the flock; there is no separate unlock.
	return l.f.Close()
}
