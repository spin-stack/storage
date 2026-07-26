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

func (d *Disk) Create(name string) (disk.File, error) {
	p := d.path(name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &realFile{f: f}, nil
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
// identity. The WAL has to tell "the device is full" — sticky, with its own remedy —
// from a transient failure, and it may not import syscall (§25.1 denies it outside
// this package). The original error is preserved in the chain, so an errors.As for
// *os.PathError still works.
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
