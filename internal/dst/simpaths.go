package dst

import (
	"errors"
	"io"
	"strings"

	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// simPaths is qcow.Paths over the simulated disk: the volume's records, on a device that
// can be made to lose an acknowledged fsync and then lose power.
//
// It is the only implementation of that interface outside internal/simio/real, which is
// what lets a scenario here drive the reconciler's own reads and writes of state.json
// instead of a copy of them. Directories are not modelled — the simulated disk is a flat
// namespace of names that may contain "/" — so MkdirAll is a no-op and List works on the
// prefix.
type simPaths struct {
	disk *sim.Disk
}

func (p simPaths) MkdirAll(string) error { return nil }

func (p simPaths) Exists(path string) (bool, error) { return p.disk.Exists(path) }

func (p simPaths) Size(path string) (int64, error) {
	f, err := p.disk.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	return f.Size()
}

func (p simPaths) ReadFile(path string) ([]byte, error) {
	f, err := p.disk.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	n, err := f.Size()
	if err != nil || n == 0 {
		return nil, err
	}
	buf := make([]byte, n)
	if _, err := f.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buf, nil
}

// WriteAtomic replaces the file's contents, durably, in one step.
//
// Written in place rather than through a temporary file and a rename, which is what the
// real implementation does. The simulated disk already gives the property the rename is
// there for: a file's contents become crash-visible only at Sync, so a reader after a
// crash sees the previous whole version or the new whole version and never a mixture.
// What the sim adds on top, and the reason this is here at all, is the failure the rename
// cannot rule out either — an fsync that reports success without persisting, after which
// a power loss rolls the file back to the version before it.
func (p simPaths) WriteAtomic(path string, data []byte) error {
	f, err := p.open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if err := f.Truncate(0); err != nil {
		return err
	}
	if _, err := f.Append(data); err != nil {
		return err
	}
	return f.Sync()
}

// open returns the existing file, so that its durable content — the version a crash
// reverts to — survives the write. Create would discard it.
func (p simPaths) open(path string) (disk.File, error) {
	switch exists, err := p.disk.Exists(path); {
	case err != nil:
		return nil, err
	case exists:
		return p.disk.Open(path)
	default:
		return p.disk.Create(path)
	}
}

func (p simPaths) List(dir string) ([]string, error) {
	names, err := p.disk.List(dir + "/")
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(names))
	for _, name := range names {
		entry := strings.TrimPrefix(name, dir+"/")
		if !strings.Contains(entry, "/") {
			out = append(out, entry)
		}
	}
	return out, nil
}

func (p simPaths) Remove(path string) error {
	if err := p.disk.Remove(path); err != nil && !errors.Is(err, disk.ErrNotExist) {
		return err
	}
	return nil
}
