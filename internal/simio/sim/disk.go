package sim

import (
	"errors"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/spin-stack/storage/internal/simio/disk"
)

// ErrShortWrite is returned by an injected partial/torn append.
var ErrShortWrite = errors.New("simio/sim: injected short write")

// Disk is a deterministic in-memory disk with an explicit crash model: content
// written but not Synced lives only in the per-file cache and is discarded by
// Crash. It also injects partial appends and lost syncs (§25.3).
type Disk struct {
	mu    sync.Mutex
	files map[string]*content
	// one-shot fault injection keyed by file name
	shortAppend map[string]int
	syncLoss    map[string]bool
}

type content struct {
	durable []byte // survives a crash
	cache   []byte // current visible content; lost-to-durable until Sync
}

// NewDisk returns an empty in-memory disk.
func NewDisk() *Disk {
	return &Disk{
		files:       map[string]*content{},
		shortAppend: map[string]int{},
		syncLoss:    map[string]bool{},
	}
}

// InjectShortAppend makes the next Append to name write only n bytes then fail.
func (d *Disk) InjectShortAppend(name string, n int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.shortAppend[name] = n
}

// InjectSyncLoss makes the next Sync of name report success without persisting
// (the data stays vulnerable to a subsequent Crash).
func (d *Disk) InjectSyncLoss(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.syncLoss[name] = true
}

// Crash discards every file's unsynced cache, reverting to durable content.
func (d *Disk) Crash() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, c := range d.files {
		c.cache = append([]byte(nil), c.durable...)
	}
}

// TornTail truncates a file's durable content to keep bytes, modelling a torn
// write at the durable tail, then reverts caches (as a crash would).
func (d *Disk) TornTail(name string, keep int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if c, ok := d.files[name]; ok && keep < len(c.durable) {
		c.durable = c.durable[:keep]
	}
}

func (d *Disk) Create(name string) (disk.File, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.files[name] = &content{}
	return &simFile{d: d, name: name}, nil
}

func (d *Disk) Open(name string) (disk.File, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.files[name]; !ok {
		return nil, disk.ErrNotExist
	}
	return &simFile{d: d, name: name}, nil
}

func (d *Disk) Remove(name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.files[name]; !ok {
		return disk.ErrNotExist
	}
	delete(d.files, name)
	return nil
}

func (d *Disk) Rename(oldName, newName string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	c, ok := d.files[oldName]
	if !ok {
		return disk.ErrNotExist
	}
	d.files[newName] = c
	delete(d.files, oldName)
	return nil
}

func (d *Disk) Exists(name string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.files[name]
	return ok, nil
}

func (d *Disk) List(prefix string) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var names []string
	for name := range d.files {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

type simFile struct {
	d    *Disk
	name string
}

func (f *simFile) Append(p []byte) (int, error) {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	c := f.d.files[f.name]
	if limit, ok := f.d.shortAppend[f.name]; ok {
		delete(f.d.shortAppend, f.name)
		n := min(limit, len(p))
		c.cache = append(c.cache, p[:n]...)
		return n, ErrShortWrite
	}
	c.cache = append(c.cache, p...)
	return len(p), nil
}

func (f *simFile) ReadAt(p []byte, off int64) (int, error) {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	c := f.d.files[f.name]
	if off >= int64(len(c.cache)) {
		return 0, io.EOF
	}
	n := copy(p, c.cache[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *simFile) Truncate(size int64) error {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	c := f.d.files[f.name]
	switch {
	case size <= int64(len(c.cache)):
		c.cache = c.cache[:size]
	default:
		c.cache = append(c.cache, make([]byte, size-int64(len(c.cache)))...)
	}
	return nil
}

func (f *simFile) Sync() error {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	if f.d.syncLoss[f.name] {
		delete(f.d.syncLoss, f.name)
		return nil // reported success, not persisted
	}
	c := f.d.files[f.name]
	c.durable = append([]byte(nil), c.cache...)
	return nil
}

func (f *simFile) Size() (int64, error) {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return int64(len(f.d.files[f.name].cache)), nil
}

func (f *simFile) Close() error { return nil }
