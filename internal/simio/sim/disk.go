package sim

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/spin-stack/storage/internal/simio/disk"
)

// ErrShortWrite is returned by an injected partial/torn append.
var ErrShortWrite = errors.New("simio/sim: injected short write")

// ErrNoSpace models ENOSPC: the device backing the file has no room left. It is the
// simulated counterpart of syscall.ENOSPC from a real Append/Truncate. Unlike every
// other fault here it is *not* one-shot — a full device stays full until space is
// reclaimed — because the failure mode worth testing is what the caller does after
// the first failure, not the first failure itself.
var ErrNoSpace = fmt.Errorf("simio/sim: injected ENOSPC: %w", disk.ErrNoSpace)

// Disk is a deterministic in-memory disk with an explicit crash model: content
// written but not Synced lives only in the per-file cache and is discarded by
// Crash. It also injects partial appends, lost syncs, and full devices (§25.3).
type Disk struct {
	mu    sync.Mutex
	files map[string]*content
	// one-shot fault injection keyed by file name
	shortAppend map[string]int
	syncLoss    map[string]bool
	// capacity is a standing (not one-shot) per-file device size in bytes.
	capacity map[string]int64
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
		capacity:    map[string]int64{},
	}
}

// InjectENOSPC caps the device backing name at capacity bytes. Appends are accepted
// while they fit; the one that crosses the cap writes only what fits and returns
// ErrNoSpace (the partial append a real ENOSPC delivers), and every append after it
// writes nothing and returns ErrNoSpace. Growing the file with Truncate is refused
// the same way. Space is reclaimed by truncating the file down or by ClearENOSPC.
//
// Allocation is charged at append time, so Sync of bytes the device already took
// always succeeds. A filesystem that defers allocation can instead fail at fsync;
// that variant is not modelled.
func (d *Disk) InjectENOSPC(name string, capacity int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.capacity[name] = capacity
}

// ClearENOSPC removes the device cap on name (the operator grew the device, or a
// checkpoint authorised a truncation that freed it).
func (d *Disk) ClearENOSPC(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.capacity, name)
}

// freeSpace reports the bytes name's device can still take, and whether it is
// capped at all. Callers hold d.mu.
func (d *Disk) freeSpace(name string, used int64) (int64, bool) {
	capacity, capped := d.capacity[name]
	if !capped {
		return 0, false
	}
	free := capacity - used
	if free < 0 {
		free = 0
	}
	return free, true
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
	if free, capped := f.d.freeSpace(f.name, int64(len(c.cache))); capped && int64(len(p)) > free {
		c.cache = append(c.cache, p[:free]...)
		return int(free), ErrNoSpace
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
		grow := size - int64(len(c.cache))
		if free, capped := f.d.freeSpace(f.name, int64(len(c.cache))); capped && grow > free {
			return ErrNoSpace
		}
		c.cache = append(c.cache, make([]byte, grow)...)
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
