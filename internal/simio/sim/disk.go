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

// ErrNoDeviceBudget is returned by Usage on a disk that was never given a device
// size. Inventing one would be worse than refusing: a zero total makes every
// ADR-0013 threshold derived from it read as "empty device", silently, in exactly
// the scenarios written to exercise pressure.
var ErrNoDeviceBudget = errors.New("simio/sim: this disk has no device budget: call SetDeviceBudget")

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
	// budget is the size of the whole simulated device, or 0 for "never declared".
	budget int64
	// locks are the names currently held (DEV-0014). One sim.Disk is one host's
	// filesystem, so the set living on the Disk is the model: two callers of the same
	// Disk are two processes on one host, and the second must be refused.
	locks map[string]bool
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

// InjectENOSPC caps the device backing target at capacity bytes. Appends are accepted
// while they fit; the one that crosses the cap writes only what fits and returns
// ErrNoSpace (the partial append a real ENOSPC delivers), and every append after it
// writes nothing and returns ErrNoSpace. Growing a file with Truncate is refused the
// same way. Space is reclaimed by truncating a file down, by removing one, or by
// ClearENOSPC.
//
// target is a file name or a directory prefix, and the cap is charged against the
// *sum* of the files under it. A WAL is a directory of segments, so "how much room
// this volume's log has" is not a property of any one file, and a per-file cap would
// be lifted by the mere act of rotating to a new segment.
//
// Allocation is charged at append time, so Sync of bytes the device already took
// always succeeds. A filesystem that defers allocation can instead fail at fsync;
// that variant is not modelled.
func (d *Disk) InjectENOSPC(target string, capacity int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.capacity[target] = capacity
}

// ClearENOSPC removes the device cap on target (the operator grew the device, or a
// checkpoint authorised a truncation that freed it).
func (d *Disk) ClearENOSPC(target string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.capacity, target)
}

// matches reports whether a fault registered under target applies to name: the exact
// file, or any file under it as a directory prefix.
func matches(target, name string) bool {
	return name == target || strings.HasPrefix(name, target+"/")
}

// faultTarget returns the key under which a fault is registered for name, preferring
// the most specific (longest) match so a per-file injection wins over a directory-wide
// one.
func faultTarget[V any](m map[string]V, name string) (string, bool) {
	best, found := "", false
	for target := range m {
		if matches(target, name) && len(target) >= len(best) {
			best, found = target, true
		}
	}
	return best, found
}

// chargedBytes sums the visible size of every file the cap registered under target
// covers. Callers hold d.mu.
func (d *Disk) chargedBytes(target string) int64 {
	var used int64
	for name, c := range d.files {
		if matches(target, name) {
			used += int64(len(c.cache))
		}
	}
	return used
}

// SetDeviceBudget declares how big this simulated device is: the total Usage
// reports, and the ceiling every file on it is charged against together. It is the
// simulated counterpart of the real disk's statfs, and the knob ADR-0013's device
// budget, reserve and thresholds are exercised through — a scenario sets the size of
// the NVMe it wants to fill, then fills it.
//
// It is deliberately not the same thing as InjectENOSPC, which caps one file: N
// volumes each comfortably inside their own cap can still exhaust the device between
// them, and that unsummed backlog is the failure ADR-0013 §1 exists for. Both
// ceilings bind; the tighter one wins.
//
// Zero (the default) leaves the device unsized: appends are unbounded and Usage
// refuses to answer.
func (d *Disk) SetDeviceBudget(bytes int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.budget = bytes
}

// Usage reports the simulated device: the declared budget, and what the files on it
// actually occupy. The used figure is derived from the files rather than tracked
// alongside them, so it cannot drift from the disk it describes; it costs one pass
// over the file table, which is a simulator's price to pay for not having a second
// source of truth.
func (d *Disk) Usage() (disk.Usage, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.budget <= 0 {
		return disk.Usage{}, ErrNoDeviceBudget
	}
	used := d.usedLocked()
	avail := d.budget - used
	if avail < 0 {
		avail = 0
	}
	return disk.Usage{TotalBytes: d.budget, UsedBytes: used, AvailBytes: avail}, nil
}

// usedLocked sums the visible size of every file. Callers hold d.mu.
func (d *Disk) usedLocked() int64 {
	var used int64
	for _, c := range d.files {
		used += int64(len(c.cache))
	}
	return used
}

// freeSpace reports the bytes an append to name may still take, and whether
// anything caps it at all. Two ceilings apply — the InjectENOSPC cap covering name and
// the whole-device budget — and the tighter one wins, because a real writer meets
// whichever it reaches first. Callers hold d.mu.
func (d *Disk) freeSpace(name string) (int64, bool) {
	free, capped := int64(0), false
	if target, ok := faultTarget(d.capacity, name); ok {
		free, capped = max(d.capacity[target]-d.chargedBytes(target), 0), true
	}
	if d.budget > 0 {
		deviceFree := max(d.budget-d.usedLocked(), 0)
		if !capped || deviceFree < free {
			free, capped = deviceFree, true
		}
	}
	return free, capped
}

// InjectShortAppend makes the next Append under target write only n bytes then fail.
// target is a file name or a directory prefix (see InjectENOSPC), so a fault can be
// aimed at a volume's WAL without naming the segment it will land in.
func (d *Disk) InjectShortAppend(target string, n int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.shortAppend[target] = n
}

// InjectSyncLoss makes the next Sync under target report success without persisting
// (the data stays vulnerable to a subsequent Crash).
func (d *Disk) InjectSyncLoss(target string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.syncLoss[target] = true
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

// Create adds the file with durable-but-empty content, which is the simulated form of
// the interface's contract: the *name* survives a crash (the real disk fsyncs the
// parent directory), the contents do not until they are Synced.
//
// Unlink durability is deliberately not modelled the same way: a Remove here is
// immediate and final, whereas a real one can be undone by a crash before the
// directory is synced. Nothing depends on the difference — a resurrected file costs a
// later sweep, never data — and modelling it would only add a fault no caller may
// react to.
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
	if target, ok := faultTarget(f.d.shortAppend, f.name); ok {
		limit := f.d.shortAppend[target]
		delete(f.d.shortAppend, target)
		n := min(limit, len(p))
		c.cache = append(c.cache, p[:n]...)
		return n, ErrShortWrite
	}
	if free, capped := f.d.freeSpace(f.name); capped && int64(len(p)) > free {
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
		if free, capped := f.d.freeSpace(f.name); capped && grow > free {
			return ErrNoSpace
		}
		c.cache = append(c.cache, make([]byte, grow)...)
	}
	return nil
}

func (f *simFile) Sync() error {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	if target, ok := faultTarget(f.d.syncLoss, f.name); ok {
		delete(f.d.syncLoss, target)
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

// Lock takes an exclusive, non-blocking lock on name.
func (d *Disk) Lock(name string) (io.Closer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.locks == nil {
		d.locks = map[string]bool{}
	}
	if d.locks[name] {
		return nil, fmt.Errorf("%w: %s", disk.ErrLocked, name)
	}
	d.locks[name] = true
	return &simLock{d: d, name: name}, nil
}

// simLock releases on Close, and only once: a double Close must not free a lock a
// *later* caller has since taken, which is the shape of every use-after-free.
type simLock struct {
	d      *Disk
	name   string
	closed bool
}

func (l *simLock) Close() error {
	l.d.mu.Lock()
	defer l.d.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	delete(l.d.locks, l.name)
	return nil
}
