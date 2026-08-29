// Package disk is the simulable durable-storage interface (§25.1, INV-01). It models the
// properties this Agent's own records depend on: append, read, durable sync
// (fdatasync-level), truncate, and a crash model where data written but not synced may be
// lost. Production code depends on Disk/File, never on os directly.
//
// The guest's data path is not here: QEMU writes the qcow2 chain itself (v6), and what this
// models is the state.json, the pointer and the lock that say what that chain is.
package disk

import (
	"errors"
	"io"
)

// ErrNotExist is returned when opening or renaming a missing file.
var ErrNotExist = errors.New("simio/disk: file does not exist")

// ErrNoSpace means the device backing the file has no room left: the real disk's ENOSPC
// and the simulator's injected equivalent, wrapped so callers can tell them apart with
// errors.Is. It lives here because a caller must distinguish a full device — sticky until
// somebody gives room back — from a transient error, and may import neither syscall
// (denied outside simio) nor the simulator; without the sentinel the only portable test is
// a comparison on the error's message, in the path that records what a host holds.
var ErrNoSpace = errors.New("simio/disk: no space left on device")

// ErrLocked means another live process holds the lock. It is not a transient
// condition to retry: it is the answer to "may I own this directory?", and it is no.
var ErrLocked = errors.New("simio/disk: the lock is held by another process")

// Usage is the state of the device backing a Disk: what statfs answers, in bytes.
//
// The three numbers do not add up on a real filesystem and are not meant to:
// TotalBytes counts blocks that are reserved for root and therefore appear in
// neither of the other two, so Used + Avail <= Total. Callers that want "how full is
// this device" want Used/Total; callers that want "can I still write" want Avail.
type Usage struct {
	// TotalBytes is the capacity of the device.
	TotalBytes int64
	// UsedBytes is what is occupied on it — by this process and by everything else
	// sharing the filesystem. The difference from summing our own files is the
	// point: another tenant's bytes are not reclaimable by anything we can do to
	// our own files, and a threshold that ignores them fires too late.
	UsedBytes int64
	// AvailBytes is what an unprivileged writer can still take.
	AvailBytes int64
}

// Disk is a flat-ish namespace of append-only files (names may contain "/").
type Disk interface {
	// Create returns a new empty file, truncating any existing one, and makes the
	// file's *name* durable before returning: on a real filesystem that means fsyncing
	// the parent directory (and every directory Create had to make).
	//
	// The guarantee belongs on the interface rather than a separate SyncDir the caller
	// is trusted to remember: File.Sync says nothing about the directory entry, so a
	// crash can lose a freshly created file whole, with its fdatasync'd records inside
	// it. Unlink is deliberately not covered — a directory entry that outlives a crash
	// costs a later sweep, never data.
	Create(name string) (File, error)
	// Open returns an existing file for read and append.
	Open(name string) (File, error)
	// Remove deletes a file.
	Remove(name string) error
	// Rename atomically renames a file.
	Rename(oldName, newName string) error
	// Exists reports whether a file exists.
	Exists(name string) (bool, error)
	// List returns the names of files with the given prefix, sorted.
	List(prefix string) ([]string, error)
	// Usage reports the device backing this Disk (ADR-0013). It takes no context:
	// the real implementation is a single statfs, which does not block on I/O and
	// cannot be cancelled halfway.
	Usage() (Usage, error)
	// Lock takes an exclusive, non-blocking lock on name, creating it if needed, and
	// returns a handle whose Close releases it. ErrLocked means another *process* holds
	// it (§10 "un proceso por host", DEV-0014).
	//
	// Non-blocking is the design: a caller that blocked would hang with no output
	// instead of exiting with a message naming the directory. The lock is owned by the
	// open file description, so the kernel releases it however the process dies and a
	// `kill -9` leaves nothing to clean up (ADR-0024).
	Lock(name string) (io.Closer, error)
}

// File is an append-only file with random reads and durable sync.
type File interface {
	// Append writes p at the end of the file. A short write returns n < len(p)
	// with a non-nil error (models a partial/torn write).
	Append(p []byte) (n int, err error)
	// ReadAt reads len(p) bytes at off, returning io.EOF past the end (os semantics).
	ReadAt(p []byte, off int64) (n int, err error)
	// Truncate changes the file size to size bytes.
	Truncate(size int64) error
	// Sync makes all prior writes durable (fdatasync-level).
	Sync() error
	// Size returns the current visible size in bytes.
	Size() (int64, error)
	// Close releases the file.
	Close() error
}
