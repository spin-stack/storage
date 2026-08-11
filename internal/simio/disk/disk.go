// Package disk is the simulable durable-storage interface (§25.1, INV-01). It
// models the properties the WAL depends on: append, read, durable sync
// (fdatasync-level), truncate, and a crash model where data written but not
// synced may be lost. Production code depends on Disk/File, never on os directly.
package disk

import (
	"errors"
	"io"
)

// ErrNotExist is returned when opening or renaming a missing file.
var ErrNotExist = errors.New("simio/disk: file does not exist")

// ErrNoSpace means the device backing the file has no room left: the real disk's
// ENOSPC and the simulator's injected equivalent, wrapped so callers can tell them
// apart from any other I/O failure with errors.Is.
//
// It lives here because the WAL has to distinguish "the device is full" — a sticky
// condition that clears only when somebody gives the filesystem room back, and never
// on its own — from a transient error, and it may import neither syscall (denied
// outside simio) nor the simulator. Without a sentinel
// on this interface the only portable test is the error's message, which is a string
// comparison in the durability path.
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
	// file's *name* durable before returning: on a real filesystem that means
	// fsyncing the parent directory (and every directory Create had to make).
	//
	// The guarantee belongs here rather than in a separate SyncDir the caller is
	// trusted to remember. File.Sync makes a file's contents durable and says nothing
	// about the directory entry pointing at them, so a crash can lose a freshly
	// created file whole — with its fdatasync'd records inside it. The WAL, which
	// creates a segment per 32 MiB and ACKs writes into it, is one Create away from
	// that at all times, and so is every future caller. Making it a property of the
	// interface removes the possibility of forgetting it; the cost is one fsync per
	// file creation, and nothing in this tree creates files at a rate where that is
	// visible.
	//
	// Unlink is deliberately *not* covered: a directory entry that outlives a crash
	// costs a file a later sweep removes, never data.
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
	// returns a handle whose Close releases it. ErrLocked means another *process*
	// holds it (§10: "un proceso por host", DEV-0014).
	//
	// Non-blocking is the design, not a convenience: a caller that blocked would hang
	// with no output instead of exiting with a message naming the directory, and
	// "started but wedged" is harder to diagnose than "refused to start".
	//
	// The lock is owned by the open file description, so the kernel releases it when
	// the process dies by any means. That is what makes it usable for a data
	// directory at all: a `kill -9` leaves nothing to clean up, and the next
	// incarnation starts (ADR-0024). A lock needing explicit release would trade one
	// bug for a worse one — an Agent that will not come back after a crash.
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
