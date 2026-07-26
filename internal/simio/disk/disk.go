// Package disk is the simulable durable-storage interface (§25.1, INV-01). It
// models the properties the WAL and checkpoints depend on: append, read, durable
// sync (fdatasync-level), truncate, and a crash model where data written but not
// synced may be lost. Production code depends on Disk/File, never on os directly.
package disk

import "errors"

// ErrNotExist is returned when opening or renaming a missing file.
var ErrNotExist = errors.New("simio/disk: file does not exist")

// ErrNoSpace means the device backing the file has no room left: the real disk's
// ENOSPC and the simulator's injected equivalent, wrapped so callers can tell them
// apart from any other I/O failure with errors.Is.
//
// It lives here because the WAL has to distinguish "the device is full" — a sticky
// condition with its own remedy (truncate after a checkpoint, grow the device, restore
// the object store so the remote gap can close) — from a transient error, and it may
// import neither syscall (denied outside simio) nor the simulator. Without a sentinel
// on this interface the only portable test is the error's message, which is a string
// comparison in the durability path.
var ErrNoSpace = errors.New("simio/disk: no space left on device")

// Disk is a flat-ish namespace of append-only files (names may contain "/").
type Disk interface {
	// Create returns a new empty file, truncating any existing one.
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
