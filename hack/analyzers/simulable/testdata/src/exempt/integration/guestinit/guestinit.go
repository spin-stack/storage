// Package guestinit sits under integration/guestinit, the third INV-01 exemption
// (DEV-0013). Unlike the other two it is not host code at all: it is PID 1 *inside
// the guest VM*, on the far side of the interface INV-01 governs, and it is never
// linked into any binary this repository ships. Its whole purpose is to be the real
// world simio models — an fsync it could simulate would prove nothing about a kernel
// deciding a write must be durable. The analyzer must produce zero diagnostics here.
package guestinit

import (
	"os"
	"syscall"
)

func openDevice() (*os.File, error) { return os.OpenFile("/dev/vda", os.O_RDWR, 0) }

func readBack() (*os.File, error) { return os.Open("/dev/vda") }

func mountProc() error { return syscall.Mount("proc", "/proc", "proc", 0, "") }
