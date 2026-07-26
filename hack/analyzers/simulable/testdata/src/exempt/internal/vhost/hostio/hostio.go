// Package hostio sits under internal/vhost/hostio, the one exception to INV-01
// outside internal/simio (ADR-0020): vhost-user needs a Unix socket that carries
// file descriptors and an mmap of the front-end's memory, neither of which simio
// models. The analyzer must produce zero diagnostics here.
package hostio

import (
	"net"
	"os"
)

func realListen() (net.Listener, error) { return net.Listen("unix", "/tmp/vhost.sock") }

func realOpen() (*os.File, error) { return os.OpenFile("disk.raw", os.O_RDWR, 0o600) }
