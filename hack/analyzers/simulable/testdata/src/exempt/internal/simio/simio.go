// Package simio sits under an internal/simio import path, so it is the one place
// the real primitives may be used. The analyzer must produce zero diagnostics
// here even though it calls time/net/os directly (ADR-0003): this is exactly
// where the real implementations live.
package simio

import (
	"net"
	"os"
	"time"
)

func realClock() time.Time { return time.Now() }

func realDial() (net.Conn, error) { return net.Dial("tcp", "x:1") }

func realOpen() (*os.File, error) { return os.Open("f") }
