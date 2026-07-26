// Package vhost is the *parent* of the exempt leaf. ADR-0020 grants the INV-01
// exception to internal/vhost/hostio and to nothing else, so the protocol, the
// virtqueue and the request handling stay simulable. This fixture is what proves
// the exemption is a leaf and not a subtree: every call below must be flagged.
package vhost

import (
	"net"
	"os"
	"time"
)

func now() time.Time { return time.Now() } // want `direct use of time\.Now outside internal/simio`

func listen() (net.Listener, error) {
	return net.Listen("unix", "/tmp/vhost.sock") // want `direct use of net\.Listen outside internal/simio`
}

func open() (*os.File, error) { return os.Open("f") } // want `direct use of os\.Open outside internal/simio`
