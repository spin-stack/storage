// Package vhost lives under integration/, next to the exempt guest program, and is
// host code: it drives QEMU and asserts on what our backend did. The DEV-0013
// exemption is for integration/guestinit and nothing else — "it is under
// integration/" is not the reason it was granted, "it runs inside the guest" is.
// This fixture is what proves the exemption did not widen to the directory: every
// call below must still be flagged.
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
