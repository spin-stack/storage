// Package testinfradriver does not exist in the tree, and that is the point. Its
// import path *contains* the exempt fragment "internal/testinfra" as a plain
// substring, so a strings.Contains match would hand it the DEV-0016 exemption for
// no reason other than how somebody named a package — and nothing would fail,
// because a check that stops checking is silent. This fixture is what makes the
// segment anchoring in hasPathSegments load-bearing: every call below must still be
// flagged.
package testinfradriver

import (
	"net"
	"os"
	"time"
)

func now() time.Time { return time.Now() } // want `direct use of time\.Now outside internal/simio`

func dial() (net.Conn, error) {
	return net.Dial("tcp", "127.0.0.1:5432") // want `direct use of net\.Dial outside internal/simio`
}

func read() ([]byte, error) { return os.ReadFile("f") } // want `direct use of os\.ReadFile outside internal/simio`
