// This file is the flagged half of the DEV-0016 fixture. It sits in the same
// directory and the same package as the exempt fixture_test.go, and it is not a
// test file — so it is ordinary code that could be linked into anything, and INV-01
// still governs it. Every call below must be reported.
//
// Without this half the exemption could be quietly rewritten from "build-tagged
// harness" to "anything under integration/" and every test would stay green, which
// is exactly the widening the golden fixtures exist to catch.
package e2e

import (
	"net"
	"os"
	"time"
)

func now() time.Time { return time.Now() } // want `direct use of time\.Now outside internal/simio`

func listen() (net.Listener, error) {
	return net.Listen("unix", "helper.sock") // want `direct use of net\.Listen outside internal/simio`
}

func open() (*os.File, error) { return os.Open("f") } // want `direct use of os\.Open outside internal/simio`
