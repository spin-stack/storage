// This file is the exempt half of the DEV-0016 fixture: a build-tagged harness
// under integration/ that starts real processes and waits for them. Nothing here
// may be flagged — there is no clock to inject into another process, and the lane
// exists precisely so that the real world, not a simulation, decides.
//
// Its non-test neighbour helper.go is in the same package and the same directory
// and is still flagged. That pair is the whole assertion: the exemption is per
// file, not per directory, so it cannot spread to host-side code by proximity.
package e2e

import (
	"net"
	"os"
	"time"
)

func startAgent() ([]byte, error) {
	if err := os.WriteFile("agent.flags", nil, 0o600); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.Dial("unix", "agent.sock"); err == nil {
			_ = c.Close()
			return os.ReadFile("agent.pid")
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil, os.ErrDeadlineExceeded
}
