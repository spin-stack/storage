// Package testinfra is the build-tagged harness the integration and e2e lanes are
// made of (DEV-0016): it starts containers, starts our own binaries as processes,
// and waits for them to become reachable. Every real file in it carries
// `//go:build integration || e2e`, so none of it is linked into a binary this
// repository ships. There is no clock to inject into another *process*, and a
// harness that waited on a simulated one would be measuring nothing. The analyzer
// must produce zero diagnostics here.
package testinfra

import (
	"net"
	"os"
	"time"
)

func waitForPort() error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.Dial("tcp", "127.0.0.1:5432")
		if err == nil {
			return c.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}
	return os.ErrDeadlineExceeded
}

func readPIDFile() ([]byte, error) { return os.ReadFile("agent.pid") }
