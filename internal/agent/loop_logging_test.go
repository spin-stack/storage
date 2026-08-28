package agent_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/spin-stack/storage/internal/simio/disk"
)

// syncBuffer is a bytes.Buffer a test goroutine may read while the loop writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// TestAFailingCycleSaysWhy. Run feeds Reconcile's error into nextDelay and drops it. The
// backoff is right; the silence is not — an Agent that can never succeed (wrong Control
// Plane URL, a host id the database rejects, expired credentials) logged one line at
// startup and then behaved exactly like a healthy one. Once per failure, not once per
// retry: a loop backing off from a dead Control Plane must not become the thing that fills
// the disk.
func TestAFailingCycleSaysWhy(t *testing.T) {
	var out syncBuffer
	restore := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(restore) })

	h := newHarness(t, testConfig(), disk.Usage{TotalBytes: 1 << 40, UsedBytes: 1 << 30})
	h.cp.setErr(errUnreachable)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = h.loop.Run(ctx)
	}()

	h.awaitBeat(t, 1)
	h.waitSleeping(t)
	cancel()
	<-done

	logged := out.String()
	if !strings.Contains(logged, errUnreachable.Error()) {
		t.Fatalf("a failed reconciliation logged nothing about the cause.\nlogged:\n%s", logged)
	}
	// The retry delay belongs in the line too: "it failed" without "and I will try
	// again in 1s" reads as a fatal error to whoever is watching.
	if !strings.Contains(logged, "retry") {
		t.Errorf("the failure line does not say when it will retry.\nlogged:\n%s", logged)
	}
}
