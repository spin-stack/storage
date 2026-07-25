// Package ioclass implements the internal I/O classes (§11): every Agent I/O
// belongs to exactly one class — foreground (guest data path), flush (PUTs that
// gate FLUSH/FUA ACKs), or background (objectization, compaction, hidration,
// prefetch, GC). Foreground and flush have absolute priority; background runs under
// a fixed per-resource token budget and always yields to the higher classes (§5.9,
// INV-17), so no background improvement can degrade data-path latency.
package ioclass

import (
	"fmt"
	"sync"
)

// Class is an I/O class.
type Class uint8

const (
	// Foreground: the guest data path (WAL append, fdatasync, read misses).
	Foreground Class = iota
	// Flush: PUTs that block a FLUSH/FUA ACK.
	Flush
	// Background: objectization, compaction, hidration, prefetch, GC, materialization.
	Background
)

func (c Class) String() string {
	switch c {
	case Foreground:
		return "foreground"
	case Flush:
		return "flush"
	case Background:
		return "background"
	default:
		return fmt.Sprintf("Class(%d)", uint8(c))
	}
}

func (c Class) high() bool { return c == Foreground || c == Flush }

// Scheduler arbitrates I/O between the classes for one resource (NVMe or NIC). It is
// safe for concurrent use.
type Scheduler struct {
	mu         sync.Mutex
	highActive int // in-flight foreground + flush ops
	bgBudget   int // background tokens per refill window
	bgUsed     int
}

// NewScheduler returns a scheduler whose background class gets bgBudget tokens per
// refill window.
func NewScheduler(bgBudget int) *Scheduler { return &Scheduler{bgBudget: bgBudget} }

// Begin marks a foreground/flush op as in flight (a no-op for background). Pair with End.
func (s *Scheduler) Begin(c Class) {
	if !c.high() {
		return
	}
	s.mu.Lock()
	s.highActive++
	s.mu.Unlock()
}

// End marks a foreground/flush op complete.
func (s *Scheduler) End(c Class) {
	if !c.high() {
		return
	}
	s.mu.Lock()
	if s.highActive > 0 {
		s.highActive--
	}
	s.mu.Unlock()
}

// TryAcquire requests cost tokens for class c. Foreground and flush are always
// granted (absolute priority). Background is granted only if no higher class is in
// flight AND it is within its budget — otherwise it yields (returns false, §5.9).
func (s *Scheduler) TryAcquire(c Class, cost int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.high() {
		return true
	}
	if s.highActive > 0 {
		return false // yield to foreground/flush
	}
	if s.bgUsed+cost > s.bgBudget {
		return false // over budget this window
	}
	s.bgUsed += cost
	return true
}

// Refill resets the background budget for a new window.
func (s *Scheduler) Refill() {
	s.mu.Lock()
	s.bgUsed = 0
	s.mu.Unlock()
}

// HighActive reports how many foreground/flush ops are in flight (for checkers).
func (s *Scheduler) HighActive() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.highActive
}
