package ioclass_test

import (
	"testing"

	"github.com/spin-stack/storage/internal/ioclass"
)

func TestHighPriorityAlwaysGranted(t *testing.T) {
	s := ioclass.NewScheduler(0) // zero background budget
	if !s.TryAcquire(ioclass.Foreground, 1000) {
		t.Fatal("foreground must always be granted")
	}
	if !s.TryAcquire(ioclass.Flush, 1000) {
		t.Fatal("flush must always be granted")
	}
}

// TestBackgroundYieldsToHighClasses is INV-17: while a foreground/flush op is in
// flight, background is refused.
func TestBackgroundYieldsToHighClasses(t *testing.T) {
	s := ioclass.NewScheduler(1000)

	// Idle: background is granted within budget.
	if !s.TryAcquire(ioclass.Background, 100) {
		t.Fatal("idle background within budget should be granted")
	}

	// A foreground op is in flight → background yields.
	s.Begin(ioclass.Foreground)
	if s.TryAcquire(ioclass.Background, 1) {
		t.Fatal("background must yield while foreground is in flight (INV-17)")
	}
	// A flush op too.
	s.Begin(ioclass.Flush)
	if s.TryAcquire(ioclass.Background, 1) {
		t.Fatal("background must yield while flush is in flight")
	}

	// Once the high-priority ops complete, background resumes (within budget).
	s.End(ioclass.Foreground)
	s.End(ioclass.Flush)
	if s.HighActive() != 0 {
		t.Fatalf("high active = %d, want 0", s.HighActive())
	}
	if !s.TryAcquire(ioclass.Background, 100) {
		t.Fatal("background should resume once high classes drain")
	}
}

func TestBackgroundRespectsBudget(t *testing.T) {
	s := ioclass.NewScheduler(150)
	if !s.TryAcquire(ioclass.Background, 100) {
		t.Fatal("first background within budget")
	}
	if s.TryAcquire(ioclass.Background, 100) {
		t.Fatal("second background exceeds the 150 budget, must be refused")
	}
	s.Refill()
	if !s.TryAcquire(ioclass.Background, 100) {
		t.Fatal("after refill background resumes")
	}
}

func TestClassString(t *testing.T) {
	for c, want := range map[ioclass.Class]string{
		ioclass.Foreground: "foreground",
		ioclass.Flush:      "flush",
		ioclass.Background: "background",
		ioclass.Class(9):   "Class(9)",
	} {
		if c.String() != want {
			t.Fatalf("Class(%d).String() = %q, want %q", c, c.String(), want)
		}
	}
}
