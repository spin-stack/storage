package wal_test

import (
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/wal"
)

// TestModeForCoversEveryDurability: the data path and the Control Plane must agree on
// the §14.8 vocabulary. If a mode is added to lifecycle and not mapped here, this
// fails — the two ends cannot drift apart silently.
func TestModeForCoversEveryDurability(t *testing.T) {
	for _, d := range lifecycle.Durabilities() {
		mode, err := wal.ModeFor(d)
		if err != nil {
			t.Fatalf("durability %q has no data-path mode: %v", d, err)
		}
		if got := mode.Durability(); got != d {
			t.Fatalf("round trip %q -> %v -> %q", d, mode, got)
		}
		if mode.String() != d.String() {
			t.Fatalf("String() = %q, want %q", mode.String(), d)
		}
	}
}

// TestModeForRejectsUnknown: an unmapped value fails closed instead of quietly
// promising remote durability the volume never asked for.
func TestModeForRejectsUnknown(t *testing.T) {
	if _, err := wal.ModeFor(lifecycle.Durability("eventual")); !errors.Is(err, lifecycle.ErrUnknownState) {
		t.Fatalf("want ErrUnknownState, got %v", err)
	}
}
