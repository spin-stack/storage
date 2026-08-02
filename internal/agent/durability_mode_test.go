package agent_test

import (
	"errors"
	"testing"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/wal"
)

// §14.8's mode is a per-volume contract the Control Plane decides and stores, sends on
// DesiredVolume.durability, and — until this test — the Agent ignored completely.
// Log.SetDurabilityMode had no production caller, so every volume was served in the
// default `remote` mode whatever the catalog said.
//
// The direction was the safe one, which is exactly why nothing noticed: a `local` volume
// got the stricter ACK. But a mode the Control Plane can set and the Agent silently
// discards is worse than a mode that does not exist, because the catalog says one thing
// is true of a volume and nothing makes it true.
func TestTheAgentHonoursTheDurabilityTheControlPlaneSet(t *testing.T) {
	tests := []struct {
		name string
		sent storagev1.Durability
		want wal.DurabilityMode
	}{
		{"remote is served remote", storagev1.Durability_DURABILITY_REMOTE, wal.ModeRemote},
		{"local is served local", storagev1.Durability_DURABILITY_LOCAL, wal.ModeLocal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, _, _ := newTestManager(t)
			v := desiredVolume(t, 1)
			v.Durability = tc.sent
			if err := m.Apply(t.Context(), []*storagev1.DesiredVolume{v}); err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if got, ok := m.DurabilityMode(v.GetVolumeId()); !ok || got != tc.want {
				t.Fatalf("mode = %v (found=%t), want %v", got, ok, tc.want)
			}
		})
	}
}

// An unset durability is refused rather than guessed. wal.ModeFor already decided this —
// "an unknown value is an error, never a silent fallback to remote" — and the Agent keeps
// that promise instead of quietly choosing for the Control Plane. It is the same call
// encryptionFor makes: a volume whose contract this host cannot establish is failed, not
// degraded.
func TestAVolumeWithNoDurabilityIsRefused(t *testing.T) {
	m, _, _ := newTestManager(t)
	v := desiredVolume(t, 1)
	v.Durability = storagev1.Durability_DURABILITY_UNSPECIFIED

	err := m.Apply(t.Context(), []*storagev1.DesiredVolume{v})
	if err == nil {
		t.Fatal("a volume with no durability mode was served anyway; §14.8 says what a FLUSH ACK means, so an unset one has no answer")
	}
	if !errors.Is(err, lifecycle.ErrUnknownState) {
		t.Fatalf("the refusal must name the unknown state so an operator knows which field to set: %v", err)
	}
	if _, served := m.Device(v.GetVolumeId()); served {
		t.Fatal("the volume is being served despite the refusal")
	}
}
