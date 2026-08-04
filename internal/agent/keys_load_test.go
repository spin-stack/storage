package agent_test

import (
	"context"
	"errors"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"

	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// A volume whose key material this host cannot use is not served. There is no
// degraded mode: serving it unencrypted would write this guest's data into the bucket
// under a name that says it is encrypted, and §15.3's crypto-shredding guarantee does
// not survive that — the objects stay readable after the DEK is destroyed.
//
// Each case is a different way the wiring can be wrong in production, and every one of
// them must end the same way: Apply fails and nothing is being served.
func TestAVolumeWhoseKeysAreUnusableIsNotServed(t *testing.T) {
	var kek [crypto.DEKSize]byte
	kek[0] = 1
	kms := crypto.NewDevKMS(kek, "kek-good")
	dek, err := crypto.GenerateDEK(&ramp{b: 3}, 5)
	if err != nil {
		t.Fatal(err)
	}
	wrapped, err := kms.WrapDEK(&ramp{b: 4}, dek)
	if err != nil {
		t.Fatal(err)
	}
	good := agent.VolumeKeys{DEKWrapped: wrapped, KEKID: "kek-good", DEKKeyID: dek.KeyID}

	tests := []struct {
		name string
		keys func() (agent.VolumeKeys, error)
	}{
		{
			name: "the Control Plane could not answer",
			keys: func() (agent.VolumeKeys, error) { return agent.VolumeKeys{}, errors.New("no answer") },
		},
		{
			name: "wrapped under a KEK this host does not hold",
			keys: func() (agent.VolumeKeys, error) {
				k := good
				k.KEKID = "kek-somewhere-else"
				return k, nil
			},
		},
		{
			name: "the version does not match the ciphertext (it is the GCM AAD)",
			keys: func() (agent.VolumeKeys, error) {
				k := good
				k.DEKKeyID = dek.KeyID + 1
				return k, nil
			},
		},
		{
			name: "the wrapped key is corrupt",
			keys: func() (agent.VolumeKeys, error) {
				k := good
				k.DEKWrapped = append([]byte(nil), wrapped...)
				k.DEKWrapped[0] ^= 0xFF
				return k, nil
			},
		},
		{
			name: "a version of 0, which is the WAL's plaintext marker",
			keys: func() (agent.VolumeKeys, error) {
				// Reachable only past Loop.VolumeKeys' own refusal — a different
				// KeysFunc, a repaired row — and it must still not open a volume.
				zeroDEK, derr := crypto.GenerateDEK(&ramp{b: 8}, 0)
				if derr != nil {
					return agent.VolumeKeys{}, derr
				}
				w, werr := kms.WrapDEK(&ramp{b: 8}, zeroDEK)
				if werr != nil {
					return agent.VolumeKeys{}, werr
				}
				return agent.VolumeKeys{DEKWrapped: w, KEKID: "kek-good", DEKKeyID: 0}, nil
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newListenerFactory()
			m, merr := agent.NewVolumeManager(agent.VolumeManagerConfig{
				DataDir: "/var/lib/spin", SocketDir: "/run/spin",
			}, agent.VolumeManagerDeps{
				Clock:   sim.NewClock(time.Unix(1_700_000_000, 0).UTC()),
				Disk:    sim.NewDisk(),
				Listen:  f.listen,
				Mapper:  unusedMapper{},
				EventFD: unusedEventFD,
				KMS:     kms,
				Keys: func(context.Context, string) (agent.VolumeKeys, error) {
					return tc.keys()
				},
			})
			if merr != nil {
				t.Fatal(merr)
			}
			defer func() { _ = m.Close(t.Context()) }()

			id := ids.New().String()
			err := m.Apply(t.Context(), []*storagev1.DesiredVolume{{
				VolumeId: id, SizeBytes: 1 << 20, BlockSize: 512, Epoch: 1,
				State: storagev1.VolumeState_VOLUME_STATE_ACTIVE,
			}})
			if err == nil {
				t.Fatal("the volume started with key material this host cannot use")
			}
			if _, served := m.Device(id); served {
				t.Fatal("the volume is being served after its keys were refused")
			}
		})
	}
}

// TestAKMSNeedsASourceOfKeys: the wiring failure that would otherwise surface as every
// volume failing to attach, one at a time, looking like a Control Plane problem.
func TestAKMSNeedsASourceOfKeys(t *testing.T) {
	var kek [crypto.DEKSize]byte
	_, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/data", SocketDir: "/run",
	}, agent.VolumeManagerDeps{
		Clock:   sim.NewClock(time.Unix(0, 0).UTC()),
		Disk:    sim.NewDisk(),
		Listen:  newListenerFactory().listen,
		Mapper:  unusedMapper{},
		EventFD: unusedEventFD,
		KMS:     crypto.NewDevKMS(kek, "k"),
	})
	if err == nil {
		t.Fatal("a manager with a KMS and no key source was accepted")
	}
}

// TestOneAgentPerDataDir is DEV-0014 at the manager's own seam. The e2e lane proves it
// between two real processes with a real flock; this proves the manager takes the lock
// at all, and gives it back — which is what makes a supervisor's stop-then-start work.
//
// §10 opens with "un proceso por host" and nothing enforced it: two Agents on one
// directory both resume the same segment files and both append to them, and
// hostio.Listen unlinks a stale socket before binding, so the second steals the guest
// from the first rather than failing to bind.
func TestOneAgentPerDataDir(t *testing.T) {
	d := sim.NewDisk()
	newManager := func() (*agent.VolumeManager, error) {
		return agent.NewVolumeManager(agent.VolumeManagerConfig{
			DataDir: "/var/lib/spin", SocketDir: "/run/spin",
		}, agent.VolumeManagerDeps{
			Clock:   sim.NewClock(time.Unix(1_700_000_000, 0).UTC()),
			Disk:    d,
			Listen:  newListenerFactory().listen,
			Mapper:  unusedMapper{},
			EventFD: unusedEventFD,
		})
	}

	first, err := newManager()
	if err != nil {
		t.Fatalf("the first manager: %v", err)
	}
	if _, err := newManager(); !errors.Is(err, disk.ErrLocked) {
		t.Fatalf("a second manager on the same data directory: want ErrLocked, got %v", err)
	}

	// Released by Close, or a supervisor could never restart an Agent in place.
	if err := first.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	second, err := newManager()
	if err != nil {
		t.Fatalf("after the holder closed: %v", err)
	}
	if err := second.Close(t.Context()); err != nil {
		t.Fatal(err)
	}

	// A different directory on the same host is a different claim.
	other, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
		DataDir: "/var/lib/spin-other", SocketDir: "/run/spin",
	}, agent.VolumeManagerDeps{
		Clock:   sim.NewClock(time.Unix(1_700_000_000, 0).UTC()),
		Disk:    d,
		Listen:  newListenerFactory().listen,
		Mapper:  unusedMapper{},
		EventFD: unusedEventFD,
	})
	if err != nil {
		t.Fatalf("a different data directory must be claimable: %v", err)
	}
	if err := other.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}
