package agent_test

import (
	"bytes"
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

// LoadKEK is the one place a host's key-encryption key enters the process (§15.1's
// "KEK por host en archivo"). Its whole contract is the size check, and the size check
// matters because *any* 32 bytes are a valid AES-256 key: a KEK read from a file with a
// trailing newline, or from a hex dump, would be perfectly usable and completely wrong,
// and every volume on the host would then fail to unwrap with an authentication error
// that points at the DEK rather than at the file.

func writeKEKFile(t *testing.T, d disk.Disk, name string, body []byte) {
	t.Helper()
	f, err := d.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Append(body); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadKEK(t *testing.T) {
	exact := bytes.Repeat([]byte{0xAB}, crypto.DEKSize)

	tests := []struct {
		name string
		body []byte
		// write is false for the missing-file case: there is nothing to write.
		write   bool
		wantErr error
	}{
		{name: "exactly 32 bytes", body: exact, write: true},
		{name: "empty file", body: nil, write: true, wantErr: agent.ErrBadKEK},
		{
			name: "one byte short — a truncated write, and still a usable-looking key",
			body: exact[:crypto.DEKSize-1], write: true, wantErr: agent.ErrBadKEK,
		},
		{
			name: "a trailing newline — what `echo` and most editors produce",
			body: append(append([]byte(nil), exact...), '\n'), write: true, wantErr: agent.ErrBadKEK,
		},
		{
			name:  "64 hex characters — a key that is right, in the wrong encoding",
			body:  []byte("ababababababababababababababababababababababababababababababab00"),
			write: true, wantErr: agent.ErrBadKEK,
		},
		{name: "no file at all", write: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := sim.NewDisk()
			if tc.write {
				writeKEKFile(t, d, "kek", tc.body)
			}
			got, err := agent.LoadKEK(d, "kek")
			switch {
			case tc.wantErr != nil:
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("want %v, got %v", tc.wantErr, err)
				}
			case !tc.write:
				if err == nil {
					t.Fatal("a missing KEK file must be an error, not an all-zero key")
				}
			default:
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !bytes.Equal(got[:], exact) {
					t.Fatalf("the KEK read back as %x", got[:8])
				}
			}
			// Whatever went wrong, nothing partial comes back: a caller that ignored
			// the error would otherwise hold a key made of whatever fitted.
			if err != nil && got != ([crypto.DEKSize]byte{}) {
				t.Fatal("a failed load returned key material")
			}
		})
	}
}

// TestLoadKEKRoundTripsThroughTheKMS closes the loop the size check exists to protect:
// the bytes on disk are the bytes that unwrap a DEK wrapped under them. Without this,
// LoadKEK could return a correctly-sized, correctly-rejected-when-wrong, and entirely
// scrambled key and every test above would still pass.
func TestLoadKEKRoundTripsThroughTheKMS(t *testing.T) {
	var kek [crypto.DEKSize]byte
	for i := range kek {
		kek[i] = byte(i * 7)
	}
	d := sim.NewDisk()
	writeKEKFile(t, d, "kek", kek[:])

	loaded, err := agent.LoadKEK(d, "kek")
	if err != nil {
		t.Fatal(err)
	}
	wrapper := crypto.NewDevKMS(kek, "kek-1")
	dek, err := crypto.GenerateDEK(&ramp{b: 5}, 3)
	if err != nil {
		t.Fatal(err)
	}
	wrapped, err := wrapper.WrapDEK(&ramp{b: 9}, dek)
	if err != nil {
		t.Fatal(err)
	}
	back, err := crypto.NewDevKMS(loaded, "kek-1").UnwrapDEK(wrapped, 3)
	if err != nil {
		t.Fatalf("a DEK wrapped under the file's key did not unwrap under the loaded one: %v", err)
	}
	if back.Key != dek.Key {
		t.Fatal("the loaded KEK unwrapped a different key")
	}
}

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
			defer func() { _ = m.Close() }()

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
