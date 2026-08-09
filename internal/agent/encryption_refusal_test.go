package agent_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// An Agent started without a key-encryption key used to serve *any* volume, including
// one the catalog records as encrypted. The whole of the guard was a single WARN printed
// once at start-up, which is not per volume and which nobody reads twice: the volume got
// a socket, the guest wrote through it, and the image published at detach was raw guest
// plaintext under a key that says it is sealed. What made it possible is that
// encryptionFor returned before it ever asked what the volume was provisioned with.
//
// The assertions here are the two things the outside can see, and neither is a check on
// the error: no socket is bound for the volume, and nothing is served through it. A gate
// that returns exactly the right error and starts the runtime anyway satisfies an
// assertion on `err` and fails both of these.
//
// The second row is the half that must keep working: §15's dev/local mode is an Agent
// with no KEK serving volumes provisioned without one, and it stays. A fix that refused
// every volume would pass a test that only looked at the encrypted case.
func TestAKEKlessAgentRefusesAVolumeTheCatalogSaysIsEncrypted(t *testing.T) {
	tests := []struct {
		name string
		// keys is what this host's source of key material answers about the volume —
		// in production `Loop.VolumeKeys`, which asks the Control Plane.
		keys func(volumeID string) (agent.VolumeKeys, error)
		// wantErr is the sentinel the refusal must carry, where there is one to carry.
		wantErr error
		// wantRefusal is what the *next report* says about the volume. It is the
		// separate half of the same refusal: everything else here is what the guest
		// cannot do, and this is the only thing anything outside the host can see.
		// A volume that never started is in no report at all unless this is set, and
		// an absence on the wire is indistinguishable from a volume nobody placed here.
		wantRefusal storagev1.VolumeRefusal
		served      bool
	}{
		{
			name: "the catalog says it was wrapped under a KEK this host does not hold",
			keys: func(id string) (agent.VolumeKeys, error) {
				return agent.VolumeKeys{
					VolumeID: id, KEKID: "kek-prod", DEKWrapped: []byte{1, 2, 3}, DEKKeyID: 7,
				}, nil
			},
			wantErr: agent.ErrNoKEK,
			// NO_KEY and not the catch-all: the fix is one flag on one process, or a
			// placement, and that is a different page of the runbook from a socket that
			// would not bind.
			wantRefusal: storagev1.VolumeRefusal_VOLUME_REFUSAL_NO_KEY,
		},
		{
			name: "the catalog cannot be asked, so whether it is encrypted is unknown",
			keys: func(string) (agent.VolumeKeys, error) {
				return agent.VolumeKeys{}, errors.New("the control plane refused")
			},
			wantRefusal: storagev1.VolumeRefusal_VOLUME_REFUSAL_ATTACH_FAILED,
		},
		{
			name:   "the catalog says it was provisioned without one: the dev mode of §15",
			keys:   func(id string) (agent.VolumeKeys, error) { return agent.VolumeKeys{VolumeID: id}, nil },
			served: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newListenerFactory()
			m, err := agent.NewVolumeManager(agent.VolumeManagerConfig{
				DataDir: "/var/lib/spin", SocketDir: "/run/spin", Budget: testBudget(),
			}, agent.VolumeManagerDeps{
				Clock:   sim.NewClock(time.Unix(1_700_000_000, 0).UTC()),
				Disk:    sim.NewDisk(),
				Listen:  f.listen,
				Mapper:  unusedMapper{},
				EventFD: unusedEventFD,
				// No KMS: this is the Agent started without -kek-file. Keys is wired
				// anyway, which is what the binary does — it reads the catalog through
				// the Control Plane whether or not it holds a key.
				Keys: func(_ context.Context, volumeID string) (agent.VolumeKeys, error) {
					return tc.keys(volumeID)
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = m.Close(t.Context()) }()

			id := ids.New().String()
			applyErr := m.Apply(t.Context(), []*storagev1.DesiredVolume{{
				VolumeId: id, SizeBytes: testVolumeSize, BlockSize: testBlockSize, Epoch: 1,
				State: storagev1.VolumeState_VOLUME_STATE_ACTIVE,
			}})

			_, served := m.Device(id)
			sockets := f.socketPaths()
			if tc.served {
				if applyErr != nil {
					t.Fatalf("a volume provisioned without a KEK was refused by a KEK-less Agent: %v", applyErr)
				}
				if !served {
					t.Fatal("the volume is not being served, and §15's dev mode says it must be")
				}
				if len(sockets) != 1 {
					t.Fatalf("the volume was served over %d socket(s): %v", len(sockets), sockets)
				}
				return
			}
			// What an operator sees: no socket for this volume, so no guest can write
			// a byte of plaintext through it.
			if len(sockets) != 0 {
				t.Fatalf("the volume got a vhost-user socket %v: a guest can write plaintext through it, "+
					"and the image published at detach is that plaintext", sockets)
			}
			if served {
				t.Fatal("the volume is being served, so its writes reach a WAL and an image with no DEK anywhere")
			}
			if applyErr == nil {
				t.Fatal("the Agent reported no failure at all, so nothing is retried and nothing is logged")
			}
			// Named, and named as this failure rather than as some failure: the message
			// the loop prints every cycle is the only thing that tells an operator which
			// volume this host is refusing and why.
			if tc.wantErr != nil && !errors.Is(applyErr, tc.wantErr) {
				t.Fatalf("the refusal is not %v, so nothing can tell it from an unrelated failure: %v", tc.wantErr, applyErr)
			}
			if !strings.Contains(applyErr.Error(), id) {
				t.Fatalf("the refusal does not name the volume, so the operator cannot tell which one it is: %v", applyErr)
			}
			// And it reaches the fleet. `applyErr` goes to slog and the cycle's backoff
			// and nowhere else, so before this the volume simply stopped appearing on the
			// wire — the catalog kept whatever watermarks last worked and -fleet-status
			// printed a healthy row for a volume with no runtime behind it.
			got := reportFor(t, m, id)
			if got.Refusal != tc.wantRefusal {
				t.Fatalf("the report says refusal=%s, want %s", got.Refusal, tc.wantRefusal)
			}
			if !strings.Contains(got.RefusalDetail, id) {
				t.Fatalf("refusal detail = %q, which does not name the volume", got.RefusalDetail)
			}
		})
	}
}
