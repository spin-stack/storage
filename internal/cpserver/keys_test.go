package cpserver_test

import (
	"bytes"
	"strings"
	"testing"

	"connectrpc.com/connect"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/internal/metadata"
)

// Key material is served by a call of its own, not folded into the desired state.
// These are the properties that choice has to earn: a volume's keys reach the host
// that is its writer and nobody else, and the state an Agent polls every few seconds
// carries no key material at all.

func (f *fixture) volumeKeys(t *testing.T, hostID, volumeID string) (*storagev1.GetVolumeKeysResponse, error) {
	t.Helper()
	resp, err := f.srv.GetVolumeKeys(t.Context(), connect.NewRequest(&storagev1.GetVolumeKeysRequest{
		HostId: hostID, VolumeId: volumeID,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// TestGetVolumeKeysReturnsTheWrappedDEK: the wrapped DEK and the id of the KEK that
// wraps it. The Control Plane never holds the KEK — unwrapping is the host's, with
// the key its KMS already gives it (§15.1) — so this answer is useless to anyone who
// cannot already reach that KMS.
func TestGetVolumeKeysReturnsTheWrappedDEK(t *testing.T) {
	f := newFixture(t)
	wrapped := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	f.createVolume(t, metadata.Volume{
		VolumeID: "vol-a", SizeBytes: 1 << 30, BlockSize: 4096, CurrentEpoch: 3,
		PrimaryHostID: hostA, DEKWrapped: wrapped, KEKID: "kek-7", DEKKeyID: 1,
	})

	msg, err := f.volumeKeys(t, hostA, "vol-a")
	if err != nil {
		t.Fatal(err)
	}
	if msg.GetVolumeId() != "vol-a" {
		t.Errorf("volume_id = %q", msg.GetVolumeId())
	}
	if !bytes.Equal(msg.GetDekWrapped(), wrapped) {
		t.Errorf("dek_wrapped = %x, want %x", msg.GetDekWrapped(), wrapped)
	}
	if msg.GetKekId() != "kek-7" {
		t.Errorf("kek_id = %q, want kek-7", msg.GetKekId())
	}
}

// TestGetVolumeKeysRefusesAHostThatIsNotTheWriter is the reason the call is separate.
// Ownership is checked per request, against the volume's current primary, so a host
// that has been fenced or never held the volume is refused — a check that has no
// equivalent for a field bundled into a list answer, where the only granularity is
// the whole list.
func TestGetVolumeKeysRefusesAHostThatIsNotTheWriter(t *testing.T) {
	f := newFixture(t)
	f.createVolume(t, metadata.Volume{
		VolumeID: "vol-a", SizeBytes: 1 << 30, BlockSize: 4096,
		PrimaryHostID: hostA, DEKWrapped: []byte{1, 2, 3}, KEKID: "kek-7", DEKKeyID: 1,
	})

	if _, err := f.volumeKeys(t, hostB, "vol-a"); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Fatalf("code = %v, want PermissionDenied (err=%v)", connect.CodeOf(err), err)
	}
}

func TestGetVolumeKeysArgumentErrors(t *testing.T) {
	f := newFixture(t)
	f.createVolume(t, metadata.Volume{
		VolumeID: "vol-a", SizeBytes: 1 << 30, BlockSize: 4096,
		PrimaryHostID: hostA, DEKWrapped: []byte{1}, KEKID: "kek", DEKKeyID: 1,
	})
	tests := []struct {
		name         string
		host, volume string
		want         connect.Code
	}{
		{name: "no host id", host: "", volume: "vol-a", want: connect.CodeInvalidArgument},
		{name: "no volume id", host: hostA, volume: "", want: connect.CodeInvalidArgument},
		{name: "unknown volume", host: hostA, volume: "vol-nope", want: connect.CodeNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := f.volumeKeys(t, tc.host, tc.volume)
			if got := connect.CodeOf(err); got != tc.want {
				t.Fatalf("code = %v, want %v (err=%v)", got, tc.want, err)
			}
		})
	}
}

// TestDesiredStateCarriesNoKeyMaterial pins the decision in the schema itself rather
// than in a comment. The desired state is polled by every host every few seconds,
// for every volume it holds; key material is fetched once per volume and does not
// change while the volume is that host's. Bundling the second into the first would
// put a wrapped DEK on the wire thousands of times a day per volume, in the one
// message an operator is most likely to dump while debugging — and would remove the
// only place a per-volume ownership check can be made.
func TestDesiredStateCarriesNoKeyMaterial(t *testing.T) {
	fields := (&storagev1.DesiredVolume{}).ProtoReflect().Descriptor().Fields()
	for i := range fields.Len() {
		name := string(fields.Get(i).Name())
		for _, banned := range []string{"dek", "kek", "key", "secret"} {
			if strings.Contains(name, banned) {
				t.Errorf("DesiredVolume.%s carries key material: it belongs in GetVolumeKeys", name)
			}
		}
	}
}
