package qcow_test

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"pgregory.net/rapid"

	"github.com/spin-stack/storage/internal/framed"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/qcow"
)

// statePaths is the little filesystem ReadState and WriteState need: names, and the
// bytes of the one file whose contents this test is about. It is its own fake rather
// than the chain's, because the chain's carries an image's size and a pointer's path
// and this test would be asserting through them.
type statePaths struct {
	made    []string
	files   map[string][]byte
	readErr error
}

func newStatePaths() *statePaths { return &statePaths{files: map[string][]byte{}} }

func (p *statePaths) MkdirAll(dir string) error { p.made = append(p.made, dir); return nil }

func (p *statePaths) Exists(path string) (bool, error) {
	if p.readErr != nil {
		return false, p.readErr
	}
	_, ok := p.files[path]
	return ok, nil
}

func (p *statePaths) Size(path string) (int64, error) { return int64(len(p.files[path])), nil }

func (p *statePaths) ReadFile(path string) ([]byte, error) {
	body, ok := p.files[path]
	if !ok {
		return nil, fmt.Errorf("no such file: %s", path)
	}
	return body, nil
}

func (p *statePaths) WriteAtomic(path string, data []byte) error {
	p.files[path] = bytes.Clone(data)
	return nil
}

// aState is one host that has published two commits and owes the object store a third.
func aState() qcow.State {
	return qcow.State{
		VolumeID: vol,
		Commits: []qcow.CommitLayer{
			{CommitID: "0198c0de-0000-7000-8000-000000000001", LayerID: layerID},
			{CommitID: "0198c0de-0000-7000-8000-000000000002", LayerID: nextID},
		},
		Pending: &qcow.PendingCommit{
			CommitID:    "0198c0de-0000-7000-8000-000000000003",
			LayerID:     "0198c0de-0000-7000-8000-00000000000a",
			Epoch:       7,
			PlainBytes:  1 << 20,
			VirtualSize: size,
		},
	}
}

func TestStateIsWrittenBesideTheVolumesOtherFiles(t *testing.T) {
	if got, want := qcow.StateFile(root, vol), qcow.VolumeDir(root, vol)+"/state.json"; got != want {
		t.Fatalf("state file is %s, want %s", got, want)
	}
}

// TestStateRoundTripsThroughTheFilesystem is the base case every corruption test needs:
// without it, a truncation test that always errors would pass against a ReadState that
// never worked.
func TestStateRoundTripsThroughTheFilesystem(t *testing.T) {
	p := newStatePaths()
	want := aState()
	if err := qcow.WriteState(p, root, vol, want); err != nil {
		t.Fatalf("write: %v", err)
	}
	body, ok := p.files[qcow.StateFile(root, vol)]
	if !ok {
		t.Fatalf("nothing was written to %s; files: %v", qcow.StateFile(root, vol), p.files)
	}
	// The digest line is what makes a flipped bit a refusal, so it is asserted on the
	// bytes as stored and not inferred from the round trip succeeding.
	if _, err := framed.Unframe(body); err != nil {
		t.Fatalf("the stored bytes are not framed: %v", err)
	}
	got, err := qcow.ReadState(p, root, vol)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.FormatVersion != framed.FormatVersion {
		t.Fatalf("format_version is %d, want %d", got.FormatVersion, framed.FormatVersion)
	}
	got.FormatVersion = 0
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip changed the state:\n want %+v\n  got %+v", want, got)
	}
}

// TestAVolumeWithNoStateFileIsANewVolume: the absent file is the ordinary case — every
// volume has one until its first rotation — and it must not be an error, or the guard
// that refuses a volume it cannot account for would refuse every volume ever created.
func TestAVolumeWithNoStateFileIsANewVolume(t *testing.T) {
	got, err := qcow.ReadState(newStatePaths(), root, vol)
	if err != nil {
		t.Fatalf("read of an absent state: %v", err)
	}
	if !reflect.DeepEqual(got, qcow.State{}) {
		t.Fatalf("an absent state read as %+v, want the zero State", got)
	}
}

// TestAFilesystemThatCannotBeAskedIsNotAnAbsentState. "I could not look" and "it is not
// there" lead to opposite decisions — republish the pending layer, or believe this host
// owes nothing — so collapsing them would create the second silently.
func TestAFilesystemThatCannotBeAskedIsNotAnAbsentState(t *testing.T) {
	p := newStatePaths()
	p.readErr = errors.New("input/output error")
	if got, err := qcow.ReadState(p, root, vol); err == nil {
		t.Fatalf("a filesystem that could not be examined read as %+v", got)
	}
}

func TestAStateNobodyCanBelieveIsRefused(t *testing.T) {
	sound, err := qcow.MarshalState(aState())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	newer, err := json.Marshal(map[string]any{"format_version": framed.FormatVersion + 1, "volume_id": vol})
	if err != nil {
		t.Fatalf("marshal newer: %v", err)
	}
	older, err := json.Marshal(map[string]any{"volume_id": vol})
	if err != nil {
		t.Fatalf("marshal older: %v", err)
	}
	tests := []struct {
		name string
		body []byte
		is   error
	}{
		{name: "truncated", body: sound[:len(sound)-1], is: framed.ErrCorrupt},
		{name: "no digest line at all", body: sound[65:], is: framed.ErrCorrupt},
		{name: "a bit turned over", body: flip(sound, len(sound)-2, 3), is: framed.ErrCorrupt},
		{name: "not json", body: framed.Frame([]byte("{")), is: nil},
		{name: "a version this binary does not write", body: framed.Frame(newer), is: framed.ErrFormatTooNew},
		{name: "no version at all", body: framed.Frame(older), is: framed.ErrFormatTooOld},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := newStatePaths()
			p.files[qcow.StateFile(root, vol)] = tc.body
			got, err := qcow.ReadState(p, root, vol)
			if err == nil {
				t.Fatalf("%s read as %+v", tc.name, got)
			}
			// One thing to branch on, so a caller refuses the volume without a list of
			// causes it has to keep in step with this file.
			if !errors.Is(err, qcow.ErrBadState) {
				t.Fatalf("error is not an ErrBadState: %v", err)
			}
			if tc.is != nil && !errors.Is(err, tc.is) {
				t.Fatalf("error does not name the cause %v: %v", tc.is, err)
			}
			if !reflect.DeepEqual(got, qcow.State{}) {
				t.Fatalf("a refused state came back as %+v, want the zero State", got)
			}
		})
	}
}

// TestAStateFileFromAnotherVolumeIsRefused. The digest proves the bytes are the bytes
// that were written and says nothing about where: a state file restored under another
// volume's directory passes it intact, and every commit it names is then credited to
// this volume — which is a download this host skips for a layer it does not have.
func TestAStateFileFromAnotherVolumeIsRefused(t *testing.T) {
	body, err := qcow.MarshalState(aState())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	other := "0198c0de-0000-7000-8000-0000000000ff"
	p := newStatePaths()
	p.files[qcow.StateFile(root, other)] = body
	got, err := qcow.ReadState(p, root, other)
	if err == nil {
		t.Fatalf("another volume's state read as %+v", got)
	}
	if !errors.Is(err, qcow.ErrBadState) {
		t.Fatalf("error is not an ErrBadState: %v", err)
	}
	if !strings.Contains(err.Error(), vol) {
		t.Fatalf("the error does not say whose state it is: %v", err)
	}
}

// TestMarshalStampsTheVersionOverWhateverItWasHanded. A State built by hand carries a
// zero, and the whole point of the field is that it cannot be absent.
func TestMarshalStampsTheVersionOverWhateverItWasHanded(t *testing.T) {
	s := aState()
	s.FormatVersion = 99
	body, err := qcow.MarshalState(s)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := qcow.UnmarshalState(vol, body)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.FormatVersion != framed.FormatVersion {
		t.Fatalf("format_version is %d, want %d", got.FormatVersion, framed.FormatVersion)
	}
}

func flip(body []byte, at, bit int) []byte {
	out := bytes.Clone(body)
	out[at] ^= 1 << bit
	return out
}

// ---- §25.2: serialize/replay under arbitrary truncations and bit corruptions ----

func stateID(rt *rapid.T, label string) string {
	return ids.NewAt(int64(rapid.IntRange(1, 1<<40).Draw(rt, label)), rand.Reader).String()
}

func genState(rt *rapid.T, volumeID string) qcow.State {
	n := rapid.IntRange(0, 6).Draw(rt, "commits")
	commits := make([]qcow.CommitLayer, 0, n)
	for i := range n {
		commits = append(commits, qcow.CommitLayer{
			CommitID: stateID(rt, fmt.Sprintf("commit_ms_%d", i)),
			LayerID:  stateID(rt, fmt.Sprintf("layer_ms_%d", i)),
		})
	}
	if n == 0 {
		// An empty slice and a nil one are the same state and must round trip to one
		// answer; JSON has only `null` for the second, so nil is what this asserts on.
		commits = nil
	}
	var pending *qcow.PendingCommit
	if rapid.Bool().Draw(rt, "has_pending") {
		pending = &qcow.PendingCommit{
			CommitID:    stateID(rt, "pending_commit_ms"),
			LayerID:     stateID(rt, "pending_layer_ms"),
			Epoch:       int64(rapid.IntRange(0, 1<<20).Draw(rt, "epoch")),
			PlainBytes:  int64(rapid.IntRange(0, 1<<40).Draw(rt, "plain_bytes")),
			VirtualSize: int64(rapid.IntRange(1, 1<<42).Draw(rt, "virtual_size")),
		}
	}
	// The tip list, drawn like the commits and nil when it is empty for the same reason.
	// It is what "sealed and not published" is derived from, so a field that survived the
	// round trip in every case but this one would be a layer nobody publishes.
	ln := rapid.IntRange(0, 6).Draw(rt, "layers")
	layers := make([]string, 0, ln)
	for i := range ln {
		layers = append(layers, stateID(rt, fmt.Sprintf("tip_ms_%d", i)))
	}
	if ln == 0 {
		layers = nil
	}
	var fenced *qcow.Fencing
	if rapid.Bool().Draw(rt, "has_fence") {
		fenced = &qcow.Fencing{
			Epoch:   int64(rapid.IntRange(0, 1<<20).Draw(rt, "fenced_epoch")),
			Refusal: int32(rapid.IntRange(0, 7).Draw(rt, "fenced_refusal")),
			Detail:  rapid.String().Draw(rt, "fenced_detail"),
		}
	}
	return qcow.State{
		// Drawn, not stamped: MarshalState must overwrite whatever is here, and a
		// generator that only ever produced the right number could not show that.
		FormatVersion: rapid.IntRange(0, 9).Draw(rt, "format_version"),
		VolumeID:      volumeID,
		Commits:       commits,
		Pending:       pending,
		Layers:        layers,
		Fenced:        fenced,
	}
}

func TestStateRoundTrips(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		volumeID := stateID(rt, "volume_ms")
		want := genState(rt, volumeID)
		body, err := qcow.MarshalState(want)
		if err != nil {
			rt.Fatalf("marshal: %v", err)
		}
		got, err := qcow.UnmarshalState(volumeID, body)
		if err != nil {
			rt.Fatalf("unmarshal: %v", err)
		}
		if got.FormatVersion != framed.FormatVersion {
			rt.Fatalf("marshal stamped format_version %d over a drawn %d; want %d",
				got.FormatVersion, want.FormatVersion, framed.FormatVersion)
		}
		// Compared as a whole, so a field added to the struct and forgotten by json is
		// caught here rather than by whoever needed it after a restart.
		want.FormatVersion, got.FormatVersion = 0, 0
		if !reflect.DeepEqual(got, want) {
			rt.Fatalf("round trip changed the state:\n want %+v\n  got %+v", want, got)
		}
	})
}

// TestStateTruncationIsDetected: cut the file at any byte and the read must fail. The
// failure mode ruled out is a short read decoding into a state with default values — no
// pending commit, so a sealed layer this host owes the bucket is republished under a
// second commit id, and no commits, so a recovery re-downloads a chain it already holds.
func TestStateTruncationIsDetected(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		volumeID := stateID(rt, "volume_ms")
		body, err := qcow.MarshalState(genState(rt, volumeID))
		if err != nil {
			rt.Fatalf("marshal: %v", err)
		}
		at := rapid.IntRange(0, len(body)-1).Draw(rt, "truncate_at")
		if got, err := qcow.UnmarshalState(volumeID, body[:at]); err == nil {
			rt.Fatalf("a state truncated to %d/%d bytes decoded as %+v", at, len(body), got)
		}
	})
}

// TestStateBitFlipIsDetected: any bit, at any offset, must make the read fail. The
// silent case is the one this is for — a flipped digit in a commit id yields a
// different and perfectly valid state, and this host then vouches for a local layer
// that came from a commit nobody published.
func TestStateBitFlipIsDetected(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		volumeID := stateID(rt, "volume_ms")
		want := genState(rt, volumeID)
		body, err := qcow.MarshalState(want)
		if err != nil {
			rt.Fatalf("marshal: %v", err)
		}
		at := rapid.IntRange(0, len(body)-1).Draw(rt, "byte")
		bit := rapid.IntRange(0, 7).Draw(rt, "bit")
		got, err := qcow.UnmarshalState(volumeID, flip(body, at, bit))
		if err == nil {
			rt.Fatalf("a state with bit %d of byte %d flipped decoded as %+v", bit, at, got)
		}
	})
}
