package descriptor_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"

	"pgregory.net/rapid"

	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/framed"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

func TestEpochRoundTrips(t *testing.T) {
	t.Parallel()
	store := sim.NewObjectStore()
	id := ids.New().String()

	if _, err := descriptor.ReadEpoch(t.Context(), store, id); !errors.Is(err, descriptor.ErrNoEpoch) {
		t.Fatalf("want ErrNoEpoch for a volume that has never been granted one, got %v", err)
	}
	for _, want := range []int64{1, 2, 4096} {
		if err := descriptor.WriteEpoch(t.Context(), store, id, want); err != nil {
			t.Fatalf("writing %d: %v", want, err)
		}
		got, err := descriptor.ReadEpoch(t.Context(), store, id)
		if err != nil || got != want {
			t.Fatalf("ReadEpoch = %d, %v; want %d", got, err, want)
		}
	}
	if got, want := descriptor.EpochKey(id), "volumes/"+id+"/epoch"; got != want {
		t.Errorf("EpochKey = %q, want %q", got, want)
	}
}

// TestEpochRefusesOneRecordedForAnotherVolume: the digest proves the bytes are the bytes
// that were written and says nothing about where. An epoch attributed to the wrong volume
// is a fence set from somebody else's history, which is worse than no fence at all.
func TestEpochRefusesOneRecordedForAnotherVolume(t *testing.T) {
	t.Parallel()
	store := sim.NewObjectStore()
	mine, theirs := ids.New().String(), ids.New().String()
	if err := descriptor.WriteEpoch(t.Context(), store, theirs, 7); err != nil {
		t.Fatal(err)
	}
	body, err := store.Get(t.Context(), descriptor.EpochKey(theirs))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(t.Context(), descriptor.EpochKey(mine), body, objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if got, err := descriptor.ReadEpoch(t.Context(), store, mine); err == nil {
		t.Fatalf("another volume's epoch was accepted as ours: %d", got)
	}
}

// TestEpochSurvivesCorruption is §25.2 over the newest on-S3 format. It is one integer,
// and the integer is a fencing token: a flipped bit that turns 4 into 5 over-fences and is
// survivable, one that turns 5 into 4 hands a predecessor a live token.
func TestEpochSurvivesCorruption(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		store := sim.NewObjectStore()
		id := ids.NewAt(int64(rapid.IntRange(1, 1<<40).Draw(rt, "ms")), rand.Reader).String()
		if err := descriptor.WriteEpoch(t.Context(), store, id, int64(rapid.IntRange(1, 1<<30).Draw(rt, "epoch"))); err != nil {
			rt.Fatalf("write: %v", err)
		}
		full, err := store.Get(t.Context(), descriptor.EpochKey(id))
		if err != nil {
			rt.Fatalf("get: %v", err)
		}
		broken := bytes.Clone(full)
		if rapid.Bool().Draw(rt, "truncate") {
			broken = broken[:rapid.IntRange(0, len(full)-1).Draw(rt, "at")]
		} else {
			at := rapid.IntRange(0, len(full)-1).Draw(rt, "byte")
			broken[at] ^= 1 << rapid.IntRange(0, 7).Draw(rt, "bit")
		}
		if _, err := store.Put(t.Context(), descriptor.EpochKey(id), broken, objectstore.PutOptions{}); err != nil {
			rt.Fatalf("put: %v", err)
		}
		if got, err := descriptor.ReadEpoch(t.Context(), store, id); err == nil {
			rt.Fatalf("a damaged epoch object decoded as %d", got)
		}
	})
}

// TestEpochRefusesAnotherFormatVersion: INV-19's detection half, over an object written
// the way a newer binary would write it.
func TestEpochRefusesAnotherFormatVersion(t *testing.T) {
	t.Parallel()
	store := sim.NewObjectStore()
	id := ids.New().String()
	body := framed.Frame([]byte(`{"format_version":2,"volume_id":"` + id + `","epoch":9}`))
	if _, err := store.Put(t.Context(), descriptor.EpochKey(id), body, objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if got, err := descriptor.ReadEpoch(t.Context(), store, id); err == nil {
		t.Fatalf("an epoch from a newer format was read as %d", got)
	}
}
