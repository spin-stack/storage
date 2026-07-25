package ids_test

import (
	"math/rand"
	"testing"

	"github.com/spin-stack/storage/internal/ids"
)

func TestNewIsV7(t *testing.T) {
	for range 100 {
		if u := ids.New(); !ids.IsV7(u) {
			t.Fatalf("New() produced non-v7 uuid: version=%d", u.Version())
		}
	}
}

func TestNewAtIsDeterministicAndV7(t *testing.T) {
	r1 := rand.New(rand.NewSource(42))
	r2 := rand.New(rand.NewSource(42))
	a := ids.NewAt(1_700_000_000_000, r1)
	b := ids.NewAt(1_700_000_000_000, r2)
	if a != b {
		t.Fatalf("NewAt not deterministic for same inputs: %s vs %s", a, b)
	}
	if !ids.IsV7(a) {
		t.Fatalf("NewAt produced non-v7: version=%d", a.Version())
	}
}

func TestNewAtDiffersByTimeAndRandom(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	a := ids.NewAt(1000, r)
	b := ids.NewAt(2000, r)
	if a == b {
		t.Fatal("different timestamps should yield different ids")
	}
}
