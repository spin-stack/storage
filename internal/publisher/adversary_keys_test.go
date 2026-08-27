package publisher_test

import (
	"crypto/rand"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/publisher"
)

// `kek_id` is carried from the catalog to the Agent to the publisher, and nothing ever
// compares it with the KEK this host actually holds.
//
// crypto/kek.go says the opposite where KEKID is defined: "`kek_id` is what a volume row
// records and what the Agent compares its own key against before unwrapping", and the
// paragraph goes on to explain that without it "the mismatch would then surface as an
// AEAD failure with no hint that the files differ". There is no such comparison in
// publisher.encryption or in recovery.encryption — the two callers — so the failure this
// field exists to prevent is exactly the failure an operator gets.
//
// It is fail-closed, so this is not a data-loss finding. It is the one message an
// operator sees at 3am when a host was handed the wrong `-kek-file`, and it points at
// the DEK.
func TestAdversaryWrongKEKBlamesTheDEK(t *testing.T) {
	t.Parallel()
	w := newWorld(t)

	// The same volume, the same wrapped DEK, the same recorded kek_id — and a host
	// holding a different key file.
	var other [crypto.DEKSize]byte
	if _, err := rand.Read(other[:]); err != nil {
		t.Fatal(err)
	}
	wrongKMS := crypto.NewDevKMS(other, crypto.KEKID(other))
	recorded := w.keys.keys[w.vol].KEKID
	if wrongKMS.KEKID() == recorded {
		t.Fatal("the fixture drew the same KEK twice")
	}
	pub := publisher.New(w.store, wrongKMS, w.keys, w.files)

	err := pub.Publish(t.Context(), w.layer)
	if err == nil {
		t.Fatal("publishing with the wrong KEK succeeded")
	}
	if !strings.Contains(err.Error(), recorded) && !strings.Contains(err.Error(), wrongKMS.KEKID()) {
		t.Fatalf("this host holds KEK %s and volume %s was wrapped under %s, and the error "+
			"names neither: %q. Nothing on this path compares the two, so the wrong "+
			"-kek-file is reported as a broken DEK.",
			wrongKMS.KEKID(), w.vol, recorded, err)
	}
}
