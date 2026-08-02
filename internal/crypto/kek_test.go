package crypto_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// LoadKEK is the one place a host's key-encryption key enters either binary, and it is
// shared precisely because it used to not be: `cmd/control-plane` and the Agent each
// had their own reader with different rules, so a hex-encoded key file was a working
// deployment for the one that wraps DEKs and a startup failure for the one that
// unwraps them.

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
	raw := bytes.Repeat([]byte{0xAB}, crypto.DEKSize)
	hexed := []byte(strings.Repeat("ab", crypto.DEKSize))

	tests := []struct {
		name string
		body []byte
		// write is false for the missing-file case: there is nothing to write.
		write   bool
		wantErr error
	}{
		{name: "32 raw bytes", body: raw, write: true},
		{name: "64 hex characters — what survives a copy-paste", body: hexed, write: true},
		{
			name: "a trailing newline, which every editor and echo adds",
			body: append(append([]byte(nil), hexed...), '\n'), write: true,
		},
		{name: "surrounding whitespace", body: []byte("  " + string(hexed) + "\n\n"), write: true},
		{name: "empty file", body: nil, write: true, wantErr: crypto.ErrBadKEK},
		{
			// Still 32 usable-looking bytes to anything that does not check.
			name: "one byte short — a truncated write",
			body: raw[:crypto.DEKSize-1], write: true, wantErr: crypto.ErrBadKEK,
		},
		{
			name: "63 hex characters — an odd-length paste",
			body: hexed[:len(hexed)-1], write: true, wantErr: crypto.ErrBadKEK,
		},
		{
			name: "a whole file that is not a key",
			body: bytes.Repeat([]byte("x"), 4096), write: true, wantErr: crypto.ErrBadKEK,
		},
		{name: "no file at all", write: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := sim.NewDisk()
			if tc.write {
				writeKEKFile(t, d, "kek", tc.body)
			}
			got, err := crypto.LoadKEK(d, "kek")
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
				// Every accepted encoding must produce the *same* key. That is the
				// whole reason both encodings are allowed: the Control Plane and the
				// Agent may have the file in different shapes and must still agree.
				if !bytes.Equal(got[:], raw) {
					t.Fatalf("the KEK read back as %x, want %x", got[:8], raw[:8])
				}
			}
			if err != nil && got != ([crypto.DEKSize]byte{}) {
				t.Fatal("a failed load returned key material")
			}
		})
	}
}

// TestLoadKEKRoundTripsThroughTheKMS closes the loop the size check protects: the bytes
// on disk are the bytes that unwrap a DEK wrapped under them. Without it LoadKEK could
// return a correctly-sized, correctly-rejected-when-wrong, entirely scrambled key and
// every case above would still pass.
func TestLoadKEKRoundTripsThroughTheKMS(t *testing.T) {
	var kek [crypto.DEKSize]byte
	for i := range kek {
		kek[i] = byte(i * 7)
	}
	d := sim.NewDisk()
	writeKEKFile(t, d, "kek", kek[:])

	loaded, err := crypto.LoadKEK(d, "kek")
	if err != nil {
		t.Fatal(err)
	}
	dek, err := crypto.GenerateDEK(&ramp{n: 5}, 3)
	if err != nil {
		t.Fatal(err)
	}
	wrapped, err := crypto.NewDevKMS(kek, crypto.KEKID(kek)).WrapDEK(&ramp{n: 9}, dek)
	if err != nil {
		t.Fatal(err)
	}
	back, err := crypto.NewDevKMS(loaded, crypto.KEKID(loaded)).UnwrapDEK(wrapped, 3)
	if err != nil {
		t.Fatalf("a DEK wrapped under the file's key did not unwrap under the loaded one: %v", err)
	}
	if back.Key != dek.Key {
		t.Fatal("the loaded KEK unwrapped a different key")
	}
}

// TestKEKIDIsDerivedFromTheMaterial. The id is what a volume row records and what the
// Agent compares against before unwrapping, so the two binaries must reach the same
// string from the same file without an operator retyping it — including when one of
// them has the key as hex and the other as raw bytes.
func TestKEKIDIsDerivedFromTheMaterial(t *testing.T) {
	rawKey := bytes.Repeat([]byte{0x5C}, crypto.DEKSize)
	d := sim.NewDisk()
	writeKEKFile(t, d, "raw", rawKey)
	writeKEKFile(t, d, "hex", []byte(strings.Repeat("5c", crypto.DEKSize)+"\n"))

	fromRaw, err := crypto.LoadKEK(d, "raw")
	if err != nil {
		t.Fatal(err)
	}
	fromHex, err := crypto.LoadKEK(d, "hex")
	if err != nil {
		t.Fatal(err)
	}
	if crypto.KEKID(fromRaw) != crypto.KEKID(fromHex) {
		t.Fatalf("the same key encoded two ways got two ids: %q and %q",
			crypto.KEKID(fromRaw), crypto.KEKID(fromHex))
	}

	var other [crypto.DEKSize]byte
	other[0] = 1
	if crypto.KEKID(other) == crypto.KEKID(fromRaw) {
		t.Fatal("two different keys share an id")
	}
	if !strings.HasPrefix(crypto.KEKID(fromRaw), "kek-") {
		t.Fatalf("a kek id must be recognisable as one: %q", crypto.KEKID(fromRaw))
	}
}

// ramp is a deterministic byte source; the wrap nonce here needs to be reproducible,
// not secret.
type ramp struct{ n byte }

func (r *ramp) Read(p []byte) (int, error) {
	for i := range p {
		r.n++
		p[i] = r.n
	}
	return len(p), nil
}
