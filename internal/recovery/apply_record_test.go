package recovery_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/sim"
	"github.com/spin-stack/storage/internal/wal"
	"github.com/spin-stack/storage/internal/wal/format"
)

// TestApplyRecordRefusesASealedRecordWithNoKey pins the rule that makes the whole
// class of "replayed without the key" bugs impossible to write silently.
//
// KeyID 0 is not a key version: it is the on-disk marker for a cleartext payload
// (wal/crypto.go, ErrUnversionedKey). So a record carrying KeyID != 0 with no
// Encryption to open it is unambiguously "this is sealed and I do not hold the key",
// and the only safe answer is to fail. Folding its ciphertext into the read view is
// not a degraded answer, it is a wrong one: crypto.Seal returns ciphertext of exactly
// the plaintext's length with the GCM tag stored separately in the header, so the
// overwrite is byte-for-byte the right shape and nothing downstream can tell.
func TestApplyRecordRefusesASealedRecordWithNoKey(t *testing.T) {
	const ciphertext = "\x91\x0c\xd4\x22ciphertext-bytes"

	tests := []struct {
		name    string
		rec     wal.Record
		enc     *wal.Encryption
		wantErr bool
	}{
		{
			name:    "sealed record with no key is refused",
			rec:     wal.Record{Type: format.RecordWrite, Sequence: 7, KeyID: 3, Offset: 0, Payload: []byte(ciphertext)},
			enc:     nil,
			wantErr: true,
		},
		{
			name:    "plaintext record with no key is applied",
			rec:     wal.Record{Type: format.RecordWrite, Sequence: 7, KeyID: 0, Offset: 0, Payload: []byte("hello")},
			enc:     nil,
			wantErr: false,
		},
		{
			// A DISCARD carries no payload to decrypt, so the key is irrelevant to it.
			// It must keep working with no key or a truncated volume's tombstones stop
			// replaying.
			name:    "discard with no key is applied",
			rec:     wal.Record{Type: format.RecordDiscard, Sequence: 7, Offset: 0, Length: 512},
			enc:     nil,
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			view := cow.NewIntervalMap()
			err := recovery.ApplyRecord(view, tc.enc, tc.rec)
			if tc.wantErr {
				if err == nil {
					t.Fatal("ApplyRecord accepted a sealed record with no key")
				}
				// The refusal has to leave nothing behind: an error plus a view that
				// already holds the ciphertext is the same wrong answer with a log line.
				buf := make([]byte, len(ciphertext))
				view.Read(tc.rec.Offset, buf)
				if bytes.Contains(buf, []byte("ciphertext-bytes")) {
					t.Fatal("ApplyRecord returned an error but folded the ciphertext into the view anyway")
				}
				return
			}
			if err != nil {
				t.Fatalf("ApplyRecord: %v", err)
			}
		})
	}
}

// TestRecoverWithoutTheKeyRefusesRatherThanServingCiphertext is the same rule at the
// seam that actually had the defect: internal/agent passed a literal nil Encryption
// into RecoverOver for a volume it had just unwrapped a DEK for, so every range served
// from the recovered base was GCM ciphertext handed to the guest as its own data.
//
// It drives the real path — seal, upload, recover — rather than a hand-built record,
// because the hand-built record is the one thing that was never in doubt.
func TestRecoverWithoutTheKeyRefusesRatherThanServingCiphertext(t *testing.T) {
	ctx := t.Context()
	store := sim.NewObjectStore()
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	vol := v7Vol()

	dek, _ := crypto.GenerateDEK(&ramp{1}, 1)
	enc := &wal.Encryption{DEK: dek, VolumeID: vol}

	l := wal.NewLog(sim.NewDisk(), "wal", clk, vol, 1, wal.Limits{MaxUnflushedBytes: 1 << 20})
	l.EnableEncryption(enc)
	l.EnableRemote(wal.NewBatcher(clk, vol, 1, dek.KeyID, wal.DefaultBatchConfig()), wal.NewUploader(store, 3), leaseOK{})

	secret := []byte("GUEST-DATA-THE-OWNER-WROTE")
	if _, err := l.Write(0, secret, 0); err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Recovering the very same volume with no key must fail, not succeed quietly.
	view, _, err := recovery.Recover(ctx, store, nil, vol, 1)
	if err == nil {
		buf := make([]byte, len(secret))
		view.Read(0, buf)
		t.Fatalf("Recover with no key succeeded and would serve %q to the guest", buf)
	}
	if errors.Is(err, wal.ErrPlaintextCRC) {
		t.Fatalf("the refusal should name the missing key, not a CRC mismatch: %v", err)
	}
}
