package format

import (
	"encoding/hex"
	"fmt"
)

// UUIDString renders a 16-byte UUID in canonical 8-4-4-4-12 form.
func UUIDString(id [16]byte) string {
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(id[0:4]),
		hex.EncodeToString(id[4:6]),
		hex.EncodeToString(id[6:8]),
		hex.EncodeToString(id[8:10]),
		hex.EncodeToString(id[10:16]),
	)
}

// WALObjectKey builds the deterministic, idempotent S3 key for a WAL object
// (§14.2): wal/<volume_id>/<epoch>/<first_seq>-<last_seq>-<sha256_prefix8>.wal.
// The sha256 prefix is the first 4 bytes (8 hex chars) of the object payload hash.
func WALObjectKey(volumeID [16]byte, epoch, firstSeq, lastSeq uint64, payloadSHA256 [32]byte) string {
	return fmt.Sprintf("wal/%s/%d/%d-%d-%s.wal",
		UUIDString(volumeID), epoch, firstSeq, lastSeq, hex.EncodeToString(payloadSHA256[:4]))
}
