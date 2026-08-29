package crypto

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// LayerFrameBytes is the plaintext each sealed frame carries, and the default a caller
// has no reason to change.
//
// It is 64 KiB because that is the qcow2 cluster size, and it costs nothing to match it.
// Measured against AES-256-GCM on this hardware: 64 KiB, 256 KiB and 1 MiB frames all
// seal at 7.7 GiB/s, and 4 MiB is *slower* (7.5) because the frame stops fitting in
// cache. The 28 bytes of nonce and tag per frame come to 0.043% at this size — 450 KiB
// on a gigabyte layer.
//
// The number only matters for a read that wants part of a layer without downloading all
// of it (v6 §23.8): the frame is the smallest unit that can be fetched and authenticated.
const LayerFrameBytes = 64 << 10

// ErrShortLayer means the sealed stream ended without a frame that says it is the last
// one. It is what a truncated layer object looks like from the inside.
var ErrShortLayer = errors.New("crypto: the sealed layer ends before its final frame")

// layerAADVersion is the generation of the framing below. It is the first byte of every
// frame's AAD, so a layer written under a different framing fails to open rather than
// opening into something else.
const layerAADVersion = 1

// SealLayer writes the sealed form of everything r yields, in frames of frameBytes
// plaintext each, to w. It is v6 §10: what leaves the host is sealed with the volume's
// DEK, and the local qcow2 is not.
//
// It is a method on Encryption and not on DEK: the volume's identity is in every frame's
// nonce, and NewEncryption is where that pairing is checked.
//
// # The nonce is derived, and here that is safe
//
// A layer has a unique number to derive from: a v7 UUID minted
// at the rotation that created the file, which is complete and read-only from that
// moment, and Chain.Rotate refuses an id that already exists. Deriving rather than
// drawing buys the property the retry story rests on — sealing the same layer twice
// produces the same bytes, so the same digest, so the same content-addressed key, and
// step 11 of v6 §9 can be repeated after any interruption without leaving an orphan.
//
// # Every frame is bound to where it is, and says whether it is the last
//
// Frames reordered, a frame duplicated, or the tail dropped all fail to open. The first
// two come from the nonce, which derives from (volume, layer, frame size, index). The
// tail is the one that matters: the manifest's SHA-256 also catches a truncated object,
// but only for a reader that has the manifest, and a layer is read by a recovery that
// may be assembling one chain out of several.
//
// The frame size is in the nonce because leaving it out is a nonce reuse on the *writer*
// side: `frame_bytes` travels in the manifest so it can be changed, and one layer sealed
// at 64 KiB and again at 1 MiB puts two plaintexts under frame 0's nonce, which leaks
// their XOR and lets the authentication key be recovered.
//
// The AAD is two bytes, the framing generation and the final flag. It was six times that
// until each field's removal was planted and only the final flag turned a test red.
func (e *Encryption) SealLayer(layerID [16]byte, frameBytes int, r io.Reader, w io.Writer) error {
	if frameBytes <= 0 {
		return fmt.Errorf("crypto: a frame of %d bytes is not a frame", frameBytes)
	}
	g, err := e.DEK.gcm()
	if err != nil {
		return err
	}
	// One frame of look-ahead, because "is this the last one" cannot be answered until
	// the read after it. A plaintext whose length is an exact multiple of frameBytes is
	// the case a length check alone would get wrong.
	cur, next := make([]byte, frameBytes), make([]byte, frameBytes)
	n, err := readFull(r, cur)
	if err != nil {
		return err
	}
	for idx := uint64(0); ; idx++ {
		m, err := readFull(r, next)
		if err != nil {
			return err
		}
		final := m == 0
		sealed := g.Seal(nil, layerNonce(e.VolumeID, layerID, frameBytes, idx),
			cur[:n], layerAAD(final))
		if _, err := w.Write(sealed); err != nil {
			return fmt.Errorf("crypto: writing sealed frame %d: %w", idx, err)
		}
		if final {
			return nil
		}
		cur, next, n = next, cur, m
	}
}

// OpenLayer is SealLayer's inverse. It fails closed on any tamper and never writes
// plaintext it has not authenticated: each frame is opened whole before a byte of it
// reaches w.
func (e *Encryption) OpenLayer(layerID [16]byte, frameBytes int, r io.Reader, w io.Writer) error {
	if frameBytes <= 0 {
		return fmt.Errorf("crypto: a frame of %d bytes is not a frame", frameBytes)
	}
	g, err := e.DEK.gcm()
	if err != nil {
		return err
	}
	size := frameBytes + TagSize
	cur, next := make([]byte, size), make([]byte, size)
	n, err := readFull(r, cur)
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrShortLayer
	}
	for idx := uint64(0); ; idx++ {
		m, err := readFull(r, next)
		if err != nil {
			return err
		}
		final := m == 0
		// A frame that is not the last must be exactly full. Anything else is a stream
		// that lost bytes in the middle, and saying so beats the authentication failure
		// it would otherwise become one frame later.
		if !final && n != size {
			return fmt.Errorf("%w: frame %d carries %d bytes, not %d", ErrShortLayer, idx, n, size)
		}
		pt, err := g.Open(nil, layerNonce(e.VolumeID, layerID, frameBytes, idx),
			cur[:n], layerAAD(final))
		if err != nil {
			// Deliberately not saying which of the reasons it was. A frame fails to
			// open because it was altered, because it is in the wrong place, because it
			// belongs to another layer, or because the stream was cut short and this is
			// the frame that had claimed not to be last — and telling them apart from
			// the outside is exactly what an attacker with a bucket would want.
			return fmt.Errorf("%w: frame %d of layer %x", ErrOpen, idx, layerID)
		}
		if _, err := w.Write(pt); err != nil {
			return fmt.Errorf("crypto: writing plaintext frame %d: %w", idx, err)
		}
		if final {
			return nil
		}
		cur, next, n = next, cur, m
	}
}

// readFull fills buf, and reports a short read as a count rather than an error: a short
// read is the ordinary end of both streams here, and only an error that is neither EOF
// nor a short read is a failure.
func readFull(r io.Reader, buf []byte) (int, error) {
	n, err := io.ReadFull(r, buf)
	switch {
	case err == nil, errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return n, nil
	default:
		return 0, fmt.Errorf("crypto: reading a layer: %w", err)
	}
}

// layerNonce is the deterministic nonce for one frame: the layer's identity, the frame
// index, and its own domain-separation tag so it can never collide with deriveNonce's.
//
// The frame size is part of it — see SealLayer.
func layerNonce(volumeID, layerID [16]byte, frameBytes int, idx uint64) []byte {
	var buf [16 + 16 + 8 + 8 + 6]byte
	copy(buf[0:16], volumeID[:])
	copy(buf[16:32], layerID[:])
	binary.LittleEndian.PutUint64(buf[32:40], uint64(frameBytes))
	binary.LittleEndian.PutUint64(buf[40:48], idx)
	copy(buf[48:], "layer1")
	sum := sha256.Sum256(buf[:])
	return sum[:NonceSize]
}

// layerAAD says which framing wrote this frame and whether it is the last one. Its
// volume, its layer, its position and its size are all bound elsewhere — see SealLayer.
//
// The version byte is the one field here that no test can turn red, and it stays for the
// reason framed.CheckVersion's too-old branch stays: it is where the first change to this
// framing lands, and adding it after layers exist in a bucket is not a change anybody can
// make.
func layerAAD(final bool) []byte {
	b := []byte{layerAADVersion, 0}
	if final {
		b[1] = 1
	}
	return b
}
