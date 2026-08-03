package image_test

import (
	"bytes"
	"testing"

	"pgregory.net/rapid"

	"github.com/spin-stack/storage/internal/cow"
	"github.com/spin-stack/storage/internal/image"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

func vol7() [16]byte {
	var v [16]byte
	v[6], v[8] = 0x70, 0x80
	return v
}

// §25.2 for the image format: publish/load is total and exact.
//
// For any sequence of writes and discards, an image published from a view and loaded
// back answers Read identically at every offset. This is the property the format exists
// to have — an image that is merely "close" loses guest data silently, since the
// difference between a byte the guest wrote and a zero is invisible to everything
// downstream.
func TestPublishLoadRoundTrip(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		const size = 8192
		ctx := t.Context()
		store := sim.NewObjectStore()
		vol := vol7()

		view := cow.NewIntervalMap()
		for range rapid.IntRange(0, 12).Draw(rt, "ops") {
			off := uint64(rapid.IntRange(0, size-1).Draw(rt, "off"))
			n := rapid.IntRange(1, 1024).Draw(rt, "len")
			if off+uint64(n) > size {
				n = size - int(off)
			}
			if rapid.Bool().Draw(rt, "discard") {
				view.Clear(off, uint64(n))
			} else {
				view.Overwrite(off, bytes.Repeat([]byte{byte(rapid.IntRange(1, 255).Draw(rt, "b"))}, n))
			}
		}

		if _, err := image.Publish(ctx, store, vol, view, 42, ""); err != nil {
			rt.Fatalf("Publish: %v", err)
		}
		loaded, man, _, err := image.Load(ctx, store, vol)
		if err != nil {
			rt.Fatalf("Load: %v", err)
		}
		if man.Sequence != 42 {
			rt.Fatalf("sequence = %d, want 42", man.Sequence)
		}

		want, got := make([]byte, size), make([]byte, size)
		view.Read(0, want)
		loaded.Read(0, got)
		if !bytes.Equal(want, got) {
			for i := range want {
				if want[i] != got[i] {
					rt.Fatalf("byte %d: published %d, loaded %d", i, want[i], got[i])
				}
			}
		}
	})
}

// Corruption is detected, never applied. A chunk that does not hash to its key, or that
// is the wrong length, fails the load — because the alternative is a view with a hole in
// it, and a hole reads as zeros, which is indistinguishable from a range the guest never
// wrote.
func TestLoadRefusesCorruptedChunks(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		ctx := t.Context()
		store := sim.NewObjectStore()
		vol := vol7()

		view := cow.NewIntervalMap()
		view.Overwrite(0, bytes.Repeat([]byte{0xAB}, 2048))
		if _, err := image.Publish(ctx, store, vol, view, 1, ""); err != nil {
			rt.Fatal(err)
		}

		objs, err := store.List(ctx, image.Prefix(vol)+"chunks/")
		if err != nil || len(objs) == 0 {
			rt.Fatalf("no chunks to corrupt: %v", err)
		}
		key := objs[0].Key
		body, err := store.Get(ctx, key)
		if err != nil {
			rt.Fatal(err)
		}

		// Either flip a bit or truncate — the two shapes §25.2 names.
		if rapid.Bool().Draw(rt, "truncate") {
			n := rapid.IntRange(0, len(body)-1).Draw(rt, "keep")
			body = body[:n]
		} else {
			i := rapid.IntRange(0, len(body)-1).Draw(rt, "byte")
			body[i] ^= 1 << rapid.IntRange(0, 7).Draw(rt, "bit")
		}
		// Unconditional Put is how the corruption gets in: it is what a backend
		// silently returning wrong bytes looks like from here.
		if _, err := store.Put(ctx, key, body, objectstore.PutOptions{}); err != nil {
			rt.Fatal(err)
		}

		if _, _, _, err := image.Load(ctx, store, vol); err == nil {
			rt.Fatal("a corrupted chunk loaded without complaint; the guest would be served zeros or wrong bytes")
		}
	})
}
