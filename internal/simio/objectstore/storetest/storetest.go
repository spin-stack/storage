// Package storetest is the objectstore.Store contract as an executable spec: one
// set of assertions every implementation must satisfy — the deterministic sim, the
// filesystem-backed store, and the S3-backed one running against a real backend in
// a container. Writing it once is the point: an implementation that drifts from the
// others fails here rather than in a recovery.
package storetest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/real"
)

// NewStore returns a fresh, empty store for one subtest.
type NewStore func(t *testing.T) objectstore.Store

// RunContract runs the full contract against the implementation newStore builds.
func RunContract(t *testing.T, newStore NewStore) {
	t.Helper()
	ctx := t.Context()

	t.Run("put/get/head round trip", func(t *testing.T) {
		s := newStore(t)
		key := "wal/vol/0/1-1-abcd.wal"
		data := []byte("encrypted-record-bytes")

		res, err := s.Put(ctx, key, data, objectstore.PutOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if res.ETag == "" {
			t.Fatal("expected an ETag; the CAS protocol (§12.4) needs one")
		}
		got, err := s.Get(ctx, key)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("get mismatch: %q err=%v", got, err)
		}
		info, err := s.Head(ctx, key)
		if err != nil || info.Size != int64(len(data)) || info.ETag != res.ETag {
			t.Fatalf("head mismatch: %+v err=%v", info, err)
		}
	})

	t.Run("get of a missing key is ErrNotFound", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Get(ctx, "absent"); !errors.Is(err, objectstore.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
		if _, err := s.Head(ctx, "absent"); !errors.Is(err, objectstore.ErrNotFound) {
			t.Fatalf("head: want ErrNotFound, got %v", err)
		}
	})

	t.Run("If-None-Match is create-only", func(t *testing.T) {
		s := newStore(t)
		opts := objectstore.PutOptions{IfNoneMatch: true}
		if _, err := s.Put(ctx, "k", []byte("v1"), opts); err != nil {
			t.Fatalf("first create: %v", err)
		}
		if _, err := s.Put(ctx, "k", []byte("v2"), opts); !errors.Is(err, objectstore.ErrPreconditionFailed) {
			t.Fatalf("want ErrPreconditionFailed on second create, got %v", err)
		}
		if got, _ := s.Get(ctx, "k"); string(got) != "v1" {
			t.Fatalf("create-only must not overwrite: got %q", got)
		}
	})

	t.Run("If-Match is a compare-and-swap", func(t *testing.T) {
		s := newStore(t)
		res, err := s.Put(ctx, "epoch", []byte("epoch-1"), objectstore.PutOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Put(ctx, "epoch", []byte("epoch-2"), objectstore.PutOptions{IfMatch: `"wrong"`}); !errors.Is(err, objectstore.ErrPreconditionFailed) {
			t.Fatalf("want ErrPreconditionFailed on a stale CAS, got %v", err)
		}
		if got, _ := s.Get(ctx, "epoch"); string(got) != "epoch-1" {
			t.Fatalf("a failed CAS must not write: got %q", got)
		}
		if _, err := s.Put(ctx, "epoch", []byte("epoch-2"), objectstore.PutOptions{IfMatch: res.ETag}); err != nil {
			t.Fatalf("CAS with the current ETag: %v", err)
		}
	})

	t.Run("list by prefix, sorted", func(t *testing.T) {
		s := newStore(t)
		for _, k := range []string{"wal/v/0/2.wal", "wal/v/0/1.wal", "other/x"} {
			if _, err := s.Put(ctx, k, []byte(k), objectstore.PutOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		got, err := s.List(ctx, "wal/v/0/")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0].Key != "wal/v/0/1.wal" || got[1].Key != "wal/v/0/2.wal" {
			t.Fatalf("unexpected list: %+v", got)
		}
	})

	t.Run("delete then get is ErrNotFound", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Put(ctx, "k", []byte("v"), objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, "k"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get(ctx, "k"); !errors.Is(err, objectstore.ErrNotFound) {
			t.Fatalf("want ErrNotFound after delete, got %v", err)
		}
	})

	t.Run("delete of a missing key is ErrNotFound", func(t *testing.T) {
		s := newStore(t)
		if err := s.Delete(ctx, "never-there"); !errors.Is(err, objectstore.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	})

	t.Run("binary payloads round trip unchanged", func(t *testing.T) {
		s := newStore(t)
		payload := bytes.Repeat([]byte{0x00, 0xFF, 0x7F, 0xAB}, 4096)
		if _, err := s.Put(ctx, "bin", payload, objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		got, err := s.Get(ctx, "bin")
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("binary round trip changed the bytes (err=%v)", err)
		}
	})

	t.Run("an empty object is a real object", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Put(ctx, "empty", nil, objectstore.PutOptions{IfNoneMatch: true}); err != nil {
			t.Fatal(err)
		}
		info, err := s.Head(ctx, "empty")
		if err != nil || info.Size != 0 {
			t.Fatalf("head of an empty object: %+v err=%v", info, err)
		}
		if got, err := s.Get(ctx, "empty"); err != nil || len(got) != 0 {
			t.Fatalf("get of an empty object: %q err=%v", got, err)
		}
	})

	// The streaming PUT, which is how a sealed layer reaches the bucket: it is the one
	// object nobody wants a second copy of in memory. Every assertion below is about the
	// object that ends up stored, because the caller's next act is to publish a manifest
	// naming the digest of exactly those bytes.
	t.Run("PutStream stores what the body yields", func(t *testing.T) {
		s := newStore(t)
		body := bytes.Repeat([]byte("sealed-frame"), 4096)
		res, err := s.PutStream(ctx, "layers/sha256/ab/cd/abcd", bytes.NewReader(body), int64(len(body)), objectstore.PutOptions{IfNoneMatch: true})
		if err != nil {
			t.Fatal(err)
		}
		if res.ETag == "" {
			t.Fatal("expected an ETag")
		}
		got, err := s.Get(ctx, "layers/sha256/ab/cd/abcd")
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("get after PutStream: %d bytes err=%v", len(got), err)
		}
		info, err := s.Head(ctx, "layers/sha256/ab/cd/abcd")
		if err != nil || info.Size != int64(len(body)) || info.ETag != res.ETag {
			t.Fatalf("head after PutStream: %+v err=%v", info, err)
		}
		// The two entry points must agree byte for byte and ETag for ETag, or a retry
		// that takes the other path reads its own object as somebody else's.
		if _, err := s.Put(ctx, "same", body, objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		if other, _ := s.Head(ctx, "same"); other.ETag != res.ETag {
			t.Fatalf("Put and PutStream disagree about the same bytes: %q vs %q", other.ETag, res.ETag)
		}
	})

	t.Run("PutStream honours create-only", func(t *testing.T) {
		s := newStore(t)
		opts := objectstore.PutOptions{IfNoneMatch: true}
		if _, err := s.PutStream(ctx, "k", strings.NewReader("v1"), 2, opts); err != nil {
			t.Fatal(err)
		}
		if _, err := s.PutStream(ctx, "k", strings.NewReader("v2"), 2, opts); !errors.Is(err, objectstore.ErrPreconditionFailed) {
			t.Fatalf("want ErrPreconditionFailed on the second create, got %v", err)
		}
		if got, _ := s.Get(ctx, "k"); string(got) != "v1" {
			t.Fatalf("create-only must not overwrite: %q", got)
		}
	})

	// A body that ends early must leave nothing. The size is what the caller measured
	// while it computed the digest, so a short body means the source changed underneath
	// the upload, and a truncated object at a content-addressed key is an object that
	// hashes to something other than its own name — permanently, since the key is
	// create-only from then on.
	t.Run("PutStream refuses a body that ends early", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.PutStream(ctx, "short", strings.NewReader("four"), 64, objectstore.PutOptions{IfNoneMatch: true}); err == nil {
			t.Fatal("a body shorter than the declared size must be an error")
		}
		if _, err := s.Get(ctx, "short"); !errors.Is(err, objectstore.ErrNotFound) {
			t.Fatalf("a refused stream must store nothing, got %v", err)
		}
	})

	// And a body that runs long is cut at the declared size rather than appended: size
	// is what the object store was told the object is, and S3 sends exactly that many
	// bytes whatever the reader goes on to offer.
	t.Run("PutStream stores no more than the declared size", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.PutStream(ctx, "long", strings.NewReader("0123456789"), 4, objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		if got, err := s.Get(ctx, "long"); err != nil || string(got) != "0123" {
			t.Fatalf("stored %q err=%v, want %q", got, err, "0123")
		}
	})

	// Finding 6. Create-only is what makes a retried WAL upload harmless (INV-21) and
	// a published manifest immutable (INV-16); the If-Match CAS is the §12.4 epoch
	// fence (INV-10). Both are claims about *concurrent* writers, and neither had a
	// single concurrent test — a check-then-act implementation passes every
	// sequential assertion above while admitting two winners, silently, with err ==
	// nil for both. Two promoters both winning CompareAndAdvance is split brain.
	t.Run("create-only admits exactly one concurrent writer", func(t *testing.T) {
		s := newStore(t)
		const writers = 16
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			winners []string
			losers  []error
			start   = make(chan struct{})
		)
		for i := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start // release every writer at once, not as they are scheduled
				body := fmt.Sprintf("writer-%d", i)
				_, err := s.Put(ctx, "wal/contended.wal", []byte(body), objectstore.PutOptions{IfNoneMatch: true})
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					winners = append(winners, body)
					return
				}
				losers = append(losers, err)
			}()
		}
		close(start)
		wg.Wait()

		if len(winners) != 1 {
			t.Fatalf("create-only admitted %d concurrent writers, want exactly 1", len(winners))
		}
		for _, err := range losers {
			if !errors.Is(err, objectstore.ErrPreconditionFailed) {
				t.Fatalf("a loser got %v, want ErrPreconditionFailed", err)
			}
		}
		got, err := s.Get(ctx, "wal/contended.wal")
		if err != nil || string(got) != winners[0] {
			t.Fatalf("stored body = %q err=%v, want the winner's %q", got, err, winners[0])
		}
	})

	t.Run("If-Match admits exactly one concurrent winner", func(t *testing.T) {
		s := newStore(t)
		res, err := s.Put(ctx, "epoch", []byte("epoch-1"), objectstore.PutOptions{})
		if err != nil {
			t.Fatal(err)
		}
		const contenders = 16
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			winners []string
			losers  []error
			start   = make(chan struct{})
		)
		for i := range contenders {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start // release every contender at once
				body := fmt.Sprintf("epoch-%d", i+2)
				_, err := s.Put(ctx, "epoch", []byte(body), objectstore.PutOptions{IfMatch: res.ETag})
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					winners = append(winners, body)
					return
				}
				losers = append(losers, err)
			}()
		}
		close(start)
		wg.Wait()

		if len(winners) != 1 {
			t.Fatalf("%d concurrent CAS operations won on one ETag, want exactly 1 — that is split brain", len(winners))
		}
		for _, err := range losers {
			if !errors.Is(err, objectstore.ErrPreconditionFailed) {
				t.Fatalf("a fenced contender got %v, want ErrPreconditionFailed", err)
			}
		}
		if got, _ := s.Get(ctx, "epoch"); string(got) != winners[0] {
			t.Fatalf("stored body = %q, want the winner's %q", got, winners[0])
		}
	})

	// Finding 3. An ETag is a CAS token and nothing else. S3 returns a quoted MD5 and
	// a multipart object's is an MD5-of-MD5s with a `-N` suffix; both in-process
	// stores happen to use SHA-256. Any code that compares an ETag against a content
	// hash it computed reports a byte-perfect object as divergent on a real backend —
	// which is the §14.5 lost-response path, i.e. the most common S3 failure there is.
	// The contract pins only what an ETag is allowed to promise.
	t.Run("an ETag is opaque, stable, and changes with the content", func(t *testing.T) {
		s := newStore(t)
		first, err := s.Put(ctx, "k", []byte("v1"), objectstore.PutOptions{})
		if err != nil {
			t.Fatal(err)
		}
		info, err := s.Head(ctx, "k")
		if err != nil {
			t.Fatal(err)
		}
		if info.ETag != first.ETag {
			t.Fatalf("HEAD ETag %q != PUT ETag %q; a CAS token that is not stable is not a CAS token", info.ETag, first.ETag)
		}
		second, err := s.Put(ctx, "k", []byte("v2-longer"), objectstore.PutOptions{IfMatch: first.ETag})
		if err != nil {
			t.Fatalf("CAS with the ETag we were handed must work: %v", err)
		}
		if second.ETag == first.ETag {
			t.Fatal("the ETag did not change when the content did; a stale CAS would then succeed")
		}
		if _, err := s.Put(ctx, "k", []byte("v3"), objectstore.PutOptions{IfMatch: first.ETag}); !errors.Is(err, objectstore.ErrPreconditionFailed) {
			t.Fatalf("CAS on the superseded ETag: %v, want ErrPreconditionFailed", err)
		}
	})

	// Finding 8. The GC runs on a schedule and an operator can run it by hand at the
	// same time. Whether Delete of a key another pass already marked is an error or a
	// no-op decides whether the second pass abandons the rest of the bucket, so the
	// contract has to state it rather than leave each implementation to choose.
	t.Run("delete of an already-marked key is ErrNotFound", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Put(ctx, "k", []byte("v"), objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, "k"); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, "k"); !errors.Is(err, objectstore.ErrNotFound) {
			t.Fatalf("second delete: %v, want ErrNotFound", err)
		}
	})

	// Findings 1 and 7. INV-14 says a GC mistake costs a restore, not the data. That
	// argument only holds if Restore exists on the store the GC actually runs
	// against, and if it either returns the marked bytes or says why it cannot.
	t.Run("a mark is reversible", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Put(ctx, "wal/v/1/x.wal", []byte("acked-payload"), objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, "wal/v/1/x.wal"); err != nil {
			t.Fatal(err)
		}
		if err := s.Restore(ctx, "wal/v/1/x.wal"); err != nil {
			t.Fatalf("restore: %v", err)
		}
		if got, err := s.Get(ctx, "wal/v/1/x.wal"); err != nil || string(got) != "acked-payload" {
			t.Fatalf("restored body = %q err=%v", got, err)
		}
	})

	t.Run("restore of a key that was never marked is ErrNotFound", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Put(ctx, "k", []byte("v"), objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := s.Restore(ctx, "k"); !errors.Is(err, objectstore.ErrNotFound) {
			t.Fatalf("restore of a live key: %v, want ErrNotFound", err)
		}
		if err := s.Restore(ctx, "absent"); !errors.Is(err, objectstore.ErrNotFound) {
			t.Fatalf("restore of a missing key: %v, want ErrNotFound", err)
		}
	})

	// Finding 7. The un-GC runbook is "restore the version the sweep marked". If anything
	// wrote the key since, returning the new bytes would have the operator rebuild a
	// volume from content that was never marked. Refuse, distinctly.
	t.Run("restore after the key was rewritten is refused, not silently wrong", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Put(ctx, "wal/v/1/x.wal", []byte("the-marked-bytes"), objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, "wal/v/1/x.wal"); err != nil {
			t.Fatal(err)
		}
		// A writer rewrites the key; on a versioned bucket this stacks a new current
		// version over the delete marker.
		if _, err := s.Put(ctx, "wal/v/1/x.wal", []byte("different"), objectstore.PutOptions{IfNoneMatch: true}); err != nil {
			t.Fatalf("create-only over a marked key must succeed: %v", err)
		}
		err := s.Restore(ctx, "wal/v/1/x.wal")
		if !errors.Is(err, objectstore.ErrRestoreSuperseded) {
			t.Fatalf("restore after a rewrite: %v, want ErrRestoreSuperseded", err)
		}
		if got, _ := s.Get(ctx, "wal/v/1/x.wal"); string(got) != "different" {
			t.Fatalf("a refused restore must not change the object: %q", got)
		}
	})

	// The two cases above spawn goroutines, and an in-process mutex makes
	// read-compare-publish look atomic to anything in the same process — so a
	// check-then-act conditional write passes both every time and admits a second winner
	// the moment the contenders are separate processes. The filesystem store shipped
	// exactly that for If-Match. These two cases re-exec the test binary, so the exclusion
	// has to be real: on `-object-store-dir`, two Agents publishing one volume's manifest
	// are two processes on one filesystem.
	t.Run("create-only admits exactly one writer across processes", func(t *testing.T) {
		dir := crossProcessDir(t, newStore(t))
		s, err := real.NewObjectStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		for round := range crossProcessRounds {
			key := fmt.Sprintf("wal/contended-%d.wal", round)
			winners := contend(t, dir, key, "", true)
			if len(winners) != 1 {
				t.Fatalf("round %d: %d of %d separate processes created %q with If-None-Match, want exactly 1 — every one of them believes it owns the object",
					round, len(winners), crossProcessWriters, key)
			}
			assertStoredBodyIsTheWinners(t, s, key, winners[0])
		}
	})

	t.Run("If-Match admits exactly one winner across processes", func(t *testing.T) {
		dir := crossProcessDir(t, newStore(t))
		s, err := real.NewObjectStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		for round := range crossProcessRounds {
			key := fmt.Sprintf("image/vol-%d/manifest.json", round)
			res, err := s.Put(ctx, key, bodyOf("epoch-1"), objectstore.PutOptions{})
			if err != nil {
				t.Fatal(err)
			}
			winners := contend(t, dir, key, res.ETag, false)
			if len(winners) != 1 {
				t.Fatalf("round %d: %d of %d separate processes won the same If-Match CAS on %q from one prevETag, want exactly 1 — that is two hosts both believing they published, with no error anywhere",
					round, len(winners), crossProcessWriters, key)
			}
			assertStoredBodyIsTheWinners(t, s, key, winners[0])
		}
	})

	// A POSIX lock lives on an *inode*, so an exclusion keyed off a path an operator can
	// unlink is not an exclusion: the next writer opens the name, gets a new inode and is
	// inside the read-compare-publish with the holder — two CAS winners from one prevETag,
	// no error anywhere. So the contract says it from the outside: while `*.lock` files
	// are being deleted underneath the writers, the CAS still admits exactly one of them.
	t.Run("If-Match admits exactly one winner across processes while *.lock files are swept", func(t *testing.T) {
		dir := crossProcessDir(t, newStore(t))
		s, err := real.NewObjectStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		sweepLockFiles(t, dir)
		for round := range crossProcessRounds {
			key := fmt.Sprintf("image/swept-%d/manifest.json", round)
			res, err := s.Put(ctx, key, bodyOfSize("epoch-1", sweptBody), objectstore.PutOptions{})
			if err != nil {
				t.Fatal(err)
			}
			winners := contendWith(t, dir, key, res.ETag, false, sweptBody)
			if len(winners) != 1 {
				t.Fatalf("round %d: with a `find -name '*.lock' -delete` running, %d of %d separate processes won the same If-Match CAS on %q from one prevETag, want exactly 1 — deleting a lock file is not supposed to let two hosts both publish",
					round, len(winners), crossProcessWriters, key)
			}
			assertStoredBodyIsTheWinnersSized(t, s, key, winners[0], sweptBody)
		}
	})
}

// sweepLockFiles runs the operator's `find -name '*.lock' -delete` continuously until the
// test ends. Errors are ignored — they are its own races with the writers, which `find`
// would ignore too — and it must touch nothing else: a `.tmp` is a writer's in-flight
// body.
func sweepLockFiles(t *testing.T, dir string) {
	t.Helper()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = filepath.WalkDir(dir, func(p string, entry os.DirEntry, err error) error {
				// An error here is this sweep racing the writers — a name that
				// vanished between the walk and the stat. `find` ignores those too.
				if err != nil || entry.IsDir() || !strings.HasSuffix(p, ".lock") {
					return nil
				}
				_ = os.Remove(p)
				return nil
			})
		}
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
	})
}

// The contention shape is the one a readiness run measured a violation with: four
// separate processes and a 4096-byte body. The children are released from a barrier
// rather than merely started together, which is what makes the window reliable —
// against the check-then-act implementation this was written for, round 0 admitted
// 2-4 winners in 8 runs out of 8. The rounds are margin for a regression that races
// less often than that one did, and their cost is process spawns: cheap normally,
// ~250ms each under -race, which is why there are four and not forty.
const (
	crossProcessWriters = 4
	crossProcessRounds  = 4
	crossProcessBody    = 4096
	// sweptBody is a megabyte because the read-compare half of a CAS is a read of the
	// *current* object, so the window a second writer can slip into is as long as that
	// read takes: at 4KB, measured against the sidecar implementation, 2 runs in 20 caught
	// the violation; at a megabyte — the ordinary size of a manifest listing every chunk —
	// the same implementation loses every run.
	sweptBody = 1 << 20
)

// crossProcessDir returns the directory a second process can open the store under
// test from, or skips. Only the filesystem store has one: the sim store's state is
// process memory by construction, and the S3-backed store's exclusion is evaluated
// by the backend, which is already a separate process from every client — the
// in-process cases above are the whole client-side story there.
func crossProcessDir(t *testing.T, s objectstore.Store) string {
	t.Helper()
	rooted, ok := s.(interface{ Root() string })
	if !ok {
		t.Skipf("%T keeps no state a second process can open; cross-process exclusion is not a property it can have", s)
	}
	return rooted.Root()
}

func assertStoredBodyIsTheWinners(t *testing.T, s objectstore.Store, key, winner string) {
	t.Helper()
	assertStoredBodyIsTheWinnersSized(t, s, key, winner, crossProcessBody)
}

func assertStoredBodyIsTheWinnersSized(t *testing.T, s objectstore.Store, key, winner string, size int) {
	t.Helper()
	got, err := s.Get(t.Context(), key)
	if err != nil {
		t.Fatalf("get %q after the contended write: %v", key, err)
	}
	if !bytes.Equal(got, bodyOfSize(winner, size)) {
		t.Fatalf("stored object at %q is not the winner's: got %d bytes starting %q, want %q's body",
			key, len(got), first32(got), winner)
	}
}

// bodyOf makes a writer's body long enough to be worth racing over and still
// self-identifying.
func bodyOf(name string) []byte { return bodyOfSize(name, crossProcessBody) }

func bodyOfSize(name string, size int) []byte {
	b := make([]byte, 0, size)
	for len(b) < size {
		b = append(b, (name + "|")...)
	}
	return b[:size]
}

func first32(b []byte) string {
	if len(b) > 32 {
		return string(b[:32])
	}
	return string(b)
}

// contend runs one round: crossProcessWriters child processes, released together,
// each attempting the same conditional Put. It returns the names of the children
// whose Put returned nil. Every other child must have been told
// ErrPreconditionFailed — a child that failed for any other reason fails the test,
// because "nobody won" would otherwise read as "exactly one winner" minus one.
func contend(t *testing.T, dir, key, ifMatch string, ifNoneMatch bool) []string {
	t.Helper()
	return contendWith(t, dir, key, ifMatch, ifNoneMatch, crossProcessBody)
}

func contendWith(t *testing.T, dir, key, ifMatch string, ifNoneMatch bool, size int) []string {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("locating this test binary to re-exec it: %v", err)
	}

	type child struct {
		name string
		cmd  *exec.Cmd
		in   io.WriteCloser
		out  *bufio.Reader
	}
	children := make([]*child, 0, crossProcessWriters)
	for i := range crossProcessWriters {
		name := fmt.Sprintf("writer-%d", i)
		spec, err := json.Marshal(childSpec{
			Dir: dir, Key: key, Body: name, Size: size, IfMatch: ifMatch, IfNoneMatch: ifNoneMatch,
		})
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(self)
		cmd.Env = append(os.Environ(), childEnvVar+"="+string(spec))
		cmd.Stderr = os.Stderr
		in, err := cmd.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatalf("starting a contending process: %v", err)
		}
		c := &child{name: name, cmd: cmd, in: in, out: bufio.NewReader(out)}
		children = append(children, c)
		t.Cleanup(func() {
			_ = c.in.Close()
			_ = c.cmd.Wait()
		})
	}

	// Every child opens the store and reports ready before any of them is released,
	// so the race window is the conditional write itself and not process startup.
	for _, c := range children {
		line, err := c.out.ReadString('\n')
		if err != nil || strings.TrimSpace(line) != childReady {
			t.Fatalf("%s never became ready (got %q, err=%v)", c.name, line, err)
		}
	}
	for _, c := range children {
		if _, err := io.WriteString(c.in, "go\n"); err != nil {
			t.Fatalf("releasing %s: %v", c.name, err)
		}
	}

	var winners []string
	for _, c := range children {
		line, err := c.out.ReadString('\n')
		if err != nil {
			t.Fatalf("%s produced no verdict: %v", c.name, err)
		}
		verdict := strings.TrimSpace(line)
		waitErr := c.cmd.Wait()
		switch verdict {
		case childWon:
			if waitErr != nil {
				t.Fatalf("%s said it won but exited %v", c.name, waitErr)
			}
			winners = append(winners, c.name)
		case childLost:
		default:
			t.Fatalf("%s neither won nor was fenced: %q (exit %v)", c.name, verdict, waitErr)
		}
	}
	return winners
}

// The child half of contend. storetest is linked into the test binary, so re-execing
// that binary with childEnvVar set is a second process running this code — no helper
// program to build, and no TestMain to demand from the three packages that run this
// contract. init runs before flag parsing and before any test, so the child never
// looks like a test run.
const (
	childEnvVar = "SPIN_STORETEST_CONTENDER"
	childReady  = "READY"
	childWon    = "WON"
	childLost   = "FENCED"
)

type childSpec struct {
	Dir         string
	Key         string
	Body        string
	Size        int
	IfMatch     string
	IfNoneMatch bool
}

func init() {
	spec := os.Getenv(childEnvVar)
	if spec == "" {
		return
	}
	os.Exit(contendAsChild(spec))
}

func contendAsChild(encoded string) int {
	fail := func(format string, args ...any) int {
		fmt.Printf("ERR "+format+"\n", args...)
		return 1
	}
	var spec childSpec
	if err := json.Unmarshal([]byte(encoded), &spec); err != nil {
		return fail("bad spec: %v", err)
	}
	// The filesystem store is the only one whose state a second process can reach
	// without a network; crossProcessDir skips every other implementation.
	s, err := real.NewObjectStore(spec.Dir)
	if err != nil {
		return fail("opening the store: %v", err)
	}
	fmt.Println(childReady)
	if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
		return fail("waiting for the start signal: %v", err)
	}
	_, err = s.Put(context.Background(), spec.Key, bodyOfSize(spec.Body, spec.Size), objectstore.PutOptions{
		IfMatch: spec.IfMatch, IfNoneMatch: spec.IfNoneMatch,
	})
	switch {
	case err == nil:
		fmt.Println(childWon)
		return 0
	case errors.Is(err, objectstore.ErrPreconditionFailed):
		fmt.Println(childLost)
		return 0
	default:
		return fail("put: %v", err)
	}
}
