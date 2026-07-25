package real_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/real"
)

// Finding 10. The filesystem store wrote objects with a bare os.WriteFile — which
// opens with O_TRUNC and then writes — and cleared the delete marker *before*
// writing. Two consequences, both silent:
//
//   - a reader concurrent with a rewrite can observe a truncated or half-written
//     object. That object is a WAL object: recovery's integrity check reads a short
//     body, declares the durable prefix ends there, and the writes past it are gone.
//     A crash mid-write leaves the same thing permanently, at a create-only key that
//     can never be repaired (the retry gets ErrPreconditionFailed);
//   - an object an operator retired can come back and be served, because the marker
//     is gone before the new bytes exist. ENOSPC, EIO or a crash in that window
//     resurrects the old content.
//
// Neither is reachable through the contract's sequential assertions, so both need a
// concurrent observer.

func newFSStore(t *testing.T) (*real.ObjectStore, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := real.NewObjectStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return s, dir
}

// TestPutIsNeverPartiallyVisible: whatever a reader gets, it is a complete object
// somebody wrote — never a prefix of one, never zero bytes of a non-empty one.
func TestPutIsNeverPartiallyVisible(t *testing.T) {
	ctx := context.Background()
	s, _ := newFSStore(t)
	const key = "wal/v/1/1-1-a.wal"

	bodies := [][]byte{
		bytes.Repeat([]byte("A"), 512*1024),
		bytes.Repeat([]byte("B"), 3),
		bytes.Repeat([]byte("C"), 128*1024),
	}
	if _, err := s.Put(ctx, key, bodies[0], objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	var (
		wg   sync.WaitGroup
		stop = make(chan struct{})
		bad  = make(chan string, 1)
	)
	wg.Add(1)
	go func() { // the reader: an Agent replaying, a materializer fetching
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			got, err := s.Get(ctx, key)
			if err != nil {
				select {
				case bad <- "a rewrite made the object unreadable: " + err.Error():
				default:
				}
				return
			}
			complete := false
			for _, b := range bodies {
				if bytes.Equal(got, b) {
					complete = true
					break
				}
			}
			if !complete {
				select {
				case bad <- "read a torn object: " + describe(got):
				default:
				}
				return
			}
		}
	}()

	for range 200 {
		for _, b := range bodies {
			if _, err := s.Put(ctx, key, b, objectstore.PutOptions{}); err != nil {
				t.Errorf("put: %v", err)
			}
		}
	}
	close(stop)
	wg.Wait()
	select {
	case msg := <-bad:
		t.Fatal(msg)
	default:
	}
}

// TestARewriteOverAMarkedObjectNeverResurrectsTheOldBytes: while a marked key is
// being written again, a reader may see nothing or the new object. The bytes the
// operator retired must never be served.
func TestARewriteOverAMarkedObjectNeverResurrectsTheOldBytes(t *testing.T) {
	ctx := context.Background()
	const key = "wal/v/1/2-2-b.wal"
	retired := bytes.Repeat([]byte("retired-by-the-operator"), 4096)
	replacement := bytes.Repeat([]byte("the-new-object"), 4096)

	for range 200 {
		s, _ := newFSStore(t)
		if _, err := s.Put(ctx, key, retired, objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}

		var (
			wg          sync.WaitGroup
			resurrected bool
			mu          sync.Mutex
			done        = make(chan struct{})
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				got, err := s.Get(ctx, key)
				if err == nil && bytes.Equal(got, retired) {
					mu.Lock()
					resurrected = true
					mu.Unlock()
					return
				}
				select {
				case <-done:
					return
				default:
				}
			}
		}()

		if _, err := s.Put(ctx, key, replacement, objectstore.PutOptions{IfNoneMatch: true}); err != nil {
			t.Fatalf("create-only over a marked key: %v", err)
		}
		close(done)
		wg.Wait()
		mu.Lock()
		bad := resurrected
		mu.Unlock()
		if bad {
			t.Fatal("a reader saw the retired object again while it was being rewritten")
		}
	}
}

// TestStoreInternalsAreNotObjects: whatever bookkeeping the store keeps next to an
// object (delete markers, temp files, locks) must never surface as a key. A stray
// key is an orphan to the GC and a corrupt WAL object to recovery.
func TestStoreInternalsAreNotObjects(t *testing.T) {
	ctx := context.Background()
	s, dir := newFSStore(t)
	keys := []string{"wal/v/1/1-1-a.wal", "wal/v/1/2-2-b.wal", "volumes/v/descriptor.json"}
	for _, k := range keys {
		if _, err := s.Put(ctx, k, []byte(k), objectstore.PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	// A full mark/rewrite/restore cycle, which is where sidecars appear.
	if err := s.Delete(ctx, keys[0]); err != nil {
		t.Fatal(err)
	}
	if err := s.Restore(ctx, keys[0]); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, keys[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, keys[1], []byte("rewritten"), objectstore.PutOptions{IfNoneMatch: true}); err != nil {
		t.Fatal(err)
	}

	got, err := s.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(keys) {
		t.Fatalf("List returned %d objects, want %d: %+v", len(got), len(keys), got)
	}
	for _, info := range got {
		if strings.Contains(info.Key, ".tmp") || strings.Contains(info.Key, ".lock") || strings.Contains(info.Key, ".deleted") {
			t.Fatalf("store bookkeeping surfaced as an object: %q", info.Key)
		}
	}
	// And nothing half-written was left behind on disk.
	err = filepath.WalkDir(dir, func(p string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !e.IsDir() && strings.Contains(e.Name(), ".tmp") {
			return errors.New("leftover temp file: " + p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// describe renders what a torn read actually looked like, which is the whole point
// of the failure message.
func describe(b []byte) string {
	if len(b) > 32 {
		return fmt.Sprintf("%q... (%d bytes)", b[:32], len(b))
	}
	return fmt.Sprintf("%q (%d bytes)", b, len(b))
}
