package real_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/real"
)

// The If-Match CAS on a volume's manifest is the only fencing V1 has, and on a
// single-machine deployment (`-object-store-dir`) the writers it must exclude are two
// Agent *processes* on one filesystem. The exclusion is a per-key flock; these two
// tests are its halves — that it excludes another process at all, and that a holder
// which dies does not leave the key locked forever, which is the reason it is a flock
// and not an O_CREAT|O_EXCL lock file.
//
// The conformance suite (storetest) proves the property from the outside, by racing
// four processes. This proves the mechanism, including the case a race cannot reach:
// a writer that is SIGKILLed between its ETag comparison and its rename.

const holdLockEnv = "SPIN_REAL_HOLD_LOCK"

func TestAProcessHoldingTheKeyLockExcludesAnotherAndDyingUnwedgesIt(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	s, err := real.NewObjectStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	const key = "image/vol-1/manifest.json"
	first, err := s.Put(ctx, key, []byte("manifest-1"), objectstore.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Putting the object created the key's lock file; the child takes that same lock
	// the way a second Agent's Put would.
	lockPath := filepath.Join(dir, filepath.FromSlash(key)) + ".lock"
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("a conditional write must leave a per-key lock to take: %v", err)
	}

	holder := exec.Command(os.Args[0], "-test.run=TestHelperHoldsTheLock", "-test.timeout=0")
	holder.Env = append(os.Environ(), holdLockEnv+"="+lockPath)
	holder.Stderr = os.Stderr
	out, err := holder.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = holder.Process.Kill()
		_ = holder.Wait()
	}()
	held := make([]byte, len("HELD\n"))
	if _, err := out.Read(held); err != nil || !strings.HasPrefix(string(held), "HELD") {
		t.Fatalf("the holder never took the lock: %q err=%v", held, err)
	}

	// While another process holds the key, this one's CAS must not get past its ETag
	// comparison. A check-then-act implementation sails through here.
	done := make(chan error, 1)
	go func() {
		_, err := s.Put(ctx, key, []byte("manifest-2"), objectstore.PutOptions{IfMatch: first.ETag})
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("a CAS completed (err=%v) while another process held the key: the lock excluded nothing, so two hosts can both publish", err)
	case <-time.After(500 * time.Millisecond):
	}

	// The holder dies mid-publish, the worst moment there is. The kernel drops the
	// flock with the fd, so the waiting writer proceeds — no lease to expire, no
	// stale-lock timeout, nothing for an operator to clear.
	if err := holder.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = holder.Wait()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the CAS after the holder died: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the key stayed locked after its holder was killed: a dead writer wedged it, and nothing an operator can see says so")
	}
	got, err := s.Get(ctx, key)
	if err != nil || string(got) != "manifest-2" {
		t.Fatalf("stored object = %q err=%v, want the surviving writer's manifest-2", got, err)
	}
}

// TestHelperHoldsTheLock is the other process: it takes the flock on the lock file it
// is pointed at, says so, and then waits to be killed. It is a no-op in an ordinary
// run.
func TestHelperHoldsTheLock(t *testing.T) {
	path := os.Getenv(holdLockEnv)
	if path == "" {
		t.Skip("helper process only")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	fmt.Println("HELD")
	select {} // held until the parent kills this process
}

// A key whose name would collide with the lock sidecar is refused, like every other
// sidecar: an object called `x.lock` and the lock protecting `x` are the same path,
// and the store would then serialise writes to one on the bytes of the other.
func TestALockFileIsNotAnObject(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	s, err := real.NewObjectStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "image/vol-1/manifest.json", []byte("m"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, "image/vol-1/manifest.json.lock", []byte("x"), objectstore.PutOptions{}); err == nil {
		t.Fatal("a key ending in the lock suffix must be refused")
	}
	objs, err := s.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 1 || objs[0].Key != "image/vol-1/manifest.json" {
		t.Fatalf("the lock sidecar surfaced as an object: %+v", objs)
	}
	if _, err := s.Get(ctx, "image/vol-1/manifest.json.lock"); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("get of a lock sidecar: %v, want ErrNotFound", err)
	}
}
