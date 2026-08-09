package real_test

import (
	"errors"
	"fmt"
	"io/fs"
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
// Agent *processes* on one filesystem. The exclusion is an flock; these tests are its
// three halves — that it excludes another process at all, that a holder which dies
// does not leave the key locked forever (which is why it is an flock and not an
// O_CREAT|O_EXCL lock file), and that an operator deleting lock files does not
// dissolve it.
//
// The conformance suite (storetest) proves the property from the outside, by racing
// four processes. These prove the mechanism, including the two cases a race cannot
// reach: a writer SIGKILLed between its ETag comparison and its rename, and a second
// writer that arrives *after* a sweep and before the first writer publishes.

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

	// Nothing this store writes may be named for a lock. An exclusion that lives at a
	// path is only an exclusion until somebody deletes the path — and `*.lock` is the
	// one name an operator, a tidy-up script or a backup restore singles out.
	if found := lockFilesUnder(t, dir); len(found) != 0 {
		t.Fatalf("the store left lock files a stale-lock sweep would delete: %v", found)
	}

	// The holder takes every lock this store could be taking for the key: the
	// directory the key lives in, and the `<key>.lock` sidecar this used to be. Which
	// one is load-bearing is the implementation's business; that the CAS blocks while
	// somebody holds it is not.
	keyDir := filepath.Join(dir, "image", "vol-1")
	lockPath := filepath.Join(dir, filepath.FromSlash(key)) + ".lock"
	holder := holdLocks(t, keyDir, lockPath)

	// While another process holds the key, this one's CAS must not get past its ETag
	// comparison. A check-then-act implementation sails through here.
	casA := make(chan error, 1)
	go func() {
		_, err := s.Put(ctx, key, []byte("manifest-A"), objectstore.PutOptions{IfMatch: first.ETag})
		casA <- err
	}()
	select {
	case err := <-casA:
		t.Fatalf("a CAS completed (err=%v) while another process held the key: the lock excluded nothing, so two hosts can both publish", err)
	case <-time.After(500 * time.Millisecond):
	}

	// The operator now does the thing that reopened this: sweeps stale lock files.
	// `find -name '*.lock' -delete`, an rsync that skips sidecars, a restore from a
	// backup that never had them — all the same unlink. A lock lives on an inode, so
	// if the exclusion is a path the sweep can remove, the *next* writer creates a
	// fresh inode, locks that, and is inside the read-compare-publish alongside the
	// holder: two winners from one prevETag, both told they published, nothing logged
	// anywhere. Note that this second writer has to arrive after the sweep — the one
	// already blocked in flock() is waiting on the inode it opened and never notices.
	swept := sweepLockFiles(t, dir)
	casB := make(chan error, 1)
	go func() {
		_, err := s.Put(ctx, key, []byte("manifest-B"), objectstore.PutOptions{IfMatch: first.ETag})
		casB <- err
	}()
	select {
	case err := <-casB:
		t.Fatalf("a CAS completed (err=%v) after %d lock file(s) were swept, while another process still held the key: an operator deleting a file dissolved the only fencing there is, and two hosts would both believe they published", err, swept)
	case <-time.After(500 * time.Millisecond):
	}

	// The holder dies mid-publish, the worst moment there is. The kernel drops the
	// flock with the fd, so the waiting writers proceed — no lease to expire, no
	// stale-lock timeout, nothing for an operator to clear.
	holder.kill()

	// Exactly one of the two waiting writers may publish: they hold the same
	// prevETag, so whichever renames first invalidates the other.
	results := make([]error, 0, 2)
	for _, ch := range []chan error{casA, casB} {
		select {
		case err := <-ch:
			results = append(results, err)
		case <-time.After(30 * time.Second):
			t.Fatal("the key stayed locked after its holder was killed: a dead writer wedged it, and nothing an operator can see says so")
		}
	}
	var won int
	for _, err := range results {
		switch {
		case err == nil:
			won++
		case errors.Is(err, objectstore.ErrPreconditionFailed):
		default:
			t.Fatalf("a CAS failed for a reason that is neither winning nor being fenced: %v", err)
		}
	}
	if won != 1 {
		t.Fatalf("%d of 2 CAS operations won on one prevETag (%v), want exactly 1", won, results)
	}
	got, err := s.Get(ctx, key)
	if err != nil || (string(got) != "manifest-A" && string(got) != "manifest-B") {
		t.Fatalf("stored object = %q err=%v, want one of the two writers' manifests", got, err)
	}
}

// Moving the exclusion onto the directory does not make it a thing no action can
// replace — it makes the action one nobody performs by accident, because it takes the
// objects with it. That case still has to be answered: a writer that wakes up holding
// a lock on a directory the path no longer names is excluding nobody, and if it
// publishes anyway the next writer is inside the critical section with it. It must
// take the lock on whatever replaced the directory instead, and this is the only
// place that can tell the difference — a second holder on the new directory.
func TestAWriterWhoseDirectoryWasReplacedTakesTheNewLockAndPublishesNothingUnderTheOldOne(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	s, err := real.NewObjectStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	const key = "image/vol-1/manifest.json"
	if _, err := s.Put(ctx, key, []byte("manifest-1"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	keyDir := filepath.Join(dir, "image", "vol-1")

	first := holdLocks(t, keyDir)
	put := make(chan error, 1)
	go func() {
		_, err := s.Put(ctx, key, []byte("manifest-2"), objectstore.PutOptions{})
		put <- err
	}()
	select {
	case err := <-put:
		t.Fatalf("a write completed (err=%v) while another process held the directory", err)
	case <-time.After(500 * time.Millisecond):
	}

	// The directory the waiting writer is queued on is replaced wholesale — the
	// shape a restore-from-backup or a `mv` aside leaves — and somebody else is
	// already publishing under the new one.
	if err := os.Rename(keyDir, keyDir+".orphaned"); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(keyDir, 0o755); err != nil {
		t.Fatal(err)
	}
	second := holdLocks(t, keyDir)

	// The first holder dies. The waiting writer now owns a lock on the orphaned
	// directory, which excludes nobody: if it publishes on the strength of it, it and
	// the second holder are both inside the critical section for this key.
	first.kill()
	select {
	case err := <-put:
		t.Fatalf("the write stopped waiting (err=%v) while another process held the live directory: it had been queued on a directory that was replaced, and a hold on that one excludes nobody — it has to take the lock on the directory the key now resolves through, not act on the one it woke up with", err)
	case <-time.After(500 * time.Millisecond):
	}

	// Once the live directory is free it proceeds — and fails, loudly, because the
	// body it staged went with the directory that was moved aside. Loud and nothing
	// published is the whole requirement here; the caller retries a write it was
	// told did not happen.
	second.kill()
	select {
	case err := <-put:
		if err == nil {
			t.Fatal("the write reported success after its directory was replaced under it")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the writer never got the replacement directory's lock: it is wedged on an inode nobody can reach")
	}
	if _, err := s.Get(ctx, key); !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("get after the failed write: %v, want ErrNotFound — a write that returned an error must not have published", err)
	}
}

// holdLocks starts a process that takes an exclusive flock on each path and keeps it
// until it is killed.
func holdLocks(t *testing.T, paths ...string) *lockHolder {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestHelperHoldsTheLock", "-test.timeout=0")
	cmd.Env = append(os.Environ(), holdLockEnv+"="+strings.Join(paths, string(os.PathListSeparator)))
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	h := &lockHolder{t: t, cmd: cmd}
	t.Cleanup(h.kill)
	held := make([]byte, len("HELD\n"))
	if _, err := out.Read(held); err != nil || !strings.HasPrefix(string(held), "HELD") {
		t.Fatalf("the holder never took the lock on %v: %q err=%v", paths, held, err)
	}
	return h
}

type lockHolder struct {
	t    *testing.T
	cmd  *exec.Cmd
	dead bool
}

func (h *lockHolder) kill() {
	if h.dead {
		return
	}
	h.dead = true
	_ = h.cmd.Process.Kill()
	_ = h.cmd.Wait()
}

// sweepLockFiles is the operator's `find -name '*.lock' -delete`, run once. It returns
// how many it deleted, which is 0 against a store that keeps its exclusion somewhere a
// sweep cannot reach — the holder's own sidecar is what it finds here.
func sweepLockFiles(t *testing.T, dir string) int {
	t.Helper()
	var n int
	for _, p := range lockFilesUnder(t, dir) {
		if err := os.Remove(p); err != nil {
			t.Fatalf("sweeping %q: %v", p, err)
		}
		n++
	}
	return n
}

func lockFilesUnder(t *testing.T, dir string) []string {
	t.Helper()
	var found []string
	err := filepath.WalkDir(dir, func(p string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(p, ".lock") {
			found = append(found, p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

// TestHelperHoldsTheLock is the other process: it takes an exclusive flock on every
// path it is pointed at — creating the file ones, opening the directory ones — says
// so, and then waits to be killed. It is a no-op in an ordinary run.
func TestHelperHoldsTheLock(t *testing.T) {
	paths := os.Getenv(holdLockEnv)
	if paths == "" {
		t.Skip("helper process only")
	}
	for _, p := range strings.Split(paths, string(os.PathListSeparator)) {
		var (
			f   *os.File
			err error
		)
		if info, serr := os.Stat(p); serr == nil && info.IsDir() {
			f, err = os.Open(p)
		} else {
			f, err = os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o644)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
			t.Fatal(err)
		}
	}
	fmt.Println("HELD")
	select {} // held until the parent kills this process
}

// A key whose name ends in the lock suffix is refused, like every other sidecar. This
// store no longer writes a `<key>.lock` — the exclusion moved off a path an operator
// can unlink — but that is exactly why the name stays reserved: `*.lock` is what a
// stale-lock sweep deletes, and an object stored under one would be swept away with no
// error and no way back.
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

// A sidecar is store bookkeeping, not an object. Every entry point refuses one as a key,
// so a caller cannot read, write or delete the exclusion or the delete markers through
// the same API it uses for data — which is the other half of not letting a routine
// operation un-fence the store.
func TestTheStoreRefusesItsOwnSidecarsAsKeys(t *testing.T) {
	s, err := real.NewObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	for _, key := range []string{
		"image/vol-a/manifest.json.deleted",
		"image/vol-a/manifest.json.superseded",
		"image/vol-a/manifest.json.tmp",
		"image/vol-a/.lock",
	} {
		t.Run(key, func(t *testing.T) {
			if _, err := s.Put(ctx, key, []byte("x"), objectstore.PutOptions{}); err == nil {
				t.Errorf("Put accepted the sidecar %q as an object key", key)
			}
			// A read must not invent an object either: before sidecars were hidden from
			// Get, a lock file answered with its own zero bytes and a nil error — a
			// phantom empty object where the store refuses to create one.
			if _, err := s.Get(ctx, key); err == nil {
				t.Errorf("Get answered for the sidecar %q instead of refusing it", key)
			}
		})
	}
}
