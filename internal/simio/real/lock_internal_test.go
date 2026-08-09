package real

// Internal, because what it drives is the exclusion itself: taking the lock the way a
// publisher takes it and asking whether it still excludes anybody after the directory
// moved. Going through Put could only reach it by racing a rename against a write,
// which would be a flaky test of a deterministic property.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// The two guards that make a replaced lock loud instead of silent, and the sidecar
// refusal beside them. All three are the difference between "somebody swept the store
// and we noticed" and "two hosts both published", so none of them may be reached only
// by the happy path.
//
// This is the hazard the directory lock was chosen for: a POSIX lock lives on the
// inode, so anything that replaces the path replaces the exclusion. A held lock whose
// directory has been swapped underneath it excludes nobody, and a writer that published
// under one would be the second winner INV-10 exists to prevent.
func TestALockedDirectoryReplacedUnderneathIsRefusedRatherThanPublished(t *testing.T) {
	tests := []struct {
		name string
		// swap does to the directory what an operator's cleanup, a restore or an rsync
		// would: makes the path name a different inode, or no inode at all.
		swap func(t *testing.T, dir string)
		want string
	}{
		{
			name: "replaced by another directory",
			swap: func(t *testing.T, dir string) {
				t.Helper()
				if err := os.Rename(dir, dir+".old"); err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(dir, 0o755); err != nil {
					t.Fatal(err)
				}
			},
			want: "was replaced while it was locked",
		},
		{
			name: "removed outright",
			swap: func(t *testing.T, dir string) {
				t.Helper()
				if err := os.RemoveAll(dir); err != nil {
					t.Fatal(err)
				}
			},
			want: "went away mid-write",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewObjectStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			ctx := t.Context()
			const key = "image/vol-swap/manifest.json"
			if _, err := s.Put(ctx, key, []byte("first"), objectstore.PutOptions{IfNoneMatch: true}); err != nil {
				t.Fatal(err)
			}

			// Take the lock the way a publisher does, then swap the directory before the
			// publish. stillExcludes runs immediately before anything lands, so the write
			// must be refused with the object left as it was.
			dir := filepath.Dir(s.path(key))
			lock, err := s.lockKeysIn(dir)
			if err != nil {
				t.Fatal(err)
			}
			tc.swap(t, dir)
			err = lock.stillExcludes()
			lock.release()

			if err == nil {
				t.Fatalf("a lock on a directory that was %s reported that it still excludes somebody", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not say %q, which is what tells the operator what happened", err, tc.want)
			}
		})
	}
}
