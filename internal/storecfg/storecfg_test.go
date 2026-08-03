package storecfg_test

import (
	"flag"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/real"
	"github.com/spin-stack/storage/internal/storecfg"
)

// The mutual exclusion is the package's whole reason to exist as shared code, and it is
// operator-facing: the object store is the recovery authority (§5.8), so "which one did
// they mean" is not a thing to be clever about. Both answers have to be refusals, and
// each has to name what to do.
func TestOpenRefusesAnAmbiguousConfiguration(t *testing.T) {
	dir := t.TempDir()

	tests := []struct {
		name  string
		flags storecfg.Flags
		says  string
	}{
		{
			// The Agent shipped in this state once: no store flags at all, so it could
			// heartbeat and never upload. Failing at startup is what makes that visible.
			name:  "neither",
			flags: storecfg.Flags{},
			says:  "required",
		},
		{
			name:  "both",
			flags: storecfg.Flags{Bucket: "b", Dir: dir},
			says:  "mutually exclusive",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, err := tc.flags.Open(t.Context())
			if err == nil {
				t.Fatal("an ambiguous object-store configuration was accepted")
			}
			if store != nil {
				t.Fatal("a refused configuration still returned a store")
			}
			if !strings.Contains(err.Error(), tc.says) {
				t.Fatalf("the refusal must tell an operator which flag to change: %v", err)
			}
		})
	}
}

// The filesystem store is the single-machine deployment, and it must actually be usable
// rather than merely constructed: a store that cannot round-trip an object is a
// deployment that looks configured and loses data.
func TestOpenReturnsAUsableFilesystemStore(t *testing.T) {
	ctx := t.Context()
	f := storecfg.Flags{Dir: t.TempDir()}

	store, err := f.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := store.Put(ctx, "a/b.txt", []byte("payload"), objectstore.PutOptions{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := store.Get(ctx, "a/b.txt")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "payload" {
		t.Fatalf("read back %q, want payload", got)
	}
}

// An unusable directory is reported with the path in it. The alternative is an errno an
// operator cannot map back to a flag they typed.
func TestOpenReportsTheDirectoryItCouldNotUse(t *testing.T) {
	dir := t.TempDir()
	if err := writeFile(t, dir, "file"); err != nil {
		t.Fatal(err)
	}
	notADir := filepath.Join(dir, "file")
	f := storecfg.Flags{Dir: notADir}
	_, err := f.Open(t.Context())
	if err == nil {
		t.Fatal("a file was accepted as an object-store directory")
	}
	if !strings.Contains(err.Error(), notADir) {
		t.Fatalf("the error must name the path: %v", err)
	}
}

// Register is the reason both binaries can be invoked the same way. A rename here is a
// silent break in every deployment script, so the names and the defaults are pinned.
func TestRegisterDeclaresTheDocumentedFlags(t *testing.T) {
	var f storecfg.Flags
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	f.Register(fs)

	for _, name := range []string{
		"s3-bucket", "s3-endpoint", "s3-region",
		"object-store-dir", "s3-create-bucket", "s3-request-timeout",
	} {
		if fs.Lookup(name) == nil {
			t.Errorf("flag -%s is not declared; every deployment naming it breaks silently", name)
		}
	}

	// Off by default, and that is a decision rather than an oversight: silent creation
	// on a typo'd bucket name invents an empty deployment and reports success.
	if f.CreateBucket {
		t.Error("-s3-create-bucket defaults to on; a typo'd bucket would be created rather than refused")
	}
	if f.Timeout != 30*time.Second {
		t.Errorf("-s3-request-timeout defaults to %v, want 30s", f.Timeout)
	}

	if err := fs.Parse([]string{"-s3-bucket", "b", "-s3-create-bucket", "-s3-request-timeout", "5s"}); err != nil {
		t.Fatal(err)
	}
	if f.Bucket != "b" || !f.CreateBucket || f.Timeout != 5*time.Second {
		t.Fatalf("parsed flags did not reach the struct: %+v", f)
	}
}

// Credentials are deliberately not flags (a secret on a command line is in `ps` output
// and shell history). This fails if one is ever added.
func TestCredentialsAreNotFlags(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	(&storecfg.Flags{}).Register(fs)

	fs.VisitAll(func(fl *flag.Flag) {
		for _, bad := range []string{"key", "secret", "credential", "password", "token"} {
			if strings.Contains(strings.ToLower(fl.Name), bad) {
				t.Errorf("-%s looks like a credential flag; they come from the SDK chain on purpose", fl.Name)
			}
		}
	})
}

// writeFile creates a plain file where the test wants a directory to be. It goes through
// internal/simio/real rather than os because INV-01 puts every disk syscall behind the
// simulable interfaces, and a test is not an exemption — the exemptions are named, and
// this package is not one of them.
func writeFile(t *testing.T, dir, name string) error {
	t.Helper()
	d, err := real.NewDisk(dir)
	if err != nil {
		return err
	}
	f, err := d.Create(name)
	if err != nil {
		return err
	}
	if _, err := f.Append([]byte("not a directory")); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
