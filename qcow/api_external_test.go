// This file is deliberately in package qcow_test and not in package qcow.
//
// What it asserts is not a behaviour of Open, it is that Open can be *reached* from
// another module: the Recovery it insists on is implemented here out of nothing but the
// package's exported surface — its interfaces, its types and its sentinels — the way
// spinbox has to implement it (ADR-0021 §4). An in-package test proves nothing about
// that, because it can reach internal/commit and every other thing a consumer cannot.
//
// It went red for the reason it exists: the only spelling of "this volume has never
// published" was internal/commit.ErrNoHead, so nothing outside this module could say the
// one condition Recovery is built around, and every volume handed to Open was refused.
// Delete qcow.ErrNoHistory and this file stops compiling.
package qcow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/spin-stack/storage/qcow"
)

// memPaths is a Paths a consumer could write: a map, and not one syscall. It is what the
// interface being implementable from outside means in practice, so it uses no helper of
// this repository's either.
type memPaths struct {
	files map[string][]byte
	dirs  map[string]bool
}

func newMemPaths() *memPaths {
	return &memPaths{files: map[string][]byte{}, dirs: map[string]bool{}}
}

func (m *memPaths) MkdirAll(dir string) error {
	for d := filepath.Clean(dir); d != "/" && d != "."; d = filepath.Dir(d) {
		m.dirs[d] = true
	}
	return nil
}

func (m *memPaths) Exists(p string) (bool, error) {
	_, ok := m.files[p]
	return ok, nil
}

func (m *memPaths) Size(p string) (int64, error) {
	body, ok := m.files[p]
	if !ok {
		return 0, fmt.Errorf("size %s: %w", p, fs.ErrNotExist)
	}
	return int64(len(body)), nil
}

func (m *memPaths) ReadFile(p string) ([]byte, error) {
	body, ok := m.files[p]
	if !ok {
		return nil, fmt.Errorf("read %s: %w", p, fs.ErrNotExist)
	}
	return body, nil
}

func (m *memPaths) WriteAtomic(p string, data []byte) error {
	if err := m.MkdirAll(filepath.Dir(p)); err != nil {
		return err
	}
	m.files[p] = data
	return nil
}

// List answers fs.ErrNotExist for a directory that is not there, which is the contract
// the interface states and the one a sweep depends on.
func (m *memPaths) List(dir string) ([]string, error) {
	dir = filepath.Clean(dir)
	if !m.dirs[dir] {
		return nil, fmt.Errorf("list %s: %w", dir, fs.ErrNotExist)
	}
	seen := map[string]bool{}
	for p := range m.files {
		if filepath.Dir(p) == dir {
			seen[path.Base(p)] = true
		}
	}
	for d := range m.dirs {
		if filepath.Dir(d) == dir {
			seen[path.Base(d)] = true
		}
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	return names, nil
}

func (m *memPaths) Rename(oldPath, newPath string) error {
	body, ok := m.files[oldPath]
	if !ok {
		return fmt.Errorf("rename %s: %w", oldPath, fs.ErrNotExist)
	}
	delete(m.files, oldPath)
	m.files[newPath] = body
	return nil
}

func (m *memPaths) Remove(p string) error {
	delete(m.files, p)
	return nil
}

// memRunner stands in for qemu-img: `create` puts a file where the package says the layer
// goes, `info` answers about the files that are there. A consumer outside this module
// gets exactly this interface — a process to run — and nothing about how it is run.
type memRunner struct {
	paths   *memPaths
	size    int64
	created []string
}

func (r *memRunner) Run(_ context.Context, _ string, args ...string) ([]byte, error) {
	switch args[0] {
	case "create":
		image := args[len(args)-2]
		r.created = append(r.created, image)
		return nil, r.paths.WriteAtomic(image, []byte("qcow2"))
	case "info":
		image := args[len(args)-1]
		if ok, _ := r.paths.Exists(image); !ok {
			return nil, fmt.Errorf("qemu-img: %s: %w", image, fs.ErrNotExist)
		}
		info := []map[string]any{{
			"format":       "qcow2",
			"virtual-size": r.size,
			"filename":     image,
		}}
		if !strings.Contains(strings.Join(args, " "), "--backing-chain") {
			return json.Marshal(info[0])
		}
		return json.Marshal(info)
	}
	return nil, fmt.Errorf("qemu-img: unexpected %v", args)
}

// extRecovery is the whole point: a Recovery written with nothing but the exported
// surface, saying "this volume has never published" the only way a consumer can.
type extRecovery struct {
	// head is the commit the object store holds, empty when it holds none — which is
	// what ErrNoHistory says, and what an outside implementation could not say at all.
	head     string
	restored qcow.Restored
	// storeDown is an error that is not about history: a store that would not answer,
	// which must never be read as "born empty".
	storeDown error
}

func (e extRecovery) RestoreFrom(context.Context, qcow.Lineage, int64) (qcow.Restored, error) {
	switch {
	case e.storeDown != nil:
		return qcow.Restored{}, fmt.Errorf("rebuilding this volume: %w", e.storeDown)
	case e.head == "":
		return qcow.Restored{}, fmt.Errorf("volume has no HEAD object: %w", qcow.ErrNoHistory)
	}
	return e.restored, nil
}

func (e extRecovery) Current(context.Context, string) (string, error) {
	switch {
	case e.storeDown != nil:
		return "", fmt.Errorf("reading HEAD: %w", e.storeDown)
	case e.head == "":
		return "", fmt.Errorf("volume has no HEAD object: %w", qcow.ErrNoHistory)
	}
	return e.head, nil
}

const (
	extRoot   = "/data"
	extVolume = "0199bd2f-0000-7000-8000-00000000c0de"
	extLayer  = "0199bd2f-0001-7000-8000-00000000face"
	extSize   = int64(64 << 20)
)

func extOpen(t *testing.T, p *memPaths, r *memRunner, req qcow.OpenRequest) (*qcow.Chain, error) {
	t.Helper()
	req.Root, req.SizeBytes, req.NewLayerID = extRoot, extSize, extLayer
	req.VolumeID = extVolume
	return qcow.Open(t.Context(), r, p, "qemu-img", req)
}

// TestExternalRecoveryBornEmpty is the path a consumer's very first volume takes: nothing
// has ever published it, its Recovery says so with qcow.ErrNoHistory, and Open creates
// the first layer instead of refusing.
func TestExternalRecoveryBornEmpty(t *testing.T) {
	p := newMemPaths()
	r := &memRunner{paths: p, size: extSize}

	chain, err := extOpen(t, p, r, qcow.OpenRequest{Recovery: extRecovery{}})
	if err != nil {
		t.Fatalf("a volume nothing has published must be born empty, and this consumer's Recovery said so: %v", err)
	}
	image := qcow.LayerImage(extRoot, extLayer)
	if chain.Active != image {
		t.Fatalf("active layer = %q, want %q", chain.Active, image)
	}
	if len(r.created) != 1 || r.created[0] != image {
		t.Fatalf("qemu-img create calls = %v, want exactly [%s]", r.created, image)
	}
	// What the launcher reads, and the only half of the contract that leaves this
	// process: the pointer must name the layer QEMU is to be started against.
	pointer, err := p.ReadFile(qcow.ActivePointer(extRoot, extVolume))
	if err != nil {
		t.Fatalf("reading the active pointer: %v", err)
	}
	if string(pointer) != image {
		t.Fatalf("active pointer = %q, want %q", pointer, image)
	}
}

// TestExternalRecoveryKeepsLocalChain is Current's half of the same sentence: a chain
// already on this disk is served when the store holds no history, so a consumer that
// cannot say ErrNoHistory loses its volume on the second open as well as the first.
func TestExternalRecoveryKeepsLocalChain(t *testing.T) {
	p := newMemPaths()
	r := &memRunner{paths: p, size: extSize}
	if _, err := extOpen(t, p, r, qcow.OpenRequest{Recovery: extRecovery{}}); err != nil {
		t.Fatalf("first open: %v", err)
	}

	chain, err := extOpen(t, p, r, qcow.OpenRequest{Recovery: extRecovery{}})
	if err != nil {
		t.Fatalf("reopening a chain the store has not moved past: %v", err)
	}
	if want := qcow.LayerImage(extRoot, extLayer); chain.Active != want {
		t.Fatalf("active layer = %q, want the layer already on disk %q", chain.Active, want)
	}
	if len(r.created) != 1 {
		t.Fatalf("qemu-img create calls = %v, want the first open's one and no more", r.created)
	}
}

// TestExternalRecoveryRefusals is the other side, and the reason ErrNoHistory has to be a
// sentinel and not "any error": every one of these is a volume Open must refuse rather
// than create empty, and each is expressible from outside this module too.
func TestExternalRecoveryRefusals(t *testing.T) {
	storeDown := errors.New("the object store did not answer")

	tests := []struct {
		name string
		req  qcow.OpenRequest
		// local seeds a chain on this host before the open under test.
		local bool
		want  error
	}{{
		// §14's recovery path: a host that has never seen the volume, a bucket whose
		// HEAD is gone. "No history" is true of the bucket and false of the volume, and
		// creating it empty here is the blank-disk defect.
		name: "the catalog says this volume published and the store has no HEAD",
		req:  qcow.OpenRequest{Recovery: extRecovery{}, HeadCommitID: "0199bd2f-0002-7000-8000-0000000000c1"},
		want: qcow.ErrChainMissing,
	}, {
		name: "the store could not be asked at all",
		req:  qcow.OpenRequest{Recovery: extRecovery{storeDown: storeDown}},
		want: qcow.ErrChainMissing,
	}, {
		// Two hosts have held this volume. The local chain is a fork of a history that
		// moved on, and which of the two survives is not this process's to decide.
		name:  "the published history moved past the chain on this disk",
		local: true,
		req:   qcow.OpenRequest{Recovery: extRecovery{head: "0199bd2f-0003-7000-8000-0000000000c2"}},
		want:  qcow.ErrStaleChain,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := newMemPaths()
			r := &memRunner{paths: p, size: extSize}
			if tc.local {
				if _, err := extOpen(t, p, r, qcow.OpenRequest{Recovery: extRecovery{}}); err != nil {
					t.Fatalf("seeding a local chain: %v", err)
				}
			}
			before := len(r.created)

			chain, err := extOpen(t, p, r, tc.req)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Open = (%v, %v), want an error wrapping %v", chain, err, tc.want)
			}
			if len(r.created) != before {
				t.Fatalf("a refused volume had layers created for it: %v", r.created[before:])
			}
		})
	}
}
