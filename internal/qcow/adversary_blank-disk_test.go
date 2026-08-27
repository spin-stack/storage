package qcow_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/qcow"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/real"
)

// TestAdversaryTheRealAgentRefusesToStartBecauseNothingWiresTheGuard.
//
// qcow.New refuses a Deps with no Recovery, which is the right refusal — and
// cmd/volume-agent's Deps literal has no Recovery in it. internal/recovery has no caller
// anywhere outside its own test. So the guard is not "wired but untested": the only
// binary that would run it exits before it registers, and every lane that starts the real
// Agent — task demo:stage1/2/3, integration/e2e — is dead at start-up.
//
// It is asserted on what the outside observes: the process is run, with the two flags it
// needs and a stub qemu-img, and what it printed is read. Constructing qcow.Deps here
// would assert nothing about the `main` that is missing the field.
func TestAdversaryTheRealAgentRefusesToStartBecauseNothingWiresTheGuard(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bin := filepath.Join(dir, "volume-agent")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", bin,
		"github.com/spin-stack/storage/cmd/volume-agent")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building the Agent: %v\n%s", err, out)
	}
	// A qemu-img that answers `--version`, which is the one thing New runs before it
	// would reach the loop. It is never reached: the wiring check comes first.
	stub := filepath.Join(dir, "qemu-img")
	if err := real.NewPaths().WriteAtomic(stub, []byte("#!/bin/sh\necho 'qemu-img version 11.0.2'\n")); err != nil {
		t.Fatalf("writing the stub qemu-img: %v", err)
	}
	data := filepath.Join(dir, "data")
	if err := real.NewPaths().MkdirAll(data); err != nil {
		t.Fatalf("making the data directory: %v", err)
	}

	// Bounded, because an Agent that starts correctly never returns: it retries the
	// Control Plane for ever. The observable is what it printed before it was killed.
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	run := exec.CommandContext(ctx, bin,
		"-host-id", vol,
		"-control-plane", "http://127.0.0.1:1",
		"-data-dir", data,
		"-qemu-img", stub,
		"-heartbeat-interval", "100ms")
	out, _ := run.CombinedOutput()
	if strings.Contains(string(out), "a recovery must be injected") {
		t.Errorf("the real Agent exits at start-up and serves no volume at all:\n%s", out)
	}
}

// TestAdversaryNothingProductionCanSatisfyTheRecoveryFilesystem.
//
// The other half of the same gap, and the one that says the wiring was not merely
// forgotten in a `main`. recovery.Files is qcow.Paths plus Create, Rename and Remove;
// real.Paths — the only filesystem this tree builds outside internal/simio — has none of
// the three. A restore cannot be constructed with production types, so no arrangement of
// cmd/volume-agent could have wired one.
//
// The assertion is a runtime type check rather than `var _ recovery.Files = real.NewPaths()`
// on purpose: that spelling is a compile error, which takes the package's whole test
// binary down instead of reporting one gap.
func TestAdversaryNothingProductionCanSatisfyTheRecoveryFilesystem(t *testing.T) {
	t.Parallel()
	var paths any = real.NewPaths()
	if _, ok := paths.(recovery.Files); !ok {
		t.Errorf("real.Paths (%T) is not a recovery.Files: this tree has no filesystem a restore can be built on", paths)
	}
	// The two collaborators that *are* there, so the failure above is read as the one
	// missing piece rather than as "recovery takes production types nothing implements".
	var run any = real.NewRunner()
	if _, ok := run.(recovery.Runner); !ok {
		t.Errorf("real.Runner (%T) is not a recovery.Runner", run)
	}
}

// TestAdversaryAHostThatKnowsItHoldsCommitsStillCreatesABlankDisk.
//
// The guard asks the bucket and believes whatever it says, including "this volume has
// never published". But this host keeps its own durable record of the commits whose
// layers it holds — state.json, read by ensure two statements after Open returns — and
// that record can contradict the bucket. When it does, `born` creates an empty qcow2 and
// points the guest at it.
//
// Two ways to get there, neither of them exotic:
//
//   - the Agent is restarted with `-store-bucket`/`-store-dir` naming a different or
//     empty store (a typo, a staging value, a bucket restored without its HEADs). Every
//     volume on the host then reads as new, and every guest boots blank over a chain that
//     is on the disk under it;
//   - the first Open crashed between the restore, which writes the layers and the record,
//     and writePointer, which is the last statement of `born`. The next cycle has no
//     pointer, so `born` runs again — and if the bucket's answer has changed in the
//     meantime, it takes the new one.
//
// The check the code does not make is free: the volume's own state.json is on this disk,
// it names published commits, and a volume that has published cannot be a volume that
// has never published. ErrNoHead is a claim about the bucket, and this host is entitled
// to refuse it.
func TestAdversaryAHostThatKnowsItHoldsCommitsStillCreatesABlankDisk(t *testing.T) {
	t.Parallel()
	// No pointer, but the layers of two published commits and the record that vouches
	// for them: exactly what a restore leaves behind, and what a host that published
	// them itself holds.
	held := qcow.LayerImage(root, vol, baseID)
	p := newPaths(held)
	if err := qcow.WriteState(p, root, vol, qcow.State{Commits: []qcow.CommitLayer{
		{CommitID: headCommit, LayerID: baseID},
	}}); err != nil {
		t.Fatalf("recording what this host holds: %v", err)
	}

	// The bucket says the volume is new. It is the one answer that is not a refusal.
	r := &fakeRunner{info: infoJSON("qcow2", size, false)}
	chain, err := qcow.Open(t.Context(), r, p, "/qemu-img", req(nil))

	st, stErr := qcow.ReadState(p, root, vol)
	if stErr != nil {
		t.Fatalf("reading the state back: %v", stErr)
	}
	if err == nil && len(st.Commits) > 0 {
		fresh := qcow.LayerImage(root, vol, layerID)
		t.Errorf("this host records holding commit %s in layer %s and was handed a blank tip %q anyway (qemu-img: %v);"+
			" a guest launched now reads zeros over a chain that is on this disk",
			st.Commits[0].CommitID, st.Commits[0].LayerID, chain.Active, r.commands())
		if chain.Active != fresh {
			t.Logf("the tip is %q rather than the freshly created %q", chain.Active, fresh)
		}
	}
}
