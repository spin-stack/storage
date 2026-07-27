package controlplane_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// Nothing in this tree creates a volume. Four RPCs exist and every one of them reads;
// `crypto.GenerateDEK`, `WrapDEK` and `descriptor.Write` have no production caller at
// all. The consequence is not subtle: `GetDesiredState` returns an empty list forever,
// so an Agent — however complete its data path — has nothing to serve.
//
// Provisioning is one act with three durable effects that must agree: a row in
// PostgreSQL, a wrapped DEK only this deployment's KEK can open, and a descriptor in
// the object store. The descriptor is not bookkeeping — it is the anchor
// `RebuildMetadata` reconstructs the catalog from (§22.5, INV-20) and the root
// `gc.Reachable` walks from, so a volume whose row exists and whose descriptor does not
// is a volume a metadata rebuild silently loses and the GC would mark.

func testKMS(t *testing.T) *crypto.DevKMS {
	t.Helper()
	var kek [crypto.DEKSize]byte
	for i := range kek {
		kek[i] = byte(i)
	}
	return crypto.NewDevKMS(kek, "kek-test")
}

// ramp is a deterministic byte source, so a provisioning run is reproducible (INV-02).
type ramp struct{ n byte }

func (r *ramp) Read(p []byte) (int, error) {
	for i := range p {
		r.n++
		p[i] = r.n
	}
	return len(p), nil
}

func TestProvisionCreatesTheRowTheKeyAndTheDescriptor(t *testing.T) {
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	md := metasim.New(clk.Wall)
	store := sim.NewObjectStore()
	term, err := md.AcquireLeadership(t.Context(), "cp-1")
	if err != nil {
		t.Fatal(err)
	}
	host := ids.New().String()
	if err := md.UpsertHost(t.Context(), term, metadata.Host{HostID: host, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40}); err != nil {
		t.Fatal(err)
	}

	kms := testKMS(t)
	p := controlplane.NewProvisioner(md, store, kms, &ramp{})

	vol, err := p.Provision(t.Context(), term, controlplane.VolumeSpec{
		SizeBytes:  1 << 30,
		BlockSize:  4096,
		HostID:     host,
		Durability: lifecycle.DurabilityRemote,
	})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}

	if _, err := ids.Parse(vol.VolumeID); err != nil {
		t.Fatalf("volume id %q is not a uuid: %v", vol.VolumeID, err)
	}

	got, err := md.GetVolume(t.Context(), vol.VolumeID)
	if err != nil {
		t.Fatalf("the volume row is not there: %v", err)
	}
	switch {
	case got.PrimaryHostID != host:
		t.Errorf("primary host = %q, want %q — GetDesiredState finds nothing without it", got.PrimaryHostID, host)
	case got.State != lifecycle.VolumeActive:
		t.Errorf("state = %q, want ACTIVE", got.State)
	case got.CurrentEpoch != 1:
		t.Errorf("epoch = %d, want 1: a volume that starts at 0 has no epoch to write under", got.CurrentEpoch)
	case len(got.DEKWrapped) == 0:
		t.Error("no wrapped DEK on the row: GetVolumeKeys would hand the Agent nothing")
	}

	// The wrapped key must open with this deployment's KEK and no other. Unwrapping
	// binds the version as GCM AAD, so a key stored under the wrong version is a key
	// the Agent cannot use — and it would only be discovered on the first WRITE.
	dek, err := kms.UnwrapDEK(got.DEKWrapped, vol.KeyID)
	if err != nil {
		t.Fatalf("the stored DEK does not unwrap with the KEK that wrapped it: %v", err)
	}
	if dek.KeyID == 0 {
		t.Fatal("KeyID 0 means plaintext in the WAL record header; a provisioned volume must not carry it")
	}

	d, err := descriptor.Read(t.Context(), store, vol.VolumeID)
	if err != nil {
		t.Fatalf("no descriptor in the object store: rebuild-metadata would lose this volume (%v)", err)
	}
	switch {
	case d.SizeBytes != got.SizeBytes || d.BlockSize != got.BlockSize:
		t.Errorf("descriptor geometry %d/%d disagrees with the row %d/%d", d.SizeBytes, d.BlockSize, got.SizeBytes, got.BlockSize)
	case d.KEKID != kms.KEKID():
		t.Errorf("descriptor names KEK %q, want %q", d.KEKID, kms.KEKID())
	case !bytes.Equal(d.DEKWrapped, got.DEKWrapped):
		t.Error("the descriptor's wrapped DEK differs from the row's: a rebuild would install a key that opens nothing")
	}
}

// A volume the Agent could never serve must not be created. blockdev.New refuses a
// capacity that is not a whole number of 512-byte sectors, so without this check the
// Control Plane accepts the volume, places it, and the failure appears on the host at
// attach — where the operator has the least context to understand it.
func TestProvisionRefusesGeometryNoAgentCanServe(t *testing.T) {
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	md := metasim.New(clk.Wall)
	term, err := md.AcquireLeadership(t.Context(), "cp-1")
	if err != nil {
		t.Fatal(err)
	}
	host := ids.New().String()
	if err := md.UpsertHost(t.Context(), term, metadata.Host{HostID: host, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40}); err != nil {
		t.Fatal(err)
	}
	p := controlplane.NewProvisioner(md, sim.NewObjectStore(), testKMS(t), &ramp{})

	tests := []struct {
		name string
		spec controlplane.VolumeSpec
	}{
		{"not a whole number of sectors", controlplane.VolumeSpec{SizeBytes: 1<<30 + 1, BlockSize: 4096, HostID: host}},
		{"zero size", controlplane.VolumeSpec{SizeBytes: 0, BlockSize: 4096, HostID: host}},
		{"negative size", controlplane.VolumeSpec{SizeBytes: -512, BlockSize: 4096, HostID: host}},
		{"block size that is not a sector multiple", controlplane.VolumeSpec{SizeBytes: 1 << 30, BlockSize: 999, HostID: host}},
		{"no host to place it on", controlplane.VolumeSpec{SizeBytes: 1 << 30, BlockSize: 4096}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := tc.spec
			if spec.Durability == "" {
				spec.Durability = lifecycle.DurabilityRemote
			}
			if _, err := p.Provision(t.Context(), term, spec); err == nil {
				t.Fatal("Provision accepted a volume no Agent could serve")
			}
		})
	}
}

// A stale Control Plane must create nothing (§7). The term guard is on the row; this
// asserts the whole act is refused rather than leaving a descriptor and a key behind
// for a volume that does not exist.
func TestProvisionUnderAStaleTermLeavesNothingBehind(t *testing.T) {
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	md := metasim.New(clk.Wall)
	store := sim.NewObjectStore()
	term, err := md.AcquireLeadership(t.Context(), "cp-1")
	if err != nil {
		t.Fatal(err)
	}
	host := ids.New().String()
	if err := md.UpsertHost(t.Context(), term, metadata.Host{HostID: host, State: lifecycle.HostActive, NVMeTotalBytes: 1 << 40}); err != nil {
		t.Fatal(err)
	}
	if _, err := md.AcquireLeadership(t.Context(), "cp-2"); err != nil { // term moves on
		t.Fatal(err)
	}

	p := controlplane.NewProvisioner(md, store, testKMS(t), &ramp{})
	vol, err := p.Provision(t.Context(), term, controlplane.VolumeSpec{
		SizeBytes: 1 << 30, BlockSize: 4096, HostID: host, Durability: lifecycle.DurabilityRemote,
	})
	if !errors.Is(err, metadata.ErrStaleTerm) {
		t.Fatalf("err = %v, want ErrStaleTerm", err)
	}
	if vol.VolumeID != "" {
		if _, err := descriptor.Read(t.Context(), store, vol.VolumeID); err == nil {
			t.Error("a refused provisioning left a descriptor in the object store")
		}
	}
}
