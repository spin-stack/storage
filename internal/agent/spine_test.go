package agent_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	storagev1 "github.com/spin-stack/storage/api/gen/spin/storage/v1"
	"github.com/spin-stack/storage/api/gen/spin/storage/v1/storagev1connect"
	"github.com/spin-stack/storage/internal/agent"
	"github.com/spin-stack/storage/internal/cpserver"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// TestTheSpineEndToEnd runs the real Agent loop against the real handler over a
// real HTTP transport, with only the clock and the store simulated. It is the first
// test in this repository where a report leaves one component and lands in the
// other's authority: everything until now proved the halves separately.
//
// The two properties it is here for:
//
//   - a heartbeat writes nvme_used_bytes, which has existed in the schema since
//     Phase 07 with nothing to write it (ADR-0013 §3);
//   - a watermark report is applied only under the volume's current epoch, and the
//     Agent learns that the other one was refused.
func TestTheSpineEndToEnd(t *testing.T) {
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	md := metasim.New(clk.Wall)
	term, err := md.AcquireLeadership(t.Context(), "cp-1")
	if err != nil {
		t.Fatal(err)
	}

	const leaseTTL = 30 * time.Second
	httpSrv := httptest.NewServer(cpserver.Handler(cpserver.New(md, func() int64 { return term }, leaseTTL, cpserver.DefaultBand())))
	defer httpSrv.Close()

	// mine is served by this host under epoch 4; stolen was promoted away.
	for _, v := range []metadata.Volume{
		{VolumeID: "vol-mine", DEKKeyID: 1, SizeBytes: 1 << 30, BlockSize: 4096, CurrentEpoch: 4, PrimaryHostID: testHost,
			State: lifecycle.VolumeActive},
		{VolumeID: "vol-stolen", DEKKeyID: 1, SizeBytes: 1 << 30, BlockSize: 4096, CurrentEpoch: 9, PrimaryHostID: testHost,
			State: lifecycle.VolumeActive},
	} {
		if err := md.CreateVolume(t.Context(), term, v, nil); err != nil {
			t.Fatal(err)
		}
	}

	vols := agent.NewVolumeSet()
	vols.Set(agent.VolumeStatus{VolumeID: "vol-mine", Epoch: 4, LocalSequence: 30, DurableSequence: 20, PublishedSequence: 10})
	vols.Set(agent.VolumeStatus{VolumeID: "vol-stolen", Epoch: 8, LocalSequence: 7, DurableSequence: 7, PublishedSequence: 7})

	loop, err := agent.New(testConfig(), agent.Deps{
		Clock:        clk,
		ControlPlane: storagev1connect.NewControlPlaneServiceClient(httpSrv.Client(), httpSrv.URL),
		Device:       fakeDevice{usage: disk.Usage{TotalBytes: 1 << 40, UsedBytes: 512 << 30}},
		Volumes:      vols,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := loop.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	host, err := md.GetHost(t.Context(), testHost)
	if err != nil {
		t.Fatalf("the heartbeat did not register the host: %v", err)
	}
	if host.NVMeTotalBytes != 1<<40 || host.NVMeUsedBytes != 512<<30 {
		t.Fatalf("device numbers did not cross the wire: %+v", host)
	}
	// The field still crosses the wire and is always zero: nothing measures a backlog
	// since the uploader went (ADR-0026 increment 4.5). Asserted rather than dropped,
	// because a field that silently starts carrying something again is worth catching.
	if host.RemoteBacklogBytes != 0 {
		t.Fatalf("the aggregate remote backlog did not cross the wire: %+v", host)
	}
	if host.AgentVersion != testVersion || host.MaxFormatVersion != 3 {
		t.Fatalf("identity did not cross the wire: %+v", host)
	}
	if !loop.LeaseValid() {
		t.Fatal("the Agent holds no lease after a successful heartbeat")
	}
	if got := loop.HostState(); got != storagev1.HostState_HOST_STATE_ACTIVE {
		t.Fatalf("host state = %v, want ACTIVE", got)
	}

	desired := loop.Desired()
	if len(desired) != 2 || desired[0].GetVolumeId() != "vol-mine" || desired[0].GetEpoch() != 4 {
		t.Fatalf("desired state = %+v", desired)
	}

	mine, err := md.GetVolume(t.Context(), "vol-mine")
	if err != nil {
		t.Fatal(err)
	}
	if mine.LocalSequence != 30 || mine.DurableSequence != 20 || mine.PublishedSequence != 10 {
		t.Fatalf("in-epoch watermarks were not applied: %+v", mine)
	}
	stolen, err := md.GetVolume(t.Context(), "vol-stolen")
	if err != nil {
		t.Fatal(err)
	}
	if stolen.LocalSequence != 0 {
		t.Fatalf("a report under a superseded epoch was applied: %+v", stolen)
	}
	fenced := loop.Fenced()
	if len(fenced) != 1 || fenced[0] != "vol-stolen" {
		t.Fatalf("the Agent did not learn it was fenced for vol-stolen: %v", fenced)
	}
}

// TestARefusalCrossesTheSpineAndClears is the same seam for the fact the report could
// not carry until now: this host is **not serving** a volume, and why.
//
// It is here rather than in a cpserver unit test because the failure it closes is a seam
// failure. Every component was right on its own — the Agent refused correctly, the
// Control Plane recorded watermarks correctly, `-fleet-status` printed the row correctly
// — and the fleet still could not see a volume that had stopped serving, because nothing
// on the wire said so. The proof has to be a report leaving the Agent and a catalog row
// coming back changed.
//
// Three properties, and the second and third are the ones a watermark does not need:
//
//   - it lands, with the sentence an operator reads;
//   - it clears when the volume serves again, with nothing sweeping it;
//   - a host the fleet has moved past cannot write it, so a slow report from a fenced
//     writer cannot mark a volume its successor is serving perfectly well.
func TestARefusalCrossesTheSpineAndClears(t *testing.T) {
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	md := metasim.New(clk.Wall)
	term, err := md.AcquireLeadership(t.Context(), "cp-1")
	if err != nil {
		t.Fatal(err)
	}
	httpSrv := httptest.NewServer(cpserver.Handler(cpserver.New(md, func() int64 { return term }, 30*time.Second, cpserver.DefaultBand())))
	defer httpSrv.Close()

	for _, v := range []metadata.Volume{
		{VolumeID: "vol-mine", DEKKeyID: 1, SizeBytes: 1 << 30, BlockSize: 4096, CurrentEpoch: 4,
			PrimaryHostID: testHost, State: lifecycle.VolumeActive},
		{VolumeID: "vol-stolen", DEKKeyID: 1, SizeBytes: 1 << 30, BlockSize: 4096, CurrentEpoch: 9,
			PrimaryHostID: testHost, State: lifecycle.VolumeActive},
	} {
		if err := md.CreateVolume(t.Context(), term, v, nil); err != nil {
			t.Fatal(err)
		}
	}

	vols := agent.NewVolumeSet()
	loop, err := agent.New(testConfig(), agent.Deps{
		Clock:        clk,
		ControlPlane: storagev1connect.NewControlPlaneServiceClient(httpSrv.Client(), httpSrv.URL),
		Device:       fakeDevice{usage: disk.Usage{TotalBytes: 1 << 40, UsedBytes: 1 << 30}},
		Volumes:      vols,
	})
	if err != nil {
		t.Fatal(err)
	}

	// The watermarks are the healthy ones a previous session left behind, on purpose:
	// that is exactly the state the catalog was in while a volume was refusing to serve,
	// and the reason nothing could see it.
	const detail = "agent: volume vol-mine published up to sequence 512 and the object store holds no image for it"
	vols.Set(agent.VolumeStatus{
		VolumeID: "vol-mine", Epoch: 4, LocalSequence: 30, DurableSequence: 20, PublishedSequence: 10,
		Refusal:       storagev1.VolumeRefusal_VOLUME_REFUSAL_IMAGE_MISSING,
		RefusalDetail: detail,
	})
	// A fenced writer reporting a refusal for a volume that has moved past it. Its
	// watermarks are already refused by the epoch check; the refusal must be too, and by
	// the same fact rather than by a second rule that can drift from it.
	vols.Set(agent.VolumeStatus{
		VolumeID: "vol-stolen", Epoch: 8,
		Refusal:       storagev1.VolumeRefusal_VOLUME_REFUSAL_LEASE_LOST,
		RefusalDetail: "this host's lease expired",
	})

	if err := loop.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	mine, err := md.GetVolume(t.Context(), "vol-mine")
	if err != nil {
		t.Fatal(err)
	}
	if mine.Refusal != lifecycle.RefusalImageMissing {
		t.Fatalf("the catalog does not record that vol-mine is not being served: %+v", mine)
	}
	if mine.RefusalDetail != detail {
		t.Fatalf("refusal detail = %q, want the Agent's own sentence", mine.RefusalDetail)
	}
	stolen, err := md.GetVolume(t.Context(), "vol-stolen")
	if err != nil {
		t.Fatal(err)
	}
	if stolen.Refusal != lifecycle.RefusalNone {
		t.Fatalf("a writer the fleet moved past marked vol-stolen as not served: %+v", stolen)
	}

	// The volume comes back. Nothing sweeps, nothing notices: the next report simply
	// carries no refusal, and that is the whole of the clearing mechanism.
	vols.Set(agent.VolumeStatus{
		VolumeID: "vol-mine", Epoch: 4, LocalSequence: 40, DurableSequence: 30, PublishedSequence: 20,
	})
	if err := loop.Reconcile(t.Context()); err != nil {
		t.Fatalf("the cycle after the volume recovered failed: %v", err)
	}
	mine, err = md.GetVolume(t.Context(), "vol-mine")
	if err != nil {
		t.Fatal(err)
	}
	if mine.Refusal != lifecycle.RefusalNone || mine.RefusalDetail != "" {
		t.Fatalf("a volume that is serving again still reads as refused: %+v", mine)
	}
	if mine.LocalSequence != 40 {
		t.Fatalf("the recovered watermarks were not applied: %+v", mine)
	}
}

// ramp is a deterministic byte source: DEK generation and wrapping are the only
// two consumers of randomness on this path (§15.2), and both take an injected
// reader precisely so a test can pin them.
type ramp struct{ b byte }

func (r *ramp) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = r.b
		r.b++
	}
	return len(p), nil
}

// TestTheAgentCanOpenAVolume closes the last of the three holes the spine left: the
// desired state told the Agent a volume's geometry and epoch and nothing about how
// to read a byte of it. Every payload on this path is sealed with the volume's DEK
// (§15.1), so "attach this volume" without key material is an instruction the Agent
// cannot carry out.
//
// It runs the whole way round — the Agent asks the real handler over HTTP, and what
// comes back is unwrapped with the KEK a host's KMS holds — because the property is
// not "a field arrived" but "the material is usable". A wrapped DEK that unwraps to
// the wrong bytes, or that the Control Plane truncated on the way through, would
// pass a field-by-field assertion and fail here.
func TestTheAgentCanOpenAVolume(t *testing.T) {
	clk := sim.NewClock(time.Unix(1_700_000_000, 0).UTC())
	md := metasim.New(clk.Wall)
	term, err := md.AcquireLeadership(t.Context(), "cp-1")
	if err != nil {
		t.Fatal(err)
	}

	// The KEK is the host's; the Control Plane never sees it. keyID 1 is the
	// volume's first DEK version — 0 is reserved for plaintext records (§14.1).
	var kek [crypto.DEKSize]byte
	for i := range kek {
		kek[i] = byte(i)
	}
	kms := crypto.NewDevKMS(kek, "kek-host-a")
	dek, err := crypto.GenerateDEK(&ramp{b: 1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	// A real v7 id, and not this file's usual "vol-mine": a wrap is bound to the volume
	// that carries it now, so the id has to be one. Fixed rather than drawn, because the
	// rest of this test asserts on exact strings.
	const keyedVolume = "0198c0de-0000-7000-8000-00000000d0e5"
	wrapped, err := kms.WrapDEK(&ramp{b: 100}, dek, uuid.MustParse(keyedVolume))
	if err != nil {
		t.Fatal(err)
	}

	if err := md.CreateVolume(t.Context(), term, metadata.Volume{
		VolumeID: keyedVolume, SizeBytes: 1 << 30, BlockSize: 4096, CurrentEpoch: 4,
		PrimaryHostID: testHost, State: lifecycle.VolumeActive,
		DEKWrapped: wrapped, KEKID: kms.KEKID(), DEKKeyID: 1,
	}, nil); err != nil {
		t.Fatal(err)
	}

	httpSrv := httptest.NewServer(cpserver.Handler(cpserver.New(md, func() int64 { return term }, 30*time.Second, cpserver.DefaultBand())))
	defer httpSrv.Close()

	loop, err := agent.New(testConfig(), agent.Deps{
		Clock:        clk,
		ControlPlane: storagev1connect.NewControlPlaneServiceClient(httpSrv.Client(), httpSrv.URL),
		Device:       fakeDevice{usage: disk.Usage{TotalBytes: 1 << 40, UsedBytes: 1 << 30}},
		Volumes:      agent.NewVolumeSet(),
	})
	if err != nil {
		t.Fatal(err)
	}

	keys, err := loop.VolumeKeys(t.Context(), keyedVolume)
	if err != nil {
		t.Fatalf("VolumeKeys: %v", err)
	}
	if keys.KEKID != kms.KEKID() {
		t.Fatalf("kek_id = %q, want %q — the Agent cannot tell which KEK to use", keys.KEKID, kms.KEKID())
	}
	got, err := kms.UnwrapDEK(keys.DEKWrapped, dek.KeyID, uuid.MustParse(keyedVolume))
	if err != nil {
		t.Fatalf("the material the Control Plane served does not unwrap: %v", err)
	}
	if got.Key != dek.Key {
		t.Fatal("the unwrapped DEK is not the volume's key")
	}
}
