package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/spin-stack/storage/internal/lifecycle"
	"github.com/spin-stack/storage/internal/metadata"
	metasim "github.com/spin-stack/storage/internal/metadata/sim"
)

// TestAnOperatorsCordonOutranksThePressureLoop drives -cordon-host and -uncordon-host
// against a catalog and asserts on the only thing an operator can actually see
// afterwards: the row -fleet-status prints for that host.
//
// The assertion is the report and not the store's return value on purpose. A write
// that lands with the wrong authority returns nil just as happily as the right one,
// and the difference — whether the automatic loop can undo it — is visible only in
// what the next reader is told. So each step here is "make the write, then run the
// loop's own write against it, then read the report", which is the sequence an
// incident actually performs.
//
// The device numbers are the loop's: 90% used is past ADR-0013's 70% cordon ratio, so
// the pressure write below is the one cpserver.applyPressure would make on the next
// heartbeat, verbatim (cpserver/pressure.go composes exactly these arguments).
func TestAnOperatorsCordonOutranksThePressureLoop(t *testing.T) {
	ctx := t.Context()
	now := time.Unix(1_700_000_000, 0).UTC()
	md := metasim.New(func() time.Time { return now })

	term, err := md.AcquireLeadership(ctx, "cp-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := md.UpsertHost(ctx, term, metadata.Host{
		HostID: activeHost, State: lifecycle.HostActive,
		NVMeTotalBytes: 1 << 40, NVMeUsedBytes: 100 << 30,
	}); err != nil {
		t.Fatal(err)
	}

	// A human takes the host out of the rotation — the NIC is flapping, the machine is
	// about to be rebooted, something no measurement can see.
	if err := setCordon(ctx, md, term, activeHost, lifecycle.HostCordoned); err != nil {
		t.Fatalf("cordoning: %v", err)
	}
	if got := hostRow(t, md, ctx); got[1] != "CORDONED" || got[2] != "OPERATOR" {
		t.Fatalf("after -cordon-host the report says state=%s reason=%s; want CORDONED/OPERATOR", got[1], got[2])
	}

	// The device then empties, and the loop tries to hand the host back. This is the
	// write ADR-0013 §5 must refuse: whatever the device says, the reason the human
	// cordoned it has not gone away.
	if err := md.SetHostState(ctx, term, activeHost, lifecycle.HostActive, lifecycle.CordonPressure); err == nil {
		t.Fatal("the pressure loop cleared a cordon an operator set")
	}
	if got := hostRow(t, md, ctx); got[1] != "CORDONED" || got[2] != "OPERATOR" {
		t.Fatalf("the loop's refused write still changed the report: state=%s reason=%s", got[1], got[2])
	}

	// And the human hands it back, which also has to work when the standing cordon is
	// the loop's — the 65-to-70% dead zone an operator decides is fine.
	if err := setCordon(ctx, md, term, activeHost, lifecycle.HostActive); err != nil {
		t.Fatalf("uncordoning: %v", err)
	}
	if got := hostRow(t, md, ctx); got[1] != "ACTIVE" || got[2] != "-" {
		t.Fatalf("after -uncordon-host the report says state=%s reason=%s; want ACTIVE and no reason", got[1], got[2])
	}
	if err := md.SetHostState(ctx, term, activeHost, lifecycle.HostCordoned, lifecycle.CordonPressure); err != nil {
		t.Fatalf("the loop can no longer cordon a host the operator released: %v", err)
	}
	if got := hostRow(t, md, ctx); got[2] != "DEVICE_PRESSURE" {
		t.Fatalf("a full device did not re-cordon the released host: reason=%s", got[2])
	}
}

// hostRow runs the report and returns the fields of the line for activeHost, so every
// assertion above is about text a human reads rather than a struct field.
func hostRow(t *testing.T, md metadata.Store, ctx context.Context) []string {
	t.Helper()
	var buf bytes.Buffer
	if err := fleetReport(ctx, md, &buf); err != nil {
		t.Fatalf("fleetReport: %v", err)
	}
	return row(t, buf.String(), activeHost)
}
