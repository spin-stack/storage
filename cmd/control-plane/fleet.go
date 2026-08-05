package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spin-stack/storage/internal/metadata"
)

// fleetReport writes what the catalog says about the fleet, in the three answers an
// operator wants at 3am: which hosts are out of service and why, which volumes nobody
// is serving, and which snapshots nothing is going to finish.
//
// It exists because until now the only fleet-wide *read* of this system was psql.
// cmd/control-plane grew a one-shot for every write an operator needs — seed,
// snapshot, clone, rebuild, detach, attach — and not one for looking. Every read the
// Control Plane serves is scoped to a host, because every one of them answers an
// Agent, and the rows an incident is about are precisely the rows no host owns.
//
// ADR-0021 says this binary is a test harness and will not be deployed: storage
// integrates into the sibling `spin` project, and the operator interface is that
// project's. So the bar here is "someone running this repository's own lanes can see
// the fleet", not "a CLI". That is why it is a flag on this binary rather than a
// subcommand tree, why there is no filtering or paging, and why there is no JSON:
// nothing consumes this output but a human, columnar text is what `grep` and `awk`
// already work on, and a second serialization of the catalog is a format somebody
// would then have to keep in step with the proto that already exists for machines.
//
// It takes no term and never asks for one, unlike every other one-shot in this file.
// A read guards nothing — §7's term exists so a zombie Control Plane's *writes* are
// refused — and the moment an operator most needs to see the catalog is the moment
// nothing is leading. Failing here with "start a Control Plane first" would remove
// the view exactly when it is the only thing left. The leader line below reports that
// state instead of refusing over it.
func fleetReport(ctx context.Context, md metadata.Store, out io.Writer) error {
	p := &printer{out: out}

	// The store's own clock, not this machine's: the heartbeat ages below are
	// differences against the clock that stamped them (the same authority §12.1 makes
	// a fencing deadline measure against), so an operator's laptop being minutes off
	// cannot turn a healthy host into a stale one on screen. It is also the one
	// spelling of "now" INV-01 allows here.
	now, err := md.Now(ctx)
	if err != nil {
		return fmt.Errorf("reading the catalog's clock: %w", err)
	}
	p.printf("as of %s (the catalog's clock)\n\n", now.UTC().Format(time.RFC3339))

	switch leader, lerr := md.GetLeader(ctx); {
	case errors.Is(lerr, metadata.ErrNotFound):
		// Not an error, and worth a line of its own: nothing is converging the fleet
		// right now, which is the first thing that explains a volume that is not being
		// picked up.
		p.printf("LEADER  none — no Control Plane has ever been elected\n\n")
	case lerr != nil:
		return fmt.Errorf("reading the leader: %w", lerr)
	default:
		p.printf("LEADER  %s  term %d  renewed %s ago\n\n",
			leader.HolderID, leader.Term, age(now, leader.RenewedAt))
	}

	// Each section is read and printed before the next is read. A later read that
	// fails then leaves the operator holding the sections that did come back, plus the
	// error — which is more than an all-or-nothing report gives them, and this is a
	// command whose whole purpose is being usable when something is broken.
	if err := reportHosts(ctx, md, p, now); err != nil {
		return err
	}
	vols, err := reportVolumes(ctx, md, p)
	if err != nil {
		return err
	}
	if err := reportSnapshots(ctx, md, p, vols); err != nil {
		return err
	}
	return p.err
}

func reportHosts(ctx context.Context, md metadata.Store, p *printer, now time.Time) error {
	hosts, err := md.ListHosts(ctx)
	if err != nil {
		return fmt.Errorf("listing hosts: %w", err)
	}
	var cordoned int
	for _, h := range hosts {
		if !h.State.AcceptsPlacement() {
			cordoned++
		}
	}
	p.printf("HOSTS (%d, %d not taking placements)\n", len(hosts), cordoned)
	s := p.section("HOST_ID", "STATE", "REASON", "USED", "TOTAL", "COMMITTED", "HEARTBEAT")
	for _, h := range hosts {
		s.row("%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			h.HostID, h.State, orNone(h.CordonReason.String()),
			used(h.NVMeUsedBytes, h.NVMeTotalBytes), capacity(h.NVMeTotalBytes),
			capacity(h.NVMeCommittedBytes), age(now, h.LastHeartbeat))
	}
	s.end()
	return nil
}

// reportVolumes prints the volumes and returns them, so the snapshot section can name
// the host that is supposed to take each snapshot without reading the catalog twice.
func reportVolumes(ctx context.Context, md metadata.Store, p *printer) ([]metadata.Volume, error) {
	vols, err := md.ListVolumes(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing volumes: %w", err)
	}
	var unplaced int
	for _, v := range vols {
		if v.PrimaryHostID == "" {
			unplaced++
		}
	}
	// The unplaced count is in the header because it is a question ("which volumes has
	// nobody got?") rather than a detail: after -rebuild-metadata it is every volume in
	// the catalog, and until an operator places them the fleet serves nothing.
	p.printf("VOLUMES (%d, %d with no primary host)\n", len(vols), unplaced)
	s := p.section("VOLUME_ID", "PRIMARY_HOST", "STATE", "EPOCH", "SIZE", "PARENT_SNAPSHOT")
	for _, v := range vols {
		s.row("%s\t%s\t%s\t%d\t%s\t%s\n",
			v.VolumeID, orNone(v.PrimaryHostID), v.State, v.CurrentEpoch,
			capacity(v.SizeBytes), orNone(v.ParentSnapshotID))
	}
	s.end()
	return vols, nil
}

func reportSnapshots(ctx context.Context, md metadata.Store, p *printer, vols []metadata.Volume) error {
	snaps, err := md.ListUnfinishedSnapshots(ctx)
	if err != nil {
		return fmt.Errorf("listing unfinished snapshots: %w", err)
	}
	primary := make(map[string]string, len(vols))
	for _, v := range vols {
		primary[v.VolumeID] = v.PrimaryHostID
	}
	p.printf("SNAPSHOTS NOT FINISHED (%d)\n", len(snaps))
	// ON_HOST is the volume's *current* primary, not snapshots.source_host_id, which is
	// stamped on completion and is therefore empty for every row here. It is the column
	// that says whether anything is going to happen: a CREATING snapshot whose volume
	// has no primary is nobody's work, and it will sit there until a host is placed.
	s := p.section("SNAPSHOT_ID", "VOLUME_ID", "STATE", "EPOCH", "ON_HOST")
	for _, snap := range snaps {
		s.row("%s\t%s\t%s\t%d\t%s\n",
			snap.SnapshotID, snap.VolumeID, snap.State, snap.Epoch, orNone(primary[snap.VolumeID]))
	}
	s.end()
	return nil
}

// printer is the errWriter of Dave Cheney's "practical Go": every write on a report
// goes to the same stream, so checking each of the forty of them individually says
// nothing an operator can act on and buries the report in error handling. The first
// failure is kept and everything after it is a no-op; fleetReport returns it once.
type printer struct {
	out io.Writer
	err error
}

func (p *printer) printf(format string, a ...any) { p.write(p.out, format, a...) }

func (p *printer) write(w io.Writer, format string, a ...any) {
	if p.err != nil {
		return
	}
	_, p.err = fmt.Fprintf(w, format, a...)
}

// section is one column-aligned table. It counts its own rows so that end can say
// "(none)" for an empty one: a section that is silently absent reads as a report that
// did not run, and "no host is cordoned" is an answer.
type section struct {
	p    *printer
	tw   *tabwriter.Writer
	rows int
}

func (p *printer) section(headers ...string) *section {
	s := &section{p: p, tw: tabwriter.NewWriter(p.out, 0, 0, 2, ' ', 0)}
	p.write(s.tw, "%s\n", strings.Join(headers, "\t"))
	return s
}

func (s *section) row(format string, a ...any) {
	s.rows++
	s.p.write(s.tw, format, a...)
}

func (s *section) end() {
	// The flush is where a tabwriter's writes actually reach the stream, so it is
	// where most I/O errors on this path surface at all.
	if s.p.err == nil {
		s.p.err = s.tw.Flush()
	}
	if s.rows == 0 {
		s.p.printf("  (none)\n")
	}
	s.p.printf("\n")
}

// orNone renders an empty column as a dash. Empty is a real answer here — no cordon
// reason, no primary host, no parent snapshot — and a blank cell in a whitespace-
// aligned table is one an eye slides over and a `awk '{print $2}'` misreads.
func orNone(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// age is a duration against the catalog's clock, or "never" for a zero instant — a
// host that has never heartbeaten, or a leader nobody has renewed. Seconds are enough
// resolution for a heartbeat that is measured in a lease TTL.
func age(now, then time.Time) string {
	if then.IsZero() {
		return "never"
	}
	d := now.Sub(then).Truncate(time.Second)
	if d < 0 {
		// A row stamped in the future is a clock that moved, not a negative age. Say so
		// rather than printing "-3s" and letting the reader decide it is a rounding.
		return "in the future"
	}
	return d.String()
}

// capacity renders a byte count in the units the fleet is provisioned in. Binary
// units, because every size in this system is a whole number of 512-byte sectors and
// a decimal GB would print a 1 GiB volume as 1.07.
func capacity(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for n/div >= unit && exp < 4 {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTP"[exp])
}

// used pairs the measured fill with its share of the device, because the share is
// what the ADR-0013 cordon and placement's fill ceiling are expressed in: a host at
// "870.0GiB" tells an operator nothing until they divide it by the total themselves.
func used(n, total int64) string {
	if total <= 0 {
		return capacity(n) + " (unmeasured)"
	}
	return fmt.Sprintf("%s (%.0f%%)", capacity(n), 100*float64(n)/float64(total))
}
