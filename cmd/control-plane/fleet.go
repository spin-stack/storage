package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spin-stack/storage/internal/controlplane"
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
//
// leaseTTL is the lease the Control Plane grants on a heartbeat, and it is the only
// new input this report needed to stop lying about a fleet that has died. Every
// "state" in the catalog is a column something has to write, so a host killed with
// -9 keeps reading ACTIVE for ever and a Control Plane that was SIGTERMed keeps
// reading LEADER: the process whose job it was to write the correction is the one
// that is gone. Liveness is therefore not read from a column at all — it is derived
// from how old the last write is, against the interval the fleet itself uses for
// "how long a fact may be believed".
func fleetReport(ctx context.Context, md metadata.Store, out io.Writer, leaseTTL time.Duration) error {
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

	// The hosts are read before the leader line is printed, and only here, because the
	// leader line is written out of them: see leaderSeenAt. A read failure is carried
	// rather than returned so the leader line still prints — the sections below keep
	// the same property, and this is a command whose whole purpose is being usable
	// when something is broken.
	hosts, herr := md.ListHosts(ctx)

	switch leader, lerr := md.GetLeader(ctx); {
	case errors.Is(lerr, metadata.ErrNotFound):
		// Not an error, and worth a line of its own: nothing is converging the fleet
		// right now, which is the first thing that explains a volume that is not being
		// picked up.
		p.printf("LEADER  none — no Control Plane has ever been elected\n\n")
	case lerr != nil:
		return fmt.Errorf("reading the leader: %w", lerr)
	default:
		reportLeader(p, leader, hosts, herr, now, leaseTTL)
	}

	// Each section is read and printed before the next is read. A later read that
	// fails then leaves the operator holding the sections that did come back, plus the
	// error — which is more than an all-or-nothing report gives them.
	if herr != nil {
		return fmt.Errorf("listing hosts: %w", herr)
	}
	reportHosts(p, hosts, now, leaseTTL)
	vols, err := reportVolumes(ctx, md, p)
	if err != nil {
		return err
	}
	reportRefusals(p, vols)
	if err := reportSnapshots(ctx, md, p, vols); err != nil {
		return err
	}
	return p.err
}

// reportLeader prints who is leading and — the part that was missing — whether
// anything has seen that process running.
//
// `renewed_at` was the word this line used, and it described a write nothing
// performs: metadata.Store's only leadership write is AcquireLeadership, which
// increments the term unconditionally, so the stamp is the election and never moves
// again. A Control Plane dead for five minutes and one that started five minutes ago
// printed the identical line.
//
// The renewal that would fix it properly is a term-guarded UPDATE of renewed_at that
// does *not* increment — a metadata.Store method that does not exist. Renewing with
// AcquireLeadership instead was rejected outright: it would move the term every few
// seconds, and every admin one-shot in this binary reads GetLeader and then writes
// under that term, so -flatten-volume and -delete-volume (both of which rewrite a
// whole image between the two) would start failing with ErrStaleTerm at random.
//
// What the catalog can already prove is used instead. Every host heartbeat is a
// term-guarded write performed by the serving Control Plane and by nothing else, so a
// heartbeat stamped after this term was created is that process running at that
// instant. The election is the same proof at time zero. It is a witness, not a lease:
// a fleet with no hosts, or one whose Agents are all dead too, has nothing to witness
// with and the line says exactly that rather than claiming the leader is gone.
func reportLeader(p *printer, leader metadata.Leader, hosts []metadata.Host, herr error, now time.Time, leaseTTL time.Duration) {
	head := fmt.Sprintf("LEADER  %s  term %d  elected %s ago",
		leader.HolderID, leader.Term, age(now, leader.RenewedAt))
	if herr != nil {
		p.printf("%s  (whether it is still running is unknown: the hosts that would witness it could not be read)\n\n", head)
		return
	}
	seen := leaderSeenAt(leader, hosts)
	if now.Sub(seen) <= leaseTTL {
		p.printf("%s  last seen %s ago\n\n", head, age(now, seen))
		return
	}
	p.printf("%s  NOT SEEN for %s — no host has heartbeated under this term since, so this process may be gone\n\n",
		head, age(now, seen))
}

// leaderSeenAt is the most recent instant something proves the current term's holder
// was running: the newest host heartbeat stamped at or after the election, or the
// election itself. Heartbeats older than the election are excluded because they were
// written by whoever held the *previous* term — counting them would let a Control
// Plane that took over and died immediately inherit its predecessor's liveness.
func leaderSeenAt(leader metadata.Leader, hosts []metadata.Host) time.Time {
	seen := leader.RenewedAt
	for _, h := range hosts {
		if h.LastHeartbeat.After(seen) {
			seen = h.LastHeartbeat
		}
	}
	return seen
}

func reportHosts(p *printer, hosts []metadata.Host, now time.Time, leaseTTL time.Duration) {
	var cordoned, stale int
	for _, h := range hosts {
		if !h.State.AcceptsPlacement() {
			cordoned++
		}
		if staleHeartbeat(h, now, leaseTTL) {
			stale++
		}
	}
	// The stale count is in the header next to the cordon count because they are the
	// same question — "how much of this fleet is not available?" — and the sentence
	// after it is there because the two counts do *not* mean the same thing to
	// placement. internal/placement admits any host whose State.AcceptsPlacement() is
	// true and never looks at last_heartbeat, so a host that has been dead for an hour
	// is still a candidate. Teaching it otherwise is a change to internal/placement,
	// not to this report; until then the operator has to be told, because a report
	// that marks a host dead reads as a report that took it out of the rotation.
	p.printf("HOSTS (%d, %d not taking placements, %d with no heartbeat in the last %s — placement does not exclude them)\n",
		len(hosts), cordoned, stale, leaseTTL)
	s := p.section("HOST_ID", "STATE", "REASON", "USED", "TOTAL", "COMMITTED", "HEARTBEAT")
	for _, h := range hosts {
		s.row("%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			h.HostID, hostState(h, now, leaseTTL), orNone(h.CordonReason.String()),
			used(h.NVMeUsedBytes, h.NVMeTotalBytes), capacity(h.NVMeTotalBytes),
			capacity(h.NVMeCommittedBytes), age(now, h.LastHeartbeat))
	}
	s.end()
}

// staleHeartbeat reports whether the fleet holds no current fact about a host: it has
// never introduced itself, or its last heartbeat is older than the lease the Control
// Plane grants on one. Not a state in the catalog, deliberately — a host that was
// killed cannot write "I am dead", which is why ACTIVE survived every kill -9 this
// system has seen. It is a fact about time and it is computed at read.
func staleHeartbeat(h metadata.Host, now time.Time, leaseTTL time.Duration) bool {
	return h.LastHeartbeat.IsZero() || now.Sub(h.LastHeartbeat) > leaseTTL
}

// hostState renders the STATE cell. A stale host keeps its catalog state inside the
// parentheses — placement still reads it, and it is what an operator un-cordons — but
// the cell no longer *is* that word: `ACTIVE` on a machine that has been off for an
// hour is the single most misleading thing this report printed. One token, so
// `awk '{print $2}'` and an eye both still work, and it sorts and greps as STALE.
func hostState(h metadata.Host, now time.Time, leaseTTL time.Duration) string {
	if !staleHeartbeat(h, now, leaseTTL) {
		return string(h.State)
	}
	return fmt.Sprintf("STALE(%s)", h.State)
}

// reportVolumes prints the volumes and returns them, so the snapshot section can name
// the host that is supposed to take each snapshot without reading the catalog twice.
func reportVolumes(ctx context.Context, md metadata.Store, p *printer) ([]metadata.Volume, error) {
	vols, err := md.ListVolumes(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing volumes: %w", err)
	}
	var unplaced, atCeiling int
	for _, v := range vols {
		if v.PrimaryHostID == "" {
			unplaced++
		}
		if v.ChainDepth >= controlplane.MaxChainDepth {
			atCeiling++
		}
	}
	// The unplaced count is in the header because it is a question ("which volumes has
	// nobody got?") rather than a detail: after -rebuild-metadata it is every volume in
	// the catalog, and until an operator places them the fleet serves nothing.
	//
	// The ceiling count is the second question this report can answer and nothing else
	// can. `controlplane.Clone` refuses a clone of a volume at MaxChainDepth, and the
	// operator who hits that refusal — or who wants to not hit it — needs the list of
	// volumes waiting on a FLATTEN. The `chain_depth` series does not answer it: it is
	// recorded when the Control Plane changes a depth and then goes quiet, so it says what
	// was created, while this says what the fleet is holding now.
	//
	// The refused count is the third, and it is the one that had no producer at all
	// until the Agent started reporting a refusal: a volume whose host has fail-closed
	// keeps the watermarks its last healthy report left behind, so every other number on
	// its row reads normal. It goes last in the header so the two counts that were
	// already asserted keep their position in the sentence.
	p.printf("VOLUMES (%d, %d with no primary host, %d at the depth ceiling of %d — a clone of one is refused until it is flattened; %d not being served by the host that holds it)\n",
		len(vols), unplaced, atCeiling, controlplane.MaxChainDepth, countRefused(vols))
	s := p.section("VOLUME_ID", "PRIMARY_HOST", "STATE", "EPOCH", "SIZE", "DEPTH", "PARENT_SNAPSHOT")
	for _, v := range vols {
		s.row("%s\t%s\t%s\t%d\t%s\t%d\t%s\n",
			v.VolumeID, orNone(v.PrimaryHostID), volumeState(v), v.CurrentEpoch,
			capacity(v.SizeBytes), v.ChainDepth, orNone(v.ParentSnapshotID))
	}
	s.end()
	return vols, nil
}

// volumeState renders the STATE cell, the same move hostState makes one section above
// and for the same reason: the catalog's word is still true about *ownership* — the
// volume is ACTIVE, this host is its primary, placement and every §7 transition still
// read it — and it is exactly the word that must not stand alone when the host holding
// it is refusing to serve it.
//
// One token, so `awk '{print $3}'` and an eye both still work, and it greps and sorts as
// NOT_SERVED. The reason itself is not in this cell: it belongs next to the sentence
// that explains it, in the section below, and a fourth column here would push
// PARENT_SNAPSHOT off an eighty-column terminal for every fleet, healthy or not.
func volumeState(v metadata.Volume) string {
	if !v.Refusal.Refused() {
		return string(v.State)
	}
	return fmt.Sprintf("NOT_SERVED(%s)", v.State)
}

// reportRefusals is the section that answers "why", and it is a section of its own
// rather than a column because the two questions have different shapes. "Which volumes
// are down" is a scan of the table above; "why is this one down" is one sentence with
// numbers in it — the sequence that went missing, the KEK that is not on the host — and
// a sentence does not fit a column-aligned table next to six other fields.
//
// It prints even when it is empty, like every other section here: "nothing is refusing
// to serve" is an answer, and a section that vanishes when it has nothing to say is
// indistinguishable from a report that stopped early.
//
// It is rendered from the volumes already read rather than from a filtered query. There
// is no such query, and there should not be one until something needs it at a scale a
// human is not reading: the rows are in hand, and a second read would be a second answer
// that can disagree with the table above it.
func reportRefusals(p *printer, vols []metadata.Volume) {
	p.printf("NOT SERVED (%d) — the fleet placed these volumes on a host that is refusing to serve them\n",
		countRefused(vols))
	s := p.section("VOLUME_ID", "ON_HOST", "REASON", "DETAIL")
	for _, v := range vols {
		if !v.Refusal.Refused() {
			continue
		}
		// DETAIL last and unpadded: it is the only free-text cell in this report, it is
		// the longest, and tabwriter would otherwise widen every row to the longest one.
		// orNone because a refusal an older Agent reported without a sentence is still a
		// refusal, and a blank final cell reads as a truncated line.
		s.row("%s\t%s\t%s\t%s\n", v.VolumeID, orNone(v.PrimaryHostID), v.Refusal, orNone(v.RefusalDetail))
	}
	s.end()
}

func countRefused(vols []metadata.Volume) int {
	var n int
	for _, v := range vols {
		if v.Refusal.Refused() {
			n++
		}
	}
	return n
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
