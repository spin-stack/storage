package controlplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/descriptor"
	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// GCCatalog is what a reachability walk needs from the catalog: the volumes whose HEAD is
// a root, and the published snapshots whose commits are the other roots.
type GCCatalog interface {
	ListVolumes(ctx context.Context) ([]metadata.Volume, error)
	ListPublishedSnapshots(ctx context.Context) ([]metadata.Snapshot, error)
}

// GCCandidate is one object nothing reaches.
type GCCandidate struct {
	Key       string
	SizeBytes int64
	// Age is how long ago the object was written, against the clock passed to PlanGC.
	//
	// It is the object's age and not the time it has been unreachable, and the two are
	// not the same number: an object can be written today and lose its last reference in
	// a year. The age is the upper bound of the two, so testing it against the grace
	// period is the strict direction for the case the grace period exists for — a publish
	// in flight, whose layer is on the bucket before the manifest that names it and is by
	// definition new. Rejected: a first-seen-unreachable stamp persisted between runs. It
	// would be the exact number, and it would make this command stateful — a second
	// authority about the bucket, which is the thing the object store is.
	Age time.Duration
	Why string
}

// GCPlan is what one dry run found. It proposes; nothing here deletes.
type GCPlan struct {
	Now   time.Time
	Grace time.Duration
	// Roots is how many commits the walk started from, and ReachableCommits how many it
	// ended up naming. An operator reads them together: a report proposing half the
	// bucket under one root is a report about a catalog that is not there.
	Roots            int
	ReachableCommits int
	ReachableLayers  int
	// Candidates is what a human could delete, ordered by key.
	Candidates []GCCandidate
	// Held is unreachable and inside the grace period, listed rather than dropped: an
	// object that keeps appearing here across runs is either a stuck publish or a grace
	// period that is too long, and neither is visible if the section is silent.
	Held []GCCandidate
	// heads is the ETag every rooted volume's HEAD carried when the walk read it, and
	// the whole of what ApplyGC re-checks. Unexported because it is not information for
	// a human: nothing but the delete can use it, and a plan a human edited is a plan
	// nothing re-checked.
	heads map[string]string
}

// PlanGC lists the objects in the bucket that no root reaches, and deletes nothing.
//
// §20's first rule is that an object is never deleted because it does not appear in the
// current HEAD: commits form a graph through parent_commit_id and snapshots keep old
// commits alive. So the roots are every volume's HEAD **and** every published snapshot's
// commit, and reachability is the transitive closure of parent_commit_id over both —
// every manifest and every layer object those commits name.
//
// Two conservative choices beyond that:
//
//   - the roots are the union of the catalog's volumes and the volumes with a HEAD object
//     in the bucket. A volume in the bucket that the catalog has never heard of is what a
//     lost PostgreSQL looks like (-rebuild-metadata exists for it), and taking the roots
//     from the catalog alone would turn that outage into a proposal to delete the fleet;
//   - a commit a root names that cannot be read stops the whole report. Half a walk
//     cannot tell an orphan from an object whose only reference is the one that would not
//     load.
//
// Only the two kinds of object reachability can name are classified: layers, and the
// commit manifests under a volume's prefix. HEAD, descriptor.json and the epoch object
// are a volume's identity rather than its history — nothing in the commit graph points at
// them, so a walk can never make them reachable and this command must never propose them.
func PlanGC(ctx context.Context, md GCCatalog, store objectstore.Store, now time.Time, grace time.Duration) (GCPlan, error) {
	plan := GCPlan{Now: now, Grace: grace, heads: map[string]string{}}

	roots, err := gcRoots(ctx, md, store, plan.heads)
	if err != nil {
		return GCPlan{}, err
	}
	plan.Roots = len(roots)

	reachable := map[string]bool{}
	for _, r := range roots {
		if err := walkCommits(ctx, store, r, reachable, &plan); err != nil {
			return GCPlan{}, err
		}
	}

	unreachable, err := unreachableObjects(ctx, store, reachable)
	if err != nil {
		return GCPlan{}, err
	}
	for _, o := range unreachable {
		c := GCCandidate{Key: o.Key, SizeBytes: o.Size, Age: now.Sub(o.LastModified), Why: whyUnreachable(o.Key)}
		switch {
		case o.LastModified.IsZero():
			// No modification time is not "old": it is an object store that did not say.
			// The grace period cannot be tested against a number that is not there, and
			// the untested side of that test is the side that loses data.
			c.Why = "the object store reported no modification time, so its age cannot be shown to exceed the grace period"
			plan.Held = append(plan.Held, c)
		case c.Age < grace:
			plan.Held = append(plan.Held, c)
		default:
			plan.Candidates = append(plan.Candidates, c)
		}
	}
	return plan, nil
}

// gcRoot is one commit a walk starts at, and what named it — which is what the report
// prints when it explains why an object was kept.
type gcRoot struct {
	volumeID string
	commitID string
	named    string
}

func gcRoots(ctx context.Context, md GCCatalog, store objectstore.Store, heads map[string]string) ([]gcRoot, error) {
	vols, err := md.ListVolumes(ctx)
	if err != nil {
		return nil, fmt.Errorf("controlplane: listing volumes: %w", err)
	}
	var ids []string
	for _, v := range vols {
		ids = append(ids, v.VolumeID)
	}
	bucketIDs, err := volumesWithAHead(ctx, store)
	if err != nil {
		return nil, err
	}
	for _, id := range bucketIDs {
		if !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)

	var roots []gcRoot
	for _, id := range ids {
		head, etag, err := commit.ReadHead(ctx, store, id)
		if errors.Is(err, commit.ErrNoHead) {
			// A volume that has never published, or whose HEAD is gone. Not an error:
			// the snapshots below may still root its history, and if they do not, its
			// objects are exactly what this command is for.
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("controlplane: reading the HEAD of volume %s: %w", id, err)
		}
		heads[id] = etag
		roots = append(roots, gcRoot{volumeID: id, commitID: head.CommitID, named: "the HEAD of volume " + id})
	}

	snaps, err := md.ListPublishedSnapshots(ctx)
	if err != nil {
		return nil, fmt.Errorf("controlplane: listing published snapshots: %w", err)
	}
	for _, s := range snaps {
		if s.CommitID == "" {
			// A published snapshot names a commit (the catalog CHECKs it), so this is a
			// row no walk can start at. Refused rather than skipped: skipping it would
			// propose deleting whatever the snapshot was meant to hold.
			return nil, fmt.Errorf("controlplane: snapshot %s of volume %s is published and names no commit, so nothing can be shown to be unreachable",
				s.SnapshotID, s.VolumeID)
		}
		roots = append(roots, gcRoot{
			volumeID: s.VolumeID, commitID: s.CommitID,
			named: "snapshot " + s.SnapshotID + " of volume " + s.VolumeID,
		})
	}
	return roots, nil
}

// volumesWithAHead is every volume the bucket holds a HEAD for.
func volumesWithAHead(ctx context.Context, store objectstore.Store) ([]string, error) {
	objs, err := store.List(ctx, descriptor.Prefix)
	if err != nil {
		return nil, fmt.Errorf("controlplane: listing %s: %w", descriptor.Prefix, err)
	}
	var out []string
	for _, o := range objs {
		id, ok := strings.CutPrefix(o.Key, descriptor.Prefix)
		if !ok {
			continue
		}
		id, ok = strings.CutSuffix(id, "/HEAD")
		if !ok || strings.Contains(id, "/") {
			continue
		}
		out = append(out, id)
	}
	return out, nil
}

// walkCommits marks one root's whole ancestry reachable.
func walkCommits(ctx context.Context, store objectstore.Store, r gcRoot, reachable map[string]bool, plan *GCPlan) error {
	for id := r.commitID; id != ""; {
		key := commit.ManifestKey(r.volumeID, id)
		if reachable[key] {
			// Already walked, by this root or another. It also terminates the walk on a
			// cycle: manifests refuse a self-parent, and nothing refuses a longer one.
			return nil
		}
		m, err := commit.ReadManifest(ctx, store, r.volumeID, id)
		if err != nil {
			return fmt.Errorf("controlplane: walking back from %s: reading commit %s: %w. "+
				"Nothing is proposed for deletion while a history cannot be read to its end — an object whose only reference is in the commit that would not load is indistinguishable from an orphan",
				r.named, id, err)
		}
		reachable[key] = true
		plan.ReachableCommits++
		if !reachable[m.Layer.ObjectKey] {
			reachable[m.Layer.ObjectKey] = true
			plan.ReachableLayers++
		}
		id = m.ParentCommitID
	}
	return nil
}

// unreachableObjects lists the layers and commit manifests no root named, ordered by key.
func unreachableObjects(ctx context.Context, store objectstore.Store, reachable map[string]bool) ([]objectstore.ObjectInfo, error) {
	var out []objectstore.ObjectInfo
	layers, err := store.List(ctx, commit.LayerPrefix)
	if err != nil {
		return nil, fmt.Errorf("controlplane: listing %s: %w", commit.LayerPrefix, err)
	}
	for _, o := range layers {
		if !reachable[o.Key] {
			out = append(out, o)
		}
	}
	underVolumes, err := store.List(ctx, descriptor.Prefix)
	if err != nil {
		return nil, fmt.Errorf("controlplane: listing %s: %w", descriptor.Prefix, err)
	}
	for _, o := range underVolumes {
		if isManifestKey(o.Key) && !reachable[o.Key] {
			out = append(out, o)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// isManifestKey reports whether a key is a commit manifest, by rebuilding the key from
// what it was read as: a rule about which characters may appear where would be one
// encoding away from being wrong, and the wrong answer here is an object proposed for
// deletion.
func isManifestKey(key string) bool {
	rest, ok := strings.CutPrefix(key, descriptor.Prefix)
	if !ok {
		return false
	}
	volumeID, rest, ok := strings.Cut(rest, "/")
	if !ok {
		return false
	}
	commitID, ok := strings.CutPrefix(rest, "commits/")
	if !ok {
		return false
	}
	commitID, ok = strings.CutSuffix(commitID, ".json")
	if !ok {
		return false
	}
	return commit.ManifestKey(volumeID, commitID) == key
}

func whyUnreachable(key string) string {
	if strings.HasPrefix(key, commit.LayerPrefix) {
		return "no reachable commit names this layer"
	}
	return "no HEAD and no published snapshot reaches this commit"
}

// Print writes the plan for a human. It is the whole output of the command, so it says
// in its first line that it deletes nothing: an operator who reads a list of object keys
// under a heading they skimmed will otherwise assume the objects are gone.
func (p GCPlan) Print(w io.Writer) error {
	out := &gcPrinter{w: w}
	out.printf("orphan report as of %s, grace period %s — this deletes nothing\n\n",
		p.Now.UTC().Format(time.RFC3339), p.Grace)
	out.printf("REACHABLE from %d roots (every volume's HEAD, every published snapshot's commit, and every parent of those): %d commits, %d layers\n\n",
		p.Roots, p.ReachableCommits, p.ReachableLayers)
	out.section("WOULD DELETE", "unreachable for longer than the grace period", p.Candidates)
	out.section("HELD", "unreachable, but not for long enough to be a candidate — a publish in flight looks exactly like an orphan", p.Held)
	return out.err
}

// gcPrinter is the errWriter of Dave Cheney's "practical Go": one stream, one error,
// checked once by the caller.
type gcPrinter struct {
	w   io.Writer
	err error
}

func (p *gcPrinter) printf(format string, a ...any) { p.write(p.w, format, a...) }

func (p *gcPrinter) write(w io.Writer, format string, a ...any) {
	if p.err != nil {
		return
	}
	_, p.err = fmt.Fprintf(w, format, a...)
}

func (p *gcPrinter) section(title, subtitle string, cs []GCCandidate) {
	p.printf("%s (%d) — %s\n", title, len(cs), subtitle)
	tw := tabwriter.NewWriter(p.w, 0, 0, 2, ' ', 0)
	p.write(tw, "KEY\tBYTES\tAGE\tWHY\n")
	for _, c := range cs {
		p.write(tw, "%s\t%d\t%s\t%s\n", c.Key, c.SizeBytes, c.Age.Truncate(time.Second), c.Why)
	}
	if p.err == nil {
		p.err = tw.Flush()
	}
	if len(cs) == 0 {
		p.printf("  (none)\n")
	}
	p.printf("\n")
}

// ErrGCPlanStale means a volume's HEAD is not where the report read it, so the report is
// about a bucket that no longer exists. Nothing is deleted; the answer is another report.
var ErrGCPlanStale = errors.New("controlplane: a HEAD moved after the report was taken, so the report no longer describes the bucket")

// GCResult is what one delete pass did. Deleted is what stopped answering; Bytes is what
// the bucket stops being billed for once the lifecycle policy expires the markers.
type GCResult struct {
	Deleted []GCCandidate
	Bytes   int64
}

// ApplyGC deletes the objects a plan named as candidates, and only those.
//
// The delete is the object store's reversible mark on every implementation (INV-14):
// removing the bytes is a bucket lifecycle policy this interface deliberately cannot
// reach, so being wrong here costs a Restore rather than a volume. That is what makes a
// delete a reasonable thing for this command to do at all, and it is not a licence to be
// wrong: a restore is a human noticing.
//
// One thing is re-read before anything goes, and it is HEAD.
//
// The report is a walk of a bucket that other processes keep writing, and exactly one
// change between the walk and the delete can turn a candidate into a commit that returned
// SUCCESS: an Agent killed between its manifest and its CAS republishes the *same* commit
// id — that is what makes a retry idempotent — and neither of its two objects is rewritten
// when it does. The layer is content-addressed and verified in place; the manifest is
// byte-identical and create-only. So their age never moves, a long enough outage makes the
// report correct about them, and the CAS landing a second later makes it wrong. Nothing
// about the objects can show it. What moved is HEAD.
//
// A HEAD that *appeared* is not that: a volume publishing its first commit reaches nothing
// the report saw, and with a volume per tenant, refusing on an appearing HEAD is a command
// that never runs.
//
// Manifests go before layers, so an interruption never leaves a manifest naming a layer
// that is gone — the torn shape a reader can see. The other order leaves an orphan layer,
// which is what this command exists to collect.
func ApplyGC(ctx context.Context, store objectstore.Store, plan GCPlan) (GCResult, error) {
	for volumeID, etag := range plan.heads {
		_, current, err := commit.ReadHead(ctx, store, volumeID)
		if err != nil {
			return GCResult{}, fmt.Errorf("%w: re-reading the HEAD of volume %s: %w", ErrGCPlanStale, volumeID, err)
		}
		if current != etag {
			return GCResult{}, fmt.Errorf("%w: volume %s was at %s and is now at %s",
				ErrGCPlanStale, volumeID, etag, current)
		}
	}

	var res GCResult
	for _, c := range manifestsFirst(plan.Candidates) {
		err := store.Delete(ctx, c.Key)
		if errors.Is(err, objectstore.ErrNotFound) {
			// Already gone, or already marked. Nothing to undo and nothing to report:
			// the object is in the state this pass wanted it in.
			continue
		}
		if err != nil {
			return res, fmt.Errorf("controlplane: deleting %s: %w", c.Key, err)
		}
		res.Deleted = append(res.Deleted, c)
		res.Bytes += c.SizeBytes
	}
	return res, nil
}

// manifestsFirst orders one pass: commit manifests, then layers. See ApplyGC.
func manifestsFirst(cs []GCCandidate) []GCCandidate {
	out := make([]GCCandidate, 0, len(cs))
	for _, c := range cs {
		if !strings.HasPrefix(c.Key, commit.LayerPrefix) {
			out = append(out, c)
		}
	}
	for _, c := range cs {
		if strings.HasPrefix(c.Key, commit.LayerPrefix) {
			out = append(out, c)
		}
	}
	return out
}

// Print writes what was deleted, in the same shape as the plan's own sections.
func (r GCResult) Print(w io.Writer) error {
	out := &gcPrinter{w: w}
	out.printf("deleted %d objects, %d bytes — the object store keeps the bytes behind a delete marker, so this is reversible with Restore until the bucket lifecycle expires them\n\n",
		len(r.Deleted), r.Bytes)
	out.section("DELETED", "unreachable for longer than the grace period", r.Deleted)
	return out.err
}
