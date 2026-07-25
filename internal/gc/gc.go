// Package gc implements safe garbage collection (§21.3, §5.11): the GC only MARKS
// unreachable objects; it never issues a permanent delete. The actual removal is the
// object store's lifecycle sweeping non-current versions after a grace period, on a
// versioned bucket where a delete marker is reversible. Because the GC cannot delete,
// a GC bug cannot cause the worst incident (INV-14). Reachability is computed from
// the self-describing S3 layout, so a live object is never marked.
package gc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spin-stack/storage/internal/checkpoint"
	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/snapshot"
	"github.com/spin-stack/storage/internal/wal/format"
)

// Sentinel errors. Both mean the same thing operationally: the sweep refused to
// judge, and nothing was marked. That is always the safe direction — an object that
// is collected one cycle late costs storage, an object collected one cycle early
// costs data (INV-14).
var (
	// ErrUnreadableAnchor means a manifest or checkpoint could not be parsed, does
	// not describe itself (its RootDigest disagrees with its contents), or names a
	// parent snapshot whose manifest is gone. An anchor we cannot read is a reason
	// to stop, never a reason to widen the sweep.
	ErrUnreadableAnchor = errors.New("gc: unreadable anchor")
	// ErrClockSkew means object timestamps are ahead of the GC's clock, so the two
	// clocks disagree and the grace period cannot be evaluated.
	ErrClockSkew = errors.New("gc: object timestamps are ahead of the GC clock")
)

// MaxClockSkew is how far an object's LastModified may sit in the future of the GC's
// own clock before the sweep refuses to judge ages. The two timestamps come from two
// machines: a little skew is normal, a lot means the grace period is meaningless.
const MaxClockSkew = 5 * time.Second

// anchor is a manifest or checkpoint: the two objects that keep WAL objects alive by
// naming them. Both are self-describing — RootDigest is computed over (sequence,
// object keys) — which is what lets the sweep tell a real anchor from a half-written
// one without asking anybody.
type anchor struct {
	objects []string
	// parent is the snapshot this one chains from, "" for a root snapshot.
	parent string
	// volume owns the parent lineage: a parent manifest lives under the same volume.
	volume string
}

// readAnchor decodes and validates the anchor at key. Anything it cannot fully
// verify is ErrUnreadableAnchor: the sweep's only safe response to an anchor it
// cannot read is to stop, because "anchors nothing" and "could not be read" are
// indistinguishable to the mark phase and one of them destroys a snapshot.
func readAnchor(key string, body []byte) (anchor, error) {
	if strings.HasPrefix(key, "checkpoints/") {
		var cp checkpoint.Checkpoint
		if err := json.Unmarshal(body, &cp); err != nil {
			return anchor{}, fmt.Errorf("%w: %s: %w", ErrUnreadableAnchor, key, err)
		}
		if !cp.DigestMatches() {
			return anchor{}, fmt.Errorf("%w: %s: root digest does not match its contents", ErrUnreadableAnchor, key)
		}
		return anchor{objects: cp.Objects, volume: cp.VolumeID}, nil
	}
	var m snapshot.Manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return anchor{}, fmt.Errorf("%w: %s: %w", ErrUnreadableAnchor, key, err)
	}
	if !m.DigestMatches() {
		return anchor{}, fmt.Errorf("%w: %s: root digest does not match its contents", ErrUnreadableAnchor, key)
	}
	return anchor{objects: m.Objects, parent: m.ParentSnapshotID, volume: m.VolumeID}, nil
}

// Reachable computes the set of reachable object keys: every structural object
// (descriptors, epoch objects, manifests, checkpoints, recovery-points, summaries),
// every WAL object a manifest or checkpoint references, and — the root that matters
// most — every WAL object inside a volume/epoch's **durable prefix**.
//
// The durable prefix is a root by definition: §5.8 makes it the authority for what a
// volume contains, so an object within it is live whether or not anything enumerates
// it. Without this, every ACKed write since the last checkpoint reads as an orphan —
// and for a volume that has never checkpointed, the entire log does — so a scheduled
// sweep would delete-marker the durable prefix under a live writer and recovery would
// then find nothing. That is the incident INV-14 promises the GC cannot cause.
func Reachable(ctx context.Context, store objectstore.Store) (map[string]bool, error) {
	all, err := store.List(ctx, "")
	if err != nil {
		return nil, err
	}
	reachable := make(map[string]bool)
	if err := addDurablePrefixes(ctx, store, all, reachable); err != nil {
		return nil, err
	}
	present := make(map[string]bool, len(all))
	for _, info := range all {
		present[info.Key] = true
	}
	var lineage []anchor
	for _, info := range all {
		key := info.Key
		isWALObject := strings.HasPrefix(key, "wal/") && strings.HasSuffix(key, ".wal")
		if !isWALObject {
			// Structural metadata is always reachable.
			reachable[key] = true
		}
		if !isAnchor(key) {
			continue
		}
		// Manifests and checkpoints anchor their referenced WAL objects. A GET that
		// fails — including the anchor being retired between the LIST and here — is
		// a reason to abort: the alternative is to treat it as anchoring nothing.
		body, err := store.Get(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %w", ErrUnreadableAnchor, key, err)
		}
		a, err := readAnchor(key, body)
		if err != nil {
			return nil, err
		}
		for _, o := range a.objects {
			reachable[o] = true
		}
		if a.parent != "" {
			lineage = append(lineage, a)
		}
	}
	// A snapshot chains to its parent, and it is the *parent* manifest that
	// enumerates the parent's objects. If the parent is gone — a cost-control
	// lifecycle rule on old manifests, an operator expiring a snapshot — this sweep
	// can no longer know what the parent anchored, and marking that difference
	// destroys the child. Stop instead (finding 9).
	for _, a := range lineage {
		if parentKey := snapshot.ManifestKey(a.volume, a.parent); !present[parentKey] {
			return nil, fmt.Errorf("%w: parent snapshot manifest %s is gone", ErrUnreadableAnchor, parentKey)
		}
	}
	return reachable, nil
}

// isAnchor reports whether key names a manifest or a checkpoint.
func isAnchor(key string) bool {
	return strings.HasSuffix(key, "manifest.json") || strings.HasPrefix(key, "checkpoints/")
}

// Collect returns the keys the GC would mark: everything not in reachable. It never
// deletes — it only computes marks (INV-14). The caller records the marks (tags /
// delete markers) and the bucket lifecycle sweeps them after the grace period.
func Collect(ctx context.Context, store objectstore.Store, reachable map[string]bool) ([]string, error) {
	all, err := store.List(ctx, "")
	if err != nil {
		return nil, err
	}
	var marks []string
	for _, info := range all {
		if !reachable[info.Key] {
			marks = append(marks, info.Key)
		}
	}
	sort.Strings(marks)
	return marks, nil
}

// Mark is the GC (§21.3): it computes what is unreachable, skips anything younger
// than grace, and places a reversible delete marker over the rest. It returns the
// keys it marked.
//
// Three properties make this safe to run on a schedule against live data:
//
//   - it can only mark. The store interface has no permanent delete, so a
//     reachability bug costs a restore, not the data (INV-14);
//   - a reachable object is never marked — reachability is computed from the
//     self-describing layout, not from a cached index;
//   - an object younger than grace is left alone, because it may belong to a
//     manifest that is still being published (§21.1 publishes objects before the
//     manifest that anchors them).
//
// Marking is idempotent: an already-marked object is invisible to the scan, so a
// second pass reports nothing new, and a key another sweep marked underneath this one
// is not an error.
func Mark(ctx context.Context, store objectstore.Store, clk clock.Clock, reachable map[string]bool, grace time.Duration) ([]string, error) {
	return MarkWithRecorder(ctx, store, clk, reachable, grace, nil)
}

// MarkWithRecorder is Mark with the §26.2 telemetry it owns: gc_marked_bytes_total
// and orphan_objects_total, recorded where the decision happens (DEV-0010).
//
// The order of the phases is the safety argument:
//
//  1. LIST, then refuse outright if the object timestamps sit ahead of our clock —
//     the ages the grace period is computed from would be meaningless (ErrClockSkew);
//  2. select the candidates: unreachable *and* aged past grace;
//  3. re-derive reachability immediately before marking, and drop anything that has
//     become reachable since the caller computed its set. §21.1 publishes the
//     objects before the manifest that anchors them, so a publication landing during
//     the sweep is exactly the case where the caller's set is stale;
//  4. only then place the delete markers.
func MarkWithRecorder(ctx context.Context, store objectstore.Store, clk clock.Clock, reachable map[string]bool, grace time.Duration, rec *obs.Recorder) ([]string, error) {
	all, err := store.List(ctx, "")
	if err != nil {
		return nil, err
	}
	now := clk.Wall()
	var candidates []objectstore.ObjectInfo
	for _, info := range all {
		// An object stamped in our future means the GC's clock and the backend's
		// disagree (or ours walked backwards). Every age then reads negative, every
		// object looks protected forever, and the bucket grows with nothing to show
		// for it. Say so; a silent no-op is not a signal (finding 4).
		if info.LastModified.Sub(now) > MaxClockSkew {
			return nil, fmt.Errorf("%w: %s is stamped %s, now is %s",
				ErrClockSkew, info.Key, info.LastModified, now)
		}
		if reachable[info.Key] {
			continue
		}
		if grace > 0 && now.Sub(info.LastModified) < grace {
			continue // too young to judge: a publication may still be in flight
		}
		candidates = append(candidates, info)
	}
	// The orphan population this pass observed — recorded even when the pass dies
	// part-way, because a GC that is stuck finding the same orphans and failing to
	// mark them is precisely what the gauge exists to show.
	defer rec.Gauge(ctx, "orphan_objects_total", float64(len(candidates)))

	var marked []string
	if len(candidates) == 0 {
		return nil, nil
	}
	confirmed, err := Reachable(ctx, store)
	if err != nil {
		return nil, err
	}
	for _, info := range candidates {
		if confirmed[info.Key] {
			continue // anchored while this sweep was running (§21.1)
		}
		if err := store.Delete(ctx, info.Key); err != nil {
			if errors.Is(err, objectstore.ErrNotFound) {
				// Another sweep marked it first (an overrunning cron, an operator
				// running the sweep by hand). Already in the intended state.
				continue
			}
			sort.Strings(marked)
			return marked, fmt.Errorf("gc: mark %s: %w", info.Key, err)
		}
		marked = append(marked, info.Key)
		rec.Count(ctx, "gc_marked_bytes_total", info.Size)
	}
	sort.Strings(marked)
	return marked, nil
}

// addDurablePrefixes marks every WAL object that is part of some volume/epoch's
// durable prefix. The epochs are discovered from the keyspace itself (the layout is
// self-describing, §22.5), so this needs no Control-Plane input — which matters,
// because a GC that depended on PostgreSQL being right about epochs would delete data
// whenever PostgreSQL was wrong.
func addDurablePrefixes(ctx context.Context, store objectstore.Store, all []objectstore.ObjectInfo, reachable map[string]bool) error {
	type volEpoch struct {
		vol   [16]byte
		epoch uint64
	}
	seen := map[volEpoch]bool{}
	for _, info := range all {
		if !strings.HasPrefix(info.Key, "wal/") || !strings.HasSuffix(info.Key, ".wal") {
			continue
		}
		parts := strings.Split(info.Key, "/")
		if len(parts) < 4 {
			continue
		}
		parsed, err := ids.Parse(parts[1])
		if err != nil {
			continue // not one of ours; the sweep leaves it to the reachability rules
		}
		epoch, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			continue
		}
		seen[volEpoch{vol: [16]byte(parsed), epoch: epoch}] = true
	}

	for ve := range seen {
		durable, err := recovery.DurablePoint(ctx, store, ve.vol, ve.epoch)
		if err != nil {
			// If the durable point cannot be established, nothing here may be
			// collected: an unreadable prefix is a reason to stop, not to sweep.
			return fmt.Errorf("gc: durable point for %s/%d: %w", format.UUIDString(ve.vol), ve.epoch, err)
		}
		keys, err := recovery.ObjectKeysUpTo(ctx, store, ve.vol, ve.epoch, durable)
		if err != nil {
			return err
		}
		for _, k := range keys {
			reachable[k] = true
		}
	}
	return nil
}
