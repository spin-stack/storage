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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spin-stack/storage/internal/ids"
	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/recovery"
	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/wal/format"
)

// refHolder captures the "objects" list shared by manifests and checkpoints.
type refHolder struct {
	Objects []string `json:"objects"`
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
	for _, info := range all {
		key := info.Key
		isWALObject := strings.HasPrefix(key, "wal/") && strings.HasSuffix(key, ".wal")
		if !isWALObject {
			// Structural metadata is always reachable.
			reachable[key] = true
		}
		// Manifests and checkpoints anchor their referenced WAL objects.
		if strings.HasSuffix(key, "manifest.json") || strings.HasPrefix(key, "checkpoints/") {
			body, err := store.Get(ctx, key)
			if err != nil {
				return nil, err
			}
			var r refHolder
			if err := json.Unmarshal(body, &r); err == nil {
				for _, o := range r.Objects {
					reachable[o] = true
				}
			}
		}
	}
	return reachable, nil
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
// second pass reports nothing new.
func Mark(ctx context.Context, store objectstore.Store, clk clock.Clock, reachable map[string]bool, grace time.Duration) ([]string, error) {
	return MarkWithRecorder(ctx, store, clk, reachable, grace, nil)
}

// MarkWithRecorder is Mark with the §26.2 telemetry it owns: gc_marked_bytes_total
// and orphan_objects_total, recorded where the decision happens (DEV-0010).
func MarkWithRecorder(ctx context.Context, store objectstore.Store, clk clock.Clock, reachable map[string]bool, grace time.Duration, rec *obs.Recorder) ([]string, error) {
	all, err := store.List(ctx, "")
	if err != nil {
		return nil, err
	}
	now := clk.Wall()
	var marked []string
	for _, info := range all {
		if reachable[info.Key] {
			continue
		}
		if grace > 0 && now.Sub(info.LastModified) < grace {
			continue // too young to judge: a publication may still be in flight
		}
		if err := store.Delete(ctx, info.Key); err != nil {
			return marked, fmt.Errorf("gc: mark %s: %w", info.Key, err)
		}
		marked = append(marked, info.Key)
		rec.Count(ctx, "gc_marked_bytes_total", info.Size)
	}
	rec.Gauge(ctx, "orphan_objects_total", float64(len(marked)))
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
