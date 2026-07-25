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
	"strings"
	"time"

	"github.com/spin-stack/storage/internal/obs"
	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// refHolder captures the "objects" list shared by manifests and checkpoints.
type refHolder struct {
	Objects []string `json:"objects"`
}

// Reachable computes the set of reachable object keys: every structural object
// (descriptors, epoch objects, manifests, checkpoints, recovery-points, summaries)
// plus every WAL object a manifest or checkpoint references.
func Reachable(ctx context.Context, store objectstore.Store) (map[string]bool, error) {
	all, err := store.List(ctx, "")
	if err != nil {
		return nil, err
	}
	reachable := make(map[string]bool)
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
