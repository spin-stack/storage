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
	"sort"
	"strings"

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
