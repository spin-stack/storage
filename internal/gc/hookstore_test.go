package gc_test

import (
	"context"

	"github.com/spin-stack/storage/internal/simio/objectstore"
	"github.com/spin-stack/storage/internal/simio/sim"
)

// hookStore is a sim store with a seam on each operation, so a test can make the
// world change *between* two of the sweep's own calls — which is where the
// interesting GC failures live (a manifest published between LIST and LIST, an
// anchor retired between LIST and GET, a second sweep marking under this one).
type hookStore struct {
	*sim.ObjectStore
	onList   func(prefix string) error
	onGet    func(key string) error
	onDelete func(key string) error
}

var _ objectstore.Store = (*hookStore)(nil)

func hooked(s *sim.ObjectStore) *hookStore { return &hookStore{ObjectStore: s} }

func (h *hookStore) List(ctx context.Context, prefix string) ([]objectstore.ObjectInfo, error) {
	if h.onList != nil {
		if err := h.onList(prefix); err != nil {
			return nil, err
		}
	}
	return h.ObjectStore.List(ctx, prefix)
}

func (h *hookStore) Get(ctx context.Context, key string) ([]byte, error) {
	if h.onGet != nil {
		if err := h.onGet(key); err != nil {
			return nil, err
		}
	}
	return h.ObjectStore.Get(ctx, key)
}

func (h *hookStore) Delete(ctx context.Context, key string) error {
	if h.onDelete != nil {
		if err := h.onDelete(key); err != nil {
			return err
		}
	}
	return h.ObjectStore.Delete(ctx, key)
}
