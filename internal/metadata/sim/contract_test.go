package sim_test

import (
	"testing"

	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/metadata/metadatatest"
)

// TestSimStoreContract runs the shared metadata.Store contract against the
// deterministic in-memory store. Its twin is TestPGStoreContract in the pg
// adapter's integration lane: a DST proof that relies on the sim behaving one way
// is only a proof about production if both lanes are green.
func TestSimStoreContract(t *testing.T) {
	metadatatest.RunContract(t, func(t *testing.T) metadata.Store {
		return newStore()
	})
}
