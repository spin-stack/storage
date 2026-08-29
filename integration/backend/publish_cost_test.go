//go:build integration

package backend_test

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"

	"github.com/spin-stack/storage/internal/commit"
	"github.com/spin-stack/storage/internal/crypto"
	"github.com/spin-stack/storage/internal/ids"
)

// TestMeasureWhatOneCommitCosts is the measurement v6 §11 asks for before a default can be
// chosen, and §23 makes Stage 2's exit criterion: "los defaults se fijan midiendo, no
// eligiendo".
//
// What it measures is the whole of §9's steps 9 to 13 through the real writer — seal,
// digest, upload, manifest, CAS — against whatever object store it is pointed at, over a
// range of layer sizes. Two numbers come out of the fit:
//
//	F — what a commit costs before a single byte of payload: two round trips for the
//	    manifest and the HEAD compare-and-set, and the read of the commit being built on.
//	R — the rate the payload moves at, which covers both passes over the layer, because
//	    both are what getting it into the bucket costs.
//
// A commit of S bytes then costs F + S/R, of which F is pure overhead, and the smallest
// threshold worth setting is the one where that overhead stops mattering. It is also what
// bounds the RPO: nothing can promise to be less than one commit behind.
//
// It is not in the gate and asserts nothing about the numbers. A measurement that fails a
// build is a threshold in disguise, and the number it would be asserting against is the
// one this test exists to discover. Run it with `task measure:publish`.
func TestMeasureWhatOneCommitCosts(t *testing.T) {
	if os.Getenv("MEASURE") == "" {
		t.Skip("set MEASURE=1 (or run `task measure:publish`): this is a measurement, not an assertion")
	}
	be := backendConfig(t)
	store := newVersionedS3Store(t, be, "measure-publish")
	ctx := t.Context()

	// The layer's plaintext is incompressible and identical across sizes, so nothing in
	// the path can be fast for a reason that would not hold for a guest's disk.
	payload := make([]byte, 256<<20)
	for i := range payload {
		payload[i] = byte(i*7 + i>>13)
	}

	sizes := []int64{0, 1 << 20, 4 << 20, 16 << 20, 64 << 20, 256 << 20}
	const rounds = 3

	type row struct {
		size int64
		best time.Duration
		rate float64
	}
	var rows []row
	for _, size := range sizes {
		best := time.Duration(1<<62 - 1)
		for range rounds {
			// A fresh volume per attempt: a commit's cost includes reading the one it
			// builds on, and reusing a volume would measure a deepening history.
			vol := ids.New().String()
			enc, err := crypto.NewEncryption(crypto.DEK{Key: [crypto.DEKSize]byte{5}, KeyID: 1}, [16]byte(uuid.MustParse(vol)))
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			_, err = commit.Publish(ctx, store, enc, bytes.NewReader(payload[:size]), commit.Request{
				VolumeID: vol, CommitID: ids.New().String(), LayerID: ids.New().String(),
				Epoch: 1, VirtualSize: 1 << 30,
			})
			if err != nil {
				t.Fatalf("publishing %d bytes: %v", size, err)
			}
			best = min(best, time.Since(start))
		}
		r := row{size: size, best: best}
		if size > 0 {
			r.rate = float64(size) / best.Seconds() / (1 << 20)
		}
		rows = append(rows, r)
	}

	// The fit, from the two ends: the empty commit is F, and the largest is F + S/R.
	fixed := rows[0].best
	largest := rows[len(rows)-1]
	rate := float64(largest.size) / (largest.best - fixed).Seconds() / (1 << 20)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintf(w, "\n=== what one commit costs, best of %d, against %s\n", rounds, be.Endpoint)
	_, _ = fmt.Fprintln(w, "LAYER\tPUBLISH\tMiB/s\tOVERHEAD")
	for _, r := range rows {
		over := "-"
		if r.size > 0 {
			over = fmt.Sprintf("%.0f%%", 100*fixed.Seconds()/r.best.Seconds())
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%.0f\t%s\n", capacity(r.size), r.best.Round(time.Millisecond), r.rate, over)
	}
	_, _ = fmt.Fprintf(w, "\nfixed cost per commit F = %s\n", fixed.Round(time.Millisecond))
	_, _ = fmt.Fprintf(w, "payload rate R = %.0f MiB/s (both passes over the layer)\n", rate)
	// The threshold at which the fixed cost is a tenth of the commit: F = 0.1(F + S/R),
	// so S = 9FR. Printed rather than hardcoded, because R is a property of the backend
	// this ran against and a deployment on another one gets another number.
	_, _ = fmt.Fprintf(w, "\nthreshold where the fixed cost is 10%% of a commit: %s\n",
		capacity(int64(9*fixed.Seconds()*rate*(1<<20))))
	_, _ = fmt.Fprintf(w, "one commit of that size takes %s, which is the floor under any RPO promised here\n",
		(10 * fixed).Round(time.Millisecond))
	_ = w.Flush()
}

// capacity renders a byte count the way -fleet-status does.
func capacity(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1fGiB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1fMiB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1fKiB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%dB", b)
	}
}
