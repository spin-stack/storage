package wal_test

import (
	"errors"
	"testing"

	"github.com/spin-stack/storage/internal/wal"
)

// Audit finding, 2026-07-25: "`watermark-order` and `no-truncate-above-published`
// need a fault seam in internal/wal (`Log` enforces both internally and no simulated
// I/O reaches the check)."
//
// Both checkers are pinned as `proofLiteral` in the DST harness: the planted-bug test
// emits the event by hand, so it proves the checker reads the field and nothing more.
// A checker that has never seen production code violate its invariant is a checker
// nobody has tested.
//
// The reason no I/O fault can reach them is structural, not accidental. INV-03 and
// INV-13 are decided by three comparisons on numbers the Log holds in memory; a torn
// write, a lost sync, a full device and a throttled backend all fail *before* those
// comparisons and none of them can change the answer. Inverting the behaviour needs
// the decision to be replaceable.
//
// The seam is OrderPolicy: the rules move behind an interface the Log already
// consults on every one of those three paths, with StrictOrder — the §5.6/§21.1 rules
// verbatim — as the default. There is no extra branch on the data path and no code
// that exists only for tests: the production path calls the same method the harness
// substitutes.

// permissive is what a DST scenario substitutes to break the invariant through the
// real code path.
type permissive struct{}

func (permissive) AllowDurable(uint64, wal.Watermarks) error   { return nil }
func (permissive) AllowPublished(uint64, wal.Watermarks) error { return nil }
func (permissive) AllowTruncate(uint64, wal.Watermarks) error  { return nil }

// TestStrictOrderIsTheDefault: the seam must not be a way to get the rules turned off
// by forgetting something. A log nobody configured enforces §5.6 and §21.1, and
// passing nil restores that rather than disabling it.
func TestStrictOrderIsTheDefault(t *testing.T) {
	tests := []struct {
		name    string
		drive   func(l *wal.Log) error
		wantErr error
	}{
		{
			name:    "durable above local",
			drive:   func(l *wal.Log) error { return l.AdvanceDurable(9) },
			wantErr: wal.ErrWatermarkOrder,
		},
		{
			name: "durable below published",
			drive: func(l *wal.Log) error {
				if err := l.AdvanceDurable(2); err != nil {
					return err
				}
				if err := l.AdvancePublished(2); err != nil {
					return err
				}
				return l.AdvanceDurable(1)
			},
			wantErr: wal.ErrWatermarkOrder,
		},
		{
			name:    "published above durable",
			drive:   func(l *wal.Log) error { return l.AdvancePublished(1) },
			wantErr: wal.ErrWatermarkOrder,
		},
		{
			name:    "truncate above published",
			drive:   func(l *wal.Log) error { return l.TruncateLocal(2) },
			wantErr: wal.ErrTruncateAboveDurable,
		},
	}
	for _, tc := range tests {
		for _, policy := range []struct {
			name string
			set  func(l *wal.Log)
		}{
			{"unconfigured", func(*wal.Log) {}},
			{"explicit StrictOrder", func(l *wal.Log) { l.SetOrderPolicy(wal.StrictOrder{}) }},
			{"nil restores strict", func(l *wal.Log) { l.SetOrderPolicy(permissive{}); l.SetOrderPolicy(nil) }},
		} {
			t.Run(tc.name+"/"+policy.name, func(t *testing.T) {
				l, _, _ := newLog(t, wal.Limits{MaxUnflushedBytes: 1 << 20})
				for i := range 3 {
					if _, err := l.Write(uint64(i)*64, []byte("data"), 0); err != nil {
						t.Fatal(err)
					}
				}
				policy.set(l)
				if err := tc.drive(l); !errors.Is(err, tc.wantErr) {
					t.Fatalf("got %v, want %v", err, tc.wantErr)
				}
			})
		}
	}
}

// TestASubstitutedPolicyCanBreakWatermarkOrder is the seam doing its job for INV-03:
// with the rule replaced, the real AdvancePublished produces a watermark trio the
// invariant forbids, and Watermarks() — what the DST scenario reads to emit its event
// — reports it. Without this the `watermark-order` checker can only be shown a
// fabricated event.
func TestASubstitutedPolicyCanBreakWatermarkOrder(t *testing.T) {
	l, _, _ := newLog(t, wal.Limits{MaxUnflushedBytes: 1 << 20})
	for i := range 3 {
		if _, err := l.Write(uint64(i)*64, []byte("data"), 0); err != nil {
			t.Fatal(err)
		}
	}
	l.SetOrderPolicy(permissive{})

	if err := l.AdvancePublished(9); err != nil {
		t.Fatalf("the substituted policy was not consulted: %v", err)
	}
	w := l.Watermarks()
	if w.Published <= w.Durable {
		t.Fatalf("watermarks %+v still satisfy published <= durable; the seam changed nothing", w)
	}
	if err := l.AdvanceDurable(50); err != nil {
		t.Fatalf("the substituted policy was not consulted on durable: %v", err)
	}
	if w := l.Watermarks(); w.Durable <= w.Local {
		t.Fatalf("watermarks %+v still satisfy durable <= local", w)
	}
}

// TestASubstitutedPolicyCanTruncateAbovePublished is the same for INV-13: the
// truncation actually happens — segments are unlinked and their records are gone — so
// what the checker sees is data loss, not a number.
//
// It takes two segments to stage that. Reclamation never touches the newest segment,
// so a log with one segment loses nothing however permissive the policy is; the seam
// only proves something when there is an older segment holding records above the
// published point, which is exactly the shape a real violation has.
func TestASubstitutedPolicyCanTruncateAbovePublished(t *testing.T) {
	l, _, d := newLog(t, wal.Limits{MaxUnflushedBytes: 1 << 20})
	for i := range 3 {
		if _, err := l.Write(uint64(i)*64, []byte("data"), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Seal(); err != nil { // sequences 1..3 are now in a sealed segment
		t.Fatal(err)
	}
	if _, err := l.Write(1<<20, []byte("newest"), 0); err != nil {
		t.Fatal(err)
	}
	before := walSize(t, l)
	if len(l.SegmentNames()) != 2 {
		t.Fatalf("staged %d segments, want 2", len(l.SegmentNames()))
	}

	l.SetOrderPolicy(permissive{})
	if err := l.TruncateLocal(50); err != nil {
		t.Fatalf("the substituted policy was not consulted: %v", err)
	}
	if got := l.TruncatedUpTo(); got != 50 {
		t.Fatalf("TruncatedUpTo = %d, want 50", got)
	}
	if l.TruncatedUpTo() <= l.Watermarks().Published {
		t.Fatalf("truncated to %d with published %d: the invariant is not broken",
			l.TruncatedUpTo(), l.Watermarks().Published)
	}
	after := walSize(t, l)
	if after >= before {
		t.Fatalf("the WAL still holds %d of %d bytes; records above the published point survived, so no data was lost and the seam proves nothing", after, before)
	}
	recs, err := wal.ReplaySegments(d, "wal", [16]byte{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if r.Sequence <= 3 {
			t.Fatalf("sequence %d survived a truncation that discarded its segment", r.Sequence)
		}
	}
}

// TestStrictOrderIsUsableStandalone: the harness substitutes a policy that is strict
// about everything except the one rule under test, so StrictOrder has to be callable
// as a value and not only as the Log's hidden default.
func TestStrictOrderIsUsableStandalone(t *testing.T) {
	var p wal.OrderPolicy = wal.StrictOrder{}
	w := wal.Watermarks{Local: 10, Durable: 5, Published: 3}

	if err := p.AllowDurable(7, w); err != nil {
		t.Fatalf("durable 7 within [3,10]: %v", err)
	}
	if err := p.AllowDurable(11, w); !errors.Is(err, wal.ErrWatermarkOrder) {
		t.Fatalf("durable above local: %v", err)
	}
	if err := p.AllowDurable(2, w); !errors.Is(err, wal.ErrWatermarkOrder) {
		t.Fatalf("durable below published: %v", err)
	}
	if err := p.AllowPublished(5, w); err != nil {
		t.Fatalf("published up to durable: %v", err)
	}
	if err := p.AllowPublished(6, w); !errors.Is(err, wal.ErrWatermarkOrder) {
		t.Fatalf("published above durable: %v", err)
	}
	if err := p.AllowTruncate(3, w); err != nil {
		t.Fatalf("truncate at the published point: %v", err)
	}
	if err := p.AllowTruncate(4, w); !errors.Is(err, wal.ErrTruncateAboveDurable) {
		t.Fatalf("truncate above the published point: %v", err)
	}
}

// TestTheZeroValueLogIsStrict: the Log's zero value must not be a way to lose the
// invariants either. Nothing constructs one today, but a struct literal added in a
// future refactor would silently arrive with a nil policy.
func TestTheZeroValueLogIsStrict(t *testing.T) {
	var l wal.Log
	if err := l.AdvanceDurable(1); !errors.Is(err, wal.ErrWatermarkOrder) {
		t.Fatalf("durable 1 on an empty log = %v, want ErrWatermarkOrder", err)
	}
	if err := l.AdvancePublished(1); !errors.Is(err, wal.ErrWatermarkOrder) {
		t.Fatalf("published 1 on an empty log = %v, want ErrWatermarkOrder", err)
	}
	if err := l.TruncateLocal(1); !errors.Is(err, wal.ErrTruncateAboveDurable) {
		t.Fatalf("truncate to 1 on an empty log = %v, want ErrTruncateAboveDurable", err)
	}
	if got := l.Degraded(); got != wal.DegradedNone {
		t.Fatalf("the zero value reports %q, want %q", got, wal.DegradedNone)
	}
	if got := wal.Degradation("").String(); got != "NONE" {
		t.Fatalf("the zero Degradation renders as %q, want \"NONE\"", got)
	}
	if got := wal.DegradedOutOfSpace.String(); got != "OUT_OF_SPACE" {
		t.Fatalf("DegradedOutOfSpace renders as %q", got)
	}
}
