package wal

// OrderPolicy decides the three moves that INV-03 (§5.6, published <= durable <=
// local) and INV-13 (§21.1, never truncate above the verified published point) are
// made of. The Log consults it on every one of them; StrictOrder is the default and
// is those rules verbatim.
//
// It exists because those two invariants are the only ones in this package that no
// injected fault can reach. Everything else the Log can get wrong is downstream of
// I/O the simulator controls — a torn append, a lost fdatasync, a full device, a
// throttled backend — but the ordering rules are three comparisons over numbers held
// in memory, and no disk or object-store fault changes their answer. The DST checkers
// for both therefore had nothing to catch, and were proven only against a fabricated
// event (`proofLiteral` in the harness). A scenario substitutes a policy and drives
// the real AdvanceDurable/AdvancePublished/TruncateLocal into the violation.
//
// Two properties make this a seam rather than a hole. The production path calls the
// same method the harness replaces, so there is no branch that exists only for tests
// and nothing to keep alive on the data path; and there is no way to *lose* the rules
// by omission — an unconfigured Log is strict, and SetOrderPolicy(nil) restores
// strictness rather than disabling it. Substituting a permissive policy in production
// would be as visible as deleting the checks, which is the point: it has to be a
// deliberate act, in one named place.
type OrderPolicy interface {
	// AllowDurable reports whether the durable watermark may move to seq, given the
	// watermarks as they are now.
	AllowDurable(seq uint64, w Watermarks) error
	// AllowPublished reports whether the published watermark may move to seq.
	AllowPublished(seq uint64, w Watermarks) error
	// AllowTruncate reports whether local WAL may be reclaimed up to and including
	// upTo.
	AllowTruncate(upTo uint64, w Watermarks) error
}

// StrictOrder is the production policy: §5.6's ordering and §21.1's truncation floor,
// with no exceptions. It is a zero-size value so it can be embedded by a harness
// policy that wants to relax exactly one rule and keep the other two.
type StrictOrder struct{}

// AllowDurable enforces published <= durable <= local (§5.6).
func (StrictOrder) AllowDurable(seq uint64, w Watermarks) error {
	if seq > w.Local || seq < w.Published {
		return ErrWatermarkOrder
	}
	return nil
}

// AllowPublished enforces published <= durable (§5.6) and, just as load-bearing,
// that published never moves backwards.
//
// The floor is not bookkeeping. TruncateLocal has already discarded the records below
// the published point: they live in a verified checkpoint and nowhere else on this
// host (INV-13). A publisher that recomputes the point from a listing — which is what
// checkpoint.Create does — gets a smaller answer whenever that listing is momentarily
// behind (§24), and accepting it re-opens a range whose bytes are gone, with no error
// anywhere. Refusing it costs nothing: the checkpoint that proved the higher point is
// immutable, so a lower reading is never news.
func (StrictOrder) AllowPublished(seq uint64, w Watermarks) error {
	if seq > w.Durable || seq < w.Published {
		return ErrWatermarkOrder
	}
	return nil
}

// AllowTruncate enforces INV-13: records not yet inside a published, verified
// checkpoint exist on this host only, so discarding them destroys them.
func (StrictOrder) AllowTruncate(upTo uint64, w Watermarks) error {
	if upTo > w.Published {
		return ErrTruncateAboveDurable
	}
	return nil
}

// SetOrderPolicy replaces the policy behind the watermark and truncation rules.
// Passing nil restores StrictOrder.
//
// This is for the DST harness. Production has exactly one correct argument, and it is
// the default.
func (l *Log) SetOrderPolicy(p OrderPolicy) {
	if p == nil {
		p = StrictOrder{}
	}
	l.order = p
}

// orderPolicy returns the policy in force, defaulting to strict for a Log built
// without one (the zero value must not be a way to lose the invariants).
func (l *Log) orderPolicy() OrderPolicy {
	if l.order == nil {
		return StrictOrder{}
	}
	return l.order
}
