package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/spin-stack/storage/internal/metadata"
	"github.com/spin-stack/storage/internal/simio/objectstore"
)

// ErrTermClaimExhausted means the elector could not find an unclaimed term within its
// attempt budget. It is a refusal, not a fallback: returning an unclaimed-but-unproven
// term is the fail-open shape ADR-0011 exists to remove.
var ErrTermClaimExhausted = errors.New("controlplane: no unclaimed term within the attempt budget")

// termClaimPrefix is where a term claim lives. It is not under volumes/ because it
// belongs to no volume, and it is structural, so the GC treats it as a root — a swept
// claim would restore precisely the incident the claim prevents.
const termClaimPrefix = "control-plane/terms/"

// maxTermClaimAttempts bounds the climb after a restore. Each attempt is one election
// (an UPDATE ... RETURNING) plus one create-only PUT, and the climb only has to cover
// the terms issued between the backup and now — elections are rare, so a budget this
// size is a runaway guard rather than a real limit.
const maxTermClaimAttempts = 1024

// TermClaim is the body of a term claim object: the durable statement that a term was
// issued, and to whom.
type TermClaim struct {
	Term      int64     `json:"term"`
	HolderID  string    `json:"holder_id"`
	ClaimedAt time.Time `json:"claimed_at"`
}

// TermClaimKey is the object key for a term's claim. The number is zero-padded so the
// keys sort numerically as strings: a later leader finds the high-water mark by
// listing, and a listing is lexicographic.
func TermClaimKey(term int64) string {
	return fmt.Sprintf("%s%020d", termClaimPrefix, term)
}

// ParseTermClaim decodes a claim body.
func ParseTermClaim(body []byte) (TermClaim, error) {
	var c TermClaim
	if err := json.Unmarshal(body, &c); err != nil {
		return TermClaim{}, fmt.Errorf("controlplane: decode term claim: %w", err)
	}
	return c, nil
}

// Elector hands out Control Plane terms that have never been issued before (ADR-0011).
//
// Every §7 mutation is guarded by the term, which makes a zombie affect 0 rows — but
// only while the term moves forward, and the term lives in one row of one database
// that operators restore. After a PITR, a failover to a lagging replica, or a
// disaster-recovery drill, control_plane_leader.term reads below what a live leader
// is using, and the next election hands that term out again: two processes then pass
// every guard, and neither is a zombie by any check the system has. The epoch CAS
// narrows the damage but does not restore single-writer — the two leaders take turns,
// each reading the other's writes as its own resumed work.
//
// So the object store, which already outlives PostgreSQL for the epoch objects (§12.4)
// and for rebuild-metadata (§22.5), is where a term becomes real: the elector claims
// it create-only before returning it, and a claim that already exists means the
// database was rewound. Terms are then unique for the life of the bucket rather than
// for the life of the current database, and the failure mode is a stall — a CP that
// cannot reach the object store does not become leader — which is the direction
// everything else here fails in.
type Elector struct {
	md    metadata.Store
	store objectstore.Store
}

// NewElector returns an Elector over the metadata store that issues terms and the
// object store that witnesses them.
func NewElector(md metadata.Store, store objectstore.Store) *Elector {
	return &Elector{md: md, store: store}
}

// Acquire takes leadership and returns a term that has never been issued.
//
// The loop is the recovery from a rewound database: each attempt takes the next term
// the database offers and tries to claim it, and an already-claimed term sends it
// round again — the database increments, so the climb terminates at the high-water
// mark of everything ever claimed. A caller that gets a term from here holds one no
// other process has ever held; a caller that gets an error holds nothing.
func (e *Elector) Acquire(ctx context.Context, holderID string) (int64, error) {
	for attempt := 0; attempt < maxTermClaimAttempts; attempt++ {
		term, err := e.md.AcquireLeadership(ctx, holderID)
		if err != nil {
			return 0, err
		}
		claimedAt, err := e.md.Now(ctx)
		if err != nil {
			return 0, fmt.Errorf("controlplane: reading the leadership clock: %w", err)
		}
		body, err := json.Marshal(TermClaim{Term: term, HolderID: holderID, ClaimedAt: claimedAt})
		if err != nil {
			return 0, err
		}
		_, err = e.store.Put(ctx, TermClaimKey(term), body, objectstore.PutOptions{IfNoneMatch: true})
		switch {
		case err == nil:
			return term, nil
		case errors.Is(err, objectstore.ErrPreconditionFailed):
			// This term was issued before — from a database state that has since been
			// restored. Whoever holds it may still be running, so it is not ours to
			// use. Try the next one.
			continue
		default:
			// Fail closed. A term nobody witnessed is a term a restored database can
			// hand out again, which is the whole failure this is here to prevent.
			return 0, fmt.Errorf("controlplane: claiming term %d: %w", term, err)
		}
	}
	return 0, fmt.Errorf("%w: %d attempts", ErrTermClaimExhausted, maxTermClaimAttempts)
}

// HighestClaimedTerm returns the largest term ever claimed, or 0 if none has been.
// It is what an operator (or a startup check) reads to compare the bucket's view of
// leadership against the database's — the two disagreeing is the signature of a
// restore.
func (e *Elector) HighestClaimedTerm(ctx context.Context) (int64, error) {
	infos, err := e.store.List(ctx, termClaimPrefix)
	if err != nil {
		return 0, err
	}
	var highest int64
	for _, info := range infos {
		body, err := e.store.Get(ctx, info.Key)
		if err != nil {
			return 0, fmt.Errorf("controlplane: reading term claim %s: %w", info.Key, err)
		}
		claim, err := ParseTermClaim(body)
		if err != nil {
			return 0, err
		}
		if claim.Term > highest {
			highest = claim.Term
		}
	}
	return highest, nil
}
