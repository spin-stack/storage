package wal

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/spin-stack/storage/internal/simio/clock"
	"github.com/spin-stack/storage/internal/simio/disk"
	"github.com/spin-stack/storage/internal/wal/format"
)

// SegmentBytes is the size at which a segment is sealed and the next one started.
//
// 32 MiB. The number does not bound what a crash leaves unsynced — MaxUnflushedBytes
// does that, because we fdatasync per FLUSH. It governs three things: how coarsely
// TruncateLocal can reclaim (the segment holding the published point is never
// unlinked, so one segment per volume is always immobilised), how many descriptors and
// directory entries a host carries, and how often the directory churns.
//
// That makes the cost scale with volume count, not volume size: 100 volumes hold up to
// 3.2 GiB the checkpoint cannot yet reclaim. The reference points with the same shape
// — an append-only segmented log reclaimed by segment — are PostgreSQL's WAL at 16 MB,
// the Cassandra/Scylla commitlog at 32 MB, etcd's WAL at 64 MB and TiKV's raft-engine
// at 128 MB; the commitlog is the closest analogue (many independent streams, a
// segment set per stream, reclamation tied to a progress point) and it uses 32 MB.
const SegmentBytes int64 = 32 << 20

// segmentSuffix is the extension every segment file carries.
const segmentSuffix = ".seg"

// segmentNameDigits is how wide a segment's name is zero-padded. 18 digits keeps
// lexicographic order equal to numeric order, which is what makes the directory
// listing the index. A volume would have to accept 10^18 records in one epoch to
// outgrow it.
const segmentNameDigits = 18

// ErrSegmentGap is returned when the segments in a WAL directory do not form one
// contiguous run of sequences: a segment starts above where the previous one ended, so
// a file between them is missing.
//
// It is a hard error, not a torn tail. It is the local twin of the prefix floor in
// recovery, where a missing object ends the contiguous run rather than being skipped:
// the records in the hole exist nowhere, and continuing past it would apply a later
// state on top of a volume that never reached the earlier one.
var ErrSegmentGap = errors.New("wal: a segment is missing from the WAL directory")

// ErrTornSegment is returned when a segment that is not the newest ends in a partial
// record. Only the newest segment is ever open for append, so only the newest can be
// caught mid-write by a crash; a tear anywhere else is corruption of a file nothing
// should have been writing to.
var ErrTornSegment = errors.New("wal: a sealed segment ends in a partial record")

// ErrCorruptSegment is returned when a segment file cannot be trusted for a reason
// that is not a torn tail: an unreadable header, a name that disagrees with the header
// it contains, or a first record that is not the one the header announces.
var ErrCorruptSegment = errors.New("wal: corrupt WAL segment")

// SegmentDir is the directory holding one volume's WAL segments for one epoch:
// <root>/<volume-id>/<epoch>. The epoch is in the path — mirroring the S3 key layout
// — so a foreign-epoch directory is impossible rather than merely detected, and "which
// epochs still have local segments" is a listing. The header repeats both: the path
// says where the directory was filed, the header says what it is, and a restored
// directory can disagree with either.
func SegmentDir(root string, volumeID [16]byte, epoch uint64) string {
	return fmt.Sprintf("%s/%s/%d", root, format.UUIDString(volumeID), epoch)
}

// segmentName is the file name of the segment whose first record is `first`.
func segmentName(first uint64) string {
	return fmt.Sprintf("%0*d%s", segmentNameDigits, first, segmentSuffix)
}

// parseSegmentName recovers the first sequence a segment file name encodes.
func parseSegmentName(base string) (uint64, error) {
	digits, ok := strings.CutSuffix(base, segmentSuffix)
	if !ok || len(digits) != segmentNameDigits {
		return 0, fmt.Errorf("%w: %q is not a segment file name", ErrCorruptSegment, base)
	}
	first, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %q is not a segment file name: %w", ErrCorruptSegment, base, err)
	}
	return first, nil
}

// SegmentFiles lists the segment files of one volume's WAL directory, oldest first.
// The names are disk names, usable with Disk.Open — which is what a test or a
// diagnostic wants; the WAL itself goes through the segments type below.
func SegmentFiles(d disk.Disk, root string, volumeID [16]byte, epoch uint64) ([]string, error) {
	return listSegments(d, SegmentDir(root, volumeID, epoch))
}

// listSegments returns dir's entries in name order, which is first-sequence order, and
// refuses anything else in the directory. This is our directory: a file in it that is
// not a segment is either a bug in a path or something else writing here, and both are
// worth failing on rather than stepping over.
func listSegments(d disk.Disk, dir string) ([]string, error) {
	names, err := d.List(dir + "/")
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		if _, err := parseSegmentName(strings.TrimPrefix(name, dir+"/")); err != nil {
			return nil, err
		}
	}
	return names, nil
}

// segmentInfo is one segment as replay found it on disk.
type segmentInfo struct {
	name  string
	first uint64
	size  int64
}

// scanResult is the state of a whole WAL directory: every record it holds, in order,
// plus what replay learned about the newest segment — which is the only one allowed to
// be mid-write.
type scanResult struct {
	segs    []segmentInfo
	records []Record
	// cleanLen is the byte length of the newest segment's intact prefix. It is below
	// its size exactly when a crash tore the last append; a resumed log truncates to
	// it, so the next record does not land after a hole that would stop replay.
	cleanLen int64
	torn     bool
	// stub is set when the newest segment is too short to hold even a header: the
	// crash landed between creating the file and writing its header. It carries no
	// records, so a resumed log removes it.
	stub bool
}

// scanSegments replays a whole WAL directory.
//
// It is total in the same sense Replay is (INV-05): the normal crash case — a torn
// last record in the newest segment — returns the intact prefix with no error, and
// everything else that could make the returned records differ from what was written is
// an error rather than a shorter answer.
func scanSegments(d disk.Disk, dir string, volumeID [16]byte, epoch uint64) (scanResult, error) {
	var out scanResult
	names, err := listSegments(d, dir)
	if err != nil {
		return out, err
	}
	var expectedNext uint64 // 0 = the first segment, which may start anywhere
	for i, name := range names {
		last := i == len(names)-1
		first, err := parseSegmentName(strings.TrimPrefix(name, dir+"/"))
		if err != nil {
			return out, err
		}
		buf, err := readWholeFile(d, name)
		if err != nil {
			return out, err
		}
		if len(buf) < format.SegmentHeaderSize {
			// The crash landed between creating the file and its header reaching the
			// device. A header that is not all there cannot carry records, so this is
			// the newest segment or nothing.
			if !last {
				return out, fmt.Errorf("%w: %s holds %d bytes, less than a header", ErrCorruptSegment, name, len(buf))
			}
			out.segs = append(out.segs, segmentInfo{name: name, first: first, size: int64(len(buf))})
			out.stub, out.cleanLen = true, 0
			continue
		}
		h, err := format.UnmarshalSegmentHeader(buf)
		if err != nil {
			return out, fmt.Errorf("%w: %s: %w", ErrCorruptSegment, name, err)
		}
		if h.VolumeID != volumeID {
			return out, fmt.Errorf("%w: %s belongs to volume %s, opening %s",
				ErrForeignVolume, name, format.UUIDString(h.VolumeID), format.UUIDString(volumeID))
		}
		if h.Epoch != epoch {
			return out, fmt.Errorf("%w: %s belongs to epoch %d, opening %d", ErrForeignEpoch, name, h.Epoch, epoch)
		}
		if h.FirstSequence != first {
			return out, fmt.Errorf("%w: %s announces first sequence %d", ErrCorruptSegment, name, h.FirstSequence)
		}

		body := buf[format.SegmentHeaderSize:]
		recs, consumed, err := replayPrefix(body)
		if err != nil {
			return out, fmt.Errorf("%s: %w", name, err)
		}
		if consumed < len(body) && !last {
			return out, fmt.Errorf("%w: %s (%d of %d body bytes decode)", ErrTornSegment, name, consumed, len(body))
		}
		// A segment's first record is the one its name and header promise. Anything
		// else means the file is not the segment the directory says it is.
		if len(recs) > 0 && recs[0].Sequence != first {
			return out, fmt.Errorf("%w: %s opens with sequence %d", ErrCorruptSegment, name, recs[0].Sequence)
		}
		if expectedNext != 0 && first != expectedNext {
			return out, fmt.Errorf("%w: %s starts at %d, the previous segment ended at %d",
				ErrSegmentGap, name, first, expectedNext-1)
		}
		if len(recs) > 0 {
			expectedNext = recs[len(recs)-1].Sequence + 1
		}

		out.segs = append(out.segs, segmentInfo{name: name, first: first, size: int64(len(buf))})
		out.records = append(out.records, recs...)
		if last {
			out.cleanLen = int64(format.SegmentHeaderSize + consumed)
			out.torn = consumed < len(body)
		}
	}
	return out, nil
}

// readWholeFile reads a file's visible bytes.
func readWholeFile(d disk.Disk, name string) ([]byte, error) {
	f, err := d.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck // read-only handle; a close error cannot lose data
	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	if size > 0 {
		if _, err := f.ReadAt(buf, 0); err != nil {
			return nil, fmt.Errorf("wal: read segment %s: %w", name, err)
		}
	}
	return buf, nil
}

// ReplaySegments decodes every record a volume's WAL directory holds, in sequence
// order. It is what Resume replays and what a test or a diagnostic uses to ask "what
// is actually in the local WAL".
func ReplaySegments(d disk.Disk, root string, volumeID [16]byte, epoch uint64) ([]Record, error) {
	scan, err := scanSegments(d, SegmentDir(root, volumeID, epoch), volumeID, epoch)
	if err != nil {
		return nil, err
	}
	return scan.records, nil
}

// segments is one volume's local WAL: a directory of append-only segment files, of
// which only the newest is ever open.
//
// The type owns two rules that make reclamation safe to reason about. A record never
// straddles a segment, so replaying one segment is self-contained and unlinking one
// cannot cut a record in half. And sealing a segment writes nothing to it — the next
// record simply goes to a new file — so a sealed segment is immutable, and a crash
// mid-seal leaves either no new segment or an empty one, never a half-updated header.
type segments struct {
	d disk.Disk
	// root is <data-dir>/wal: the directory holding every volume's every epoch. It is
	// kept beside dir, which is one epoch of one volume under it, because carrying an
	// earlier epoch forward has to look at its *siblings* — and deriving the parent by
	// trimming two path components off dir would be a second spelling of the layout
	// SegmentDir defines.
	root     string
	dir      string
	volumeID [16]byte
	epoch    uint64
	clk      clock.Clock

	// segBytes is the size at which a segment is sealed. Fixed for the log's life.
	segBytes int64
	// firsts is the first sequence of every retained segment, ascending. The last
	// entry is the newest segment, which is the only one that may be open and the
	// only one truncation never unlinks.
	firsts []uint64
	// open is the newest segment held for append, or nil when it has been sealed (or
	// nothing has been written yet). A nil open with a non-empty firsts is the sealed
	// state: the next record creates the next file.
	open disk.File
	size int64 // bytes in the open segment, header included
	// retainedBytes is what the retained segments occupy on the device, maintained as
	// they are written and unlinked rather than measured.
	//
	// It exists because bytes() below — which opens and stats every segment — is the
	// honest answer and the wrong one to put on the WRITE path: the device bound
	// (Limits.MaxLocalBytes) is evaluated on every append, and paying a stat per
	// segment per guest write would put the cost of the whole log into each record.
	// The counter is maintained at the four places the number can change (a created
	// header, an accepted append, a reclaimed segment, an adopted directory) and
	// nowhere else, and the DST scenario that fills a device asserts on what the
	// device reports rather than on this — so a counter that drifted from the disk
	// would fail there rather than silently loosen the bound it enforces.
	retainedBytes int64
}

func newSegments(d disk.Disk, root string, clk clock.Clock, volumeID [16]byte, epoch uint64, segBytes int64) *segments {
	if segBytes <= 0 {
		segBytes = SegmentBytes
	}
	return &segments{
		d:        d,
		root:     root,
		dir:      SegmentDir(root, volumeID, epoch),
		volumeID: volumeID,
		epoch:    epoch,
		clk:      clk,
		segBytes: segBytes,
	}
}

// empty reports whether this log has created no segment yet.
func (s *segments) empty() bool { return len(s.firsts) == 0 }

// names returns the disk names of the retained segments, oldest first.
func (s *segments) names() []string {
	out := make([]string, len(s.firsts))
	for i, first := range s.firsts {
		out[i] = s.path(first)
	}
	return out
}

func (s *segments) path(first uint64) string { return s.dir + "/" + segmentName(first) }

// create starts the next segment. Its name is the sequence of the first record it will
// carry, and its header is written and made durable before it is used: a segment whose
// name survived a crash but whose header did not is a file replay cannot classify, and
// paying one fdatasync per 32 MiB to rule that out is not a cost worth arguing about.
//
// Disk.Create is responsible for making the *name* durable (it fsyncs the parent
// directory) — without that, fdatasync of the file's contents guarantees nothing,
// because the directory entry pointing at them may not have reached the device.
func (s *segments) create(first uint64) error {
	name := s.path(first)
	f, err := s.d.Create(name)
	if err != nil {
		return err
	}
	h := format.SegmentHeader{
		VolumeID:      s.volumeID,
		Epoch:         s.epoch,
		FirstSequence: first,
		CreatedAtMs:   uint64(s.clk.Wall().UnixMilli()), //nolint:gosec // wall ms is far from overflow
	}
	b, err := h.MarshalBinary()
	if err != nil {
		return errors.Join(err, f.Close())
	}
	if _, err := f.Append(b); err != nil {
		// A file whose header did not land is worse than no file: replay would have
		// to decide what it is. Take it back out of the directory and report the
		// failure — the device state (ENOSPC) is the caller's to latch.
		return errors.Join(err, f.Close(), s.d.Remove(name))
	}
	if err := f.Sync(); err != nil {
		return errors.Join(err, f.Close())
	}
	s.open, s.size = f, int64(len(b))
	s.retainedBytes += int64(len(b))
	s.firsts = append(s.firsts, first)
	return nil
}

// retained reports what the retained segments occupy on the device. See the field.
func (s *segments) retained() int64 { return s.retainedBytes }

// seal closes the newest segment for good. It writes nothing: sealing is the absence
// of further appends, and the next record creates the next file. That is what keeps a
// sealed segment immutable and a crash mid-seal uninteresting.
func (s *segments) seal() error {
	if s.open == nil {
		return nil
	}
	f := s.open
	s.open, s.size = nil, 0
	// A sealed segment is synced once, here. Everything in it is now the
	// responsibility of no further write.
	if err := f.Sync(); err != nil {
		return errors.Join(err, f.Close())
	}
	return f.Close()
}

// ensureRoom leaves the newest segment able to take n more bytes, sealing it and
// starting the next one if it cannot. A record never straddles a segment.
func (s *segments) ensureRoom(seq uint64, n int) error {
	switch {
	case s.open == nil:
		return s.create(seq)
	case s.size+int64(n) <= s.segBytes:
		return nil
	case s.size <= int64(format.SegmentHeaderSize):
		// The segment is empty and the record is larger than a whole segment. Sealing
		// here would produce an endless chain of empty files; the record goes in
		// alone and the one after it rotates.
		return nil
	default:
		if err := s.seal(); err != nil {
			return err
		}
		return s.create(seq)
	}
}

// appendRecord writes one record's encoded bytes into the newest segment.
//
// A failed append is not necessarily an append of nothing: a partial write (ENOSPC, a
// torn write at a device boundary) leaves bytes that are not a record, so the segment
// is rolled back to its last intact record before the failure is reported. If that
// rollback also fails the segment's tail is unknown, and rollbackErr is returned
// alongside so the caller can refuse to serve the log.
//
// When the failure follows a rotation the empty new segment stays. It holds no record,
// so replay skips it; the caller does not consume the sequence, so the next accepted
// record has the sequence the file is named for and lands in it. Removing it here would
// be the harder thing to reason about, not the safer one.
func (s *segments) appendRecord(seq uint64, enc []byte) (err, rollbackErr error) {
	if err := s.ensureRoom(seq, len(enc)); err != nil {
		return err, nil
	}
	before := s.size
	n, err := s.open.Append(enc)
	if err != nil {
		if terr := s.open.Truncate(before); terr != nil {
			return err, terr
		}
		return err, nil
	}
	s.size += int64(n)
	s.retainedBytes += int64(n)
	return nil, nil
}

// sync makes the open segment's appends durable. Sealed segments were synced when they
// were sealed and are never written to again.
func (s *segments) sync() error {
	if s.open == nil {
		return nil
	}
	return s.open.Sync()
}

// close releases the open segment without sealing anything: it is what a Log gives
// back when it is done with the volume, not a durability step.
func (s *segments) close() error {
	if s.open == nil {
		return nil
	}
	f := s.open
	s.open, s.size = nil, 0
	return f.Close()
}

// reclaim unlinks every segment whose records are all at or below upTo, oldest first,
// and reports how many bytes that gave back.
//
// The newest segment is never unlinked, whether or not it is open: it is the one that
// may still be appended to, and it is the only one allowed to end in a torn record.
// The segment holding upTo is therefore kept whole — one segment is the granularity of
// reclamation, which is the whole reason SegmentBytes is a number anyone argues about.
//
// Oldest-first is not cosmetic. The published watermark has already advanced when this
// runs, so a crash part-way through leaves segments a later truncation removes; the
// reverse order would remove segments the published point does not yet cover.
func (s *segments) reclaim(upTo uint64) (int64, error) {
	var freed int64
	for len(s.firsts) > 1 {
		// Segment 0 carries [firsts[0], firsts[1]-1]. All of it is covered exactly
		// when its last possible record is at or below upTo.
		if s.firsts[1]-1 > upTo {
			break
		}
		name := s.path(s.firsts[0])
		size, err := segmentSize(s.d, name)
		if err != nil {
			return freed, err
		}
		if err := s.d.Remove(name); err != nil {
			return freed, fmt.Errorf("wal: unlink reclaimed segment %s: %w", name, err)
		}
		s.firsts = s.firsts[1:]
		freed += size
		s.retainedBytes -= size
	}
	return freed, nil
}

// segmentSize reports a segment's size, so reclaim can say how much it gave back.
func segmentSize(d disk.Disk, name string) (int64, error) {
	f, err := d.Open(name)
	if err != nil {
		return 0, err
	}
	defer f.Close() //nolint:errcheck // read-only handle
	return f.Size()
}

// bytes reports what this log currently occupies on the device.
func (s *segments) bytes() (int64, error) {
	var total int64
	for _, name := range s.names() {
		size, err := segmentSize(s.d, name)
		if err != nil {
			return 0, err
		}
		total += size
	}
	return total, nil
}

// adopt takes over the segments a scan found, opening the newest for append.
//
// The newest is reopened rather than superseded by a fresh file, and that is
// deliberate: sealing writes nothing, so after a crash there is no way to tell a
// sealed newest from an open one, and starting a new segment would leave the torn tail
// of the previous newest in a file that is no longer the newest — which the next
// replay would correctly call corruption.
//
// A torn tail is cut off here rather than appended past. Continuing after a partial
// record would put every later record behind a hole replay stops at, which is the
// silent-data-loss shape INV-05 exists to rule out.
func (s *segments) adopt(scan scanResult) error {
	if len(scan.segs) == 0 {
		return nil
	}
	newest := scan.segs[len(scan.segs)-1]
	if scan.stub {
		// A file too short to hold a header carries nothing. Remove it; the next
		// record creates a segment with the name it should have had.
		if err := s.d.Remove(newest.name); err != nil {
			return fmt.Errorf("wal: remove a segment stub %s: %w", newest.name, err)
		}
		scan.segs = scan.segs[:len(scan.segs)-1]
		if len(scan.segs) == 0 {
			return nil
		}
		// The segment now newest was not the newest during the scan, so a torn tail
		// in it would already have been an error: all of it decodes.
		newest = scan.segs[len(scan.segs)-1]
		scan.torn, scan.cleanLen = false, newest.size
	}
	for _, seg := range scan.segs {
		s.firsts = append(s.firsts, seg.first)
		s.retainedBytes += seg.size
	}
	// The newest segment's torn tail is about to be cut off, so the bytes it holds are
	// cleanLen and not the size the scan measured. Counting the tail would leave a
	// resumed log believing it occupies more of its share than it does — a bound that
	// tightens itself a little on every crash.
	s.retainedBytes -= newest.size - scan.cleanLen
	f, err := s.d.Open(newest.name)
	if err != nil {
		return err
	}
	if scan.torn {
		if err := f.Truncate(scan.cleanLen); err != nil {
			return errors.Join(fmt.Errorf("wal: cut the torn tail of %s: %w", newest.name, err), f.Close())
		}
	}
	s.open, s.size = f, scan.cleanLen
	return nil
}
