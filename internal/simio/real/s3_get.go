package real

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// One S3 connection carries far less than a host's network card: a single GET stream
// settles around 100 MiB/s, while the instance an Agent runs on has ten times that. The
// difference is not a nicety — a restore is the time a guest is not running, and it is
// spent almost entirely pulling one layer after another out of the bucket.
//
// So a layer is fetched as many ranges at once and handed over in order. In order is what
// lets the caller keep streaming: the digest over the bytes as stored is a SHA-256, which
// has to see them from first to last, and unsealing walks frame by frame. The alternative —
// writing ranges into a file at their offsets and reading it back — needs the sealed layer
// and the plaintext on disk at once, which for a compacted root is twice the guest's disk.
//
// What is held instead is getConcurrency × getChunkBytes, whatever the object weighs.
const (
	getChunkBytes  = 8 << 20
	getConcurrency = 8
)

// GetStream fetches key as a stream. An object that fits in one chunk is one request, the
// same as Get; anything larger is read in parallel.
func (s *S3Store) GetStream(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	// The first range doubles as the size probe: S3 answers it with Content-Range, which
	// carries the total. A HEAD first would be a round trip spent learning what this
	// request reports anyway.
	first, total, err := s.getRange(ctx, key, 0, getChunkBytes)
	if err != nil {
		return nil, 0, err
	}
	if total <= int64(len(first)) {
		return io.NopCloser(bytes.NewReader(first)), total, nil
	}

	rctx, cancel := context.WithCancel(ctx)
	r := &rangeReader{
		store: s, key: key, total: total, cancel: cancel,
		pending: make(chan chan chunk, getConcurrency),
		cur:     bytes.NewReader(first),
	}
	go r.dispatch(rctx, int64(len(first)))
	return r, total, nil
}

// getRange reads [off, off+n) and reports the object's total size. A zero-length object
// answers with no Content-Range at all, so the length of what came back is the fallback.
func (s *S3Store) getRange(ctx context.Context, key string, off, n int64) ([]byte, int64, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", off, off+n-1)),
	})
	if err != nil {
		return nil, 0, translate(err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("simio/real: read %s at %d: %w", key, off, err)
	}
	total := totalOfContentRange(aws.ToString(out.ContentRange))
	if total < 0 {
		total = off + int64(len(data))
	}
	return data, total, nil
}

// totalOfContentRange reads the length out of "bytes 0-8388607/20000000". A header this
// cannot parse returns -1 rather than a guess: a wrong total truncates the layer, and a
// truncated layer at a content-addressed key fails its digest, which is the right failure
// but a mystifying one to arrive at from here.
func totalOfContentRange(h string) int64 {
	_, size, ok := strings.Cut(h, "/")
	if !ok {
		return -1
	}
	n, err := strconv.ParseInt(strings.TrimSpace(size), 10, 64)
	if err != nil {
		return -1
	}
	return n
}

// chunk is one range's outcome.
type chunk struct {
	data []byte
	err  error
}

// rangeReader hands the ranges over in the order they belong in, while as many as
// getConcurrency of them are in flight. pending is that window: one channel per range,
// queued in order, each filled by the goroutine that fetched it. Read waits on the head of
// the queue, so a range that arrives early waits its turn and one that is slow blocks only
// what follows it.
type rangeReader struct {
	store   *S3Store
	key     string
	total   int64
	cancel  context.CancelFunc
	pending chan chan chunk

	cur     io.Reader
	err     error
	closed  bool
	closeMu sync.Mutex
}

// dispatch queues every remaining range in order and starts a fetch for each. The queue's
// capacity is what bounds concurrency: the send blocks once getConcurrency ranges are
// outstanding, so nothing is fetched far ahead of what the caller is reading.
func (r *rangeReader) dispatch(ctx context.Context, from int64) {
	defer close(r.pending)
	for off := from; off < r.total; off += getChunkBytes {
		ch := make(chan chunk, 1)
		select {
		case r.pending <- ch:
		case <-ctx.Done():
			return
		}
		go func(off int64) {
			n := int64(getChunkBytes)
			if rest := r.total - off; rest < n {
				n = rest
			}
			data, _, err := r.store.getRange(ctx, r.key, off, n)
			if err == nil && int64(len(data)) != n {
				err = fmt.Errorf("simio/real: %s at %d gave %d bytes, want %d", r.key, off, len(data), n)
			}
			ch <- chunk{data: data, err: err}
		}(off)
	}
}

func (r *rangeReader) Read(p []byte) (int, error) {
	for {
		if r.err != nil {
			return 0, r.err
		}
		if r.cur != nil {
			n, err := r.cur.Read(p)
			if n > 0 {
				return n, nil
			}
			if err != nil && !errors.Is(err, io.EOF) {
				r.err = err
				return 0, err
			}
			r.cur = nil
		}
		ch, ok := <-r.pending
		if !ok {
			r.err = io.EOF
			return 0, io.EOF
		}
		c := <-ch
		if c.err != nil {
			r.err = c.err
			return 0, c.err
		}
		r.cur = bytes.NewReader(c.data)
	}
}

// Close stops the fetches still in flight. A caller that gives up halfway — a digest that
// failed, a restore that was refused — must not leave goroutines pulling the rest of a
// layer it has no use for.
func (r *rangeReader) Close() error {
	r.closeMu.Lock()
	defer r.closeMu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	r.cancel()
	return nil
}
