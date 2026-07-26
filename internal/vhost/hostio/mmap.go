package hostio

import (
	"fmt"
	"os"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/spin-stack/storage/internal/vhost"
)

// Mapper maps the front-end's memory regions into this process.
//
// MAP_SHARED is not a tuning choice: the whole protocol depends on the guest
// seeing this process's stores and vice versa. A MAP_PRIVATE mapping would read
// correctly, complete requests, and write the guest's data into a copy nobody
// ever looks at.
type Mapper struct {
	mu   sync.Mutex
	maps map[*byte][]byte
}

// NewMapper returns a Mapper.
func NewMapper() *Mapper { return &Mapper{maps: make(map[*byte][]byte)} }

// Map implements vhost.Mapper.
func (m *Mapper) Map(f *os.File, offset, size uint64) ([]byte, error) {
	if f == nil {
		return nil, fmt.Errorf("hostio: memory region without a file descriptor")
	}
	if size == 0 {
		return nil, fmt.Errorf("hostio: memory region of zero bytes")
	}
	if size > uint64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("hostio: memory region of %d bytes does not fit in this address space", size)
	}
	b, err := unix.Mmap(int(f.Fd()), int64(offset), int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("hostio: mmap %d bytes at offset %d: %w", size, offset, err)
	}
	m.mu.Lock()
	m.maps[&b[0]] = b
	m.mu.Unlock()
	return b, nil
}

// Unmap implements vhost.Mapper. It only accepts a slice this Mapper produced:
// munmap on an arbitrary slice would unmap Go heap memory and take the process
// with it at some later, unrelated moment.
func (m *Mapper) Unmap(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	m.mu.Lock()
	full, ok := m.maps[&b[0]]
	if ok {
		delete(m.maps, &b[0])
	}
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("hostio: unmap of a slice this mapper did not create")
	}
	if err := unix.Munmap(full); err != nil {
		return fmt.Errorf("hostio: munmap: %w", err)
	}
	return nil
}

var _ vhost.Mapper = (*Mapper)(nil)
