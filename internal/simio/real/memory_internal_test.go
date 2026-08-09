package real

// Internal, because the subject is the resolution — which files are consulted, in what
// order, and which answers are discarded — and the only honest way to drive it is to
// hand it a filesystem that does not exist on this machine. An external test could read
// the real /proc and /sys, which on a developer's laptop is one case (no cgroup limit)
// and on CI is another, and neither of them is the case this exists for: a 2 GiB limit
// on a 256 GiB host.

import (
	"errors"
	"fmt"
	"io/fs"
	"testing"
)

const (
	gib = int64(1) << 30
	// The 256 GiB host every cgroup case below is running on, so that a limit being
	// honoured is visible as a number two orders of magnitude away from the machine's.
	bigHost = 256 * gib
	// What cgroup v1 writes when nothing is limited: not a word, not a zero, but
	// page-aligned MaxInt64. See memoryLimit for why nothing here special-cases it.
	v1Unlimited = "9223372036854771712"
)

// meminfo is a /proc/meminfo with the shape the kernel actually prints: MemTotal is not
// the first line on every kernel, the units are kB, and the columns are space-padded.
func meminfo(kb int64) string {
	return fmt.Sprintf("MemFree:         1000000 kB\nMemTotal:       %8d kB\nMemAvailable:    2000000 kB\n", kb)
}

func TestTheMemoryAnAgentSizesItselfFromIsTheSmallestCeilingItCanFind(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		files      map[string]string
		wantBytes  int64
		wantSource string
		wantErr    bool
	}{
		{
			// The bare host: no cgroup anywhere. RAM is the ceiling.
			name:       "a host with no cgroup limit is bounded by its RAM",
			files:      map[string]string{procMeminfo: meminfo(bigHost / 1024)},
			wantBytes:  bigHost,
			wantSource: procMeminfo,
		},
		{
			// The case this whole measurement exists for. A 2 GiB container on a
			// 256 GiB host: sizing from MemTotal would compute a bound 128x too
			// large, and in a container the OOM killer takes the Agent, not the
			// guest that caused it.
			name: "a cgroup v2 limit is what a container is bounded by, not the host's RAM",
			files: map[string]string{
				procMeminfo:                  meminfo(bigHost / 1024),
				procSelfCgroup:               "0::/\n",
				cgroupV2Root + "/memory.max": "2147483648\n",
			},
			wantBytes:  2 * gib,
			wantSource: cgroupV2Root + "/memory.max",
		},
		{
			// A cgroup namespace is not always what the process sits in: under
			// `systemd-run --user -p MemoryMax=...` the path is several levels deep
			// and /sys/fs/cgroup/memory.max is the *root's*, which says "max".
			name: "the limit is read at the cgroup this process is actually in",
			files: map[string]string{
				procMeminfo:                             meminfo(bigHost / 1024),
				procSelfCgroup:                          "0::/user.slice/user-1000.slice/run-r42.scope\n",
				cgroupV2Root + "/memory.max":            "max\n",
				cgroupV2Root + "/user.slice/memory.max": "max\n",
				cgroupV2Root + "/user.slice/user-1000.slice/memory.max":               "max\n",
				cgroupV2Root + "/user.slice/user-1000.slice/run-r42.scope/memory.max": "2147483648\n",
			},
			wantBytes:  2 * gib,
			wantSource: cgroupV2Root + "/user.slice/user-1000.slice/run-r42.scope/memory.max",
		},
		{
			// An ancestor's limit binds this process just as hard as its own, and a
			// walk that stopped at the leaf would miss it entirely: the leaf here
			// says "max" and the slice above it says 4 GiB.
			name: "an ancestor cgroup's limit binds this process too",
			files: map[string]string{
				procMeminfo:                  meminfo(bigHost / 1024),
				procSelfCgroup:               "0::/agents.slice/volume-agent.service\n",
				cgroupV2Root + "/memory.max": "max\n",
				cgroupV2Root + "/agents.slice/memory.max":                      "4294967296\n",
				cgroupV2Root + "/agents.slice/volume-agent.service/memory.max": "max\n",
			},
			wantBytes:  4 * gib,
			wantSource: cgroupV2Root + "/agents.slice/memory.max",
		},
		{
			// cgroup v1 says "unlimited" by printing a number bigger than any
			// machine. It needs no special case: it loses to MemTotal.
			name: "cgroup v1's unlimited sentinel is not a limit",
			files: map[string]string{
				procMeminfo:    meminfo(bigHost / 1024),
				procSelfCgroup: "8:memory:/\n0::/\n",
				cgroupV1MemoryRoot + "/memory.limit_in_bytes": v1Unlimited + "\n",
			},
			wantBytes:  bigHost,
			wantSource: procMeminfo,
		},
		{
			name: "a real cgroup v1 limit is honoured",
			files: map[string]string{
				procMeminfo:    meminfo(bigHost / 1024),
				procSelfCgroup: "9:cpu,cpuacct:/docker/abc\n8:memory:/docker/abc\n",
				cgroupV1MemoryRoot + "/docker/abc/memory.limit_in_bytes": "1073741824\n",
			},
			wantBytes:  gib,
			wantSource: cgroupV1MemoryRoot + "/docker/abc/memory.limit_in_bytes",
		},
		{
			// A limit above the machine's RAM is not a ceiling this process can
			// reach, and reporting it would be reporting memory that does not exist.
			name: "a cgroup limit larger than the machine loses to the machine",
			files: map[string]string{
				procMeminfo:                  meminfo(2 * gib / 1024),
				procSelfCgroup:               "0::/\n",
				cgroupV2Root + "/memory.max": "1099511627776\n",
			},
			wantBytes:  2 * gib,
			wantSource: procMeminfo,
		},
		{
			// No MemTotal, no derivation, no start-up. See measureMemory.
			name:    "a machine that will not say how much memory it has is refused",
			files:   map[string]string{procMeminfo: "MemFree: 1000 kB\n"},
			wantErr: true,
		},
		{
			name:    "an unreadable /proc/meminfo is refused",
			files:   map[string]string{},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := measureMemory(fakeFS(tc.files))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("a machine whose memory could not be read reported %+v; the Agent would size itself from nothing", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("measureMemory: %v", err)
			}
			if got.LimitBytes != tc.wantBytes {
				t.Fatalf("measured %d bytes, want %d (from %q)", got.LimitBytes, tc.wantBytes, tc.wantSource)
			}
			if got.Source != tc.wantSource {
				t.Fatalf("the number is attributed to %q, want %q — an operator reading the start-up line cannot check a figure whose origin is wrong",
					got.Source, tc.wantSource)
			}
		})
	}
}

// TestTheRealMachineAnswers is the one case the table cannot fake: this host. It asserts
// nothing about the value beyond it being a plausible amount of memory, because the
// value is whatever the machine has — the point is that the files this reads are the
// files that exist, which a hand-built map can never prove.
func TestTheRealMachineAnswers(t *testing.T) {
	t.Parallel()
	m, err := MeasureMemory()
	if err != nil {
		t.Fatalf("measuring this machine's memory: %v", err)
	}
	if m.LimitBytes < 64<<20 {
		t.Fatalf("this machine reports %d bytes of usable memory (from %s); the parse is wrong", m.LimitBytes, m.Source)
	}
	if m.Source == "" {
		t.Fatal("the measurement names no source; the start-up line would print a number an operator cannot check")
	}
	t.Logf("this machine: %d bytes from %s", m.LimitBytes, m.Source)
}

// fakeFS is a read function over a map, answering fs.ErrNotExist for anything absent —
// which is most of what the resolution asks for, and is the answer that must not become
// an error.
func fakeFS(files map[string]string) func(string) ([]byte, error) {
	return func(name string) ([]byte, error) {
		body, ok := files[name]
		if !ok {
			return nil, fmt.Errorf("open %s: %w", name, fs.ErrNotExist)
		}
		if body == "" {
			return nil, errors.New("read: I/O error")
		}
		return []byte(body), nil
	}
}
