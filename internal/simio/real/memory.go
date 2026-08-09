package real

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
)

// The files a Linux process has to read to find out how much memory it may actually
// use. All four are text, all four are in /proc or /sys, and none of them is the whole
// answer on its own.
const (
	procMeminfo    = "/proc/meminfo"
	procSelfCgroup = "/proc/self/cgroup"
	// cgroupV2Root is where the unified hierarchy is mounted. Inside a container with
	// its own cgroup namespace the container's own cgroup *is* this directory, which is
	// why the walk below starting at "/" still finds a container's limit.
	cgroupV2Root = "/sys/fs/cgroup"
	// cgroupV1MemoryRoot is the memory controller's mount in the legacy hierarchy.
	cgroupV1MemoryRoot = "/sys/fs/cgroup/memory"
)

// Memory is how much memory this process may use before something kills it, and where
// that number came from.
//
// The Source is not decoration. The number is printed on the Agent's start-up line and
// an operator has to be able to check it: "2147483648 from
// /sys/fs/cgroup/memory.max" is a claim they can verify with cat, while a bare
// 2147483648 on a 256 GiB host looks like a bug in the parse.
type Memory struct {
	// LimitBytes is the smallest ceiling found: the machine's RAM, or a cgroup limit
	// below it.
	LimitBytes int64
	// Source is the path LimitBytes was read from.
	Source string
}

// MeasureMemory answers "how much memory does this Agent have", the way Disk.Usage
// answers "how big is this device" — by asking the machine rather than by taking a
// number from configuration.
//
// # Why a function here and not a method on an interface
//
// disk.Disk is an interface because the WAL calls it on every write and DST substitutes
// a simulated device for it: the *data path* runs through it. Nothing runs through this.
// It is read once, in `main`, before any volume exists, and the result — two plain
// numbers — is handed to agent.NewBudget, which is a pure function that every test can
// call with whatever machine it wants to describe. An interface would buy a seam nobody
// needs to move and cost an implementation nobody calls, which is the shape CLAUDE.md
// calls a liability. Putting it on disk.Disk was rejected for a smaller reason and a
// bigger one: memory is not a property of the device backing a directory, and both
// implementations of that interface would have to grow a method to say so.
//
// It lives in internal/simio/real because reading these files is `os.ReadFile`, which
// INV-01 permits nowhere else, and because a real measurement is exactly what this
// package is for.
func MeasureMemory() (Memory, error) { return measureMemory(os.ReadFile) }

// measureMemory takes the read as a parameter so the resolution can be driven against
// machines this one is not: a 2 GiB container on a 256 GiB host, a cgroup v1 sentinel, a
// limit on an ancestor slice. Reading the real /proc would test whichever machine the
// suite happens to run on.
func measureMemory(read func(string) ([]byte, error)) (Memory, error) {
	total, err := memTotal(read)
	if err != nil {
		// Fatal, and deliberately not a fallback. The two available fallbacks are both
		// worse than not starting: assuming a large machine computes a bound that lets
		// the guests overcommit until the OOM killer takes the Agent down with every
		// tenant on it, and assuming a small one refuses guest writes on a host that was
		// fine. A process that cannot find out how big its machine is has nothing to
		// divide, and saying so at start-up is the only honest answer.
		return Memory{}, err
	}
	m := Memory{LimitBytes: total, Source: procMeminfo}
	for _, l := range cgroupLimits(read) {
		if l.bytes < m.LimitBytes {
			m = Memory{LimitBytes: l.bytes, Source: l.path}
		}
	}
	return m, nil
}

// memTotal reads MemTotal out of /proc/meminfo, in bytes.
//
// MemTotal and not MemAvailable, and the difference matters at start-up: MemAvailable is
// what is free *right now*, which on a host with a warm page cache is a fraction of the
// machine and on a freshly booted one is nearly all of it. A bound derived from it would
// depend on when the Agent happened to restart — the same host would give its guests
// different bounds on Monday and Tuesday — and the page cache it is measuring against is
// reclaimable anyway. The ceiling is the machine.
func memTotal(read func(string) ([]byte, error)) (int64, error) {
	body, err := read(procMeminfo)
	if err != nil {
		return 0, fmt.Errorf("simio/real: reading %s: %w", procMeminfo, err)
	}
	for line := range strings.Lines(string(body)) {
		rest, ok := strings.CutPrefix(line, "MemTotal:")
		if !ok {
			continue
		}
		// "MemTotal:       32659388 kB" — the unit is always kB, and has been since
		// the field existed; the kernel prints it unconditionally in show_val_kb.
		fields := strings.Fields(rest)
		if len(fields) < 2 || fields[1] != "kB" {
			return 0, fmt.Errorf("simio/real: %s has a MemTotal this build cannot read: %q", procMeminfo, strings.TrimSpace(line))
		}
		kb, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("simio/real: %s: MemTotal %q: %w", procMeminfo, fields[0], err)
		}
		return kb * 1024, nil
	}
	return 0, fmt.Errorf("simio/real: %s carries no MemTotal line", procMeminfo)
}

// limit is one ceiling and the file it was read from.
type limit struct {
	bytes int64
	path  string
}

// cgroupLimits returns every memory ceiling the cgroup hierarchy imposes on this
// process. Errors are deliberately absent from the signature: a machine with no cgroups,
// a kernel that mounts them somewhere else, and a hierarchy this build does not
// understand are all "no limit found here", and MemTotal is still the answer. The failure
// that must be loud is not being able to read the machine at all, and memTotal owns it.
//
// # Every ancestor, not just the leaf
//
// A limit on a parent slice binds this process exactly as hard as one on its own cgroup
// — `systemd-run --user -p MemoryMax=2G` puts the process several levels below a root
// that says "max" — so a walk that stopped at the leaf would report "unlimited" for a
// process that is very much limited. Taking every ancestor and letting the caller keep
// the smallest is the only reading that cannot be too large, and too large is the
// direction that ends at the OOM killer.
//
// # cgroup v1
//
// It is read, with one difference that needs no code: v1 has no "max" keyword and writes
// a page-aligned MaxInt64 (9223372036854771712) when nothing is limited. Special-casing
// that sentinel would be guessing at a number; instead every candidate here competes with
// MemTotal, and a ceiling larger than the machine's RAM loses on its own. The same rule
// covers a v2 cgroup whose limit is genuinely above the machine, and it is why "max" and
// "unlimited" need no shared spelling.
//
// What is *not* read is memory.high. It is where reclaim pressure starts, not where the
// process dies; the kill line is memory.max, and the bound this feeds exists to keep the
// OOM killer away from an Agent holding sixteen tenants' sessions.
func cgroupLimits(read func(string) ([]byte, error)) []limit {
	body, err := read(procSelfCgroup)
	if err != nil {
		return nil
	}
	var out []limit
	for line := range strings.Lines(string(body)) {
		// "hierarchy-ID:controller-list:cgroup-path", and the v2 line is the one whose
		// controller list is empty (its id is 0).
		parts := strings.SplitN(strings.TrimSpace(line), ":", 3)
		if len(parts) != 3 || parts[2] == "" {
			continue
		}
		root, file := cgroupV2Root, "memory.max"
		if parts[1] != "" {
			if !slices.Contains(strings.Split(parts[1], ","), "memory") {
				continue // a v1 controller that is not the memory one
			}
			root, file = cgroupV1MemoryRoot, "memory.limit_in_bytes"
		}
		for dir := parts[2]; ; dir = path.Dir(dir) {
			if l, ok := readLimit(read, path.Join(root, dir, file)); ok {
				out = append(out, l)
			}
			if dir == "/" || dir == "." {
				break
			}
		}
	}
	return out
}

// readLimit parses one cgroup limit file. A missing file is the ordinary case — most of
// the paths the walk above builds do not exist — and so is "max"; both mean "no ceiling
// here" rather than a failure.
func readLimit(read func(string) ([]byte, error), file string) (limit, bool) {
	body, err := read(file)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			// A limit file that exists and will not be read is the one case where this
			// silently reports a ceiling that is too high, so it says so. Not fatal:
			// MemTotal still bounds the answer, and refusing to start because one file
			// under /sys was unreadable would take a host down for a permission bit.
			slog.Warn("this cgroup's memory limit could not be read; the Agent is sizing its read-view bound from a ceiling that may be too high",
				"file", file, "error", err)
		}
		return limit{}, false
	}
	text := strings.TrimSpace(string(body))
	if text == "max" {
		return limit{}, false
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil || n <= 0 {
		return limit{}, false
	}
	return limit{bytes: n, path: file}, true
}
