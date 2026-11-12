package metrics

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	topProcessLimit = 5
	// processCPUWindow is how long per-process CPU time is measured. ps reports
	// lifetime averages, which hide a long-running process that is busy now.
	processCPUWindow = time.Second
	// Linux reports /proc/<pid>/stat times in USER_HZ, fixed at 100 for the
	// userspace ABI on every supported architecture.
	clockTicksPerSecond = 100
	// maxProcessScan bounds work and memory on hosts with runaway forking.
	maxProcessScan = 32768
)

var procRoot = "/proc"

// processSample is one process's cumulative CPU time and resident memory.
type processSample struct {
	name  string
	ticks uint64 // utime + stime
	start uint64 // start time since boot; detects PID reuse
	rss   uint64 // resident pages
}

// parseProcessStat reads /proc/<pid>/stat. The command name is wrapped in
// parentheses and can itself contain spaces or parentheses, so fields are
// counted from the last closing parenthesis.
func parseProcessStat(data []byte) (processSample, bool) {
	open := bytes.IndexByte(data, '(')
	end := bytes.LastIndexByte(data, ')')
	if open < 0 || end <= open {
		return processSample{}, false
	}
	name := strings.TrimSpace(string(data[open+1 : end]))
	// Fields after the name start at field 3 (state).
	fields := strings.Fields(string(data[end+1:]))
	const utime, stime, starttime, rss = 14 - 3, 15 - 3, 22 - 3, 24 - 3
	if name == "" || len(fields) <= rss {
		return processSample{}, false
	}
	parse := func(i int) (uint64, bool) {
		value, err := strconv.ParseUint(fields[i], 10, 64)
		return value, err == nil
	}
	user, ok1 := parse(utime)
	system, ok2 := parse(stime)
	started, ok3 := parse(starttime)
	// rss is signed in the kernel ABI; a negative value is treated as zero.
	pages, err := strconv.ParseInt(fields[rss], 10, 64)
	if !ok1 || !ok2 || !ok3 || err != nil || user+system < user {
		return processSample{}, false
	}
	return processSample{name: name, ticks: user + system, start: started, rss: uint64(max(pages, 0))}, true
}

func readProcessSamples(root string) map[int]processSample {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	samples := make(map[int]processSample, min(len(entries), maxProcessScan))
	for _, entry := range entries {
		if len(samples) >= maxProcessScan {
			break
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, entry.Name(), "stat"))
		if err != nil {
			continue // exited or not readable
		}
		if sample, ok := parseProcessStat(data); ok {
			samples[pid] = sample
		}
	}
	return samples
}

type processGroup struct {
	name  string
	ticks uint64
	rss   uint64
	count int
}

// topProcessesBetween groups processes by name and ranks them by CPU used
// between the two samples and by current resident memory. CPU is a share of
// whole-host capacity (0-100), comparable with the server CPU reading; memory
// is a share of total RAM. A process absent from the first sample, or whose
// PID was reused, started during the window and contributes all of its time.
func topProcessesBetween(before, after map[int]processSample, elapsed time.Duration, cpus int, memTotalBytes, pageSize uint64) (topCPU, topMem []TopProcess) {
	if len(after) == 0 {
		return nil, nil
	}
	groups := map[string]*processGroup{}
	for pid, current := range after {
		used := current.ticks
		if previous, ok := before[pid]; ok && previous.start == current.start {
			if current.ticks < previous.ticks {
				used = 0
			} else {
				used = current.ticks - previous.ticks
			}
		}
		group := groups[current.name]
		if group == nil {
			group = &processGroup{name: current.name}
			groups[current.name] = group
		}
		group.ticks += used
		group.rss += current.rss
		group.count++
	}

	capacity := elapsed.Seconds() * clockTicksPerSecond * float64(max(cpus, 1))
	list := make([]TopProcess, 0, len(groups))
	for _, group := range groups {
		process := TopProcess{Name: strings.Clone(group.name), Count: group.count}
		if capacity > 0 {
			process.CPU = min(float64(group.ticks)/capacity*100, 100)
		}
		if memTotalBytes > 0 {
			process.Mem = min(float64(group.rss*pageSize)/float64(memTotalBytes)*100, 100)
		}
		list = append(list, process)
	}

	rank := func(less func(a, b TopProcess) bool) []TopProcess {
		sorted := append([]TopProcess(nil), list...)
		sort.Slice(sorted, func(i, j int) bool {
			if less(sorted[i], sorted[j]) {
				return true
			}
			if less(sorted[j], sorted[i]) {
				return false
			}
			return sorted[i].Name < sorted[j].Name
		})
		return sorted[:min(len(sorted), topProcessLimit)]
	}
	topCPU = rank(func(a, b TopProcess) bool { return a.CPU > b.CPU })
	topMem = rank(func(a, b TopProcess) bool { return a.Mem > b.Mem })
	return topCPU, topMem
}

// collectTopProcesses measures per-process CPU over processCPUWindow. It
// returns nil lists when /proc is unavailable or ctx ends first.
func collectTopProcesses(ctx context.Context, memTotalBytes uint64) (topCPU, topMem []TopProcess) {
	if memTotalBytes == 0 {
		memTotalBytes = parseKB(parseKeyValueFile(filepath.Join(procRoot, "meminfo"))["MemTotal"])
	}
	before := readProcessSamples(procRoot)
	if before == nil {
		return nil, nil
	}
	// Timing from the end of one scan to the end of the next matches each
	// process's own read-to-read interval, whatever its position in the scan.
	startedAt := time.Now()
	timer := time.NewTimer(processCPUWindow)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, nil
	case <-timer.C:
	}
	after := readProcessSamples(procRoot)
	return topProcessesBetween(before, after, time.Since(startedAt), runtime.NumCPU(), memTotalBytes, uint64(os.Getpagesize()))
}
