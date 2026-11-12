package metrics

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// stat builds a /proc/<pid>/stat line with the fields this package reads.
func stat(pid int, name string, utime, stime, start uint64, rss int64) string {
	// Fields 3-13, then utime(14) stime(15), 16-21, starttime(22), vsize(23), rss(24), more.
	return fmt.Sprintf("%d (%s) S 1 1 1 0 -1 4194560 100 0 0 0 %d %d 0 0 20 0 1 0 %d 1000000 %d 18446744073709551615 0 0\n", pid, name, utime, stime, start, rss)
}

func TestParseProcessStat(t *testing.T) {
	sample, ok := parseProcessStat([]byte(stat(7, "my (odd) app", 120, 30, 5000, 256)))
	if !ok || sample.name != "my (odd) app" || sample.ticks != 150 || sample.start != 5000 || sample.rss != 256 {
		t.Fatalf("parsed %+v ok=%v", sample, ok)
	}
	if sample, ok := parseProcessStat([]byte(stat(8, "kthread", 1, 1, 1, -1))); !ok || sample.rss != 0 {
		t.Fatalf("negative rss: %+v ok=%v", sample, ok)
	}
	for _, input := range []string{"", "1 nameonly S 1", "1 () S 1 1 1 0 -1 0 0 0 0 0 1 1 0 0 20 0 1 0 1 1 1", "1 (short) S 1 2 3", "1 (bad) S 1 1 1 0 -1 0 0 0 0 0 x 1 0 0 20 0 1 0 1 1 1"} {
		if _, ok := parseProcessStat([]byte(input)); ok {
			t.Fatalf("accepted %q", input)
		}
	}
}

func TestTopProcessesMeasureCurrentCPUNotLifetime(t *testing.T) {
	const page = 4096
	before := map[int]processSample{
		// Long-running daemon: huge lifetime total, busy now.
		10: {name: "daemon", ticks: 1_000_000, start: 100, rss: 1000},
		// Busy in the past, idle now.
		11: {name: "batch", ticks: 900_000, start: 200, rss: 10},
		// PID reused by a new process during the window.
		12: {name: "old", ticks: 50_000, start: 300, rss: 10},
	}
	after := map[int]processSample{
		10: {name: "daemon", ticks: 1_000_000 + 150, start: 100, rss: 1000},
		11: {name: "batch", ticks: 900_000, start: 200, rss: 10},
		12: {name: "fresh", ticks: 40, start: 9000, rss: 10},
		13: {name: "cron", ticks: 20, start: 9001, rss: 10},
	}
	// Two CPUs for one second: 200 ticks of capacity.
	topCPU, topMem := topProcessesBetween(before, after, time.Second, 2, 100*1000*page, page)
	want := []struct {
		name string
		cpu  float64
	}{{"daemon", 75}, {"fresh", 20}, {"cron", 10}, {"batch", 0}}
	if len(topCPU) != len(want) {
		t.Fatalf("topCPU = %+v", topCPU)
	}
	for i, w := range want {
		if topCPU[i].Name != w.name || math.Abs(topCPU[i].CPU-w.cpu) > 1e-9 || topCPU[i].Count != 1 {
			t.Fatalf("topCPU[%d] = %+v, want %s %g", i, topCPU[i], w.name, w.cpu)
		}
	}
	if topMem[0].Name != "daemon" || math.Abs(topMem[0].Mem-1) > 1e-9 {
		t.Fatalf("topMem = %+v", topMem)
	}
}

func TestTopProcessesGroupByName(t *testing.T) {
	const page = 4096
	before := map[int]processSample{}
	after := map[int]processSample{}
	for pid := 1; pid <= 24; pid++ {
		before[pid] = processSample{name: "php-fpm", ticks: 0, start: 1, rss: 1000}
		after[pid] = processSample{name: "php-fpm", ticks: 2, start: 1, rss: 1000}
	}
	before[100] = processSample{name: "mysqld", ticks: 0, start: 1, rss: 8000}
	after[100] = processSample{name: "mysqld", ticks: 10, start: 1, rss: 8000}
	for i := 0; i < 6; i++ {
		after[200+i] = processSample{name: fmt.Sprintf("tool%d", i), start: 1}
	}
	topCPU, topMem := topProcessesBetween(before, after, time.Second, 1, 40000*page, page)
	if len(topCPU) != topProcessLimit || len(topMem) != topProcessLimit {
		t.Fatalf("lists not limited: %d %d", len(topCPU), len(topMem))
	}
	if topMem[0].Name != "php-fpm" || topMem[0].Count != 24 || math.Abs(topMem[0].Mem-60) > 1e-9 {
		t.Fatalf("grouped memory = %+v", topMem[0])
	}
	if topCPU[0].Name != "php-fpm" || math.Abs(topCPU[0].CPU-48) > 1e-9 || topCPU[1].Name != "mysqld" {
		t.Fatalf("grouped CPU = %+v", topCPU)
	}
	// Ties are ordered by name so repeated samples are stable.
	if topCPU[2].Name != "tool0" || topCPU[3].Name != "tool1" {
		t.Fatalf("tie order = %+v", topCPU)
	}
}

func TestTopProcessesBoundValues(t *testing.T) {
	before := map[int]processSample{1: {name: "a", ticks: 500, start: 1}}
	after := map[int]processSample{
		1: {name: "a", ticks: 100, start: 1, rss: 1 << 40}, // counter went backwards
		2: {name: "b", ticks: 10_000, start: 2},            // more than capacity
	}
	topCPU, topMem := topProcessesBetween(before, after, time.Second, 1, 1024, 4096)
	if topCPU[0].Name != "b" || topCPU[0].CPU != 100 || topCPU[1].CPU != 0 {
		t.Fatalf("CPU bounds = %+v", topCPU)
	}
	if topMem[0].Mem != 100 {
		t.Fatalf("memory bound = %+v", topMem)
	}
	if cpu, mem := topProcessesBetween(before, nil, time.Second, 1, 1024, 4096); cpu != nil || mem != nil {
		t.Fatal("empty sample produced processes")
	}
	if cpu, _ := topProcessesBetween(nil, after, 0, 1, 0, 4096); cpu[0].CPU != 0 || cpu[0].Mem != 0 {
		t.Fatalf("zero window/memory produced %+v", cpu)
	}
}

func TestReadProcessSamplesSkipsInvalidEntries(t *testing.T) {
	root := t.TempDir()
	write := func(dir, content string) {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o700); err != nil {
			t.Fatal(err)
		}
		if content != "" {
			if err := os.WriteFile(filepath.Join(root, dir, "stat"), []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	write("42", stat(42, "nginx", 3, 4, 10, 20))
	write("43", "garbage")
	write("44", "")
	write("self", stat(1, "self", 1, 1, 1, 1))
	write("-1", stat(1, "neg", 1, 1, 1, 1))
	samples := readProcessSamples(root)
	if len(samples) != 1 || samples[42].name != "nginx" || samples[42].ticks != 7 {
		t.Fatalf("samples = %+v", samples)
	}
	if readProcessSamples(filepath.Join(root, "missing")) != nil {
		t.Fatal("missing root returned samples")
	}
}

func TestCollectTopProcessesUsesProcAndHonorsContext(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "5"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "5", "stat"), []byte(stat(5, "app", 1, 1, 1, 100)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "meminfo"), []byte("MemTotal:       1000 kB\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	previous := procRoot
	procRoot = root
	t.Cleanup(func() { procRoot = previous })

	topCPU, topMem := collectTopProcesses(context.Background(), 0)
	if len(topCPU) != 1 || topCPU[0].Name != "app" || topCPU[0].CPU != 0 || len(topMem) != 1 || topMem[0].Mem <= 0 {
		t.Fatalf("collected %+v %+v", topCPU, topMem)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if cpu, mem := collectTopProcesses(ctx, 1); cpu != nil || mem != nil {
		t.Fatal("cancelled collection returned processes")
	}
}
