//go:build unix

package metrics

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Small retained fields must not pin each command's entire near-limit output.
// Sixteen samples retained over 29 MiB before the substring copies were fixed.
func TestHistoryDetailsDoNotRetainCommandBuffers(t *testing.T) {
	for _, kind := range []string{"processes", "containers", "gpus"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			padding := strings.Repeat("ignored\n", 120000)
			fixtures := map[string]string{
				"ps":      strings.Repeat(" 0.0 0.0 worker\n", 60000),
				"docker":  strings.Repeat("a", 64) + "\tapp\timage\tUp 1 hour\t80/tcp\n" + padding,
				"inspect": strings.Repeat("a", 64) + "\t0\t2026-01-01T00:00:00Z\thealthy\n" + padding,
				"gpu":     "Example GPU, 10, 20, 100, 40\n" + padding,
			}
			for name, data := range fixtures {
				if err := os.WriteFile(filepath.Join(dir, name+".out"), []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
			}
			scripts := map[string]string{
				"ps":         "exec cat \"$VPSMON_RETENTION_FIXTURES/ps.out\"",
				"nvidia-smi": "exec cat \"$VPSMON_RETENTION_FIXTURES/gpu.out\"",
				"docker":     "case \"$1\" in ps) cat \"$VPSMON_RETENTION_FIXTURES/docker.out\";; inspect) cat \"$VPSMON_RETENTION_FIXTURES/inspect.out\";; esac",
			}
			for name, script := range scripts {
				if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+script+"\n"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("VPSMON_RETENTION_FIXTURES", dir)
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			history := make([]Metrics, 16)
			for i := range history {
				switch kind {
				case "processes":
					history[i].TopCPU = getTopProcessesContext(context.Background(), false)
					history[i].TopMem = getTopProcessesContext(context.Background(), true)
					if len(history[i].TopCPU) != 5 || history[i].TopCPU[0].Name != "worker" || len(history[i].TopMem) != 5 {
						t.Fatal("process fixture was not collected")
					}
				case "containers":
					history[i].Containers = getDockerContainersContext(context.Background())
					if len(history[i].Containers) != 1 || history[i].Containers[0].Health != "healthy" {
						t.Fatal("container fixture was not collected")
					}
				case "gpus":
					history[i].GPUs = getNVIDIAGPUsContext(context.Background())
					if len(history[i].GPUs) != 1 || history[i].GPUs[0].Name != "Example GPU" {
						t.Fatal("GPU fixture was not collected")
					}
				}
			}
			runtime.GC()
			runtime.ReadMemStats(&after)
			retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
			if retained > 8<<20 {
				t.Fatalf("small history fields retained %d bytes of command output", retained)
			}
			runtime.KeepAlive(history)
		})
	}
}
