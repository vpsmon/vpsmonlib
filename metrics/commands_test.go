package metrics

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCollectorCommandHelper(t *testing.T) {
	switch os.Getenv("VPSMON_COMMAND_HELPER") {
	case "hang":
		time.Sleep(time.Minute)
	case "flood":
		os.Stdout.Write(bytes.Repeat([]byte("x"), 2*commandOutputLimit))
	case "stderr":
		os.Stderr.Write(bytes.Repeat([]byte("x"), 2*commandOutputLimit))
	default:
		return
	}
	os.Exit(0)
}

func TestCommandsBoundTimeAndOutput(t *testing.T) {
	for _, mode := range []string{"hang", "flood", "stderr"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("VPSMON_COMMAND_HELPER", mode)
			timeout := 3 * time.Second
			if mode == "hang" {
				timeout = 150 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(t.Context(), timeout)
			defer cancel()
			start := time.Now()
			out, err := commandOutput(ctx, true, os.Args[0], "-test.run=^TestCollectorCommandHelper$")
			if err == nil || len(out) != 0 || time.Since(start) > timeout+time.Second {
				t.Fatalf("unbounded command: bytes=%d elapsed=%s err=%v", len(out), time.Since(start), err)
			}
			if mode == "hang" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("deadline: %v", err)
			}
			if mode != "hang" && !errors.Is(err, errCommandOutputLimit) {
				t.Fatalf("output limit: %v", err)
			}
		})
	}
}

func TestHungDockerDoesNotFreezeCoreCollection(t *testing.T) {
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte("#!/bin/sh\nexec sleep 30\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	start := time.Now()
	first := collectMetrics(Options{Containers: true})
	second := collectMetrics(Options{Containers: true})
	if !first.Timestamp.After(start.Add(-time.Second)) || !second.Timestamp.After(first.Timestamp) || time.Since(start) > 9*time.Second {
		t.Fatalf("frozen collector: first=%s second=%s elapsed=%s", first.Timestamp, second.Timestamp, time.Since(start))
	}
	if first.CPUCount == 0 || len(first.Containers) != 0 {
		t.Fatalf("core metrics missing or hung details published: %#v", first)
	}
}
