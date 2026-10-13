//go:build unix

package metrics

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestCommandTimeoutKillsDescendants(t *testing.T) {
	dir := t.TempDir()
	pidfile := filepath.Join(dir, "child.pid")
	script := filepath.Join(dir, "spawn")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nsleep 30 &\necho $! > \"$PIDFILE\"\nwait\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PIDFILE", pidfile)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := commandOutput(ctx, false, script)
	if err == nil {
		t.Fatal("expected timeout")
	}
	data, readErr := os.ReadFile(pidfile)
	if readErr != nil {
		t.Fatalf("child did not start: %v", readErr)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		err = syscall.Kill(pid, 0)
		if err == syscall.ESRCH {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("descendant %d survived command timeout", pid)
}

func TestCommandEarlyExitKillsDescendantsHoldingPipes(t *testing.T) {
	dir := t.TempDir()
	pidfile := filepath.Join(dir, "child.pid")
	script := filepath.Join(dir, "spawn")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nsleep 30 &\necho $! > \"$PIDFILE\"\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PIDFILE", pidfile)
	started := time.Now()
	_, _ = commandOutput(context.Background(), false, script)
	if time.Since(started) > time.Second {
		t.Fatal("command did not respect pipe wait bound")
	}
	data, err := os.ReadFile(pidfile)
	if err != nil {
		t.Fatalf("child did not start: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if err = syscall.Kill(pid, 0); err == syscall.ESRCH {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("descendant %d survived early command exit", pid)
}
