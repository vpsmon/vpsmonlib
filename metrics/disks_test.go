package metrics

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCapacityMountExcludesReadOnlyImages(t *testing.T) {
	for _, tt := range []struct {
		device, filesystem, options string
		want                        bool
	}{
		{"/dev/vda1", "ext4", "rw,relatime", true},
		{"/dev/loop1", "ext4", "rw", true},
		{"/dev/vdb1", "xfs", "rw", true},
		{"/dev/loop0", "squashfs", "ro,nodev", false},
		{"/dev/loop0", "squashfs", "rw", false},
		{"/dev/sr0", "iso9660", "ro", false},
		// ProtectSystem=strict makes the real root disk appear read-only.
		{"/dev/vdc1", "ext4", "ro,relatime", true},
		{"tmpfs", "tmpfs", "rw", false},
		{"tank/data", "zfs", "rw,xattr", true},
		{"server:/export", "nfs4", "rw", false},
		{"none", "binfmt_misc", "rw", false},
		{"none", "efivarfs", "rw", false},
		{"pool/dataset", "zfs", "rw", true},
		{"unknown", "unknownfs", "rw", false},
	} {
		if got := capacityMount(tt.device, tt.filesystem, tt.options); got != tt.want {
			t.Errorf("capacityMount(%q, %q, %q) = %v", tt.device, tt.filesystem, tt.options, got)
		}
	}
}

func TestDecodeMountField(t *testing.T) {
	got := decodeMountField(`/mnt/a\040b\011c\012d\134e`)
	if got != "/mnt/a b\tc\nd\\e" {
		t.Fatalf("decoded path %q", got)
	}
}

func TestDiskProbesAreBoundedAndRecover(t *testing.T) {
	var active, maxActive atomic.Int32
	var slowCalls atomic.Int32
	entered := make(chan struct{}, diskProbeWorkers)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	pool := newDiskProbePool(diskProbeWorkers, diskProbeQueue, func(path string, stat *syscallStatfs) error {
		n := active.Add(1)
		for prior := maxActive.Load(); n > prior && !maxActive.CompareAndSwap(prior, n); prior = maxActive.Load() {
		}
		if strings.HasPrefix(path, "/slow") {
			slowCalls.Add(1)
			entered <- struct{}{}
			<-release
		}
		active.Add(-1)
		stat.Bsize, stat.Blocks, stat.Bavail = 4096, 10, 5
		return nil
	})
	t.Cleanup(func() { releaseAll(); pool.close() })
	mounts := strings.NewReader("zfs/ds /slow zfs rw 0 0\nzfs/ds2 /healthy zfs rw 0 0\n")
	started := time.Now()
	got := diskInfoFromMountsWithPool(mounts, pool)
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("blocked collection for %s", elapsed)
	}
	if len(got) != 1 || got[0].Mount != "/healthy" {
		t.Fatalf("healthy result behind stalled mount was lost: %#v", got)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not enter statfs")
	}
	for i := 0; i < 4; i++ {
		got = diskInfoFromMountsWithPool(strings.NewReader("zfs/ds /slow zfs rw 0 0\nzfs/ds2 /healthy zfs rw 0 0\n"), pool)
		if len(got) != 1 || got[0].Mount != "/healthy" {
			t.Fatalf("healthy disk lost on repeat %d: %#v", i, got)
		}
		if len(pool.jobs) > diskProbeQueue {
			t.Fatalf("probe queue exceeded bound: %d", len(pool.jobs))
		}
	}
	if slowCalls.Load() != 1 {
		t.Fatalf("same hung mount was probed %d times", slowCalls.Load())
	}
	if maxActive.Load() > diskProbeWorkers {
		t.Fatalf("active statfs calls exceeded worker bound: %d", maxActive.Load())
	}
	releaseAll()
	got = diskInfoFromMountsWithPool(strings.NewReader("tank/data /zfs zfs rw 0 0\n"), pool)
	if len(got) != 1 || got[0].Mount != "/zfs" {
		t.Fatalf("recovery/ZFS probe: %#v", got)
	}
	var probedPath string
	pathPool := newDiskProbePool(1, 2, func(path string, stat *syscallStatfs) error {
		probedPath = path
		stat.Bsize, stat.Blocks, stat.Bavail = 4096, 10, 5
		return nil
	})
	t.Cleanup(pathPool.close)
	got = diskInfoFromMountsWithPool(strings.NewReader("tank/data /mnt/a\\040b zfs rw 0 0\n"), pathPool)
	if len(got) != 1 || probedPath != "/mnt/a b" {
		t.Fatalf("escaped mount path was not decoded: path=%q disks=%#v", probedPath, got)
	}
}

func TestDistinctHungDiskProbesUseAtMostWorkerCount(t *testing.T) {
	var calls, active, maxActive atomic.Int32
	entered := make(chan struct{}, diskProbeWorkers)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	pool := newDiskProbePool(diskProbeWorkers, diskProbeQueue, func(_ string, stat *syscallStatfs) error {
		calls.Add(1)
		n := active.Add(1)
		for prior := maxActive.Load(); n > prior && !maxActive.CompareAndSwap(prior, n); prior = maxActive.Load() {
		}
		entered <- struct{}{}
		<-release
		active.Add(-1)
		stat.Bsize, stat.Blocks, stat.Bavail = 4096, 10, 5
		return nil
	})
	t.Cleanup(func() { releaseAll(); pool.close() })
	var mounts strings.Builder
	for i := 0; i < 10; i++ {
		fmt.Fprintf(&mounts, "tank/%d /hang-%d zfs rw 0 0\n", i, i)
	}
	input := mounts.String()
	_ = diskInfoFromMountsWithPool(strings.NewReader(input), pool)
	for i := 0; i < diskProbeWorkers; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("hung probes did not start")
		}
	}
	_ = diskInfoFromMountsWithPool(strings.NewReader(input), pool)
	if calls.Load() != diskProbeWorkers || maxActive.Load() > diskProbeWorkers {
		t.Fatalf("distinct hung probes exceeded worker bound: calls=%d active=%d", calls.Load(), maxActive.Load())
	}
	if pool.outstandingCount() > diskProbeWorkers+diskProbeQueue || len(pool.jobs) > diskProbeQueue {
		t.Fatalf("outstanding work exceeded bound: outstanding=%d queued=%d", pool.outstandingCount(), len(pool.jobs))
	}
}
