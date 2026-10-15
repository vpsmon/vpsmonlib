package metrics

import (
	"context"
	"errors"
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

func TestDistinctHungDiskProbesStayWithinFixedBudget(t *testing.T) {
	var calls, active, maxActive atomic.Int32
	entered := make(chan struct{}, maxOutstandingDiskProbes)
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
	if calls.Load() == 0 || calls.Load() > maxOutstandingDiskProbes || maxActive.Load() > maxOutstandingDiskProbes {
		t.Fatalf("distinct hung probes exceeded fixed probe budget: calls=%d active=%d", calls.Load(), maxActive.Load())
	}
	if pool.outstandingCount() > maxOutstandingDiskProbes || len(pool.jobs) > diskProbeQueue {
		t.Fatalf("outstanding work exceeded bound: outstanding=%d queued=%d", pool.outstandingCount(), len(pool.jobs))
	}
}

func TestTwoHungDiskProbesDoNotStarveHealthyMounts(t *testing.T) {
	var active, maxActive, hungCalls atomic.Int32
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	pool := newDiskProbePool(diskProbeWorkers, diskProbeQueue, func(path string, stat *syscallStatfs) error {
		n := active.Add(1)
		for prior := maxActive.Load(); n > prior && !maxActive.CompareAndSwap(prior, n); prior = maxActive.Load() {
		}
		if path == "/hang-a" || path == "/hang-b" {
			hungCalls.Add(1)
			entered <- struct{}{}
			<-release
		}
		active.Add(-1)
		stat.Bsize, stat.Blocks, stat.Bavail = 4096, 10, 5
		return nil
	})
	t.Cleanup(func() { releaseAll(); pool.close() })
	input := "tank/a /hang-a zfs rw 0 0\ntank/b /hang-b zfs rw 0 0\ntank/c /healthy zfs rw 0 0\n"
	for i := 0; i < 5; i++ {
		got := diskInfoFromMountsWithPool(strings.NewReader(input), pool)
		if len(got) != 1 || got[0].Mount != "/healthy" {
			t.Fatalf("healthy disk lost on repeat %d: %#v", i, got)
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("both stalled mounts did not enter statfs")
		}
	}
	if hungCalls.Load() != 2 {
		t.Fatalf("same hung mounts were retried: calls=%d", hungCalls.Load())
	}
	if maxActive.Load() > maxOutstandingDiskProbes {
		t.Fatalf("active statfs calls exceeded fixed probe budget: %d", maxActive.Load())
	}
	if pool.outstandingCount() > maxOutstandingDiskProbes || len(pool.jobs) > diskProbeQueue {
		t.Fatalf("outstanding work exceeded bound: outstanding=%d queued=%d", pool.outstandingCount(), len(pool.jobs))
	}
	got, status := diskInfoFromMountsWithStatus(strings.NewReader(input), pool)
	if status != DiskCollectionPartial || len(got) != 1 || got[0].Mount != "/healthy" {
		t.Fatalf("partial availability not reported: status=%q disks=%#v", status, got)
	}
}

func TestChangingHungMountsStayWithinOutstandingProbeBudget(t *testing.T) {
	var calls atomic.Int32
	entered := make(chan struct{}, maxOutstandingDiskProbes)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	pool := newDiskProbePool(diskProbeWorkers, diskProbeQueue, func(_ string, stat *syscallStatfs) error {
		calls.Add(1)
		entered <- struct{}{}
		<-release
		stat.Bsize, stat.Blocks, stat.Bavail = 4096, 10, 5
		return nil
	})
	t.Cleanup(func() { releaseAll(); pool.close() })

	// Cancel each scheduling wait after its syscalls have started. The calls
	// themselves remain stuck and retain their mount reservations.
	for batch := 0; batch < maxOutstandingDiskProbes/diskProbeWorkers; batch++ {
		ctx, cancel := context.WithCancel(context.Background())
		for worker := 0; worker < diskProbeWorkers; worker++ {
			mount := fmt.Sprintf("/changing-%02d-%d", batch, worker)
			if !pool.enqueue(diskProbe{index: worker, mount: mount, ctx: ctx, result: make(chan diskProbeResult, 1)}) {
				cancel()
				t.Fatalf("failed to enqueue probe batch %d worker %d", batch, worker)
			}
		}
		for worker := 0; worker < diskProbeWorkers; worker++ {
			select {
			case <-entered:
			case <-time.After(time.Second):
				cancel()
				t.Fatal("scheduler did not start bounded probe")
			}
		}
		cancel()
	}

	if got := pool.outstandingCount(); got != maxOutstandingDiskProbes {
		t.Fatalf("outstanding probes = %d, want budget %d", got, maxOutstandingDiskProbes)
	}
	if calls.Load() != maxOutstandingDiskProbes {
		t.Fatalf("started calls = %d, want budget %d", calls.Load(), maxOutstandingDiskProbes)
	}
	for i := 0; i < 100; i++ {
		mount := fmt.Sprintf("/overflow-%d", i)
		if pool.enqueue(diskProbe{mount: mount, ctx: context.Background(), result: make(chan diskProbeResult, 1)}) {
			t.Fatalf("accepted mount beyond fixed outstanding budget: %q", mount)
		}
	}
	if got := pool.outstandingCount(); got > maxOutstandingDiskProbes {
		t.Fatalf("outstanding probes exceeded budget: %d", got)
	}
	if len(pool.jobs) > diskProbeQueue {
		t.Fatalf("queued jobs exceeded queue cap: %d", len(pool.jobs))
	}
	disks, status := diskInfoFromMountsWithStatus(strings.NewReader("tank/new /overflow zfs rw 0 0\n"), pool)
	if len(disks) != 0 || status != DiskCollectionUnavailable {
		t.Fatalf("budget exhaustion was not reported: status=%q disks=%#v", status, disks)
	}
}

func TestDiskCollectionStatus(t *testing.T) {
	pool := newDiskProbePool(1, 1, func(path string, stat *syscallStatfs) error {
		if path == "/bad" {
			return fmt.Errorf("unavailable")
		}
		stat.Bsize, stat.Blocks, stat.Bavail = 4096, 10, 5
		return nil
	})
	t.Cleanup(pool.close)
	for _, tt := range []struct {
		name, mounts, want string
	}{
		{"complete", "tank/a /ok zfs rw 0 0\n", DiskCollectionComplete},
		{"partial", "tank/a /ok zfs rw 0 0\ntank/b /bad zfs rw 0 0\n", DiskCollectionPartial},
		{"unavailable", "tank/b /bad zfs rw 0 0\n", DiskCollectionUnavailable},
		{"no eligible mounts", "tmpfs /run tmpfs rw 0 0\n", DiskCollectionUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, got := diskInfoFromMountsWithStatus(strings.NewReader(tt.mounts), pool)
			if got != tt.want {
				t.Fatalf("status = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSlowProbeFinishingBeforeCollectionDeadlineIsIncluded(t *testing.T) {
	pool := newDiskProbePool(1, 1, func(_ string, stat *syscallStatfs) error {
		time.Sleep(150 * time.Millisecond)
		stat.Bsize, stat.Blocks, stat.Bavail = 4096, 10, 5
		return nil
	})
	t.Cleanup(pool.close)
	disks, status := diskInfoFromMountsWithStatus(strings.NewReader("tank/a /slow zfs rw 0 0\n"), pool)
	if status != DiskCollectionComplete || len(disks) != 1 || disks[0].Mount != "/slow" {
		t.Fatalf("slow fresh probe was lost: status=%q disks=%#v", status, disks)
	}
}

type errorAfterReader struct {
	data []byte
	done bool
}

func (r *errorAfterReader) Read(p []byte) (int, error) {
	if !r.done {
		r.done = true
		return copy(p, r.data), nil
	}
	return 0, errors.New("mount source failed")
}

func TestIncompleteMountInputReportsPartialAndCapReportsPartial(t *testing.T) {
	pool := newDiskProbePool(2, maxDiskProbes, func(_ string, stat *syscallStatfs) error {
		stat.Bsize, stat.Blocks, stat.Bavail = 4096, 10, 5
		return nil
	})
	t.Cleanup(pool.close)

	input := &errorAfterReader{data: []byte("tank/a /ok zfs rw 0 0\n")}
	disks, status := diskInfoFromMountsWithStatus(input, pool)
	if status != DiskCollectionPartial || len(disks) != 1 {
		t.Fatalf("scanner error not reported as partial: status=%q disks=%#v", status, disks)
	}

	var mounts strings.Builder
	for i := 0; i < maxDiskProbes+1; i++ {
		fmt.Fprintf(&mounts, "tank/%d /mount-%d zfs rw 0 0\n", i, i)
	}
	disks, status = diskInfoFromMountsWithStatus(strings.NewReader(mounts.String()), pool)
	if status != DiskCollectionPartial || len(disks) != maxDiskProbes {
		t.Fatalf("mount count cap not reported as partial: status=%q disks=%d", status, len(disks))
	}
}
