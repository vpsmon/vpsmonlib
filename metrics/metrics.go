package metrics

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const historySize = 120

var (
	metricsMu      sync.RWMutex
	metricsHistory []Metrics
	containerIDRE  = regexp.MustCompile(`^[a-fA-F0-9]{12,64}$`)
)

type TopProcess struct {
	Name string  `json:"name"`
	CPU  float64 `json:"cpu"`
	Mem  float64 `json:"mem"`
}

type DockerContainer struct {
	ID           string  `json:"id"`
	Name         string  `json:"name"`
	Image        string  `json:"image"`
	Status       string  `json:"status"`
	Health       string  `json:"health"`
	Uptime       string  `json:"uptime"`
	RestartCount int     `json:"restart_count"`
	Ports        string  `json:"ports"`
	CPU          float64 `json:"cpu"`
	Mem          float64 `json:"mem"`
}

type GPUInfo struct {
	Name        string  `json:"name"`
	Utilization float64 `json:"utilization"`
	MemoryUsed  uint64  `json:"memory_used"`
	MemoryTotal uint64  `json:"memory_total"`
	Temperature int     `json:"temperature"`
}

type Metrics struct {
	Hostname             string            `json:"hostname"`
	Uptime               string            `json:"uptime"`
	LoadAvg              string            `json:"load_avg"`
	CPUCount             int               `json:"cpu_count"`
	CPUUsage             float64           `json:"cpu_usage"`
	MemTotal             uint64            `json:"mem_total"`
	MemUsed              uint64            `json:"mem_used"`
	MemFree              uint64            `json:"mem_free"`
	MemPercent           float64           `json:"mem_percent"`
	SwapTotal            uint64            `json:"swap_total"`
	SwapUsed             uint64            `json:"swap_used"`
	SwapPercent          float64           `json:"swap_percent"`
	Disks                []DiskInfo        `json:"disks"`
	DiskCollectionStatus string            `json:"disk_collection_status,omitempty"`
	NetRx                uint64            `json:"net_rx"`
	NetTx                uint64            `json:"net_tx"`
	NetRxSpeed           float64           `json:"net_rx_speed"`
	NetTxSpeed           float64           `json:"net_tx_speed"`
	Processes            int               `json:"processes"`
	TopCPU               []TopProcess      `json:"top_cpu"`
	TopMem               []TopProcess      `json:"top_mem"`
	Containers           []DockerContainer `json:"containers"`
	GPUs                 []GPUInfo         `json:"gpus"`
	Listeners            []ListeningSocket `json:"listeners,omitempty"`
	Timestamp            time.Time         `json:"timestamp"`
}

type DiskInfo struct {
	Mount   string  `json:"mount"`
	Device  string  `json:"device"`
	Total   uint64  `json:"total"`
	Used    uint64  `json:"used"`
	Free    uint64  `json:"free"`
	Percent float64 `json:"percent"`
}

const (
	DiskCollectionComplete    = "complete"
	DiskCollectionPartial     = "partial"
	DiskCollectionUnavailable = "unavailable"
)

// Options controls optional details. Core host metrics are always collected.
type Options struct {
	TopProcesses bool
	Containers   bool
	GPUs         bool
	Listeners    bool
}

var DashboardOptions = Options{TopProcesses: true, Containers: true, GPUs: true, Listeners: true}

func collectMetrics(options Options) Metrics {
	m := Metrics{
		CPUCount:  runtime.NumCPU(),
		Timestamp: time.Now(),
	}

	// Hostname
	m.Hostname, _ = os.Hostname()

	// Uptime
	if data, err := os.ReadFile("/proc/uptime"); err == nil {
		parts := strings.Fields(string(data))
		if len(parts) > 0 {
			if secs, err := strconv.ParseFloat(parts[0], 64); err == nil {
				d := time.Duration(secs) * time.Second
				days := int(d.Hours()) / 24
				hours := int(d.Hours()) % 24
				mins := int(d.Minutes()) % 60
				m.Uptime = fmt.Sprintf("%dd %dh %dm", days, hours, mins)
			}
		}
	}

	// Load average
	if data, err := os.ReadFile("/proc/loadavg"); err == nil {
		parts := strings.Fields(string(data))
		if len(parts) >= 3 {
			m.LoadAvg = strings.Join(parts[:3], " ")
		}
	}

	// CPU usage from /proc/stat
	m.CPUUsage = getCPUUsage()

	// Memory from /proc/meminfo
	meminfo := parseKeyValueFile("/proc/meminfo")
	m.MemTotal = parseKB(meminfo["MemTotal"])
	memFree := parseKB(meminfo["MemFree"])
	buffers := parseKB(meminfo["Buffers"])
	cached := parseKB(meminfo["Cached"])
	m.MemFree = memFree + buffers + cached
	m.MemUsed = m.MemTotal - m.MemFree
	if m.MemTotal > 0 {
		m.MemPercent = float64(m.MemUsed) / float64(m.MemTotal) * 100
	}
	m.SwapTotal = parseKB(meminfo["SwapTotal"])
	swapFree := parseKB(meminfo["SwapFree"])
	m.SwapUsed = m.SwapTotal - swapFree
	if m.SwapTotal > 0 {
		m.SwapPercent = float64(m.SwapUsed) / float64(m.SwapTotal) * 100
	}

	// Disk usage from /proc/mounts + syscall
	m.Disks, m.DiskCollectionStatus = getDiskInfoWithStatus()

	// Network from /proc/net/dev
	m.NetRx, m.NetTx = getNetworkBytes()

	// Process count
	if entries, err := os.ReadDir("/proc"); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				if _, err := strconv.Atoi(e.Name()); err == nil {
					m.Processes++
				}
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), detailTimeout)
	defer cancel()
	var details sync.WaitGroup
	if options.TopProcesses {
		details.Go(func() { m.TopCPU = getTopProcessesContext(ctx, false); m.TopMem = getTopProcessesContext(ctx, true) })
	}
	if options.Containers {
		details.Go(func() { m.Containers = getDockerContainersContext(ctx) })
	}
	if options.GPUs {
		details.Go(func() { m.GPUs = getGPUsContext(ctx) })
	}
	if options.Listeners {
		details.Go(func() { m.Listeners = getListeningSockets() })
	}
	details.Wait()

	return m
}

func getGPUsContext(ctx context.Context) []GPUInfo {
	return append(getNVIDIAGPUsContext(ctx), getAMDGPUsContext(ctx)...)
}

func getNVIDIAGPUsContext(ctx context.Context) []GPUInfo {
	out, err := commandOutput(ctx, false, "nvidia-smi", "--query-gpu=name,utilization.gpu,memory.used,memory.total,temperature.gpu", "--format=csv,noheader,nounits")
	if err != nil {
		return nil
	}

	var gpus []GPUInfo
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Split(strings.TrimSpace(line), ",")
		if len(fields) != 5 {
			continue
		}

		utilization, err := strconv.ParseFloat(strings.TrimSpace(fields[1]), 64)
		if err != nil {
			continue
		}
		memoryUsed, err := strconv.ParseUint(strings.TrimSpace(fields[2]), 10, 64)
		if err != nil {
			continue
		}
		memoryTotal, err := strconv.ParseUint(strings.TrimSpace(fields[3]), 10, 64)
		if err != nil {
			continue
		}
		temperature, err := strconv.Atoi(strings.TrimSpace(fields[4]))
		if err != nil {
			continue
		}

		gpus = append(gpus, GPUInfo{
			Name:        strings.Clone(strings.TrimSpace(fields[0])),
			Utilization: utilization,
			MemoryUsed:  memoryUsed * 1024 * 1024,
			MemoryTotal: memoryTotal * 1024 * 1024,
			Temperature: temperature,
		})
	}

	return gpus
}

func getAMDGPUsContext(ctx context.Context) []GPUInfo {
	out, err := commandOutput(ctx, false, "amd-smi", "monitor", "-tuv", "--csv")
	if err != nil {
		return nil
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) < 2 {
		return nil
	}

	columns := map[string]int{}
	for i, column := range strings.Split(lines[0], ",") {
		columns[strings.TrimSpace(column)] = i
	}

	required := []string{"GPU", "GPU_TEMP", "GFX_UTIL", "VRAM_USED", "VRAM_TOTAL"}
	for _, column := range required {
		if _, ok := columns[column]; !ok {
			return nil
		}
	}

	var gpus []GPUInfo
	for _, line := range lines[1:] {
		fields := strings.Split(line, ",")
		if len(fields) != len(columns) {
			continue
		}

		utilization, ok := parseAMDMetric(fields[columns["GFX_UTIL"]])
		if !ok {
			continue
		}
		memoryUsed, ok := parseAMDMetric(fields[columns["VRAM_USED"]])
		if !ok {
			continue
		}
		memoryTotal, ok := parseAMDMetric(fields[columns["VRAM_TOTAL"]])
		if !ok {
			continue
		}
		temperature, ok := parseAMDMetric(fields[columns["GPU_TEMP"]])
		if !ok {
			continue
		}

		gpus = append(gpus, GPUInfo{
			Name:        "AMD GPU " + strings.TrimSpace(fields[columns["GPU"]]),
			Utilization: utilization,
			MemoryUsed:  uint64(memoryUsed * 1024 * 1024),
			MemoryTotal: uint64(memoryTotal * 1024 * 1024),
			Temperature: int(temperature),
		})
	}

	return gpus
}

func parseAMDMetric(value string) (float64, bool) {
	parts := strings.Fields(strings.TrimSpace(value))
	if len(parts) == 0 {
		return 0, false
	}
	metric, err := strconv.ParseFloat(parts[0], 64)
	return metric, err == nil
}

func getDockerContainersContext(ctx context.Context) []DockerContainer {
	out, err := commandOutput(ctx, false, "docker", "ps", "-a", "--no-trunc", "--format", "{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}")
	if err != nil {
		return nil
	}

	containersByID := map[string]*DockerContainer{}
	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		line = strings.TrimSuffix(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) != 5 {
			continue
		}
		id := strings.Clone(parts[0])
		containersByID[id] = &DockerContainer{
			ID:     id,
			Name:   strings.Clone(parts[1]),
			Image:  strings.Clone(parts[2]),
			Status: strings.Clone(parts[3]),
			Ports:  strings.Clone(parts[4]),
			Health: "none",
			Uptime: "—",
		}
	}

	if len(containersByID) == 0 {
		return nil
	}

	populateDockerStatsContext(ctx, containersByID)
	populateDockerDetailsContext(ctx, containersByID)

	containers := make([]DockerContainer, 0, len(containersByID))
	for _, container := range containersByID {
		containers = append(containers, *container)
	}
	return containers
}

func populateDockerStatsContext(ctx context.Context, containersByID map[string]*DockerContainer) {
	out, err := commandOutput(ctx, false, "docker", "stats", "--no-stream", "--no-trunc", "--format", "{{.ID}}\t{{.CPUPerc}}\t{{.MemPerc}}")
	if err != nil {
		return
	}

	for _, line := range strings.Split(string(out), "\n") {
		parts := strings.Split(strings.TrimSpace(line), "\t")
		if len(parts) != 3 {
			continue
		}
		container, ok := containersByID[parts[0]]
		if !ok {
			continue
		}
		container.CPU, _ = strconv.ParseFloat(strings.TrimSuffix(parts[1], "%"), 64)
		container.Mem, _ = strconv.ParseFloat(strings.TrimSuffix(parts[2], "%"), 64)
	}
}

func populateDockerDetailsContext(ctx context.Context, containersByID map[string]*DockerContainer) {
	ids := make([]string, 0, len(containersByID))
	for id := range containersByID {
		ids = append(ids, id)
	}

	args := append([]string{"inspect", "--format", "{{.Id}}\t{{.RestartCount}}\t{{.State.StartedAt}}\t{{with index .State \"Health\"}}{{.Status}}{{else}}none{{end}}"}, ids...)
	out, err := commandOutput(ctx, false, "docker", args...)
	if err != nil {
		return
	}

	for _, line := range strings.Split(string(out), "\n") {
		parts := strings.Split(strings.TrimSpace(line), "\t")
		if len(parts) != 4 {
			continue
		}
		container, ok := containersByID[parts[0]]
		if !ok {
			continue
		}
		container.RestartCount, _ = strconv.Atoi(parts[1])
		container.Health = strings.Clone(parts[3])
		if startedAt, err := time.Parse(time.RFC3339Nano, parts[2]); err == nil && !startedAt.IsZero() && strings.HasPrefix(container.Status, "Up ") {
			container.Uptime = formatUptime(time.Since(startedAt))
		}
	}
}

func formatUptime(duration time.Duration) string {
	if duration < 0 {
		return "—"
	}
	days := int(duration.Hours()) / 24
	hours := int(duration.Hours()) % 24
	minutes := int(duration.Minutes()) % 60
	return fmt.Sprintf("%dd %dh %dm", days, hours, minutes)
}

func GetContainerLogs(id string) (string, error) {
	if !containerIDRE.MatchString(id) {
		return "", fmt.Errorf("invalid container ID")
	}

	out, err := commandOutput(context.Background(), true, "docker", "logs", "--tail", "100", "--timestamps", id)
	if err != nil {
		return "", fmt.Errorf("read container logs: %w", err)
	}
	return string(out), nil
}

func getTopProcessesContext(ctx context.Context, sortByMem bool) []TopProcess {
	var procs []TopProcess
	sortArg := "-%cpu"
	if sortByMem {
		sortArg = "-%mem"
	}

	out, err := commandOutput(ctx, false, "ps", "-eo", "pcpu,pmem,comm", "--sort="+sortArg, "--no-headers")
	if err != nil {
		return procs
	}

	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		cpu, _ := strconv.ParseFloat(fields[0], 64)
		mem, _ := strconv.ParseFloat(fields[1], 64)
		name := strings.Join(fields[2:], " ")

		// History keeps only five names; do not keep the entire ps response
		// alive through a substring when a name contains a single word.
		name = strings.Clone(filepath.Base(name))

		procs = append(procs, TopProcess{
			Name: name,
			CPU:  cpu,
			Mem:  mem,
		})

		if len(procs) >= 5 {
			break
		}
	}
	return procs
}

// CollectProcessDetails reads process names and percentages on demand. It does
// not collect command arguments, environment variables, logs, or containers.
func CollectProcessDetails() (topCPU, topMem []TopProcess) {
	ctx, cancel := context.WithTimeout(context.Background(), detailTimeout)
	defer cancel()
	return getTopProcessesContext(ctx, false), getTopProcessesContext(ctx, true)
}

// A service sandbox can remount real host disks read-only in its namespace.
// Mount permissions therefore do not tell us whether host capacity matters.
// Exclude immutable application/media images by filesystem type instead.
func capacityMount(device, filesystem, _ string) bool {
	switch filesystem {
	case "zfs":
		return true
	case "proc", "sysfs", "devtmpfs", "devpts", "tmpfs", "cgroup", "cgroup2", "pstore", "securityfs", "debugfs", "tracefs", "configfs", "fusectl", "mqueue", "hugetlbfs", "rpc_pipefs", "autofs", "overlay", "squashfs", "iso9660", "nfs", "nfs4", "cifs", "smb3", "9p", "ceph", "glusterfs", "fuse.sshfs", "fuse.rclone", "fuse.s3fs":
		return false
	}
	return strings.HasPrefix(device, "/dev/")
}

func getCPUUsage() float64 {
	read := func() (idle, total uint64, ok bool) {
		data, err := os.ReadFile("/proc/stat")
		if err != nil {
			return
		}
		return parseCPUStat(data)
	}

	idle1, total1, ok := read()
	if !ok {
		return 0
	}
	time.Sleep(500 * time.Millisecond)
	idle2, total2, ok := read()
	if !ok {
		return 0
	}
	return cpuUsageBetween(idle1, total1, idle2, total2)
}

func parseCPUStat(data []byte) (idle, total uint64, ok bool) {
	line, _, _ := strings.Cut(string(data), "\n")
	fields := strings.Fields(line)
	if len(fields) < 5 || fields[0] != "cpu" {
		return 0, 0, false
	}
	// Linux already includes guest and guest_nice in user and nice. Sum only
	// user, nice, system, idle, iowait, irq, softirq and steal.
	for i, field := range fields[1:min(len(fields), 9)] {
		value, err := strconv.ParseUint(field, 10, 64)
		if err != nil || total+value < total {
			return 0, 0, false
		}
		total += value
		if i == 3 {
			idle = value
		}
	}
	return idle, total, true
}

func cpuUsageBetween(idle1, total1, idle2, total2 uint64) float64 {
	if total2 <= total1 || idle2 < idle1 {
		return 0
	}
	idleDelta, totalDelta := idle2-idle1, total2-total1
	if idleDelta > totalDelta {
		return 0
	}
	return (1.0 - float64(idleDelta)/float64(totalDelta)) * 100
}

const diskProbeTimeout = 300 * time.Millisecond
const diskProbeWorkerWait = 100 * time.Millisecond

// statfs may block in the kernel beyond context cancellation. Scheduler workers
// therefore wait only until the collection deadline while the bounded probe
// goroutine remains reserved until the syscall returns.
const diskProbeWorkers = 2
const diskProbeQueue = 32
const maxDiskProbes = 32
const maxOutstandingDiskProbes = 32

type diskProbe struct {
	index         int
	mount, device string
	ctx           context.Context
	result        chan diskProbeResult
}
type diskProbeResult struct {
	index int
	info  DiskInfo
	ok    bool
}

type diskProbePool struct {
	jobs        chan diskProbe
	probe       func(string, *syscallStatfs) error
	mu          sync.Mutex
	outstanding map[string]struct{}
	wg          sync.WaitGroup
	probeWG     sync.WaitGroup
}

func newDiskProbePool(workers, queueSize int, probe func(string, *syscallStatfs) error) *diskProbePool {
	pool := &diskProbePool{jobs: make(chan diskProbe, queueSize), probe: probe, outstanding: make(map[string]struct{})}
	for i := 0; i < workers; i++ {
		pool.wg.Add(1)
		go pool.worker()
	}
	return pool
}

func (pool *diskProbePool) enqueue(job diskProbe) bool {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if _, exists := pool.outstanding[job.mount]; exists || len(pool.outstanding) >= maxOutstandingDiskProbes {
		return false
	}
	pool.outstanding[job.mount] = struct{}{}
	select {
	case pool.jobs <- job:
		return true
	default:
		delete(pool.outstanding, job.mount)
		return false
	}
}

func (pool *diskProbePool) finish(mount string) {
	pool.mu.Lock()
	delete(pool.outstanding, mount)
	pool.mu.Unlock()
}

func (pool *diskProbePool) outstandingCount() int {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return len(pool.outstanding)
}

func (pool *diskProbePool) close() {
	close(pool.jobs)
	pool.wg.Wait()
	pool.probeWG.Wait()
}

func (pool *diskProbePool) worker() {
	defer pool.wg.Done()
	for job := range pool.jobs {
		if job.ctx.Err() != nil {
			pool.finish(job.mount)
			continue
		}
		done := make(chan struct{})
		pool.probeWG.Add(1)
		go func() {
			defer pool.probeWG.Done()
			result := pool.probeJob(job)
			if job.ctx.Err() == nil {
				// Deliver independently of the scheduler wait: a mount can take
				// longer than the worker wait and still finish before collection
				// times out. The result channel is buffered for every probe slot.
				job.result <- result
			}
			// Release only after the actual syscall has returned. A timed-out
			// caller cannot cause duplicate probes for this mount.
			pool.finish(job.mount)
			close(done)
		}()
		select {
		case <-done:
		case <-job.ctx.Done():
			// The probe continues in its bounded goroutine; this scheduler is
			// immediately available for another mount.
		case <-time.After(diskProbeWorkerWait):
			// Free the scheduler before the collection deadline so queued
			// healthy mounts still have time to return a fresh sample.
		}
	}
}

func (pool *diskProbePool) probeJob(job diskProbe) diskProbeResult {
	var stat syscallStatfs
	if err := pool.probe(job.mount, &stat); err != nil || stat.Bsize <= 0 {
		return diskProbeResult{index: job.index}
	}
	total := stat.Blocks * uint64(stat.Bsize)
	free := stat.Bavail * uint64(stat.Bsize)
	used := uint64(0)
	if free <= total {
		used = total - free
	}
	pct := 0.0
	if total > 0 {
		pct = float64(used) / float64(total) * 100
	}
	return diskProbeResult{index: job.index, info: DiskInfo{Mount: job.mount, Device: job.device, Total: total, Used: used, Free: free, Percent: pct}, ok: true}
}

var globalDiskProbePool = newDiskProbePool(diskProbeWorkers, diskProbeQueue, statfs)

func getDiskInfo() []DiskInfo {
	disks, _ := getDiskInfoWithStatus()
	return disks
}

func getDiskInfoWithStatus() ([]DiskInfo, string) {
	f, err := os.Open("/proc/mounts")
	if err != nil {
		return nil, DiskCollectionUnavailable
	}
	defer f.Close()
	return diskInfoFromMountsWithStatus(f, globalDiskProbePool)
}

func diskInfoFromMounts(r io.Reader) []DiskInfo {
	return diskInfoFromMountsWithPool(r, globalDiskProbePool)
}

func diskInfoFromMountsWithPool(r io.Reader, pool *diskProbePool) []DiskInfo {
	disks, _ := diskInfoFromMountsWithStatus(r, pool)
	return disks
}

func diskInfoFromMountsWithStatus(r io.Reader, pool *diskProbePool) ([]DiskInfo, string) {
	var disks []DiskInfo
	ctx, cancel := context.WithTimeout(context.Background(), diskProbeTimeout)
	defer cancel()
	results := make(chan diskProbeResult, maxDiskProbes)
	type probeSlot struct {
		mount     string
		submitted bool
	}
	var slots []probeSlot
	eligible := 0
	inputIncomplete := false
	seen := map[string]bool{}
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 4 {
			inputIncomplete = true
			continue
		}
		device, mount := fields[0], decodeMountField(fields[1])
		if !capacityMount(device, fields[2], fields[3]) {
			continue
		}
		if seen[mount] {
			continue
		}
		eligible++
		if len(slots) >= maxDiskProbes {
			inputIncomplete = true
			continue
		}
		seen[mount] = true
		index := len(slots)
		slots = append(slots, probeSlot{mount: mount})
		if pool.enqueue(diskProbe{index: index, mount: mount, device: filepath.Base(device), ctx: ctx, result: results}) {
			slots[index].submitted = true
		}
	}
	if scanner.Err() != nil {
		inputIncomplete = true
	}
	completed := make([]bool, len(slots))
	resultsByIndex := make([]diskProbeResult, len(slots))
	remaining := 0
	for i := range slots {
		if slots[i].submitted {
			remaining++
		}
	}
	for remaining > 0 {
		select {
		case result := <-results:
			if result.index >= 0 && result.index < len(completed) && !completed[result.index] {
				completed[result.index] = true
				remaining--
				if result.ok {
					resultsByIndex[result.index] = result
				}
			}
		case <-ctx.Done():
			remaining = 0
		}
	}
	for i, result := range resultsByIndex {
		if i < len(slots) && result.ok {
			disks = append(disks, result.info)
		}
	}
	if len(disks) == 0 {
		return disks, DiskCollectionUnavailable
	}
	if inputIncomplete || len(disks) < eligible {
		return disks, DiskCollectionPartial
	}
	return disks, DiskCollectionComplete
}

func decodeMountField(s string) string {
	return strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`).Replace(s)
}

func getNetworkBytes() (rx, tx uint64) {
	f, err := os.Open("/proc/net/dev")
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.Contains(line, ":") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		iface := strings.TrimSpace(parts[0])
		if iface == "lo" {
			continue
		}
		fields := strings.Fields(parts[1])
		if len(fields) < 10 {
			continue
		}
		r, _ := strconv.ParseUint(fields[0], 10, 64)
		t, _ := strconv.ParseUint(fields[8], 10, 64)
		rx += r
		tx += t
	}
	return
}

func parseKeyValueFile(path string) map[string]string {
	result := map[string]string{}
	data, err := os.ReadFile(path)
	if err != nil {
		return result
	}
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			result[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	return result
}

func parseKB(s string) uint64 {
	s = strings.TrimSuffix(s, " kB")
	s = strings.TrimSpace(s)
	v, _ := strconv.ParseUint(s, 10, 64)
	return v * 1024
}

func StartCollector() {
	StartCollectorWithOptions(DashboardOptions)
}

// StartCollectorWithOptions collects immediately, then refreshes every five seconds.
func StartCollectorWithOptions(options Options) {
	collectAndStore(options)
	go func() {
		for {
			time.Sleep(5 * time.Second)
			collectAndStore(options)
		}
	}()
}

func collectAndStore(options Options) {
	m := collectMetrics(options)

	metricsMu.Lock()
	defer metricsMu.Unlock()

	if len(metricsHistory) > 0 {
		lastIndex := len(metricsHistory) - 1
		last := metricsHistory[lastIndex]
		dt := m.Timestamp.Sub(last.Timestamp).Seconds()
		if dt > 0 {
			if m.NetRx >= last.NetRx {
				m.NetRxSpeed = float64(m.NetRx-last.NetRx) / dt
			}
			if m.NetTx >= last.NetTx {
				m.NetTxSpeed = float64(m.NetTx-last.NetTx) / dt
			}
		}

		metricsHistory[lastIndex].Listeners = nil
	}

	metricsHistory = append(metricsHistory, m)
	if len(metricsHistory) > historySize {
		metricsHistory[0] = Metrics{}
		metricsHistory = metricsHistory[1:]
	}
}

func GetHistory() []Metrics {
	metricsMu.RLock()
	defer metricsMu.RUnlock()
	res := make([]Metrics, len(metricsHistory))
	copy(res, metricsHistory)
	return res
}

// GetLatest returns the most recent completed sample.
func GetLatest() (Metrics, bool) {
	metricsMu.RLock()
	defer metricsMu.RUnlock()
	if len(metricsHistory) == 0 {
		return Metrics{}, false
	}
	return metricsHistory[len(metricsHistory)-1], true
}
