//go:build linux

package queue

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

// refreshCpu returns instantaneous cgroup-aware CPU usage in millicpu (0-1000).
// Ported from go-zero core/stat/internal (cpu_linux.go + cgroup_linux.go) with
// the go-zero/x-sys helpers replaced by the standard library.
const (
	cpuTicks    = 100
	cpuFields   = 8
	cpuMaxMilli = 1000
	procStat    = "/proc/stat"
	cgroupDir   = "/sys/fs/cgroup"
	cpuMaxFile  = cgroupDir + "/cpu.max"
	cpuStatFile = cgroupDir + "/cpu.stat"
	cpusetFile  = cgroupDir + "/cpuset.cpus.effective"
)

var (
	preSystem   uint64
	preTotal    uint64
	cpuLimit    float64
	cpuCores    uint64
	noCgroup    bool
	cpuInitOnce sync.Once

	isUnifiedOnce sync.Once
	isUnified     bool
)

func refreshCpu() uint64 {
	cpuInitOnce.Do(func() {
		defer func() {
			if recover() != nil {
				noCgroup = true
			}
		}()
		if err := cpuInitialize(); err != nil {
			noCgroup = true
		}
	})

	if noCgroup {
		return 0
	}

	total, err := cgroupCpuUsage()
	if err != nil {
		return 0
	}
	system, err := systemCpuUsage()
	if err != nil {
		return 0
	}

	var usage uint64
	cpuDelta := total - preTotal
	systemDelta := system - preSystem
	if cpuDelta > 0 && systemDelta > 0 {
		usage = uint64(float64(cpuDelta*cpuCores*cpuMaxMilli) / (float64(systemDelta) * cpuLimit))
		if usage > cpuMaxMilli {
			usage = cpuMaxMilli
		}
	}
	preSystem = system
	preTotal = total

	return usage
}

func cpuInitialize() error {
	cpus, err := cgroupEffectiveCpus()
	if err != nil {
		return err
	}

	cpuCores = uint64(cpus)
	cpuLimit = float64(cpus)
	if quota, err := cgroupCpuQuota(); err == nil && quota > 0 && quota < cpuLimit {
		cpuLimit = quota
	}

	preSystem, err = systemCpuUsage()
	if err != nil {
		return err
	}
	preTotal, err = cgroupCpuUsage()
	return err
}

func systemCpuUsage() (uint64, error) {
	lines, err := readLines(procStat)
	if err != nil {
		return 0, err
	}

	for _, line := range lines {
		fields := strings.Fields(line)
		if fields[0] == "cpu" {
			if len(fields) < cpuFields {
				return 0, errors.New("bad format of cpu stats")
			}

			var totalClockTicks uint64
			for _, i := range fields[1:cpuFields] {
				v, err := parseUint(i)
				if err != nil {
					return 0, err
				}
				totalClockTicks += v
			}

			return (totalClockTicks * uint64(time.Second)) / cpuTicks, nil
		}
	}

	return 0, errors.New("bad stats format")
}

// --- cgroup abstraction ---

type cgroup interface {
	cpuQuota() (float64, error)
	cpuUsage() (uint64, error)
	effectiveCpus() (int, error)
}

func cgroupCpuQuota() (float64, error) {
	cg, err := currentCgroup()
	if err != nil {
		return 0, err
	}
	return cg.cpuQuota()
}

func cgroupCpuUsage() (uint64, error) {
	cg, err := currentCgroup()
	if err != nil {
		return 0, err
	}
	return cg.cpuUsage()
}

func cgroupEffectiveCpus() (int, error) {
	cg, err := currentCgroup()
	if err != nil {
		return 0, err
	}
	return cg.effectiveCpus()
}

func currentCgroup() (cgroup, error) {
	if isCgroup2Unified() {
		return currentCgroupV2()
	}
	return currentCgroupV1()
}

// isCgroup2Unified reports cgroup v2 unified mode. v2 exposes
// /sys/fs/cgroup/cgroup.controllers, which v1 does not.
func isCgroup2Unified() bool {
	isUnifiedOnce.Do(func() {
		if _, err := os.Stat(path.Join(cgroupDir, "cgroup.controllers")); err == nil {
			isUnified = true
		}
	})
	return isUnified
}

// --- cgroup v1 ---

type cgroupV1 struct {
	cgroups map[string]string
}

func (c *cgroupV1) cpuQuota() (float64, error) {
	quotaUs, err := c.readInt("cpu", "cpu.cfs_quota_us")
	if err != nil {
		return 0, err
	}
	if quotaUs == -1 {
		return -1, nil
	}
	periodUs, err := c.readUint("cpu", "cpu.cfs_period_us")
	if err != nil {
		return 0, err
	}
	return float64(quotaUs) / float64(periodUs), nil
}

func (c *cgroupV1) cpuUsage() (uint64, error) {
	return c.readUint("cpuacct", "cpuacct.usage")
}

func (c *cgroupV1) effectiveCpus() (int, error) {
	data, err := readText(path.Join(c.cgroups["cpuset"], "cpuset.cpus"))
	if err != nil {
		return 0, err
	}
	cpus, err := parseUints(data)
	if err != nil {
		return 0, err
	}
	return len(cpus), nil
}

func (c *cgroupV1) readUint(subsys, file string) (uint64, error) {
	data, err := readText(path.Join(c.cgroups[subsys], file))
	if err != nil {
		return 0, err
	}
	return parseUint(data)
}

func (c *cgroupV1) readInt(subsys, file string) (int64, error) {
	data, err := readText(path.Join(c.cgroups[subsys], file))
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(data, 10, 64)
}

func currentCgroupV1() (cgroup, error) {
	cgroupFile := fmt.Sprintf("/proc/%d/cgroup", os.Getpid())
	lines, err := readLines(cgroupFile)
	if err != nil {
		return nil, err
	}

	cgroups := make(map[string]string)
	for _, line := range lines {
		cols := strings.Split(line, ":")
		if len(cols) != 3 {
			return nil, fmt.Errorf("invalid cgroup line: %s", line)
		}
		subsys := cols[1]
		if !strings.HasPrefix(subsys, "cpu") {
			continue
		}
		for _, val := range strings.Split(subsys, ",") {
			cgroups[val] = path.Join(cgroupDir, val)
		}
	}

	return &cgroupV1{cgroups: cgroups}, nil
}

// --- cgroup v2 ---

type cgroupV2 struct {
	cgroups map[string]string
}

func (c *cgroupV2) cpuQuota() (float64, error) {
	data, err := readText(cpuMaxFile)
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(data)
	if len(fields) != 2 {
		return 0, fmt.Errorf("cgroup: bad %s file: %s", cpuMaxFile, data)
	}
	if fields[0] == "max" {
		return -1, nil
	}
	quotaUs, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, err
	}
	periodUs, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, err
	}
	return float64(quotaUs) / float64(periodUs), nil
}

func (c *cgroupV2) cpuUsage() (uint64, error) {
	usec, err := parseUint(c.cgroups["usage_usec"])
	if err != nil {
		return 0, err
	}
	return usec * uint64(time.Microsecond), nil
}

func (c *cgroupV2) effectiveCpus() (int, error) {
	data, err := readText(cpusetFile)
	if err != nil {
		return 0, err
	}
	cpus, err := parseUints(data)
	if err != nil {
		return 0, err
	}
	return len(cpus), nil
}

func currentCgroupV2() (cgroup, error) {
	lines, err := readLines(cpuStatFile)
	if err != nil {
		return nil, err
	}
	cgroups := make(map[string]string)
	for _, line := range lines {
		cols := strings.Fields(line)
		if len(cols) != 2 {
			return nil, fmt.Errorf("invalid cgroupV2 line: %s", line)
		}
		cgroups[cols[0]] = cols[1]
	}
	return &cgroupV2{cgroups: cgroups}, nil
}

// --- small std-lib helpers (replacing go-zero iox/lang) ---

func readText(filename string) (string, error) {
	b, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

func readLines(filename string) ([]string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			lines = append(lines, line)
		}
	}
	return lines, sc.Err()
}

func parseUint(s string) (uint64, error) {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return 0, nil
		}
		return 0, fmt.Errorf("cgroup: bad int format: %s", s)
	}
	if v < 0 {
		return 0, nil
	}
	return uint64(v), nil
}

func parseUints(val string) ([]uint64, error) {
	if val == "" {
		return nil, nil
	}

	var sets []uint64
	seen := make(map[uint64]struct{})
	for _, r := range strings.Split(val, ",") {
		if strings.Contains(r, "-") {
			fields := strings.SplitN(r, "-", 2)
			minimum, err := parseUint(fields[0])
			if err != nil {
				return nil, fmt.Errorf("cgroup: bad int list format: %s", val)
			}
			maximum, err := parseUint(fields[1])
			if err != nil {
				return nil, fmt.Errorf("cgroup: bad int list format: %s", val)
			}
			if maximum < minimum {
				return nil, fmt.Errorf("cgroup: bad int list format: %s", val)
			}
			for i := minimum; i <= maximum; i++ {
				if _, ok := seen[i]; !ok {
					seen[i] = struct{}{}
					sets = append(sets, i)
				}
			}
		} else {
			v, err := parseUint(r)
			if err != nil {
				return nil, err
			}
			if _, ok := seen[v]; !ok {
				seen[v] = struct{}{}
				sets = append(sets, v)
			}
		}
	}

	return sets, nil
}
