package queue

import (
	"sync"
	"sync/atomic"
	"time"
)

// CPU sampler — a dependency-free port of go-zero's core/stat CPU usage. It
// reports cgroup-aware CPU usage in millicpu (0-1000), smoothed with an EWMA,
// refreshed by a background goroutine. The goroutine starts lazily on the first
// call so importing this package costs nothing unless a CPU gate is used.
const (
	cpuRefreshInterval = 250 * time.Millisecond
	// 250ms and 0.95 beta average CPU load over roughly the last 5 seconds.
	cpuBeta = 0.95
)

var (
	cpuUsageValue int64
	cpuSampleOnce sync.Once
)

// cpuUsage returns the smoothed cgroup-aware CPU usage in millicpu (0-1000).
func cpuUsage() int64 {
	cpuSampleOnce.Do(startCpuSampler)
	return atomic.LoadInt64(&cpuUsageValue)
}

func startCpuSampler() {
	// Prime the baseline so the first tick produces a meaningful delta.
	refreshCpu()
	go func() {
		ticker := time.NewTicker(cpuRefreshInterval)
		defer ticker.Stop()
		for range ticker.C {
			runSafe(func() {
				cur := int64(refreshCpu())
				prev := atomic.LoadInt64(&cpuUsageValue)
				usage := int64(float64(prev)*cpuBeta + float64(cur)*(1-cpuBeta))
				atomic.StoreInt64(&cpuUsageValue, usage)
			})
		}
	}()
}

// defaultCpuUsage is the default CPU source for gates (millicpu, 0-1000).
func defaultCpuUsage() int64 {
	return cpuUsage()
}
