package queue

import "github.com/zeromicro/go-zero/core/stat"

// defaultCpuUsage reports cgroup-aware CPU usage in millicpu (0-1000), the same
// signal go-zero's adaptive shedder uses.
func defaultCpuUsage() int64 {
	return stat.CpuUsage()
}
