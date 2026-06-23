//go:build !linux

package queue

// refreshCpu returns CPU usage in millicpu; always 0 on non-linux systems
// (matching go-zero's behavior).
func refreshCpu() uint64 {
	return 0
}
