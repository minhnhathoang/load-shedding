package queue

import "sync/atomic"

// Gate is an admission gate placed in front of the queue. Allow reports whether
// a request may enter; when ok is true the caller must call done(success)
// exactly once when the request finishes, is dropped, or is otherwise shed.
//
// A Gate can be toggled at runtime via SetEnabled; a disabled gate admits
// everything (done is a no-op).
type Gate interface {
	Allow() (done func(success bool), ok bool)
	SetEnabled(on bool)
	Enabled() bool
}

var noopDone = func(bool) {}

// CPUThresholdGate rejects a request when CPU usage (millicpu, 0-1000) is at or
// above threshold. It is a simple static gate.
type CPUThresholdGate struct {
	threshold int64
	cpuUsage  func() int64 // injectable for tests
	enabled   atomic.Bool
}

// NewCPUThresholdGate returns an enabled static CPU gate. A threshold <= 0
// effectively never rejects.
func NewCPUThresholdGate(threshold int64) *CPUThresholdGate {
	return NewCPUThresholdGateFunc(threshold, nil)
}

// NewCPUThresholdGateFunc is like NewCPUThresholdGate but with a custom CPU
// source (millicpu, 0-1000) — useful for testing or non-default CPU sampling.
// A nil cpuUsage uses go-zero's stat.CpuUsage.
func NewCPUThresholdGateFunc(threshold int64, cpuUsage func() int64) *CPUThresholdGate {
	if cpuUsage == nil {
		cpuUsage = defaultCpuUsage
	}
	g := &CPUThresholdGate{threshold: threshold, cpuUsage: cpuUsage}
	g.enabled.Store(true)
	return g
}

// Allow implements Gate.
func (g *CPUThresholdGate) Allow() (func(bool), bool) {
	if !g.enabled.Load() {
		return noopDone, true
	}
	if g.threshold > 0 && g.cpuUsage() >= g.threshold {
		return nil, false
	}
	return noopDone, true
}

// SetEnabled toggles the gate at runtime.
func (g *CPUThresholdGate) SetEnabled(on bool) { g.enabled.Store(on) }

// Enabled reports whether the gate is currently active.
func (g *CPUThresholdGate) Enabled() bool { return g.enabled.Load() }

// For an adaptive CPU + Little's-Law gate backed by go-zero's shedder, use
// gozero.NewGate from the github.com/minhnhathoang/load-shedding/gozero package;
// it returns a value that satisfies this package's Gate interface while keeping
// queue itself free of any go-zero dependency.

// gateFor builds the gate for a pool: an explicit Gate wins; otherwise a static
// CPU-threshold gate is installed when cpuThreshold > 0; otherwise nil (no gate).
func gateFor(explicit Gate, cpuThreshold int64) Gate {
	if explicit != nil {
		return explicit
	}
	if cpuThreshold > 0 {
		return NewCPUThresholdGate(cpuThreshold)
	}
	return nil
}
