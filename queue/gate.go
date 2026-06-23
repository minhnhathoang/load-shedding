package queue

import (
	"sync/atomic"

	"github.com/zeromicro/go-zero/core/load"
)

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

// GozeroGate uses go-zero's adaptive shedder (CPU usage + Little's Law + cool-off
// hysteresis) as the admission gate. Unlike the static threshold gate, it learns
// the service's capacity from live throughput and latency, and only sheds when
// CPU is saturated AND in-flight concurrency exceeds the learned limit.
type GozeroGate struct {
	shedder load.Shedder
	enabled atomic.Bool
}

// NewGozeroGate returns an enabled gate backed by go-zero's adaptive shedder.
// opts are forwarded to load.NewAdaptiveShedder (e.g. load.WithCpuThreshold).
func NewGozeroGate(opts ...load.ShedderOption) *GozeroGate {
	g := &GozeroGate{shedder: load.NewAdaptiveShedder(opts...)}
	g.enabled.Store(true)
	return g
}

// Allow implements Gate. On admission it returns a done that reports the outcome
// back to the adaptive shedder (Pass on success, Fail otherwise) so the model
// keeps learning.
func (g *GozeroGate) Allow() (func(bool), bool) {
	if !g.enabled.Load() {
		return noopDone, true
	}
	p, err := g.shedder.Allow()
	if err != nil {
		return nil, false
	}
	return func(success bool) {
		if success {
			p.Pass()
		} else {
			p.Fail()
		}
	}, true
}

// SetEnabled toggles the gate at runtime.
func (g *GozeroGate) SetEnabled(on bool) { g.enabled.Store(on) }

// Enabled reports whether the gate is currently active.
func (g *GozeroGate) Enabled() bool { return g.enabled.Load() }

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
