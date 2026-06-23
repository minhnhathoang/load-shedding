package gozero

import (
	"sync/atomic"

	"github.com/zeromicro/go-zero/core/load"
)

// Gate uses go-zero's adaptive shedder (CPU usage + Little's Law + cool-off
// hysteresis) as an admission gate. It satisfies the queue.Gate interface
// structurally, so it can be passed as queue.Config.Gate / queue.CodelConfig.Gate
// without the queue package depending on go-zero.
//
// Unlike a static CPU-threshold gate, it learns the service's capacity from live
// throughput and latency, and only sheds when CPU is saturated AND in-flight
// concurrency exceeds the learned limit.
type Gate struct {
	shedder load.Shedder
	enabled atomic.Bool
}

// NewGate returns an enabled gate backed by go-zero's adaptive shedder. opts are
// forwarded to load.NewAdaptiveShedder (e.g. load.WithCpuThreshold).
func NewGate(opts ...load.ShedderOption) *Gate {
	g := &Gate{shedder: load.NewAdaptiveShedder(opts...)}
	g.enabled.Store(true)
	return g
}

// Allow reports whether a request may proceed. On admission it returns a done
// callback that reports the outcome back to the adaptive shedder (Pass on
// success, Fail otherwise) so the model keeps learning.
func (g *Gate) Allow() (func(bool), bool) {
	if !g.enabled.Load() {
		return func(bool) {}, true
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

// SetEnabled toggles the gate at runtime. A disabled gate admits everything.
func (g *Gate) SetEnabled(on bool) { g.enabled.Store(on) }

// Enabled reports whether the gate is currently active.
func (g *Gate) Enabled() bool { return g.enabled.Load() }
