package queue

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zeromicro/go-zero/core/load"
)

func TestCPUThresholdGateAllow(t *testing.T) {
	var cpu atomic.Int64
	g := NewCPUThresholdGateFunc(900, func() int64 { return cpu.Load() })

	cpu.Store(899)
	done, ok := g.Allow()
	assert.True(t, ok)
	assert.NotNil(t, done)

	cpu.Store(900) // >= threshold
	_, ok = g.Allow()
	assert.False(t, ok)

	cpu.Store(901)
	_, ok = g.Allow()
	assert.False(t, ok)
}

func TestCPUThresholdGateZeroThresholdNeverRejects(t *testing.T) {
	g := NewCPUThresholdGateFunc(0, func() int64 { return 1000 })
	_, ok := g.Allow()
	assert.True(t, ok)
}

func TestCPUThresholdGateDisabledAdmits(t *testing.T) {
	g := NewCPUThresholdGateFunc(900, func() int64 { return 1000 })
	assert.True(t, g.Enabled())
	g.SetEnabled(false)
	assert.False(t, g.Enabled())
	_, ok := g.Allow()
	assert.True(t, ok, "a disabled gate admits everything")
}

func TestGozeroGateAllowAndToggle(t *testing.T) {
	g := NewGozeroGate(load.WithCpuThreshold(900))
	done, ok := g.Allow()
	assert.True(t, ok)
	assert.NotNil(t, done)
	assert.NotPanics(t, func() { done(true) })

	done, ok = g.Allow()
	assert.True(t, ok)
	assert.NotPanics(t, func() { done(false) })

	g.SetEnabled(false)
	d, ok := g.Allow()
	assert.True(t, ok)
	assert.NotNil(t, d)
}

// recordingGate records admissions and how each one is resolved, to prove the
// pool calls done(...) exactly once per admission (no promise leak).
type recordingGate struct {
	admit  bool
	admits atomic.Int64
	trueN  atomic.Int64
	falseN atomic.Int64
}

func (g *recordingGate) Allow() (func(bool), bool) {
	if !g.admit {
		return nil, false
	}
	g.admits.Add(1)
	return func(s bool) {
		if s {
			g.trueN.Add(1)
		} else {
			g.falseN.Add(1)
		}
	}, true
}
func (g *recordingGate) SetEnabled(bool) {}
func (g *recordingGate) Enabled() bool   { return true }

func (g *recordingGate) resolved() int64 { return g.trueN.Load() + g.falseN.Load() }

func TestGateRejectCallsNoDone(t *testing.T) {
	g := &recordingGate{admit: false}
	p := New(Config{Workers: 2, QueueCapacity: 4, Gate: g})
	defer p.Stop()

	assert.ErrorIs(t, p.Submit(func() {}), ErrOverloaded)
	assert.Equal(t, int64(0), g.admits.Load())
	assert.Equal(t, int64(0), g.resolved())
}

func TestGateDoneTrueOnRun(t *testing.T) {
	g := &recordingGate{admit: true}
	p := New(Config{Workers: 2, QueueCapacity: 4, Gate: g})
	defer p.Stop()

	assert.NoError(t, p.SubmitWait(func() {}))
	assert.Equal(t, int64(1), g.admits.Load())
	assert.Equal(t, int64(1), g.trueN.Load())
	assert.Equal(t, int64(0), g.falseN.Load())
}

func TestGateDoneFalseOnQueueFull(t *testing.T) {
	g := &recordingGate{admit: true}
	p := New(Config{Workers: 1, QueueCapacity: 0, Gate: g}) // capacity 1
	defer p.Stop()

	block := make(chan struct{})
	started := make(chan struct{})
	assert.NoError(t, p.Submit(func() {
		close(started)
		<-block
	}))
	<-started

	// Gate admits, but the queue is full → done(false), ErrQueueFull.
	assert.ErrorIs(t, p.Submit(func() {}), ErrQueueFull)
	assert.GreaterOrEqual(t, g.falseN.Load(), int64(1))
	close(block)
}

func TestGateConservationUnderLoad(t *testing.T) {
	// Sustained load through CoDel (which drops) with a gate that always admits:
	// every admission must be resolved by exactly one done(true|false).
	g := &recordingGate{admit: true}
	p := NewCodel(CodelConfig{
		Workers:  1,
		Capacity: 100000,
		Target:   time.Millisecond,
		Interval: 10 * time.Millisecond,
		Gate:     g,
	})
	defer p.Stop()

	deadline := time.After(250 * time.Millisecond)
loop:
	for {
		select {
		case <-deadline:
			break loop
		default:
		}
		_ = p.Submit(func() { time.Sleep(2 * time.Millisecond) })
		time.Sleep(300 * time.Microsecond)
	}

	assert.Eventually(t, func() bool {
		st := p.Stats()
		return st.Queued == 0 && st.Active == 0 && g.resolved() == g.admits.Load()
	}, 5*time.Second, 10*time.Millisecond)

	st := p.Stats()
	assert.Positive(t, st.Dropped, "sustained load should produce CoDel drops")
	assert.Equal(t, g.admits.Load(), g.resolved(), "every admission resolved exactly once (no leak)")
	assert.Equal(t, g.falseN.Load(), st.Dropped, "drops map to done(false)")
}
