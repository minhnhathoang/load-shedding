package queue

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zeromicro/go-zero/core/load"
)

// newFakeCPUGate builds a static CPU gate whose CPU reading is driven by cpu.
func newFakeCPUGate(threshold int64, cpu *atomic.Int64) *CPUThresholdGate {
	g := NewCPUThresholdGate(threshold)
	g.cpuUsage = func() int64 { return cpu.Load() }
	return g
}

func TestPoolCpuGateRejects(t *testing.T) {
	var cpu atomic.Int64
	p := New(Config{Workers: 4, QueueCapacity: 16, Gate: newFakeCPUGate(900, &cpu)})
	defer p.Stop()

	cpu.Store(800) // below threshold → admitted
	assert.NoError(t, p.SubmitWait(func() {}))

	cpu.Store(950) // at/above threshold → rejected before the queue
	assert.ErrorIs(t, p.Submit(func() {}), ErrOverloaded)
	assert.Equal(t, int64(1), p.Stats().Rejected)

	cpu.Store(500) // recovers
	assert.NoError(t, p.SubmitWait(func() {}))
}

func TestPoolGateOnOff(t *testing.T) {
	var cpu atomic.Int64
	cpu.Store(1000) // permanently "overloaded"
	p := New(Config{Workers: 2, QueueCapacity: 4, Gate: newFakeCPUGate(900, &cpu)})
	defer p.Stop()

	// Gate ON → rejects.
	assert.ErrorIs(t, p.Submit(func() {}), ErrOverloaded)

	// Toggle the gate OFF → admits despite high CPU.
	p.SetGateEnabled(false)
	assert.False(t, p.Gate().Enabled())
	assert.NoError(t, p.SubmitWait(func() {}))

	// Toggle back ON → rejects again.
	p.SetGateEnabled(true)
	assert.ErrorIs(t, p.Submit(func() {}), ErrOverloaded)
}

func TestPoolNoGateByDefault(t *testing.T) {
	p := New(Config{Workers: 2, QueueCapacity: 4})
	defer p.Stop()
	assert.Nil(t, p.Gate())
	assert.NoError(t, p.SubmitWait(func() {}))
}

func TestCpuThresholdFromConfig(t *testing.T) {
	// CpuThreshold (no explicit Gate) installs a static CPU gate.
	p := New(Config{Workers: 2, QueueCapacity: 4, CpuThreshold: 900})
	defer p.Stop()
	assert.NotNil(t, p.Gate())
	_, isCPU := p.Gate().(*CPUThresholdGate)
	assert.True(t, isCPU)
}

func TestGozeroGateAdmitsAndReportsBack(t *testing.T) {
	// The go-zero gate admits under normal conditions; the done callback must
	// feed Pass/Fail back without panicking. (CPU won't saturate in a unit test.)
	p := New(Config{Workers: 2, QueueCapacity: 8, Gate: NewGozeroGate(load.WithCpuThreshold(900))})
	defer p.Stop()

	_, isGozero := p.Gate().(*GozeroGate)
	assert.True(t, isGozero)

	var ran atomic.Int64
	for i := 0; i < 10; i++ {
		assert.NoError(t, p.SubmitWait(func() { ran.Add(1) }))
	}
	assert.Equal(t, int64(10), ran.Load())
}

func TestCodelGateRejects(t *testing.T) {
	var cpu atomic.Int64
	p := NewCodel(CodelConfig{Workers: 2, Capacity: 64, Gate: newFakeCPUGate(900, &cpu)})
	defer p.Stop()

	cpu.Store(850)
	assert.NoError(t, p.SubmitWait(func() {}))

	cpu.Store(900) // >= threshold
	assert.ErrorIs(t, p.Submit(func() {}), ErrOverloaded)
	assert.Equal(t, int64(1), p.Stats().Rejected)

	p.SetGateEnabled(false)
	assert.NoError(t, p.SubmitWait(func() {}))
}
