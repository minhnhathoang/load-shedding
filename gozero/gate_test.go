package gozero

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zeromicro/go-zero/core/load"

	"github.com/minhnhathoang/load-shedding/queue"
)

func TestGateSatisfiesQueueGate(t *testing.T) {
	// Compile-time + runtime check that *Gate is usable as a queue.Gate.
	var _ queue.Gate = NewGate(load.WithCpuThreshold(900))
}

func TestGateAllowAndToggle(t *testing.T) {
	g := NewGate(load.WithCpuThreshold(900))

	done, ok := g.Allow()
	assert.True(t, ok)
	assert.NotNil(t, done)
	assert.NotPanics(t, func() { done(true) })

	done, ok = g.Allow()
	assert.True(t, ok)
	assert.NotPanics(t, func() { done(false) })

	assert.True(t, g.Enabled())
	g.SetEnabled(false)
	assert.False(t, g.Enabled())
	d, ok := g.Allow()
	assert.True(t, ok)
	assert.NotNil(t, d)
}

func TestGateAsQueuePoolGate(t *testing.T) {
	// Use the go-zero gate as a queue pool's admission gate end-to-end.
	p := queue.New(queue.Config{Workers: 2, QueueCapacity: 8, Gate: NewGate(load.WithCpuThreshold(900))})
	defer p.Stop()

	assert.NoError(t, p.SubmitWait(func() {}))
}
