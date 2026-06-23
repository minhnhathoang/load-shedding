package quarkus

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLog10Plus1(t *testing.T) {
	tests := []struct {
		in   int64
		want int64
	}{
		{0, 1},
		{1, 1},
		{9, 1},
		{10, 2},
		{99, 2},
		{100, 3},
		{999, 3},
		{1000, 4}, // out of table range
		{12345, 5},
	}
	for _, tt := range tests {
		assert.Equalf(t, tt.want, log10Plus1(tt.in), "log10Plus1(%d)", tt.in)
	}
}

func TestOverloadDetectorInitialState(t *testing.T) {
	d := NewOverloadDetector(DefaultConfig())
	assert.Equal(t, int64(100), d.CurrentLimit())
	assert.Equal(t, int64(0), d.CurrentRequests())
	assert.False(t, d.IsOverloaded())
}

func TestOverloadDetectorOverloadBoundary(t *testing.T) {
	cfg := DefaultConfig()
	cfg.InitialLimit = 3
	d := NewOverloadDetector(cfg)

	d.RequestBegin()
	d.RequestBegin()
	assert.False(t, d.IsOverloaded()) // 2 < 3
	d.RequestBegin()
	assert.True(t, d.IsOverloaded()) // 3 >= 3
}

func TestOverloadDetectorBeginEndCount(t *testing.T) {
	d := NewOverloadDetector(DefaultConfig())
	d.RequestBegin()
	d.RequestBegin()
	assert.Equal(t, int64(2), d.CurrentRequests())
	d.RequestEnd(time.Millisecond)
	assert.Equal(t, int64(1), d.CurrentRequests())
}

func TestOverloadDetectorIgnoresZeroRtt(t *testing.T) {
	d := NewOverloadDetector(DefaultConfig())
	d.RequestBegin()
	before := d.CurrentLimit()
	d.RequestEnd(0) // sub-microsecond, must not panic or change limit
	assert.Equal(t, before, d.CurrentLimit())
}

func TestOverloadDetectorLimitStaysBounded(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxLimit = 200
	cfg.InitialLimit = 100
	d := NewOverloadDetector(cfg)

	// Drive many completions with fast, stable response times. The limit must
	// always remain within [1, MaxLimit] regardless of the gradient outcome.
	for i := 0; i < 100000; i++ {
		d.RequestBegin()
	}
	for i := 0; i < 100000; i++ {
		d.RequestEnd(time.Millisecond)
		limit := d.CurrentLimit()
		assert.GreaterOrEqual(t, limit, int64(1))
		assert.LessOrEqual(t, limit, cfg.MaxLimit)
	}
}

func TestOverloadDetectorConcurrent(t *testing.T) {
	d := NewOverloadDetector(DefaultConfig())
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				d.RequestBegin()
				d.RequestEnd(time.Duration(j%5+1) * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int64(0), d.CurrentRequests())
	assert.GreaterOrEqual(t, d.CurrentLimit(), int64(1))
	assert.LessOrEqual(t, d.CurrentLimit(), DefaultConfig().MaxLimit)
}
