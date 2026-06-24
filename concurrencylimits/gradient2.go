package concurrencylimits

import (
	"math"
	"sync"
	"time"
)

// Gradient2Limit is a faithful port of Netflix's Gradient2Limit. Instead of an
// absolute minimum RTT, it compares the current (short) RTT against a long-window
// moving average, making it robust to baseline drift without probing.
type Gradient2Limit struct {
	mu sync.Mutex

	estimatedLimit float64
	maxLimit       int
	minLimit       int
	smoothing      float64
	tolerance      float64
	queueSizeFunc  func(limit int) float64

	longRtt *expAvg
}

// Gradient2Option customizes a Gradient2Limit.
type Gradient2Option func(*Gradient2Limit)

// Gradient2InitialLimit sets the starting limit (default 20).
func Gradient2InitialLimit(n int) Gradient2Option {
	return func(g *Gradient2Limit) { g.estimatedLimit = float64(n) }
}

// Gradient2MaxConcurrency caps the limit (default 200).
func Gradient2MaxConcurrency(n int) Gradient2Option {
	return func(g *Gradient2Limit) { g.maxLimit = n }
}

// Gradient2MinLimit sets the lower bound (default 20).
func Gradient2MinLimit(n int) Gradient2Option { return func(g *Gradient2Limit) { g.minLimit = n } }

// Gradient2RttTolerance sets the RTT tolerance (default 1.5).
func Gradient2RttTolerance(t float64) Gradient2Option {
	return func(g *Gradient2Limit) { g.tolerance = t }
}

// Gradient2LongWindow sets the long-RTT EWMA window (default 600).
func Gradient2LongWindow(n int) Gradient2Option {
	return func(g *Gradient2Limit) { g.longRtt = newExpAvg(n, 10) }
}

// NewGradient2Limit returns a Gradient2Limit with Netflix's defaults.
func NewGradient2Limit(opts ...Gradient2Option) *Gradient2Limit {
	g := &Gradient2Limit{
		estimatedLimit: 20,
		maxLimit:       200,
		minLimit:       20,
		smoothing:      0.2,
		tolerance:      1.5,
		queueSizeFunc:  func(int) float64 { return 4 },
		longRtt:        newExpAvg(600, 10),
	}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// Limit implements Limit.
func (g *Gradient2Limit) Limit() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return int(g.estimatedLimit)
}

// OnSample implements Limit.
func (g *Gradient2Limit) OnSample(rtt time.Duration, inflight int, didDrop bool) {
	if rtt <= 0 {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	queueSize := g.queueSizeFunc(int(g.estimatedLimit))
	shortRtt := float64(rtt)
	longRtt := g.longRtt.add(shortRtt)

	// If the recent RTT is far below the long average, let the baseline fall
	// faster (replaces Gradient's probe).
	if longRtt/shortRtt > 2 {
		longRtt = g.longRtt.update(func(current float64) float64 { return current * 0.95 })
	}

	if float64(inflight) < g.estimatedLimit/2 {
		return
	}

	gradient := math.Max(0.5, math.Min(1.0, g.tolerance*longRtt/shortRtt))
	newLimit := g.estimatedLimit*gradient + queueSize
	newLimit = g.estimatedLimit*(1-g.smoothing) + newLimit*g.smoothing
	newLimit = math.Max(float64(g.minLimit), math.Min(float64(g.maxLimit), newLimit))
	g.estimatedLimit = newLimit
}
