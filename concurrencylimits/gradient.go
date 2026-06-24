package concurrencylimits

import (
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// GradientLimit is a faithful port of Netflix's GradientLimit. The limit tracks
// the gradient of the current RTT against an absolute minimum (no-load) RTT,
// growing while latency is healthy and shrinking as it rises, with periodic
// probing of the no-load baseline.
type GradientLimit struct {
	mu sync.Mutex

	estimatedLimit float64
	maxLimit       int
	minLimit       int
	smoothing      float64
	rttTolerance   float64
	backoffRatio   float64
	probeInterval  int
	queueSizeFunc  func(limit int) float64

	rttNoLoad       time.Duration // running minimum; 0 means "unset"
	resetRttCounter int
}

const gradientDisabledProbe = -1

// GradientOption customizes a GradientLimit.
type GradientOption func(*GradientLimit)

// GradientInitialLimit sets the starting limit (default 50).
func GradientInitialLimit(n int) GradientOption {
	return func(g *GradientLimit) { g.estimatedLimit = float64(n) }
}

// GradientMaxConcurrency caps the limit (default 1000).
func GradientMaxConcurrency(n int) GradientOption { return func(g *GradientLimit) { g.maxLimit = n } }

// GradientMinLimit sets the lower bound (default 1).
func GradientMinLimit(n int) GradientOption { return func(g *GradientLimit) { g.minLimit = n } }

// GradientRttTolerance sets how much RTT may exceed the no-load baseline before
// shrinking (default 2.0).
func GradientRttTolerance(t float64) GradientOption {
	return func(g *GradientLimit) { g.rttTolerance = t }
}

// NewGradientLimit returns a GradientLimit with Netflix's defaults.
func NewGradientLimit(opts ...GradientOption) *GradientLimit {
	g := &GradientLimit{
		estimatedLimit: 50,
		maxLimit:       1000,
		minLimit:       1,
		smoothing:      0.2,
		rttTolerance:   2.0,
		backoffRatio:   0.9,
		probeInterval:  1000,
		// default queueSize = max(4, sqrt(limit))
		queueSizeFunc: func(limit int) float64 { return math.Max(4, math.Sqrt(float64(limit))) },
	}
	for _, opt := range opts {
		opt(g)
	}
	g.resetRttCounter = g.nextProbeCountdown()
	return g
}

// Limit implements Limit.
func (g *GradientLimit) Limit() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return int(g.estimatedLimit)
}

// OnSample implements Limit.
func (g *GradientLimit) OnSample(rtt time.Duration, inflight int, didDrop bool) {
	if rtt <= 0 {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	queueSize := g.queueSizeFunc(int(g.estimatedLimit))

	if g.probeInterval != gradientDisabledProbe {
		g.resetRttCounter--
		if g.resetRttCounter <= 0 {
			g.resetRttCounter = g.nextProbeCountdown()
			g.estimatedLimit = math.Max(float64(g.minLimit), queueSize)
			g.rttNoLoad = 0 // reset baseline
			return
		}
	}

	// running minimum no-load RTT
	if g.rttNoLoad == 0 || rtt < g.rttNoLoad {
		g.rttNoLoad = rtt
	}

	gradient := math.Max(0.5, math.Min(1.0, g.rttTolerance*float64(g.rttNoLoad)/float64(rtt)))

	var newLimit float64
	switch {
	case didDrop:
		newLimit = g.estimatedLimit * g.backoffRatio
	case float64(inflight) < g.estimatedLimit/2:
		return
	default:
		newLimit = g.estimatedLimit*gradient + queueSize
	}

	if newLimit < g.estimatedLimit {
		newLimit = math.Max(float64(g.minLimit), g.estimatedLimit*(1-g.smoothing)+g.smoothing*newLimit)
	}
	newLimit = math.Max(queueSize, math.Min(float64(g.maxLimit), newLimit))
	g.estimatedLimit = newLimit
}

func (g *GradientLimit) nextProbeCountdown() int {
	if g.probeInterval == gradientDisabledProbe {
		return gradientDisabledProbe
	}
	return g.probeInterval + rand.IntN(g.probeInterval)
}
