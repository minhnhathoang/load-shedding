package concurrencylimits

import (
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// VegasLimit is a faithful port of Netflix's VegasLimit: a TCP Vegas limit that
// estimates a queue length from the RTT ratio and adjusts the limit with a
// log-scaled tiered AIMD, periodically probing for a fresh no-load RTT.
type VegasLimit struct {
	mu sync.Mutex

	estimatedLimit  float64
	maxLimit        int
	smoothing       float64
	probeMultiplier int

	rttNoLoad   time.Duration
	probeCount  int64
	probeJitter float64

	alpha     func(limit int) int
	beta      func(limit int) int
	threshold func(limit int) int
	increase  func(limit float64) float64
	decrease  func(limit float64) float64
}

// VegasOption customizes a VegasLimit.
type VegasOption func(*VegasLimit)

// VegasInitialLimit sets the starting limit (default 20).
func VegasInitialLimit(n int) VegasOption {
	return func(v *VegasLimit) { v.estimatedLimit = float64(n) }
}

// VegasMaxConcurrency caps the limit (default 1000).
func VegasMaxConcurrency(n int) VegasOption { return func(v *VegasLimit) { v.maxLimit = n } }

// VegasSmoothing sets the smoothing factor in (0,1] (default 1.0 = none).
func VegasSmoothing(s float64) VegasOption { return func(v *VegasLimit) { v.smoothing = s } }

// VegasProbeMultiplier sets how often the no-load RTT is re-probed (default 30).
func VegasProbeMultiplier(m int) VegasOption { return func(v *VegasLimit) { v.probeMultiplier = m } }

// NewVegasLimit returns a VegasLimit with Netflix's defaults.
func NewVegasLimit(opts ...VegasOption) *VegasLimit {
	v := &VegasLimit{
		estimatedLimit:  20,
		maxLimit:        1000,
		smoothing:       1.0,
		probeMultiplier: 30,
		alpha:           func(limit int) int { return 3 * log10Int(limit) },
		beta:            func(limit int) int { return 6 * log10Int(limit) },
		threshold:       log10Int,
		increase:        func(limit float64) float64 { return limit + float64(log10Int(int(limit))) },
		decrease:        func(limit float64) float64 { return limit - float64(log10Int(int(limit))) },
	}
	for _, opt := range opts {
		opt(v)
	}
	v.resetProbeJitter()
	return v
}

// Limit implements Limit.
func (v *VegasLimit) Limit() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return int(v.estimatedLimit)
}

// OnSample implements Limit.
func (v *VegasLimit) OnSample(rtt time.Duration, inflight int, didDrop bool) {
	if rtt <= 0 {
		return
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	v.probeCount++
	if v.shouldProbe() {
		v.resetProbeJitter()
		v.probeCount = 0
		v.rttNoLoad = rtt
		return
	}

	if v.rttNoLoad == 0 || rtt < v.rttNoLoad {
		v.rttNoLoad = rtt
		return
	}

	v.updateEstimatedLimit(rtt, inflight, didDrop)
}

func (v *VegasLimit) updateEstimatedLimit(rtt time.Duration, inflight int, didDrop bool) {
	limit := v.estimatedLimit
	queueSize := int(math.Ceil(limit * (1 - float64(v.rttNoLoad)/float64(rtt))))

	var newLimit float64
	switch {
	case didDrop:
		newLimit = v.decrease(limit)
	case float64(inflight)*2 < limit:
		return
	default:
		alpha := v.alpha(int(limit))
		beta := v.beta(int(limit))
		threshold := v.threshold(int(limit))
		switch {
		case queueSize <= threshold:
			newLimit = limit + float64(beta)
		case queueSize < alpha:
			newLimit = v.increase(limit)
		case queueSize > beta:
			newLimit = v.decrease(limit)
		default:
			return
		}
	}

	newLimit = math.Max(1, math.Min(float64(v.maxLimit), newLimit))
	newLimit = (1-v.smoothing)*limit + v.smoothing*newLimit
	v.estimatedLimit = newLimit
}

func (v *VegasLimit) shouldProbe() bool {
	return v.probeJitter*float64(v.probeMultiplier)*v.estimatedLimit <= float64(v.probeCount)
}

func (v *VegasLimit) resetProbeJitter() {
	v.probeJitter = 0.5 + rand.Float64()*0.5 // [0.5, 1.0)
}
