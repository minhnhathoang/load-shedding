package quarkus

import (
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// log10Plus1Table caches 1 + floor(log10(i)) for i in [0, 1000), matching the
// LOG10_PLUS_1_TABLE lookup in the Quarkus OverloadDetector.
var log10Plus1Table [1000]int64

func init() {
	log10Plus1Table[0] = 1
	for i := 1; i < 1000; i++ {
		log10Plus1Table[i] = 1 + int64(math.Log10(float64(i)))
	}
}

func log10Plus1(limit int64) int64 {
	if limit >= 0 && limit < 1000 {
		return log10Plus1Table[limit]
	}
	return 1 + int64(math.Log10(float64(limit)))
}

// OverloadDetector is an adaptive concurrency limiter based on TCP Vegas.
//
// It keeps an exact count of in-flight requests and an adaptive limit. The limit
// is grown or shrunk on each completion by estimating the queue size from the
// ratio of the lowest observed response time to the current one. The system is
// considered overloaded when in-flight requests reach the current limit.
type OverloadDetector struct {
	maxLimit    int64
	alphaFactor int64
	betaFactor  int64
	probeFactor float64

	currentRequests atomic.Int64
	currentLimit    atomic.Int64

	mu                sync.Mutex
	lowestRequestTime int64
	probeCount        float64
	probeJitter       float64
}

// NewOverloadDetector returns an OverloadDetector configured from cfg.
func NewOverloadDetector(cfg Config) *OverloadDetector {
	d := &OverloadDetector{
		maxLimit:          cfg.MaxLimit,
		alphaFactor:       cfg.AlphaFactor,
		betaFactor:        cfg.BetaFactor,
		probeFactor:       cfg.ProbeFactor,
		lowestRequestTime: math.MaxInt64,
	}
	d.currentLimit.Store(cfg.InitialLimit)
	d.resetProbeJitter()
	return d
}

// IsOverloaded reports whether in-flight requests have reached the current limit.
func (d *OverloadDetector) IsOverloaded() bool {
	return d.currentRequests.Load() >= d.currentLimit.Load()
}

// RequestBegin records that a request has started.
func (d *OverloadDetector) RequestBegin() {
	d.currentRequests.Add(1)
}

// RequestEnd records that a request has finished after the given round-trip
// time, and updates the adaptive limit.
func (d *OverloadDetector) RequestEnd(rtt time.Duration) {
	// getAndDecrement semantics: use the value before the decrement.
	current := d.currentRequests.Add(-1) + 1
	d.update(rtt.Microseconds(), current)
}

// CurrentLimit returns the current adaptive concurrency limit.
func (d *OverloadDetector) CurrentLimit() int64 {
	return d.currentLimit.Load()
}

// CurrentRequests returns the current number of in-flight requests.
func (d *OverloadDetector) CurrentRequests() int64 {
	return d.currentRequests.Load()
}

func (d *OverloadDetector) update(requestTime, currentRequests int64) {
	// sub-microsecond timings would make the ratio meaningless / divide by zero.
	if requestTime <= 0 {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	limit := d.currentLimit.Load()

	d.probeCount++
	// Periodically reset the baseline so a drifting minimum gets re-probed.
	if d.probeFactor*d.probeJitter*float64(limit) <= d.probeCount {
		d.resetProbeJitter()
		d.probeCount = 0
		d.lowestRequestTime = requestTime
		return
	}

	if requestTime < d.lowestRequestTime {
		d.lowestRequestTime = requestTime
		return
	}

	// Only react once concurrency is at least half the limit.
	if 2*currentRequests < limit {
		return
	}

	queueSize := int64(math.Ceil(float64(limit) * (1.0 - float64(d.lowestRequestTime)/float64(requestTime))))

	limitLog10Plus1 := log10Plus1(limit)
	alpha := d.alphaFactor * limitLog10Plus1
	beta := d.betaFactor * limitLog10Plus1

	var newLimit int64
	switch {
	case queueSize <= limitLog10Plus1:
		newLimit = limit + beta
	case queueSize < alpha:
		newLimit = limit + limitLog10Plus1
	case queueSize > beta:
		newLimit = limit - limitLog10Plus1
	default:
		return
	}

	if newLimit < 1 {
		newLimit = 1
	}
	if newLimit > d.maxLimit {
		newLimit = d.maxLimit
	}
	d.currentLimit.Store(newLimit)
}

// resetProbeJitter picks a new jitter in [0.5, 1.0). Callers must hold d.mu
// (or be in the constructor).
func (d *OverloadDetector) resetProbeJitter() {
	d.probeJitter = 0.5 + rand.Float64()*0.5
}
