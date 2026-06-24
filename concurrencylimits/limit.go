// Package concurrencylimits is a Go port of Netflix's concurrency-limits
// (https://github.com/Netflix/concurrency-limits). It adaptively discovers a
// service's concurrency limit from request latency using algorithms borrowed
// from TCP congestion control — Vegas, Gradient, Gradient2 — and gates admission
// with a Limiter (fail-fast SimpleLimiter or the bounded LIFO backlog queue).
//
// Unlike the quarkus package (which ports only Quarkus's simplified Vegas
// variant), this package ports the full Netflix limit algorithms and limiters.
package concurrencylimits

import (
	"math"
	"time"
)

// Limit is an adaptive concurrency limit algorithm. OnSample feeds a completed
// request's round-trip time back into the model; Limit returns the current
// estimated concurrency limit.
type Limit interface {
	// OnSample updates the limit from a completed request. rtt is the measured
	// round-trip time, inflight is the in-flight count observed when the request
	// was admitted, and didDrop reports whether the request was dropped/failed.
	OnSample(rtt time.Duration, inflight int, didDrop bool)
	// Limit returns the current estimated concurrency limit.
	Limit() int
}

// log10Lookup caches max(1, floor(log10(i))) for i in [0, 1000), matching
// Netflix's Log10RootIntFunction.
var log10Lookup [1000]int

func init() {
	log10Lookup[0] = 1
	for i := 1; i < 1000; i++ {
		log10Lookup[i] = max(1, int(math.Log10(float64(i))))
	}
}

func log10Int(t int) int {
	if t < 0 {
		t = 0
	}
	if t < 1000 {
		return log10Lookup[t]
	}
	return int(math.Log10(float64(t)))
}

// expAvg is an exponential moving average with a simple-average warm-up,
// porting Netflix's ExpAvgMeasurement (used by Gradient2 for the long-window
// RTT baseline).
type expAvg struct {
	window int
	warmup int
	count  int
	sum    float64
	value  float64
}

func newExpAvg(window, warmup int) *expAvg {
	return &expAvg{window: window, warmup: warmup}
}

func (e *expAvg) add(sample float64) float64 {
	if e.count < e.warmup {
		e.count++
		e.sum += sample
		e.value = e.sum / float64(e.count)
		return e.value
	}
	factor := 2.0 / (float64(e.window) + 1.0)
	e.value = e.value*(1-factor) + sample*factor
	return e.value
}

func (e *expAvg) update(fn func(float64) float64) float64 {
	e.value = fn(e.value)
	return e.value
}

func (e *expAvg) get() float64 { return e.value }
