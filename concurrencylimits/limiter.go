package concurrencylimits

import (
	"sync/atomic"
	"time"
)

// Listener is the per-request token returned by Limiter.Acquire. Exactly one of
// OnSuccess / OnIgnore / OnDropped must be called when the request finishes:
//
//   - OnSuccess: completed normally → feeds RTT into the limit (didDrop=false).
//   - OnDropped: failed/overloaded → feeds RTT with didDrop=true.
//   - OnIgnore:  do not sample this request at all (just release the slot).
type Listener interface {
	OnSuccess()
	OnIgnore()
	OnDropped()
}

// Limiter gates admission against an adaptive Limit.
type Limiter interface {
	// Acquire returns a Listener and true if admitted, or (nil, false) if the
	// request should be rejected.
	Acquire() (Listener, bool)
}

// SimpleLimiter is a fail-fast limiter (Netflix's SimpleLimiter): it admits a
// request when in-flight concurrency is below the algorithm's current limit, and
// rejects otherwise.
type SimpleLimiter struct {
	limit    Limit
	inflight atomic.Int64
	now      func() time.Time
}

// NewSimpleLimiter returns a SimpleLimiter driven by the given Limit algorithm.
func NewSimpleLimiter(limit Limit) *SimpleLimiter {
	return &SimpleLimiter{limit: limit, now: time.Now}
}

// Acquire implements Limiter.
func (l *SimpleLimiter) Acquire() (Listener, bool) {
	start := l.now()
	current := int(l.inflight.Add(1))
	if current > l.limit.Limit() {
		l.inflight.Add(-1)
		return nil, false
	}
	return &simpleListener{l: l, start: start, inflight: current}, true
}

// Inflight returns the current in-flight request count.
func (l *SimpleLimiter) Inflight() int { return int(l.inflight.Load()) }

// CurrentLimit returns the algorithm's current concurrency limit.
func (l *SimpleLimiter) CurrentLimit() int { return l.limit.Limit() }

type simpleListener struct {
	l        *SimpleLimiter
	start    time.Time
	inflight int
	done     atomic.Bool
}

func (s *simpleListener) finish(sample, didDrop bool) {
	if !s.done.CompareAndSwap(false, true) {
		return
	}
	s.l.inflight.Add(-1)
	if sample {
		s.l.limit.OnSample(s.l.now().Sub(s.start), s.inflight, didDrop)
	}
}

func (s *simpleListener) OnSuccess() { s.finish(true, false) }
func (s *simpleListener) OnIgnore()  { s.finish(false, false) }
func (s *simpleListener) OnDropped() { s.finish(true, true) }
