package concurrencylimits

import (
	"sync"
	"time"
)

// WindowedLimit wraps a Limit and batches per-request samples into time windows,
// porting Netflix's WindowedLimit. Instead of updating the underlying algorithm
// on every request, it aggregates samples (average RTT, max in-flight, any drop)
// over a window and updates the delegate once per window — smoothing the limit
// against per-request noise. Tiny RTTs below MinRttThreshold are ignored.
type WindowedLimit struct {
	delegate        Limit
	minWindowTime   time.Duration
	maxWindowTime   time.Duration
	windowSize      int
	minRttThreshold time.Duration

	mu         sync.Mutex
	window     *sampleWindow
	nextUpdate time.Time
	now        func() time.Time
}

// Netflix defaults.
const (
	defaultMinWindowTime   = time.Second
	defaultMaxWindowTime   = time.Second
	defaultWindowSize      = 10
	defaultMinRttThreshold = 100 * time.Microsecond
)

// WindowedOption customizes a WindowedLimit.
type WindowedOption func(*WindowedLimit)

// WindowedMinWindowTime sets the minimum window duration (default 1s).
func WindowedMinWindowTime(d time.Duration) WindowedOption {
	return func(w *WindowedLimit) { w.minWindowTime = d }
}

// WindowedMaxWindowTime sets the maximum window duration (default 1s).
func WindowedMaxWindowTime(d time.Duration) WindowedOption {
	return func(w *WindowedLimit) { w.maxWindowTime = d }
}

// WindowedWindowSize sets the minimum samples a window needs before it updates
// the delegate (default 10).
func WindowedWindowSize(n int) WindowedOption {
	return func(w *WindowedLimit) { w.windowSize = n }
}

// WindowedMinRttThreshold ignores samples with an RTT below this (default 100µs).
func WindowedMinRttThreshold(d time.Duration) WindowedOption {
	return func(w *WindowedLimit) { w.minRttThreshold = d }
}

// NewWindowedLimit wraps delegate with windowed sampling using Netflix's defaults.
func NewWindowedLimit(delegate Limit, opts ...WindowedOption) *WindowedLimit {
	w := &WindowedLimit{
		delegate:        delegate,
		minWindowTime:   defaultMinWindowTime,
		maxWindowTime:   defaultMaxWindowTime,
		windowSize:      defaultWindowSize,
		minRttThreshold: defaultMinRttThreshold,
		window:          &sampleWindow{},
		now:             time.Now,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Limit implements Limit.
func (w *WindowedLimit) Limit() int { return w.delegate.Limit() }

// OnSample implements Limit.
func (w *WindowedLimit) OnSample(rtt time.Duration, inflight int, didDrop bool) {
	if rtt < w.minRttThreshold {
		return
	}
	now := w.now()

	w.mu.Lock()
	w.window.add(rtt, inflight, didDrop)
	if w.nextUpdate.IsZero() {
		// First sample starts the window rather than spuriously flushing it.
		w.nextUpdate = now.Add(w.minWindowTime)
		w.mu.Unlock()
		return
	}
	if !now.After(w.nextUpdate) {
		w.mu.Unlock()
		return
	}
	current := w.window
	w.window = &sampleWindow{}
	w.nextUpdate = now.Add(clampDuration(current.candidateRtt()*2, w.minWindowTime, w.maxWindowTime))
	ready := current.count >= w.windowSize
	w.mu.Unlock()

	if ready {
		w.delegate.OnSample(current.trackedRtt(), current.maxInFlight, current.didDrop)
	}
}

func clampDuration(d, lo, hi time.Duration) time.Duration {
	if d < lo {
		return lo
	}
	if d > hi {
		return hi
	}
	return d
}

// sampleWindow aggregates samples over a window (average RTT like Netflix's
// AverageSampleWindow): tracked RTT = mean, candidate RTT = min.
type sampleWindow struct {
	minRtt      time.Duration
	sumRtt      time.Duration
	count       int
	maxInFlight int
	didDrop     bool
}

func (s *sampleWindow) add(rtt time.Duration, inflight int, drop bool) {
	if s.count == 0 || rtt < s.minRtt {
		s.minRtt = rtt
	}
	s.sumRtt += rtt
	s.count++
	if inflight > s.maxInFlight {
		s.maxInFlight = inflight
	}
	s.didDrop = s.didDrop || drop
}

func (s *sampleWindow) candidateRtt() time.Duration { return s.minRtt }

func (s *sampleWindow) trackedRtt() time.Duration {
	if s.count == 0 {
		return 0
	}
	return s.sumRtt / time.Duration(s.count)
}
