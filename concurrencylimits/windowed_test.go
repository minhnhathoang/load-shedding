package concurrencylimits

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// countingLimit records how many times OnSample reaches the delegate and with
// what aggregated values.
type countingLimit struct {
	mu        sync.Mutex
	samples   int
	lastRtt   time.Duration
	lastFlite int
	lastDrop  bool
	limit     int
}

func (c *countingLimit) OnSample(rtt time.Duration, inflight int, didDrop bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.samples++
	c.lastRtt, c.lastFlite, c.lastDrop = rtt, inflight, didDrop
}
func (c *countingLimit) Limit() int { return c.limit }

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newWindowed(delegate Limit, clk *fakeClock, opts ...WindowedOption) *WindowedLimit {
	w := NewWindowedLimit(delegate, opts...)
	w.now = clk.now
	return w
}

func TestWindowedIgnoresTinyRtt(t *testing.T) {
	d := &countingLimit{limit: 10}
	clk := &fakeClock{t: time.Unix(1000, 0)}
	w := newWindowed(d, clk, WindowedMinRttThreshold(time.Millisecond))

	w.OnSample(100*time.Microsecond, 1, false) // below threshold → ignored
	clk.advance(2 * time.Second)
	w.OnSample(100*time.Microsecond, 1, false)
	assert.Equal(t, 0, d.samples)
}

func TestWindowedBatchesUntilWindowElapsesAndSizeMet(t *testing.T) {
	d := &countingLimit{limit: 10}
	clk := &fakeClock{t: time.Unix(2000, 0)}
	w := newWindowed(d, clk,
		WindowedWindowSize(5),
		WindowedMinWindowTime(time.Second),
		WindowedMaxWindowTime(time.Second),
	)

	// Within the window: constant samples accumulate, delegate not called yet.
	for i := 0; i < 5; i++ {
		w.OnSample(10*time.Millisecond, 5, false)
	}
	assert.Equal(t, 0, d.samples, "no delegate update before the window elapses")

	// Advance past the window and add one more sample to trigger the flush; the
	// triggering sample is included in the window it flushes.
	clk.advance(1100 * time.Millisecond)
	w.OnSample(10*time.Millisecond, 5, false)

	assert.Equal(t, 1, d.samples, "exactly one aggregated update")
	assert.Equal(t, 10*time.Millisecond, d.lastRtt) // mean of constant 10ms samples
	assert.Equal(t, 5, d.lastFlite)                 // max in-flight in the window
}

func TestWindowedSkipsUpdateWhenTooFewSamples(t *testing.T) {
	d := &countingLimit{limit: 10}
	clk := &fakeClock{t: time.Unix(3000, 0)}
	w := newWindowed(d, clk,
		WindowedWindowSize(10), // need 10, we'll only give 2
		WindowedMinWindowTime(time.Second),
		WindowedMaxWindowTime(time.Second),
	)

	w.OnSample(10*time.Millisecond, 1, false)
	w.OnSample(10*time.Millisecond, 1, false)
	clk.advance(1100 * time.Millisecond)
	w.OnSample(10*time.Millisecond, 1, false) // window elapses but only 2 samples

	assert.Equal(t, 0, d.samples, "window with < windowSize samples is skipped")
}

func TestWindowedPropagatesDropAndLimit(t *testing.T) {
	d := &countingLimit{limit: 42}
	clk := &fakeClock{t: time.Unix(4000, 0)}
	w := newWindowed(d, clk, WindowedWindowSize(2),
		WindowedMinWindowTime(time.Second), WindowedMaxWindowTime(time.Second))

	assert.Equal(t, 42, w.Limit()) // delegates Limit()

	w.OnSample(10*time.Millisecond, 3, false)
	w.OnSample(10*time.Millisecond, 5, true) // a drop in the window
	clk.advance(1100 * time.Millisecond)
	w.OnSample(10*time.Millisecond, 1, false)

	assert.Equal(t, 1, d.samples)
	assert.True(t, d.lastDrop, "any drop in the window propagates")
	assert.Equal(t, 5, d.lastFlite)
}

func TestWindowedWrapsRealLimit(t *testing.T) {
	// Smoke test: a windowed Gradient2 still works end-to-end without panicking.
	w := NewWindowedLimit(NewGradient2Limit())
	for i := 0; i < 100; i++ {
		w.OnSample(5*time.Millisecond, 10, false)
	}
	assert.GreaterOrEqual(t, w.Limit(), 1)
}
