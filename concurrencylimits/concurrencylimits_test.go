package concurrencylimits

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLog10Int(t *testing.T) {
	cases := map[int]int{0: 1, 1: 1, 9: 1, 10: 1, 99: 1, 100: 2, 999: 2, 1000: 3, 12345: 4}
	for in, want := range cases {
		assert.Equalf(t, want, log10Int(in), "log10Int(%d)", in)
	}
}

func TestFixedLimit(t *testing.T) {
	assert.Equal(t, 5, FixedLimit(5).Limit())
}

// --- limit algorithms ---

func TestVegasGrowsUnderHealthyLatencyAndShrinksUnderHighLatency(t *testing.T) {
	v := NewVegasLimit(VegasInitialLimit(20), VegasMaxConcurrency(200))
	// Establish a no-load baseline.
	v.OnSample(10*time.Millisecond, 1, false)
	start := v.Limit()

	// Healthy: rtt ~ no-load, loaded enough to react → should grow (or hold).
	for i := 0; i < 50; i++ {
		v.OnSample(10*time.Millisecond, v.Limit(), false)
	}
	assert.GreaterOrEqual(t, v.Limit(), start)
	assert.LessOrEqual(t, v.Limit(), 200)

	// Sustained high latency (queueing) → the limit must shrink below the peak
	// at some point. (It can oscillate back up because Vegas periodically probes
	// and re-baselines rttNoLoad to the prevailing latency, so we track the min.)
	peak := v.Limit()
	minLimit := peak
	for i := 0; i < 200; i++ {
		v.OnSample(200*time.Millisecond, v.Limit(), false)
		if l := v.Limit(); l < minLimit {
			minLimit = l
		}
		assert.GreaterOrEqual(t, v.Limit(), 1)
	}
	assert.Less(t, minLimit, peak)
}

func TestGradientBoundedAndReactsToDrop(t *testing.T) {
	g := NewGradientLimit(GradientInitialLimit(50), GradientMaxConcurrency(100), GradientMinLimit(1))
	for i := 0; i < 100; i++ {
		g.OnSample(5*time.Millisecond, g.Limit(), false)
		assert.GreaterOrEqual(t, g.Limit(), 1)
		assert.LessOrEqual(t, g.Limit(), 100)
	}
	// A drop applies multiplicative backoff.
	before := g.Limit()
	g.OnSample(5*time.Millisecond, before, true)
	assert.LessOrEqual(t, g.Limit(), before)
}

func TestGradient2Bounded(t *testing.T) {
	g := NewGradient2Limit(Gradient2InitialLimit(20), Gradient2MaxConcurrency(200), Gradient2MinLimit(20))
	for i := 0; i < 500; i++ {
		rtt := 5 * time.Millisecond
		if i%3 == 0 {
			rtt = 20 * time.Millisecond
		}
		g.OnSample(rtt, g.Limit(), false)
		assert.GreaterOrEqual(t, g.Limit(), 20)
		assert.LessOrEqual(t, g.Limit(), 200)
	}
}

func TestLimitsHandleZeroRtt(t *testing.T) {
	for _, l := range []Limit{NewVegasLimit(), NewGradientLimit(), NewGradient2Limit(), FixedLimit(10)} {
		assert.NotPanics(t, func() { l.OnSample(0, 1, false) })
	}
}

// --- SimpleLimiter ---

func TestSimpleLimiterAdmitsUpToLimitThenRejects(t *testing.T) {
	l := NewSimpleLimiter(FixedLimit(2))

	a, ok := l.Acquire()
	assert.True(t, ok)
	b, ok := l.Acquire()
	assert.True(t, ok)
	assert.Equal(t, 2, l.Inflight())

	_, ok = l.Acquire() // over limit
	assert.False(t, ok)

	a.OnSuccess()
	assert.Equal(t, 1, l.Inflight())
	c, ok := l.Acquire() // slot freed
	assert.True(t, ok)

	b.OnSuccess()
	c.OnSuccess()
	assert.Equal(t, 0, l.Inflight())
}

func TestSimpleListenerIdempotent(t *testing.T) {
	l := NewSimpleLimiter(FixedLimit(1))
	a, _ := l.Acquire()
	a.OnSuccess()
	a.OnSuccess() // second call must be a no-op
	a.OnDropped()
	assert.Equal(t, 0, l.Inflight())
}

func TestSimpleLimiterFeedsSamples(t *testing.T) {
	v := NewVegasLimit(VegasInitialLimit(10))
	l := NewSimpleLimiter(v)
	a, ok := l.Acquire()
	assert.True(t, ok)
	a.OnSuccess() // first sample sets the no-load baseline
	assert.GreaterOrEqual(t, v.Limit(), 1)
}

// --- Handler ---

func TestHandlerServesAndSheds(t *testing.T) {
	l := NewSimpleLimiter(FixedLimit(1))
	block := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	h := l.Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		once.Do(func() { close(started); <-block })
		w.WriteHeader(http.StatusOK)
	}))

	go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	<-started // first request occupies the only slot

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	close(block)
}

func TestHandlerReleasesPermitOnPanic(t *testing.T) {
	l := NewSimpleLimiter(FixedLimit(1))
	h := l.Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))

	assert.Panics(t, func() {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	})
	assert.Equal(t, 0, l.Inflight())

	listener, ok := l.Acquire()
	assert.True(t, ok, "permit should be available after panic")
	listener.OnIgnore()
}

// --- LifoBlockingLimiter ---

func TestLifoRejectsWhenBacklogFull(t *testing.T) {
	delegate := NewSimpleLimiter(FixedLimit(1))
	b := NewLifoBlockingLimiter(delegate, 1, 50*time.Millisecond)

	a, ok := b.Acquire() // takes the only permit
	assert.True(t, ok)

	// one waiter fits the backlog (size 1); run it in the background
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _, _ = b.Acquire() }()
	time.Sleep(10 * time.Millisecond) // let it enqueue

	// backlog full → immediate reject
	_, ok = b.Acquire()
	assert.False(t, ok)

	a.OnSuccess() // frees the permit → the waiter is admitted
	wg.Wait()
}

func TestLifoTimesOut(t *testing.T) {
	delegate := NewSimpleLimiter(FixedLimit(1))
	b := NewLifoBlockingLimiter(delegate, 4, 40*time.Millisecond)

	a, ok := b.Acquire()
	assert.True(t, ok)
	defer a.OnSuccess()

	start := time.Now()
	_, ok = b.Acquire() // no permit, will wait then time out
	assert.False(t, ok)
	assert.GreaterOrEqual(t, time.Since(start), 30*time.Millisecond)
}

func TestLifoWaiterAdmittedOnRelease(t *testing.T) {
	delegate := NewSimpleLimiter(FixedLimit(1))
	b := NewLifoBlockingLimiter(delegate, 4, time.Second)

	a, ok := b.Acquire()
	assert.True(t, ok)

	admitted := make(chan bool, 1)
	go func() {
		l, ok := b.Acquire()
		admitted <- ok
		if ok {
			l.OnSuccess()
		}
	}()
	time.Sleep(20 * time.Millisecond)
	a.OnSuccess() // release → waiter should be admitted

	select {
	case ok := <-admitted:
		assert.True(t, ok, "waiter should be admitted when a permit frees")
	case <-time.After(time.Second):
		t.Fatal("waiter never admitted")
	}
}

func TestLifoNoPermitLeakUnderLoad(t *testing.T) {
	delegate := NewSimpleLimiter(FixedLimit(4))
	b := NewLifoBlockingLimiter(delegate, 1000, 200*time.Millisecond)

	var done atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 300; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, ok := b.Acquire()
			if !ok {
				return
			}
			time.Sleep(time.Millisecond)
			l.OnSuccess()
			done.Add(1)
		}()
	}
	wg.Wait()

	// After everything drains, inflight must be back to zero (no leaked permits).
	assert.Eventually(t, func() bool { return delegate.Inflight() == 0 }, 2*time.Second, 10*time.Millisecond)
	assert.Positive(t, done.Load())
}
