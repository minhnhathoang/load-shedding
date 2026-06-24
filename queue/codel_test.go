package queue

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCodelRunsWhenNotOverloaded(t *testing.T) {
	p := NewCodel(CodelConfig{Workers: 2, Capacity: 100})
	defer p.Stop()

	var ran atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		assert.NoError(t, p.Submit(func() {
			ran.Add(1)
			wg.Done()
		}))
	}
	wg.Wait()
	assert.Equal(t, int64(20), ran.Load())
	assert.Zero(t, p.Stats().Dropped)
}

func TestCodelRejectsWhenCapacityFull(t *testing.T) {
	p := NewCodel(CodelConfig{Workers: 1, Capacity: 1})
	defer p.Stop()

	block := make(chan struct{})
	started := make(chan struct{})
	assert.NoError(t, p.Submit(func() {
		close(started)
		<-block
	}))
	<-started // worker busy

	assert.NoError(t, p.Submit(func() {}))               // fills the 1-slot queue
	assert.ErrorIs(t, p.Submit(func() {}), ErrQueueFull) // over capacity
	close(block)
}

// fakeClock drives sojourn time deterministically.
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

func TestShouldDropControlLaw(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	p := &CodelPool{target: 5 * time.Millisecond, interval: 100 * time.Millisecond, now: clk.now}

	enq := clk.now()

	// Sojourn below target → never drop, not in dropping state.
	assert.False(t, p.shouldDrop(enq, clk.now()))
	assert.False(t, p.dropping)

	// Sojourn above target but standing-queue timer not yet elapsed → no drop.
	clk.advance(10 * time.Millisecond)            // sojourn 10ms > 5ms target
	assert.False(t, p.shouldDrop(enq, clk.now())) // arms firstAbove
	assert.False(t, p.shouldDrop(enq, clk.now())) // still within interval

	// After the interval persists above target → enter dropping, drop one.
	clk.advance(100 * time.Millisecond)
	assert.True(t, p.shouldDrop(enq, clk.now()))
	assert.True(t, p.dropping)

	// Immediately after a drop, dropNext is in the future → no drop yet.
	assert.False(t, p.shouldDrop(enq, clk.now()))

	// A fresh (below-target) sojourn clears the dropping state.
	fresh := clk.now()
	assert.False(t, p.shouldDrop(fresh, clk.now()))
	assert.False(t, p.dropping)
}

func TestCodelDropsUnderSustainedLoad(t *testing.T) {
	// Submit faster than a slow worker can serve so the queue stands above target
	// for longer than interval — the condition under which CoDel drops. (A
	// transient burst that drains quickly is correctly NOT dropped, which is why
	// this needs sustained, real-time load.)
	p := NewCodel(CodelConfig{
		Workers:  1,
		Capacity: 100000,
		Target:   time.Millisecond,
		Interval: 10 * time.Millisecond,
	})
	defer p.Stop()

	var ran atomic.Int64
	var submitted int64
	deadline := time.After(250 * time.Millisecond)
loop:
	for {
		select {
		case <-deadline:
			break loop
		default:
		}
		if err := p.Submit(func() {
			time.Sleep(2 * time.Millisecond) // service slower than submit rate
			ran.Add(1)
		}); err == nil {
			submitted++
		}
		time.Sleep(300 * time.Microsecond) // ~3300/s submit vs ~500/s service
	}

	// Drain: every accepted task must end up either run or dropped.
	assert.Eventually(t, func() bool {
		st := p.Stats()
		return st.Queued == 0 && st.Active == 0 && ran.Load()+st.Dropped == st.Accepted
	}, 5*time.Second, 10*time.Millisecond)

	st := p.Stats()
	assert.Positive(t, st.Dropped, "sustained overload should trigger CoDel drops")
	assert.Equal(t, submitted, st.Accepted)
}

func TestCodelSubmitWaitRuns(t *testing.T) {
	p := NewCodel(CodelConfig{Workers: 2, Capacity: 16})
	defer p.Stop()

	var ran atomic.Bool
	assert.NoError(t, p.SubmitWait(func() { ran.Store(true) }))
	assert.True(t, ran.Load())
}

func TestCodelStopIdempotent(t *testing.T) {
	p := NewCodel(CodelConfig{Workers: 2, Capacity: 4})
	p.Stop()
	assert.NotPanics(t, p.Stop)
	assert.ErrorIs(t, p.Submit(func() {}), ErrStopped)
}

func TestCodelAdaptiveLIFOServesNewestFirst(t *testing.T) {
	// Smoke test: adaptive LIFO must not deadlock or lose tasks.
	p := NewCodel(CodelConfig{Workers: 1, Capacity: 50, AdaptiveLIFO: true})
	defer p.Stop()

	var ran atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		assert.NoError(t, p.Submit(func() { ran.Add(1); wg.Done() }))
	}
	wg.Wait()
	assert.Equal(t, int64(30), ran.Load())
}

func TestCodelHandlerPropagatesPanicAndMarksGateFailure(t *testing.T) {
	g := &recordingGate{admit: true}
	p := NewCodel(CodelConfig{Workers: 1, Capacity: 8, Gate: g})
	defer p.Stop()

	h := p.Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))

	assert.Panics(t, func() {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	})
	assert.Equal(t, int64(1), g.falseN.Load())
	assert.Equal(t, int64(0), g.trueN.Load())
}
