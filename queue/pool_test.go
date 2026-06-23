package queue

import (
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func init() {
	// silence runSafe's panic logs during the panic-recovery test
	log.SetOutput(io.Discard)
}

func TestMaxWaitTimesOut(t *testing.T) {
	// 1 worker, no queue, short MaxWait: occupy the worker, then a second submit
	// should block ~MaxWait and return ErrQueueTimeout.
	p := New(Config{Workers: 1, QueueCapacity: 0, MaxWait: 50 * time.Millisecond})
	defer p.Stop()

	block := make(chan struct{})
	started := make(chan struct{})
	assert.NoError(t, p.Submit(func() {
		close(started)
		<-block
	}))
	<-started

	start := time.Now()
	err := p.Submit(func() {})
	elapsed := time.Since(start)

	assert.ErrorIs(t, err, ErrQueueTimeout)
	assert.GreaterOrEqual(t, elapsed, 40*time.Millisecond) // waited ~MaxWait
	close(block)
}

func TestMaxWaitSucceedsWhenSlotFreesUp(t *testing.T) {
	p := New(Config{Workers: 1, QueueCapacity: 0, MaxWait: 500 * time.Millisecond})
	defer p.Stop()

	release := make(chan struct{})
	started := make(chan struct{})
	assert.NoError(t, p.Submit(func() {
		close(started)
		<-release
	}))
	<-started

	// Free the worker shortly; the waiting submit should then succeed.
	go func() {
		time.Sleep(30 * time.Millisecond)
		close(release)
	}()

	var ran atomic.Bool
	assert.NoError(t, p.SubmitWait(func() { ran.Store(true) }))
	assert.True(t, ran.Load())
}

func TestSubmitNil(t *testing.T) {
	p := New(Config{Workers: 1, QueueCapacity: 1})
	defer p.Stop()
	assert.Equal(t, errNilTask, p.Submit(nil))
	assert.Equal(t, errNilTask, p.SubmitWait(nil))
}

func TestRejectWhenQueueFull(t *testing.T) {
	p := New(Config{Workers: 1, QueueCapacity: 1})
	defer p.Stop()

	block := make(chan struct{})
	started := make(chan struct{})

	// Occupy the single worker.
	assert.NoError(t, p.Submit(func() {
		close(started)
		<-block
	}))
	<-started // worker is now busy

	// Queue (capacity 1) accepts one waiting task.
	assert.NoError(t, p.Submit(func() {}))

	// Queue full and worker busy => reject.
	assert.ErrorIs(t, p.Submit(func() {}), ErrQueueFull)

	st := p.Stats()
	assert.Equal(t, int64(1), st.Rejected)
	assert.Equal(t, int64(2), st.Accepted)

	close(block)
}

func TestZeroQueueRejectsWhenBusy(t *testing.T) {
	p := New(Config{Workers: 1, QueueCapacity: 0})
	defer p.Stop()

	block := make(chan struct{})
	started := make(chan struct{})
	assert.NoError(t, p.Submit(func() {
		close(started)
		<-block
	}))
	<-started

	// No queue and worker busy => immediate reject.
	assert.ErrorIs(t, p.Submit(func() {}), ErrQueueFull)
	close(block)
}

func TestSubmitWaitRunsToCompletion(t *testing.T) {
	p := New(Config{Workers: 2, QueueCapacity: 4})
	defer p.Stop()

	var ran atomic.Bool
	assert.NoError(t, p.SubmitWait(func() { ran.Store(true) }))
	assert.True(t, ran.Load())
}

func TestPanicDoesNotKillWorker(t *testing.T) {
	p := New(Config{Workers: 1, QueueCapacity: 1})
	defer p.Stop()

	// A panicking task must be recovered, leaving the worker alive.
	assert.NoError(t, p.SubmitWait(func() { panic("boom") }))

	var ran atomic.Bool
	assert.NoError(t, p.SubmitWait(func() { ran.Store(true) }))
	assert.True(t, ran.Load())
}

func TestStopIsGracefulAndDrainsQueue(t *testing.T) {
	p := New(Config{Workers: 1, QueueCapacity: 8})

	var count atomic.Int64
	block := make(chan struct{})
	started := make(chan struct{})
	assert.NoError(t, p.Submit(func() {
		close(started)
		<-block
		count.Add(1)
	}))
	<-started

	// Queue several more tasks behind the blocked worker.
	for i := 0; i < 5; i++ {
		assert.NoError(t, p.Submit(func() { count.Add(1) }))
	}

	close(block)
	p.Stop() // must wait for all 6 tasks to finish
	assert.Equal(t, int64(6), count.Load())
}

func TestSubmitAfterStop(t *testing.T) {
	p := New(Config{Workers: 1, QueueCapacity: 1})
	p.Stop()
	assert.ErrorIs(t, p.Submit(func() {}), ErrStopped)
	assert.ErrorIs(t, p.SubmitWait(func() {}), ErrStopped)
}

func TestStopIdempotent(t *testing.T) {
	p := New(Config{Workers: 2, QueueCapacity: 2})
	p.Stop()
	assert.NotPanics(t, p.Stop)
}

func TestDefaultsClampInvalidConfig(t *testing.T) {
	p := New(Config{Workers: 0, QueueCapacity: -5})
	defer p.Stop()
	// Workers clamped to 1, queue clamped to 0: one task runs, second rejected.
	block := make(chan struct{})
	started := make(chan struct{})
	assert.NoError(t, p.Submit(func() {
		close(started)
		<-block
	}))
	<-started
	assert.ErrorIs(t, p.Submit(func() {}), ErrQueueFull)
	close(block)
}

func TestHandlerServesAndSheds(t *testing.T) {
	p := New(Config{Workers: 1, QueueCapacity: 0})
	defer p.Stop()

	block := make(chan struct{})
	started := make(chan struct{})
	var occupyOnce sync.Once

	h := p.Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		occupyOnce.Do(func() {
			close(started)
			<-block
		})
		w.WriteHeader(http.StatusOK)
	}))

	// First request occupies the worker; run it in the background.
	go h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	<-started

	// Second request is shed: no queue, worker busy.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	close(block)
}

func TestConcurrentSubmit(t *testing.T) {
	p := New(Config{Workers: 4, QueueCapacity: 16})
	defer p.Stop()

	var done atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if err := p.Submit(func() {
					time.Sleep(time.Microsecond)
					done.Add(1)
				}); err != nil {
					// rejection is expected under load; just count nothing.
					_ = err
				}
			}
		}()
	}
	wg.Wait()

	st := p.Stats()
	assert.Equal(t, int64(10000), st.Accepted+st.Rejected)
}
