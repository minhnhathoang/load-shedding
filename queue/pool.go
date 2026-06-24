// Package queue provides queue-based load shedding modeled on a Java fixed
// thread pool with a bounded queue, i.e. a ThreadPoolExecutor backed by a
// bounded BlockingQueue with the AbortPolicy:
//
//   - a fixed number of worker goroutines process submitted tasks;
//   - tasks wait in a bounded queue when all workers are busy;
//   - submissions are rejected once the budget is exhausted.
//
// It supports two of the four queue shedding policies directly:
//
//   - policy 1, length: reject immediately when full (MaxWait == 0, ErrQueueFull);
//   - policy 2, enqueue-wait timeout: block up to MaxWait for a slot, then
//     reject (ErrQueueTimeout).
//
// Policies 3 and 4 (CoDel sojourn-time dropping and adaptive LIFO) are provided
// by CodelPool in codel.go.
package queue

import (
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrQueueFull is returned by Submit when the bounded queue is full.
	ErrQueueFull = errors.New("queue: queue full, request rejected")
	// ErrQueueTimeout is returned by Submit when MaxWait elapses before a slot
	// becomes available (policy 2 — enqueue-wait timeout).
	ErrQueueTimeout = errors.New("queue: timed out waiting for a slot")
	// ErrOverloaded is returned by Submit when the CPU gate rejects the request
	// before it reaches the queue (CpuThreshold exceeded).
	ErrOverloaded = errors.New("queue: cpu overloaded")
	// ErrStopped is returned by Submit after the pool has been stopped.
	ErrStopped = errors.New("queue: pool stopped")
	// errNilTask is returned by Submit when given a nil task.
	errNilTask = errors.New("queue: nil task")
)

// Config configures the bounded worker pool.
type Config struct {
	// Workers is the fixed number of worker goroutines (the "thread pool size").
	// Values below 1 are treated as 1.
	Workers int
	// QueueCapacity is the size of the bounded wait queue. When the queue is
	// full and all workers are busy, submissions are rejected. A value of 0
	// means no queue: a task is rejected unless a worker is immediately ready.
	QueueCapacity int
	// MaxWait, when > 0, makes Submit block up to this duration for a free slot
	// before rejecting with ErrQueueTimeout (policy 2 — enqueue-wait timeout,
	// like Resilience4j ThreadPoolBulkhead.maxWaitDuration). When 0 (default),
	// Submit never blocks and rejects immediately with ErrQueueFull (policy 1).
	MaxWait time.Duration
	// CpuThreshold, when > 0 and Gate is nil, installs a static CPU gate: a
	// request is rejected with ErrOverloaded (before reserving a slot) when CPU
	// usage (millicpu, 0-1000) is at or above this value.
	CpuThreshold int64
	// Gate, when set, is the admission gate placed in front of the queue
	// (overrides CpuThreshold). Use NewCPUThresholdGate, NewGozeroGate, or a
	// custom Gate. The gate can be toggled on/off at runtime via SetGateEnabled.
	Gate Gate
}

// Stats is a point-in-time snapshot of pool activity.
type Stats struct {
	Queued   int   // tasks currently waiting in the queue
	Active   int64 // tasks currently being executed
	Accepted int64 // cumulative accepted submissions
	Rejected int64 // cumulative rejected submissions
}

// Pool is a fixed-size worker pool with a bounded queue.
//
// Admission is governed by a semaphore sized Workers + QueueCapacity (the total
// number of in-flight tasks: running plus waiting). This decouples acceptance
// from worker scheduling, so an idle worker never wrongly rejects a task and a
// zero-length queue behaves deterministically. The tasks channel shares that
// capacity, so a send after acquiring a token never blocks.
type Pool struct {
	tasks    chan func()
	sem      chan struct{}
	maxWait  time.Duration
	gate     Gate
	wg       sync.WaitGroup
	mu       sync.RWMutex
	closed   bool
	stopOnce sync.Once

	active   atomic.Int64
	accepted atomic.Int64
	rejected atomic.Int64
}

// New starts a Pool with cfg.Workers goroutines and a queue of cfg.QueueCapacity.
func New(cfg Config) *Pool {
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.QueueCapacity < 0 {
		cfg.QueueCapacity = 0
	}

	capacity := cfg.Workers + cfg.QueueCapacity
	p := &Pool{
		tasks:   make(chan func(), capacity),
		sem:     make(chan struct{}, capacity),
		maxWait: cfg.MaxWait,
		gate:    gateFor(cfg.Gate, cfg.CpuThreshold),
	}
	p.wg.Add(cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		go p.worker()
	}
	return p
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for task := range p.tasks {
		p.run(task)
		<-p.sem // release the in-flight slot after the task completes
	}
}

func (p *Pool) run(task func()) {
	p.active.Add(1)
	defer p.active.Add(-1)
	// runSafe recovers and logs panics so a faulty task can't kill the worker.
	runSafe(task)
}

// Submit enqueues task for execution. With MaxWait == 0 it never blocks and
// returns ErrQueueFull when the budget is exhausted (policy 1). With MaxWait > 0
// it blocks up to MaxWait for a slot, returning ErrQueueTimeout on expiry
// (policy 2). It returns ErrStopped if the pool has been stopped.
func (p *Pool) Submit(task func()) error {
	if task == nil {
		return errNilTask
	}

	return p.submitWithOutcome(func() bool {
		task()
		return true
	})
}

func (p *Pool) submitWithOutcome(task func() bool) error {
	// Admission gate (e.g. CPU): reject early before reserving a slot.
	done, ok := p.gateAllow()
	if !ok {
		p.rejected.Add(1)
		return ErrOverloaded
	}

	if !p.acquire() {
		done(false) // release the gate (e.g. go-zero promise) — request shed
		p.rejected.Add(1)
		if p.maxWait > 0 {
			return ErrQueueTimeout
		}
		return ErrQueueFull
	}

	// Slot acquired (not under lock, so a blocking wait never stalls Stop).
	// Briefly lock to send safely against a concurrent Stop closing the channel.
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		<-p.sem // release the slot we took
		done(false)
		return ErrStopped
	}
	// cap(tasks) == cap(sem) and we hold a token ⇒ this send never blocks.
	p.tasks <- func() {
		success := false
		defer func() {
			if panicValue := recover(); panicValue != nil {
				done(false)
				panic(panicValue)
			}
			done(success)
		}()
		success = task()
	}
	p.accepted.Add(1)
	return nil
}

func (p *Pool) gateAllow() (func(bool), bool) {
	if p.gate == nil {
		return noopDone, true
	}
	return p.gate.Allow()
}

// Gate returns the admission gate, or nil if none is configured.
func (p *Pool) Gate() Gate { return p.gate }

// SetGateEnabled toggles the admission gate on/off at runtime. No-op if there is
// no gate.
func (p *Pool) SetGateEnabled(on bool) {
	if p.gate != nil {
		p.gate.SetEnabled(on)
	}
}

// acquire takes an in-flight slot, waiting up to maxWait when set.
func (p *Pool) acquire() bool {
	if p.maxWait <= 0 {
		select {
		case p.sem <- struct{}{}:
			return true
		default:
			return false
		}
	}

	timer := time.NewTimer(p.maxWait)
	defer timer.Stop()
	select {
	case p.sem <- struct{}{}:
		return true
	case <-timer.C:
		return false
	}
}

// SubmitWait submits task and blocks until it has finished executing. It returns
// ErrQueueFull / ErrStopped without waiting if the task could not be accepted.
func (p *Pool) SubmitWait(task func()) error {
	if task == nil {
		return errNilTask
	}

	return p.submitWaitOutcome(func() bool {
		task()
		return true
	})
}

func (p *Pool) submitWaitOutcome(task func() bool) error {
	done := make(chan struct{})
	if err := p.submitWithOutcome(func() bool {
		defer close(done)
		return task()
	}); err != nil {
		return err
	}

	<-done
	return nil
}

// Stats returns a snapshot of current pool activity.
func (p *Pool) Stats() Stats {
	return Stats{
		Queued:   len(p.tasks),
		Active:   p.active.Load(),
		Accepted: p.accepted.Load(),
		Rejected: p.rejected.Load(),
	}
}

// Stop closes the pool and waits for all workers to drain the queue and exit.
// Queued tasks are still executed (graceful, like ExecutorService.shutdown()).
// Stop is safe to call multiple times.
func (p *Pool) Stop() {
	p.stopOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		close(p.tasks)
		p.mu.Unlock()
		p.wg.Wait()
	})
}

// Handler returns a middleware that runs each request on the pool, responding
// with 503 Service Unavailable when the request is shed (queue full / stopped).
func (p *Pool) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var panicValue any
		err := p.submitWaitOutcome(func() (success bool) {
			cw := &codeWriter{ResponseWriter: w, code: http.StatusOK}
			defer func() {
				if p := recover(); p != nil {
					panicValue = p
					success = false
				}
			}()
			next.ServeHTTP(cw, r)
			return cw.code < http.StatusInternalServerError
		})
		if err != nil {
			w.Header().Set("Connection", "close")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if panicValue != nil {
			panic(panicValue)
		}
	})
}
