// Package queue provides queue-based load shedding modeled on a Java fixed
// thread pool with a bounded queue, i.e. a ThreadPoolExecutor backed by a
// bounded BlockingQueue with the AbortPolicy:
//
//   - a fixed number of worker goroutines process submitted tasks;
//   - tasks wait in a bounded queue when all workers are busy;
//   - submissions are rejected (ErrQueueFull) once the queue is full.
//
// Unlike the gozero (adaptive, CPU + Little's Law) and quarkus (Vegas +
// priority) packages, this one sheds purely on a fixed concurrency budget: workers +
// queue. There is no adaptivity — capacity is exactly Workers + QueueCapacity
// in-flight tasks.
package queue

import (
	"errors"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/zeromicro/go-zero/core/threading"
)

var (
	// ErrQueueFull is returned by Submit when the bounded queue is full.
	ErrQueueFull = errors.New("queueshedding: queue full, request rejected")
	// ErrStopped is returned by Submit after the pool has been stopped.
	ErrStopped = errors.New("queueshedding: pool stopped")
	// errNilTask is returned by Submit when given a nil task.
	errNilTask = errors.New("queueshedding: nil task")
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
		tasks: make(chan func(), capacity),
		sem:   make(chan struct{}, capacity),
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
	// RunSafe recovers and logs panics so a faulty task can't kill the worker.
	threading.RunSafe(task)
}

// Submit enqueues task for execution, returning ErrQueueFull if the queue is
// full or ErrStopped if the pool has been stopped. It never blocks.
func (p *Pool) Submit(task func()) error {
	if task == nil {
		return errNilTask
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrStopped
	}

	select {
	case p.sem <- struct{}{}:
		// Token acquired: capacity(tasks) == capacity(sem) guarantees this send
		// never blocks, so it is safe to do under the read lock.
		p.tasks <- task
		p.accepted.Add(1)
		return nil
	default:
		p.rejected.Add(1)
		return ErrQueueFull
	}
}

// SubmitWait submits task and blocks until it has finished executing. It returns
// ErrQueueFull / ErrStopped without waiting if the task could not be accepted.
func (p *Pool) SubmitWait(task func()) error {
	if task == nil {
		return errNilTask
	}

	done := make(chan struct{})
	if err := p.Submit(func() {
		defer close(done)
		task()
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
		err := p.SubmitWait(func() {
			next.ServeHTTP(w, r)
		})
		if err != nil {
			w.Header().Set("Connection", "close")
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
}
