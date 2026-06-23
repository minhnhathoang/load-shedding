package queue

import (
	"errors"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zeromicro/go-zero/core/threading"
)

// Default CoDel parameters (the values from the CoDel paper / folly).
const (
	defaultTarget   = 5 * time.Millisecond
	defaultInterval = 100 * time.Millisecond
)

// ErrDropped is returned by SubmitWait when CoDel drops the task at dequeue
// because it waited longer than the acceptable sojourn time.
var ErrDropped = errors.New("queue: task dropped by codel")

// CodelConfig configures a CodelPool.
type CodelConfig struct {
	// Workers is the fixed number of worker goroutines. Values below 1 → 1.
	Workers int
	// Capacity is the hard cap on queued (waiting) tasks; submissions are
	// rejected with ErrQueueFull once exceeded. Values below 1 → 1.
	Capacity int
	// Target is the acceptable standing-queue sojourn time. Default 5ms.
	Target time.Duration
	// Interval is the window over which a standing queue must persist before
	// CoDel starts dropping. Default 100ms.
	Interval time.Duration
	// AdaptiveLIFO serves the newest task first while CoDel is in its dropping
	// state, so fresh requests meet their deadline and the stale tail is shed.
	AdaptiveLIFO bool
	// CpuThreshold, when > 0 and Gate is nil, installs a static CPU gate: a
	// request is rejected with ErrOverloaded (before it enters the queue) when
	// CPU usage (millicpu, 0-1000) is at or above this value.
	CpuThreshold int64
	// Gate, when set, is the admission gate placed in front of the queue
	// (overrides CpuThreshold). Toggle on/off at runtime via SetGateEnabled.
	Gate Gate
}

// CodelStats is a point-in-time snapshot of CodelPool activity.
type CodelStats struct {
	Queued   int
	Active   int64
	Accepted int64
	Rejected int64 // rejected at submit (capacity)
	Dropped  int64 // dropped at dequeue (CoDel sojourn)
}

type codelItem struct {
	task   func()
	onDrop func() // invoked instead of task when CoDel drops it; may be nil
	enq    time.Time
}

// CodelPool is a worker pool whose queue sheds by sojourn time (CoDel, policy 3)
// rather than by length, optionally serving newest-first under load (adaptive
// LIFO, policy 4). It bounds tail latency instead of concurrency: a task that
// has waited too long is dropped at dequeue rather than run as dead work.
type CodelPool struct {
	workers      int
	capacity     int
	target       time.Duration
	interval     time.Duration
	adaptiveLIFO bool
	gate         Gate

	mu       sync.Mutex
	notEmpty *sync.Cond
	buf      []codelItem
	closed   bool
	stopOnce sync.Once
	wg       sync.WaitGroup

	// CoDel control state (guarded by mu).
	dropping   bool
	firstAbove time.Time // time at which sojourn first exceeded target
	dropNext   time.Time
	count      int

	active   atomic.Int64
	accepted atomic.Int64
	rejected atomic.Int64
	dropped  atomic.Int64

	now func() time.Time // injectable clock for tests
}

// NewCodel starts a CodelPool from cfg.
func NewCodel(cfg CodelConfig) *CodelPool {
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.Capacity < 1 {
		cfg.Capacity = 1
	}
	if cfg.Target <= 0 {
		cfg.Target = defaultTarget
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultInterval
	}

	p := &CodelPool{
		workers:      cfg.Workers,
		capacity:     cfg.Capacity,
		target:       cfg.Target,
		interval:     cfg.Interval,
		adaptiveLIFO: cfg.AdaptiveLIFO,
		gate:         gateFor(cfg.Gate, cfg.CpuThreshold),
		now:          time.Now,
	}
	p.notEmpty = sync.NewCond(&p.mu)
	p.wg.Add(cfg.Workers)
	for i := 0; i < cfg.Workers; i++ {
		go p.worker()
	}
	return p
}

// Submit enqueues task, rejecting with ErrQueueFull when the queue is at
// Capacity or ErrStopped after Stop. It never blocks. Note: an accepted task may
// still be dropped later by CoDel; use SubmitWait to observe that.
func (p *CodelPool) Submit(task func()) error {
	return p.submit(task, nil)
}

func (p *CodelPool) submit(task, onDrop func()) error {
	if task == nil {
		return errNilTask
	}

	// Admission gate (e.g. CPU): reject early before the request enters the queue.
	done, ok := p.gateAllow()
	if !ok {
		p.rejected.Add(1)
		return ErrOverloaded
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		done(false)
		return ErrStopped
	}
	if len(p.buf) >= p.capacity {
		p.rejected.Add(1)
		done(false)
		return ErrQueueFull
	}

	// Thread the gate outcome: done(true) on run, done(false) on CoDel drop.
	runTask := func() { defer done(true); task() }
	dropCb := func() {
		done(false)
		if onDrop != nil {
			onDrop()
		}
	}
	p.buf = append(p.buf, codelItem{task: runTask, onDrop: dropCb, enq: p.now()})
	p.accepted.Add(1)
	p.notEmpty.Signal()
	return nil
}

func (p *CodelPool) gateAllow() (func(bool), bool) {
	if p.gate == nil {
		return noopDone, true
	}
	return p.gate.Allow()
}

// Gate returns the admission gate, or nil if none is configured.
func (p *CodelPool) Gate() Gate { return p.gate }

// SetGateEnabled toggles the admission gate on/off at runtime. No-op if there is
// no gate.
func (p *CodelPool) SetGateEnabled(on bool) {
	if p.gate != nil {
		p.gate.SetEnabled(on)
	}
}

// SubmitWait submits task and blocks until it has run or been dropped. It
// returns ErrQueueFull/ErrStopped if rejected at submit, or ErrDropped if CoDel
// shed it at dequeue.
func (p *CodelPool) SubmitWait(task func()) error {
	if task == nil {
		return errNilTask
	}

	done := make(chan struct{})
	var wasDropped atomic.Bool
	err := p.submit(
		func() { defer close(done); task() },
		func() { wasDropped.Store(true); close(done) },
	)
	if err != nil {
		return err
	}

	<-done
	if wasDropped.Load() {
		return ErrDropped
	}
	return nil
}

func (p *CodelPool) worker() {
	defer p.wg.Done()
	for {
		p.mu.Lock()
		for len(p.buf) == 0 && !p.closed {
			p.notEmpty.Wait()
		}
		if len(p.buf) == 0 && p.closed {
			p.mu.Unlock()
			return
		}

		now := p.now()
		// CoDel evaluates the oldest item (head = max sojourn).
		if p.shouldDrop(p.buf[0].enq, now) {
			it := p.buf[0]
			p.buf = p.buf[1:]
			p.dropped.Add(1)
			p.mu.Unlock()
			if it.onDrop != nil {
				it.onDrop()
			}
			continue
		}

		// Serve the survivor: newest-first while dropping if adaptive LIFO.
		var it codelItem
		if p.adaptiveLIFO && p.dropping {
			last := len(p.buf) - 1
			it = p.buf[last]
			p.buf = p.buf[:last]
		} else {
			it = p.buf[0]
			p.buf = p.buf[1:]
		}
		p.mu.Unlock()

		p.run(it.task)
	}
}

// shouldDrop implements the CoDel control law. Caller holds p.mu.
func (p *CodelPool) shouldDrop(enq, now time.Time) bool {
	sojourn := now.Sub(enq)
	if sojourn < p.target {
		// Queue drained below target → leave the dropping state.
		p.firstAbove = time.Time{}
		p.dropping = false
		return false
	}

	// Sojourn is above target.
	if p.firstAbove.IsZero() {
		// Start the timer; a standing queue must persist for `interval`.
		p.firstAbove = now.Add(p.interval)
		return false
	}
	if now.Before(p.firstAbove) {
		return false
	}

	// Standing queue confirmed.
	if !p.dropping {
		p.dropping = true
		p.count = 1
		p.dropNext = now.Add(p.interval)
		return true
	}
	if now.Before(p.dropNext) {
		return false
	}
	// Control law: drop faster the longer the overload persists.
	p.count++
	p.dropNext = now.Add(time.Duration(float64(p.interval) / math.Sqrt(float64(p.count))))
	return true
}

func (p *CodelPool) run(task func()) {
	p.active.Add(1)
	defer p.active.Add(-1)
	threading.RunSafe(task)
}

// Stats returns a snapshot of current activity.
func (p *CodelPool) Stats() CodelStats {
	p.mu.Lock()
	queued := len(p.buf)
	p.mu.Unlock()
	return CodelStats{
		Queued:   queued,
		Active:   p.active.Load(),
		Accepted: p.accepted.Load(),
		Rejected: p.rejected.Load(),
		Dropped:  p.dropped.Load(),
	}
}

// Stop closes the pool and waits for workers to drain and exit. Stop is
// idempotent.
func (p *CodelPool) Stop() {
	p.stopOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.notEmpty.Broadcast()
		p.mu.Unlock()
		p.wg.Wait()
	})
}

// Handler returns a middleware that runs each request on the pool, responding
// with 503 when the request is shed (rejected at submit or dropped by CoDel).
func (p *CodelPool) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := p.SubmitWait(func() { next.ServeHTTP(w, r) }); err != nil {
			w.Header().Set("Connection", "close")
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
}
