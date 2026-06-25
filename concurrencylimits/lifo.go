package concurrencylimits

import (
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// defaultBacklogSize is Netflix's default maxBacklogSize.
const defaultBacklogSize = 100

// LifoBlockingLimiter wraps a Limiter (typically a SimpleLimiter) with a bounded
// LIFO backlog queue, porting Netflix's LifoBlockingLimiter. When the delegate
// is at its limit, callers wait in a backlog instead of being rejected; the
// newest waiter is served first (LIFO), waiters are rejected once the backlog is
// full, and each waiter is rejected after BacklogTimeout.
type LifoBlockingLimiter struct {
	delegate       Limiter
	backlogSize    int
	backlogTimeout time.Duration
	logger         *slog.Logger // nil = no logging

	mu      sync.Mutex
	backlog []*waiter // index 0 is the newest waiter (front)
}

type waiter struct {
	ready    chan struct{}
	listener Listener // set by unblock when admitted (guarded by limiter mu)
	removed  bool     // guarded by limiter mu
}

// LifoOption customizes a LifoBlockingLimiter.
type LifoOption func(*LifoBlockingLimiter)

// WithLifoLogger attaches an slog.Logger. When set, the limiter emits a Debug
// log when a request enters the backlog (and when it is shed by a full backlog
// or a timeout). Default is no logging.
func WithLifoLogger(logger *slog.Logger) LifoOption {
	return func(b *LifoBlockingLimiter) { b.logger = logger }
}

// NewLifoBlockingLimiter wraps delegate with a LIFO backlog. backlogSize <= 0
// uses the default (100).
func NewLifoBlockingLimiter(delegate Limiter, backlogSize int, backlogTimeout time.Duration, opts ...LifoOption) *LifoBlockingLimiter {
	if backlogSize <= 0 {
		backlogSize = defaultBacklogSize
	}
	b := &LifoBlockingLimiter{
		delegate:       delegate,
		backlogSize:    backlogSize,
		backlogTimeout: backlogTimeout,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Acquire implements Limiter. It blocks up to BacklogTimeout when the delegate
// is saturated, returning (nil, false) if the backlog is full or the wait times
// out.
func (b *LifoBlockingLimiter) Acquire() (Listener, bool) {
	inner, ok := b.tryAcquire()
	if !ok {
		return nil, false
	}
	return &lifoListener{inner: inner, b: b}, true
}

func (b *LifoBlockingLimiter) tryAcquire() (Listener, bool) {
	// Fast path: a permit is immediately available.
	if l, ok := b.delegate.Acquire(); ok {
		return l, true
	}

	b.mu.Lock()
	if len(b.backlog) >= b.backlogSize {
		size := len(b.backlog)
		b.mu.Unlock()
		if b.logger != nil {
			b.logger.Debug("concurrencylimits: shed (LIFO backlog full)", slog.Int("backlog", size))
		}
		return nil, false
	}
	w := &waiter{ready: make(chan struct{})}
	b.backlog = append([]*waiter{w}, b.backlog...) // addFirst → LIFO
	size := len(b.backlog)
	b.mu.Unlock()
	if b.logger != nil {
		b.logger.Debug("concurrencylimits: request added to LIFO backlog", slog.Int("backlog", size))
	}

	timer := time.NewTimer(b.backlogTimeout)
	defer timer.Stop()

	select {
	case <-w.ready:
		b.mu.Lock()
		l := w.listener
		b.mu.Unlock()
		return l, l != nil
	case <-timer.C:
		b.mu.Lock()
		if w.removed {
			// unblock admitted us between the timer firing and acquiring the lock
			l := w.listener
			b.mu.Unlock()
			return l, l != nil
		}
		b.removeWaiter(w)
		w.removed = true
		b.mu.Unlock()
		if b.logger != nil {
			b.logger.Debug("concurrencylimits: shed (LIFO backlog timeout)")
		}
		return nil, false
	}
}

// unblock is called when a wrapped request completes, freeing a permit. It hands
// the freed permit to the newest waiter (LIFO), if any.
func (b *LifoBlockingLimiter) unblock() {
	b.mu.Lock()
	if len(b.backlog) == 0 {
		b.mu.Unlock()
		return
	}
	w := b.backlog[0] // peekFirst (newest)
	l, ok := b.delegate.Acquire()
	if !ok {
		b.mu.Unlock()
		return
	}
	b.backlog = b.backlog[1:] // removeFirst
	w.listener = l
	w.removed = true
	close(w.ready)
	b.mu.Unlock()
}

// removeWaiter removes w from the backlog. Caller holds b.mu.
func (b *LifoBlockingLimiter) removeWaiter(w *waiter) {
	for i, x := range b.backlog {
		if x == w {
			b.backlog = append(b.backlog[:i], b.backlog[i+1:]...)
			return
		}
	}
}

// Handler returns a middleware gating admission via this limiter.
func (b *LifoBlockingLimiter) Handler(next http.Handler) http.Handler {
	return Handler(b, next)
}

type lifoListener struct {
	inner Listener
	b     *LifoBlockingLimiter
}

func (l *lifoListener) OnSuccess() { l.inner.OnSuccess(); l.b.unblock() }
func (l *lifoListener) OnIgnore()  { l.inner.OnIgnore(); l.b.unblock() }
func (l *lifoListener) OnDropped() { l.inner.OnDropped(); l.b.unblock() }
